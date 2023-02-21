
# Default imports
from airflow import DAG
from contextlib import contextmanager
from airflow.operators.dummy_operator import DummyOperator
from airflow.sensors.external_task_sensor import ExternalTaskSensor
from airflow.operators.latest_only_operator import LatestOnlyOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.operators.python import BranchPythonOperator
from airflow.utils.trigger_rule import TriggerRule

# Custom operators
from custom.operators.custom_api_to_s3 import ApiToS3Operator
from custom.operators.custom_crawler_to_s3 import CrawlerToS3Operator
from custom.operators.custom_s3_to_postgres import S3ToPostgresOperator

# Utils
from dag_factory.dag_factory_utils import *


# Main structure
def create_extraction_dags(
    origin_type,
    origin_name,
    dag_id,
    dag_version,
    start_date,
    schedule_interval,
    tags,
    default_args,
    extraction_method,
    extraction_object,
    extraction_keys,
    extraction_parameters = None,
    postgres_columns_list = None,
    history_saving = True,
    only_latest = False,
    sensors_list = None
): 

    # S3 connection info 
    airflow_s3_connection = "s3_bucket_connection"
    s3_bucket_name = "s3-bucket-luizg"
    s3_bucket_replace = True

    # Postgres connection info
    airflow_postgres_connection = "rds_postgres_connection"
    postgres_landing_schema = "landing"
    postgres_current_schema = "currentraw"
    postgres_history_schema = "historyraw"

    # Compound variables
    dag_id_compound = f"{dag_id}_v{dag_version}"
    s3_bucket_key = f"{origin_name}_{extraction_object}_{{{{ts_nodash}}}}.json"
    postgres_landing_table = f"{postgres_landing_schema}.{origin_name}_{extraction_object}_{{{{ts_nodash}}}}"
    postgres_current_table = f"{postgres_current_schema}.{origin_name}_{extraction_object}"
    postgres_history_table = f"{postgres_history_schema}.{origin_name}_{extraction_object}"
    
    with DAG(
        dag_id = dag_id_compound, 
        default_args = default_args, 
        schedule_interval = schedule_interval, 
        start_date = start_date,
        tags = tags,
        catchup = False
    ) as dag:

        # Task initialize
        initialize = DummyOperator(
           task_id = "initialize",
           dag = dag
        )

        # Task extract data from API to S3
        if origin_type == "api":
            extract_data_to_s3 = ApiToS3Operator(
                task_id = f"{origin_name}_to_s3",
                api_request = extraction_method,
                api_endpoint = extraction_object,
                airflow_s3_connection = airflow_s3_connection,
                s3_bucket_name = s3_bucket_name,
                s3_bucket_key = s3_bucket_key,
                s3_bucket_replace = s3_bucket_replace
            )

        # Task extract data from Crawler to S3
        elif origin_type == "webcrawler":
            extract_data_to_s3 = CrawlerToS3Operator(
                task_id = f"{origin_name}_to_s3",
                webcrawler_request = extraction_method,
                airflow_postgres_connection = airflow_postgres_connection,
                postgres_sql_query = extraction_parameters["query"],
                airflow_s3_connection = airflow_s3_connection,
                s3_bucket_name = s3_bucket_name,
                s3_bucket_key = s3_bucket_key,
                s3_bucket_replace = s3_bucket_replace
            )
            
        else: 
            raise ValueError('Extraction method not implemented.')
            
        # Task drop landing table if already exists
        drop_existing_landing_tbl = PostgresOperator(
            task_id = "drop_exists_landing_tbl",
            postgres_conn_id = airflow_postgres_connection,
            sql = f"DROP TABLE IF EXISTS {postgres_landing_table}; "
        )

        # Task create landing table
        create_landing_tbl = PostgresOperator(
            task_id="create_landing_tbl",
            postgres_conn_id = airflow_postgres_connection,
            sql = f"CREATE TABLE {postgres_landing_table} (rawdata jsonb);"
        )

        # Task transfer data from S3 to Postgres
        transfer_s3_to_postgres = S3ToPostgresOperator(
            task_id = "s3_to_postgres",
            airflow_s3_connection = airflow_s3_connection,
            s3_bucket_name = s3_bucket_name,
            s3_bucket_key = s3_bucket_key,
            airflow_postgres_connection = airflow_postgres_connection,
            postgres_table = postgres_landing_table,
            postgres_columns_list = postgres_columns_list
        )
        
        # Task create history table
        create_history_tbl = PostgresOperator(
            task_id = "create_history_tbl",
            postgres_conn_id = airflow_postgres_connection,
            sql = f"""
                CREATE TABLE IF NOT EXISTS {postgres_history_table} (
                    rawdata jsonb
                    , reference_datetime timestamp
                    , load_datetime timestamp default CURRENT_TIMESTAMP
                );"""
        )

        # Task insert from landing to history table
        load_history_tbl = PostgresOperator(
            task_id = "load_history_tbl",
            postgres_conn_id = airflow_postgres_connection,
            sql = f"""
                INSERT INTO {postgres_history_table} (rawdata, reference_datetime) 
                SELECT rawdata, '{{{{ts}}}}' FROM {postgres_landing_table};"""
        )

        # Task create current table
        create_current_tbl = PostgresOperator(
            task_id = "create_current_tbl",
            postgres_conn_id = airflow_postgres_connection,
            sql = f"CREATE TABLE IF NOT EXISTS {postgres_current_table} (rawdata jsonb);",
            trigger_rule = TriggerRule.NONE_FAILED
        )

        # Task insert from landing to current table if data not exists
        load_current_tbl_ins = PostgresOperator(
            task_id = "load_current_tbl_insert",
            postgres_conn_id = airflow_postgres_connection,
            sql = f"""
                INSERT INTO {postgres_current_table} (rawdata)
                SELECT rawdata FROM {postgres_landing_table} a
                WHERE NOT EXISTS (
                    SELECT 1 
                    FROM {postgres_current_table} b
                    WHERE 
                        1=1
                        {create_key_relation(extraction_keys)}
                );"""
        )

        # Task update from landing to current table if data exists
        load_current_tbl_upd = PostgresOperator(
            task_id = "load_current_tbl_update",
            postgres_conn_id = airflow_postgres_connection,
            sql = f"""
                UPDATE {postgres_current_table} a
                SET rawdata = b.rawdata
                FROM {postgres_landing_table} b
                WHERE 
                    1=1
                    AND a.rawdata <> b.rawdata
                    {create_key_relation(extraction_keys)}"""
        )
           
        # Task drop landing in the end of load data
        drop_landing_tbl = PostgresOperator(
            task_id = "drop_landing_tbl",
            postgres_conn_id = airflow_postgres_connection,
            sql = f"DROP TABLE IF EXISTS {postgres_landing_table};"
        )

        # Chose between saving or not history table
        history_saving_branch = BranchPythonOperator(
            task_id = "history_saving_path",
            python_callable = lambda x: create_history_tbl.task_id if x == True else create_current_tbl.task_id,
            op_kwargs = {"x": history_saving}
        )
        
        # Task path not saving history
        initialize >> extract_data_to_s3 >> drop_existing_landing_tbl >> create_landing_tbl >> transfer_s3_to_postgres
        transfer_s3_to_postgres >> history_saving_branch >> create_current_tbl >> load_current_tbl_upd 
        load_current_tbl_upd >> load_current_tbl_ins >> drop_landing_tbl
        
        # Task path saving history
        initialize >> extract_data_to_s3 >> drop_existing_landing_tbl >> create_landing_tbl >> transfer_s3_to_postgres
        transfer_s3_to_postgres >> history_saving_branch >> create_history_tbl >> load_history_tbl >> create_current_tbl 
        create_current_tbl >> load_current_tbl_upd >> load_current_tbl_ins >> drop_landing_tbl

    return dag