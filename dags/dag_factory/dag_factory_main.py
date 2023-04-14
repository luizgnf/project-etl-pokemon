
# DAG Configurations
from airflow import DAG
from airflow.sensors.external_task_sensor import ExternalTaskSensor
from airflow.utils.trigger_rule import TriggerRule

# DAG Operators
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.latest_only_operator import LatestOnlyOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.operators.python import BranchPythonOperator
from custom.operators.custom_api_to_s3 import ApiToS3Operator
from custom.operators.custom_crawler_to_s3 import CrawlerToS3Operator
from custom.operators.custom_s3_to_postgres import S3ToPostgresOperator

# DAG Utils
from dag_factory.dag_factory_utils import create_key_relation
from contextlib import contextmanager
from datetime import datetime


# Main structure of DAG factory
def create_pipeline_dag(
    origin_type: str,
    origin_name: str,
    dag_id: str,
    dag_version: int,
    start_date: datetime,
    schedule_interval,
    tags: list,
    default_args: dict,
    extraction_method: function,
    extraction_object: str,
    extraction_keys: list,
    extraction_parameters: dict = None,
    postgres_columns_list: list = None,
    history_saving: bool = True,
    only_latest: bool = False,
    sensors_list: bool = None
): 
    """
    Function responsible for creating and returning a ``DAG object`` with complete data pipeline. This work flow will extract data from a external source, transform and load into relational database. About the arguments:

    ``origin_type`` and ``origin_name`` are related to the external source (API, Crawler).

    ``dag_id`` and ``dag_version`` will compose the DAG identification.
    
    ``tags`` are very useful to organize and filter the DAGs. 
    
    ``start_date``, ``schedule_interval`` and ``default_args`` are default configurations for DAG runs.

    ``extraction_method`` is the custom function responsible for extract data from external source.  
    
    ``extraction_object`` is the endpoint, collection or table name from external source.
    
    ``extraction_keys`` are the keys from data result after extraction, required for database upsert.
    
    ``extraction_parameters`` (optional) accepts a dict of external parameters, such as queries, API filters.  
    
    ``postgres_columns_list`` (optional) accepts a list naming the columns to be copied into database.
    
    ``history_saving``: (optional) let you choose between save or not your final data into historical schema. 
    """

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
        initialize >> extract_data_to_s3 >> drop_existing_landing_tbl >> create_landing_tbl >> transfer_s3_to_postgres >> history_saving_branch >> create_current_tbl >> load_current_tbl_upd >> load_current_tbl_ins >> drop_landing_tbl
        
        # Task path saving history
        initialize >> extract_data_to_s3 >> drop_existing_landing_tbl >> create_landing_tbl >> transfer_s3_to_postgres >> history_saving_branch >> create_history_tbl >> load_history_tbl >> create_current_tbl >> load_current_tbl_upd >> load_current_tbl_ins >> drop_landing_tbl

    return dag