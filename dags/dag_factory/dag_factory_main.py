
# Default imports
from airflow import DAG
from contextlib import contextmanager
from airflow.operators.dummy_operator import DummyOperator
from airflow.sensors.external_task_sensor import ExternalTaskSensor
from airflow.operators.latest_only_operator import LatestOnlyOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator

# Custom operators
from custom.operators.custom_api_to_s3 import ApiToS3Operator
from custom.operators.custom_s3_to_postgres import S3ToPostgresOperator

# Utils
from dag_factory.dag_factory_utils import *


# Main structure
def create_extraction_dags(
    origin_type,
    dag_id,
    dag_version,
    start_date,
    schedule_interval,
    tags,
    default_args,
    api_request_type,
    api_endpoint,
    extract_keys,
    postgres_columns_list = None,
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

    # Compound variables
    dag_id_compound = f"{dag_id}_v{dag_version}"

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
        extract_data_to_s3 = ApiToS3Operator(
            task_id = f"{origin_type}_to_s3",
            api_request_type = api_request_type,
            api_endpoint = api_endpoint,
            airflow_s3_connection = airflow_s3_connection,
            s3_bucket_name = s3_bucket_name,
            s3_bucket_key = f"{origin_type}_{api_endpoint}_{{{{ts_nodash}}}}.json",
            s3_bucket_replace = s3_bucket_replace
        )

        # Task drop landing table if already exists
        drop_existing_landing_tbl = PostgresOperator(
            task_id = "drop_exists_landing_tbl",
            postgres_conn_id = airflow_postgres_connection,
            sql = f"DROP TABLE IF EXISTS {postgres_landing_schema}.{origin_type}_{api_endpoint}_{{{{ts_nodash}}}}; "
        )

        # Task create landing table
        create_landing_tbl = PostgresOperator(
            task_id="create_landing_tbl",
            postgres_conn_id = airflow_postgres_connection,
            sql = f"CREATE TABLE {postgres_landing_schema}.{origin_type}_{api_endpoint}_{{{{ts_nodash}}}} (rawdata jsonb);"
        )

        # Task transfer data from S3 to Postgres
        transfer_s3_to_postgres = S3ToPostgresOperator(
            task_id = "s3_to_postgres",
            airflow_s3_connection = airflow_s3_connection,
            s3_bucket_name = s3_bucket_name,
            s3_bucket_key = f"{origin_type}_{api_endpoint}_{{{{ts_nodash}}}}.json",
            airflow_postgres_connection = airflow_postgres_connection,
            postgres_schema = postgres_landing_schema,  
            postgres_table = f"{origin_type}_{api_endpoint}_{{{{ts_nodash}}}}",
            postgres_columns_list = postgres_columns_list
        )

        # Task create current table
        create_current_tbl = PostgresOperator(
            task_id = "create_current_tbl",
            postgres_conn_id = airflow_postgres_connection,
            sql = f"CREATE TABLE IF NOT EXISTS {postgres_current_schema}.{origin_type}_{api_endpoint} (rawdata jsonb);"
        )

        # Task insert from landing to current table if data not exists
        load_current_tbl_ins = PostgresOperator(
            task_id = "load_current_tbl_insert",
            postgres_conn_id = airflow_postgres_connection,
            sql = f"""
                INSERT INTO {postgres_current_schema}.{origin_type}_{api_endpoint} (rawdata)
                SELECT rawdata FROM {postgres_landing_schema}.{origin_type}_{api_endpoint}_{{{{ts_nodash}}}} a
                WHERE NOT EXISTS (
                    SELECT 1 
                    FROM {postgres_current_schema}.{origin_type}_{api_endpoint} b
                    WHERE 
                        1=1
                        {create_key_relation(extract_keys)}
                );
            """
        )

        # Task update from landing to current table if data exists
        load_current_tbl_upd = PostgresOperator(
            task_id = "load_current_tbl_update",
            postgres_conn_id = airflow_postgres_connection,
            sql = f"""
                UPDATE {postgres_current_schema}.{origin_type}_{api_endpoint} a
                SET rawdata = b.rawdata
                FROM {postgres_landing_schema}.{origin_type}_{api_endpoint}_{{{{ts_nodash}}}} b
                WHERE 
                    1=1
                    AND a.rawdata <> b.rawdata
                    {create_key_relation(extract_keys)}
            """
        )
           
        # Task drop landing in the end of load data
        drop_landing_tbl = PostgresOperator(
            task_id = "drop_landing_tbl",
            postgres_conn_id = airflow_postgres_connection,
            sql = f"DROP TABLE IF EXISTS {postgres_landing_schema}.{origin_type}_{api_endpoint}_{{{{ts_nodash}}}}; "
        )

        initialize >> extract_data_to_s3 >> drop_existing_landing_tbl >> create_landing_tbl >> transfer_s3_to_postgres
        transfer_s3_to_postgres >> create_current_tbl >> load_current_tbl_upd >> load_current_tbl_ins >> drop_landing_tbl
    
    return dag