from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.postgres.hooks.postgres import PostgresHook


class S3ToPostgresOperator(BaseOperator):

    template_fields = ['s3_bucket_key', 'postgres_table']

    @apply_defaults
    def __init__(
            self,
            airflow_s3_connection,
            s3_bucket_name,
            s3_bucket_key,
            airflow_postgres_connection,
            postgres_table,
            postgres_columns_list,
            *args, **kwargs
        ):

        super().__init__(*args, **kwargs)
        self.airflow_s3_connection = airflow_s3_connection  
        self.s3_bucket_name = s3_bucket_name
        self.s3_bucket_key = s3_bucket_key
        self.airflow_postgres_connection = airflow_postgres_connection
        self.postgres_table = postgres_table
        self.postgres_columns_list = postgres_columns_list

    def execute(self, context):
        
        # Download file from S3
        s3_hook = S3Hook(self.airflow_s3_connection)
        tmp_filepath = s3_hook.download_file(key=self.s3_bucket_key, bucket_name=self.s3_bucket_name)
        self.log.info(f'File {self.s3_bucket_key} downloaded from S3.')

        with open(tmp_filepath, 'r') as tmp_file:

            # Copy data to postgres
            postgres_hook = PostgresHook(self.airflow_postgres_connection)
            self.log.info(f'Connection to Postgres established.')

            postgres_conn = postgres_hook.get_conn()
            postgres_cur = postgres_conn.cursor()

            postgres_columns = ''
            if self.postgres_columns_list is not None:
                postgres_columns = f'({", ".join(self.postgres_columns_list)})'                

            postgres_cur.copy_expert(f"""
                COPY {self.postgres_table} {postgres_columns} FROM STDIN
            """, tmp_file)            
            postgres_conn.commit()
            self.log.info(f'Data copied to {self.postgres_table} Postgres table.')
        