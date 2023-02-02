import json
from tempfile import NamedTemporaryFile

from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.providers.amazon.aws.hooks.s3 import S3Hook


class ApiToS3Operator(BaseOperator):

    template_fields = ['s3_bucket_key']

    @apply_defaults
    def __init__(
            self,
            api_request_type,
            api_endpoint,
            airflow_s3_connection,
            s3_bucket_name,
            s3_bucket_replace,
            s3_bucket_key,
            *args, **kwargs
        ):
        
        super().__init__(*args, **kwargs)
        self.api_request_type = api_request_type
        self.api_endpoint = api_endpoint
        self.airflow_s3_connection = airflow_s3_connection
        self.s3_bucket_name = s3_bucket_name
        self.s3_bucket_replace = s3_bucket_replace
        self.s3_bucket_key = s3_bucket_key

    def execute(self, context):

        with NamedTemporaryFile('w', suffix = '.json') as tmp_file:
            
            # Extract API data
            result = self.api_request_type(self.api_endpoint)   
            self.log.info(f'Full extraction finished. {len(result)} total items.')

            # Clean and save as ndjson file
            for elem in result:
                tmp_file.write(json.dumps(elem, ensure_ascii = False).replace("\\\"", "'").replace("\\n", " "))
                tmp_file.write("\n")
            tmp_file.seek(0)
            self.log.info(f'Data treated. Temp file prepared for upload.')
            
            # Upload file to S3
            s3_hook = S3Hook(self.airflow_s3_connection)
            self.log.info(f'Connection to S3 established.')
            s3_hook.load_file(
                filename = tmp_file.name,
                key = self.s3_bucket_key,
                bucket_name = self.s3_bucket_name, 
                replace = self.s3_bucket_replace
            )
            self.log.info(f'File {self.s3_bucket_key} uploaded to S3.')