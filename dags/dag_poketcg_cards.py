from airflow import DAG
from dag_factory.dag_factory_main import create_extraction_dags
from custom.functions.api_poketcg_integration import *
from datetime import datetime, timedelta

DEFAULT_ARGS = {
    "retries": 3,
    "retry_delay": timedelta(minutes = 10)
}

dag_full = create_extraction_dags(
    origin_type = "poketcg", 
    dag_id = "poketcg_cards_full",
    dag_version = "1",
    start_date = datetime(2023, 1, 31, 3, 15),
    schedule_interval = timedelta(days = 1),
    tags = ["pokemon", "tcg", "cards", "s3", "postgres", "daily"],
    default_args = DEFAULT_ARGS,
    api_request_type = full_request,
    api_endpoint = 'cards',
    extract_keys = ["id"],
    postgres_columns_list = ["rawdata"],
    history_saving = False
)

dag_hourly = create_extraction_dags(
    origin_type = "poketcg", 
    dag_id = "poketcg_cards_hourly",
    dag_version = "1",
    start_date = datetime(2023, 1, 31, 3, 15),
    schedule_interval = timedelta(hours = 1),
    tags = ["pokemon", "tcg", "cards", "s3", "postgres", "hourly"],
    default_args = DEFAULT_ARGS,
    api_request_type = full_request, # prepare intraday function to place here
    api_endpoint = 'cards',
    extract_keys = ["id"],
    postgres_columns_list = ["rawdata"]
)