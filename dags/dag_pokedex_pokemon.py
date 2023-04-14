from airflow import DAG
from dag_factory.dag_factory_main import create_pipeline_dag
from custom.functions.api_pokedex_integration import *
from datetime import datetime, timedelta

DEFAULT_ARGS = {
    "retries": 3,
    "retry_delay": timedelta(minutes = 10)
}

dag_full = create_pipeline_dag(
    origin_type = "api", 
    origin_name = "pokedex",
    dag_id = "pokedex_pokemon_full",
    dag_version = "1",
    start_date = datetime(2023, 1, 31, 3, 15),
    schedule_interval = timedelta(days = 1),
    tags = ["pokemon", "pokedex", "s3", "postgres", "daily"],
    default_args = DEFAULT_ARGS,
    extraction_method = full_request,
    extraction_object = 'pokemon',
    extraction_keys = ["id"],
    postgres_columns_list = ["rawdata"],
    history_saving = False
)

dag_hourly = create_pipeline_dag(
    origin_type = "api", 
    origin_name = "pokedex",
    dag_id = "pokedex_pokemon_hourly",
    dag_version = "1",
    start_date = datetime(2023, 1, 31, 3, 15),
    schedule_interval = timedelta(hours = 1),
    tags = ["pokemon", "pokedex", "s3", "postgres", "hourly"],
    default_args = DEFAULT_ARGS,
    extraction_method = full_request, # prepare intraday function to place here
    extraction_object = 'pokemon',
    extraction_keys = ["id"],
    postgres_columns_list = ["rawdata"]
)