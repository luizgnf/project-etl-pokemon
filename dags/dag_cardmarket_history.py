from airflow import DAG
from dag_factory.dag_factory_main import create_pipeline_dag
from custom.functions.webcrawler_extraction import *
from datetime import datetime, timedelta

DEFAULT_ARGS = {
    "retries": 3,
    "retry_delay": timedelta(minutes = 10)
}

sql_query = """
    SELECT
        rawdata->>'id'::text AS card_id
        , rawdata->'cardmarket'->>'url'::text AS url
    FROM currentraw.poketcg_cards
"""

dag_once = create_pipeline_dag(
    origin_type = "webcrawler", 
    origin_name = "cardmarket",
    dag_id = "crawler_cardmarket_once",
    dag_version = "1",
    start_date = datetime(2023, 1, 31, 3, 15),
    schedule_interval = timedelta(days = 7),
    tags = ["pokemon", "cardmarket", "crawler", "once"],
    default_args = DEFAULT_ARGS,
    extraction_method = cardmarket_pricehistory,
    extraction_parameters = {"query": sql_query},
    extraction_object = "pricehistory",
    extraction_keys = ["card_id"],
    postgres_columns_list = ["rawdata"],
    history_saving = False
)