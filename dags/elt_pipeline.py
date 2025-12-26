import os
from datetime import datetime, timedelta

import pandas as pd
from loguru import logger

from airflow.sdk import dag, task, Variable
from airflow.providers.standard.operators.bash import BashOperator

from utils.api_client import XkcdClient

DBT_PROJECT_DIR = "/opt/airflow/dbt"
LANDING_ZONE_PATH = "/opt/airflow/data/landing/"
LATEST_PROCESSED_VAR = "xkcd_last_processed_num"


@task
def extract_and_load_xkcd_comics(**context) -> bool:
    logical_date = context['logical_date']

    latest_processed_comic = int(Variable.get(LATEST_PROCESSED_VAR, default="0"))
    client = XkcdClient()

    latest_api_comic = client.fetch_latest().json()['num']

    if latest_api_comic <= latest_processed_comic:
        logger.info(
            f"No new comics to sync. Last processed: {latest_processed_comic}, Latest: {latest_api_comic}"
        )
        return False
    
    logger.info(
        f"Processing comics from #{latest_processed_comic + 1} to #{latest_api_comic}"
    )
    
    new_records = []
    for comic_num in range(latest_processed_comic + 1, latest_api_comic + 1):
        logger.info(f"Fetching comic #{comic_num}")
        response = client.fetch_by_id(comic_num)

        data_map = {
            "raw_json": response.json() if response.status_code == 200 else {}, 
            "http_status": response.status_code,
            "ingestion_time": logical_date
        }
        new_records.append(data_map)
    
    df = pd.DataFrame(new_records)
    
    timestamp = logical_date.strftime('%Y%m%d_%H%M%S')
    file_path = os.path.join(LANDING_ZONE_PATH, f"xkcd_{timestamp}.parquet")
    df.to_parquet(file_path, index=False)

    Variable.set(LATEST_PROCESSED_VAR, str(latest_api_comic))
    logger.info(f"Saved {len(new_records)} comics to {file_path}")

    return True


@task.short_circuit
def check_has_data(has_new_data: bool) -> bool:
    if not has_new_data:
        logger.info("No new data loaded. Skipping transformation.")
    return has_new_data


@dag(
    dag_id='elt_dag',
    default_args={
        'owner': 'amir_hashemi',
        'depends_on_past': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=1),
    },
    schedule='*/5 * * * 1,3,5',  
    start_date=datetime(2025, 1, 1),
    max_active_runs=1,
    catchup=False,
    is_paused_upon_creation=False,
    tags=['elt', 'xkcd', 'dbt'],
)
def xkcd_elt_pipeline():
   
    data_loaded = extract_and_load_xkcd_comics()
    
    has_data = check_has_data(data_loaded)
    
    dbt_run = BashOperator(
        task_id="dbt_run",
        bash_command=f"cd {DBT_PROJECT_DIR} && dbt run",
    )
    
    dbt_test = BashOperator(
        task_id="dbt_test",
        bash_command=f"cd {DBT_PROJECT_DIR} && dbt test",
    )
    
    data_loaded >> has_data >> dbt_run >> dbt_test


xkcd_elt_pipeline()
