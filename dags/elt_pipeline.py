import os
from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator, ShortCircuitOperator
from airflow.providers.standard.operators.bash import BashOperator
from airflow.sdk import Variable 
from datetime import datetime, timedelta, timezone
import pandas as pd
from utils.api_client import XkcdClient
from loguru import logger

DBT_PROJECT_DIR = "/opt/airflow/dbt_jet"
LANDING_ZONE_PATH = "/opt/airflow/data/landing/"
LATEST_PROCESSED_VAR = "xkcd_last_processed_num"


def set_varible(value):
    Variable.set(LATEST_PROCESSED_VAR, str(value))

def extract_and_load_xkcd_comics():

    latest_processed_comic = int(Variable.get(LATEST_PROCESSED_VAR, default="0"))
    client = XkcdClient()

    latest_api_comic = client.fetch_latest().json()['num']

    if latest_api_comic <= latest_processed_comic:
        logger.info("No new comics to sync")
        return False
    
    
    new_records = []
    for comic_num in range(latest_processed_comic+1, latest_api_comic+1):
        logger.info(f"Processing comic #{comic_num}")
        response = client.fetch_by_id(comic_num)

        data_map = {
            "comic_num": comic_num, 
            "raw_json": response.json() if response.status_code == 200 else {}, 
            "http_status": response.status_code,
            "ingestion_time": datetime.now(timezone.utc)
        }
        new_records.append(data_map)
    
    df = pd.DataFrame(new_records)
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    file_path = os.path.join(LANDING_ZONE_PATH, f"xkcd_{timestamp}.parquet")
    df.to_parquet(file_path, index=False)

    set_varible(latest_api_comic)
    logger.info(f"Saved update to {file_path}")

    return True
    

def has_data(ti):
    return ti.xcom_pull(
        task_ids="extract_and_load_xkcd_comics"
    )
    
default_args = {
    'owner': 'amir_hashemi',
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    'elt_dag',
    default_args=default_args,
    schedule='*/5 * * * 1,3,5', 
    start_date=datetime(2025, 1, 1),
    max_active_runs=1, 
    catchup=False,
    is_paused_upon_creation=False
) as dag:

    extract_and_load_task = PythonOperator(
        task_id='extract_and_load_xkcd_comics',
        python_callable=extract_and_load_xkcd_comics
    )

    check_data_loaded = ShortCircuitOperator(
        task_id="check_has_data",
        python_callable=has_data,
    )

    transform_task = BashOperator( 
        task_id="transform_xkcd_comics",
        bash_command=f"cd {DBT_PROJECT_DIR} && dbt deps && dbt run && dbt test" 
    )
    
    extract_and_load_task >> check_data_loaded >> transform_task