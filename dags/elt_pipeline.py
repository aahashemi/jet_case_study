from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.sqlite.hooks.sqlite import SqliteHook 
from datetime import datetime, timedelta
from utils.database import DatabaseManager
from utils.api_client import XkcdClient
from utils.models import ComicRaw
from loguru import logger


def sync_xkcd_comics(db_manager):

    client = XkcdClient()
    sql = f"SELECT MAX(num) FROM {ComicRaw.__tablename__}"

    latest_db_comic = db_manager.execute_query(sql).scalar() or 0

    latest_api_comic = client.fetch_latest().json()['num']

    if latest_api_comic> latest_db_comic:
        logger.info(f"Syncing from {latest_db_comic + 1} to {latest_api_comic}...")

        for comic_id in range(latest_db_comic + 1, latest_api_comic + 1):

            response = client.fetch_by_id(comic_id)

            data_map= {
                "num": comic_id, 
                "raw_json": {}, 
                "ingestion_status": response.status_code
            }

            if response.status_code == 200:
                data_map['raw_json'] = response.json()
            
            db_manager.insert_record(ComicRaw, data_map=data_map)
          
    logger.info(f"Comics are synced")
     

def main():
    
    hook = SqliteHook(sqlite_conn_id='xkcd')
    engine = hook.get_sqlalchemy_engine()
    db_manager = DatabaseManager(engine)
    db_manager.initialize_warehouse()
    sync_xkcd_comics(db_manager)
    
    
default_args = {
    'owner': 'amir_hashemi',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
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

    ingest_task = PythonOperator(
        task_id='extract_load_xkcd_comics',
        python_callable=main
    )