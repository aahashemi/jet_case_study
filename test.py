# from airflow import DAG
# from airflow.operators.python import PythonOperator
# from airflow.providers.sqlite.hooks.sqlite import SqliteHook
from utils.database import DatabaseManager
from utils.api_client import XkcdClient
from airflow.utils.models import Base, ComicRaw
import sqlalchemy as sa
from sqlalchemy.orm import Session
from loguru import logger

def ingest_comic(db_manager, comic_id):
    
    # hook = SqliteHook(sqlite_conn_id='xkcd')
    # engine = hook.get_sqlalchemy_engine()

    pass

def sync_comics(db_manager, client):
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
    return 

def main():
    engine = sa.create_engine("sqlite:///data/xkcd.db")

    db_manager = DatabaseManager(engine)
    db_manager.initialize_warehouse()

    client = XkcdClient()
    sync_comics(db_manager, client)


    
if __name__ == "__main__":
    main()