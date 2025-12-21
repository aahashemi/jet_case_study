from abc import ABC, abstractmethod
from .models import Base
from sqlalchemy.orm import sessionmaker
import sqlalchemy as sa
from loguru import logger
import sys

logger.remove()  
logger.add(sys.stdout, level="INFO") 

class DatabaseManager:
    def __init__(self, engine):
        self.engine = engine
        self.session = sessionmaker(bind=self.engine)

    def initialize_warehouse(self):
        Base.metadata.create_all(self.engine)
    
    def record_exists(self, model_class, pk_value):
        with self.session() as session:
            _exists = session.get(model_class, pk_value)
            return _exists
        
    def insert_record(self, model_class, data_map: dict):
        with self.session() as session:
            new_record = model_class(**data_map)
            session.add(new_record)
            session.commit()
            logger.info(f"Inserted new record into {model_class.__tablename__}: {data_map}")
        return 
    
    def execute_query(self, query_text: str, params: dict = None):

        with self.session() as session:
            return session.execute(sa.text(query_text), params or {})
     
            