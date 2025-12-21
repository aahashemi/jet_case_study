import sqlalchemy as sa
from sqlalchemy.orm import DeclarativeBase

class Base(DeclarativeBase):
    pass

class ComicRaw(Base):
    __tablename__ = 'raw_comic'
    
    num = sa.Column(sa.Integer, primary_key=True, autoincrement=False)
    
    raw_json = sa.Column(sa.JSON, nullable=True)    

    ingestion_status = sa.Column(sa.Integer)


