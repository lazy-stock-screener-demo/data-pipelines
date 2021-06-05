from sqlalchemy import create_engine
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
import pipelines.construct_stock_catalog.config.env as env

# postgresql+psycopg2://airflow:airflow@airflow-postgres:5432/airflow
# postgresql://[user[:password]@][netloc][:port][/dbname][?param1=value1&...]


def connectedPg():
    db = create_engine(env.psql_db_url, pool_size=5, max_overflow=0)
    Session = sessionmaker(db)
    session = Session()
    return session


def base():
    base = declarative_base()
    return base
