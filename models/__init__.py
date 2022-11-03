import psycopg2
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT
from psycopg2.extras import RealDictCursor

from decouple import config as settings
import time

from sqlalchemy import create_engine
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker


from .models_log import create_logger
log = create_logger()




######################### coneccion a base de datos #########################

while True:
    try:
        conn = psycopg2.connect(host = settings('DATABASE_HOSTNAME'), database = settings('DATABASE_NAME'),
                                user = settings('DATABASE_USERNAME'), password = settings('DATABASE_PASSWORD'),
                                cursor_factory= RealDictCursor)
        conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
        cursor = conn.cursor()
        log.info("environment variables correctly set Connection ready POSTGRESQL")
        print('***********************Connection ready***********************')
        break
    except Exception as error:
        log.critical(f"INCORRECTLY set environment variables DB connection error - {error}")
        print('***********************DB connection error***********************')
        print (error)
        time.sleep(2)


######################### engine #########################

SQLALCHEMY_DATABASE_URL = f"postgresql://{settings('DATABASE_USERNAME')}:{settings('DATABASE_PASSWORD')}@{settings('DATABASE_HOSTNAME')}/{settings('DATABASE_NAME')}"

engine = create_engine(SQLALCHEMY_DATABASE_URL)

SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)

Base = declarative_base()

def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()
