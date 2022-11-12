import logging
from datetime import timedelta

from airflow.utils.dates import days_ago
from airflow.decorators import (dag, task)

from plugins.mudule_Nz.extract import std_extract

import pandas as pd
from pathlib import Path

moron_sql = r'/usr/local/airflow/include/GFUMoron.sql'
moron_csv = r'/usr/local/airflow/include/GFUMoron.csv'


import logging
import logging.config
def configure_logger():
            
    LOGGING_CONFIG = Path(__file__).parent.parent/"logger.cfg"
    logging.config.fileConfig(LOGGING_CONFIG, disable_existing_loggers=False)
    logger = logging.getLogger("GFUMoron_dag_etl")
    return logger




@dag(
    schedule=timedelta(minutes=60),
    start_date=days_ago(1),
    catchup=False,
    default_args={
        "retries": 5,
        'retry_delay': timedelta(minutes=1)
    },
    tags=['DataProcessing'])
def GFUMoron_dag_etl():
    
    @task()
    def extract():
        
        logger = configure_logger()
        logger.info('extract-init')
        
        
        return std_extract(moron_sql,moron_csv)
    @task()
    def transform(extract):
        logger = configure_logger()
        if extract:
            df = pd.read_csv(moron_csv,encoding="utf-8")
            print(df.head())
 
    @task()
    def load():
        logger = configure_logger()
        logging.info('PandaslOAD')
        logging.info('PandaslOAD')
        logging.info('PandaslOAD')
        logging.info('PandaslOAD')
        
    transform(extract())
    load()

GFUMoron_dag_etl = GFUMoron_dag_etl()










"""
version: "3.1"
services:
  scheduler:
    volumes:
      - ./files:/usr/local/airflow/files:rw
      - ./dags/logs:/usr/local/airflow/mylogs:rw
"""