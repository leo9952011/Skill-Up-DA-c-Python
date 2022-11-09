import logging
from datetime import timedelta

from airflow.utils.dates import days_ago
from airflow.decorators import (dag, task)

from plugins.mudule_Nz.extract import std_extract

import pandas as pd


moron_sql = r'/usr/local/airflow/include/GFUMoron.sql'
moron_csv = r'/usr/local/airflow/files/GFUMoron.csv'

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
        
        
        return std_extract(moron_sql,moron_csv)
    @task()
    def transform(extract):
        if extract:
            df = pd.read_csv(moron_csv,encoding="utf-8")
            print(df.head())
 
    @task()
    def load():
        logging.info('PandaslOAD')
        logging.info('PandaslOAD')
        logging.info('PandaslOAD')
        logging.info('PandaslOAD')
        
    transform(extract())
    load()

GFUMoron_dag_etl = GFUMoron_dag_etl()
