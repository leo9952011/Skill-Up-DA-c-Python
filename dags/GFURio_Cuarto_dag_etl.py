import logging
from datetime import timedelta

from airflow.utils.dates import days_ago
from airflow.decorators import (dag, task)

from plugins.mudule_Nz.extract import std_extract
from plugins.mudule_Nz.transform import std_transform

moron_sql = r'/usr/local/airflow/include/GFUMoron.sql'
moron_csv = r'/usr/local/airflow/include/GFUMoron.csv'


@dag(
    schedule=timedelta(minutes=60),
    start_date=days_ago(1),
    catchup=False,
    default_args={
        "retries": 5,
        'retry_delay': timedelta(minutes=1)
    },
    tags=['DataProcessing'])
def GFURio_Cuarto_dag_etl():
    
    @task()
    def extract():
        rio_cuarto_sql = r'/usr/local/airflow/include/GFUNRioCuarto.sql'
        rio_cuarto_csv = r'/usr/local/airflow/files/GFUNRioCuarto.csv'
        
        return std_extract(rio_cuarto_sql,rio_cuarto_csv)
    @task()
    def transform(extract):
        if extract:
            print('aca esta el dataframe')
            logging.info('Pandas process')
            logging.info('Pandas process')
            logging.info('Pandas process')
            logging.info('Pandas process')
            logging.info('Pandas process')
            logging.info('Pandas process')
        else:
            print('no hay dataframe por aqui')
 
    @task()
    def load():
        logging.info('PandaslOAD')
        logging.info('PandaslOAD')
        logging.info('PandaslOAD')
        logging.info('PandaslOAD')
        
    transform(extract())
    load()

GFURio_Cuarto_dag_etl = GFURio_Cuarto_dag_etl()
