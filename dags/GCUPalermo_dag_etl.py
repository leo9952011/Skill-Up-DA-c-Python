import pandas as pd
from datetime import datetime
from plugins.transform_data import transform_palermo
from pathlib import Path
import logging

from airflow.decorators import dag, task 
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.amazon.aws.hooks.s3 import S3Hook

CUR_DIR = Path(__file__).resolve().parent
PAR_DIR = CUR_DIR.parent

sqlpath= PAR_DIR/'include/GCUpalermo.sql'
csvpath= PAR_DIR/'files'

def configure_logger():
    LOG_CONF = PAR_DIR / 'logger.cfg'
    logging.config.fileConfig(LOG_CONF, disable_existing_loggers=False)
    logger = logging.getLogger('GCUPalermo_dag_etl')
    return logger

def transform_fun():
    logger = configure_logger()
    logger.info('Comienza tarea de transformación en el DAG palermo_dag')

    input_path = PAR_DIR/'files'
    output_path = PAR_DIR/'datasets'
    transform_palermo(input_path, output_path)

    logger.info('Finaliza tarea de transformación en el DAG palermo_dag')

@dag(
    'palermo_dag',
    description = 'Dag para ETL de la Universidad de Palermo',
    schedule_interval = "@hourly",
    start_date = datetime(2022, 11, 4)
)
def palermo_dag():
    @task(task_id='extract_palermo', retries = 5)

    def get_data_palermo(**kwargs):
        logger = configure_logger()
        logger.info('Comienza tarea de extracción en el DAG palermo_dag')
        with open(sqlpath, 'r') as sqlfile:
            select_query = sqlfile.read()
        hook = PostgresHook(postgres_conn_id='alkemy_db')
        df = hook.get_pandas_df(sql=select_query)
        df.to_csv(csvpath/'GCUPalermo_select.csv')

        logger.info('Finaliza tarea de extracción en el DAG palermo_dag')
    
    transform = PythonOperator(task_id='transform_fun', retries = 5, python_callable=transform_fun)

    @task(task_id='load_palermo', retries=5)
    def load_data_palermo(**kwargs):
        logger = configure_logger()
        logger.info('Comienza tarea de subida en el DAG palermo_dag')

        s3_hook = S3Hook(aws_conn_id="aws_s3_bucket")
        s3_hook.load_file(
            PAR_DIR/'datasets/GCUpalermo_process.txt',
            bucket_name='alkemy-gc',
            replace=True,
            key="process/GCUpalermo_process.txt"
            )
        logger.info('Finaliza tarea de subida en el DAG palermo_dag')

    get_data_palermo() >> transform >> load_data_palermo()

dag = palermo_dag()