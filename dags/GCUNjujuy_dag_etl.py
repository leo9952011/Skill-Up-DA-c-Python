import pandas as pd
from datetime import datetime
from plugins.transform_data import transform_jujuy
from pathlib import Path
import logging

from airflow.decorators import dag, task 
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.amazon.aws.hooks.s3 import S3Hook

CUR_DIR = Path(__file__).resolve().parent
PAR_DIR = CUR_DIR.parent

sqlpath= PAR_DIR/'include/GCUNjujuy.sql'
csvpath= PAR_DIR/'files'

def configure_logger():
    LOG_CONF = PAR_DIR / 'logger.cfg'
    logging.config.fileConfig(LOG_CONF, disable_existing_loggers=False)
    logger = logging.getLogger('GCUJujuy_dag_etl')
    return logger

def transform_fun():
    logger = configure_logger()
    logger.info('Comienza tarea de transformación en el DAG jujuy_dag')

    input_path = PAR_DIR/'files'
    output_path = PAR_DIR/'datasets'
    transform_jujuy(input_path, output_path)

    logger.info('Finaliza tarea de transformación en el DAG jujuy_dag')

@dag(
    'jujuy_dag',
    description = 'Dag para ETL de la Universidad Nacional de Jujuy',
    schedule_interval = "@hourly",
    start_date = datetime(2022, 11, 4)
)
def jujuy_dag():
    @task(task_id='extract_jujuy', retries = 5)
    def get_data_jujuy(**kwargs):

        logger = configure_logger()
        logger.info('Comienza tarea de extracción en el DAG jujuy_dag')
        with open(sqlpath, 'r') as sqlfile:
            select_query = sqlfile.read()
        hook = PostgresHook(postgres_conn_id='alkemy_db')
        df = hook.get_pandas_df(sql=select_query)
        df.to_csv(csvpath/'GCUNjujuy_select.csv')

        logger.info('Finaliza tarea de extracción en el DAG jujuy_dag')
    
    transform = PythonOperator(task_id='transform_fun', retries = 5, python_callable=transform_fun)

    @task(task_id='load_jujuy', retries=5)
    def load_data_jujuy(**kwargs):

        logger = configure_logger()
        logger.info('Comienza tarea de subida en el DAG jujuy_dag')

        s3_hook = S3Hook(aws_conn_id="aws_s3_bucket")
        s3_hook.load_file(
            PAR_DIR/'datasets/GCUNjujuy_process.txt',
            bucket_name='alkemy-gc',
            replace=True,
            key="process/GCUNjujuy_process.txt"
            )
        logger.info('Finaliza tarea de subida en el DAG jujuy_dag')


    get_data_jujuy() >> transform >> load_data_jujuy()

dag = jujuy_dag()