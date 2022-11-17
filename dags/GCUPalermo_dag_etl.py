import pandas as pd
from datetime import datetime, timedelta
from plugins.transform_data import transform_Palermo
from pathlib import Path

import logging
import logging.config

from airflow.decorators import dag, task 
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.amazon.aws.hooks.s3 import S3Hook


BASE_DIR = Path(__file__).parent.parent

# se normaliza el nombre de la universidad
university = "Palermo".strip().replace(" ", "")

# Para la convencion del nombre
name = f"GCU{university}"

sql_file_name = f"{name}.sql"
csv_file_name = f"{name}_select.csv"
txt_file_name = f"{name}_process.txt"
logger_name = f"{name}_dag_etl"


def configure_logger():
    LOG_CONF = BASE_DIR / 'logger.cfg'
    logging.config.fileConfig(LOG_CONF, disable_existing_loggers=False)
    logger = logging.getLogger(logger_name)
    return logger

def transform_fun():
    logger = configure_logger()
    logger.info('Comienza tarea de transformacion en el DAG')

    input_path = BASE_DIR / f"files/{csv_file_name}"
    output_path = BASE_DIR / f"datasets/{txt_file_name}"
    transform_Palermo(input_path, output_path)

    logger.info('Finaliza tarea de transformacion en el DAG')

@dag(
    "GCUPalermo",
    default_args={
        "retries": 5,
        "retry_delay": timedelta(minutes=5),
    },
    description="Realiza un ETL de los datos de la Universidad de Palermo.",
    schedule=timedelta(hours=1),
    start_date=datetime(2022, 11, 11),
    catchup=False
)
def gcdag():
    @task(task_id='extract', retries = 5)

    def get_data(**kwargs):
        logger = configure_logger()
        logger.info('Comienza tarea de extraccion en el DAG')
        sqlpath = BASE_DIR / f"include/{sql_file_name}"
        with open(sqlpath, 'r') as sqlfile:
            select_query = sqlfile.read()
        hook = PostgresHook(postgres_conn_id="alkemy_db")
        df = hook.get_pandas_df(sql=select_query)
        csvpath = BASE_DIR / f"files/{csv_file_name}"
        df.to_csv(csvpath)

        logger.info('Finaliza tarea de extraccion en el DAG')
    
    transform = PythonOperator(task_id='transform_fun', retries = 5, python_callable=transform_fun)

    @task(task_id='load', retries=5)
    def load_data(**kwargs):
        logger = configure_logger()
        logger.info('Comienza tarea de subida en el DAG')

        s3_hook = S3Hook(aws_conn_id="aws_s3_bucket")
        s3_hook.load_file(
            BASE_DIR / f"datasets/{txt_file_name}",
            bucket_name='alkemy-gc',
            replace=True,
            key=f"process/{txt_file_name}"
            )
        logger.info('Finaliza tarea de subida en el DAG')

    get_data() >> transform >> load_data()

dag = gcdag()