"""ETL DAG for 'Facultad Latinoamericana de Ciencias Sociales' (Grupo G)."""

from datetime import datetime
from pathlib import Path
import logging

from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.amazon.aws.hooks.s3 import S3Hook

from plugins.GGtransform import transform_dataset


def configure_logger():
    """Configure logging from cfg file. Return custom logger."""
    LOGGING_CONFIG = Path(__file__).parent.parent / 'logger.cfg'
    logging.config.fileConfig(LOGGING_CONFIG, disable_existing_loggers=False)
    logger = logging.getLogger(f'{dag_id}_dag_etl')
    return logger


def extract_task():
    """Get data from remote postgres DB and save to csv file locally."""

    logger = configure_logger()
    logger.info(f'Started Extract Task for DAG {dag_id}.')

    local_basepath = Path(__file__).resolve().parent.parent

    # Read SQL query
    sql_filepath = local_basepath / 'include/GGFLCienciasSociales.sql'
    with open(sql_filepath, encoding='utf8') as sql_file:
        sql_str = sql_file.read()

    # connect to remote postgres DB via hook
    pg_hook = PostgresHook(postgres_conn_id="alkemy_db")
    uni_df = pg_hook.get_pandas_df(sql=sql_str)

    # save df data to csv locally
    csv_filepath = local_basepath / 'files/GGFLCienciasSociales_select.csv'
    uni_df.to_csv(csv_filepath, sep=',', header=True, encoding='utf-8')

    logger.info('Finished Extract Task for DAG {dag_id}.')


def transform_task():
    """Load data from local csv, normalize with pandas and save to txt file locally."""
    
    logger = configure_logger()
    logger.info(f'Started Transform Task for DAG {dag_id}.')

    local_basepath = Path(__file__).resolve().parent.parent

    csv_path = local_basepath / 'files/GGFLCienciasSociales_select.csv'
    txt_path = local_basepath / 'datasets/GGFLCienciasSociales_process.txt'
    transform_dataset(input_path=csv_path, output_path=txt_path, date_format='')

    logger.info('Finished Transform Task for DAG {dag_id}.')

def load_task():
    """Take txt file and upload it to s3 bucket."""

    logger = configure_logger()
    logger.info(f'Started Load Task for DAG {dag_id}.')
    
    s3_hook = S3Hook(aws_conn_id='aws_s3_bucket')
    bucket_name = 'alkemy-gg'
    local_basepath = Path(__file__).resolve().parent.parent

    # Upload to S3 using predefined method
    txt_path = local_basepath / f'datasets/{dag_id}_process.txt'
    s3_hook.load_file(txt_path,
                        bucket_name=bucket_name,
                        replace=True,
                        key='process/{dag_id}_process.txt')

    logger.info('Finished Load Task for DAG {dag_id}.')


with DAG('AUTO_GGFLCienciasSociales_dag_etl',
        start_date=datetime(2022,11,1),
        catchup=False,
        schedule_interval='@hourly',
        ) as dag:

    extract = PythonOperator(task_id='extract',
                             python_callable=extract_task,
                             retries=5)

    transform = PythonOperator(task_id='transform',
                             python_callable=transform_task,
                             retries=5)

    load = PythonOperator(task_id='load',
                             python_callable=load_task,
                             retries=5)

    # set task dependencies
    extract >> transform >> load