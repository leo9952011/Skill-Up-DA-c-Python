from datetime import datetime
import logging
import os

from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator

from plugins.GA_modules.constants import DATASETS_DIR, FILES_DIR, INCLUDE_DIR,\
    LOGGER_CFG_PATH, POSTAL_DATA_PATH
from plugins.GA_modules.ETL_funcs import extract_func, transform_func, \
    load_func


# Constants
UNIVERSITY_ID = 'GAUNVillaMaria'
SQL_PATH = os.path.join(INCLUDE_DIR, f'{UNIVERSITY_ID}.sql')
CSV_PATH = os.path.join(FILES_DIR, f'{UNIVERSITY_ID}_select.csv')
TXT_PATH = os.path.join(DATASETS_DIR, f'{UNIVERSITY_ID}_process.txt')


logging_config = LOGGER_CFG_PATH
logging.config.fileConfig(logging_config, disable_existing_loggers=False)
logger = logging.getLogger(UNIVERSITY_ID)


with DAG(
    dag_id=f'{UNIVERSITY_ID}_dag_etl',
    description=f'ETL process for {UNIVERSITY_ID}',
    schedule_interval='@hourly',
    start_date=datetime(2022, 11, 15, 6)
) as dag:
    # Read .sql in INCLUDE_DIR, save data as .csv in FILES_DIR
    extract = PythonOperator(
        task_id='extract_func',
        retries=5,
        python_callable=extract_func,
        op_kwargs={
            'university_id': UNIVERSITY_ID,
            'sql_path': SQL_PATH,
            'csv_path': CSV_PATH,
            'logger': logger
        }
    )

    # Apply requested formats for each column, add location or postal
    # code from codigos_postales.csv in ASSETS_DIR, compute ages from
    # birthdate, eliminate wrong age registries and save data as .txt in
    # DATASETS_DIR
    transform = PythonOperator(
        task_id='transform_func',
        retries=5,
        python_callable=transform_func,
        op_kwargs={
            'university_id': UNIVERSITY_ID,
            'csv_path': CSV_PATH,
            'txt_path': TXT_PATH,
            'logger': logger,
            'to_lower': [
                'university', 'career', 'last_name', 'location', 'email'
            ],
            'replace_underscores': [
                'university', 'career', 'last_name', 'location'
            ],
            'gender': {'M': 'male', 'F': 'female'},
            'date_format': '%d-%b-%y',
            'min_age': 15,
            'postal_data_path': POSTAL_DATA_PATH,
            'fill': 'postal_code'
        }
    )

    # Load .txt from DATASETS_DIR, upload to S3
    load = PythonOperator(
        task_id='load_func',
        retries=5,
        python_callable=load_func,
        op_kwargs={
            'university_id': UNIVERSITY_ID,
            'txt_path': TXT_PATH,
            'logger': logger
        }
    )

    extract >> transform >> load
