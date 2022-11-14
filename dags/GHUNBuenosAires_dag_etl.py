from datetime import datetime, timedelta
from pathlib import Path

from airflow import DAG

from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook

import logging
import logging.config

from plugins.GH_transform import transform_df


BASE_DIR = Path(__file__).parent.parent
sql_file_name = "GHUNBuenosAires.sql"
csv_file_name = "GHUNBuenosAires_select.csv"
txt_file_name = "GHUNBuenosAires_process.txt"
logger_name = "GHUNBuenosAires_dag_etl"


def configure_logger():
    LOGGING_CONFIG = BASE_DIR / "logger.cfg"
    logging.config.fileConfig(LOGGING_CONFIG, disable_existing_loggers=False)
    logger = logging.getLogger(logger_name)
    return logger


def extract_data():

    logger = configure_logger()
    logger.info("Start of extraction task")

    # Consulta sql.
    sql_path = BASE_DIR / f"include/{sql_file_name}"
    query = open(sql_path).read()

    # PostgresHook --> DataFrame
    hook = PostgresHook(postgres_conn_id="alkemy_db")
    df = hook.get_pandas_df(sql=query)

    file_path = BASE_DIR / f"files/{csv_file_name}"
    file_path.parent.mkdir(parents=True, exist_ok=True)

    logger.info("Done...")

    return df.to_csv(file_path, header=True, index=False)


def transform_data():

    logger = configure_logger()
    logger.info("Start of transform task")

    df_path = BASE_DIR / f"files/{csv_file_name}"
    output_path = BASE_DIR / f"datasets/{txt_file_name}"

    transform_df(df_path, output_path)

    logger.info("Done...")


def load_data():
    logger = configure_logger()
    logger.info("Start load task")

    s3_hook = S3Hook(aws_conn_id="aws_s3_bucket")
    s3_hook.load_file(
        BASE_DIR / f"datasets/{txt_file_name}",
        bucket_name="alkemy-gh",
        replace=True,
        key=f"process/{txt_file_name}",
    )

    logger.info("Done...")


with DAG(
    "Universidad_Buenos_Aires_etl",
    default_args={
        "retries": 5,
        "retry_delay": timedelta(minutes=5),
    },
    description="Realiza un ETL de los datos de la Universidad de Buenos Aires.",
    schedule=timedelta(hours=1),
    start_date=datetime(2022, 11, 11),
    tags=["etl"],
) as dag:

    # Utilizar postgres_hook
    # https://airflow.apache.org/docs/apache-airflow/1.10.6/_api/airflow/hooks/postgres_hook/index.html
    extract = PythonOperator(task_id="Extract", python_callable=extract_data)

    # Utilizar PythonOperator
    # Se debe realizar una funcion que levante los csv obtenidos del proceso de extracción y los transforme acorde a las necesidades.
    transform = PythonOperator(task_id="Transform", python_callable=transform_data)

    # Utilizar Providers de Amazon para la carga de datos.
    # https://airflow.apache.org/docs/apache-airflow-providers-amazon/stable/index.html
    load = PythonOperator(task_id="load", python_callable=load_data)

    extract >> transform >> load
