from datetime import datetime, timedelta
from pathlib import Path

from airflow import DAG

from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator

import logging
import logging.config

# Path del logger.cfg
BASE_DIR = Path().parent.parent

logger_cfg = BASE_DIR / "logger.cfg"

logging.config.fileConfig(
    fname=logger_cfg,
)
# Crear el logger ya configurado.
logger = logging.getLogger("GHUBuenos_Aires_dag_etl")


def extract_data():

    logger.warning("test")

    # Antes de ejecutar asegurarse de crear la conexion.
    logger.warning("probando")

    # Consulta sql.
    sql_path = Path("/usr/local/airflow/include/GHUBuenosAires.sql")
    query = open(sql_path).read()

    # PostgresHook --> DataFrame
    hook = PostgresHook(postgres_conn_id="postgres_univ")
    df = hook.get_pandas_df(sql=query)

    file_path = Path("/usr/local/airflow/files/GHUBuenosAires_select.csv")
    file_path.parent.mkdir(parents=True, exist_ok=True)

    return df.to_csv(file_path, header=True, index=False)


with DAG(
    "Universidad_Buenos_Aires_etl",
    default_args={
        "retries": 5,
        "retry_delay": timedelta(seconds=5),
    },
    description="Realiza un ETL de los datos de la Universidad de Buenos Aires.",
    schedule=timedelta(hours=1),
    start_date=datetime(2022, 11, 2),
    tags=["etl"],
) as dag:

    # Utilizar postgres_hook
    # https://airflow.apache.org/docs/apache-airflow/1.10.6/_api/airflow/hooks/postgres_hook/index.html
    extract = PythonOperator(task_id="Extract", python_callable=extract_data)

    # Utilizar PythonOperator
    # Se debe realizar una funcion que levante los csv obtenidos del proceso de extracción y los transforme acorde a las necesidades.
    transform = EmptyOperator(task_id="transform")

    # Utilizar Providers de Amazon para la carga de datos.
    # https://airflow.apache.org/docs/apache-airflow-providers-amazon/stable/index.html
    load = EmptyOperator(task_id="load")

    extract >> transform >> load
