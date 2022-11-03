from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.empty import EmptyOperator

# from airflow.providers.postgres.operators.postgres import PostgresOperator


with DAG(
    "Universidad_de_Buenos_Aires",
    description="Realiza los procesos de Extracción, transformacion y carga de los datos de la UBA.",
    start_date=datetime(2022, 11, 1),
    schedule_interval=timedelta(hours=1),
) as dag:

    extract = EmptyOperator(task_id=1)
    transform = EmptyOperator(task_id="Transformación de datos")
    load = EmptyOperator(task_id="Carga de datos de datos")

    extract >> transform >> load
