"""
Story 2
## Grupo de Universidades B
## USalvador

COMO: Analista de datos
QUIERO: Configurar un DAG, sin consultas, ni procesamiento
PARA: Hacer un ETL para 2 universidades distintas.

Criterios de aceptacion:
Configurar el DAG para procese las siguientes universidades:
- Univ. Nacional Del Comahue
- Universidad Del Salvador
Documentar los operators que se deberian utilizar a futuro,
teniendo en cuenta que se va a hacer dos consultas SQL (una para cada universidad),
 se van a procesar los datos con pandas y se van a cargar los datos en S3.  
El DAG se debe ejecutar cada 1 hora, todos los dias y cada tarea se debe ejecutar
5 veces antes de fallar.

# Dev: Aldo Agunin
# Fecha: 05/11/2022
"""

from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator


# ------- DECLARACIONES -----------
universidad_corto = "USalvador"
universidad_largo = "Universidad de Salvador"

# def function_task_1():
#     print('Hello World!')

# PENDIENTE Hacerlo con Taskflow (decoradores)
# https://airflow.apache.org/docs/apache-airflow/stable/tutorial_taskflow_api.html
dag = DAG(
    dag_id=f"GB{universidad_corto}_2",   # dag_id="GBUNComahue_2",
    description=f"DAG para hacer ETL de la {universidad_largo}",
    tags=['Aldo', 'ETL'],
    start_date=datetime(2022, 11, 1),
    schedule=timedelta(hours=1),  # cada hora
    catchup=False,
    default_args={
        "retries": 5, # If a task fails, it will retry 5 times.
        "retry_delay": timedelta(minutes=5),
        },
    )


# task1 = EmptyOperator(
#     task_id="task_1", # debe ser único dentro del dag
#     dag=dag # dag al que pertenece esta tarea
#     )

# task1 = PythonOperator(
#     task_id="task_1", # debe ser único dentro del dag
#     python_callable=function_task_1, # función que se ejecutará
#     dag=dag # dag al que pertenece esta tarea
#     )

# primera tarea: correr script .SQL y la salida a .CSV
# se usara un PythonOperator que ejecute la consulta .SQL de la Story1 y genere salida a .CSV
extract = EmptyOperator(
    task_id="extraction_task",
    dag=dag
    )

# segunda tarea: procesar datos en pandas
# se usara un PythonOperator que llame a un modulo externo
transform = EmptyOperator(
    task_id="transformation_task",
    dag=dag
    )

# tercera tarea: subir resultados a amazon s3
# se usara un operador desarrollado por la comunidad
load = EmptyOperator(
    task_id="load_task",
    dag=dag
    )

extract >> transform >> load

