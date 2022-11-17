"""
Story 4
## Grupo de Universidades B
## USalvador

COMO: Analista de datos
QUIERO: Implementar Python Operator para Extracción
PARA: tomar los datos de las bases de datos en el DAG

Criterios de aceptacion: 
Configurar un Python Operator, para que extraiga informacion de la base de datos 
utilizando el .sql disponible en el repositorio base de las siguientes universidades: 
- Univ. Nacional Del Comahue
- Universidad Del Salvador
Dejar la información en un archivo .csv dentro de la carpeta files.
Documentacion
https://airflow.apache.org/docs/apache-airflow/stable/_api/airflow/operators/python/index.html#module-airflow.operators.python
Utilizar el provider https://airflow.apache.org/docs/apache-airflow-providers-postgres/stable/_api/airflow/providers/postgres/hooks/postgres/index.html 
Analizar implementacion sugerida en este post:
https://stackoverflow.com/questions/72165393/use-result-from-one-operator-inside-another


# Dev: Aldo Agunin
# Fecha: 07/11/2022
"""

from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from pathlib import Path
import logging
import logging.config
import pandas as pd

# ------- DECLARACIONES -----------
universidad_corto = 'USalvador'
universidad_largo = 'Universidad del Salvador'

# ------- LOGGER ------------------
log_cfg = ('GB' + universidad_corto + '_log.cfg')
configfile = Path(__file__).parent.parent / 'plugins' / log_cfg
logging.config.fileConfig(configfile, disable_existing_loggers=False)
logger = logging.getLogger(__name__)

#---------------  extraccion  --------------
logger.info('*** Comenzando Extracción ***')
def datos_a_csv():
    ## Ubicacion del .sql
    sql_path = Path(__file__).parent.parent / 'include' / ('GB' + universidad_corto + '.sql')
    ## Leo el .sql
    sql_consulta = open(sql_path, 'r').read()
    ## Conexion a la base
    hook = PostgresHook(postgres_conn_id='alkemy_db')
    conexion = hook.get_conn()
    df = pd.read_sql(sql_consulta, conexion)
    ## Guardo .csv
    csv_path = Path.cwd() / 'files' / ('GB' + universidad_corto + '_select.csv')
    df.to_csv(csv_path, index=False)
    return
#------------------------------------------

dag = DAG(
    dag_id=f'GB{universidad_corto}_dag_etl',   # dag_id='GBUSalvador_dag_etl',
    description=f'DAG para hacer ETL de la {universidad_largo}',
    tags=['Aldo', 'ETL'],
    start_date=datetime(2022, 11, 1),
    schedule=timedelta(hours=1),  # cada hora
    catchup=False,
    default_args={
        'retries': 5, # If a task fails, it will retry 5 times.
        'retry_delay': timedelta(minutes=5),
        },
    )

# primera tarea: correr script .SQL y la salida a .CSV
# se usara un PythonOperator que ejecute la consulta .SQL de la Story1 y genere salida a .CSV
# extract = EmptyOperator(
#     task_id="extraction_task",
#     dag=dag
#     )
extract = PythonOperator(
        task_id='extraction_task',
        python_callable=datos_a_csv,
        dag=dag,
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
