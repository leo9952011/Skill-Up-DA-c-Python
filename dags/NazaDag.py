from datetime import datetime, timedelta
from airflow import DAG

from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago

default_args = {
    'owner':'Nazareno',
    'depends_on_past':False,
    'email':'airflow@example.com',
    'email_on_failure':False,
    'email_on_retry':False,
    'retries':5,
    'retry_delay': timedelta(minutes=1)
}

def sql_execute():
    print('ejecucion de archivos sql')
def data_processing():
    print('procesamiento en pandas')
def data_upload():
    print('carga de datos a S3')

with DAG(
    'Naza-Dag',
    default_args=default_args,
    description='Naza Test DAG',
    schedule_interval=timedelta(hours=1),
    start_date=days_ago(1),
    tags=['test','dag','naza']
) as dag:
    sql_task = PythonOperator(task_id ='SQL-EXECUTE', python_callable=sql_execute)
    processing_task = PythonOperator(task_id ='PANDAS', python_callable=data_processing)
    upload_task = PythonOperator(task_id ='SQL', python_callable=data_upload)
    
    sql_task >> processing_task >> upload_task
 