from datetime import datetime
from airflow import DAG
from airflow.operators.python_operator import PythonOperator

def print_hello():
    print('Hello World!')

dag = DAG('hello_world', description='Hola Mundo DAG',
        schedule_interval='* * * * *',
        start_date=datetime(2021, 10, 20),
        catchup=False,)

hello_operator = PythonOperator(task_id='hello_task', python_callable=print_hello, dag=dag)
# hello_operator = PythonOperator(task_id='hello_task', python_callable=print_hello, dag=dag)
