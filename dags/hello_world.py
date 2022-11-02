from datetime import datetime
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
def print_hello():
  print('¡Hola Mundo!')
dag = DAG('hello_world', description='Hola Mundo DAG',
schedule_interval='* * * * *',
start_date=datetime(2017, 3, 20),
catchup=False,)
hello_operator = PythonOperator(task_id='hello_task', python_callable=print_hello, dag=dag)example_dag_basic = example_dag_basic()