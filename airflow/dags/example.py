from datetime import timedelta
import logging

from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago

from airflow import DAG

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': days_ago(2),
    'email': ['james.runnalls@eawag.ch'],
    'email_on_failure': True,
    'email_on_retry': False
}
dag = DAG(
    'example_dag',
    default_args=default_args,
    description='Example Dag',
    schedule_interval=None,
    tags=['example'],
)


def hello_world():
    logging.info('Hello world run')
    print("hello world")


helloworld = PythonOperator(
    task_id='hello_world',
    python_callable=hello_world,
    queue='api',
    dag=dag,
)

helloworld
