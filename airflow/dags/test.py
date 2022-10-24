from datetime import timedelta
import logging

from airflow.operators.python_operator import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.utils.dates import days_ago

from airflow import DAG

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': days_ago(2),
    'email': ['james.runnalls@eawag.ch'],
    'email_on_failure': True,
    'email_on_retry': False,
    # 'retries': 1,
    # 'retry_delay': timedelta(minutes=5),
    # 'queue': 'api',
    # 'pool': 'backfill',
    # 'priority_weight': 10,
    # 'end_date': datetime(2016, 1, 1),
    # 'wait_for_downstream': False,
    # 'sla': timedelta(hours=2),
    # 'execution_timeout': timedelta(seconds=300),
    # 'on_failure_callback': some_function,
    # 'on_success_callback': some_other_function,
    # 'on_retry_callback': another_function,
    # 'sla_miss_callback': yet_another_function,
    # 'trigger_rule': 'all_success'
}
dag = DAG(
    'test_dag',
    default_args=default_args,
    description='Test Dag',
    schedule_interval=None,
    tags=['test'],
)


def python_test_func():
    logging.info('Python Operator functioning.')
    print("Python Operator functioning.")


python_test = PythonOperator(
    task_id='python_test',
    python_callable=python_test_func,
    dag=dag,
)

bash_test = BashOperator(
    task_id='bash_test',
    bash_command='echo "Bash Operator functioning."',
    dag=dag,
)

python_test >> bash_test
