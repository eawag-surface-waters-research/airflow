from datetime import timedelta
import logging

from airflow.operators.python_operator import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.operators.email import EmailOperator
from airflow.utils.dates import days_ago

from functions.email import report_failure, report_success

from airflow import DAG

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': days_ago(2),
    'email': ['james.runnalls@eawag.ch'],
    'email_on_failure': False,
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
    'test_setup',
    default_args=default_args,
    description='Test all services functioning.',
    schedule_interval=None,
    tags=['test', 'on demand'],
)


def python_test_func():
    logging.info('Python Operator functioning.')
    print("Python Operator functioning.")


def python_fail_func():
    logging.info('Testing email on failure.')
    raise ValueError("Test failure event.")


python_test_api = PythonOperator(
    task_id='python_test_api',
    python_callable=python_test_func,
    queue='api',
    on_failure_callback=report_failure,
    dag=dag,
)

python_test_simulation = PythonOperator(
    task_id='python_test_simulation',
    python_callable=python_test_func,
    queue='simulation',
    on_failure_callback=report_failure,
    dag=dag,
)

python_test_fail = PythonOperator(
    task_id='python_test_fail',
    python_callable=python_fail_func,
    queue='api',
    on_failure_callback=report_failure,
    dag=dag,
)

bash_test_api = BashOperator(
    task_id='bash_test_api',
    bash_command='echo "Bash Operator functioning."',
    queue='api',
    on_failure_callback=report_failure,
    dag=dag,
)

bash_test_simulation = BashOperator(
    task_id='bash_test_simulation',
    bash_command='echo "Bash Operator functioning."',
    queue='simulation',
    on_failure_callback=report_failure,
    dag=dag,
)

bash_test_simulation_docker = BashOperator(
    task_id='bash_test_simulation_docker',
    bash_command='docker run --rm hello-world',
    queue='simulation',
    on_failure_callback=report_failure,
    dag=dag,
)

bash_test_success = BashOperator(
    task_id='bash_test_success',
    bash_command='echo "Hello World"',
    queue='simulation',
    on_failure_callback=report_failure,
    on_success_callback=report_success,
    dag=dag,
)

python_test_api >> bash_test_api >> python_test_simulation >> bash_test_simulation >> bash_test_success >> bash_test_simulation_docker >> python_test_fail
