from datetime import timedelta
import logging

from airflow.operators.python_operator import PythonOperator
from airflow.contrib.operators.docker_swarm_operator import DockerSwarmOperator
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


def test_python_func():
    logging.info('PythonOperator working.')
    print("PythonOperator working.")


test_python = PythonOperator(
    task_id='test_python',
    python_callable=test_python_func,
    dag=dag,
)

test_bash = BashOperator(
    task_id='test_bash',
    bash_command='echo "BashOperator working."',
    dag=dag,
)

test_swarm = DockerSwarmOperator(
    task_id='test_swarm',
    api_version='auto',
    command='echo "DockerSwarmOperator working."',
    image='alpine',
    auto_remove=True,
    dag=dag,
)

test_python >> test_bash >> test_swarm
