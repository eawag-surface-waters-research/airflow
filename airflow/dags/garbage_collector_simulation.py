from datetime import timedelta

from airflow.operators.bash import BashOperator
from airflow.operators.python_operator import PythonOperator
from airflow.models import Variable
from airflow.utils.dates import days_ago

from functions.email import report_failure

from airflow import DAG

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': days_ago(2),
    'email': ['james.runnalls@eawag.ch'],
    'email_on_failure': False,
    'email_on_retry': False,
    'queue': 'simulation',
    # 'retries': 1,
    # 'retry_delay': timedelta(minutes=5),
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
    'garbage_collector_simulation',
    default_args=default_args,
    description='Remove old files from simulation node to free up storage space',
    schedule_interval="@weekly",
    catchup=False,
    tags=['operational'],
)

remove_sencast_inputs = BashOperator(
    task_id='remove_sencast_inputs',
    bash_command="rm -rf /opt/airflow/filesystem/DIAS/input_data/*",
    on_failure_callback=report_failure,
    dag=dag,
)

remove_simulations = BashOperator(
    task_id='remove_simulations',
    bash_command='find /opt/airflow/filesystem/git/alplakes-simulations/runs -mtime +7 -mindepth 1 -type d -execdir rm -r {} \; 2>/dev/null || true',
    on_failure_callback=report_failure,
    dag=dag,
)

remove_sencast_inputs >> remove_simulations
