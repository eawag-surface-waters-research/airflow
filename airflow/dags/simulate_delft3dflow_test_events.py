from datetime import timedelta, datetime

from airflow.operators.bash import BashOperator
from airflow.operators.python_operator import PythonOperator
from airflow.models import Variable
from airflow.utils.dates import days_ago

from functions.email import report_failure, report_success
from functions.simulate import parse_profile, upload_restart_files

from airflow import DAG

"""
Example config input
{ 
    "folder": "media/simulations/delft3d-flow/test_events/geneva",
    "docker": "eawag/delft3d-flow:6.02.10.142612"
}
"""

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': days_ago(2),
    'email': ['james.runnalls@eawag.ch'],
    'email_on_failure': False,
    'email_on_retry': False,
    'queue': 'api',
    'retries': 0,
    'retry_delay': timedelta(minutes=30),
    # 'pool': 'backfill',
    'priority_weight': 5,
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
    'simulate_delft3dflow_test_events',
    default_args=default_args,
    description='Test event detection algorithms',
    schedule_interval=None,
    catchup=False,
    tags=['simulation', 'on demand'],
    user_defined_macros={'filesystem': '/opt/airflow/filesystem',
                         'simulation_repo_name': "alplakes-simulations",
                         'simulation_repo_https': "https://github.com/eawag-surface-waters-research/alplakes-simulations.git"
                         }
)

events_simulation_output = BashOperator(
        task_id='events_simulation_output',
        bash_command="mkdir -p {{ filesystem }}/git;"
                     "cd {{ filesystem }}/git;"
                     "git clone {{ simulation_repo_https }} && cd {{ simulation_repo_name }} || cd {{ simulation_repo_name }} && git stash && git pull;"
                     "python src/events.py -f {{ filesystem }}/{{ dag_run.conf.folder }} -d {{ dag_run.conf.docker }}",
        on_failure_callback=report_failure,
        dag=dag,
    )

events_simulation_output
