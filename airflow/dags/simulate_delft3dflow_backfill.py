from datetime import timedelta, datetime

from airflow.operators.bash import BashOperator
from airflow.operators.python_operator import PythonOperator
from airflow.models import Variable
from airflow.utils.dates import days_ago

from functions.email import report_failure
from functions.simulate import get_last_sunday, get_end_date, get_today, get_restart, number_of_cores, parse_restart

from airflow import DAG

"""
Example config input
{ "model": "zurich",
  "profile": "20190109",
  "cores": 5
}
"""

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': days_ago(2),
    'email': ['james.runnalls@eawag.ch'],
    'email_on_failure': False,
    'email_on_retry': False,
    'queue': 'simulation',
    'retries': None,
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
    'simulate_delft3dflow_backfill',
    default_args=default_args,
    description='Backfill Delft3D-Flow simulation.',
    schedule_interval=None,
    catchup=False,
    tags=['simulation', 'backfill'],
    user_defined_macros={'filesystem': '/opt/airflow/filesystem',
                         'docker': 'eawag/delft3d-flow:6.02.10.142612',
                         'simulation_repo_name': "alplakes-simulations",
                         'simulation_repo_https': "https://github.com/eawag-surface-waters-research/alplakes-simulations.git",
                         'api_user': "alplakes",
                         'api_server': 'eaw-alplakes2',
                         'API_PASSWORD': Variable.get("API_PASSWORD"),
                         'AWS_ID': Variable.get("AWS_ACCESS_KEY_ID"),
                         'AWS_KEY': Variable.get("AWS_SECRET_ACCESS_KEY")}
)

run_backfill = BashOperator(
    task_id='run_backfill',
    bash_command="mkdir -p {{ filesystem }}/git;"
                 "cd {{ filesystem }}/git;"
                 "git clone {{ simulation_repo_https }} && cd {{ simulation_repo_name }} || cd {{ simulation_repo_name }} && git stash && git pull;"
                 "python src/backfill.py -m delft3d-flow/{{ dag_run.conf.model }} -d {{ docker }} -c {{ dag_run.conf.cores }} "
                 "-p {{ dag_run.conf.profile }} -i {{ AWS_ID }} -k {{ AWS_KEY }} -w {{ API_PASSWORD }} -u {{ api_user }} -v {{ api_server }}",
    on_failure_callback=report_failure,
    dag=dag,
)

run_backfill
