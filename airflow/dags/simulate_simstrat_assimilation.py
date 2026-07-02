from datetime import datetime, timedelta

from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.models import Variable

from functions.email import report_failure

from airflow import DAG

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email': ['james.runnalls@eawag.ch'],
    'email_on_failure': False,
    'email_on_retry': False,
    'queue': 'simulation',
    'retries': 1,
    'retry_delay': timedelta(minutes=60),
    # 'pool': 'backfill',
    'priority_weight': 8,
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
    "simulate_simstrat_assimilation",
    default_args=default_args,
    description='Operational Simstrat assimilation.',
    schedule="15 9 * * *",
    catchup=False,
    tags=['simulation', 'operational'],
    user_defined_macros={'filesystem': '/opt/airflow/filesystem',
                         'FILESYSTEM': Variable.get("FILESYSTEM"),
                         'simulation_repo_https': "https://github.com/Eawag-AppliedSystemAnalysis/operational-simstrat.git",
                         'simulation_repo_name': "operational-simstrat",
                         'API_HOST': "eaw-alplakes2",
                         'API_USER': "alplakes",
                         'API_PASSWORD': Variable.get("API_PASSWORD"),
                         'VISUALCROSSING_KEY': Variable.get("VISUALCROSSING_KEY")
                         },
    params={
        'overwrite_simulation': 'false',
    }
)

run_simulation = BashOperator(
    task_id='run_simulation',
    bash_command="mkdir -p {{ filesystem }}/git/assimilation;"
                 "cd {{ filesystem }}/git/assimilation;"
                 "git config --global --add safe.directory '*';"
                 "git clone --recurse-submodules {{ simulation_repo_https }} && cd {{ simulation_repo_name }} || cd {{ simulation_repo_name }} && git stash && git pull;"
                 "git config submodule.recurse true;"
                 "python src/data_assimilation.py assimilation server_host={{ API_HOST }} sever_user={{ API_USER }} server_password={{ API_PASSWORD }} visualcrossing_key={{ VISUALCROSSING_KEY }} "
                 "overwrite_simulation={{ params.overwrite_simulation }} docker_dir={{ FILESYSTEM }}/git/assimilation/{{ simulation_repo_name }}",
    on_failure_callback=report_failure,
    dag=dag,
)

run_simulation
