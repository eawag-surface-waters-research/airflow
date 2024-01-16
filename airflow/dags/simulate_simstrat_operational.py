from datetime import datetime, timedelta

from airflow.operators.bash import BashOperator
from airflow.operators.python_operator import PythonOperator
from airflow.models import Variable
from airflow.utils.dates import days_ago

from functions.simulate import get_today

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
    "simulate_simstrat_operational",
    default_args=default_args,
    description='Operational Simstrat simulation.',
    schedule_interval="15 9 * * *",
    catchup=False,
    tags=['simulation', 'operational'],
    user_defined_macros={'filesystem': '/opt/airflow/filesystem',
                         'FILESYSTEM': Variable.get("FILESYSTEM"),
                         'simulation_repo_https': "https://github.com/Eawag-AppliedSystemAnalysis/operational-simstrat.git",
                         'simulation_repo_name': "operational-simstrat",
                         'today': get_today,
                         'argfile': "operational.par",
                         'api': "http://eaw-alplakes2:8000",
                         'api_user': "alplakes",
                         'api_server': 'eaw-alplakes2',
                         'API_PASSWORD': Variable.get("API_PASSWORD"),
                         'api_server_folder': "/nfsmount/filesystem/media/simulations/simstrat/results"
                         }
)

prepare_simulation_files = BashOperator(
    task_id='prepare_simulation_files',
    bash_command="mkdir -p {{ filesystem }}/git;"
                 "cd {{ filesystem }}/git;"
                 "git clone {{ simulation_repo_https }} && cd {{ simulation_repo_name }} || cd {{ simulation_repo_name }} && git stash && git pull;"
                 "python src/preprocessing.py -a {{ argfile }} -p {{ params.parallel }} -t {{ today(ds) }} -a {{ api }}",
    params={'parallel': 5},
    on_failure_callback=report_failure,
    dag=dag,
)

run_simulation = BashOperator(
    task_id='run_simulation',
    bash_command="cd {{ filesystem }}/git/{{ simulation_repo_name }};"
                 "python src/run.py -a {{ argfile }} -p {{ params.parallel }} -t {{ today(ds) }}",
    params={'parallel': 5},
    on_failure_callback=report_failure,
    dag=dag,
)

postprocess_simulation = BashOperator(
    task_id='postprocess_simulation',
    bash_command="cd {{ filesystem }}/git/{{ simulation_repo_name }};"
                 "python src/postprocess.py -a {{ argfile }} -p {{ params.parallel }} -t {{ today(ds) }}",
    params={'parallel': 5},
    on_failure_callback=report_failure,
    dag=dag,
)

send_results = BashOperator(
    task_id='send_results',
    bash_command="",
    on_failure_callback=report_failure,
    dag=dag,
)

remove_results = BashOperator(
    task_id='remove_results',
    bash_command="",
    on_failure_callback=report_failure,
    dag=dag,
)

prepare_simulation_files >> run_simulation >> postprocess_simulation >> send_results >> remove_results
