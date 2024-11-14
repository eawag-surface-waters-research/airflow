from datetime import datetime, timedelta

from airflow.operators.bash import BashOperator
from airflow.operators.python_operator import PythonOperator
from airflow.models import Variable
from airflow.utils.dates import days_ago

from functions.simstrat import cache_simstrat_operational_data, validate_simstrat_operational_data

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
    schedule_interval="45 8 * * *",
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
    bash_command="mkdir -p {{ filesystem }}/git;"
                 "cd {{ filesystem }}/git;"
                 "git clone {{ simulation_repo_https }} && cd {{ simulation_repo_name }} || cd {{ simulation_repo_name }} && git stash && git pull;"
                 "python src/main.py operational server_host={{ API_HOST }} sever_user={{ API_USER }} server_password={{ API_PASSWORD }} visualcrossing_key={{ VISUALCROSSING_KEY }} "
                 "overwrite_simulation={{ params.overwrite_simulation }} docker_dir={{ FILESYSTEM }}/git/{{ simulation_repo_name }}",
    on_failure_callback=report_failure,
    dag=dag,
)

cache_data = PythonOperator(
        task_id='cache_data',
        python_callable=cache_simstrat_operational_data,
        op_kwargs={"bucket": "https://alplakes-eawag.s3.eu-central-1.amazonaws.com",
                   "api": "https://alplakes-api.eawag.ch",
                   'AWS_ID': Variable.get("AWS_ACCESS_KEY_ID"),
                   'AWS_KEY': Variable.get("AWS_SECRET_ACCESS_KEY")},
        on_failure_callback=report_failure,
        dag=dag,
    )

validate_results = PythonOperator(
        task_id='validate_results',
        python_callable=validate_simstrat_operational_data,
        op_kwargs={"api": "https://alplakes-api.eawag.ch"},
        retries=0,
        on_failure_callback=report_failure,
        dag=dag,
    )

run_simulation >> cache_data >> validate_results
