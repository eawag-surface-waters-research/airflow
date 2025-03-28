from datetime import datetime, timedelta

from airflow.operators.bash import BashOperator
from airflow.operators.python_operator import PythonOperator
from airflow.models import Variable
from airflow.utils.dates import days_ago

from functions.simstrat import upload_simstrat_download_data

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
    "simulate_simstrat_download",
    default_args=default_args,
    description='Simstrat simulations for download.',
    schedule_interval="30 17 1 * *",
    catchup=False,
    tags=['simulation', 'download'],
    user_defined_macros={'filesystem': '/opt/airflow/filesystem',
                         'FILESYSTEM': Variable.get("FILESYSTEM"),
                         'simulation_repo_https': "https://github.com/Eawag-AppliedSystemAnalysis/operational-simstrat.git",
                         'simulation_repo_name': "operational-simstrat",
                         },
    params={
        'overwrite_simulation': 'false',
    }
)

run_simulation = BashOperator(
    task_id='run_simulation',
    bash_command="mkdir -p {{ filesystem }}/git/download;"
                 "cd {{ filesystem }}/git/download;"
                 "git clone {{ simulation_repo_https }} && cd {{ simulation_repo_name }} || cd {{ simulation_repo_name }} && git stash && git pull;"
                 "python src/main.py download docker_dir={{ FILESYSTEM }}/git/download/{{ simulation_repo_name }}",
    on_failure_callback=report_failure,
    dag=dag,
)

upload_data = PythonOperator(
        task_id='upload_data',
        python_callable=upload_simstrat_download_data,
        op_kwargs={"bucket": "https://alplakes-eawag.s3.eu-central-1.amazonaws.com",
                   "repo": "{{ filesystem }}/git/download/{{ simulation_repo_name }}",
                   'AWS_ID': Variable.get("AWS_ACCESS_KEY_ID"),
                   'AWS_KEY': Variable.get("AWS_SECRET_ACCESS_KEY")},
        on_failure_callback=report_failure,
        dag=dag,
    )

run_simulation >> upload_data
