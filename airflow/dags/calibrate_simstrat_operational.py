from airflow.operators.bash import BashOperator
from airflow.operators.python_operator import PythonOperator
from airflow.models import Variable
from airflow.utils.dates import days_ago

from functions.simstrat import upload_simstrat_calibration_result

from functions.email import report_failure, report_success

from airflow import DAG

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': days_ago(2),
    'email': ['james.runnalls@eawag.ch'],
    'email_on_failure': False,
    'email_on_retry': False,
    'queue': 'simulation',
    'retries': 0,
    # 'retry_delay': timedelta(minutes=60),
    # 'pool': 'backfill',
    'priority_weight': 2,
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
    "calibrate_simstrat_operational",
    default_args=default_args,
    description='Calibrate Simstrat operational.',
    schedule_interval=None,
    catchup=False,
    tags=['simulation', 'calibration'],
    user_defined_macros={'filesystem': '/opt/airflow/filesystem',
                         'FILESYSTEM': Variable.get("FILESYSTEM"),
                         'simulation_repo_https': "https://github.com/Eawag-AppliedSystemAnalysis/operational-simstrat.git",
                         'simulation_repo_name': "operational-simstrat"
                         }
)

run_calibration = BashOperator(
    task_id='run_calibration',
    bash_command="mkdir -p {{ filesystem }}/git/calibrate;"
                 "cd {{ filesystem }}/git/calibrate;"
                 "git config --global --add safe.directory '*';"
                 "git clone --recurse-submodules {{ simulation_repo_https }} && cd {{ simulation_repo_name }} || cd {{ simulation_repo_name }} && git stash && git pull;"
                 "git config submodule.recurse true;"
                 "python src/calibrator.py calibration docker_dir={{ FILESYSTEM }}/git/calibrate/{{ simulation_repo_name }};",
    on_failure_callback=report_failure,
    dag=dag,
)

upload_result = PythonOperator(
        task_id='upload_result',
        python_callable=upload_simstrat_calibration_result,
        op_kwargs={"bucket": "https://alplakes-eawag.s3.eu-central-1.amazonaws.com",
                   "file": "{{ filesystem }}/git/calibrate/{{ simulation_repo_name }}/static/lake_parameters.json",
                   'AWS_ID': Variable.get("AWS_ACCESS_KEY_ID"),
                   'AWS_KEY': Variable.get("AWS_SECRET_ACCESS_KEY")},
        on_failure_callback=report_failure,
        on_success_callback=report_success,
        dag=dag,
    )

run_calibration >> upload_result
