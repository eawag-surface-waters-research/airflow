from datetime import timedelta

from airflow.operators.bash import BashOperator
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
    'retries': 2,
    'retry_delay': timedelta(minutes=30),
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
    "lemanscope_operational_qa",
    default_args=default_args,
    description='Quality assurance for Lemanscope data.',
    schedule_interval="45 23 * * *",
    catchup=False,
    tags=['data pipeline', 'operational'],
    user_defined_macros={'filesystem': '/opt/airflow/filesystem',
                         'simulation_repo_https': "https://github.com/eawag-surface-waters-research/lemanscope-qa.git",
                         'simulation_repo_name': "lemanscope-qa"}
)

process_data = BashOperator(
    task_id='process_data',
    bash_command="mkdir -p {{ filesystem }}/git;"
                 "cd {{ filesystem }}/git;"
                 "git clone {{ simulation_repo_https }} && cd {{ simulation_repo_name }} || cd {{ simulation_repo_name }} && git stash && git pull;"
                 "python src/main.py -u -b {{ params.bucket }} -i {{ params.AWS_ID }} -k {{ params.AWS_KEY }}",
    params={"bucket": "eawagrs",
            'AWS_ID': Variable.get("AWS_ACCESS_KEY_ID"),
            'AWS_KEY': Variable.get("AWS_SECRET_ACCESS_KEY")},
    on_failure_callback=report_failure,
    dag=dag,
)

process_data
