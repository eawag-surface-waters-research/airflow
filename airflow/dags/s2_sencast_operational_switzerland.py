from datetime import timedelta

from airflow.operators.bash import BashOperator
from airflow.operators.python_operator import PythonOperator
from airflow.models import Variable
from airflow.utils.dates import days_ago

from functions.email import report_failure
from functions.sencast import write_logical_date_to_parameter_file

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
    's2_sencast_operational_switzerland',
    default_args=default_args,
    description='Process Sentinel 2 data for Switzerland.',
    schedule_interval="0 2 * * *",
    catchup=False,
    tags=['sencast', 'operational'],
    user_defined_macros={'docker': 'eawag/sencast:0.0.1',
                         'DIAS': '/opt/airflow/filesystem/DIAS',
                         'git_repos': '/opt/airflow/filesystem/git',
                         'git_name': 'sencast',
                         'git_remote': 'https://github.com/eawag-surface-waters-research/sencast.git',
                         'environment_file': 'docker.ini',
                         'FILESYSTEM': Variable.get("FILESYSTEM")}
)

clone_repo = BashOperator(
    task_id='clone_repo',
    bash_command="mkdir -p {{ DIAS }}; mkdir -p {{ git_repos }}; cd {{ git_repos }}; "
                 "git clone --depth 1 {{ git_remote }} || (cd {{ git_name }} ; git stash ; git pull)",
    on_failure_callback=report_failure,
    dag=dag,
)

set_parameter_dates = PythonOperator(
    task_id='edit_input',
    python_callable=write_logical_date_to_parameter_file,
    on_failure_callback=report_failure,
    op_kwargs={"file": '/opt/airflow/filesystem/git/sencast/parameters/datalakes_sui_S2.ini'},
    dag=dag,
)

run_sencast = BashOperator(
    task_id='run_sencast',
    bash_command='docker run '
                 '-v {{ FILESYSTEM }}/DIAS:/DIAS '
                 '-v {{ FILESYSTEM }}/git/{{ git_name }}:/sencast '
                 '-i {{ docker }} -e {{ environment_file }} -p datalakes_sui_S2.ini',
    on_failure_callback=report_failure,
    dag=dag,
)

clone_repo >> set_parameter_dates >> run_sencast