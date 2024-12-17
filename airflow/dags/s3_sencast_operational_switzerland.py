from datetime import datetime, timedelta

from airflow.operators.bash import BashOperator
from airflow.operators.python_operator import PythonOperator
from airflow.models import Variable

from functions.email import report_failure
from functions.sencast import write_logical_date_to_parameter_file_ss

from airflow import DAG

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2016, 4, 26),
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
    's3_sencast_operational_switzerland',
    default_args=default_args,
    description='Process Sentinel 3 data for Switzerland.',
    schedule_interval="0 1 * * *",
    catchup=True,
    max_active_runs=10,
    tags=['sencast', 'operational'],
    user_defined_macros={'docker': 'eawag/sencast:0.0.1',
                         'DIAS': '/opt/airflow/filesystem/DIAS',
                         'git_repos': '/opt/airflow/filesystem/git',
                         'git_name': 'sencast',
                         'git_remote': 'https://github.com/eawag-surface-waters-research/sencast.git',
                         'environment_file': 'docker.ini',
                         'wkt': "sui",
                         'prefix': "datalakes_sui_S3",
                         'api_user': "alplakes",
                         'api_server': 'eaw-alplakes2',
                         'API_PASSWORD': Variable.get("API_PASSWORD"),
                         'api_server_folder': "/nfsmount/filesystem/DIAS/output_data",
                         'FILESYSTEM': Variable.get("FILESYSTEM")}
)

clone_repo = BashOperator(
    task_id='clone_repo',
    bash_command="mkdir -p {{ DIAS }}; mkdir -p {{ git_repos }}; cd {{ git_repos }}; "
                 "git clone --depth 1 {{ git_remote }} || "
                 "(cd {{ git_name }} ; git config --global --add safe.directory '*' ; git stash ; git pull)",
    on_failure_callback=report_failure,
    dag=dag,
)

set_parameter_dates_logical = PythonOperator(
    task_id='set_parameter_dates_logical',
    python_callable=write_logical_date_to_parameter_file_ss,
    on_failure_callback=report_failure,
    op_kwargs={"file": '/opt/airflow/filesystem/git/sencast/parameters/datalakes_sui_S3.ini'},
    dag=dag,
)

run_sencast_logical = BashOperator(
    task_id='run_sencast_logical',
    bash_command='docker run '
                 '-v {{ FILESYSTEM }}/DIAS:/DIAS '
                 '-v {{ FILESYSTEM }}/git/{{ git_name }}:/sencast '
                 '--rm '
                 '-i {{ docker }} -e {{ environment_file }} -p {{ prefix }}_{{ ds }}.ini',
    on_failure_callback=report_failure,
    dag=dag,
)

send_results = BashOperator(
    task_id='send_results',
    bash_command='f="{{ DIAS }}/output_data/{{ prefix }}_{{ ds }}_{{ wkt }}_{{ ds }}_{{ ds }}"; [ -d "$f" ] && '
                 'sshpass -p {{ API_PASSWORD }} scp -r "$f" {{ api_user }}@{{ api_server }}:{{ api_server_folder }} || '
                 'echo "Folder does not exist."',
    on_failure_callback=report_failure,
    dag=dag,
)

remove_results = BashOperator(
    task_id='remove_results',
    bash_command='f="{{ DIAS }}/output_data/{{ prefix }}_{{ ds }}_{{ wkt }}_{{ ds }}_{{ ds }}"; [ -d "$f" ] && '
                 'rm -rf "$f" || echo "Folder does not exist."',
    on_failure_callback=report_failure,
    dag=dag,
)

clone_repo >> set_parameter_dates_logical >> run_sencast_logical >> send_results >> remove_results
