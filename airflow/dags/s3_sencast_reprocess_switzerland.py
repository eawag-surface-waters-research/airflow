from datetime import datetime

from airflow.operators.bash import BashOperator
from airflow.operators.python_operator import PythonOperator
from airflow.models import Variable
from airflow.utils.dates import days_ago

from functions.email import report_failure
from functions.sencast import write_logical_date_to_parameter_file, get_two_weeks_ago

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
    's3_sencast_reprocess_switzerland',
    default_args=default_args,
    description='Re-process Sentinel 3 data for Switzerland with updated metadata.',
    schedule_interval="0 3 * * *",
    catchup=False,
    max_active_runs=5,
    tags=['sencast', 'operational'],
    user_defined_macros={'docker': 'eawag/sencast:0.0.1',
                         'DIAS': '/opt/airflow/filesystem/DIAS',
                         'git_repos': '/opt/airflow/filesystem/git',
                         'git_name': 'sencast',
                         'git_remote': 'https://github.com/eawag-surface-waters-research/sencast.git',
                         'environment_file': 'docker.ini',
                         'date': get_two_weeks_ago,
                         'sencast_folder_prefix': "datalakes_sui_S3_sui",
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

set_parameter_dates_14days = PythonOperator(
    task_id='set_parameter_dates_14days',
    python_callable=write_logical_date_to_parameter_file,
    on_failure_callback=report_failure,
    op_kwargs={"file": '/opt/airflow/filesystem/git/sencast/parameters/datalakes_sui_S3.ini', "offset": -14},
    dag=dag,
)

run_sencast_14days = BashOperator(
    task_id='run_sencast_14days',
    bash_command='docker run '
                 '-v {{ FILESYSTEM }}/DIAS:/DIAS '
                 '-v {{ FILESYSTEM }}/git/{{ git_name }}:/sencast '
                 '--rm '
                 '-i {{ docker }} -e {{ environment_file }} -p datalakes_sui_S3.ini',
    on_failure_callback=report_failure,
    dag=dag,
)

send_results = BashOperator(
    task_id='send_results',
    bash_command='f="{{ DIAS }}/output_data/{{ sencast_folder_prefix }}_{{ date(ds) }}_{{ date(ds) }}"; [ -d "$f" ] && '
                 'sshpass -p {{ API_PASSWORD }} scp -r "$f" {{ api_user }}@{{ api_server }}:{{ api_server_folder }} || '
                 'echo "Folder does not exist."',
    on_failure_callback=report_failure,
    dag=dag,
)

remove_results = BashOperator(
    task_id='remove_results',
    bash_command='f="{{ DIAS }}/output_data/{{ sencast_folder_prefix }}_{{ date(ds) }}_{{ date(ds) }}"; [ -d "$f" ] && '
                 'rm -rf "$f" || echo "Folder does not exist."',
    on_failure_callback=report_failure,
    dag=dag,
)

clone_repo >> set_parameter_dates_14days >> run_sencast_14days >> send_results >> remove_results
