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
    'queue': 'api',
    'retries': 2,
    'retry_delay': timedelta(minutes=60),
    # 'pool': 'backfill',
    'priority_weight': 20,
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
    'download_meteoswiss_meteodata',
    default_args=default_args,
    description='Download Meteodata from MeteoSwiss.',
    schedule_interval="0 11 * * *",
    catchup=False,
    tags=['api', 'operational'],
)

clone_repo = BashOperator(
    task_id='clone_repo',
    bash_command="mkdir -p {{ params.git_repos }}; cd {{ params.git_repos }}; "
                 "git clone {{ params.git_remote }} || (cd {{ params.git_name }} ; git stash ; git pull)",
    params={'git_repos': '/opt/airflow/filesystem/git',
            'git_remote': 'https://github.com/eawag-surface-waters-research/alplakes-externaldata.git',
            'git_name': 'alplakes-externaldata'},
    on_failure_callback=report_failure,
    dag=dag,
)

download = BashOperator(
    task_id='download',
    bash_command="mkdir -p {{ params.filesystem }}; cd {{ params.git_dir }}; python src/main.py -s {{ params.source }} "
                 "-f {{ params.filesystem }} -p {{ params.METEO_FTP_PASSWORD }}",
    params={'git_dir': '/opt/airflow/filesystem/git/alplakes-externaldata',
            'source': 'meteoswiss_meteodata',
            'filesystem': '/opt/airflow/filesystem/media',
            'METEO_FTP_PASSWORD': Variable.get("METEO_FTP_PASSWORD")},
    on_failure_callback=report_failure,
    dag=dag,
)

clone_repo >> download
