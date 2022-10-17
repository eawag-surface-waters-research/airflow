from datetime import timedelta

from airflow.operators.bash import BashOperator
from airflow.models import Variable
from airflow.utils.dates import days_ago

from airflow import DAG

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': days_ago(2),
    'email': ['james.runnalls@eawag.ch'],
    'email_on_failure': True,
    'email_on_retry': False,
    'queue': 'api',
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
    'download_meteoswiss_cosmo',
    default_args=default_args,
    description='Download COSMO data from MeteoSwiss.',
    schedule_interval="0 8 * * *",
    tags=['api'],
)

clone_repo = BashOperator(
    task_id='clone_repo',
    bash_command="mkdir -p {{ params.git_repos }}; cd {{ params.git_repos }}; "
                 "git clone {{ params.git_remote }} || (cd {{ params.git_name }} ; git stash ; git pull)",
    params={'git_repos': '/opt/airflow/filesystem/git',
            'git_remote': 'https://github.com/eawag-surface-waters-research/alplakes-externaldata.git',
            'git_name': 'alplakes-externaldata'},
    dag=dag,
)

download = BashOperator(
    task_id='download',
    bash_command="mkdir -p {{ params.filesystem }}; cd {{ params.git_dir }}; python src/main.py -s {{ params.source }} "
                 "-f {{ params.filesystem }} -p {{ params.COSMO_FTP_PASSWORD }}",
    params={'git_dir': '/opt/airflow/filesystem/git/alplakes-externaldata',
            'source': 'meteoswiss_cosmo',
            'filesystem': '/opt/airflow/filesystem/media',
            'COSMO_FTP_PASSWORD': Variable.get("COSMO_FTP_PASSWORD")},
    dag=dag,
)

clone_repo >> download