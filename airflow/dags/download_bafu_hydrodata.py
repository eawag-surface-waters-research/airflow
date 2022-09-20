from datetime import timedelta

from airflow.operators.bash import BashOperator
from airflow.utils.dates import days_ago

from airflow import DAG

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': days_ago(2),
    'email': ['james.runnalls@eawag.ch'],
    'email_on_failure': True,
    'email_on_retry': False,
    # 'retries': 1,
    # 'retry_delay': timedelta(minutes=5),
    'queue': 'api',
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
    'download_bafu_hydrodata',
    default_args=default_args,
    description='Download hydrodata data from Bafu.',
    schedule_interval=None,
    tags=['api'],
)

clone_repo = BashOperator(
    task_id='clone_repo',
    bash_command="cd {{ params.folder }}; git clone {{ params.git }} || (cd {{ params.repo }} ; git stash; git pull)",
    params={'folder': '/opt/airflow/data',
            'git': 'https://github.com/eawag-surface-waters-research/alplakes-externaldata.git',
            'repo': 'alplakes-externaldata'},
    dag=dag,
)

download = BashOperator(
    task_id='download',
    bash_command="mkdir -p {{ params.data }}; cd {{ params.dir }}; python -m {{ params.script }} {{ params.data }} {{ var.value.cosmo_ftp_password }}",
    params={'dir': '/opt/airflow/data/alplakes-externaldata',
            'script': 'externaldata.runs.bafu_hydrodata',
            'data': '/opt/airflow/data/rawdata'},
    dag=dag,
)

clone_repo >> download
