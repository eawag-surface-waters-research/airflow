from datetime import timedelta

from airflow.operators.bash_operator import BashOperator
from airflow.utils.dates import days_ago

from airflow import DAG

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': days_ago(2),
    'email': ['james.runnalls@eawag.ch'],
    'email_on_failure': True,
    'email_on_retry': False
}
dag = DAG(
    'download_meteoswiss_cosmo1',
    default_args=default_args,
    description='Download COSMO1 data from MeteoSwiss.',
    schedule_interval=None,
)

clone_repo = BashOperator(
    task_id='clone_repo',
    bash_command="cd {{ params.folder }}; git clone {{ params.git }} || (cd {{ params.folder }} ; git pull)",
    params={'folder': '/opt/airflow/data',
            'git': 'https://github.com/eawag-surface-waters-research/alplakes-externaldata.git',
            'repo': 'alplakes-externaldata'},
    queue='api',
    dag=dag,
)

download = BashOperator(
    task_id='download',
    bash_command="mkdir -p {{ params.data }}; cd {{ params.dir }}; python -m {{ params.script }} {{ params.data }} {{ var.value.cosmo_ftp_password }}",
    params={'dir': '/opt/airflow/data/alplakes-externaldata',
            'script': 'externaldata.runs.meteoswiss_cosmo1',
            'data': '/opt/airflow/data/rawdata'},

    queue='api',
    dag=dag,
)

clone_repo >> download
