from airflow.operators.bash import BashOperator
from airflow.utils.dates import days_ago
from airflow.models import Variable

from functions.email import report_failure

from airflow import DAG

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': days_ago(2),
    'email': ['james.runnalls@eawag.ch'],
    'email_on_failure': False,
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
    'current_water_temperature',
    default_args=default_args,
    description='Collect current water temperature values',
    schedule_interval='*/20 * * * *',
    catchup=False,
    tags=['insitu', 'monitoring'],
)

clone_repo = BashOperator(
    task_id='clone_repo',
    bash_command="mkdir -p {{ params.git_repos }}; cd {{ params.git_repos }}; "
                 "git clone {{ params.git_remote }} || (cd {{ params.git_name }} ; git stash ; git pull)",
    params={'git_repos': '/opt/airflow/filesystem/git',
            'git_remote': 'https://github.com/JamesRunnalls/lake-scrape.git',
            'git_name': 'lake-scrape'},
    on_failure_callback=report_failure,
    dag=dag,
)

download = BashOperator(
    task_id='download',
    bash_command="cd {{ params.git_dir }}; python src/main.py -t temperature "
                 "-f {{ params.filesystem }} -m -u -b {{ params.bucket }} -i {{ params.AWS_ID }} -k {{ params.AWS_KEY }}",
    params={'git_dir': '/opt/airflow/filesystem/git/lake-scrape',
            'filesystem': '/opt/airflow/filesystem',
            "bucket": "https://alplakes-eawag.s3.eu-central-1.amazonaws.com",
            'AWS_ID': Variable.get("AWS_ACCESS_KEY_ID"),
            'AWS_KEY': Variable.get("AWS_SECRET_ACCESS_KEY")
            },
    on_failure_callback=report_failure,
    dag=dag,
)

clone_repo >> download
