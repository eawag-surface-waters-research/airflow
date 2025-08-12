from datetime import datetime, timedelta

from airflow.operators.bash import BashOperator
from airflow.models import Variable
from airflow.utils.task_group import TaskGroup

from functions.email import report_failure

from airflow import DAG

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(1982, 1, 1),
    'email': ['james.runnalls@eawag.ch'],
    'email_on_failure': False,
    'email_on_retry': False,
    'queue': 'api',
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
    'ai_summaries',
    default_args=default_args,
    description=f'AI summaries of Alplakes data',
    schedule_interval="0 11,23 * * *",
    catchup=False,
    tags=['ai', 'monitoring'],
    user_defined_macros={'git_repos': '/opt/airflow/filesystem/git',
                         'git_name': 'alplakes-ai-summaries',
                         'docker': 'eawag/ai-summaries:1.0.0',
                         'git_remote': 'https://github.com/eawag-surface-waters-research/alplakes-ai-summaries.git',
                         'filesystem': '/opt/airflow/filesystem',
                         'FILESYSTEM': Variable.get("FILESYSTEM")}
)


clone_repo = BashOperator(
    task_id='clone_repo',
    bash_command="mkdir -p {{ git_repos }}; cd {{ git_repos }}; "
                 "git clone --depth 1 {{ git_remote }} || "
                 "(cd {{ git_name }} ; git config --global --add safe.directory '*' ; git stash ; git pull)",
    on_failure_callback=report_failure,
    dag=dag,
)

build_docker = BashOperator(
    task_id='build_docker',
    bash_command="""
        if docker image inspect {{ docker }} > /dev/null 2>&1; then
            echo "Docker image {{ docker }} already exists locally."
        else
            echo "Docker image {{ docker }} does not exist. Building it now..."
            docker build -t {{ docker }} {{ git_repos }}/{{ git_name }}
        fi
        """,
    on_failure_callback=report_failure,
    dag=dag,
)


summarise = BashOperator(
    task_id='summarise',
    bash_command='docker run '
                 '-e OPENAI_API_KEY={{ params.OPENAI_API_KEY }} '
                 '-v {{ FILESYSTEM }}/git/{{ git_name }}:/repository '
                 '--rm '
                 '{{ docker }} '
                 '-u '
                 '-b {{ params.bucket }} '
                 '-i {{ params.AWS_ID }} '
                 '-k {{ params.AWS_KEY }} ',
    params={
        'AWS_ID': Variable.get("AWS_ACCESS_KEY_ID"),
        'AWS_KEY': Variable.get("AWS_SECRET_ACCESS_KEY"),
        'OPENAI_API_KEY': Variable.get("OPENAI_API_KEY"),
        'bucket': "https://alplakes-eawag.s3.eu-central-1.amazonaws.com",
    },
    on_failure_callback=report_failure,
    retries=1,
    retry_delay=timedelta(minutes=30),
    dag=dag,
)

clone_repo >> build_docker >> summarise
