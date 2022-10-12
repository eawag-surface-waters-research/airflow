from datetime import timedelta
from dateutil import relativedelta

from airflow.operators.bash import BashOperator
from airflow.operators.docker_operator import DockerOperator
from airflow.utils.dates import days_ago
from airflow import DAG


def get_last_sunday(dt):
    return "20220102"
    bd = dt + relativedelta(weekday=SU(-1))
    return bd.strftime('%Y%m%d')


def get_end_date(dt):
    return "20220103"
    bd = dt + timedelta(days=5)
    return bd.strftime('%Y%m%d')


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': days_ago(2),
    'email': ['james.runnalls@eawag.ch'],
    'email_on_failure': True,
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
    'simulate_delft3dflow_operational_greifensee',
    default_args=default_args,
    description='Operational Delft3D-Flow simulation of Greifensee.',
    schedule_interval=None,
    tags=['simulation'],
    user_defined_macros={'model': 'delft3d-flow/greifensee',
                         'docker': 'eawag/delft3d-flow:6.03.00.62434',
                         'start': get_last_sunday,
                         'end': get_end_date,
                         'bucket': 'alplakes-eawag',
                         'api': "http://host.docker.internal:8000",
                         'upload': True}
)

"""prepare_simulation_files = BashOperator(
    task_id='prepare_simulation_files',
    bash_command="mkdir -p {{ params.git_repos }};"
                 "cd {{ params.git_repos }};"
                 "git clone {{ params.git_remote }} && cd {{ params.git_name }} || cd {{ params.git_name }} && git stash && git pull;"
                 "python src/main.py -m {{ model }} -d {{ docker }} -s {{ start(ds) }} -e {{ end(ds) }} -b {{ bucket }} -u {{ upload }} -a {{ api }}",
    params={'git_repos': '/opt/airflow/filesystem/git',
            'git_remote': 'https://github.com/eawag-surface-waters-research/alplakes-simulations.git',
            'git_name': 'alplakes-simulations',
            },
    dag=dag,
)"""

run_simulation = DockerOperator(
    task_id='run_simulation',
    image='eawag/delft3d-flow:6.03.00.62434',
    api_version='auto',
    auto_remove=True,
    # command="/bin/sleep 30",
    # docker_url="unix://var/run/docker.sock",
    # network_mode="bridge",
    dag=dag,
)

"""run_simulation = BashOperator(
    task_id='run_simulation',
    bash_command="docker run hello-world",
    dag=dag,
)"""

"""notify_api = BashOperator(
    task_id='notify_api',
    bash_command="curl {{ var.value.alplakes_api }}{{ params.api }}",
    params={'api': '/new_resource?bucket=alplakes-eawag&simulation=delft3d-flow&model=greifensee'},
    dag=dag,
)"""

run_simulation
