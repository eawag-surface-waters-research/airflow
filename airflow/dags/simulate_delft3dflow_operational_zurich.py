from datetime import timedelta, datetime

from airflow.operators.bash import BashOperator
from airflow.operators.docker_operator import DockerOperator
from airflow.models import Variable
from airflow.utils.dates import days_ago

from functions.email import report_failure
from functions.simulate import get_last_sunday, get_end_date, get_today

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
    'simulate_delft3dflow_operational_zurich',
    default_args=default_args,
    description='Operational Delft3D-Flow simulation of Lake Zurich.',
    schedule_interval="0 9 * * *",
    catchup=False,
    tags=['simulation'],
    user_defined_macros={'model': 'delft3d-flow/zurich',
                         'docker': 'eawag/delft3d-flow:6.03.00.62434',
                         'start': get_last_sunday,
                         'end': get_end_date,
                         'today': get_today,
                         'bucket': 'alplakes-eawag',
                         'api': "http://eaw-alplakes2:8000",
                         'cores': 5,
                         'upload': True}
)

prepare_simulation_files = BashOperator(
    task_id='prepare_simulation_files',
    bash_command="mkdir -p {{ params.git_repos }};"
                 "cd {{ params.git_repos }};"
                 "git clone {{ params.git_remote }} && cd {{ params.git_name }} || cd {{ params.git_name }} && git stash && git pull;"
                 "python src/main.py -m {{ model }} -d {{ docker }} -s {{ start(ds) }} -e {{ end(ds) }} -b {{ bucket }} -u {{ upload }} -a {{ api }}",
    params={'git_repos': '/opt/airflow/filesystem/git',
            'git_remote': 'https://github.com/eawag-surface-waters-research/alplakes-simulations.git',
            'git_name': 'alplakes-simulations',
            },
    on_failure_callback=report_failure,
    dag=dag,
)

run_simulation = BashOperator(
    task_id='run_simulation',
    bash_command='docker run -e AWS_ID={{ params.AWS_ID }} -e AWS_KEY={{ params.AWS_KEY }} {{ docker }} '
                 '-d "{{ params.download }}_{{ start(ds) }}_{{ end(ds) }}.zip" '
                 '-n "{{ params.netcdf }}_{{ start(ds) }}_{{ end(ds) }}.nc" '
                 '-p {{ cores }} '
                 '-r "{{ params.restart }}{{ today(ds) }}.000000"',
    params={'download': "https://alplakes-eawag.s3.eu-central-1.amazonaws.com/simulations/delft3d-flow/simulation"
                        "-files/eawag_delft3dflow6030062434_delft3dflow_zurich",
            'netcdf': 's3://alplakes-eawag/simulations/delft3d-flow/results/eawag_delft3dflow6030062434_delft3dflow_zurich',
            'restart': 's3://alplakes-eawag/simulations/delft3d-flow/restart-files/zurich/tri-rst.Simulation_Web_rst.',
            'AWS_ID': Variable.get("AWS_ACCESS_KEY_ID"),
            'AWS_KEY': Variable.get("AWS_SECRET_ACCESS_KEY")},
    on_failure_callback=report_failure,
    dag=dag,
)

"""notify_api = BashOperator(
    task_id='notify_api',
    bash_command="curl {{ api }}{{ params.api }}",
    params={'api': '/new_resource?bucket=alplakes-eawag&simulation=delft3d-flow&model=zurich'},
    dag=dag,
)"""

prepare_simulation_files >> run_simulation
