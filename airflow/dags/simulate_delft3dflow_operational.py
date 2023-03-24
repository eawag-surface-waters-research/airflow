import json
from datetime import timedelta, datetime

from airflow.operators.bash import BashOperator
from airflow.operators.python_operator import PythonOperator
from airflow.models import Variable
from airflow.utils.dates import days_ago

from functions.email import report_failure
from functions.simulate import get_last_sunday, get_end_date, get_today, get_restart, number_of_cores, post_notify_api

from airflow import DAG


def create_dag(dag_id, parameters):
    default_args = {
        'owner': 'airflow',
        'depends_on_past': False,
        'start_date': days_ago(2),
        'email': ['james.runnalls@eawag.ch'],
        'email_on_failure': False,
        'email_on_retry': False,
        'queue': 'simulation',
        'retries': 1,
        'retry_delay': timedelta(minutes=30),
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
        dag_id,
        default_args=default_args,
        description='Operational Delft3D-Flow simulation.',
        schedule_interval=parameters["schedule_interval"],
        catchup=False,
        tags=['simulation', 'operational'],
        user_defined_macros={'model': 'delft3d-flow/' + parameters["simulation_id"],
                             'docker': 'eawag/delft3d-flow:6.03.00.62434',
                             'start': get_last_sunday,
                             'end': get_end_date,
                             'today': get_today,
                             'restart': get_restart,
                             'bucket': 'alplakes-eawag',
                             'api': "http://eaw-alplakes2:8000",
                             'cores': parameters["cores"],
                             'number_of_cores': number_of_cores,
                             'upload': True}
    )

    prepare_simulation_files = BashOperator(
        task_id='prepare_simulation_files',
        bash_command="mkdir -p {{ params.git_repos }};"
                     "cd {{ params.git_repos }};"
                     "git clone {{ params.git_remote }} && cd {{ params.git_name }} || cd {{ params.git_name }} && git stash && git pull;"
                     "python src/main.py -m {{ model }} -d {{ docker }} -t {{ today(ds) }} -s {{ start(ds) }} -e {{ end(ds) }} -b {{ bucket }} -u {{ upload }} -a {{ api }}",
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
                     '-n "{{ params.netcdf }}_{{ start(ds) }}.nc" '
                     '-p {{ number_of_cores(task_instance, cores) }} '
                     '-r "{{ params.restart }}{{ restart(ds) }}.000000"',
        params={'download': "https://alplakes-eawag.s3.eu-central-1.amazonaws.com/simulations/delft3d-flow/simulation"
                            "-files/eawag_delft3dflow6030062434_delft3dflow_" + parameters["simulation_id"],
                'netcdf': 's3://alplakes-eawag/simulations/delft3d-flow/results'
                          '/eawag_delft3dflow6030062434_delft3dflow_' + parameters["simulation_id"],
                'restart': 's3://alplakes-eawag/simulations/delft3d-flow/restart-files/{}/tri-rst.Simulation_Web_rst.'.format(
                    parameters["simulation_id"]),
                'AWS_ID': Variable.get("AWS_ACCESS_KEY_ID"),
                'AWS_KEY': Variable.get("AWS_SECRET_ACCESS_KEY")},
        on_failure_callback=report_failure,
        dag=dag,
    )

    notify_api = PythonOperator(
        task_id='notify_api',
        python_callable=post_notify_api,
        params={
            "file": "https://alplakes-eawag.s3.eu-central-1.amazonaws.com/simulations/delft3d-flow/results"
                    "/eawag_delft3dflow6030062434_delft3dflow_" + parameters["simulation_id"],
            "api": "http://eaw-alplakes2:8000",
            "model": "delft3d-flow"},
        provide_context=True,
        on_failure_callback=report_failure,
        dag=dag,
    )

    prepare_simulation_files >> run_simulation >> notify_api

    return dag


with open('dags/simulate_delft3dflow_operational.json') as f:
    simulations = json.load(f)

for simulation in simulations:
    dag_id = "simulate_delft3dflow_operational_" + simulation["simulation_id"]
    globals()[dag_id] = create_dag(dag_id, simulation)