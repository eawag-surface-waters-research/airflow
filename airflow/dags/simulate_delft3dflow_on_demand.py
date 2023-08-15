import json
from datetime import timedelta, datetime

from airflow.operators.bash import BashOperator
from airflow.operators.python_operator import PythonOperator
from airflow.models import Variable
from airflow.utils.dates import days_ago

from functions.email import report_failure
from functions.simulate import get_last_sunday, get_end_date, get_today, get_restart, number_of_cores, parse_restart

from airflow import DAG

"""
Example config input
{ "model": "geneva",
  "start": "20230530",
  "end": "20230704",
  "cores": 5,
  "restart": ""
}
"""


def get_dag_run_conf(**kwargs):
    conf = kwargs['dag_run'].conf
    return conf


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': days_ago(2),
    'email': ['james.runnalls@eawag.ch'],
    'email_on_failure': False,
    'email_on_retry': False,
    'queue': 'simulation',
    'retries': None,
    'retry_delay': timedelta(minutes=30),
    # 'pool': 'backfill',
    'priority_weight': 5,
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
    'simulate_delft3dflow_on_demand',
    default_args=default_args,
    description='Operational Delft3D-Flow simulation.',
    schedule_interval=None,
    catchup=False,
    tags=['simulation', 'on demand'],
    user_defined_macros={'filesystem': '/opt/airflow/filesystem',
                         'FILESYSTEM': Variable.get("FILESYSTEM"),
                         'docker': 'eawag/delft3d-flow:6.03.00.62434',
                         'simulation_folder_prefix': 'eawag_delft3dflow6030062434_delft3dflow',
                         'today': get_today,
                         'bucket': 'alplakes-eawag',
                         'api': "http://eaw-alplakes2:8000",
                         'simulation_repo_name': "alplakes-simulations",
                         'simulation_repo_https': "https://github.com/eawag-surface-waters-research/alplakes-simulations.git",
                         'api_user': "alplakes",
                         'api_server': 'eaw-alplakes2',
                         'API_PASSWORD': Variable.get("API_PASSWORD"),
                         'api_server_folder': "/nfsmount/filesystem/media/simulations/delft3d-flow/results/",
                         'number_of_cores': number_of_cores}
)

prepare_simulation_files = BashOperator(
    task_id='prepare_simulation_files',
    bash_command="mkdir -p {{ filesystem }}/git;"
                 "cd {{ filesystem }}/git;"
                 "git clone {{ simulation_repo_https }} && cd {{ simulation_repo_name }} || cd {{ simulation_repo_name }} && git stash && git pull;"
                 "python src/main.py -m delft3d-flow/{{ dag_run.conf.model }} -d {{ docker }} {{ dag_run.conf.restart }} -t {{ today(ds) }} -s {{ dag_run.conf.start }} -e {{ dag_run.conf.end }} -b {{ bucket }} -u True -a {{ api }}",
    on_failure_callback=report_failure,
    dag=dag,
)

run_simulation = BashOperator(
    task_id='run_simulation',
    bash_command='docker run '
                 '-e AWS_ID={{ params.AWS_ID }} '
                 '-e AWS_KEY={{ params.AWS_KEY }} '
                 '-v {{ FILESYSTEM }}/git/{{ simulation_repo_name }}/runs/{{ simulation_folder_prefix }}_{{ dag_run.conf.model }}_{{ dag_run.conf.start }}_{{ dag_run.conf.end }}:/job '
                 '--rm '
                 '{{ docker }} '
                 '-p {{ number_of_cores(task_instance, dag_run.conf.cores) }} '
                 '-r "{{ params.restart }}/{{ dag_run.conf.model }}/tri-rst.Simulation_Web_rst.{{ dag_run.conf.start }}.000000"',
    params={'restart': 's3://alplakes-eawag/simulations/delft3d-flow/restart-files/',
            'AWS_ID': Variable.get("AWS_ACCESS_KEY_ID"),
            'AWS_KEY': Variable.get("AWS_SECRET_ACCESS_KEY")},
    on_failure_callback=report_failure,
    dag=dag,
)

standardise_simulation_output = BashOperator(
    task_id='standardise_simulation_output',
    bash_command="cd {{ filesystem }}/git/{{ simulation_repo_name }};"
                 "python src/postprocess.py -f {{ filesystem }}/git/{{ simulation_repo_name }}/runs/{{ simulation_folder_prefix }}_{{ dag_run.conf.model }}_{{ dag_run.conf.start }}_{{ dag_run.conf.end }}",
    on_failure_callback=report_failure,
    dag=dag,
)

calculate_parameters = BashOperator(
    task_id='calculate_parameters',
    bash_command="",
    on_failure_callback=report_failure,
    dag=dag,
)

send_results = BashOperator(
    task_id='send_results',
    bash_command="sshpass -p {{ API_PASSWORD }} scp -r "
                 "-o StrictHostKeyChecking=no "
                 "{{ filesystem }}/git/{{ simulation_repo_name }}/runs/{{ simulation_folder_prefix }}_{{ dag_run.conf.model }}_{{ dag_run.conf.start) }}_{{ dag_run.conf.end }}/postprocess/* "
                 "{{ api_user }}@{{ api_server }}:{{ api_server_folder }}",
    on_failure_callback=report_failure,
    dag=dag,
)

prepare_simulation_files >> run_simulation >> standardise_simulation_output >> calculate_parameters >> send_results

