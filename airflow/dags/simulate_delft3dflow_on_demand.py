from datetime import timedelta, datetime

from airflow.operators.bash import BashOperator
from airflow.operators.python_operator import PythonOperator
from airflow.models import Variable
from airflow.utils.dates import days_ago

from functions.email import report_failure
from functions.simulate import parse_profile, upload_restart_files

from airflow import DAG

"""
Example config input
{ "lake": "zurich",
  "profile": "20190109",
  "start": "20190109",
  "end": "20190110",
  "cores": 5
}
"""

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
    description='On Demand Delft3D-Flow simulation.',
    schedule_interval=None,
    catchup=False,
    tags=['simulation', 'on demand'],
    user_defined_macros={'filesystem': '/opt/airflow/filesystem',
                         'FILESYSTEM': Variable.get("FILESYSTEM"),
                         'docker': 'eawag/delft3d-flow:6.02.10.142612',
                         'simulation_folder_prefix': 'eawag_delft3dflow60210142612_delft3dflow',
                         'bucket': 'alplakes-eawag',
                         'parse_profile': parse_profile,
                         'api': "http://eaw-alplakes2:8000", # Remote: http://eaw-alplakes2:8000, Local: http://172.17.0.1:8000
                         'simulation_repo_name': "alplakes-simulations",
                         'simulation_repo_https': "https://github.com/eawag-surface-waters-research/alplakes-simulations.git",
                         'api_user': "alplakes",
                         'api_server': 'eaw-alplakes2',
                         'API_PASSWORD': Variable.get("API_PASSWORD"),
                         'api_server_folder': "/nfsmount/filesystem/media/simulations/delft3d-flow/results",
                         }
)

prepare_simulation_files = BashOperator(
    task_id='prepare_simulation_files',
    bash_command="mkdir -p {{ filesystem }}/git;"
                 "cd {{ filesystem }}/git;"
                 "git clone {{ simulation_repo_https }} && cd {{ simulation_repo_name }} || cd {{ simulation_repo_name }} && git stash && git pull;"
                 "python src/main.py -m delft3d-flow/{{ dag_run.conf.lake }} -d {{ docker }} -s {{ dag_run.conf.start }} -e {{ dag_run.conf.end }} -a {{ api }} {{ parse_profile(dag_run.conf.profile) }}",
    on_failure_callback=report_failure,
    dag=dag,
)

run_simulation = BashOperator(
    task_id='run_simulation',
    bash_command='docker run '
                 '-v {{ FILESYSTEM }}/git/{{ simulation_repo_name }}/runs/{{ simulation_folder_prefix }}_{{ dag_run.conf.lake }}_{{ dag_run.conf.start }}_{{ dag_run.conf.end }}:/job '
                 '--rm '
                 '{{ docker }} '
                 '-p {{ dag_run.conf.cores }} ',
    on_failure_callback=report_failure,
    dag=dag,
)

postprocess_simulation_output = BashOperator(
    task_id='postprocess_simulation_output',
    bash_command="cd {{ filesystem }}/git/{{ simulation_repo_name }};"
                 "python src/postprocess.py -f {{ filesystem }}/git/{{ simulation_repo_name }}/runs/{{ simulation_folder_prefix }}_{{ dag_run.conf.lake }}_{{ dag_run.conf.start }}_{{ dag_run.conf.end }} -d {{ docker }}",
    on_failure_callback=report_failure,
    dag=dag,
)

process_restart_files = PythonOperator(
        task_id='process_restart_files',
        python_callable=upload_restart_files,
        op_kwargs={"model": "delft3d-flow",
                   "bucket": "alplakes-eawag",
                   'AWS_ID': Variable.get("AWS_ACCESS_KEY_ID"),
                   'AWS_KEY': Variable.get("AWS_SECRET_ACCESS_KEY"),
                   'restart': 'simulations/delft3d-flow/restart-files/{}_new/tri-rst.Simulation_Web_rst.{}.000000',
                   'filesystem': '{{ filesystem }}',
                   'simulation_repo_name': '{{ simulation_repo_name }}',
                   'simulation_folder_prefix': '{{ simulation_folder_prefix }}'},
        on_failure_callback=report_failure,
        provide_context=True,
        dag=dag,
    )

send_results = BashOperator(
        task_id='send_results',
        bash_command="sshpass -p {{ API_PASSWORD }} scp -r "
                     "-o StrictHostKeyChecking=no "
                     "{{ filesystem }}/git/{{ simulation_repo_name }}/runs/{{ simulation_folder_prefix }}_{{ dag_run.conf.lake }}_{{ dag_run.conf.start }}_{{ dag_run.conf.end }}/postprocess/* "
                     "{{ api_user }}@{{ api_server }}:{{ api_server_folder }}/{{ dag_run.conf.lake }}_new",
        on_failure_callback=report_failure,
        dag=dag,
    )

remove_results = BashOperator(
    task_id='remove_results',
    bash_command="rm -rf {{ filesystem }}/git/{{ simulation_repo_name }}/runs/{{ simulation_folder_prefix }}_{{ dag_run.conf.lake }}_{{ dag_run.conf.start }}_{{ dag_run.conf.end }}",
    on_failure_callback=report_failure,
    dag=dag,
)

prepare_simulation_files >> run_simulation >> postprocess_simulation_output >> process_restart_files >> send_results >> remove_results
