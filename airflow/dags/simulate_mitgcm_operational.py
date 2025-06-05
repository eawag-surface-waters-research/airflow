import json
from datetime import timedelta, datetime

from airflow.operators.bash import BashOperator
from airflow.operators.python_operator import PythonOperator
from airflow.models import Variable
from airflow.utils.dates import days_ago

from functions.email import report_failure
from functions.simulate import (get_last_sunday, get_end_date, get_today, get_restart, number_of_cores,
                                cache_simulation_data, format_simulation_directory, process_event_notifications, upload_pickup)

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
        'priority_weight': 10,
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
        description='Operational MitGCM simulation.',
        schedule_interval=parameters["schedule_interval"],
        catchup=False,
        tags=['simulation', 'operational'],
        user_defined_macros={'filesystem': '/opt/airflow/filesystem',
                             'FILESYSTEM': Variable.get("FILESYSTEM"),
                             'model': 'mitgcm/' + parameters["simulation_id"],
                             'docker': parameters["docker"],
                             'simulation_folder_prefix': format_simulation_directory(parameters["docker"]),
                             'start': get_last_sunday,
                             'end': get_end_date,
                             'today': get_today,
                             'restart': get_restart,
                             'bucket': 'alplakes-eawag',
                             'api': "http://eaw-alplakes2:8000",
                             'cores': parameters["cores"],
                             'id': parameters["simulation_id"],
                             'simulation_repo_name': "alplakes-simulations",
                             'simulation_repo_https': "https://github.com/eawag-surface-waters-research/alplakes-simulations.git",
                             'api_user': "alplakes",
                             'api_server': 'eaw-alplakes2',
                             'API_PASSWORD': Variable.get("API_PASSWORD"),
                             'api_server_folder': "/nfsmount/filesystem/media/simulations/mitgcm/results/{}".format(
                                 parameters["simulation_id"]),
                             'threads': number_of_cores}
    )

    prepare_simulation_files = BashOperator(
        task_id='prepare_simulation_files',
        bash_command="mkdir -p {{ filesystem }}/git;"
                     "cd {{ filesystem }}/git;"
                     "git clone {{ simulation_repo_https }} && cd {{ simulation_repo_name }} || cd {{ simulation_repo_name }} && git stash && git pull;"
                     "python src/main.py -m {{ model }} -d {{ docker }} -t {{ today(ds) }} -s {{ start(ds) }} -e {{ end(ds) }} -a {{ api }} -th {{ threads }}",
        on_failure_callback=report_failure,
        dag=dag,
    )

    compile_simulation = BashOperator(
        task_id='compile_simulation',
        bash_command='cd {{ FILESYSTEM }}/git/{{ simulation_repo_name }}/runs/{{ simulation_folder_prefix }}_{{ id }}_{{ start(ds) }}_{{ end(ds) }};'
                     'docker build -t {{ docker }}_{{ model }}_{{ threads }} .',
        on_failure_callback=report_failure,
        retries=2,
        retry_delay=timedelta(minutes=2),
        dag=dag,
    )

    run_simulation = BashOperator(
        task_id='run_simulation',
        bash_command='docker run '
                     '-v {{ FILESYSTEM }}/git/{{ simulation_repo_name }}/runs/{{ simulation_folder_prefix }}_{{ id }}_{{ start(ds) }}_{{ end(ds) }}/binary_data:/simulation/binary_data '
                     '-v {{ FILESYSTEM }}/git/{{ simulation_repo_name }}/runs/{{ simulation_folder_prefix }}_{{ id }}_{{ start(ds) }}_{{ end(ds) }}/run_config:/simulation/run_config '
                     '-v {{ FILESYSTEM }}/git/{{ simulation_repo_name }}/runs/{{ simulation_folder_prefix }}_{{ id }}_{{ start(ds) }}_{{ end(ds) }}/run:/simulation/run '
                     '--rm '
                     '{{ docker }}_{{ model }}_{{ threads }} ',
        on_failure_callback=report_failure,
        retries=2,
        retry_delay=timedelta(minutes=2),
        dag=dag,
    )

    postprocess_simulation_output = BashOperator(
        task_id='postprocess_simulation_output',
        bash_command="cd {{ filesystem }}/git/{{ simulation_repo_name }};"
                     "python src/postprocess.py -f {{ filesystem }}/git/{{ simulation_repo_name }}/runs/{{ simulation_folder_prefix }}_{{ id }}_{{ start(ds) }}_{{ end(ds) }} -d {{ docker }}",
        on_failure_callback=report_failure,
        dag=dag,
    )

    upload_pickup_files = PythonOperator(
        task_id='upload_pickup_files',
        python_callable=upload_pickup,
        op_kwargs={"lake": parameters["simulation_id"],
                   "model": "mitgcm",
                   "bucket": "https://alplakes-eawag.s3.eu-central-1.amazonaws.com",
                   "folder": "{{ filesystem }}/git/{{ simulation_repo_name }}/runs/{{ simulation_folder_prefix }}_{{ id }}_{{ start(ds) }}_{{ end(ds) }}",
                   'AWS_ID': Variable.get("AWS_ACCESS_KEY_ID"),
                   'AWS_KEY': Variable.get("AWS_SECRET_ACCESS_KEY")},
        on_failure_callback=report_failure,
        dag=dag,
    )

    events_simulation_output = BashOperator(
        task_id='events_simulation_output',
        bash_command="cd {{ filesystem }}/git/{{ simulation_repo_name }};"
                     "python src/events.py -f {{ filesystem }}/git/{{ simulation_repo_name }}/runs/{{ simulation_folder_prefix }}_{{ id }}_{{ start(ds) }}_{{ end(ds) }} -d {{ docker }}",
        on_failure_callback=report_failure,
        dag=dag,
    )

    event_notifications = PythonOperator(
        task_id='event_notifications',
        python_callable=process_event_notifications,
        op_kwargs={"lake": parameters["simulation_id"],
                   "name": parameters["name"],
                   "model": "mitgcm",
                   "email_list": ["james.runnalls@eawag.ch", "damien.bouffard@eawag.ch", "anne.leroquais@eawag.ch"],
                   "bucket": "https://alplakes-eawag.s3.eu-central-1.amazonaws.com",
                   "folder": "{{ filesystem }}/git/{{ simulation_repo_name }}/runs/{{ simulation_folder_prefix }}_{{ id }}_{{ start(ds) }}_{{ end(ds) }}",
                   'AWS_ID': Variable.get("AWS_ACCESS_KEY_ID"),
                   'AWS_KEY': Variable.get("AWS_SECRET_ACCESS_KEY")},
        on_failure_callback=report_failure,
        dag=dag,
    )

    send_results = BashOperator(
        task_id='send_results',
        bash_command="sshpass -p {{ API_PASSWORD }} scp -r "
                     "-o StrictHostKeyChecking=no "
                     "{{ filesystem }}/git/{{ simulation_repo_name }}/runs/{{ simulation_folder_prefix }}_{{ id }}_{{ start(ds) }}_{{ end(ds) }}/postprocess/* "
                     "{{ api_user }}@{{ api_server }}:{{ api_server_folder }}",
        on_failure_callback=report_failure,
        dag=dag,
    )

    remove_results = BashOperator(
        task_id='remove_results',
        bash_command="rm -rf {{ filesystem }}/git/{{ simulation_repo_name }}/runs/{{ simulation_folder_prefix }}_{{ id }}_{{ start(ds) }}_{{ end(ds) }}",
        on_failure_callback=report_failure,
        dag=dag,
    )

    """cache_data = PythonOperator(
        task_id='cache_data',
        python_callable=cache_simulation_data,
        op_kwargs={"lake": parameters["simulation_id"],
                   "model": "mitgcm",
                   "bucket": "https://alplakes-eawag.s3.eu-central-1.amazonaws.com",
                   "api": "https://alplakes-api.eawag.ch",
                   'AWS_ID': Variable.get("AWS_ACCESS_KEY_ID"),
                   'AWS_KEY': Variable.get("AWS_SECRET_ACCESS_KEY")},
        on_failure_callback=report_failure,
        dag=dag,
    )"""

    prepare_simulation_files >> compile_simulation >> run_simulation >> postprocess_simulation_output >> upload_pickup_files >> events_simulation_output >> event_notifications >> send_results >> remove_results

    return dag


with open('dags/simulate_mitgcm_operational.json') as f:
    simulations = json.load(f)

for simulation in simulations:
    dag_id = "simulate_mitgcm_operational_" + simulation["simulation_id"]
    globals()[dag_id] = create_dag(dag_id, simulation)
