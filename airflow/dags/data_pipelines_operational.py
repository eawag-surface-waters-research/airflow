import os
import json
import time
import requests
from datetime import timedelta, datetime, timezone

from airflow.operators.bash import BashOperator
from airflow.operators.python_operator import PythonOperator
from airflow.models import Variable
from airflow.utils.dates import days_ago

from functions.email import report_failure

from airflow import DAG


def create_credentials(credential_id, credential_keys):
    credentials = {}
    for key in credential_keys:
        credentials[key] = Variable.get(credential_id + "_" + key)
    return json.dumps(credentials).replace('"', '\\"')


def verify_data_updates(**kwargs):
    datasets = kwargs['datalakes']
    filesystem = kwargs['filesystem']
    repo_name = kwargs['repo_name']
    errors = []
    error_file = os.path.join(filesystem, "git", repo_name, ".error")
    for index, dataset in enumerate(datasets):
        url = "https://api.datalakes-eawag.ch/datasets/" + str(dataset["id"])
        response = requests.get(url)
        if response.status_code != 200:
            continue
        data = response.json()
        last_update = datetime.strptime(data["maxdatetime"], '%Y-%m-%dT%H:%M:%S.%fZ').replace(tzinfo=timezone.utc)
        current_datetime = datetime.now(timezone.utc)
        if last_update > current_datetime:
            errors.append({"dataset": dataset["id"], "message": "Datetime greater than current datetime"})
        elif (current_datetime - last_update).total_seconds() > dataset["failure_notification"] * 3600:
            errors.append({"dataset": dataset["id"], "message": "Dataset out of date"})
    if len(errors) > 0:
        print(errors)
        if os.path.exists(error_file):
            print("Error email already sent")
        else:
            with open(error_file, "w") as file:
                pass
            raise ValueError(errors)
    elif os.path.exists(error_file):
        print("Removed error status")
        os.remove(error_file)


def create_dag(dag_id, parameters):
    default_args = {
        'owner': 'airflow',
        'depends_on_past': False,
        'start_date': days_ago(2),
        'email': parameters["email"],
        'email_on_failure': False,
        'email_on_retry': False,
        'queue': 'api',
        'retries': 0,
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
        description='Operational data pipeline.',
        schedule_interval=None if not parameters["schedule_interval"] else parameters["schedule_interval"],
        max_active_runs=1,
        catchup=False,
        tags=['data pipeline', 'operational'],
        user_defined_macros={'filesystem': '/opt/airflow/filesystem',
                             'FILESYSTEM': Variable.get("FILESYSTEM"),
                             'docker_id': parameters["docker_id"],
                             'repo_https': parameters["repo_https"],
                             'repo_name': parameters["repo_name"],
                             "datalakes_ids": ",".join([str(d["id"]) for d in parameters["datalakes"]])}
    )

    initial_setup = BashOperator(
        task_id='setup_pipeline',
        bash_command='[ -d "{{ filesystem }}/git/{{ repo_name }}" ] && exit 0 || (echo "Repository does not exist, running setup...";'
                     "mkdir -p {{ filesystem }}/git;"
                     "cd {{ filesystem }}/git;"
                     "git clone {{ repo_https }};"
                     "chmod -R 777 {{ repo_name }};"
                     "cd {{ repo_name }};"
                     'echo "{{ params.creds }}" > creds.json;'
                     "docker build -t {{ docker_id }} .)",
        params={'creds': create_credentials(parameters["credential_id"], parameters["credential_keys"])},
        on_failure_callback=report_failure,
        dag=dag,
    )
    
    run_pipeline = BashOperator(
        task_id='run_pipeline',
        bash_command='docker run -e AWS_ACCESS_KEY_ID={{ params.AWS_ID }} -e AWS_SECRET_ACCESS_KEY={{ params.AWS_KEY }} '
                     '-v {{ FILESYSTEM }}/git/{{ repo_name }}:/repository --rm {{ docker_id }} -d -p -uf -dl {{ datalakes_ids }}',
        params={
            'AWS_ID': Variable.get("AWS_ACCESS_KEY_ID"),
            'AWS_KEY': Variable.get("AWS_SECRET_ACCESS_KEY")},
        on_failure_callback=report_failure,
        dag=dag,
    )

    verify_updates = PythonOperator(
        task_id='verify_updates',
        python_callable=verify_data_updates,
        op_kwargs={
            "datalakes": parameters["datalakes"],
            'repo_name': parameters["repo_name"],
            'filesystem': '/opt/airflow/filesystem'
        },
        on_failure_callback=report_failure,
        dag=dag,
    )

    initial_setup >> run_pipeline >> verify_updates

    return dag


with open('dags/data_pipelines.json') as f:
    pipelines = json.load(f)

for pipeline in pipelines:
    try:
        dag_id = "data_pipelines_operational_" + pipeline["id"]
        globals()[dag_id] = create_dag(dag_id, pipeline)
    except Exception as e:
        print(f"Failed to create DAG {pipeline.get('id')}: {e}")
