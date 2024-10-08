import json
import requests
from datetime import timedelta, datetime

from airflow.operators.bash import BashOperator
from airflow.operators.python_operator import PythonOperator
from airflow.models import Variable
from airflow.utils.dates import days_ago

from functions.email import report_failure

from airflow import DAG


def update_datalakes_datasets(ids):
    for dataset_id in ids:
        url = "https://api.datalakes-eawag.ch/update/"+str(dataset_id)
        try:
            requests.get(url)
        except Exception as e:
            print(f"Error calling {url}: {e}")


def create_dag(dag_id, parameters):
    default_args = {
        'owner': 'airflow',
        'depends_on_past': False,
        'start_date': days_ago(2),
        'email': ['james.runnalls@eawag.ch'],
        'email_on_failure': False,
        'email_on_retry': False,
        'queue': 'simulation',
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
        description='Reprocess data pipeline.',
        schedule_interval=None if not parameters["schedule_interval"] else parameters["schedule_interval"],
        catchup=False,
        tags=['data pipeline', 'operational'],
        user_defined_macros={'filesystem': '/opt/airflow/filesystem',
                             'FILESYSTEM': Variable.get("FILESYSTEM"),
                             'docker_id': parameters["docker_id"],
                             'repo_https': parameters["repo_https"],
                             'repo_name': parameters["repo_name"]}
    )

    initial_setup = BashOperator(
        task_id='setup_pipeline',
        bash_command='[ -d "{{ filesystem }}/git/{{ repo_name }}" ] && exit 0 || (echo "Repository does not exist, running setup...";'
                     "mkdir -p {{ filesystem }}/git;"
                     "cd {{ filesystem }}/git;"
                     "git clone {{ repo_https }} && cd {{ repo_name }};"
                     "docker build -t {{ docker_id }} .;"
                     "docker run -v {{ FILESYSTEM }}/git/{{ repo_name }}:/repository --rm -t {{ docker_id }} --download)",
        on_failure_callback=report_failure,
        dag=dag,
    )

    run_pipeline = BashOperator(
        task_id='run_pipeline',
        bash_command='docker run -v {{ FILESYSTEM }}/git/{{ repo_name }}:/repository --rm {{ docker_id }} --live',
        on_failure_callback=report_failure,
        dag=dag,
    )

    if parameters["upload"]:
        upload_data = BashOperator(
            task_id='upload_data',
            bash_command='docker run -e AWS_ID={{ params.AWS_ID }} -e AWS_KEY={{ params.AWS_KEY }} '
                         '-v {{ FILESYSTEM }}/git/{{ repo_name }}:/repository --rm {{ docker }} --upload',
            params={
                'AWS_ID': Variable.get("AWS_ACCESS_KEY_ID"),
                'AWS_KEY': Variable.get("AWS_SECRET_ACCESS_KEY")},
            on_failure_callback=report_failure,
            dag=dag,
        )

        update_datalakes = PythonOperator(
            task_id='update_datalakes',
            python_callable=update_datalakes_datasets,
            op_args=[parameters["datalakes_ids"]],
            on_failure_callback=report_failure,
            dag=dag,
        )

        initial_setup >> run_pipeline >> upload_data >> update_datalakes
    else:
        initial_setup >> run_pipeline

    return dag


with open('dags/data_pipelines.json') as f:
    pipelines = json.load(f)

for pipeline in pipelines:
    dag_id = "data_pipelines_reprocess_" + pipeline["id"]
    globals()[dag_id] = create_dag(dag_id, pipeline)
