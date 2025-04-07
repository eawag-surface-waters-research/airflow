import json
from datetime import timedelta

from airflow.operators.bash import BashOperator
from airflow.operators.python_operator import PythonOperator
from airflow.models import Variable
from airflow.utils.dates import days_ago

from airflow import DAG

"""
Example config input
{ "id": "lexplore_meteostation" }
"""

def create_credentials(credential_id, credential_keys):
    credentials = {}
    for key in credential_keys:
        credentials[key] = Variable.get(credential_id + "_" + key)
    return json.dumps(credentials).replace('"', '\\"')


def select_config(**kwargs):
    id = kwargs['dag_run'].conf.get('id')
    with open('dags/data_pipelines.json') as f:
        config = json.load(f)
    selected = next((c for c in config if c["id"] == id), None)
    if selected is None:
        raise ValueError("Cannot locate data pipeline with id: {}".format(id))
    kwargs['ti'].xcom_push(key='repo_name', value=selected["repo_name"])
    kwargs['ti'].xcom_push(key='docker_id', value=selected["docker_id"])
    kwargs['ti'].xcom_push(key='creds', value=create_credentials(selected["credential_id"], selected["credential_keys"]))


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': days_ago(2),
    'email': "james.runnalls@eawag.ch",
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
    "data_pipelines_update",
    default_args=default_args,
    description='Update data pipeline code and environment.',
    schedule_interval=None,
    catchup=False,
    tags=['data pipeline', 'update'],
    user_defined_macros={'filesystem': '/opt/airflow/filesystem',
                         'FILESYSTEM': Variable.get("FILESYSTEM")}
)

get_config = PythonOperator(
    task_id='get_config',
    python_callable=select_config,
    dag=dag
)

update_setup = BashOperator(
    task_id='setup_pipeline',
    bash_command="cd {{ filesystem }}/git/{{ ti.xcom_pull(task_ids='get_config', key='repo_name') }};"
                 "git stash; git pull; cd ..;"
                 "chmod -R 777 {{ ti.xcom_pull(task_ids='get_config', key='repo_name') }};"
                 "cd {{ ti.xcom_pull(task_ids='get_config', key='repo_name') }};"
                 'echo "{{ ti.xcom_pull(task_ids="get_config", key="creds") }}" > creds.json;'
                 "docker build -t {{ ti.xcom_pull(task_ids='get_config', key='docker_id') }} .",
    dag=dag,
)

get_config >> update_setup
