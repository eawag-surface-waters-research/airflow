import json
from datetime import timedelta

from airflow.operators.bash import BashOperator
from airflow.operators.python_operator import PythonOperator
from airflow.models import Variable
from airflow.utils.dates import days_ago

from functions.email import report_failure, report_success

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
    kwargs['ti'].xcom_push(key='docker_id', value=selected["docker_id"])
    kwargs['ti'].xcom_push(key='docker_ids', value=",".join([str(d["id"]) for d in selected["datalakes"]]))
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
    "data_pipelines_reprocess",
    default_args=default_args,
    description='Reprocess data pipeline',
    schedule_interval=None,
    catchup=False,
    tags=['data pipeline', 'reprocess'],
    user_defined_macros={'filesystem': '/opt/airflow/filesystem',
                         'FILESYSTEM': Variable.get("FILESYSTEM")}
)

get_config = PythonOperator(
    task_id='get_config',
    python_callable=select_config,
    dag=dag
)

download_sync = BashOperator(
    task_id='download_sync',
    bash_command='docker run -e AWS_ACCESS_KEY_ID={{ params.AWS_ID }} -e AWS_SECRET_ACCESS_KEY={{ params.AWS_KEY }} '
                 '-v {{ FILESYSTEM }}/git/{{ ti.xcom_pull(task_ids="get_config", key="repo_name") }}:/repository --rm {{ ti.xcom_pull(task_ids="get_config", key="docker_id") }} -d',
    params={
        'AWS_ID': Variable.get("AWS_ACCESS_KEY_ID"),
        'AWS_KEY': Variable.get("AWS_SECRET_ACCESS_KEY")},
    on_failure_callback=report_failure,
    dag=dag,
)

remove_folders = BashOperator(
    task_id='remove_folders',
    bash_command="rm -rf {{ filesystem }}/git/{{ ti.xcom_pull(task_ids='get_config', key='repo_name') }}/data/Level1*;"
                 "rm -rf {{ filesystem }}/git/{{ ti.xcom_pull(task_ids='get_config', key='repo_name') }}/data/Level2*",
    dag=dag,
)

run_reprocess = BashOperator(
    task_id='run_reprocess',
    bash_command='docker run -e AWS_ACCESS_KEY_ID={{ params.AWS_ID }} -e AWS_SECRET_ACCESS_KEY={{ params.AWS_KEY }} '
                 '-v {{ FILESYSTEM }}/git/{{ ti.xcom_pull(task_ids="get_config", key="repo_name") }}:/repository --rm {{ ti.xcom_pull(task_ids="get_config", key="docker_id") }} -p -r -u -dl {{ ti.xcom_pull(task_ids="get_config", key="docker_ids") }}',
    params={
        'AWS_ID': Variable.get("AWS_ACCESS_KEY_ID"),
        'AWS_KEY': Variable.get("AWS_SECRET_ACCESS_KEY")},
    on_failure_callback=report_failure,
    on_success_callback=report_success,
    dag=dag,
)

get_config >> download_sync >> remove_folders >> run_reprocess
