from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago

from functions.email import report_failure
from functions.sencast import create_sencast_operational_metadata

from airflow import DAG

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': days_ago(2),
    'email': ['james.runnalls@eawag.ch'],
    'email_on_failure': False,
    'email_on_retry': False,
    'queue': 'api',
    # 'retries': 1,
    # 'retry_delay': timedelta(minutes=5),
    # 'pool': 'backfill',
    'priority_weight': 8,
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
    'create_sencast_operational_metadata',
    default_args=default_args,
    description='Create metadata files from Sencast output bucket',
    schedule_interval="0 4 * * *",
    catchup=False,
    tags=['sencast', 'metadata'],
)

python_create_metadata = PythonOperator(
    task_id='python_create_metadata',
    python_callable=create_sencast_operational_metadata,
    op_kwargs={'bucket': 'eawagrs',
               'satellites': {"sentinel2": "datalakes/sui/S2",
                              "sentinel3": "datalakes/sui/S3"},
               'filesystem': "/opt/airflow/filesystem"},
    on_failure_callback=report_failure,
    dag=dag,
)

python_create_metadata
