from datetime import datetime, timedelta

from airflow.operators.python import PythonOperator
from airflow.models import Variable
from functions.email import report_failure
from functions.swot import cache_swot_data

from airflow import DAG

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email': ['james.runnalls@eawag.ch'],
    'email_on_failure': False,
    'email_on_retry': False,
    'queue': 'api',
    'retries': 2,
    'retry_delay': timedelta(minutes=10),
    # 'pool': 'backfill',
    'priority_weight': 20,
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
    'cache_swot_water_level',
    default_args=default_args,
    description='Cache SWOT satellite water levels for each lake.',
    schedule="0 1 * * 0",
    catchup=False,
    tags=['api', 'operational'],
)

cache_swot = PythonOperator(
    task_id='cache_swot',
    python_callable=cache_swot_data,
    op_kwargs={'bucket': 'https://alplakes-eawag.s3.eu-central-1.amazonaws.com',
               'prefix': 'swot',
               'AWS_ID': Variable.get("AWS_ACCESS_KEY_ID"),
               'AWS_KEY': Variable.get("AWS_SECRET_ACCESS_KEY")},
    on_failure_callback=report_failure,
    dag=dag,
)
