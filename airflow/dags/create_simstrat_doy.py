from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago

from functions.email import report_failure
from functions.simstrat import create_simstrat_doy

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
    'create_simstrat_doy',
    default_args=default_args,
    description='Create doy files for simstrat',
    schedule_interval="0 0 2 1 *",
    catchup=False,
    tags=['simstrat', 'doy'],
)

python_create_simstrat_doy = PythonOperator(
    task_id='python_create_simstrat_doy',
    python_callable=create_simstrat_doy,
    op_kwargs={'depths': ["min", "max"],
               "parameters": ["T"],
               "api": "https://alplakes-api.eawag.ch"},
    on_failure_callback=report_failure,
    dag=dag,
)

python_create_simstrat_doy
