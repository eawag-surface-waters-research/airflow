from datetime import datetime, timedelta

from airflow.operators.python import PythonOperator
from airflow.models import Variable
from functions.email import report_failure
from functions.general import cache_bodensee_online_data

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
    'download_bodenseeonline',
    default_args=default_args,
    description='Cache surface water temperature from BodenseeOnline.',
    schedule="0 0 * * *",
    catchup=False,
    tags=['api', 'operational'],
)

cache_bodensee_online = PythonOperator(
    task_id='cache_bodensee_online',
    python_callable=cache_bodensee_online_data,
    op_kwargs={'url': 'https://bodenseeonline.lubw.baden-wuerttemberg.de/public_data/bonline_model_data/alplakes/'
                      'bonline_profile_fischbachuttwil_prog.nc',
               'bucket': 'https://alplakes-eawag.s3.eu-central-1.amazonaws.com',
               'key': 'simulations/bodensee/cache/constance.json',
               'depth': 1.0,
               'AWS_ID': Variable.get("AWS_ACCESS_KEY_ID"),
               'AWS_KEY': Variable.get("AWS_SECRET_ACCESS_KEY")},
    on_failure_callback=report_failure,
    dag=dag,
)
