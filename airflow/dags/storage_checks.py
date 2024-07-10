from datetime import timedelta
import logging
import subprocess

from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago

from functions.email import report_failure

from airflow import DAG

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': days_ago(2),
    'email': ['james.runnalls@eawag.ch'],
    'email_on_failure': False,
    'email_on_retry': False,
    # 'retries': 1,
    # 'retry_delay': timedelta(minutes=5),
    # 'queue': 'api',
    # 'pool': 'backfill',
    # 'priority_weight': 10,
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
    'storage_checks',
    default_args=default_args,
    description='Check for sufficient storage.',
    schedule_interval='0 */3 * * *',
    catchup=False,
    tags=['storage', 'monitoring'],
)


def check_disk_space(ds, **kwargs):
    drives = kwargs["drives"]
    allowable = kwargs["allowable"]
    result = subprocess.run(['df', '-h'], stdout=subprocess.PIPE)
    lines = result.stdout.decode('utf-8').split('\n')
    for line in lines:
        for drive in drives:
            if line.startswith(drive):
                parts = line.split()
                if len(parts) > 4:
                    p = int(parts[4][:-1])
                    if p > allowable:
                        raise ValueError("Allowable usage capacity exceeded on {} ({}%)".format(parts[0], p))
                    else:
                        print("Acceptable usage capacity on {} ({}%)".format(parts[0], p))


check_api_storage = PythonOperator(
        task_id='check_api_storage',
        python_callable=check_disk_space,
        op_kwargs={"drives": ["/dev/mapper", "172"], "allowable": 75},
        queue='api',
        on_failure_callback=report_failure,
        dag=dag,
    )

check_simulation_storage = PythonOperator(
        task_id='check_simulation_storage',
        python_callable=check_disk_space,
        op_kwargs={"drives": ["/dev/mapper"], "allowable": 75},
        queue='simulation',
        on_failure_callback=report_failure,
        dag=dag,
    )

check_api_storage >> check_simulation_storage
