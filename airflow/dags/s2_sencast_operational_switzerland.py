from datetime import timedelta

from airflow.operators.bash import BashOperator
from airflow.operators.python_operator import PythonOperator
from airflow.models import Variable
from airflow.utils.dates import days_ago

from functions.email import report_failure
from functions.simulate import get_last_sunday, get_end_date, get_today, get_restart, post_notify_api

from airflow import DAG

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': days_ago(2),
    'email': ['james.runnalls@eawag.ch'],
    'email_on_failure': True,
    'email_on_retry': False,
    'queue': 'api',
    # 'retries': 1,
    # 'retry_delay': timedelta(minutes=5),
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
    's2_sencast_operational_switzerland',
    default_args=default_args,
    description='Process S2 data for Switzerland.',
    schedule_interval=None,
    catchup=False,
    tags=['remote sensing', 'operational'],
    user_defined_macros={'model': 'delft3d-flow/biel',
                         'docker': 'eawag/delft3d-flow:6.03.00.62434',
                         'start': get_last_sunday,
                         'end': get_end_date,
                         'today': get_today,
                         'restart': get_restart,
                         'bucket': 'alplakes-eawag',
                         'api': "http://eaw-alplakes2:8000",
                         'cores': 5,
                         'upload': True}
)

run_sencast = BashOperator(
    task_id='run_sencast',
    bash_command='docker run -e AWS_ID={{ params.AWS_ID }} -e AWS_KEY={{ params.AWS_KEY }} {{ docker }} '
                 '-d "{{ params.download }}_{{ start(ds) }}_{{ end(ds) }}.zip" '
                 '-n "{{ params.netcdf }}_{{ start(ds) }}.nc" '
                 '-p {{ cores }} '
                 '-r "{{ params.restart }}{{ restart(ds) }}.000000"',
    params={'download': "https://alplakes-eawag.s3.eu-central-1.amazonaws.com/simulations/sencast/simulation"
                        "-files/eawag_delft3dflow6030062434_delft3dflow_geneva",
            'netcdf': 's3://alplakes-eawag/simulations/delft3d-flow/results/eawag_delft3dflow6030062434_delft3dflow_geneva',
            'restart': 's3://alplakes-eawag/simulations/delft3d-flow/restart-files/geneva/tri-rst.Simulation_Web_rst.',
            'AWS_ID': Variable.get("AWS_ACCESS_KEY_ID"),
            'AWS_KEY': Variable.get("AWS_SECRET_ACCESS_KEY")},
    on_failure_callback=report_failure,
    dag=dag,
)

notify_api = PythonOperator(
    task_id='notify_api',
    python_callable=post_notify_api,
    params={
        "file": "https://alplakes-eawag.s3.eu-central-1.amazonaws.com/simulations/delft3d-flow/results/eawag_delft3dflow6030062434_delft3dflow_geneva",
        "api": "http://eaw-alplakes2:8000",
        "model": "delft3d-flow"},
    provide_context=True,
    on_failure_callback=report_failure,
    dag=dag,
)

run_sencast >> notify_api
