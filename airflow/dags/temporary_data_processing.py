from datetime import timedelta, datetime
import netCDF4
import os
import numpy as np

from airflow.operators.bash import BashOperator
from airflow.operators.python_operator import PythonOperator
from airflow.models import Variable
from airflow.utils.dates import days_ago

from functions.email import report_failure
from functions.simulate import parse_profile, format_simulation_directory, upload_pickup

from airflow import DAG

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
    'priority_weight': 5,
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

def edit_files(ds, **kwargs):
    folder = kwargs["folder"]
    for file in os.listdir(folder):
        with netCDF4.Dataset(os.path.join(folder, file), mode="a") as nc:
            t = np.array(nc.variables["t"][:])
            u = np.array(nc.variables["u"][:])
            v = np.array(nc.variables["v"][:])
            w = np.array(nc.variables["w"][:])
            thermocline = np.array(nc.variables["thermocline"][:])

            mask = (t == 0) | np.isnan(t)
            t[mask] = -999.0
            u[mask] = -999.0
            v[mask] = -999.0
            w[mask] = -999.0
            thermocline[mask[:, 0, :]] = -999.0

            nc.variables["t"][:] = t
            nc.variables["u"][:] = u
            nc.variables["v"][:] = v
            nc.variables["w"][:] = w
            nc.variables["thermocline"][:] = thermocline


dag = DAG(
    'temporary_data_processing',
    default_args=default_args,
    description='Fix mitgcm files.',
    schedule_interval=None,
    catchup=False,
    tags=['simulation', 'on demand'],
)

process = PythonOperator(
        task_id='process_files',
        python_callable=edit_files,
        op_kwargs={"folder": '/opt/airflow/filesystem/media/simulations/mitgcm/results/zurich'},
        queue='api',
        on_failure_callback=report_failure,
        dag=dag,
    )

process
