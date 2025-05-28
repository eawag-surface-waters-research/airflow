# from __future__ import annotations

import datetime
import os
import pendulum

from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.operators.bash import BashOperator
from airflow.sensors.filesystem import FileSensor

from functions.firecrest import FirecRESTSubmitOperator, FirecRESTUploadOperator, FirecRESTDownloadFromJobOperator


workdir = '/opt/airflow/filesystem/media/firecrest'  # directory on local workstation
remotedir = '/capstor/scratch/cscs/course75'  # directory on remote HPC system
username = 'course75'  # username on the HPC system
system = 'daint'  # HPC system name

job_script = """#!/bin/bash -l

#SBATCH --job-name=airflow-example
#SBATCH --output=slurm-%j.out
#SBATCH --time=00:05:00
#SBATCH --nodes=1

export OMP_NUM_THREADS=$SLURM_CPUS_PER_TASK

uenv repo create
uenv image pull quantumespresso/v7.3.1:v2

srun --uenv quantumespresso/v7.3.1:v2 --view default pw.x -in si.scf.in
"""

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': days_ago(2),
    'email': ['james.runnalls@eawag.ch'],
    'email_on_failure': False,
    'email_on_retry': False,
    # 'retries': 1,
    # 'retry_delay': timedelta(minutes=5),
    'queue': 'api',
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
    'firecrest_test',
    default_args=default_args,
    description='Some description',
    schedule_interval=None,
    catchup=False,
    tags=['firecrest', 'testing'],
)

upload_in = FirecRESTUploadOperator(
    task_id="upload-in",
    system=system,
    source_path=os.path.join(workdir, "structs", "si.scf.in"),
    target_path=remotedir,
dag=dag
)

upload_pp = FirecRESTUploadOperator(
    task_id="upload-pp",
    system=system,
    source_path=os.path.join(workdir, "Si.pz-vbc.UPF"),
    target_path=remotedir,
dag=dag
)

submit_task = FirecRESTSubmitOperator(
    task_id="job-submit",
    system=system,
    script=job_script,
    working_dir=remotedir,
dag=dag
)

download_out_task = FirecRESTDownloadFromJobOperator(
    task_id="download-out",
    system=system,
    remote_files = ["slurm-{_jobid_}.out"],
    local_path=os.path.join(workdir, "output.out"),
    target_task_id="job-submit",
dag=dag
)

download_xml_task = FirecRESTDownloadFromJobOperator(
    task_id="download-xml",
    system=system,
    remote_files = ['output/silicon.xml'],
    local_path=os.path.join(workdir, "silicon.xml"),
    target_task_id="job-submit",
dag=dag
)

log_results = BashOperator(
    task_id="log-results",
    bash_command=(f"echo $(date) '  |  ' "
                  f"$(grep '!    total energy' {workdir}/output.out) >> {workdir}/list.txt"),
dag=dag
)

remove_struct = BashOperator(
    task_id="remove-struct",
    bash_command=(f"rm {workdir}/structs/si.scf.in"),
dag=dag
)

upload_in
upload_pp
upload_in >> submit_task >> download_out_task >> log_results
submit_task >> download_xml_task
upload_pp >> submit_task
upload_in >> remove_struct
