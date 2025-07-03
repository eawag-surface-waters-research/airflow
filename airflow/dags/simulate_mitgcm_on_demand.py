from datetime import timedelta, datetime

from airflow.operators.bash import BashOperator
from airflow.operators.python_operator import PythonOperator
from airflow.models import Variable
from airflow.utils.dates import days_ago

from functions.email import report_failure
from functions.simulate import parse_profile, format_simulation_directory, upload_pickup

from airflow import DAG

"""
Example config input
{ "lake": "zurich",
  "profile": "20190109",
  "start": "20190109",
  "end": "20190110",
  "threads": "5_2",
  "server_folder": "results_reprocess",
  "docker": "eawag/mitgcm:67z"
}
Use "profile": false for restarting from restart files.
"""

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

def config_format_simulation_directory(docker):
    return format_simulation_directory(docker, "mitgcm")

dag = DAG(
    'simulate_mitgcm_on_demand',
    default_args=default_args,
    description='On Demand MitGCM simulation.',
    schedule_interval=None,
    catchup=False,
    tags=['simulation', 'on demand'],
    user_defined_macros={'filesystem': '/opt/airflow/filesystem',
                         'FILESYSTEM': Variable.get("FILESYSTEM"),
                         'model': 'mitgcm',
                         'bucket': 'alplakes-eawag',
                         'parse_profile': parse_profile,
                         'config_format_simulation_directory': config_format_simulation_directory,
                         'api': "http://eaw-alplakes2:8000", # Remote: http://eaw-alplakes2:8000, Local: http://172.17.0.1:8000
                         'simulation_repo_name': "alplakes-simulations",
                         'simulation_repo_https': "https://github.com/eawag-surface-waters-research/alplakes-simulations.git",
                         'api_user': "alplakes",
                         'api_server': 'eaw-alplakes2',
                         'API_PASSWORD': Variable.get("API_PASSWORD"),
                         'api_server_folder': "/nfsmount/filesystem/media/simulations/mitgcm",
                         }
)

prepare_simulation_files = BashOperator(
        task_id='prepare_simulation_files',
        bash_command="mkdir -p {{ filesystem }}/git;"
                     "cd {{ filesystem }}/git;"
                     "git clone {{ simulation_repo_https }} && cd {{ simulation_repo_name }} || cd {{ simulation_repo_name }} && git stash && git pull;"
                     "python src/main.py -m mitgcm/{{ dag_run.conf.lake }} -d {{ dag_run.conf.docker }} -s {{ dag_run.conf.start }} -e {{ dag_run.conf.end }} -a {{ api }} -th {{ dag_run.conf.threads }} {{ parse_profile(dag_run.conf.profile) }};"
                     "chmod -R 777 runs/{{ config_format_simulation_directory(dag_run.conf.docker)  }}_{{ dag_run.conf.lake }}_{{ dag_run.conf.start }}_{{ dag_run.conf.end }}_{{ dag_run.conf.threads }}",
        on_failure_callback=report_failure,
        dag=dag,
    )

compile_simulation = BashOperator(
        task_id='compile_simulation',
        bash_command='cd {{ filesystem }}/git/{{ simulation_repo_name }}/runs/{{ config_format_simulation_directory(dag_run.conf.docker)  }}_{{ dag_run.conf.lake }}_{{ dag_run.conf.start }}_{{ dag_run.conf.end }}_{{ dag_run.conf.threads }};'
                     'docker build -t {{ dag_run.conf.docker }}_mitgcm_{{ dag_run.conf.lake }}_{{ dag_run.conf.threads }} .',
        on_failure_callback=report_failure,
        retries=2,
        retry_delay=timedelta(minutes=2),
        dag=dag,
    )

run_simulation = BashOperator(
    task_id='run_simulation',
    bash_command='docker run '
                 '-v {{ FILESYSTEM }}/git/{{ simulation_repo_name }}/runs/{{ config_format_simulation_directory(dag_run.conf.docker)  }}_{{ dag_run.conf.lake }}_{{ dag_run.conf.start }}_{{ dag_run.conf.end }}_{{ dag_run.conf.threads }}/binary_data:/simulation/binary_data '
                 '-v {{ FILESYSTEM }}/git/{{ simulation_repo_name }}/runs/{{ config_format_simulation_directory(dag_run.conf.docker)  }}_{{ dag_run.conf.lake }}_{{ dag_run.conf.start }}_{{ dag_run.conf.end }}_{{ dag_run.conf.threads }}/run_config:/simulation/run_config '
                 '-v {{ FILESYSTEM }}/git/{{ simulation_repo_name }}/runs/{{ config_format_simulation_directory(dag_run.conf.docker)  }}_{{ dag_run.conf.lake }}_{{ dag_run.conf.start }}_{{ dag_run.conf.end }}_{{ dag_run.conf.threads }}/run:/simulation/run '
                 '--rm '
                 '{{ dag_run.conf.docker }}_mitgcm_{{ dag_run.conf.lake }}_{{ dag_run.conf.threads }} ',
    on_failure_callback=report_failure,
    retries=2,
    retry_delay=timedelta(minutes=2),
    dag=dag,
)

postprocess_simulation_output = BashOperator(
    task_id='postprocess_simulation_output',
    bash_command="cd {{ filesystem }}/git/{{ simulation_repo_name }};"
                 "python src/postprocess.py -f {{ filesystem }}/git/{{ simulation_repo_name }}/runs/{{ config_format_simulation_directory(dag_run.conf.docker)  }}_{{ dag_run.conf.lake }}_{{ dag_run.conf.start }}_{{ dag_run.conf.end }}_{{ dag_run.conf.threads }} -d {{ dag_run.conf.docker }}",
    on_failure_callback=report_failure,
    dag=dag,
)

upload_pickup_files = PythonOperator(
    task_id='upload_pickup_files',
    python_callable=upload_pickup,
    op_kwargs={"lake": "{{ dag_run.conf.lake }}",
               "model": "mitgcm",
               "bucket": "https://alplakes-eawag.s3.eu-central-1.amazonaws.com",
               "folder": "{{ filesystem }}/git/{{ simulation_repo_name }}/runs/{{ config_format_simulation_directory(dag_run.conf.docker)  }}_{{ dag_run.conf.lake }}_{{ dag_run.conf.start }}_{{ dag_run.conf.end }}_{{ dag_run.conf.threads }}",
               'AWS_ID': Variable.get("AWS_ACCESS_KEY_ID"),
               'AWS_KEY': Variable.get("AWS_SECRET_ACCESS_KEY")},
    on_failure_callback=report_failure,
    dag=dag,
)

send_results = BashOperator(
    task_id='send_results',
    bash_command="sshpass -p {{ API_PASSWORD }} scp -r "
                 "-o StrictHostKeyChecking=no "
                 "{{ filesystem }}/git/{{ simulation_repo_name }}/runs/{{ config_format_simulation_directory(dag_run.conf.docker)  }}_{{ dag_run.conf.lake }}_{{ dag_run.conf.start }}_{{ dag_run.conf.end }}_{{ dag_run.conf.threads }}/postprocess/* "
                 "{{ api_user }}@{{ api_server }}:{{ api_server_folder }}/{{ dag_run.conf.server_folder }}/{{ dag_run.conf.lake }}",
    on_failure_callback=report_failure,
    dag=dag,
)

remove_results = BashOperator(
    task_id='remove_results',
    bash_command="rm -rf {{ filesystem }}/git/{{ simulation_repo_name }}/runs/{{ config_format_simulation_directory(dag_run.conf.docker)  }}_{{ dag_run.conf.lake }}_{{ dag_run.conf.start }}_{{ dag_run.conf.end }}_{{ dag_run.conf.threads }}",
    on_failure_callback=report_failure,
    dag=dag,
)

prepare_simulation_files >> compile_simulation >> run_simulation >> postprocess_simulation_output >> upload_pickup_files >> send_results >> remove_results
