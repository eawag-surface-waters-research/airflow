from datetime import timedelta, datetime

from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.models import Variable
from airflow.models.param import Param

from functions.email import report_failure
from functions.simulate import format_simulation_directory, upload_restart

from airflow import DAG

"""
Example config input
{ "lake": "geneva",
  "start": "20190109",
  "end": "20190110",
  "server_folder": "results_reprocess",
  "docker": "delftwaves/swan:v41.51"
}
"""

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
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
    return format_simulation_directory(docker, "swan")

dag = DAG(
    'simulate_swan_on_demand',
    default_args=default_args,
    description='On Demand SWAN simulation.',
    schedule=None,
    catchup=False,
    tags=['simulation', 'on demand'],
    params={
        "lake": Param("geneva", type="string"),
        "start": Param("20260104", type="string"),
        "end": Param("20260110", type="string"),
        "server_folder": Param("results_reprocess", type="string"),
        "docker": Param("delftwaves/swan:v41.51", type="string"),
    },
    user_defined_macros={'filesystem': '/opt/airflow/filesystem',
                         'FILESYSTEM': Variable.get("FILESYSTEM"),
                         'model': 'swan',
                         'bucket': 'alplakes-eawag',
                         'config_format_simulation_directory': config_format_simulation_directory,
                         'api': "http://eaw-alplakes2:8000", # Remote: http://eaw-alplakes2:8000, Local: http://172.17.0.1:8000
                         'simulation_repo_name': "alplakes-simulations",
                         'simulation_repo_https': "https://github.com/eawag-surface-waters-research/alplakes-simulations.git",
                         'api_user': "alplakes",
                         'api_server': 'eaw-alplakes2',
                         'API_PASSWORD': Variable.get("API_PASSWORD"),
                         'api_server_folder': "/nfsmount/filesystem/media/simulations/swan",
                         }
)

prepare_simulation_files = BashOperator(
    task_id='prepare_simulation_files',
    bash_command="mkdir -p {{ filesystem }}/git;"
                 "cd {{ filesystem }}/git;"
                 "git clone {{ simulation_repo_https }} && cd {{ simulation_repo_name }} || cd {{ simulation_repo_name }} && git stash && git pull;"
                 "python src/main.py -m swan/{{ dag_run.conf.lake }} -d {{ dag_run.conf.docker }} -s {{ dag_run.conf.start }} -e {{ dag_run.conf.end }} -a {{ api }}",
    on_failure_callback=report_failure,
    dag=dag,
)

pull_docker = BashOperator(
    task_id='pull_docker',
    bash_command="""
                    if docker image inspect {{ dag_run.conf.docker }} > /dev/null 2>&1; then
                        echo "Docker image {{ dag_run.conf.docker }} already exists locally."
                    else
                        echo "Docker image {{ dag_run.conf.docker }} does not exist. Trying to pull it now..."
                        docker pull {{ dag_run.conf.docker }}
                    fi
                    """,
    on_failure_callback=report_failure,
    dag=dag,
)

run_simulation = BashOperator(
    task_id='run_simulation',
    bash_command='docker run '
                 '-v {{ FILESYSTEM }}/git/{{ simulation_repo_name }}/runs/{{ config_format_simulation_directory(dag_run.conf.docker) }}_{{ dag_run.conf.lake }}_{{ dag_run.conf.start }}_{{ dag_run.conf.end }}:/home/swan '
                 '--rm '
                 '{{ dag_run.conf.docker }} '
                 'swanrun -input control',
    on_failure_callback=report_failure,
    dag=dag,
)

postprocess_simulation_output = BashOperator(
    task_id='postprocess_simulation_output',
    bash_command="cd {{ filesystem }}/git/{{ simulation_repo_name }};"
                 "python src/postprocess.py -f {{ filesystem }}/git/{{ simulation_repo_name }}/runs/{{ config_format_simulation_directory(dag_run.conf.docker) }}_{{ dag_run.conf.lake }}_{{ dag_run.conf.start }}_{{ dag_run.conf.end }} -d {{ dag_run.conf.docker }}",
    on_failure_callback=report_failure,
    dag=dag,
)

upload_restart_files = PythonOperator(
    task_id='upload_restart_files',
    python_callable=upload_restart,
    op_kwargs={"lake": "{{ dag_run.conf.lake }}",
               "model": "swan",
               "bucket": "https://alplakes-eawag.s3.eu-central-1.amazonaws.com",
               "folder": "{{ filesystem }}/git/{{ simulation_repo_name }}/runs/{{ config_format_simulation_directory(dag_run.conf.docker)  }}_{{ dag_run.conf.lake }}_{{ dag_run.conf.start }}_{{ dag_run.conf.end }}",
               'AWS_ID': Variable.get("AWS_ACCESS_KEY_ID"),
               'AWS_KEY': Variable.get("AWS_SECRET_ACCESS_KEY")},
    on_failure_callback=report_failure,
    dag=dag,
)

send_results = BashOperator(
    task_id='send_results',
    bash_command="sshpass -p {{ API_PASSWORD }} scp -r "
                 "-o StrictHostKeyChecking=no "
                 "{{ filesystem }}/git/{{ simulation_repo_name }}/runs/{{ config_format_simulation_directory(dag_run.conf.docker)  }}_{{ dag_run.conf.lake }}_{{ dag_run.conf.start }}_{{ dag_run.conf.end }}/postprocess/* "
                 "{{ api_user }}@{{ api_server }}:{{ api_server_folder }}/{{ dag_run.conf.server_folder }}/{{ dag_run.conf.lake }}",
    on_failure_callback=report_failure,
    dag=dag,
)

remove_results = BashOperator(
    task_id='remove_results',
    bash_command="rm -rf {{ filesystem }}/git/{{ simulation_repo_name }}/runs/{{ config_format_simulation_directory(dag_run.conf.docker)  }}_{{ dag_run.conf.lake }}_{{ dag_run.conf.start }}_{{ dag_run.conf.end }}",
    on_failure_callback=report_failure,
    dag=dag,
)

prepare_simulation_files >> pull_docker >> run_simulation >> postprocess_simulation_output >> upload_restart_files >> send_results >> remove_results
