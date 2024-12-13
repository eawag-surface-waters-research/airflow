from datetime import datetime, timedelta

from airflow.operators.bash import BashOperator
from airflow.operators.python_operator import PythonOperator
from airflow.models import Variable
from airflow.utils.task_group import TaskGroup

from functions.email import report_failure
from functions.sencast import write_logical_date_and_tile_to_parameter_file
from functions.simulate import four_days_ago

from airflow import DAG

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(1982, 1, 1),
    'email': ['james.runnalls@eawag.ch'],
    'email_on_failure': False,
    'email_on_retry': False,
    'queue': 'simulation',
    'retries': 1,
    'retry_delay': timedelta(minutes=60),
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
    'collection_sencast_4days_alpinespace',
    default_args=default_args,
    description='Process Collection data for the Alpine Space.',
    schedule_interval="0 23 * * *",
    catchup=False,
    max_active_runs=5,
    tags=['sencast', 'operational'],
    user_defined_macros={'docker': 'eawag/sencast:0.0.1',
                         'DIAS': '/opt/airflow/filesystem/DIAS',
                         'git_repos': '/opt/airflow/filesystem/git',
                         'git_name': 'sencast',
                         'git_remote': 'https://github.com/eawag-surface-waters-research/sencast.git',
                         'environment_file': 'docker.ini',
                         'wkt': "alpinespace",
                         'prefix': "alplakes_collection",
                         'api_user': "alplakes",
                         'api_server': 'eaw-alplakes2',
                         'run_date': four_days_ago,
                         'API_PASSWORD': Variable.get("API_PASSWORD"),
                         'api_server_folder': "/nfsmount/filesystem/DIAS/output_data",
                         'FILESYSTEM': Variable.get("FILESYSTEM")}
)

tiles = ["193027", "193028", "194027", "194028", "195027", "195028", "196027", "196028"]

with dag:
    clone_repo = BashOperator(
        task_id='clone_repo',
        bash_command="mkdir -p {{ DIAS }}; mkdir -p {{ git_repos }}; cd {{ git_repos }}; "
                     "git clone --depth 1 {{ git_remote }} || "
                     "(cd {{ git_name }} ; git config --global --add safe.directory '*' ; git stash ; git pull)",
        on_failure_callback=report_failure,
        dag=dag,
    )

    for tile in tiles:
        with TaskGroup(group_id=f"tile_{tile}") as task_group:
            write_parameter_file = PythonOperator(
                task_id='write_parameter_file',
                python_callable=write_logical_date_and_tile_to_parameter_file,
                on_failure_callback=report_failure,
                op_kwargs={"file": '/opt/airflow/filesystem/git/sencast/parameters/alplakes_collection.ini',
                           "tile": tile,
                           "offset": -3},
                dag=dag,
            )

            run_sencast = BashOperator(
                task_id='run_sencast',
                bash_command='docker run '
                             '-v {{ FILESYSTEM }}/DIAS:/DIAS '
                             '-v {{ FILESYSTEM }}/git/{{ git_name }}:/sencast '
                             '--rm '
                             '-i {{ docker }} -e {{ environment_file }} -p {{ prefix }}_{{ run_date(ds) }}_' + tile + '.ini',
                on_failure_callback=report_failure,
                dag=dag,
            )

            send_results = BashOperator(
                task_id='send_results',
                bash_command='f="{{ DIAS }}/output_data/{{ prefix }}_{{ run_date(ds) }}_' + tile + '_{{ wkt }}_{{ run_date(ds) }}_{{ run_date(ds) }}"; [ -d "$f" ] && '
                             'sshpass -p {{ API_PASSWORD }} scp -r "$f" {{ api_user }}@{{ api_server }}:{{ api_server_folder }} || '
                             'echo "Folder does not exist."',
                on_failure_callback=report_failure,
                dag=dag,
            )

            remove_results = BashOperator(
                task_id='remove_results',
                bash_command='f="{{ DIAS }}/output_data/{{ prefix }}_{{ run_date(ds) }}_' + tile + '_{{ wkt }}_{{ run_date(ds) }}_{{ run_date(ds) }}"; [ -d "$f" ] && '
                             'rm -rf "$f" || echo "Folder does not exist."',
                on_failure_callback=report_failure,
                dag=dag,
            )

            remove_parameter_file = BashOperator(
                task_id='remove_parameter_file',
                bash_command='rm -f {{ git_repos }}/{{ git_name }}/parameters/{{ prefix }}_{{ run_date(ds) }}_' + tile + '.ini',
                on_failure_callback=report_failure,
                dag=dag,
            )

            clone_repo >> write_parameter_file >> run_sencast >> send_results >> remove_results >> remove_parameter_file
