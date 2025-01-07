from datetime import datetime, timedelta

from airflow.operators.bash import BashOperator
from airflow.operators.python_operator import PythonOperator
from airflow.models import Variable
from airflow.utils.task_group import TaskGroup

from functions.email import report_failure
from functions.sencast import write_logical_date_and_tile_to_parameter_file
from functions.simulate import generate_days_ago_function

from airflow import DAG

def create_dag(dag_id, prefix, run_date, tiles, schedule_interval, download_pool="default_pool", run_pool="default_pool"):
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
        dag_id,
        default_args=default_args,
        description=f'Process {prefix} data.',
        schedule_interval=schedule_interval,
        catchup=False,
        tags=['sencast', 'operational'],
        user_defined_macros={'docker': 'eawag/sencast:0.0.2',
                             'DIAS': '/opt/airflow/filesystem/DIAS',
                             'git_repos': '/opt/airflow/filesystem/git',
                             'git_name': 'sencast',
                             'git_remote': 'https://github.com/eawag-surface-waters-research/sencast.git',
                             'environment_file': 'docker.ini',
                             'wkt': "alpinespace",
                             'prefix': prefix,
                             'run_date': run_date,
                             'FILESYSTEM': Variable.get("FILESYSTEM")}
    )

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
                write_parameter_files = PythonOperator(
                    task_id='write_parameter_files',
                    python_callable=write_logical_date_and_tile_to_parameter_file,
                    on_failure_callback=report_failure,
                    op_kwargs={"folder": f'/opt/airflow/filesystem/git/sencast/parameters',
                               "prefix": prefix,
                               "tile": tile,
                               "run_date": run_date},
                    dag=dag,
                )

                download_products = BashOperator(
                    task_id='download_products',
                    bash_command='docker run '
                                 '-v {{ FILESYSTEM }}/DIAS:/DIAS '
                                 '-v {{ FILESYSTEM }}/git/{{ git_name }}:/sencast '
                                 '--rm '
                                 '-i {{ docker }} -e {{ environment_file }} -p {{ prefix }}_{{ run_date(ds) }}_' + tile + '_download.ini',
                    pool=download_pool,
                    on_failure_callback=report_failure,
                    dag=dag,
                )

                run_sencast = BashOperator(
                    task_id='run_sencast',
                    bash_command='docker run '
                                 '-v {{ FILESYSTEM }}/DIAS:/DIAS '
                                 '-v {{ FILESYSTEM }}/git/{{ git_name }}:/sencast '
                                 '--rm '
                                 '-i {{ docker }} -e {{ environment_file }} -p {{ prefix }}_{{ run_date(ds) }}_' + tile + '.ini',
                    pool=run_pool,
                    on_failure_callback=report_failure,
                    dag=dag,
                )

                remove_results = BashOperator(
                    task_id='remove_results',
                    bash_command='f="{{ DIAS }}/output_data/{{ prefix }}_{{ run_date(ds) }}_' + tile + '_{{ wkt }}_{{ run_date(ds,"%Y-%m-%d") }}_{{ run_date(ds,"%Y-%m-%d") }}"; [ -d "$f" ] && '
                                 'rm -rf "$f" || echo "Folder does not exist."',
                    on_failure_callback=report_failure,
                    dag=dag,
                )

                remove_parameter_file = BashOperator(
                    task_id='remove_parameter_file',
                    bash_command='rm -rf {{ git_repos }}/{{ git_name }}/parameters/{{ prefix }}_{{ run_date(ds) }}_' + tile + '*',
                    on_failure_callback=report_failure,
                    dag=dag,
                )

                clone_repo >> write_parameter_files >> download_products >> run_sencast >> remove_results >> remove_parameter_file

    return dag


# Collection 2 Products
collection_tiles = ["193027", "193028", "194027", "194028", "195027", "195028", "196027", "196028"]
for days_ago in [4, 14]:
    dag_id = f"sencast_alpinespace_collection_{days_ago}"
    globals()[dag_id] = create_dag(dag_id,
                                   "alplakes_collection",
                                   generate_days_ago_function(days_ago),
                                   collection_tiles,
                                   "0 1 * * *",
                                   download_pool="EROS",
                                   run_pool="collection")

# Sentinel 2 Products
sentinel2_tiles = ["T31TGL","T31TGM","T31TGN","T32TLR","T32TLS","T32TLT","T32TMR","T32TMS","T32TMT","T32TNR","T32TNS","T32TNT","T32TPS","T32TPR"]
for days_ago in [2]:
    dag_id = f"sencast_alpinespace_sentinel2_{days_ago}"
    globals()[dag_id] = create_dag(dag_id,
                                   "alplakes_s2",
                                   generate_days_ago_function(days_ago),
                                   sentinel2_tiles,
                                   "0 2 * * *",
                                   download_pool="COAH",
                                   run_pool="sentinel2")