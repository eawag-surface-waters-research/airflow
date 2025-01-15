from datetime import datetime, timedelta

from airflow.operators.bash import BashOperator
from airflow.models import Variable
from airflow.utils.task_group import TaskGroup

from functions.email import report_failure

from airflow import DAG

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(1982, 1, 1),
    'email': ['james.runnalls@eawag.ch'],
    'email_on_failure': False,
    'email_on_retry': False,
    'queue': 'api',
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
    'sencast_metadata',
    default_args=default_args,
    description=f'Generate metadata and cropped tiff files for Sencast operational pipelines',
    schedule_interval="0 6 * * *",
    catchup=False,
    tags=['sencast', 'operational'],
    user_defined_macros={'git_repos': '/opt/airflow/filesystem/git',
                         'git_name': 'alplakes-sencast-metadata',
                         'docker': 'eawag/sencast-metadata:1.0.0',
                         'lake_geometry': 'https://eawagrs.s3.eu-central-1.amazonaws.com/alplakes/metadata/lakes.geojson',
                         'git_remote': 'https://github.com/eawag-surface-waters-research/alplakes-sencast-metadata.git',
                         'filesystem': '/opt/airflow/filesystem',
                         'FILESYSTEM': Variable.get("FILESYSTEM")}
)

with dag:
    clone_repo = BashOperator(
        task_id='clone_repo',
        bash_command="mkdir -p {{ git_repos }}; cd {{ git_repos }}; "
                     "git clone --depth 1 {{ git_remote }} || "
                     "(cd {{ git_name }} ; git config --global --add safe.directory '*' ; git stash ; git pull)",
        on_failure_callback=report_failure,
        dag=dag,
    )

    build_docker = BashOperator(
        task_id='build_docker',
        bash_command="""
            if docker image inspect {{ docker }} > /dev/null 2>&1; then
                echo "Docker image {{ docker }} already exists locally."
            else
                echo "Docker image {{ docker }} does not exist. Building it now..."
                docker build -t {{ docker }} {{ git_repos }}/{{ git_name }}
            fi
            """,
        on_failure_callback=report_failure,
        dag=dag,
    )

    metadata_products = [{"product": "sentinel2",
                          "remote_tiff": "s3://eawagrs/datalakes/alplakes"},
                         {"product": "sentinel3",
                          "remote_tiff": "s3://eawagrs/datalakes/sui"},
                         {"product": "collection",
                          "remote_tiff": "s3://eawagrs/datalakes/collection/alplakes"}]

    for mp in metadata_products:
        with TaskGroup(group_id="metadata_{}".format(mp["product"])) as task_group:
            create_metadata = BashOperator(
            task_id='create_metadata',
            bash_command='mkdir -p {{ filesystem }}{{ params.local_tiff }}; '
                         'mkdir -p {{ filesystem }}{{ params.local_tiff_cropped }}; '
                         'mkdir -p {{ filesystem }}{{ params.local_metadata }}; '
                         'docker run '
                         '-e AWS_ACCESS_KEY_ID={{ params.AWS_ID }} '
                         '-e AWS_SECRET_ACCESS_KEY={{ params.AWS_KEY }} '
                         '-v {{ FILESYSTEM }}/git/{{ git_name }}:/repository '
                         '-v {{ FILESYSTEM }}{{ params.local_tiff }}:/local_tiff '
                         '-v {{ FILESYSTEM }}{{ params.local_tiff_cropped }}:/local_tiff_cropped '
                         '-v {{ FILESYSTEM }}{{ params.local_metadata }}:/local_metadata '
                         '--rm '
                         '{{ docker }} '
                         '-rt {{ params.remote_tiff }} '
                         '-rtc {{ params.remote_tiff_cropped }} '
                         '-g {{ lake_geometry }} '
                         '-rm {{ params.remote_metadata }} '
                         '-ms {{ params.metadata_summary }} '
                         '-mn {{ params.metadata_name }} '
                         '-u ',
            params={
                'AWS_ID': Variable.get("AWS_ACCESS_KEY_ID"),
                'AWS_KEY': Variable.get("AWS_SECRET_ACCESS_KEY"),
                'local_tiff': "/media/remotesensing/local_tiff/" + mp["product"],
                'local_tiff_cropped': "/media/remotesensing/local_tiff_cropped/" + mp["product"],
                'local_metadata': "/media/remotesensing/local_metadata/" + mp["product"],
                'remote_tiff': mp["remote_tiff"],
                'remote_tiff_cropped': "s3://eawagrs/alplakes/cropped/" + mp["product"],
                'remote_metadata': "s3://eawagrs/alplakes/metadata/" + mp["product"],
                'metadata_summary': "s3://eawagrs/alplakes/metadata/summary.json",
                'metadata_name': mp["product"],
            },
            on_failure_callback=report_failure,
            retries=1,
            retry_delay=timedelta(minutes=30),
            dag=dag,
            )

            clone_repo >> build_docker >> create_metadata
