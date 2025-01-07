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
    'Sencast metadata',
    default_args=default_args,
    description=f'Generate metadata and cropped tiff files for Sencast operational pipelines',
    schedule_interval="0 6 * * *",
    catchup=False,
    tags=['sencast', 'operational'],
    user_defined_macros={'git_repos': '/opt/airflow/filesystem/git',
                         'git_name': 'sencast-metadata',
                         'docker': 'eawag/sencast-metadata:1.0.0',
                         'lake_geometry': 'https://eawagrs.s3.eu-central-1.amazonaws.com/metadata/lakes.json',
                         'git_remote': 'https://github.com/eawag-surface-waters-research/sencast-metadata.git',
                         'FILESYSTEM': Variable.get("FILESYSTEM")}
)


clone_repo = BashOperator(
    task_id='clone_repo',
    bash_command="mkdir -p {{ DIAS }}; mkdir -p {{ git_repos }}; cd {{ git_repos }}; "
                 "git clone --depth 1 {{ git_remote }} || "
                 "(cd {{ git_name }} ; git config --global --add safe.directory '*' ; git stash ; git pull)",
    on_failure_callback=report_failure,
    dag=dag,
)

build_docker = BashOperator(
    task_id='build_docker',
    bash_command="""
        IMAGE_NAME="your-docker-image-name"
        IMAGE_TAG="your-tag"

        if ! docker images {{ docker }} --format '{{.Repository}}:{{.Tag}}' | grep -q {{ docker }}; then
            echo "Docker image {{ docker }} does not exist. Building..."
            docker build -t {{ docker }} {{ git_repos }}/{{ git_name }}
        else
            echo "Docker image {{ docker }} already exists."
        fi
        """,
    on_failure_callback=report_failure,
    dag=dag,
)

metadata_products = [{"product": "sentinel2",
                      "remote_tiff": "s3://eawagrs/datalakes/alplakes/S2"},
                     {"product": "sentinel3",
                      "remote_tiff": "datalakes/sui/S3"},
                     {"product": "collection",
                      "remote_tiff": "s3://eawagrs/datalakes/collection/alplakes"}]

for mp in metadata_products:
    with TaskGroup(group_id="metadata_{}".format(mp["product"])) as task_group:
        create_metadata = BashOperator(
        task_id='create_metadata',
        bash_command='docker run '
                     '-e AWS_ACCESS_KEY_ID={{ params.AWS_ID }} '
                     '-e AWS_SECRET_ACCESS_KEY={{ params.AWS_KEY }} '
                     '-v {{ params.local_tiff }}:/local_tiff '
                     '-v {{ params.local_tiff_cropped }}:/local_tiff_cropped '
                     '-v {{ params.local_metadata }}:/local_metadata '
                     '--rm '
                     '{{ docker }} '
                     '-u'
                     '-rt {{ params.remote_tiff }} '
                     '-rtc {{ params.remote_tiff_cropped }}'
                     '-g {{ lake_geometry }}'
                     '-rm {{ params.remote_metadata }}',
        params={
            'AWS_ID': Variable.get("AWS_ACCESS_KEY_ID"),
            'AWS_KEY': Variable.get("AWS_SECRET_ACCESS_KEY"),
            'local_tiff': Variable.get("FILESYSTEM") + "/media/remotesensing/local_tiff/" + mp["product"],
            'local_tiff_cropped': Variable.get("FILESYSTEM") + "/media/remotesensing/local_tiff_cropped/" + mp["product"],
            'local_metadata': Variable.get("FILESYSTEM") + "/media/remotesensing/local_metadata/" + mp["product"],
            'remote_tiff': mp["remote_tiff"],
            'remote_tiff_cropped': "s3://eawagrs/alplakes/cropped/" + mp["product"],
            'remote_metadata': "s3://eawagrs/alplakes/metadata/" + mp["product"],
        },
        on_failure_callback=report_failure,
        retries=1,
        retry_delay=timedelta(minutes=30),
        dag=dag,
        )

        clone_repo >> build_docker >> create_metadata
