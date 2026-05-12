from datetime import datetime, timedelta

from airflow.operators.bash import BashOperator
from airflow.models import Variable
from airflow.models.param import Param

from functions.email import report_failure

from airflow import DAG

"""
Run sencast-metadata for a single user-specified product with fully configurable paths.

Example config input (custom isolated run):
{
  "local_tiff": "/media/remotesensing/sandbox/local_tiff/sentinel2",
  "local_tiff_cropped": "/media/remotesensing/sandbox/local_tiff_cropped/sentinel2",
  "local_metadata": "/media/remotesensing/sandbox/local_metadata/sentinel2",
  "remote_tiff": "s3://eawagrs/datalakes/alplakes",
  "remote_tiff_cropped": "s3://eawagrs/alplakes-sandbox/cropped/sentinel2",
  "remote_metadata": "s3://eawagrs/alplakes-sandbox/metadata/sentinel2",
  "metadata_summary": "s3://eawagrs/alplakes-sandbox/metadata/summary.json",
  "metadata_name": "sentinel2",
  "extra_flags": "-u -r",
  "lakes": "geneva",
  "period": "20240101_20240201"
}
"""

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
    'priority_weight': 8,
}
dag = DAG(
    'sencast_metadata_on_demand',
    default_args=default_args,
    description='Run sencast-metadata for a single user-specified product with fully configurable paths',
    schedule=None,
    catchup=False,
    tags=['sencast', 'on demand'],
    params={
        "local_tiff": Param("/media/remotesensing/local_tiff/sentinel2", type="string",
                            description='Host path (under FILESYSTEM) mounted as /local_tiff in the container.'),
        "local_tiff_cropped": Param("/media/remotesensing/local_tiff_cropped/sentinel2", type="string",
                                    description='Host path (under FILESYSTEM) mounted as /local_tiff_cropped.'),
        "local_metadata": Param("/media/remotesensing/local_metadata/sentinel2", type="string",
                                description='Host path (under FILESYSTEM) mounted as /local_metadata.'),
        "remote_tiff": Param("s3://eawagrs/datalakes/alplakes", type="string",
                             description='Input tiff bucket/prefix (-rt).'),
        "remote_tiff_cropped": Param("s3://eawagrs/alplakes/cropped/sentinel2", type="string",
                                     description='Cropped tiff output bucket/prefix (-rtc).'),
        "remote_metadata": Param("s3://eawagrs/alplakes/metadata/sentinel2", type="string",
                                 description='Metadata output bucket/prefix (-rm).'),
        "metadata_summary": Param("s3://eawagrs/alplakes/metadata/summary.json", type="string",
                                  description='Full S3 path of the summary.json output (-ms).'),
        "metadata_name": Param("sentinel2", type="string",
                               description='Metadata product name (-mn).'),
        "extra_flags": Param("-u -r", type="string",
                             description='Trailing flags appended to the container command. Use "-u" for '
                                         'operational mode or "-u -r" for reprocess.'),
        "lakes": Param("false", type="string",
                       description='Comma-separated lake names (e.g. "zurich,geneva"), or "false" for all (-n).'),
        "period": Param("false", type="string",
                        description='Period as "YYYYMMDD_YYYYMMDD", or "false" for full range (-p).'),
    },
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

    run_metadata = BashOperator(
        task_id='run_metadata',
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
                     '{{ params.extra_flags }} '
                     '-n {{ params.lakes }} '
                     '-p {{ params.period }} ',
        params={
            'AWS_ID': Variable.get("AWS_ACCESS_KEY_ID"),
            'AWS_KEY': Variable.get("AWS_SECRET_ACCESS_KEY"),
        },
        on_failure_callback=report_failure,
        retries=0,
        dag=dag,
    )

    clone_repo >> build_docker >> run_metadata
