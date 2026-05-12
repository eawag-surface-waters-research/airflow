from datetime import datetime, timedelta

from airflow.operators.bash import BashOperator
from airflow.models import Variable
from airflow.models.param import Param

from functions.email import report_failure

from airflow import DAG

"""
Run sencast-metadata for a single user-specified product.

`metadata_name` is the product identifier passed to -mn (used downstream, e.g. "sentinel2").
`folder` is the subdirectory name used in local and remote paths (e.g. "sentinel2" or
"sentinel3_dimark"); it may differ from metadata_name.

Example config input:
{
  "metadata_name": "sentinel2",
  "folder": "sentinel3_dimark",
  "remote_tiff": "s3://eawagrs/datalakes/alplakes",
  "metadata_summary": "s3://eawagrs/alplakes/metadata/summary.json",
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
        "metadata_name": Param("sentinel2", type="string",
                               description='Metadata product name passed to -mn (used downstream).'),
        "folder": Param("sentinel2", type="string",
                        description='Subdirectory name used in local and remote paths '
                                    '(e.g. "sentinel2" or "sentinel3_dimark"). May differ from metadata_name.'),
        "remote_tiff": Param("s3://eawagrs/datalakes/alplakes", type="string",
                             description='Input tiff bucket/prefix (-rt).'),
        "metadata_summary": Param("s3://eawagrs/alplakes/metadata/summary.json", type="string",
                                  description='Full S3 path of the summary.json output (-ms).'),
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
        bash_command='mkdir -p {{ filesystem }}/media/remotesensing/local_tiff/{{ params.folder }}; '
                     'mkdir -p {{ filesystem }}/media/remotesensing/local_tiff_cropped/{{ params.folder }}; '
                     'mkdir -p {{ filesystem }}/media/remotesensing/local_metadata/{{ params.folder }}; '
                     'docker run '
                     '-e AWS_ACCESS_KEY_ID={{ params.AWS_ID }} '
                     '-e AWS_SECRET_ACCESS_KEY={{ params.AWS_KEY }} '
                     '-v {{ FILESYSTEM }}/git/{{ git_name }}:/repository '
                     '-v {{ FILESYSTEM }}/media/remotesensing/local_tiff/{{ params.folder }}:/local_tiff '
                     '-v {{ FILESYSTEM }}/media/remotesensing/local_tiff_cropped/{{ params.folder }}:/local_tiff_cropped '
                     '-v {{ FILESYSTEM }}/media/remotesensing/local_metadata/{{ params.folder }}:/local_metadata '
                     '--rm '
                     '{{ docker }} '
                     '-rt {{ params.remote_tiff }} '
                     '-rtc s3://eawagrs/alplakes/cropped/{{ params.folder }} '
                     '-g {{ lake_geometry }} '
                     '-rm s3://eawagrs/alplakes/metadata/{{ params.folder }} '
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
