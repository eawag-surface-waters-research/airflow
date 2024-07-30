import os
import traceback
import urllib.parse
from jinja2 import Template
from datetime import datetime, timezone

from airflow.operators.email import EmailOperator


def report_failure(context):
    ti = context.get('task_instance')
    exception = context.get('exception')
    parameters = {
        "s3_url": 'https://alplakes-eawag.s3.eu-central-1.amazonaws.com/airflow/logs/' +
                  urllib.parse.quote((f'dag_id={ti.dag_id}/'
                                      f'run_id={ti.run_id}/'
                                      f'task_id={ti.task_id}/'
                                      f'attempt=1.log')),
        "graph_url": f'http://eaw-alplakes2:8080/dags/{ti.dag_id}/graph',
        "formatted_exception": ''.join(
            traceback.format_exception(etype=type(exception), value=exception, tb=exception.__traceback__)).strip(),
        "task_id": ti.task_id,
        "dag_id": ti.dag_id,
        "start_date": ti.start_date
    }

    with open(os.path.join(os.path.dirname(__file__), "..", 'emails/failure_template.html')) as file_:
        template = Template(file_.read())

    html_content = template.render(parameters)
    subject = f'DAG ERROR - {ti.dag_id} failed.'
    send_email = EmailOperator(task_id="report_failure", to="james.runnalls@eawag.ch", subject=subject,
                               html_content=html_content)
    send_email.execute(context)


def report_success(context):
    ti = context.get('task_instance')
    dag_run = context.get('dag_run')
    parameters = {
        "graph_url": f'http://eaw-alplakes2:8080/dags/{ti.dag_id}/graph',
        "task_id": ti.task_id,
        "dag_id": ti.dag_id,
        "start_date": ti.start_date,
        "runtime": datetime.now(timezone.utc) - dag_run.execution_date,
        "config": dag_run.conf if dag_run.conf else {}
    }

    with open(os.path.join(os.path.dirname(__file__), "..", 'emails/success_template.html')) as file_:
        template = Template(file_.read())

    html_content = template.render(parameters)
    subject = f'TASK COMPLETE - {ti.task_id}'
    send_email = EmailOperator(task_id="report_success", to="james.runnalls@eawag.ch", subject=subject,
                               html_content=html_content)
    send_email.execute(context)
