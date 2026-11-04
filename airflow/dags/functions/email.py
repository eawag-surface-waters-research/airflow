import os
import re
import traceback
import urllib.parse
from collections import deque
from jinja2 import Template
from datetime import datetime, timezone

from airflow.configuration import conf
from airflow.operators.email import EmailOperator

LOG_OUTPUT_LINES = 50
LOG_LINE_LENGTH = 500
LOG_PREFIX = re.compile(r'^\[[^]]+\] \{[^}]+\} [A-Z]+ - ')
LOG_OUTPUT_START = re.compile(r'INFO - (Output:|::endgroup::)$')
LOG_OUTPUT_END = re.compile(r'INFO - Command exited with return code|ERROR - Task failed with exception')
ANSI_ESCAPE = re.compile(r'\x1b\[[0-9;]*m')


def read_task_output(log_path):
    """Return the last lines the task printed, without the Airflow log lines around them."""
    lines = deque(maxlen=LOG_OUTPUT_LINES)
    try:
        with open(log_path, errors='replace') as file_:
            for line in file_:
                line = line.rstrip()
                if LOG_OUTPUT_START.search(line):
                    lines.clear()
                elif LOG_OUTPUT_END.search(line):
                    break
                else:
                    line = ANSI_ESCAPE.sub('', LOG_PREFIX.sub('', line))
                    lines.append(line[:LOG_LINE_LENGTH])
    except OSError:
        # Callbacks for dead tasks run on the scheduler, which doesn't have the worker's log file
        return ''
    return '\n'.join(lines).strip()


def format_error(exception):
    """Return the error summary and traceback, without the leading frames from Airflow internals."""
    if not isinstance(exception, BaseException):
        return str(exception), ''
    summary = ''.join(traceback.format_exception_only(exception)).strip()
    tb = exception.__traceback__
    while tb and '/site-packages/airflow/' in tb.tb_frame.f_code.co_filename:
        tb = tb.tb_next
    if tb is None and not (exception.__cause__ or exception.__context__):
        return summary, ''
    return summary, ''.join(traceback.format_exception(type(exception), exception, tb)).strip()


def report_failure(context):
    emails = context['dag'].default_args.get('email', [])
    ti = context.get('task_instance')
    error_summary, error_traceback = format_error(context.get('exception'))
    log_path = (f'dag_id={ti.dag_id}/'
                f'run_id={ti.run_id}/'
                f'task_id={ti.task_id}/'
                f'attempt={ti.try_number}.log')
    parameters = {
        "s3_url": 'https://alplakes-eawag.s3.eu-central-1.amazonaws.com/airflow/logs/' + urllib.parse.quote(log_path),
        "graph_url": f'http://eaw-alplakes2:8080/dags/{ti.dag_id}/graph',
        "error_summary": error_summary,
        "error_traceback": error_traceback,
        "task_output": read_task_output(os.path.join(conf.get('logging', 'base_log_folder'), log_path)),
        "task_id": ti.task_id,
        "dag_id": ti.dag_id,
        "run_id": ti.run_id,
        "try_number": ti.try_number,
        "start_date": ti.start_date
    }

    with open(os.path.join(os.path.dirname(__file__), "..", 'emails/failure_template.html')) as file_:
        template = Template(file_.read())

    html_content = template.render(parameters)
    subject = f'DAG ERROR - {ti.dag_id} failed.'
    send_email = EmailOperator(task_id="report_failure", to=emails, subject=subject,
                               html_content=html_content)
    send_email.execute(context)


def report_success(context):
    emails = context['dag'].default_args.get('email', [])
    ti = context.get('task_instance')
    dag_run = context.get('dag_run')
    parameters = {
        "graph_url": f'http://eaw-alplakes2:8080/dags/{ti.dag_id}/graph',
        "task_id": ti.task_id,
        "dag_id": ti.dag_id,
        "start_date": ti.start_date,
        "runtime": datetime.now(timezone.utc) - dag_run.logical_date,
        "config": dag_run.conf if dag_run.conf else {}
    }

    with open(os.path.join(os.path.dirname(__file__), "..", 'emails/success_template.html')) as file_:
        template = Template(file_.read())

    html_content = template.render(parameters)
    subject = f'TASK COMPLETE - {ti.task_id}'
    send_email = EmailOperator(task_id="report_success", to=emails, subject=subject,
                               html_content=html_content)
    send_email.execute(context)
