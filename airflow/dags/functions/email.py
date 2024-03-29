from airflow.operators.email import EmailOperator
import urllib.parse
import traceback
from datetime import datetime, timezone


def report_failure(context):
    ti = context.get('task_instance')
    print(ti)
    s3_url = 'https://alplakes-eawag.s3.eu-central-1.amazonaws.com/airflow/logs/' + \
             urllib.parse.quote((f'dag_id={ti.dag_id}/'
                                 f'run_id={ti.run_id}/'
                                 f'task_id={ti.task_id}/'
                                 f'attempt=1.log'))
    graph_url = f'http://eaw-alplakes2:8080/dags/{ti.dag_id}/graph'

    print(dir(ti))

    exception = context.get('exception')
    formatted_exception = ''.join(
        traceback.format_exception(etype=type(exception), value=exception, tb=exception.__traceback__)).strip()

    html_content = (
        f'<table style="'
        f'margin: 20px;'
        f'width: calc(100% - 40px);'
        f'padding: 20px;'
        f'box-sizing: border-box;'
        f'table-layout: fixed;'
        f'font-family: Helvetica;'
        f'border: 1px solid #c7d0d4;'
        f'border-radius: 4px;">'
        f'<tbody>'
        f'<tr><td style="font-size: 30px">Eawag Airflow <br />Error Reporting</td>'
        f'<td style="text-align: right">'
        f'<a href="{graph_url}" style="text-decoration: none">'
        f'<button style="padding: 10px 20px;margin: 5px;color: #38bec9;background-color: white;border: 1px solid #38bec9;border-radius: 4px;">View Graph</button></a>'
        f'<a href="{s3_url}" style="text-decoration: none">'
        f'<button style="padding: 10px 20px;margin: 5px;color: white;background-color: #38bec9;border: 1px solid #38bec9;border-radius: 4px;">Download Log</button></a></td></tr>'
        f'<tr><td colspan="2" style="padding: 30px 0">Admin, <br /><br />The following exception in the operator <b>{ti.task_id}</b> caused the DAG <b>{ti.dag_id}</b> to fail at <b>{ti.start_date}</b>.</td></tr>'
        f'<tr style="color: red"><td colspan="2" style="padding: 30px 0; border-top: 1px solid #c7d0d4">{formatted_exception}</td></tr>'
        f'</tbody></table>'
    )

    subject = f'DAG ERROR - {ti.dag_id} failed.'
    send_email = EmailOperator(task_id="report_failure", to="james.runnalls@eawag.ch", subject=subject,
                               html_content=html_content)
    send_email.execute(context)


def report_success(context):
    ti = context.get('task_instance')
    graph_url = f'http://eaw-alplakes2:8080/dags/{ti.dag_id}/graph'
    dag_run = context.get('dag_run')
    total_runtime = datetime.now(timezone.utc) - dag_run.execution_date
    params_from_config = dag_run.conf if dag_run.conf else {}
    html_content = (
        f'<table style="'
        f'margin: 20px;'
        f'width: calc(100% - 40px);'
        f'padding: 20px;'
        f'box-sizing: border-box;'
        f'table-layout: fixed;'
        f'font-family: Helvetica;'
        f'border: 1px solid #c7d0d4;'
        f'border-radius: 4px;">'
        f'<tbody>'
        f'<tr><td style="font-size: 30px">Eawag Airflow <br />Task Success</td>'
        f'<td style="text-align: right">'
        f'<a href="{graph_url}" style="text-decoration: none">'
        f'<button style="padding: 10px 20px;margin: 5px;color: #38bec9;background-color: white;border: 1px solid #38bec9;border-radius: 4px;">View Graph</button></a></td></tr>'
        f'<tr><td colspan="2" style="padding: 30px 0">Admin, <br /><br />The task <b>{ti.task_id}</b> in DAG <b>{ti.dag_id}</b> succeeded at <b>{ti.start_date}</b>.</td></tr>'
        f'<tr><td colspan="2" style="padding: 15px 0">Total runtime: {total_runtime}</td></tr>'
        f'<tr><td colspan="2" style="padding: 15px 0">Parameters from config: {params_from_config}</td></tr>'
        f'</tbody></table>'
    )

    subject = f'DAG COMPLETE - {ti.dag_id}'
    send_email = EmailOperator(task_id="report_success", to="james.runnalls@eawag.ch", subject=subject,
                               html_content=html_content)
    send_email.execute(context)


