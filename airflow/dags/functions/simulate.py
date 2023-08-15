import requests
from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta, SU


def get_last_sunday(dt):
    today = datetime.strptime(dt, "%Y-%m-%d") + timedelta(days=1)
    bd = today + relativedelta(weekday=SU(-1))
    return bd.strftime('%Y%m%d')


def get_end_date(dt):
    today = datetime.strptime(dt, "%Y-%m-%d") + timedelta(days=1)
    bd = today + timedelta(days=5)
    return bd.strftime('%Y%m%d')


def get_today(dt):
    today = datetime.strptime(dt, "%Y-%m-%d") + timedelta(days=1)
    return today.strftime('%Y%m%d')


def get_restart(dt):
    today = datetime.strptime(dt, "%Y-%m-%d") + timedelta(days=1)
    bd = today + relativedelta(weekday=SU(-1)) + timedelta(days=7)
    return bd.strftime('%Y%m%d')


def parse_restart(restart_file):
    if restart_file == False or restart_file == "false":
        return "-x"
    else:
        return ""


def number_of_cores(task_instance, cores):
    if task_instance.try_number == task_instance.max_tries:
        return 1
    elif task_instance.try_number > 1:
        return cores + 1
    else:
        return cores


def post_notify_api(params, **kwargs):
    url = params["api"] + "/simulations/notify"
    body = {"type": "new",
            "model": params["model"],
            "value": "{}_{}.nc".format(params["file"], get_last_sunday(str(kwargs['ds'])))}
    resp = requests.post(url, json=body)
    if resp.status_code != 200:
        raise ValueError("Failed to notify Alplakes API.")
