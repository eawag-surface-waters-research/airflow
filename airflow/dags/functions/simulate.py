import requests
from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta, SU


def get_last_sunday(dt):
    bd = datetime.strptime(dt, "%Y-%m-%d") + relativedelta(weekday=SU(-1))
    return bd.strftime('%Y%m%d')


def get_end_date(dt):
    bd = datetime.strptime(dt, "%Y-%m-%d") + timedelta(days=6)
    return bd.strftime('%Y%m%d')


def get_today(dt):
    return (datetime.strptime(dt, "%Y-%m-%d") + timedelta(days=1)).strftime('%Y%m%d')


def get_restart(dt):
    bd = datetime.strptime(dt, "%Y-%m-%d") + relativedelta(weekday=SU(-1)) + timedelta(days=7)
    return bd.strftime('%Y%m%d')


def number_of_cores(task_instance, cores):
    if task_instance.try_number > 1:
        return 1
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

