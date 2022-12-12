from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta, SU


def get_last_sunday(dt):
    bd = datetime.strptime(dt, "%Y-%m-%d") + relativedelta(weekday=SU(-1))
    return bd.strftime('%Y%m%d')


def get_end_date(dt):
    bd = datetime.strptime(dt, "%Y-%m-%d") + timedelta(days=4)
    return bd.strftime('%Y%m%d')


def get_today(dt):
    return datetime.strptime(dt, "%Y-%m-%d").strftime('%Y%m%d')


def get_restart(dt):
    bd = datetime.strptime(dt, "%Y-%m-%d") + relativedelta(weekday=SU(-1)) + timedelta(days=7)
    return bd.strftime('%Y%m%d')
