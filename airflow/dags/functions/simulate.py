import os
import json
import boto3
import requests
import tempfile
import numpy as np
from datetime import datetime, timedelta, timezone
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


def format_depth(number):
    string = str(number)
    if "." not in string:
        return string + ".0"
    return string


def number_of_cores(task_instance, cores):
    if task_instance.try_number == task_instance.max_tries + 1:
        return 1
    elif task_instance.try_number > 1:
        return cores + 1
    else:
        return cores


def closest(lst, k):
    return lst[min(range(len(lst)), key=lambda i: abs(lst[i] - k))]


def cache_simulation_data(ds, **kwargs):
    lake = kwargs["lake"]
    model = kwargs["model"]
    api = kwargs["api"]
    bucket = kwargs["bucket"]
    aws_access_key_id = kwargs["AWS_ID"]
    aws_secret_access_key = kwargs["AWS_KEY"]
    bucket_key = bucket.split(".")[0].split("//")[1]

    s3 = boto3.client("s3",
                      aws_access_key_id=aws_access_key_id,
                      aws_secret_access_key=aws_secret_access_key)

    response = requests.get("{}/static/website/metadata/{}.json".format(bucket, lake))
    lake_info = response.json()

    response = requests.get("{}/simulations/metadata/{}/{}".format(api, model, lake))
    lake_metadata = response.json()

    max_date = datetime.strptime(lake_metadata["end_date"], '%Y-%m-%d %H:%M').replace(tzinfo=timezone.utc)
    start_date = max_date + timedelta(days=lake_info["customPeriod"]["start"])
    depth = closest(lake_metadata["depths"], lake_info["depth"])
    start = start_date.strftime("%Y%m%d%H")
    end = max_date.strftime("%Y%m%d%H")

    response = requests.get(
        "{}/simulations/layer_alplakes/{}/{}/geometry/{}/{}/{}".format(api, model, lake, start, end, depth))
    geometry = response.text

    with tempfile.NamedTemporaryFile(mode='w', delete=False) as temp_file:
        temp_filename = temp_file.name
        temp_file.write(geometry)
    s3.upload_file(temp_filename, bucket_key, "simulations/{}/metadata/{}/geometry.txt".format(model, lake))
    os.remove(temp_filename)

    for parameter in ["temperature", "velocity"]:
        response = requests.get(
            "{}/simulations/layer_alplakes/{}/{}/{}/{}/{}/{}".format(api, model, lake, parameter, start, end, format_depth(depth)))
        temperature = response.text

        with tempfile.NamedTemporaryFile(mode='w', delete=False) as temp_file:
            temp_filename = temp_file.name
            temp_file.write(temperature)
        s3.upload_file(temp_filename, bucket_key,
                       "simulations/{}/data/{}/{}_{}_{}_{}.txt".format(model, lake, parameter, start, end, format_depth(depth)))
        os.remove(temp_filename)

    with tempfile.NamedTemporaryFile(mode='w', delete=False) as temp_file:
        temp_filename = temp_file.name
        json.dump(lake_metadata, temp_file)
    s3.upload_file(temp_filename, bucket_key, "simulations/{}/metadata/{}/metadata.json".format(model, lake))
    os.remove(temp_filename)

    response = requests.get("{}/static/website/metadata/list.json".format(bucket))
    lake_list = response.json()

    start = datetime.now().replace(tzinfo=timezone.utc).replace(hour=0, minute=0, second=0, microsecond=0) - timedelta(
        days=1)
    forecast = {}
    for lake in lake_list:
        response = requests.get("{}/simulations/metadata/{}/{}".format(api, model, lake["key"]))
        lake_metadata = response.json()
        response = requests.get("{}/static/website/metadata/{}.json".format(bucket, lake["key"]))
        lake_info = response.json()
        end = datetime.strptime(lake_metadata["end_date"] + ":00+00:00", '%Y-%m-%d %H:%M:%S%z')
        response = requests.get(
            "{}/simulations/layer/average_temperature/{}/{}/{}/{}/{}".format(api, model, lake["key"],
                                                                             start.strftime("%Y%m%d%H%M"),
                                                                             end.strftime("%Y%m%d%H%M"),
                                                                             lake_info["depth"]))
        data = response.json()
        forecast[lake["key"]] = {"date": list(np.array(data["date"]) * 1000), "value": data["temperature"]}

    with tempfile.NamedTemporaryFile(mode='w', delete=False) as temp_file:
        temp_filename = temp_file.name
        json.dump(forecast, temp_file)
    s3.upload_file(temp_filename, bucket_key, "simulations/forecast.json")
    os.remove(temp_filename)
