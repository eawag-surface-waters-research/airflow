import os
import json
import gzip
import boto3
import requests
import tempfile
import numpy as np
from urllib.parse import quote
from datetime import datetime, timedelta, timezone
from dateutil.relativedelta import relativedelta, SU
from airflow.utils.email import send_email
from jinja2 import Template


def get_last_sunday(dt):
    today = datetime.strptime(dt, "%Y-%m-%d") + timedelta(days=1)
    bd = today + relativedelta(weekday=SU(-1))
    return bd.strftime('%Y%m%d')

def generate_days_ago_function(num_days):
    def days_ago(dt, pattern="%Y%m%d"):
        date_obj = datetime.strptime(dt, "%Y-%m-%d")
        adjusted_date = date_obj - timedelta(days=num_days - 1)
        return adjusted_date.strftime(pattern)
    return days_ago

def get_end_date(dt):
    today = datetime.strptime(dt, "%Y-%m-%d") + timedelta(days=1)
    bd = today + timedelta(days=5)
    return bd.strftime('%Y%m%d')


def parse_profile(profile):
    if profile == False or profile == "false":
        return ""
    else:
        return "-p {}".format(profile)


def get_today(dt):
    today = datetime.strptime(dt, "%Y-%m-%d") + timedelta(days=1)
    return today.strftime('%Y%m%d')


def get_restart(dt):
    today = datetime.strptime(dt, "%Y-%m-%d") + timedelta(days=1)
    bd = today + relativedelta(weekday=SU(-1)) + timedelta(days=7)
    return bd.strftime('%Y%m%d')


def format_depth(number):
    string = str(number)
    if "." not in string:
        return string + ".0"
    return string


def format_simulation_directory(docker):
    folder = "{}_delft3dflow".format(docker)
    return folder.replace("/", "_").replace(".", "").replace(":", "").replace("-", "")


def number_of_cores(task_instance, cores):
    if task_instance.try_number == task_instance.max_tries + 1:
        return 1
    elif task_instance.try_number > 1:
        return cores + 1
    else:
        return cores


def closest(lst, k):
    return lst[min(range(len(lst)), key=lambda i: abs(lst[i] - k))]


def interpolate(original_times, new_times, original_data):
    ot = np.array(original_times)
    nt = np.array(new_times)
    od = np.array(original_data)
    mask = (nt >= ot[0]) & (nt <= ot[-1])
    interpolated_data = np.full_like(nt, np.nan, dtype=float)
    if np.any(mask):
        clamped = np.clip(nt, ot[0], ot[-1])
        interpolated_data[mask] = np.interp(clamped[mask], ot, od)
    interpolated_data = [val if not np.isnan(val) else None for val in interpolated_data]
    return interpolated_data


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

    # Collect information
    response = requests.get("{}/simulations/metadata/{}/{}".format(api, model, lake))
    if response.status_code != 200:
        raise ValueError("Unable to access {}/simulations/metadata/{}/{}".format(api, model, lake))
    lake_metadata = response.json()

    default_depth = 1
    default_period = -6
    response = requests.get("{}/static/website/metadata/master/{}.json".format(bucket, lake))
    if response.status_code != 200:
        raise ValueError("Unable to access {}/static/website/metadata/master/{}.json".format(bucket, lake))
    lake_info = response.json()
    try:
        default_depth = lake_info["metadata"]["default_depth"]
    except:
        print("Failed to collect custom depth, using default of {}".format(default_depth))
    try:
        layer = [l for l in lake_info["layers"] if l["id"] == "3D_temperature"][0]
        default_period = layer["sources"]["alplakes_delft3d"]["start"]
    except:
        print("Failed to collect custom period, using default of {}".format(default_period))

    # Cache lake page files
    max_date = datetime.strptime(lake_metadata["end_date"] + " 21:00", '%Y-%m-%d %H:%M').replace(tzinfo=timezone.utc)
    start_date = max_date + timedelta(days=default_period)
    depth = closest(lake_metadata["depth"], default_depth)
    start = start_date.strftime("%Y%m%d%H") + "00"
    end = max_date.strftime("%Y%m%d%H") + "00"

    for parameter in ["geometry", "temperature", "velocity", "thermocline"]:
        response = requests.get(
            "{}/simulations/layer_alplakes/{}/{}/{}/{}/{}/{}".format(api, model, lake, parameter, start, end, depth))
        if response.status_code == 200:
            temperature = response.text
            try:
                with tempfile.NamedTemporaryFile(mode='wb', delete=False) as temp_file:
                    temp_filename = temp_file.name
                    with gzip.GzipFile(fileobj=temp_file, mode='wb') as gz_file:
                        gz_file.write(temperature.encode('utf-8'))
                s3.upload_file(temp_filename, bucket_key, "simulations/{}/cache/{}/{}.txt.gz".format(model, lake, parameter))
            except Exception as e:
                print(e)
            with tempfile.NamedTemporaryFile(mode='w', delete=False) as temp_file:
                temp_filename = temp_file.name
                temp_file.write(temperature)
            s3.upload_file(temp_filename, bucket_key, "simulations/{}/cache/{}/{}.txt".format(model, lake, parameter))
            os.remove(temp_filename)
        else:
            print("Failed to cache {}".format(parameter))
            print(response.text)

    # Cache metadata
    with tempfile.NamedTemporaryFile(mode='w', delete=False) as temp_file:
        temp_filename = temp_file.name
        json.dump(lake_metadata, temp_file)
    s3.upload_file(temp_filename, bucket_key, "simulations/{}/cache/{}/metadata.json".format(model, lake))
    os.remove(temp_filename)

    # Cache home page forecast
    start = datetime.now().replace(tzinfo=timezone.utc).replace(hour=0, minute=0, second=0, microsecond=0) - timedelta(
        days=1)
    end = datetime.strptime(lake_metadata["end_date"] + " 21:00:00+00:00", '%Y-%m-%d %H:%M:%S%z')
    response = requests.get(
        "{}/simulations/layer/average_temperature/{}/{}/{}/{}/{}"
        .format(api, model, lake,start.strftime("%Y%m%d%H%M"), end.strftime("%Y%m%d%H%M"), default_depth))
    if response.status_code != 200:
        raise ValueError(
            "Unable to access {}/simulations/layer/average_temperature/{}/{}/{}/{}/{}"
            .format(api, model, lake, start.strftime("%Y%m%d%H%M"), end.strftime("%Y%m%d%H%M"), default_depth)
        )
    data = response.json()

    response = requests.get("{}/simulations/forecast.json".format(bucket))
    if response.status_code == 200:
        forecast = response.json()
    elif response.status_code == 404:
        forecast = {}
    else:
        raise ValueError("Problems connecting to forecast in {}".format(bucket))
    if lake not in forecast or int(datetime.fromisoformat(data["time"][-1]).timestamp() * 1000) > forecast[lake]["time"][-1] + (6 * 3600 * 1000):
        out = {"time": [int(datetime.fromisoformat(d).timestamp() * 1000) for d in data["time"]], "temperature": data["variable"]["data"]}
        forecast[lake] = out
        with tempfile.NamedTemporaryFile(mode='w', delete=False) as temp_file:
            temp_filename = temp_file.name
            json.dump(forecast, temp_file)
        s3.upload_file(temp_filename, bucket_key, "simulations/forecast.json".format(model, lake))
        os.remove(temp_filename)


def process_event_notifications(ds, **kwargs):
    bucket = kwargs["bucket"]
    folder = kwargs["folder"]
    lake = kwargs["lake"]
    name = kwargs["name"]
    model = kwargs["model"]
    email_list = kwargs["email_list"]
    aws_access_key_id = kwargs["AWS_ID"]
    aws_secret_access_key = kwargs["AWS_KEY"]
    bucket_key = bucket.split(".")[0].split("//")[1]

    s3 = boto3.client("s3",
                      aws_access_key_id=aws_access_key_id,
                      aws_secret_access_key=aws_secret_access_key)

    os.path.join(os.path.dirname(__file__),  'events_email_template.html')

    now = datetime.now().replace(tzinfo=timezone.utc)
    now = now.replace(hour=0, minute=0, second=0, microsecond=0)
    last_sunday = (now + timedelta(days=1)) + relativedelta(weekday=SU(-1))
    templates = {
        "email": os.path.join(os.path.dirname(__file__), "..", 'emails/events_template.html'),
        "upwelling": os.path.join(os.path.dirname(__file__), "..", 'emails/upwelling_template.html'),
        "localisedCurrents": os.path.join(os.path.dirname(__file__), "..", 'emails/localisedCurrents_template.html')
    }
    events_html = ""
    if os.path.isfile(os.path.join(folder, "events.json")):
        with open(os.path.join(folder, "events.json")) as f:
            events = json.load(f)
        if len(events) > 0:
            response = requests.get("{}/simulations/{}/events/{}/events.json".format(bucket, model, lake))
            if response.status_code == 200:
                data = response.json()
                event_list = []
                for old_event in data:
                    if datetime.fromisoformat(old_event["start"]) < last_sunday:
                        event_list.append(old_event)
                event_list = event_list + events
            else:
                event_list = events

            with tempfile.NamedTemporaryFile(mode='w', delete=False) as temp_file:
                temp_filename = temp_file.name
                json.dump(event_list, temp_file)
            s3.upload_file(temp_filename, bucket_key, "simulations/{}/events/{}/events.json".format(model, lake))
            os.remove(temp_filename)

            if os.path.exists(os.path.join(folder, "events")):
                for file in os.listdir(os.path.join(folder, "events")):
                    if file.endswith(".png"):
                        s3.upload_file(os.path.join(folder, "events", file), bucket_key,
                                       "simulations/{}/events/{}/images/{}".format(model, lake, file))

            for event in events:
                if datetime.fromisoformat(event["end"]) > now:
                    with open(templates[event["type"]]) as file_:
                        template = Template(file_.read())
                    if event["type"] == "upwelling":
                        context = {
                            "lake": lake,
                            "period": datetime.fromisoformat(event["start"]).strftime(
                                "%H:%M %d %B %Y") + " - " + datetime.fromisoformat(event["end"]).strftime(
                                "%H:%M %d %B %Y"),
                            "peak": datetime.fromisoformat(event["properties"]["peak"]).strftime("%H:%M %d %B %Y"),
                            "max_centroid": str(round(event["properties"]["max_centroid"], 2)) + "°C",
                            "img": quote(event["properties"]["peak"], safe=''),
                            "description": event["description"]
                        }
                    elif event["type"] == "localisedCurrents":
                        context = {
                            "lake": lake,
                            "period": datetime.fromisoformat(event["start"]).strftime(
                                "%H:%M %d %B %Y") + " - " + datetime.fromisoformat(event["end"]).strftime(
                                "%H:%M %d %B %Y"),
                            "img": quote(event["start"], safe=''),
                            "description": event["description"]
                        }
                    else:
                        print("Unrecognised event", event["type"])
                        continue
                    events_html = events_html + template.render(context) + "\n"
                else:
                    print("Event in the past")

            if events_html != "":
                with open(templates["email"]) as file_:
                    template = Template(file_.read())
                context = {
                    "lake": lake,
                    "name": name,
                    "today": now.strftime("%d.%m.%y"),
                    "end_date": (now + timedelta(days=4)).strftime("%d.%m.%y"),
                    "events": events_html
                }
                email_html = template.render(context)
                for i in range(len(email_list)):
                    send_email(
                        to=str(email_list[i]),
                        subject='New events predicted for {}'.format(name),
                        html_content=email_html
                    )


def upload_restart_files(ds, **kwargs):
    lake = kwargs['dag_run'].conf.get('lake')
    start = kwargs['dag_run'].conf.get('start')
    end = kwargs['dag_run'].conf.get('end')
    model = kwargs["model"]
    bucket = kwargs["bucket"]
    restart = kwargs["restart"]
    aws_access_key_id = kwargs["AWS_ID"]
    aws_secret_access_key = kwargs["AWS_KEY"]

    s3 = boto3.client("s3",
                      aws_access_key_id=aws_access_key_id,
                      aws_secret_access_key=aws_secret_access_key)

    folder = "{}/git/{}/runs/{}_{}_{}_{}".format(kwargs["filesystem"],
                                                 kwargs["simulation_repo_name"],
                                                 kwargs["simulation_folder_prefix"],
                                                 lake, start, end)

    s = datetime.strptime(start, "%Y%m%d") + relativedelta(weekday=SU(-1))
    e = datetime.strptime(end, "%Y%m%d")
    i = 0
    while s < e and i < 1000:
        path = os.path.join(folder, "tri-rst.Simulation_Web.{}.000000".format(s.strftime('%Y%m%d')))
        if os.path.isfile(path):
            print("Uploading restart file: {}".format(path))
            s3.upload_file(path, bucket, restart.format(lake, s.strftime('%Y%m%d')))
        else:
            print("Cannot locate restart file: {}".format(path))
        s = s + relativedelta(days=7)
        i = i + 1
