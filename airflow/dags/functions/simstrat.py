import os
import json
import math
import time
import gzip
import boto3
import zipfile
import requests
import tempfile
import numpy as np
from datetime import datetime, timedelta, timezone
from functions.general import download_datalakes_data, calculate_rmse


def iso_to_unix(input_time):
    return int(datetime.fromisoformat(input_time).astimezone(timezone.utc).timestamp()) * 1000


def zip_files(base_folder, file_paths, output_filename):
    with zipfile.ZipFile(output_filename, 'w') as f:
        for file_path in file_paths:
            f.write(file_path, os.path.relpath(file_path, base_folder))


def custom_depth_array(max_num):
    arr = np.concatenate([np.arange(1, 20, 1), np.arange(20, 60, 2), np.arange(60, 1000, 10)])
    return np.sort(np.append(arr[arr < max_num][:-1], max_num))[::-1]


def validate_simstrat_operational_data(ds, **kwargs):
    api = kwargs["api"]
    response = requests.get("{}/simulations/1d/metadata".format(api))
    if response.status_code != 200:
        raise ValueError("Unable to access Simstrat metadata")
    lakes = next((d for d in response.json() if d.get('model') == "simstrat"), {"lakes": []})["lakes"]
    failed = []
    now = datetime.now(timezone.utc)
    midnight_today = datetime(year=now.year, month=now.month, day=now.day, tzinfo=timezone.utc)
    forecast_date = midnight_today + timedelta(days=4)
    for lake in lakes:
        if datetime.strptime(lake["end_date"] + " 22:00", "%Y-%m-%d %H:%M").replace(tzinfo=timezone.utc) < forecast_date:
            failed.append(lake["name"])
    if len(failed) > 0:
        raise ValueError("Simstrat simulation failed for: {}".format(", ".join(failed)))


def cache_simstrat_operational_data(ds, **kwargs):
    api = kwargs["api"]
    bucket = kwargs["bucket"]
    aws_access_key_id = kwargs["AWS_ID"]
    aws_secret_access_key = kwargs["AWS_KEY"]
    bucket_key = bucket.split(".")[0].split("//")[1]

    s3 = boto3.client("s3",
                      aws_access_key_id=aws_access_key_id,
                      aws_secret_access_key=aws_secret_access_key)

    parameters = [
        {"simstrat_key": "T", "alplakes_key": "temperature", "depth": 0}
    ]

    rename = {
        "upperzurich": False,
        "lowerzurich": "zurich",
        "upperlugano": "lugano",
        "lowerlugano": False,
        "upperconstance": "constance",
        "lowerconstance": False,
        "lucernealpnachersee": "alpnachersee",
        "lucernekreuztrichterandvitznauerbecken": "lucerne",
        "lucernegersauerandtreibbecken": False,
        "lucerneurnersee": False,
        "aegeri": "ageri"
    }

    response = requests.get("{}/simulations/1d/metadata".format(api))
    if response.status_code != 200:
        raise ValueError("Unable to access Simstrat metadata")
    lakes = next((d for d in response.json() if d.get('model') == "simstrat"), {"lakes": []})["lakes"]

    response = requests.get("{}/simulations/forecast.json".format(bucket))
    if response.status_code == 200:
        forecast = response.json()
    elif response.status_code == 404:
        forecast = {}
    else:
        raise ValueError("Problems connecting to forecast in {}".format(bucket))

    failed = 0
    start = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0) - timedelta(days=1)
    end = start + timedelta(days=6)
    for lake in lakes:
        if lake["name"] in rename and lake["name"] == False:
            continue
        data = {}
        for parameter in parameters:
            url = "{}/simulations/1d/point/simstrat/{}/{}/{}/{}?variables={}".format(
                api, lake["name"], start.strftime("%Y%m%d%H%M"), end.strftime("%Y%m%d%H%M"),
                lake["depth"][parameter["depth"]], parameter["simstrat_key"])
            response = requests.get(url)
            if response.status_code == 200:
                if "time" not in data:
                    data["time"] = [iso_to_unix(t) for t in response.json()["time"]]
                data[parameter["alplakes_key"]] = response.json()["variables"][parameter["simstrat_key"]]["data"]
            else:
                failed = failed + 1
                print("Error: {}".format(url))
        if lake["name"] in rename:
            forecast[rename[lake["name"]]] = data
        else:
            forecast[lake["name"]] = data

    with tempfile.NamedTemporaryFile(mode='w', delete=False) as temp_file:
        temp_filename = temp_file.name
        json.dump(forecast, temp_file)
    s3.upload_file(temp_filename, bucket_key, "simulations/forecast.json")
    os.remove(temp_filename)

    # Get version of website metadata
    branch = "master"
    try:
        response = requests.get(
            "https://raw.githubusercontent.com/eawag-surface-waters-research/alplakes-react/refs/heads/master/src/config.json")
        if response.status_code == 200:
            branch = response.json()["branch"]
    except:
        print("Failed to find branch")

    response = requests.get("{}/static/website/metadata/{}/performance.json".format(bucket, branch))
    if response.status_code == 200:
        performance_info = response.json()

    for lake in lakes:
        # Performance
        try:
            if "simstrat" in performance_info and lake["name"] in performance_info["simstrat"]:
                live = performance_info["simstrat"][lake["name"]].copy()
                stop = datetime.now() - timedelta(days=1)
                stop = stop.replace(hour=23, minute=0, second=0, microsecond=0)
                start = stop - timedelta(days=10)
                rmse_total = []
                for location in live:
                    for depth in live[location]["depth"]:
                        try:
                            if live[location]["type"] == "datalakes":
                                live[location]["depth"][depth]["insitu"] = (
                                    download_datalakes_data(live[location]["id"],live[location]["depth"][depth]["depth"], start, stop))
                            else:
                                raise ValueError("Unrecognised data source")

                            response = requests.get(
                                "{}/simulations/1d/point/simstrat/{}/{}/{}/{}?variables=T"
                                .format(api, lake["name"], start.strftime("%Y%m%d2300"), stop.strftime("%Y%m%d2300"), live[location]["depth"][depth]["depth"]))
                            if response.status_code != 200:
                                raise ValueError("Failed to get model values")
                            out = response.json()
                            live[location]["depth"][depth]["model"] = {"time": out["time"],
                                                                       "values": out["variables"]["T"]["data"]}
                            rmse = calculate_rmse(live[location]["depth"][depth]["model"],
                                                  live[location]["depth"][depth]["insitu"])
                            if isinstance(rmse, float) and not math.isnan(rmse):
                                rmse_total.append(rmse)
                                live[location]["depth"][depth]["rmse"] = round(rmse, 1)
                        except:
                            print("Failed to collect insitu")
                if len(rmse_total) > 0:
                    lake["rmse"] = round(rmse_total[0], 1)
                    with tempfile.NamedTemporaryFile(mode='w', delete=False) as temp_file:
                        temp_filename = temp_file.name
                        json.dump(live, temp_file)
                    s3.upload_file(temp_filename, bucket_key,
                                   "simulations/simstrat/cache/{}/performance.json".format(lake["name"]))
        except:
            print("Failed to add performance for {}".format(lake["name"]))

        # Metadata
        with tempfile.NamedTemporaryFile(mode='w', delete=False) as temp_file:
            temp_filename = temp_file.name
            json.dump(lake, temp_file)
        s3.upload_file(temp_filename, bucket_key, "simulations/simstrat/cache/{}/metadata.json".format(lake["name"]))
        os.remove(temp_filename)

        # Heatmaps
        end = datetime.strptime(lake["end_date"] + " 22:00", "%Y-%m-%d %H:%M")
        start = end - timedelta(days=365)
        for parameter in ["T", "OxygenSat"]:
            url = "{}/simulations/1d/depthtime/simstrat/{}/{}/{}?variables={}".format(api, lake["name"],
                                                                          start.strftime("%Y%m%d%H%M"),
                                                                          end.strftime("%Y%m%d%H%M"), parameter)
            response = requests.get(url)
            if response.status_code == 200:
                data = response.json()
                with tempfile.NamedTemporaryFile(mode='w', delete=False) as temp_file:
                    temp_filename = temp_file.name
                    json.dump(data, temp_file)
                s3.upload_file(temp_filename, bucket_key,
                               "simulations/simstrat/cache/{}/heatmap_{}.json".format(lake["name"], parameter))
                os.remove(temp_filename)
                try:
                    depths = np.array(data["depth"]["data"])
                    depth_array = custom_depth_array(np.max(depths))
                    data["depth"]["data"] = depth_array.tolist()
                    indices = np.isin(depths, depth_array)
                    values = np.array(data["variables"][parameter]["data"])
                    values = values[indices, :]
                    data["variables"][parameter]["data"] = values.tolist()
                    with tempfile.NamedTemporaryFile(mode='wb', delete=False) as temp_file:
                        temp_filename = temp_file.name
                        with gzip.GzipFile(fileobj=temp_file, mode='wb') as gz_file:
                            gz_file.write(json.dumps(data).encode('utf-8'))
                    s3.upload_file(temp_filename, bucket_key,
                                   "simulations/simstrat/cache/{}/heatmap_{}.json.gz".format(lake["name"], parameter))
                    os.remove(temp_filename)
                except Exception as e:
                    print(e)
            else:
                print("Failed to retrieve simulations", url)

        # Linegraphs
        end = datetime.strptime(lake["end_date"] + " 22:00", "%Y-%m-%d %H:%M")
        start = end - timedelta(days=5)
        start_year = datetime(datetime.now().year, 1, 1)
        depth = int(min(lake["depth"]))
        for parameter in ["T"]:
            url = "{}/simulations/1d/point/simstrat/{}/{}/{}/{}?variables={}".format(api, lake["name"],
                                                                         start.strftime("%Y%m%d%H%M"),
                                                                         end.strftime("%Y%m%d%H%M"), depth, parameter)
            response = requests.get(url)
            if response.status_code == 200:
                data = response.json()
                with tempfile.NamedTemporaryFile(mode='w', delete=False) as temp_file:
                    temp_filename = temp_file.name
                    json.dump(data, temp_file)
                s3.upload_file(temp_filename, bucket_key,
                               "simulations/simstrat/cache/{}/linegraph_{}.json".format(lake["name"], parameter))
                os.remove(temp_filename)
            else:
                print("Failed to retrieve simulations", url)

        # DOY current year
        url = "{}/simulations/1d/point/simstrat/{}/{}/{}/{}?resample=daily&variables={}".format(api, lake["name"],
                                                                                    start_year.strftime("%Y%m%d%H%M"),
                                                                                    end.strftime("%Y%m%d%H%M"), depth, parameter)
        response = requests.get(url)
        if response.status_code == 200:
            data = response.json()
            with tempfile.NamedTemporaryFile(mode='w', delete=False) as temp_file:
                temp_filename = temp_file.name
                json.dump(data, temp_file)
            s3.upload_file(temp_filename, bucket_key,
                           "simulations/simstrat/cache/{}/doy_currentyear.json".format(lake["name"]))
            os.remove(temp_filename)
        else:
            print("Failed to retrieve simulations", url)


def create_simstrat_doy(ds, **kwargs):
    depths = kwargs["depths"]
    parameters = kwargs["parameters"]
    api = kwargs["api"]
    response = requests.get("{}/simulations/1d/metadata".format(api))
    if response.status_code != 200:
        raise ValueError("Unable to access Simstrat metadata")
    lakes = response.json()[0]["lakes"]
    for lake in lakes:
        for depth in depths:
            for parameter in parameters:
                d = depth
                if depth == "min":
                    d = 0
                if depth == "max":
                    d = max(lake["depths"])
                print("{}/simulations/1d/doy/write/simstrat/{}/{}/{}".format(api, lake["name"], parameter, d))
                requests.get("{}/simulations/1d/doy/write/simstrat/{}/{}/{}".format(api, lake["name"], parameter, d))
                print("Wait 20 seconds")
                time.sleep(20)


def cache_simstrat_doy(ds, **kwargs):
    depths = kwargs["depths"]
    parameters = kwargs["parameters"]
    api = kwargs["api"]
    bucket = kwargs["bucket"]
    aws_access_key_id = kwargs["AWS_ID"]
    aws_secret_access_key = kwargs["AWS_KEY"]
    bucket_key = bucket.split(".")[0].split("//")[1]

    s3 = boto3.client("s3",
                      aws_access_key_id=aws_access_key_id,
                      aws_secret_access_key=aws_secret_access_key)

    response = requests.get("{}/simulations/1d/metadata".format(api))
    if response.status_code != 200:
        raise ValueError("Unable to access Simstrat metadata")
    lakes = response.json()[0]["lakes"]
    for lake in lakes:
        for depth in depths:
            for parameter in parameters:
                d = depth
                if depth == "min":
                    d = 0
                if depth == "max":
                    d = max(lake["depths"])
                response = requests.get(
                    "{}/simulations/1d/doy/simstrat/{}/{}/{}".format(api, lake["name"], parameter, d))
                if response.status_code == 200:
                    data = response.json()
                    with tempfile.NamedTemporaryFile(mode='w', delete=False) as temp_file:
                        temp_filename = temp_file.name
                        json.dump(data, temp_file)
                    s3.upload_file(temp_filename, bucket_key,
                                   "simulations/simstrat/cache/{}/doy_{}.json".format(lake["name"], parameter))
                    os.remove(temp_filename)
                else:
                    print("Failed to retrieve simulations/1d/doy/simstrat/{}/{}/{}".format(lake["name"], parameter, d))

def upload_simstrat_calibration_result(ds, **kwargs):
    bucket = kwargs["bucket"]
    aws_access_key_id = kwargs["AWS_ID"]
    aws_secret_access_key = kwargs["AWS_KEY"]
    local_file = kwargs["file"]
    bucket_key = bucket.split(".")[0].split("//")[1]

    s3 = boto3.client("s3",
                      aws_access_key_id=aws_access_key_id,
                      aws_secret_access_key=aws_secret_access_key)

    s3_name = "calibrations/simstrat/calibration_{}.json".format(kwargs['execution_date'].strftime('%Y%m%dT%H%M'))
    s3.upload_file(local_file, bucket_key, s3_name)
    print("Results", bucket + "/" + s3_name)

def upload_simstrat_download_data(ds, **kwargs):
    bucket = kwargs["bucket"]
    aws_access_key_id = kwargs["AWS_ID"]
    aws_secret_access_key = kwargs["AWS_KEY"]
    repo = kwargs["repo"]
    bucket_key = bucket.split(".")[0].split("//")[1]

    s3 = boto3.client("s3",
                      aws_access_key_id=aws_access_key_id,
                      aws_secret_access_key=aws_secret_access_key)
    lakes = [l for l in os.listdir(os.path.join(repo, "runs")) if os.path.isdir(os.path.join(repo, "runs", l))]
    for lake in lakes:
        inputs = []
        results = []
        for root, _, files in os.walk(os.path.join(repo, "runs", lake)):
            for file in files:
                file_path = os.path.join(root, file)
                if "/Results/" in file_path:
                    if file_path.endswith(".dat"):
                        results.append(file_path)
                elif os.path.basename(file_path).startswith("simulation-snapshot") or file_path.endswith(".log"):
                    continue
                else:
                    inputs.append(file_path)
        zip_file = os.path.join(repo, "runs", lake, "{}.zip".format(lake))
        zip_files(os.path.join(repo, "runs", lake), inputs, zip_file)
        print("Uploading {} input files".format(lake))
        s3_name = "simulations/simstrat/downloads/{}.zip".format(lake)
        s3.upload_file(zip_file, bucket_key, s3_name)
        os.remove(zip_file)

        for local_file in results:
            print("   Uploading {} result file {}".format(lake, os.path.basename(local_file)))
            s3_name = "simulations/simstrat/results/{}/{}".format(lake, os.path.basename(local_file))
            s3.upload_file(local_file, bucket_key, s3_name)

