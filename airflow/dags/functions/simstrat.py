import os
import json
import time
import boto3
import zipfile
import requests
import tempfile
from datetime import datetime, timedelta, timezone


def iso_to_unix(input_time):
    return int(datetime.fromisoformat(input_time).astimezone(timezone.utc).timestamp()) * 1000


def zip_files(base_folder, file_paths, output_filename):
    with zipfile.ZipFile(output_filename, 'w') as f:
        for file_path in file_paths:
            f.write(file_path, os.path.relpath(file_path, base_folder))


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
        {"simstrat_key": "T", "alplakes_key": "temperature", "depth": 0},
        {"simstrat_key": "TotalIceH", "alplakes_key": "ice", "depth": 0},
        {"simstrat_key": "OxygenSat", "alplakes_key": "oxygen", "depth": -1}
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

    for lake in lakes:
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
        s3_name = "simulations/simstrat/downloads/{}/{}.zip".format(lake, lake)
        s3.upload_file(zip_file, bucket_key, s3_name)
        os.remove(zip_file)

        for local_file in results:
            print("   Uploading {} result file {}".format(lake, os.path.basename(local_file)))
            s3_name = "simulations/simstrat/downloads/{}/{}".format(lake, os.path.basename(local_file))
            s3.upload_file(local_file, bucket_key, s3_name)

