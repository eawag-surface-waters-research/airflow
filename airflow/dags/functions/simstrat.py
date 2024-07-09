import os
import json
import time
import boto3
import requests
import tempfile
from datetime import datetime, timedelta, timezone


def iso_to_unix(input_time):
    return int(datetime.fromisoformat(input_time).astimezone(timezone.utc).timestamp()) * 1000


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
            url = "{}/simulations/1d/point/simstrat/{}/{}/{}/{}/{}".format(
                api, lake["name"], parameter["simstrat_key"], start.strftime("%Y%m%d%H%M"), end.strftime("%Y%m%d%H%M"),
                lake["depths"][parameter["depth"]])
            response = requests.get(url)
            if response.status_code == 200:
                if "time" not in data:
                    data["time"] = [iso_to_unix(t) for t in response.json()["time"]]
                data[parameter["alplakes_key"]] = response.json()[parameter["simstrat_key"]]
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
        end = datetime.strptime(lake["end_date"], "%Y-%m-%d %H:%M")
        start = end - timedelta(days=365)
        for parameter in ["T", "OxygenSat"]:
            response = requests.get(
                "{}/simulations/1d/depthtime/simstrat/{}/{}/{}/{}".format(api, lake["name"], parameter,
                                                                          start.strftime("%Y%m%d%H%M"),
                                                                          end.strftime("%Y%m%d%H%M")))
            if response.status_code == 200:
                data = response.json()
                with tempfile.NamedTemporaryFile(mode='w', delete=False) as temp_file:
                    temp_filename = temp_file.name
                    json.dump(data, temp_file)
                s3.upload_file(temp_filename, bucket_key,
                               "simulations/simstrat/cache/{}/heatmap_{}.json".format(lake["name"], parameter))
                os.remove(temp_filename)
            else:
                print("Failed to retrieve simulations",
                      "{}/simulations/1d/depthtime/simstrat/{}/{}/{}/{}".format(api, lake["name"], parameter,
                                                                                start.strftime("%Y%m%d%H%M"),
                                                                                end.strftime("%Y%m%d%H%M")))

        # Linegraphs
        end = datetime.strptime(lake["end_date"], "%Y-%m-%d %H:%M")
        start = end - timedelta(days=5)
        start_year = datetime(datetime.now().year, 1, 1)
        depth = int(min(lake["depths"]))
        for parameter in ["T"]:
            response = requests.get(
                "{}/simulations/1d/point/simstrat/{}/{}/{}/{}/{}".format(api, lake["name"], parameter,
                                                                         start.strftime("%Y%m%d%H%M"),
                                                                         end.strftime("%Y%m%d%H%M"), depth))
            if response.status_code == 200:
                data = response.json()
                with tempfile.NamedTemporaryFile(mode='w', delete=False) as temp_file:
                    temp_filename = temp_file.name
                    json.dump(data, temp_file)
                s3.upload_file(temp_filename, bucket_key,
                               "simulations/simstrat/cache/{}/linegraph_{}.json".format(lake["name"], parameter))
                os.remove(temp_filename)
            else:
                print("Failed to retrieve simulations",
                      "{}/simulations/1d/point/simstrat/{}/{}/{}/{}/{}".format(api, lake["name"], parameter,
                                                                               start.strftime("%Y%m%d%H%M"),
                                                                               end.strftime("%Y%m%d%H%M"), depth))

        # DOY current year
        response = requests.get(
            "{}/simulations/1d/point/simstrat/{}/{}/{}/{}/{}?resample=daily".format(api, lake["name"], parameter,
                                                                                    start_year.strftime("%Y%m%d%H%M"),
                                                                                    end.strftime("%Y%m%d%H%M"), depth))
        if response.status_code == 200:
            data = response.json()
            with tempfile.NamedTemporaryFile(mode='w', delete=False) as temp_file:
                temp_filename = temp_file.name
                json.dump(data, temp_file)
            s3.upload_file(temp_filename, bucket_key,
                           "simulations/simstrat/cache/{}/doy_currentyear.json".format(lake["name"]))
            os.remove(temp_filename)
        else:
            print("Failed to retrieve simulations",
                  "{}/simulations/1d/point/simstrat/{}/{}/{}/{}/{}".format(api, lake["name"], "T",
                                                                           start_year.strftime("%Y%m%d%H%M"),
                                                                           end.strftime("%Y%m%d%H%M"), depth))


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
