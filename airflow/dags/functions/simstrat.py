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

    forecast = {}
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
    s3.upload_file(temp_filename, bucket_key, "simulations/forecast2.json")
    os.remove(temp_filename)

    if failed > 0:
        raise ValueError("Failed to collect data for {} lakes, see log for details.".format(failed))


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
