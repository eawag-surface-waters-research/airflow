from datetime import datetime, timezone
import os
import json
import pytz
import boto3
import requests
import tempfile
import pandas as pd
from html.parser import HTMLParser

from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago
from airflow.models import Variable

from functions.email import report_failure

from airflow import DAG

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': days_ago(2),
    'email': ['james.runnalls@eawag.ch'],
    'email_on_failure': False,
    'email_on_retry': False,
    # 'retries': 1,
    # 'retry_delay': timedelta(minutes=5),
    'queue': 'api',
    # 'pool': 'backfill',
    # 'priority_weight': 10,
    # 'end_date': datetime(2016, 1, 1),
    # 'wait_for_downstream': False,
    # 'sla': timedelta(hours=2),
    # 'execution_timeout': timedelta(seconds=300),
    # 'on_failure_callback': some_function,
    # 'on_success_callback': some_other_function,
    # 'on_retry_callback': another_function,
    # 'sla_miss_callback': yet_another_function,
    # 'trigger_rule': 'all_success'
}
dag = DAG(
    'current_water_temperature',
    default_args=default_args,
    description='Collect current water temperature values',
    schedule_interval='*/20 * * * *',
    catchup=False,
    tags=['insitu', 'monitoring'],
)


def ch1903_plus_to_latlng(x, y):
    x_aux = (x - 2600000) / 1000000
    y_aux = (y - 1200000) / 1000000
    lat = 16.9023892 + 3.238272 * y_aux - 0.270978 * x_aux ** 2 - 0.002528 * y_aux ** 2 - 0.0447 * x_aux ** 2 * y_aux - 0.014 * y_aux ** 3
    lng = 2.6779094 + 4.728982 * x_aux + 0.791484 * x_aux * y_aux + 0.1306 * x_aux * y_aux ** 2 - 0.0436 * x_aux ** 3
    lat = (lat * 100) / 36
    lng = (lng * 100) / 36
    return lat, lng


class TableHTMLParser(HTMLParser):
    def __init__(self):
        super().__init__()
        self.rows = []
        self.current_row = []
        self.in_td = False

    def handle_starttag(self, tag, attrs):
        if tag == 'td':
            self.in_td = True

    def handle_endtag(self, tag):
        if tag == 'tr':
            if self.current_row:
                self.rows.append(self.current_row)
                self.current_row = []
        elif tag == 'td':
            self.in_td = False

    def handle_data(self, data):
        if self.in_td:
            self.current_row.append(data.strip())


def parse_html_table(html_table):
    parser = TableHTMLParser()
    parser.feed(html_table)
    df = pd.DataFrame(parser.rows)
    return df


def collect_water_temperature(ds, **kwargs):
    bucket = kwargs["bucket"]
    aws_access_key_id = kwargs["AWS_ID"]
    aws_secret_access_key = kwargs["AWS_KEY"]
    bucket_key = bucket.split(".")[0].split("//")[1]

    s3 = boto3.client("s3",
                      aws_access_key_id=aws_access_key_id,
                      aws_secret_access_key=aws_secret_access_key)

    features = []
    failed = []
    swiss_timezone = pytz.timezone("Europe/Zurich")

    # BAFU
    try:
        lookup = {"2606": "geneva", "2104": "walensee", "2152": "lucerne", "2030": "thun", "2457": "brienz"}
        response = requests.get("https://www.hydrodaten.admin.ch/web-hydro-maps/hydro_sensor_temperature.geojson")
        if response.status_code == 200:
            for f in response.json()["features"]:
                lat, lng = ch1903_plus_to_latlng(f["geometry"]["coordinates"][0], f["geometry"]["coordinates"][1])
                time = datetime.strptime(f["properties"]["last_measured_at"], "%Y-%m-%dT%H:%M:%S.%f%z").timestamp()
                lake = False
                if str(f["properties"]["key"]) in lookup:
                    lake = lookup[str(f["properties"]["key"])]
                features.append({
                    "type": "Feature",
                    "id": "bafu_" + f["properties"]["key"],
                    "properties": {
                        "label": f["properties"]["label"],
                        "last_time": time,
                        "last_value": float(f["properties"]["last_value"]),
                        "url": "https://www.hydrodaten.admin.ch/en/seen-und-fluesse/stations/{}".format(
                            f["properties"]["key"]),
                        "source": "BAFU Hydrodaten",
                        "icon": "river",
                        "lake": lake
                    },
                    "geometry": {
                        "coordinates": [lng, lat],
                        "type": "Point"}})
    except Exception as e:
        print(e)
        failed.append("BAFU")

    # Thurgau
    try:
        ids = ["M1090", "F1150", "00005"]
        response = requests.get("http://www.hydrodaten.tg.ch/data/internet/layers/30/index.json")
        if response.status_code == 200:
            for f in response.json():
                if f["metadata_station_no"] in ids:
                    time = datetime.strptime(f["L1_timestamp"], "%Y-%m-%dT%H:%M:%S.%f%z").timestamp()
                    if f["metadata_river_name"] == "":
                        icon = "lake"
                    else:
                        icon = "river"
                    features.append({
                        "type": "Feature",
                        "id": "thurgau_" + f["metadata_station_no"],
                        "properties": {
                            "label": f["metadata_station_name"],
                            "last_time": time,
                            "last_value": float(f["L1_ts_value"]),
                            "url": "http://www.hydrodaten.tg.ch/app/index.html#{}".format(f["metadata_station_no"]),
                            "source": "Kanton Thurgau",
                            "icon": icon,
                            "lake": "constance"
                        },
                        "geometry": {
                            "coordinates": [float(f["metadata_station_longitude"]),
                                            float(f["metadata_station_latitude"])],
                            "type": "Point"}})
    except Exception as e:
        print(e)
        failed.append("Thurgau")

    # Zurich
    try:
        stations = {
            "Zürichsee-Oberrieden": {
                "id": "zurich_502",
                "icon": "lake",
                "lake": "zurich",
                "coordinates": [8.584007, 47.272856]},
            "Limmat-Zch. KW Letten": {
                "id": "zurich_578",
                "icon": "river",
                "lake": "zurich",
                "coordinates": [8.531311, 47.387810]},
            "Glatt-Wuhrbrücke": {
                "id": "zurich_531",
                "icon": "river",
                "lake": "greifensee",
                "coordinates": [8.655083, 47.373080]},
            "Türlersee": {
                "id": "zurich_552",
                "icon": "river",
                "lake": "turlersee",
                "coordinates": [8.498728, 47.274612]},
            "Sihl-Blattwag": {
                "id": "zurich_547",
                "icon": "river",
                "lake": False,
                "coordinates": [8.674020, 47.174593]}
        }
        response = requests.get("https://hydroproweb.zh.ch/Listen/AktuelleWerte/AktWassertemp.html")
        if response.status_code == 200:
            df = parse_html_table(response.text.encode('latin-1').decode('utf-8'))
            for index, row in df.iterrows():
                label = row.iloc[0]
                if label in stations:
                    time = datetime.strptime(str(row.iloc[3] + row.iloc[2]), "%d.%m.%Y%H:%M")
                    time = swiss_timezone.localize(time).timestamp()
                    features.append({
                        "type": "Feature",
                        "id": stations[label]["id"],
                        "properties": {
                            "label": label,
                            "last_time": time,
                            "last_value": float(row.iloc[4]),
                            "url": "https://www.zh.ch/de/umwelt-tiere/wasser-gewaesser/messdaten/wassertemperaturen.html",
                            "source": "Kanton Zurich",
                            "icon": stations[label]["icon"],
                            "lake": stations[label]["lake"]
                        },
                        "geometry": {
                            "coordinates": stations[label]["coordinates"],
                            "type": "Point"}})
    except Exception as e:
        print(e)
        failed.append("Zurich")

    # Datalakes
    try:
        stations = [
            {"id": 1264, "parameters": "y", "label": "Kastanienbaum", "lake": "lucerne"},
            {"id": 515, "parameters": "z?x_index=3", "label": "Greifensee CTD", "lake": "greifensee"},
            {"id": 597, "parameters": "y", "label": "Buchillon", "lake": "geneva"},
            {"id": 448, "parameters": "z?x_index=0", "label": "LéXPLORE Chain", "lake": "geneva"},
            {"id": 1046, "parameters": "z?x_index=0", "label": "Hallwil Chain", "lake": "hallwil"},
            {"id": 956, "parameters": "z?x_index=0", "label": "Murten Chain", "lake": "murten"},
            {"id": 1077, "parameters": "z?x_index=0", "label": "Aegeri Idronaut", "lake": "ageri"},
        ]
        for station in stations:
            response = requests.get(
                "https://api.datalakes-eawag.ch/data/{}/{}".format(station["id"], station["parameters"]))
            if response.status_code == 200:
                data = response.json()
                response = requests.get("https://api.datalakes-eawag.ch/datasets/{}".format(station["id"]))
                if response.status_code == 200:
                    metadata = response.json()
                    time = datetime.strptime(data["time"], "%Y-%m-%dT%H:%M:%S.%fZ").replace(
                        tzinfo=timezone.utc).timestamp()
                    features.append({
                        "type": "Feature",
                        "id": "datalakes_{}".format(station["id"]),
                        "properties": {
                            "label": station["label"],
                            "last_time": time,
                            "last_value": data["value"],
                            "url": "https://www.datalakes-eawag.ch/datadetail/{}".format(station["id"]),
                            "source": "Datalakes",
                            "icon": "lake",
                            "lake": station["lake"]
                        },
                        "geometry": {
                            "coordinates": [metadata["longitude"], metadata["latitude"]],
                            "type": "Point"}})
    except Exception as e:
        print(e)
        failed.append("Datalakes")

    response = requests.get("{}/insitu/summary/water_temperature.geojson".format(bucket))
    if response.status_code == 200:
        ids = [f["id"] for f in features]
        old_features = response.json()["features"]
        for f in old_features:
            if f["id"] not in ids:
                features.append(f)

    geojson = {
        "type": "FeatureCollection",
        "name": "Current Water Temperatures",
        "crs": {"type": "name", "properties": {"name": "urn:ogc:def:crs:OGC:1.3:CRS84"}},
        "features": features
    }

    with tempfile.NamedTemporaryFile(mode='w', delete=False) as temp_file:
        temp_filename = temp_file.name
        json.dump(geojson, temp_file)
    s3.upload_file(temp_filename, bucket_key, "insitu/summary/water_temperature.geojson")
    os.remove(temp_filename)

    if len(failed) > 0:
        raise ValueError("Failed for {}".format(", ".join(failed)))


water_temperature = PythonOperator(
        task_id='water_temperature',
        python_callable=collect_water_temperature,
        op_kwargs={
            "bucket": "https://alplakes-eawag.s3.eu-central-1.amazonaws.com",
            'AWS_ID': Variable.get("AWS_ACCESS_KEY_ID"),
            'AWS_KEY': Variable.get("AWS_SECRET_ACCESS_KEY")
        },
        on_failure_callback=report_failure,
        dag=dag,
    )

water_temperature
