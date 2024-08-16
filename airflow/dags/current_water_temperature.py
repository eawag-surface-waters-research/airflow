from datetime import datetime, timezone, timedelta
import os
import json
import pytz
import boto3
import requests
import tempfile

from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago
from airflow.models import Variable

from functions.email import report_failure
from functions.parse import ch1903_plus_to_latlng, parse_html_table, parse_html, html_find_all

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
    min_date = (datetime.now() - timedelta(days=14)).timestamp()

    # BAFU
    try:
        lookup = {"2606": "geneva", "2104": "walensee", "2152": "lucerne", "2030": "thun", "2457": "brienz"}
        response = requests.get("https://www.hydrodaten.admin.ch/web-hydro-maps/hydro_sensor_temperature.geojson")
        if response.status_code == 200:
            for f in response.json()["features"]:
                lat, lng = ch1903_plus_to_latlng(f["geometry"]["coordinates"][0], f["geometry"]["coordinates"][1])
                date = datetime.strptime(f["properties"]["last_measured_at"], "%Y-%m-%dT%H:%M:%S.%f%z").timestamp()
                lake = False
                if str(f["properties"]["key"]) in lookup:
                    lake = lookup[str(f["properties"]["key"])]
                if date > min_date:
                    features.append({
                        "type": "Feature",
                        "id": "bafu_" + f["properties"]["key"],
                        "properties": {
                            "label": f["properties"]["label"],
                            "last_time": date,
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
                    date = datetime.strptime(f["L1_timestamp"], "%Y-%m-%dT%H:%M:%S.%f%z").timestamp()
                    if f["metadata_river_name"] == "":
                        icon = "lake"
                    else:
                        icon = "river"
                    if date > min_date:
                        features.append({
                            "type": "Feature",
                            "id": "thurgau_" + f["metadata_station_no"],
                            "properties": {
                                "label": f["metadata_station_name"],
                                "last_time": date,
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
                    date = datetime.strptime(str(row.iloc[3] + row.iloc[2]), "%d.%m.%Y%H:%M")
                    date = swiss_timezone.localize(date).timestamp()
                    if date > min_date:
                        features.append({
                            "type": "Feature",
                            "id": stations[label]["id"],
                            "properties": {
                                "label": label,
                                "last_time": date,
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
                    date = datetime.strptime(data["time"], "%Y-%m-%dT%H:%M:%S.%fZ").replace(
                        tzinfo=timezone.utc).timestamp()
                    if date > min_date:
                        features.append({
                            "type": "Feature",
                            "id": "datalakes_{}".format(station["id"]),
                            "properties": {
                                "label": station["label"],
                                "last_time": date,
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

    # Zurich Police
    try:
        stations = [
            {
                "id": "tiefenbrunnen",
                "label": "Tiefenbrunnen",
                "coordinates": [8.561413, 47.348139]
            },
            {
                "id": "mythenquai",
                "label": "Mythenquai",
                "coordinates": [8.536529, 47.357655]
            }
        ]

        for station in stations:
            today = datetime.today()
            startDate = (today - timedelta(days=2)).strftime('%Y-%m-%d')
            endDate = (today + timedelta(days=1)).strftime('%Y-%m-%d')
            response = requests.get(
                "https://tecdottir.herokuapp.com/measurements/{}?startDate={}&endDate={}&sort=timestamp_cet%20desc&limit=1&offset=0".format(
                    station["id"], startDate, endDate))
            if response.status_code == 200:
                data = response.json()
                result = data["result"][0]
                date = datetime.strptime(result["timestamp"], "%Y-%m-%dT%H:%M:%S.%fZ").replace(
                    tzinfo=timezone.utc).timestamp()
                if date > min_date:
                    features.append({
                        "type": "Feature",
                        "id": "zurich_police_{}".format(station["id"]),
                        "properties": {
                            "label": station["label"],
                            "last_time": date,
                            "last_value": result["values"]["water_temperature"]["value"],
                            "url": "https://www.tecson-data.ch/zurich/{}/index.php".format(station["id"]),
                            "source": "Stadtpolizei Zürich",
                            "icon": "lake",
                            "lake": False
                        },
                        "geometry": {
                            "coordinates": station["coordinates"],
                            "type": "Point"}})
    except Exception as e:
        print(e)
        failed.append("Zurich Police")

    # MySwitzerland
    try:
        response = requests.get("https://alplakes-eawag.s3.eu-central-1.amazonaws.com/insitu/myswitzerland.json")
        if response.status_code == 200:
            data = response.json()
            for station in data:
                response = requests.get("https://sospo.myswitzerland.com/lakesides-swimming-pools/{}".format(station))
                if response.status_code == 200:
                    root = parse_html(response.text)
                    element = html_find_all(root, tag="a", class_name="AreaMap--link")
                    location = element[0].get('href').split("?q=")[-1].split(",")
                    coords = [float(location[1]), float(location[0])]
                    label = html_find_all(root, tag="h1", class_name="PageHeader--title")[0].text
                    date = datetime.strptime(
                        html_find_all(root, tag="div", class_name="QuickFactsWidget--info")[0].text.strip().split(": ",
                                                                                                                  1)[1],
                        "%d.%m.%Y, %H:%M")
                    date = swiss_timezone.localize(date).timestamp()
                    value = False
                    icon = "lake"
                    for info in html_find_all(root, tag="ul", class_name="QuickFacts--info"):
                        c = html_find_all(info, tag="li", class_name="QuickFacts--content")
                        if len(c) == 1:
                            content = c[0].text
                            value = html_find_all(info, tag="li", class_name="QuickFacts--value")[0].text
                            if content in ["Lake bathing", "River pools"] and value != "—":
                                if content == "River pools":
                                    icon = "river"
                                value = float(value.replace("°", ""))
                    if value and date > min_date:
                        features.append({
                            "type": "Feature",
                            "id": "myswitzerland_{}".format(station),
                            "properties": {
                                "label": label,
                                "last_time": date,
                                "last_value": value,
                                "url": "https://sospo.myswitzerland.com/lakesides-swimming-pools/{}".format(station),
                                "source": "MySwitzerland",
                                "icon": icon,
                                "lake": False
                            },
                            "geometry": {
                                "coordinates": coords,
                                "type": "Point"}})
    except Exception as e:
        print(e)
        failed.append("MySwitzerland")

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
