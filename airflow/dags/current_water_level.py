from datetime import datetime, timezone, timedelta
import os
import json
import boto3
import requests
import tempfile

from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago
from airflow.models import Variable

from functions.email import report_failure
from functions.parse import ch1903_plus_to_latlng

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
    'current_water_level',
    default_args=default_args,
    description='Collect current water level values',
    schedule_interval='*/20 * * * *',
    catchup=False,
    tags=['insitu', 'monitoring'],
)

def collect_water_level(ds, **kwargs):
    bucket = kwargs["bucket"]
    aws_access_key_id = kwargs["AWS_ID"]
    aws_secret_access_key = kwargs["AWS_KEY"]
    bucket_key = bucket.split(".")[0].split("//")[1]

    s3 = boto3.client("s3",
                      aws_access_key_id=aws_access_key_id,
                      aws_secret_access_key=aws_secret_access_key)

    features = []
    failed = []
    min_date = (datetime.now() - timedelta(days=14)).timestamp()

    # BAFU
    try:
        lookup = {"2031": "ageri", "2137": "baldegg", "2208": "biel", "2032": "constance", "2043": "constance",
                  "2023": "brienz", "2097": "hallwil", "2007": "joux", "2154": "neuchatel", "2642": "neuchatel",
                  "2057": "brenet", "2026": "geneva", "2028": "geneva", "2027": "geneva", "2101": "lugano",
                  "2021": "lugano", "2074": "maggiore", "2022": "maggiore", "2484": "lauerz", "2004": "murten",
                  "2088": "sarnen", "2168": "sempach", "2072": "sils", "2073": "silvaplana", "2066": "stmoritz",
                  "2093": "thun", "2025": "lucerne", "2207": "lucerne", "2118": "walensee", "2017": "zug",
                  "2209": "zurich"}
        response = requests.get("https://www.hydrodaten.admin.ch/web-hydro-maps/hydro_sensor_pq.geojson")
        if response.status_code == 200:
            for f in response.json()["features"]:
                lat, lng = ch1903_plus_to_latlng(f["geometry"]["coordinates"][0], f["geometry"]["coordinates"][1])
                date = datetime.strptime(f["properties"]["last_measured_at"], "%Y-%m-%dT%H:%M:%S.%f%z").timestamp()
                if f["properties"]["kind"] == "lake" and str(f["properties"]["key"]) in lookup:
                    if date > min_date:
                        features.append({
                            "type": "Feature",
                            "id": "bafu_" + f["properties"]["key"],
                            "properties": {
                                "label": f["properties"]["label"],
                                "unit": f["properties"]["unit"],
                                "last_time": date,
                                "last_value": float(f["properties"]["last_value"]),
                                "url": "https://www.hydrodaten.admin.ch/en/seen-und-fluesse/stations/{}".format(
                                    f["properties"]["key"]),
                                "source": "BAFU Hydrodaten",
                                "icon": "lake",
                                "lake": lookup[str(f["properties"]["key"])]
                            },
                            "geometry": {
                                "coordinates": [lng, lat],
                                "type": "Point"}})

    except Exception as e:
        print(e)
        failed.append("BAFU")

    response = requests.get("{}/insitu/summary/water_level.geojson".format(bucket))
    if response.status_code == 200:
        ids = [f["id"] for f in features]
        old_features = response.json()["features"]
        for f in old_features:
            if f["id"] not in ids:
                features.append(f)

    geojson = {
        "type": "FeatureCollection",
        "name": "Current Water Level",
        "crs": {"type": "name", "properties": {"name": "urn:ogc:def:crs:OGC:1.3:CRS84"}},
        "features": features
    }

    with tempfile.NamedTemporaryFile(mode='w', delete=False) as temp_file:
        temp_filename = temp_file.name
        json.dump(geojson, temp_file)
    s3.upload_file(temp_filename, bucket_key, "insitu/summary/water_level.geojson")
    os.remove(temp_filename)

    if len(failed) > 0:
        raise ValueError("Failed for {}".format(", ".join(failed)))


water_level = PythonOperator(
        task_id='water_level',
        python_callable=collect_water_level,
        op_kwargs={
            "bucket": "https://alplakes-eawag.s3.eu-central-1.amazonaws.com",
            'AWS_ID': Variable.get("AWS_ACCESS_KEY_ID"),
            'AWS_KEY': Variable.get("AWS_SECRET_ACCESS_KEY")
        },
        on_failure_callback=report_failure,
        dag=dag,
    )

water_level
