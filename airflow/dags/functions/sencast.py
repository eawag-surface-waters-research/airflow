from rasterio.features import geometry_mask
from datetime import datetime, timedelta
import configparser
import numpy as np
import rasterio
import logging
import boto3
import json
import os

logging.getLogger('boto3').setLevel(logging.ERROR)
logging.getLogger('botocore').setLevel(logging.ERROR)


def get_two_weeks_ago(dt):
    run_date = datetime.strptime(dt, "%Y-%m-%d") - timedelta(days=14)
    return run_date.strftime('%Y-%m-%d')


def write_logical_date_to_parameter_file(file, date=False, offset=0, **context):
    if date:
        input_date = datetime.strptime(date, "%Y-%m-%d")
    else:
        input_date = datetime.strptime(context["ds"], "%Y-%m-%d")

    input_date = input_date + timedelta(days=offset)

    if not os.path.isfile(file):
        raise RuntimeError("The parameter file could not be found: {}".format(file))
    params = configparser.ConfigParser()
    params.read(file)
    start = "{}T00:00:00.000Z".format(input_date.strftime("%Y-%m-%d"))
    end = "{}T23:59:59.999Z".format(input_date.strftime("%Y-%m-%d"))
    params['General']['start'] = start
    params['General']['end'] = end

    out_file = file.replace(".ini", "_{}.ini".format(input_date.strftime("%Y-%m-%d")))
    logging.info('Writing logical date to Sencast parameter file {}'.format(out_file))
    with open(out_file, "w") as f:
        params.write(f)


def create_sencast_operational_metadata(ds, **kwargs):
    satellites = kwargs["satellites"]
    bucket_name = kwargs["bucket"]
    bucket_url = kwargs["bucket_url"]
    filesystem = kwargs["filesystem"]
    failed = []

    s3 = boto3.client('s3')

    folder = os.path.join(filesystem, "media", "remotesensing", "files")
    if not os.path.exists(folder):
        os.makedirs(folder)

    lakes_file = os.path.join(os.path.dirname(folder), "lakes.json")
    if not os.path.isfile(lakes_file):
        s3.download_file(bucket_name, "metadata/lakes.json", lakes_file)

    with open(lakes_file) as f:
        polygons = json.load(f)

    lakes = [p["properties"]["key"] for p in polygons["features"]]

    metadata = {}

    for satellite in satellites.keys():
        tiff_keys = []
        parameters = []
        prefix = satellites[satellite]
        response = s3.list_objects_v2(Bucket=bucket_name, Prefix=prefix)
        tiff_file = os.path.join(folder, "{}_tiff_keys.json".format(satellite))

        if os.path.isfile(tiff_file):
            print("Reading from existing file {}".format("{}_tiff_keys.json".format(satellite)))
            with open(tiff_file, 'r') as f:
                tiff_keys = json.load(f)

        while True:
            print("Processing {} images.".format(len(response['Contents'])))
            for obj in response['Contents']:
                key = obj['Key']
                if key.lower().endswith('.tif') or key.lower().endswith('.tiff'):
                    path = key.split("/")
                    file = path[-1]
                    parts = file.split("_")
                    parameter = "_".join(parts[1:-3])

                    if parameter not in parameters:
                        parameters.append(parameter)

                    local_file = os.path.join(folder, file)
                    if not os.path.isfile(local_file):
                        print("Downloading: {}.".format(file))
                        s3.download_file(bucket_name, key, local_file)

                    if any(d.get("key") == key for d in tiff_keys):
                        continue

                    try:
                        raster = rasterio.open(local_file)

                        for lake in polygons["features"]:
                            mask = geometry_mask([lake["geometry"]], out_shape=raster.shape, transform=raster.transform,
                                                 invert=True)
                            masked_data = raster.read(masked=True)
                            masked_data = masked_data[:, mask]
                            data = masked_data[:, ~masked_data[0].mask]
                            pixels = masked_data[0].size
                            valid_data = data[0, data[1] == 0.0]
                            valid_pixels = len(valid_data)
                            if valid_pixels > 0:
                                data_min = np.nanmin(valid_data).astype(np.float64)
                                data_max = np.nanmax(valid_data).astype(np.float64)
                                data_mean = np.nanmean(valid_data).astype(np.float64)
                                tiff_keys.append(
                                    {"lake": lake["properties"]["key"], "processor": parts[0],
                                     "tile": parts[-1].split(".")[0], "datetime": parts[-2],
                                     "satellite": parts[-3], "parameter": parameter, "key": key, "pixels": pixels,
                                     "valid_pixels": valid_pixels, "prefix": "/".join(path[0:-1]),
                                     "file": file, "min": data_min, "max": data_max, "mean": data_mean})
                            else:
                                tiff_keys.append(
                                    {"lake": lake["properties"]["key"], "processor": parts[0],
                                     "tile": parts[-1].split(".")[0], "datetime": parts[-2],
                                     "satellite": parts[-3], "parameter": parameter, "key": key, "pixels": pixels,
                                     "valid_pixels": valid_pixels, "prefix": "/".join(path[0:-1]),
                                     "file": file})
                    except:
                        failed.append(file)
                        print("FAILED.")

            if response.get('NextContinuationToken'):
                response = s3.list_objects_v2(Bucket=bucket_name, Prefix=prefix,
                                              ContinuationToken=response['NextContinuationToken'])
            else:
                break

        with open(os.path.join(folder, "{}_tiff_keys.json".format(satellite)), "w") as f:
            json.dump(tiff_keys, f)

        for parameter in parameters:
            for lake in lakes:
                out = [{"dt": d["datetime"], "k": d["key"], "p": d["pixels"], "vp": d["valid_pixels"], "min": d["min"],
                        "max": d["max"], "mean": d["mean"]} for d in tiff_keys if
                       d['parameter'] == parameter and d["lake"] == lake and d["valid_pixels"] > 0]
                if len(out) > 0:
                    max_pixels = max(out, key=lambda x: x['p'])["p"]
                    out_public = [
                        {"datetime": d["datetime"], "name": d["key"].split("/")[-1],
                         "url": "{}/{}".format(bucket_url, d["key"]),
                         "valid_pixels": "{}%".format(round(float(d["valid_pixels"]) / float(max_pixels) * 100))} for d
                        in
                        tiff_keys if
                        d['parameter'] == parameter and d["lake"] == lake and d["valid_pixels"] > 0]
                    filtered = [d for d in out if d['vp'] / max_pixels > 0.1]
                    s3.put_object(
                        Body=json.dumps(out),
                        Bucket=bucket_name,
                        Key='metadata/{}/{}_{}.json'.format(satellite, lake, parameter)
                    )
                    s3.put_object(
                        Body=json.dumps(out_public),
                        Bucket=bucket_name,
                        Key='metadata/{}/{}_{}_public.json'.format(satellite, lake, parameter)
                    )
                    if len(filtered) > 0:
                        latest = max(filtered, key=lambda x: x['dt'])
                        s3.put_object(
                            Body=json.dumps(latest),
                            Bucket=bucket_name,
                            Key='metadata/{}/{}_{}_latest.json'.format(satellite, lake, parameter)
                        )
                        if lake not in metadata:
                            metadata[lake] = {}
                        if satellite not in metadata[lake]:
                            metadata[lake][satellite] = []
                        if parameter not in metadata[lake][satellite]:
                            metadata[lake][satellite].append(parameter)

        print("Operation complete, failed for the following files:")
        for fail in failed:
            print("   {}".format(fail))

    print("Uploading metadata")
    s3.put_object(
        Body=json.dumps(metadata),
        Bucket=bucket_name,
        Key='metadata/metadata.json'
    )
