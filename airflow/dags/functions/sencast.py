from rasterio.features import geometry_mask
from datetime import datetime, timedelta
import configparser
import numpy as np
import rasterio
import logging
import boto3
import json
import os


def write_logical_date_to_parameter_file(file, date=False, offset=0, **context):
    logging.info('Writing logical date to Sencast parameter file {}'.format(file))
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
    with open(file, "w") as f:
        params.write(f)


def create_sencast_operational_metadata(ds, **kwargs):
    satellites = kwargs["satellites"]
    bucket_name = kwargs["bucket"]
    filesystem = kwargs["filesystem"]

    s3 = boto3.client('s3')

    folder = os.path.join(filesystem, "media", "remotesensing", "files")
    if not os.path.exists(folder):
        os.makedirs(folder)

    lakes_file = os.path.join(os.path.dirname(folder), "lakes.json")
    if not os.path.isfile(lakes_file):
        s3.download_file(bucket_name, "metadata/lakes.json", lakes_file)

    with open(lakes_file) as f:
        polygons = json.load(f)

    lakes = [p["properties"]["Name"] for p in polygons["features"]]

    for satellite in satellites.keys():
        tiff_keys = []
        parameters = []
        prefix = satellites[satellite]
        response = s3.list_objects_v2(Bucket=bucket_name, Prefix=prefix)

        while True:
            for obj in response['Contents']:
                key = obj['Key']
                if key.lower().endswith('.tif') or key.lower().endswith('.tiff'):
                    path = key.split("/")
                    file = path[-1]
                    parts = file.split("_")
                    parameter = "_".join(parts[1:-3])

                    local_file = os.path.join(folder, file)
                    if not os.path.isfile(local_file):
                        print("Downloading")
                        s3.download_file(bucket_name, key, local_file)

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
                                {"lake": lake["properties"]["Name"], "processor": parts[0],
                                 "tile": parts[-1].split(".")[0], "datetime": parts[-2],
                                 "satellite": parts[-3], "parameter": parameter, "key": key, "pixels": pixels,
                                 "valid_pixels": valid_pixels, "prefix": "/".join(path[0:-1]),
                                 "file": file, "min": data_min, "max": data_max, "mean": data_mean})

                    if parameter not in parameters:
                        parameters.append(parameter)

            if response.get('NextContinuationToken'):
                response = s3.list_objects_v2(Bucket=bucket_name, Prefix=prefix,
                                              ContinuationToken=response['NextContinuationToken'])
            else:
                break

        for parameter in parameters:
            for lake in lakes:
                out = [{"dt": d["datetime"], "k": d["key"], "p": d["pixels"], "vp": d["valid_pixels"], "min": d["min"],
                        "max": d["max"], "mean": d["mean"]} for d in tiff_keys if d['parameter'] == parameter and d["lake"] == lake]
                if len(out) > 0:
                    s3.put_object(
                        Body=json.dumps(out),
                        Bucket=bucket_name,
                        Key='metadata/{}/{}_{}.json'.format(satellite, lake, parameter)
                    )
