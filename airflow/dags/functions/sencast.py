from datetime import datetime, timedelta
import configparser
import logging
import random
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
                    tiff_keys.append({"processor": parts[0], "tile": parts[-1].split(".")[0], "datetime": parts[-2],
                                      "satellite": parts[-3], "parameter": parameter, "key": key, "pixels": 1810,
                                      "valid_pixels": random.randint(0, 1810), "prefix": "/".join(path[0:-1]),
                                      "file": file, "min": 0, "max": random.randint(5, 15)})
                    if parameter not in parameters:
                        parameters.append(parameter)

            if response.get('NextContinuationToken'):
                response = s3.list_objects_v2(Bucket=bucket_name, Prefix=prefix,
                                              ContinuationToken=response['NextContinuationToken'])
            else:
                break

        for parameter in parameters:
            out = [{"dt": d["datetime"], "k": d["key"], "p": d["pixels"], "vp": d["valid_pixels"], "min": d["min"],
                    "max": d["max"]} for d in tiff_keys if d['parameter'] == parameter]
            s3.put_object(
                Body=json.dumps(out),
                Bucket=bucket_name,
                Key='metadata/{}/{}.json'.format(satellite, parameter)
            )
