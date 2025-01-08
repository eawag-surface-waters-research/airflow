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


def write_logical_date_and_tile_to_parameter_file(folder, prefix, tile, run_date, **context):
    input_date = run_date(context["ds"])
    file = os.path.join(folder, prefix+".ini")
    if not os.path.isfile(file):
        raise RuntimeError("The parameter file could not be found: {}".format(file))

    out_file = file.replace(".ini", "_{}_{}.ini".format(input_date, tile))
    out_file_download = file.replace(".ini", "_{}_{}_download.ini".format(input_date, tile))

    logging.info('Writing logical date and tile to Sencast parameter file {}'.format(out_file))
    params = configparser.ConfigParser()
    params.read(file)

    formatted_date = datetime.strptime(input_date, "%Y%m%d").strftime("%Y-%m-%d")
    start = "{}T00:00:00.000Z".format(formatted_date)
    end = "{}T23:59:59.999Z".format(formatted_date)
    params['General']['start'] = start
    params['General']['end'] = end
    params['General']['tiles'] = tile
    with open(out_file, "w") as f:
        params.write(f)

    logging.info('Writing logical date and tile to Sencast parameter file {}'.format(out_file_download))
    params['General']['processors'] = ""
    params['General']['adapters'] = ""
    params['General']['remove_inputs'] = "False"
    with open(out_file_download, "w") as f:
        params.write(f)


def write_logical_date_to_parameter_file(folder, prefix, run_date, **context):
    input_date = run_date(context["ds"])
    file = os.path.join(folder, prefix+".ini")
    if not os.path.isfile(file):
        raise RuntimeError("The parameter file could not be found: {}".format(file))

    out_file = file.replace(".ini", "_{}.ini".format(input_date))
    out_file_download = file.replace(".ini", "_{}_download.ini".format(input_date))

    logging.info('Writing logical date and tile to Sencast parameter file {}'.format(out_file))
    params = configparser.ConfigParser()
    params.read(file)

    formatted_date = datetime.strptime(input_date, "%Y%m%d").strftime("%Y-%m-%d")
    start = "{}T00:00:00.000Z".format(formatted_date)
    end = "{}T23:59:59.999Z".format(formatted_date)
    params['General']['start'] = start
    params['General']['end'] = end
    with open(out_file, "w") as f:
        params.write(f)

    logging.info('Writing logical date and tile to Sencast parameter file {}'.format(out_file_download))
    params['General']['processors'] = ""
    params['General']['adapters'] = ""
    params['General']['remove_inputs'] = "False"
    with open(out_file_download, "w") as f:
        params.write(f)
