from datetime import datetime, timedelta
import configparser
import logging
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
