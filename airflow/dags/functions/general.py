import requests
import numpy as np
import pandas as pd
from datetime import datetime, timedelta, timezone
from scipy.interpolate import interp1d
from airflow.utils.email import send_email


def download_datalakes_data(datalakes_id, depth, start, stop):
    response = requests.get("https://api.datalakes-eawag.ch/datasetparameters?datasets_id={}".format(datalakes_id))
    if response.status_code != 200:
        raise ValueError("Failed to get datalakes parameters")
    parameters = response.json()

    time_axis = [p for p in parameters if p.get("parameters_id") == 1][0]["axis"]
    value_x = [p for p in parameters if p.get("parameters_id") == 5]
    if len(value_x) == 1 :
        value_axis = value_x[0]["axis"]
    elif "z" in value_x[0]["axis"]:
        value_axis = value_x[0]["axis"]
    else:
        value_xx = [p for p in value_x if "{}m".format(depth) in p["detail"]]
        if len(value_xx) == 1:
            value_axis = value_xx[0]["axis"]
        else:
            raise ValueError("Failed to find value parameter")
    if "z" in value_axis:
        d = [p for p in parameters if p.get("parameters_id") == 2]
        if len(d) == 0:
            d = [p for p in parameters if p.get("parameters_id") == 18]
        depth_axis = d[0]["axis"]

    response = requests.get("https://api.datalakes-eawag.ch/files?datasets_id={}".format(datalakes_id))
    if response.status_code != 200:
        raise ValueError("Failed to get datalakes files")
    files = response.json()
    files = [f for f in files if f["filetype"] == "json"]
    files = sorted(files, key=lambda x: datetime.strptime(x["maxdatetime"], "%Y-%m-%dT%H:%M:%S.%fZ"), reverse=True)
    file_ids = [files[0]["id"]]
    for i in range(20):
        if datetime.strptime(files[i]["mindatetime"], "%Y-%m-%dT%H:%M:%S.%fZ") > start:
            file_ids.append(files[ i +1]["id"])
        else:
            break
    time = []
    values = []

    for file_id in file_ids:
        response = requests.get("https://api.datalakes-eawag.ch/files/{}?get=raw".format(file_id))
        if response.status_code != 200:
            raise ValueError("Failed to get datalakes files")
        data = response.json()
        t = [datetime.fromtimestamp(d) for d in data[time_axis]]
        if "z" in value_axis:
            d_idx = min(range(len(data[depth_axis])), key=lambda i: abs(data[depth_axis][i] - depth))
            v = np.array(data[value_axis])[d_idx, :]
        else:
            v = np.array(data[value_axis])
        time = time + t
        values = values + v.tolist()

    df = pd.DataFrame({'time': time ,'value': values})
    df = df.dropna()
    df['time'] = pd.to_datetime(df['time'])
    df = df.sort_values(by='time')
    df = df[(df['time'] >= start) & (df['time'] <= stop)]
    if len(df) < 10:
        raise ValueError("Not enough data to compare")
    time = df['time'].dt.strftime('%Y-%m-%dT%H:%M:%S+00:00').tolist()
    values = df['value'].tolist()
    return {"time": time, "values": values}


def download_zurich_police_data(station, start, stop):
    start = start + timedelta(days=1)
    stop = stop + timedelta(days=1)
    time = []
    values = []
    all_results = []
    limit = 1000
    for i in range(5):
        offset = i * limit
        url = (
            f"https://tecdottir.herokuapp.com/measurements/{station}"
            f"?startDate={start.strftime('%Y-%m-%d')}"
            f"&endDate={stop.strftime('%Y-%m-%d')}"
            f"&sort=timestamp_cet%20asc"
            f"&limit={limit}&offset={offset}"
        )
        response = requests.get(url)
        if response.status_code != 200:
            return False
        data = response.json()
        results = data.get("result", [])
        all_results.extend(results)

        if len(results) < limit:
            break

    for i in range(len(all_results)):
        try:
            if all_results[i]["values"]["water_temperature"]["status"] == "ok":
                time.append(datetime.strptime(all_results[i]["timestamp"], "%Y-%m-%dT%H:%M:%S.%fZ").replace(
                    tzinfo=timezone.utc).strftime('%Y-%m-%dT%H:%M:%S+00:00'))
                values.append(float(all_results[i]["values"]["water_temperature"]["value"]))
        except:
            pass

    return {"time": time, "values": values}


def calculate_rmse(dataset1, dataset2):
    """
    Calculate the Root Mean Square Error (RMSE) between two datasets,
    Interpolates dataset2 to match dataset1's timestamps before computing RMSE.
    Only interpolates within the range of dataset2's timestamps (no extrapolation).
    Computes RMSE only for the overlapping time period where both datasets have data.

    Args:
        dataset1 (dict): A dictionary with 'time' and 'values' lists for the first dataset.
        dataset2 (dict): A dictionary with 'time' and 'values' lists for the second dataset.

    Returns:
        float: The RMSE value.
    """
    # Convert time strings to datetime objects
    time1 = np.array([datetime.fromisoformat(t) for t in dataset1['time']])
    time2 = np.array([datetime.fromisoformat(t) for t in dataset2['time']])
    values2 = np.array(dataset2['values'], dtype=float)

    # Convert datetime objects to numerical timestamps (seconds since epoch)
    time1_numeric = np.array([t.timestamp() for t in time1])
    time2_numeric = np.array([t.timestamp() for t in time2])

    # Determine overlapping time period
    min_time = max(time1_numeric[0], time2_numeric[0])
    max_time = min(time1_numeric[-1], time2_numeric[-1])

    # Filter time1 to only include timestamps within the overlapping range
    valid_range = (time1_numeric >= min_time) & (time1_numeric <= max_time)
    time1_numeric_interp = time1_numeric[valid_range]

    if len(time1_numeric_interp) == 0:
        raise ValueError("No valid timestamps for interpolation in overlapping period.")

    # Perform linear interpolation, ignoring extrapolation
    interp_func = interp1d(time2_numeric, values2, kind='linear', bounds_error=False, fill_value=np.nan)
    interpolated_values2 = interp_func(time1_numeric_interp)

    # Remove NaN values (which occur due to out-of-bounds timestamps)
    valid_indices = ~np.isnan(interpolated_values2)
    aligned_values1 = np.array(dataset1['values'], dtype=float)[valid_range][valid_indices]
    aligned_values2 = interpolated_values2[valid_indices]

    if len(aligned_values1) == 0 or len(aligned_values2) == 0:
        raise ValueError("No valid matching data points after interpolation in overlapping period.")

    # Compute RMSE
    squared_differences = (aligned_values1 - aligned_values2) ** 2
    mse = np.mean(squared_differences)
    rmse = np.sqrt(mse)

    return rmse