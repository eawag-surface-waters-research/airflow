import os
import json
import time
import boto3
import tempfile
import requests
from datetime import datetime, timedelta, timezone

HYDROCRON_URL = "https://soto.podaac.earthdatacloud.nasa.gov/hydrocron/v1/timeseries"
HYDROCRON_FIELDS = ("lake_id,time_str,wse,wse_u,geoid_hght,quality_f,partial_f,"
                    "dark_frac,ice_clim_f,ice_dyn_f,xtrk_dist,area_total")
NUMERIC_FIELDS = ("wse", "wse_u", "geoid_hght", "quality_f", "partial_f",
                  "dark_frac", "ice_clim_f", "ice_dyn_f", "xtrk_dist", "area_total")
FLAG_FIELDS = ("quality_f", "partial_f", "ice_clim_f", "ice_dyn_f")

# SWOT writes missing values as large negative sentinels (-999, -999999999999 and friends).
# quality_f is the dangerous one: left as a number, -999 compares as better than 1 and admits
# junk passes, which produced 39 m standard deviations before it was caught.
FILL_LIMIT = -998.0

# Start of the SWOT science orbit, used when a lake has no stored data yet.
MISSION_START = "2023-07-01T00:00:00Z"
# Re-request a couple of days either side of the last stored pass so reprocessed passes that
# arrive late are picked up rather than missed forever.
OVERLAP_DAYS = 2

# A lake is only published once it holds more than this many fully trusted ("used") passes.
# Suspect passes are still written into the file, they just do not count towards the threshold.
MIN_USED_PASSES = 10


def hydrocron_lake_series(lake_id, start, end):
    """SWOT water surface elevation passes for one prior lake, as a list of dicts."""
    params = {"feature": "PriorLake", "feature_id": lake_id,
              "start_time": start, "end_time": end,
              "output": "csv", "fields": HYDROCRON_FIELDS}
    payload = None
    for attempt in range(3):
        try:
            response = requests.get(HYDROCRON_URL, params=params, timeout=300)
            if response.status_code == 400:
                # Hydrocron rejects ids it does not hold; that is not a transient failure.
                return []
            if response.status_code >= 500:
                raise requests.RequestException("HTTP {}".format(response.status_code))
            response.raise_for_status()
            payload = response.json()
            break
        except (requests.RequestException, ValueError) as error:
            if attempt == 2:
                raise ValueError("Hydrocron request failed for {}: {}".format(lake_id, error))
            time.sleep(10 * (attempt + 1))

    body = payload.get("results", {}).get("csv") or ""
    if not body.strip():
        return []

    lines = body.strip().split("\n")
    header = lines[0].split(",")
    passes = []
    for line in lines[1:]:
        values = dict(zip(header, line.split(",")))
        if values.get("time_str", "no_data") == "no_data":
            continue
        row = {"time": values["time_str"]}
        for field in NUMERIC_FIELDS:
            if field not in values:
                continue
            try:
                number = float(values[field])
            except (TypeError, ValueError):
                number = None
            if number is None or number <= FILL_LIMIT:
                row[field] = None
            elif field in FLAG_FIELDS:
                row[field] = int(number)
            else:
                row[field] = number
        if row.get("wse") is None:
            continue
        passes.append(row)
    return passes


def classify_quality(row):
    """Label a pass, following the same rules as the BAFU comparison script.

    quality_f 0 is used, 1 is suspect, and anything else including a missing flag is rejected.
    The two ice flags do not mean the same thing: ice_clim_f is a climatological prior whose
    middle value only means "cannot rule ice out" -- it fires all winter on lakes that never
    freeze, 31 of 93 passes on Lago Maggiore, and those passes verify against gauges as well
    as unflagged ones -- so it is dropped only at full cover. ice_dyn_f comes from optical
    imagery, so any ice at all disqualifies the pass.
    """
    quality = row.get("quality_f")
    if quality is None or quality not in (0, 1):
        return "rejected_quality"
    if (row.get("ice_clim_f") or 0) >= 2 or (row.get("ice_dyn_f") or 0) >= 1:
        return "ice"
    if quality == 1:
        return "suspect"
    return "used"


def cache_swot_data(ds, **kwargs):
    """Update the per-lake SWOT water level files in S3.

    Reads the lake list from the website metadata and the SWOT prior lake cache from
    <prefix>/metadata.json, then for each lake appends any passes newer than what is already
    stored. Values stay in their native EGM2008 datum; the cached per-country separations let
    the website convert at display time:

        H_national = wse + geoid_hght - separation_m
    """
    bucket = kwargs["bucket"]
    aws_access_key_id = kwargs["AWS_ID"]
    aws_secret_access_key = kwargs["AWS_KEY"]
    prefix = kwargs.get("prefix", "swot")
    bucket_key = bucket.split(".")[0].split("//")[1]

    s3 = boto3.client("s3",
                      aws_access_key_id=aws_access_key_id,
                      aws_secret_access_key=aws_secret_access_key)

    # Get version of website metadata
    branch = "master"
    try:
        response = requests.get(
            "https://raw.githubusercontent.com/eawag-surface-waters-research/alplakes-react/refs/heads/master/src/config.json")
        if response.status_code == 200:
            branch = response.json()["branch"]
    except:
        print("Failed to find branch")

    response = requests.get("{}/static/website/metadata/{}/list.json".format(bucket, branch),
                            timeout=60)
    if response.status_code != 200:
        raise ValueError("Unable to access {}/static/website/metadata/{}/list.json"
                         .format(bucket, branch))
    lakes = response.json()

    response = requests.get("{}/{}/metadata.json".format(bucket, prefix), timeout=60)
    if response.status_code != 200:
        raise ValueError("Unable to access {}/{}/metadata.json. It is produced by "
                         "create_swot_cache.py in the alplakes-react repository."
                         .format(bucket, prefix))
    swot_lakes = response.json()

    end = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    written, skipped, failed = 0, 0, 0

    for lake in lakes:
        key = lake["key"]
        cached = swot_lakes.get(key)
        if not cached or not cached.get("lake_id"):
            skipped = skipped + 1
            continue

        try:
            existing = []
            response = requests.get("{}/{}/{}.json".format(bucket, prefix, key), timeout=60)
            if response.status_code == 200:
                existing = response.json().get("data", [])

            start = MISSION_START
            if existing:
                last = datetime.strptime(existing[-1]["time"], "%Y-%m-%dT%H:%M:%SZ")
                start = (last.replace(tzinfo=timezone.utc) - timedelta(days=OVERLAP_DAYS)
                         ).strftime("%Y-%m-%dT%H:%M:%SZ")

            passes = hydrocron_lake_series(cached["lake_id"], start, end)
            for row in passes:
                row["qa"] = classify_quality(row)
            passes = [row for row in passes if row["qa"] in ("used", "suspect")]

            merged = {row["time"]: row for row in existing}
            merged.update({row["time"]: row for row in passes})
            data = [merged[time] for time in sorted(merged)]
            used = len([row for row in data if row.get("qa") == "used"])
            if used <= MIN_USED_PASSES:
                skipped = skipped + 1
                print("{}: {} used passes of {}, below the {} needed to publish"
                      .format(key, used, len(data), MIN_USED_PASSES))
                continue

            output = {"key": key,
                      "name": lake.get("name"),
                      "lake_id": cached["lake_id"],
                      "lake_name": cached.get("lake_name"),
                      "datum": "EGM2008",
                      "conversion": "H_national = wse + geoid_hght - separation_m",
                      "offsets": cached.get("offsets", {}),
                      "last_updated": end,
                      "data": data}

            with tempfile.NamedTemporaryFile(mode='w', delete=False) as temp_file:
                temp_filename = temp_file.name
                json.dump(output, temp_file)
            s3.upload_file(temp_filename, bucket_key, "{}/{}.json".format(prefix, key),
                           ExtraArgs={'ContentType': 'application/json'})
            os.remove(temp_filename)

            written = written + 1
            print("{}: {} passes, {} used ({} new)"
                  .format(key, len(data), used, len(data) - len(existing)))
        except Exception as error:
            # One unavailable lake must not abandon the remaining few hundred.
            failed = failed + 1
            print("{}: failed, {}".format(key, error))

    print("Wrote {} lakes, skipped {}, failed {}".format(written, skipped, failed))
    if written == 0:
        raise ValueError("No SWOT lake files were written")
