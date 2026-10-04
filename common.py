"""Shared pipeline spec. Spark and Ray both import these constants and the UDF,
so the preprocessing logic is identical by construction."""
import glob, json, os

RAW_GLOB = "data/raw/yellow_tripdata_*.parquet"
ZONES_CSV = "data/lookup/taxi_zone_lookup.csv"
RESULTS_DIR = "results"

KEEP_COLS = ["VendorID", "tpep_pickup_datetime", "tpep_dropoff_datetime",
             "passenger_count", "trip_distance", "PULocationID", "DOLocationID",
             "payment_type", "fare_amount", "tip_amount", "total_amount"]

# Cleansing rules
DATE_MIN, DATE_MAX = "2021-01-01 00:00:00", "2024-09-01 00:00:00"  # pickup in [MIN, MAX)
PAX_MIN, PAX_MAX = 1, 6
DIST_MAX = 100.0          # trip_distance in (0, DIST_MAX]
FARE_MAX = 500.0          # fare_amount in (0, FARE_MAX]
TIP_MAX = 500.0           # tip_amount in [0, TIP_MAX]
TOTAL_MAX = 1000.0        # total_amount in (0, TOTAL_MAX]
DUR_MIN_SEC, DUR_MAX_SEC = 60, 6 * 3600
SPEED_MAX_MPH = 80.0      # avg_speed_mph in (0, SPEED_MAX_MPH]


def trip_speed_mph(distance, duration_sec):
    """The custom Python function (the 'UDF'): average speed in miles per hour."""
    if distance is None or duration_sec is None or duration_sec <= 0:
        return None
    return round(float(distance) / (duration_sec / 3600.0), 4)


def input_files(max_files=0):
    files = sorted(glob.glob(RAW_GLOB))
    if max_files and max_files < len(files):
        if max_files == 1:
            idx = [0]
        else:  # evenly spaced INCLUDING first and last file -> covers both schema variants
            idx = sorted({round(i * (len(files) - 1) / (max_files - 1)) for i in range(max_files)})
        files = [files[i] for i in idx]
    return [os.path.abspath(f) for f in files]


def save_metrics(framework, mode, metrics):
    os.makedirs(RESULTS_DIR, exist_ok=True)
    path = os.path.join(RESULTS_DIR, f"{framework}_{mode}.json")
    with open(path, "w") as fh:
        json.dump(metrics, fh, indent=2)
    return path
