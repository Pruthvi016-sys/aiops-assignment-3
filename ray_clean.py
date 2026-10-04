"""Ray pipeline: ingest -> cleanse -> heavy join + Python UDF -> export Parquet.
Uses the same rules and the same UDF (common.py) as spark_clean.py."""
import argparse, os, shutil, time
import numpy as np
import pandas as pd
import ray, ray.data
import common as C

CANON_PD = {"VendorID": "int64", "tpep_pickup_datetime": "datetime64[us]",
            "tpep_dropoff_datetime": "datetime64[us]", "passenger_count": "float64",
            "trip_distance": "float64", "PULocationID": "int64", "DOLocationID": "int64",
            "payment_type": "int64", "fare_amount": "float64", "tip_amount": "float64",
            "total_amount": "float64"}
OUT_COLS = C.KEEP_COLS + ["PU_Borough", "PU_Zone", "DO_Borough", "DO_Zone",
                          "duration_sec", "pickup_hour", "avg_speed_mph"]
BATCH = 50_000


def _dur(df):  # whole-second difference, same as Spark's unix_timestamp subtraction
    p = df["tpep_pickup_datetime"].values.astype("datetime64[s]").astype("int64")
    d = df["tpep_dropoff_datetime"].values.astype("datetime64[s]").astype("int64")
    return d - p


def stage_a(df):
    """Ingest + cleanse (row-wise rules). Casts to canonical types after dropping nulls."""
    df = df[C.KEEP_COLS].dropna().astype(CANON_PD)
    pu = df["tpep_pickup_datetime"]
    m = ((pu >= pd.Timestamp(C.DATE_MIN)) & (pu < pd.Timestamp(C.DATE_MAX))
         & df["passenger_count"].between(C.PAX_MIN, C.PAX_MAX)
         & (df["trip_distance"] > 0) & (df["trip_distance"] <= C.DIST_MAX)
         & (df["fare_amount"] > 0) & (df["fare_amount"] <= C.FARE_MAX)
         & df["tip_amount"].between(0, C.TIP_MAX)
         & (df["total_amount"] > 0) & (df["total_amount"] <= C.TOTAL_MAX))
    df = df[m]
    dur = _dur(df)
    return df[(dur >= C.DUR_MIN_SEC) & (dur <= C.DUR_MAX_SEC)].reset_index(drop=True)


def make_add_part(P):
    def add_part(df):
        h = pd.util.hash_pandas_object(df[C.KEEP_COLS], index=False).values
        df["_part"] = (h % np.uint64(P)).astype("int64")
        return df
    return add_part


def dedup_group(df):
    """Exact duplicates share a hash, hence a partition; drop them with pandas."""
    return df.drop_duplicates(subset=C.KEEP_COLS).drop(columns=["_part"])


def _add_hash(df):
    df["_h"] = pd.util.hash_pandas_object(df[C.KEEP_COLS], index=False).values.view("int64")
    return df


def _stream(files):
    parts = [ray.data.read_parquet(f, columns=C.KEEP_COLS)
             .map_batches(stage_a, batch_format="pandas", batch_size=BATCH) for f in files]
    ds = parts[0].union(*parts[1:]) if len(parts) > 1 else parts[0]
    return ds.map_batches(_add_hash, batch_format="pandas", batch_size=BATCH)


def _only_dups(df):
    c = [x for x in df.columns if x != "_h"][0]
    return df[df[c] > 1][["_h"]]


def twopass_dedup(files):
    """Exact global dedup with a small shuffle (see report): only an 8-byte row hash is shuffled;
    rows sharing a hash are then compared on all 11 columns, so collisions cannot drop real rows."""
    counts = _stream(files).select_columns(["_h"]).groupby("_h").count()
    dup_df = counts.map_batches(_only_dups, batch_format="pandas").to_pandas()
    dup_h = (np.asarray(dup_df["_h"].values, dtype="int64") if "_h" in dup_df.columns
             else np.array([], dtype="int64"))
    print(f"[twopass] hashes seen more than once: {len(dup_h)}", flush=True)
    keep = _stream(files).map_batches(lambda df: df[~np.isin(df["_h"].values, dup_h)],
                                      batch_format="pandas", batch_size=BATCH).drop_columns(["_h"])
    if len(dup_h) == 0:
        return keep
    dups = (_stream(files).map_batches(lambda df: df[np.isin(df["_h"].values, dup_h)],
                                       batch_format="pandas", batch_size=BATCH).to_pandas())
    dups = dups.drop_duplicates(subset=C.KEEP_COLS).drop(columns=["_h"])
    return keep.union(ray.data.from_pandas(dups))


def clean_file(batch):
    """One whole monthly file per call: read, cleanse, exact-dedup inside the task. No shuffle."""
    import pyarrow.parquet as pq
    outs = []
    for p in batch["path"]:
        df = pq.read_table(p, columns=C.KEEP_COLS).to_pandas()
        outs.append(stage_a(df).drop_duplicates(subset=C.KEEP_COLS))
    return pd.concat(outs, ignore_index=True)


def load_zones():
    # keep_default_na=False: pandas would turn the literal string "N/A" into NaN; Spark keeps it
    z = pd.read_csv(C.ZONES_CSV, keep_default_na=False, na_values=[])
    z = z[["LocationID", "Borough", "Zone"]]
    zpu = z.rename(columns={"LocationID": "PULocationID", "Borough": "PU_Borough", "Zone": "PU_Zone"})
    zdo = z.rename(columns={"LocationID": "DOLocationID", "Borough": "DO_Borough", "Zone": "DO_Zone"})
    return zpu, zdo


def make_stage_c(zpu, zdo, udf_mode):
    def stage_c(df):
        """Join with zone lookup + derived features + custom Python function."""
        df = df.merge(zpu, on="PULocationID", how="inner").merge(zdo, on="DOLocationID", how="inner")
        df["duration_sec"] = _dur(df)
        df["pickup_hour"] = df["tpep_pickup_datetime"].dt.hour.astype("int32")
        if udf_mode == "python":   # plain Python function, called row by row, no JVM involved
            df["avg_speed_mph"] = [C.trip_speed_mph(d, s) for d, s in
                                   zip(df["trip_distance"].tolist(), df["duration_sec"].tolist())]
        else:                      # vectorised numpy equivalent (timing comparison only)
            df["avg_speed_mph"] = np.round(df["trip_distance"].values / (df["duration_sec"].values / 3600.0), 4)
        df["avg_speed_mph"] = df["avg_speed_mph"].astype("float64")
        df = df[(df["avg_speed_mph"] > 0) & (df["avg_speed_mph"] <= C.SPEED_MAX_MPH)]
        return df[OUT_COLS].reset_index(drop=True)
    return stage_c


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--address", default="127.0.0.1:6379")
    ap.add_argument("--max-files", type=int, default=0, help="0 = all files")
    ap.add_argument("--udf-mode", choices=["python", "native"], default="python")
    ap.add_argument("--dedup", choices=["on", "off", "hash", "twopass", "perfile"], default="perfile")
    ap.add_argument("--dedup-partitions", type=int, default=128)
    ap.add_argument("--out", default="output/ray_clean")
    a = ap.parse_args()

    ray.init(address=a.address, logging_level="WARNING",
             runtime_env={"env_vars": {"PYTHONPATH": os.getcwd()}})   # workers can import common.py
    files = C.input_files(a.max_files)
    zpu, zdo = load_zones()
    shutil.rmtree(a.out, ignore_errors=True)

    t0 = time.perf_counter()

    # 1+2. Ingestion + cleansing (each file read on its own: the two schema variants never mix)
    parts = [ray.data.read_parquet(f, columns=C.KEEP_COLS)
             .map_batches(stage_a, batch_format="pandas", batch_size=BATCH) for f in files]
    ds = parts[0].union(*parts[1:]) if len(parts) > 1 else parts[0]

    # 2b. Exact duplicate removal (distinct over all 11 columns == Spark dropDuplicates)
    if a.dedup == "perfile":
        ds = ray.data.from_items([{"path": f} for f in files]).map_batches(
            clean_file, batch_format="pandas", batch_size=1, num_cpus=2)
    elif a.dedup == "twopass":
        ds = twopass_dedup(files)
    elif a.dedup == "hash":
        ds = ds.map_batches(make_add_part(a.dedup_partitions), batch_format="pandas", batch_size=BATCH)
        ds = ds.groupby("_part", num_partitions=a.dedup_partitions).map_groups(dedup_group, batch_format="pandas")
    elif a.dedup == "on":
        ds = ds.groupby(C.KEEP_COLS, num_partitions=a.dedup_partitions).count().select_columns(C.KEEP_COLS)

    # 3. Join + custom Python function
    ds = ds.map_batches(make_stage_c(zpu, zdo, a.udf_mode), batch_format="pandas", batch_size=BATCH)

    # 4. Export
    ds.write_parquet(a.out)

    wall = time.perf_counter() - t0
    rows = ray.data.read_parquet(a.out).count()        # verification, NOT timed
    m = {"framework": "ray", "udf_mode": a.udf_mode, "files": len(files),
         "wall_clock_sec": round(wall, 2), "output_rows": rows}
    print("METRICS:", m)
    print("saved:", C.save_metrics("ray", a.udf_mode if a.max_files == 0 else f"{a.udf_mode}_test", m))


if __name__ == "__main__":
    main()
