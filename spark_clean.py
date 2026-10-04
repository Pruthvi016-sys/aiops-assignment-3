"""Spark pipeline: ingest -> cleanse -> heavy join + Python UDF -> export Parquet."""
import argparse, functools, os, time
from pyspark.sql import SparkSession, functions as F, types as T
import common as C

CANON = {"VendorID": "long", "tpep_pickup_datetime": "timestamp",
         "tpep_dropoff_datetime": "timestamp", "passenger_count": "double",
         "trip_distance": "double", "PULocationID": "long", "DOLocationID": "long",
         "payment_type": "long", "fare_amount": "double", "tip_amount": "double",
         "total_amount": "double"}


def build_spark(master):
    spark = (SparkSession.builder.master(master).appName("spark_clean")
             .config("spark.executor.memory", "2800m")
             .config("spark.executor.cores", "4")
             .config("spark.cores.max", "8")
             .config("spark.driver.memory", "2g")
             .config("spark.local.dir", os.path.expanduser("~/spark_tmp"))
             .config("spark.sql.shuffle.partitions", "64")
             .config("spark.sql.session.timeZone", "UTC")
             .getOrCreate())
    spark.sparkContext.addPyFile(os.path.abspath("common.py"))  # so workers can import the UDF
    return spark


def load_trips(spark, files):
    parts = [spark.read.parquet(p).select(*[F.col(c).cast(t).alias(c) for c, t in CANON.items()])
             for p in files]
    return functools.reduce(lambda a, b: a.unionByName(b), parts)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--master", default="spark://127.0.0.1:7077")
    ap.add_argument("--max-files", type=int, default=0, help="0 = all files")
    ap.add_argument("--udf-mode", choices=["python", "native"], default="python")
    ap.add_argument("--dedup", choices=["on", "off"], default="on")
    ap.add_argument("--out", default="output/spark_clean")
    a = ap.parse_args()

    spark = build_spark(a.master)
    spark.sparkContext.setLogLevel("WARN")
    files = C.input_files(a.max_files)

    t0 = time.perf_counter()

    # 1. Ingestion
    trips = load_trips(spark, files).dropna(subset=C.KEEP_COLS)

    # 2. Cleansing
    ts = lambda s: F.lit(s).cast("timestamp")
    trips = trips.filter(
        (F.col("tpep_pickup_datetime") >= ts(C.DATE_MIN)) & (F.col("tpep_pickup_datetime") < ts(C.DATE_MAX))
        & F.col("passenger_count").between(C.PAX_MIN, C.PAX_MAX)
        & (F.col("trip_distance") > 0) & (F.col("trip_distance") <= C.DIST_MAX)
        & (F.col("fare_amount") > 0) & (F.col("fare_amount") <= C.FARE_MAX)
        & F.col("tip_amount").between(0, C.TIP_MAX)
        & (F.col("total_amount") > 0) & (F.col("total_amount") <= C.TOTAL_MAX))
    trips = trips.withColumn("duration_sec",
                             F.unix_timestamp("tpep_dropoff_datetime") - F.unix_timestamp("tpep_pickup_datetime"))
    trips = trips.filter(F.col("duration_sec").between(C.DUR_MIN_SEC, C.DUR_MAX_SEC))
    if a.dedup == "on":
        trips = trips.dropDuplicates(C.KEEP_COLS)

    # 3. Transformation: join with zone lookup (pickup and dropoff)
    zones = (spark.read.option("header", "true").csv(os.path.abspath(C.ZONES_CSV))
             .select(F.col("LocationID").cast("long").alias("LocationID"), "Borough", "Zone"))
    pu = F.broadcast(zones.select(F.col("LocationID").alias("PULocationID"),
                                  F.col("Borough").alias("PU_Borough"), F.col("Zone").alias("PU_Zone")))
    do = F.broadcast(zones.select(F.col("LocationID").alias("DOLocationID"),
                                  F.col("Borough").alias("DO_Borough"), F.col("Zone").alias("DO_Zone")))
    trips = trips.join(pu, "PULocationID", "inner").join(do, "DOLocationID", "inner")
    trips = trips.withColumn("pickup_hour", F.hour("tpep_pickup_datetime").cast("int"))

    # 3b. Derived feature: average speed via custom Python UDF (or a native equivalent for timing)
    if a.udf_mode == "python":
        speed_udf = F.udf(C.trip_speed_mph, T.DoubleType())
        trips = trips.withColumn("avg_speed_mph", speed_udf("trip_distance", "duration_sec"))
    else:
        trips = trips.withColumn("avg_speed_mph",
                                 F.round(F.col("trip_distance") / (F.col("duration_sec") / 3600.0), 4))
    trips = trips.filter((F.col("avg_speed_mph") > 0) & (F.col("avg_speed_mph") <= C.SPEED_MAX_MPH))

    out_cols = C.KEEP_COLS + ["PU_Borough", "PU_Zone", "DO_Borough", "DO_Zone",
                              "duration_sec", "pickup_hour", "avg_speed_mph"]

    # 4. Export
    trips.select(*out_cols).write.mode("overwrite").parquet(a.out)

    wall = time.perf_counter() - t0
    rows = spark.read.parquet(a.out).count()          # verification, NOT timed
    m = {"framework": "spark", "udf_mode": a.udf_mode, "files": len(files),
         "wall_clock_sec": round(wall, 2), "output_rows": rows}
    print("METRICS:", m)
    print("saved:", C.save_metrics("spark", a.udf_mode if a.max_files == 0 else f"{a.udf_mode}_test", m))
    spark.stop()


if __name__ == "__main__":
    main()
