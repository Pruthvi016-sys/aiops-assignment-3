"""Pipeline-parity check: exact row count + order-independent row hash, Spark vs Ray."""
import sys
import numpy as np, pandas as pd, pyarrow as pa, pyarrow.compute as pc, pyarrow.dataset as ds

OUT_COLS = ["VendorID", "tpep_pickup_datetime", "tpep_dropoff_datetime", "passenger_count",
            "trip_distance", "PULocationID", "DOLocationID", "payment_type", "fare_amount",
            "tip_amount", "total_amount", "PU_Borough", "PU_Zone", "DO_Borough", "DO_Zone",
            "duration_sec", "pickup_hour", "avg_speed_mph"]


def canon(t):
    cols = []
    for f in t.schema:
        c = t[f.name]
        if pa.types.is_timestamp(f.type):
            c = c.cast(pa.int64())
            if f.type.unit == "ns": c = pc.divide(c, 1000)
            elif f.type.unit == "ms": c = pc.multiply(c, 1000)
            elif f.type.unit == "s": c = pc.multiply(c, 1_000_000)
        elif pa.types.is_integer(f.type): c = c.cast(pa.int64())
        elif pa.types.is_floating(f.type): c = c.cast(pa.float64())
        elif pa.types.is_string(f.type) or pa.types.is_large_string(f.type): c = c.cast(pa.string())
        cols.append(c)
    return pa.table(cols, names=t.schema.names)


def fingerprint(path, per_col=True):
    rows, h = 0, 0
    colh = {c: 0 for c in OUT_COLS}
    sums = {"total_amount": 0.0, "avg_speed_mph": 0.0, "trip_distance": 0.0}
    for b in ds.dataset(path, format="parquet").to_batches(columns=OUT_COLS, batch_size=500_000):
        df = canon(pa.Table.from_batches([b])).to_pandas()
        rows += len(df)
        h = (h + int(pd.util.hash_pandas_object(df, index=False).values.sum(dtype=np.uint64))) % (1 << 64)
        if per_col:
            for c in OUT_COLS:
                colh[c] = (colh[c] + int(pd.util.hash_pandas_object(df[c], index=False).values.sum(dtype=np.uint64))) % (1 << 64)
        for k in sums: sums[k] += float(df[k].sum())
    return rows, h, colh, sums


if __name__ == "__main__":
    a, b = sys.argv[1], sys.argv[2]
    ra, ha, ca, sa = fingerprint(a)
    rb, hb, cb, sb = fingerprint(b)
    print(f"{'':16s}{'rows':>14s}  row-hash")
    print(f"{a:16s}{ra:>14,d}  {ha:016x}")
    print(f"{b:16s}{rb:>14,d}  {hb:016x}")
    for k in sa: print(f"  sum({k}): {sa[k]:,.2f} vs {sb[k]:,.2f}")
    bad = [c for c in OUT_COLS if ca[c] != cb[c]]
    print("per-column mismatches:", bad if bad else "none")
    print("PARITY:", "PASS (identical)" if (ra == rb and ha == hb) else "FAIL")
