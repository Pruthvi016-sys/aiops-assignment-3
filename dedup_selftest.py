import os, pyarrow.parquet as pq, ray, ray.data, pandas as pd
import common as C, ray_clean as R
ray.init(address="auto", logging_level="WARNING", runtime_env={"env_vars": {"PYTHONPATH": os.getcwd()}})
f = C.input_files(1)[0]
base = R.stage_a(pq.ParquetFile(f).read_row_group(0).to_pandas().head(300_000))
dup = pd.concat([base, base.head(100_000)]).sample(frac=1, random_state=0).reset_index(drop=True)
expected = len(dup.drop_duplicates())
P = 16
got = (ray.data.from_pandas(dup).map_batches(R.make_add_part(P), batch_format="pandas", batch_size=50_000)
       .groupby("_part", num_partitions=P).map_groups(R.dedup_group, batch_format="pandas").count())
print(f"input rows: {len(dup):,} | expected unique: {expected:,} | ray result: {got:,}")
print("DEDUP SELFTEST:", "PASS" if got == expected else "FAIL")
