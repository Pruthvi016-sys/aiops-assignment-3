# DA3408 Assignment 3 — Spark vs. Ray

**Name:** M M Pruthvi Raj  **Roll no.:** DA24B016
**Report:** `report.pdf`

The same NYC taxi preprocessing pipeline (ingest → cleanse → join + custom Python UDF → export Parquet) is implemented in PySpark (`spark_clean.py`) and Ray Data (`ray_clean.py`) and run on a two-node cluster for each framework. Both pipelines import their rules and the Python UDF from `common.py`.

This README covers setup and how to run the code. Results and analysis are in the report.

## Requirements

- Linux, Java 17, Python 3.11 (conda), about 25 GB free disk, 14 GB RAM
- Spark 3.5.9, Ray 2.59.0 (versions pinned in `requirements.txt`)
- Both clusters run on one machine (12 logical cores). Run only one cluster at a time.

## 1. Setup

### Python environment

```bash
sudo apt install -y openjdk-17-jdk git wget
conda create -n sparkray python=3.11 -y
conda activate sparkray
pip install -r requirements.txt

# Ray 2.59 enables token authentication by default; disable it for the local cluster
conda env config vars set RAY_AUTH_MODE=disabled -n sparkray
conda deactivate && conda activate sparkray
echo $RAY_AUTH_MODE        # should print: disabled
```

### Spark distribution

The pip `pyspark` package does not include the cluster start scripts, so download the Spark 3.5.9 distribution (same version as PySpark):

```bash
cd ~
wget -c https://archive.apache.org/dist/spark/spark-3.5.9/spark-3.5.9-bin-hadoop3.tgz
tar -xzf spark-3.5.9-bin-hadoop3.tgz
```

The cluster scripts expect it at `~/spark-3.5.9-bin-hadoop3`. If you unpack it elsewhere, edit the `SPARK_HOME` line in `scripts/start_spark_cluster.sh` and `scripts/stop_spark_cluster.sh`. Do not export `SPARK_HOME` in your shell.

### Data

NYC TLC Yellow Taxi Parquet files, Jan 2021 – Aug 2024 (44 files, 2.1 GB), plus the zone lookup table:

```bash
cd <repo>
mkdir -p data/raw data/lookup
for y in 2021 2022 2023 2024; do
  for m in 01 02 03 04 05 06 07 08 09 10 11 12; do
    [ "$y" = "2024" ] && [ "$m" -gt 08 ] && break
    wget -c -q --show-progress --tries=20 --retry-connrefused -T 30 -P data/raw \
      "https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_${y}-${m}.parquet"
  done
done
wget -c -P data/lookup https://d37ci6vzurychx.cloudfront.net/misc/taxi_zone_lookup.csv
ls data/raw | wc -l        # should print 44
```

## 2. Start a cluster

Stop the other cluster first.

### Spark: 1 master + 2 workers (4 cores, 3 GB each)

```bash
bash scripts/stop_ray_cluster.sh
bash scripts/start_spark_cluster.sh
sleep 8
curl -sL --noproxy '*' http://127.0.0.1:8080/json/ | python -c \
  "import sys,json; d=json.load(sys.stdin); print('alive workers:', d['aliveworkers'], '| cores:', d['cores'])"
# expected: alive workers: 2 | cores: 8
```

Master UI: http://localhost:8080 (workers on :8081 and :8082). While a job runs, its UI is at http://localhost:4040.
Stop: `bash scripts/stop_spark_cluster.sh`

### Ray: head node + 1 worker node (4 CPUs each)

```bash
bash scripts/stop_spark_cluster.sh
bash scripts/start_ray_cluster.sh
sleep 6
ray status --address 127.0.0.1:6379       # expected: 2 active nodes, 0.0/8.0 CPU
```

Dashboard: http://127.0.0.1:8265 (Cluster tab). Always pass `--address 127.0.0.1:6379` to `ray status`, because the cluster uses a custom temp directory (`/var/tmp/rt`).
Stop: `bash scripts/stop_ray_cluster.sh`

## 3. Run the pipelines

Start the matching cluster first.

```bash
# Spark
python spark_clean.py                                   # all 44 files -> output/spark_clean
python spark_clean.py --max-files 2 --out output/spark_demo

# Ray
python ray_clean.py                                     # all 44 files -> output/ray_clean
python ray_clean.py --max-files 2 --out output/ray_demo
```

Each run prints a `METRICS:` line (framework, files, wall-clock seconds, output rows) and writes a Parquet directory. A full output is roughly 20 GB.

| Flag | Meaning |
|---|---|
| `--max-files N` | `0` = all files; otherwise N files spaced evenly over the 44 (first and last included) |
| `--udf-mode python\|native` | `python` = custom Python UDF (default); `native` = built-in expression, for timing comparison only |
| `--dedup` | Spark: `on` (default) or `off`. Ray: `perfile` (default), `off`, or the global variants `hash`, `twopass`, `on` |
| `--out DIR` | output directory |

Read the exported Parquet:

```bash
python -c "import pandas as pd; d=pd.read_parquet('output/ray_demo'); print(d.shape); print(d.head(3).T)"
```

## 4. Check that Spark and Ray match

Run both pipelines on the same files (for example the 2-file demo above), then:

```bash
python compare_outputs.py output/spark_demo output/ray_demo
```

It compares the row count and an order-independent row hash and prints `PARITY: PASS (identical)` (3,805,035 rows for the 2-file demo).

## 5. Benchmark harness

`benchmark.py` runs a pipeline as a subprocess, records wall-clock time, samples system CPU % and memory every second, checks the output row count and hash, then deletes the output.

```bash
python benchmark.py --framework spark --max-files 11 --run 1
python benchmark.py --framework ray   --max-files 11 --dedup perfile --run 1
python benchmark.py --framework spark --max-files 0  --run 1          # full 44 files
python benchmark.py --framework ray   --max-files 0  --dedup perfile --run 1
```

Other flags: `--udf-mode native`, `--dedup off`, `--keep` (keep the output). Each run writes `results/bench/<tag>.json` and `results/bench/<tag>_usage.csv` (per-second CPU and memory). `python scripts/summary.py` prints one line per run.

Leave the machine idle while benchmarking, and run only one cluster at a time.

## Screenshots

`screenshots/` holds the Spark Master UI (2 workers), the Spark application UI (:4040), and the Ray Dashboard (2 nodes, jobs).

## AI use

An LLM (Claude) was used as an assistant, and the following came from its suggestions, which I then ran and
checked: the translation of the PySpark logic into Ray Data/pandas; the canonical-type casting that reconciles
the two schema variants; the keep default na=False fix (pandas turns the zone name “N/A” into NaN, Spark does
not); the hash-partitioned, two-pass and per-file dedup variants and the dedup self-test; the num cpus=2 memory
limit on the per-file task; the benchmark harness; and troubleshooting for Ray start-up (RAY AUTH MODE=disabled,
and a shorter --temp-dir because of the 107-byte Unix-socket path limit). I configured and started both clusters
on my own machine, and every number in this report was produced by my own runs (results/bench/*.json, usage
CSVs, and the Spark UI / Ray Dashboard screenshots below).
