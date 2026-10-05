#!/usr/bin/env bash
# Full 44-file Ray benchmark using per-file dedup (the strategy behind the reported result).
# Run from the repo root with the conda env active:  bash scripts/run_ray_full.sh
cd "$(dirname "$0")/.."
bash scripts/stop_spark_cluster.sh >/dev/null 2>&1
bash scripts/stop_ray_cluster.sh >/dev/null 2>&1; sleep 3
pkill -f ray_clean.py; pkill -f benchmark.py
rm -rf output/bench_ray
bash scripts/start_ray_cluster.sh > results/ray_start.log 2>&1
N=0
for i in $(seq 1 20); do
  N=$(ray status --address 127.0.0.1:6379 2>/dev/null | grep -c " node_")
  [ "$N" -ge 2 ] && break
  sleep 3
done
if [ "$N" -lt 2 ]; then echo "RAY CLUSTER NOT UP (nodes: $N). See results/ray_start.log"; exit 1; fi
echo "ray nodes up: $N"
python benchmark.py --framework ray --max-files 0 --run 1 --dedup perfile 2>&1
echo "RAY FULL DONE"
