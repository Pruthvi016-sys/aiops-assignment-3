#!/usr/bin/env bash
cd ~/spark-vs-ray
source ~/miniconda3/etc/profile.d/conda.sh && conda activate sparkray
bash scripts/stop_spark_cluster.sh >/dev/null 2>&1
bash scripts/stop_ray_cluster.sh >/dev/null 2>&1; sleep 3
pkill -f ray_clean.py; pkill -f benchmark.py
rm -rf /var/tmp/rt/* ~/spark_tmp/* output/bench_ray output/ray_dbg
rm -f ~/spark-3.5.9-bin-hadoop3.tgz
pip cache purge >/dev/null 2>&1; conda clean -a -y >/dev/null 2>&1
rm -f results/guard_triggered.txt
echo "free MB at start: $(df -BM --output=avail / | tail -1)"
bash scripts/start_ray_cluster.sh > results/ray_start.log 2>&1
N=0
for i in $(seq 1 20); do
  N=$(ray status --address 127.0.0.1:6379 2>/dev/null | grep -c " node_")
  [ "$N" -ge 2 ] && break
  sleep 3
done
if [ "$N" -lt 2 ]; then echo "RAY CLUSTER NOT UP (nodes: $N). See results/ray_start.log"; exit 1; fi
echo "ray nodes up: $N"
( while true; do
    AV=$(df -BM --output=avail / | tail -1 | tr -dc 0-9)
    if [ "$AV" -lt 4000 ]; then
      echo "DISK GUARD: only ${AV} MB free" > results/guard_triggered.txt
      pkill -f ray_clean.py; pkill -f benchmark.py; break
    fi
    sleep 5
  done ) &
GUARD=$!
python benchmark.py --framework ray --max-files 0 --run 1 --dedup hash 2>&1
kill $GUARD 2>/dev/null
[ -f results/guard_triggered.txt ] && cat results/guard_triggered.txt
echo "RAY FULL DONE"
