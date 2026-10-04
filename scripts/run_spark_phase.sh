#!/usr/bin/env bash
cd ~/spark-vs-ray
source ~/miniconda3/etc/profile.d/conda.sh && conda activate sparkray
bash scripts/stop_ray_cluster.sh >/dev/null 2>&1
bash scripts/stop_spark_cluster.sh >/dev/null 2>&1; sleep 3
bash scripts/start_spark_cluster.sh >/dev/null 2>&1; sleep 8
F='"tag"|pipeline_wall|output_rows|row_hash|mean_cpu|peak_mem_delta|CONSISTENT|FAILED'
for r in 2 3; do python benchmark.py --framework spark --max-files 11 --run $r | grep -E "$F"; done
for r in 1 2; do python benchmark.py --framework spark --udf-mode native --max-files 11 --run $r | grep -E "$F"; done
python benchmark.py --framework spark --max-files 0 --run 1 | grep -E "$F"
echo "SPARK PHASE DONE"
