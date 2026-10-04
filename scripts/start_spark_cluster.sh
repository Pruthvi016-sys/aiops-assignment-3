#!/usr/bin/env bash
# Starts a Spark standalone cluster: 1 master + 2 workers (4 cores, 3g each)
set -e
mkdir -p "$HOME/spark_tmp"
export SPARK_HOME="$HOME/spark-3.5.9-bin-hadoop3"
export SPARK_MASTER_HOST=127.0.0.1
export PYSPARK_PYTHON="$(which python)"
export SPARK_WORKER_INSTANCES=2
export SPARK_WORKER_CORES=4
export SPARK_WORKER_MEMORY=3g
export SPARK_LOCAL_DIRS="$HOME/spark_tmp"
"$SPARK_HOME/sbin/start-master.sh"
sleep 3
"$SPARK_HOME/sbin/start-worker.sh" spark://127.0.0.1:7077
echo "Master UI: http://127.0.0.1:8080"
