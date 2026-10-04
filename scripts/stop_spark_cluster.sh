#!/usr/bin/env bash
export SPARK_HOME="$HOME/spark-3.5.9-bin-hadoop3"
export SPARK_WORKER_INSTANCES=2
"$SPARK_HOME/sbin/stop-worker.sh"
"$SPARK_HOME/sbin/stop-master.sh"
