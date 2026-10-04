#!/usr/bin/env bash
# Ray cluster: 1 head node + 1 worker node, 4 CPUs each (2 nodes, 8 CPUs total)
set -e
mkdir -p "/var/tmp/rt"
ray stop --force >/dev/null 2>&1 || true
ray start --head --node-ip-address=127.0.0.1 --port=6379 \
  --num-cpus=4 --object-store-memory=1073741824 --temp-dir=/var/tmp/rt \
  --include-dashboard=true --dashboard-host=127.0.0.1 --dashboard-port=8265
sleep 5
ray start --address=127.0.0.1:6379 --node-ip-address=127.0.0.1 \
  --num-cpus=4 --object-store-memory=1073741824 --temp-dir=/var/tmp/rt
echo "Ray Dashboard: http://127.0.0.1:8265"
