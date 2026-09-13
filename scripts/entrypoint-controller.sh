#!/usr/bin/env bash
# S2 controller entrypoint. Builds the worker endpoint list and starts the run.
set -euo pipefail

WORKERS="${WORKERS:-1}"
NETWORK_NAME="${NETWORK_NAME:-s2-triangle}"
PORT="${CONTROLLER_PORT:-4090}"

endpoints=""
for i in $(seq 0 $((WORKERS - 1))); do
  endpoints="${endpoints}${endpoints:+,}s2-worker-${i}.s2-worker.s2.svc.cluster.local:4091"
done

exec java -XX:-UseCompressedOops \
  -jar /s2/s2_main.jar controller "${NETWORK_NAME}" "${WORKERS}" "${endpoints}" "${PORT}"
