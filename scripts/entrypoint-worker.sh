#!/usr/bin/env bash
# S2 worker entrypoint. Derives the worker id from the StatefulSet ordinal.
set -euo pipefail

WORKER_ID="${HOSTNAME##*-}"
WORKERS="${WORKERS:-1}"
NETWORK_NAME="${NETWORK_NAME:-s2-triangle}"
CONTROLLER_HOST="${CONTROLLER_HOST:-s2-controller.s2.svc.cluster.local}"
CONTROLLER_PORT="${CONTROLLER_PORT:-4090}"
SIDECAR_PORT="${SIDECAR_PORT:-4091}"

# Keep the Pod alive after the one-shot run so the StatefulSet does not restart
# the worker and re-register with a finished controller.
java -XX:-UseCompressedOops \
  -jar /s2/s2_main.jar worker \
  "${NETWORK_NAME}" "${WORKER_ID}" "${WORKERS}" \
  "${CONTROLLER_HOST}" "${CONTROLLER_PORT}" "${SIDECAR_PORT}" || true
echo "S2 worker ${WORKER_ID} finished; idling"
exec sleep infinity
