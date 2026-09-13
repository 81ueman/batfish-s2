#!/usr/bin/env bash
# S2 persistent worker service entrypoint (choice A). Derives the worker id from the
# StatefulSet ordinal, registers with the controller once, and serves snapshots until
# shut down; if it crashes, the StatefulSet restarts it and it re-registers.
set -euo pipefail

WORKER_ID="${HOSTNAME##*-}"
CONTROLLER_HOST="${CONTROLLER_HOST:-s2-controller.s2-pool.svc.cluster.local}"
CONTROLLER_PORT="${CONTROLLER_PORT:-4090}"
SIDECAR_PORT="${SIDECAR_PORT:-4091}"
NAMESPACE="${NAMESPACE:-s2-pool}"
# Peers (and the controller) reach this worker's sidecar at its stable StatefulSet DNS name.
ADVERTISED_HOST="${ADVERTISED_HOST:-${HOSTNAME}.s2-worker.${NAMESPACE}.svc.cluster.local}"

exec java -XX:-UseCompressedOops \
  -jar /s2/s2_main.jar worker-service \
  "${WORKER_ID}" "${CONTROLLER_HOST}" "${CONTROLLER_PORT}" "${SIDECAR_PORT}" "${ADVERTISED_HOST}"
