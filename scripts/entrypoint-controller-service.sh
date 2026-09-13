#!/usr/bin/env bash
# S2 persistent controller service entrypoint (choice A): accept the worker pool, then
# serve one compute request per snapshot from the engine.
set -euo pipefail

WORKERS="${WORKERS:-3}"
PORT="${CONTROLLER_PORT:-4090}"

exec java -XX:-UseCompressedOops \
  -jar /s2/s2_main.jar controller-service "${WORKERS}" "${PORT}"
