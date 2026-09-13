#!/usr/bin/env bash
# S2 verifier entrypoint. Runs the vanilla/reference verification in its own JVM/Job,
# after the controller has written the workers' results to the shared volume.
set -euo pipefail

WORKERS="${WORKERS:-1}"
NETWORK_NAME="${NETWORK_NAME:-s2-triangle}"
S2_OUTPUT_DIR="${S2_OUTPUT_DIR:-/s2/outputs}"
RESULTS="${S2_OUTPUT_DIR}/worker-results-${WORKERS}.bin"

# The verifier Job has no ordering dependency on the controller Job, so wait (bounded)
# for the controller to finish collecting the workers' results. The controller writes
# the file atomically, so seeing it means it is complete.
for _ in $(seq 1 3600); do
  if [[ -f "$RESULTS" ]]; then
    break
  fi
  sleep 1
done
if [[ ! -f "$RESULTS" ]]; then
  echo "S2 verifier: timed out waiting for ${RESULTS}" >&2
  exit 1
fi

exec java -XX:-UseCompressedOops \
  -jar /s2/s2_main.jar verify "${NETWORK_NAME}" "${WORKERS}"
