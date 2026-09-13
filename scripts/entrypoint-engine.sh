#!/usr/bin/env bash
# Batfish engine entrypoint for the persistent S2 pool (choice A): run the normal allinone
# coordinator/worker, but select the s2 dataplane engine and drive the pool. The engine
# ships each snapshot to the controller and serves questions from the slices the workers
# wrote to the shared volume (s2storedataplane=false keeps the lazy data plane in memory).
#
# Everything after this script's own env is forwarded to allinone (e.g. snapshot/question
# arguments), so pybatfish/REST clients can drive it exactly like stock Batfish.
set -euo pipefail

CONTROLLER_HOST="${CONTROLLER_HOST:-s2-controller.s2-pool.svc.cluster.local}"
CONTROLLER_PORT="${CONTROLLER_PORT:-4090}"
SLICE_DIR="${SLICE_DIR:-/s2/shared/slices}"

S2_ARGS="-dataplaneengine=s2"
S2_ARGS="${S2_ARGS} -s2controllerhost=${CONTROLLER_HOST}"
S2_ARGS="${S2_ARGS} -s2controllerport=${CONTROLLER_PORT}"
S2_ARGS="${S2_ARGS} -s2storedataplane=false"
S2_ARGS="${S2_ARGS} -s2slicedir=${SLICE_DIR}"

exec java -XX:-UseCompressedOops \
  -jar /s2/allinone.jar -batfishargs "${S2_ARGS}" "$@"
