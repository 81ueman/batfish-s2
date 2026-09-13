#!/usr/bin/env bash
# Run the persistent S2 pool locally: one controller-service plus N worker-services, then ship a
# snapshot through the pool with the pool-compute client and compare it against vanilla.
#
# Usage: scripts/s2-pool-demo.sh <numWorkers> [network]
#   network defaults to s2-triangle; any snapshot under $S2_INPUT_DIR works.
#
# The pool is long-lived: the same workers serve every snapshot the client sends, so this is the
# process-level counterpart of projects/s2/.../S2PoolServiceTest.
set -euo pipefail

W="${1:?usage: s2-pool-demo.sh <numWorkers> [network]}"
NETWORK="${2:-s2-triangle}"
cd "$(git rev-parse --show-toplevel)"

# Runner defaults: positive-only PrefixSpace memoization and the pinned partition scheme. The
# worker-service defaults to the forwarding-exact owned-only dataplane (see S2WorkerService);
# set -Ds2.ownedDataplane=false to force the full-dataplane-per-worker mode.
export JAVA_TOOL_OPTIONS="-Ds2.prefixSpacePositiveCacheOnly=true -Ds2.partition=WEIGHTED_LPT_FM ${JAVA_TOOL_OPTIONS:-}"

# Free only the ports this run will use, so concurrent runs are not disturbed.
BASE_PORT="${S2_BASE_PORT:-15090}"
for p in $(seq "$BASE_PORT" $((BASE_PORT + W + 1))); do
  pids=$(lsof -ti tcp:"$p" 2>/dev/null || true)
  if [[ -n "$pids" ]]; then kill $pids 2>/dev/null || true; fi
done
sleep 1

JAR="bazel-bin/projects/s2/s2_main_deploy.jar"
[[ -f "$JAR" ]] || { echo "build first: bazel build //projects/s2:s2_main_deploy.jar" >&2; exit 1; }

export S2_INPUT_DIR="${S2_INPUT_DIR:-$PWD/networks}"
export S2_OUTPUT_DIR="$PWD/results/local-pool-${NETWORK}-${W}"
mkdir -p "$S2_OUTPUT_DIR"

CTRL_PORT="$BASE_PORT"
echo "== controller-service (port ${CTRL_PORT}) =="
java -jar "$JAR" controller-service "$W" "$CTRL_PORT" \
  > "$S2_OUTPUT_DIR/controller.log" 2>&1 &
ctrl_pid=$!
sleep 2

echo "== ${W} worker-service(s) =="
worker_pids=()
for i in $(seq 0 $((W - 1))); do
  port=$((BASE_PORT + 1 + i))
  java -jar "$JAR" worker-service "$i" 127.0.0.1 "$CTRL_PORT" "$port" 127.0.0.1 \
    > "$S2_OUTPUT_DIR/worker-${i}.log" 2>&1 &
  worker_pids+=($!)
done

echo "== waiting for ${W} workers to register =="
for _ in $(seq 1 120); do
  if grep -q "all ${W} workers registered" "$S2_OUTPUT_DIR/controller.log" 2>/dev/null; then
    break
  fi
  sleep 1
done

rc=0
echo "== pool-compute (client) =="
if ! java -jar "$JAR" pool-compute "$NETWORK" "$CTRL_PORT" 2>&1 | tee "$S2_OUTPUT_DIR/pool-compute.log"; then
  rc=1
fi

kill "$ctrl_pid" "${worker_pids[@]}" 2>/dev/null || true
wait 2>/dev/null || true
echo "done (rc=${rc}): logs under $S2_OUTPUT_DIR"
exit "$rc"
