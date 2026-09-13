#!/usr/bin/env bash
# Run the multi-process S2 demo locally with N worker processes (JVM each).
# Usage: scripts/local-demo.sh <numWorkers> [network]
set -euo pipefail

W="${1:?usage: local-demo.sh <numWorkers> [network]}"
NETWORK="${2:-s2-triangle}"
cd "$(git rev-parse --show-toplevel)"

# Runner defaults. (1) Enable the S2 positive-only PrefixSpace memoization, which bounds the EGP
# control-plane transient (pure memoization; never changes results). (2) Pin the partition scheme
# to WEIGHTED_LPT_FM, the best-or-tied scheme across the measured Clos/WAN/line testbeds
# (PARTITIONING-PLAN.md 6.13; also the shared-code default). The memo default stays off in shared
# code so stock Batfish is unaffected. Put your own -D later in JAVA_TOOL_OPTIONS to override
# (e.g. JAVA_TOOL_OPTIONS=-Ds2.partition=METIS); the JVM honors the last one.
export JAVA_TOOL_OPTIONS="-Ds2.prefixSpacePositiveCacheOnly=true -Ds2.partition=WEIGHTED_LPT_FM ${JAVA_TOOL_OPTIONS:-}"

# Free only the ports this run will use, so concurrent runs (other worktrees/agents) are not
# disturbed. Override the base port with S2_BASE_PORT to run several demos at once.
BASE_PORT="${S2_BASE_PORT:-14090}"
for p in $(seq "$BASE_PORT" $((BASE_PORT + W))); do
  pids=$(lsof -ti tcp:"$p" 2>/dev/null || true)
  if [[ -n "$pids" ]]; then kill $pids 2>/dev/null || true; fi
done
sleep 1

JAR="bazel-bin/projects/s2/s2_main_deploy.jar"
[[ -f "$JAR" ]] || { echo "build first: bazel build //projects/s2:s2_main_deploy.jar" >&2; exit 1; }

export S2_INPUT_DIR="$PWD/networks"
export S2_OUTPUT_DIR="$PWD/results/local-${NETWORK}-${W}"
mkdir -p "$S2_OUTPUT_DIR"

CTRL_PORT="$BASE_PORT"
endpoints=""
for i in $(seq 0 $((W - 1))); do
  port=$((BASE_PORT + 1 + i))
  endpoints="${endpoints}${endpoints:+,}127.0.0.1:${port}"
done

echo "== controller (port ${CTRL_PORT}) =="
java -jar "$JAR" controller "$NETWORK" "$W" "$endpoints" "$CTRL_PORT" \
  > "$S2_OUTPUT_DIR/controller.log" 2>&1 &
ctrl_pid=$!
sleep 3

echo "== starting ${W} worker(s) =="
worker_pids=()
for i in $(seq 0 $((W - 1))); do
  port=$((BASE_PORT + 1 + i))
  java -jar "$JAR" worker "$NETWORK" "$i" "$W" 127.0.0.1 "$CTRL_PORT" "$port" \
    > "$S2_OUTPUT_DIR/worker-${i}.log" 2>&1 &
  worker_pids+=($!)
done

wait "$ctrl_pid" || true
for p in "${worker_pids[@]}"; do wait "$p" || true; done

echo "== controller output =="
cat "$S2_OUTPUT_DIR/controller.log"

# Verification runs in a separate JVM (the controller is a lightweight coordinator and
# exits as soon as it has written the workers' results). Same input/output dirs.
echo "== verifier (separate JVM) =="
java -jar "$JAR" verify "$NETWORK" "$W" 2>&1 | tee "$S2_OUTPUT_DIR/verify.log"

echo "== result file =="
cat "$S2_OUTPUT_DIR/result-${W}worker.txt" 2>/dev/null || echo "(missing)"
