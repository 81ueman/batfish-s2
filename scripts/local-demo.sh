#!/usr/bin/env bash
# Run the multi-process S2 demo locally with N worker processes (JVM each).
# Usage: scripts/local-demo.sh <1|3>
set -euo pipefail

W="${1:?usage: local-demo.sh <1|3>}"
cd "$(git rev-parse --show-toplevel)"

# Clean up workers from a previous (possibly interrupted) run that still hold ports.
pkill -f "s2_main_deploy.jar" 2>/dev/null || true
sleep 1

JAR="bazel-bin/projects/s2/s2_main_deploy.jar"
[[ -f "$JAR" ]] || { echo "build first: bazel build //projects/s2:s2_main_deploy.jar" >&2; exit 1; }

export S2_INPUT_DIR="$PWD/networks"
export S2_OUTPUT_DIR="$PWD/results/local-${W}"
mkdir -p "$S2_OUTPUT_DIR"

CTRL_PORT=14090
endpoints=""
for i in $(seq 0 $((W - 1))); do
  port=$((14091 + i))
  endpoints="${endpoints}${endpoints:+,}127.0.0.1:${port}"
done

echo "== controller (port ${CTRL_PORT}) =="
java -jar "$JAR" controller s2-triangle "$W" "$endpoints" "$CTRL_PORT" \
  > "$S2_OUTPUT_DIR/controller.log" 2>&1 &
ctrl_pid=$!
sleep 3

echo "== starting ${W} worker(s) =="
worker_pids=()
for i in $(seq 0 $((W - 1))); do
  port=$((14091 + i))
  java -jar "$JAR" worker s2-triangle "$i" "$W" 127.0.0.1 "$CTRL_PORT" "$port" \
    > "$S2_OUTPUT_DIR/worker-${i}.log" 2>&1 &
  worker_pids+=($!)
done

wait "$ctrl_pid" || true
for p in "${worker_pids[@]}"; do wait "$p" || true; done

echo "== controller output =="
cat "$S2_OUTPUT_DIR/controller.log"
echo "== result file =="
cat "$S2_OUTPUT_DIR/result-${W}worker.txt" 2>/dev/null || echo "(missing)"
