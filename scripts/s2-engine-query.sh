#!/usr/bin/env bash
# End-to-end check of the S2 drop-in engine driving a persistent pool (local, process-level):
# start a controller-service + N worker-services, then run allinone with -dataplaneengine=s2 via a
# command file that generates the dataplane (the engine ships the snapshot to the pool and serves
# from the per-host slices the workers write).
#
# Usage: S2_BASE_PORT=19490 scripts/s2-engine-query.sh [workers] [network]
set -uo pipefail
cd "$(git rev-parse --show-toplevel)"

W="${1:-3}"
NET="${2:-s2-triangle}"
BASE_PORT="${S2_BASE_PORT:-19490}"
OUT="${S2_OUTPUT_DIR:-$PWD/results/local-engine-query-${NET}-${W}}"
mkdir -p "$OUT"

bazel build //projects/s2:s2_main_deploy.jar //projects/allinone:allinone_main_deploy.jar >/dev/null 2>&1 || {
  echo "build failed" >&2
  exit 1
}
RUNNER="bazel-bin/projects/s2/s2_main_deploy.jar"
ENGINE="bazel-bin/projects/allinone/allinone_main_deploy.jar"
export JAVA_TOOL_OPTIONS="-Ds2.partition=WEIGHTED_LPT_FM ${JAVA_TOOL_OPTIONS:-}"

CTRL_PORT="$BASE_PORT"
SLICES="$OUT/slices"
mkdir -p "$SLICES"

java -jar "$RUNNER" controller-service "$W" "$CTRL_PORT" > "$OUT/controller.log" 2>&1 &
CPID=$!
worker_pids=()
for i in $(seq 0 $((W - 1))); do
  java -jar "$RUNNER" worker-service "$i" 127.0.0.1 "$CTRL_PORT" "$((BASE_PORT + 1 + i))" 127.0.0.1 \
    > "$OUT/worker-$i.log" 2>&1 &
  worker_pids+=($!)
done
for _ in $(seq 1 90); do
  grep -q "all ${W} workers registered" "$OUT/controller.log" 2>/dev/null && break
  sleep 1
done

cat > "$OUT/query.txt" <<EOF
init-network n1
init-snapshot $PWD/networks/$NET $NET
generate-dataplane
EOF

java -jar "$ENGINE" \
  -runclient true \
  -cmdfile "$OUT/query.txt" \
  -batfishargs "-dataplaneengine=s2 -s2controllerhost=127.0.0.1 -s2controllerport=$CTRL_PORT -s2storedataplane=false -s2slicedir=$SLICES" \
  > "$OUT/engine.log" 2>&1
rc=$?

kill "$CPID" "${worker_pids[@]}" 2>/dev/null || true
wait 2>/dev/null || true

echo "engine rc=$rc; logs under $OUT"
grep -E "done \(|wrote slices|IncrementalBdpAnswerElement" "$OUT/controller.log" "$OUT/worker-0.log" "$OUT/engine.log" 2>/dev/null | tail -5
exit "$rc"
