#!/usr/bin/env bash
# Sweep the control-plane prefix-shard count N and report the max per-worker peak heap.
#
# Usage:
#   S2_BASE_PORT=18300 scripts/shard-sweep.sh <workers> <network> "<N list>" [extra JAVA_TOOL_OPTIONS]
#   S2_BASE_PORT=18300 scripts/shard-sweep.sh 3 s2-big2 "1 2 4 8 16 32"
#   S2_BASE_PORT=18300 scripts/shard-sweep.sh 3 s2-mega "1 2 4 8 16 32"
#
# Each N runs scripts/local-demo.sh with -Ds2.prefixShardExternalize=true and S2_PREFIX_SHARDS=N,
# so only one shard's BGP RIB is live at a time. The table is directly comparable to
# docs/s2-port/M5-SCALE.md and the auto policy in PrefixShardCountSelector.
set -uo pipefail
cd "$(git rev-parse --show-toplevel)"

W="${1:?usage: shard-sweep.sh <workers> <network> \"<N list>\"}"
NETWORK="${2:?usage: shard-sweep.sh <workers> <network> \"<N list>\"}"
NS="${3:-1 2 4 8 16 32}"
EXTRA="${4:-}"

if [[ ! -f bazel-bin/projects/s2/s2_main_deploy.jar ]]; then
  echo "# building s2_main_deploy.jar" >&2
  bazel build //projects/s2:s2_main_deploy.jar >/dev/null 2>&1
fi

echo "| network | workers | shards | result | max peak MiB |"
echo "| --- | --- | --- | --- | --- |"
for n in $NS; do
  log="/tmp/s2-shard-sweep-${NETWORK}-${n}.log"
  JAVA_TOOL_OPTIONS="-Ds2.prefixShardExternalize=true ${EXTRA}" S2_PREFIX_SHARDS="$n" \
    scripts/local-demo.sh "$W" "$NETWORK" >"$log" 2>&1 || true
  res="results/local-${NETWORK}-${W}/result-${W}worker.txt"
  if [[ ! -f "$res" ]]; then
    printf "| %s | %s | %s | NO_RESULT | - |\n" "$NETWORK" "$W" "$n"
    continue
  fi
  result=$(grep -m1 -oE 'S2 (MATCH|DIFF)' "$log" | awk '{print $2}')
  maxpeak=$(awk '/^worker [0-9]+: /{if ($3+0 > m) m=$3+0} END{printf "%.1f", m}' "$res")
  printf "| %s | %s | %s | %s | %s |\n" "$NETWORK" "$W" "$n" "${result:-?}" "${maxpeak:--}"
done
