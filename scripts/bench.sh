#!/usr/bin/env bash
# Run a (network x workers) matrix through local-demo.sh and emit a metrics table.
#
# Usage:
#   scripts/bench.sh "<workers>" "<networks>"
#   scripts/bench.sh "1 3" "s2-line s2-mega s2-giga"
#
# Environment is forwarded to each run:
#   JAVA_TOOL_OPTIONS   e.g. -Xmx4g -Ds2.ownedDataplane=true
#   S2_PREFIX_SHARDS    e.g. 8
set -uo pipefail
cd "$(git rev-parse --show-toplevel)"

WORKERS_LIST="${1:?usage: bench.sh \"<workers>\" \"<networks>\"}"
NETWORKS="${2:?usage: bench.sh \"<workers>\" \"<networks>\"}"

if [[ ! -f bazel-bin/projects/s2/s2_main_deploy.jar ]]; then
  echo "# building s2_main_deploy.jar" >&2
  bazel build //projects/s2:s2_main_deploy.jar >/dev/null 2>&1
fi

echo "| network | workers | result | max peak MiB | controller MiB | engine s (w0) | wall s |"
echo "| --- | --- | --- | --- | --- | --- | --- |"
for net in $NETWORKS; do
  for w in $WORKERS_LIST; do
    log="/tmp/s2-bench-${net}-${w}.log"
    outdir="results/local-${net}-${w}"
    start=$(date +%s)
    scripts/local-demo.sh "$w" "$net" >"$log" 2>&1 || true
    end=$(date +%s)
    res="$outdir/result-${w}worker.txt"
    wlog="$outdir/worker-0.log"
    if [[ ! -f "$res" ]]; then
      printf "| %s | %s | NO_RESULT | - | - | - | %d |\n" "$net" "$w" "$((end - start))"
      continue
    fi
    result=$(grep -m1 -oE 'S2 (MATCH|DIFF)' "$log" | awk '{print $2}')
    maxpeak=$(awk '/^worker [0-9]+: /{if ($3+0 > m) m=$3+0} END{printf "%.1f", m}' "$res")
    ctrl=$(awk '/^controller: /{printf "%.1f", $2}' "$res")
    engine_s=$(grep -oE 't=[0-9.]+s' "$wlog" 2>/dev/null | tail -1 | tr -d 't=s')
    printf "| %s | %s | %s | %s | %s | %s | %d |\n" \
      "$net" "$w" "${result:-?}" "${maxpeak:--}" "${ctrl:--}" "${engine_s:--}" "$((end - start))"
  done
done
