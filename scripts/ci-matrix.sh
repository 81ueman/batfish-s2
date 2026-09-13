#!/usr/bin/env bash
# Full local demo correctness matrix (P0 / ops O3).
#
# Runs every "tie-stable" demo network through the multi-process runner at a fixed
# worker count, in both the default and the owned-dataplane
# (-Ds2.ownedDataplane=true) modes, and prints a pass/fail summary by grepping the
# per-run result for MATCH.
#
# This is SLOW (each cell is a full multi-JVM run), so it is OPT-IN: it refuses to
# do anything unless you pass --run (or set S2_CI_MATRIX=1).
#
# Usage:
#   scripts/ci-matrix.sh --run
#   scripts/ci-matrix.sh --run --workers 3
#   scripts/ci-matrix.sh --run --networks "s2-triangle s2-ospf"
#   scripts/ci-matrix.sh --run --modes "default owned"
#   scripts/ci-matrix.sh --list          # print the matrix, run nothing
#
# Environment:
#   S2_CI_MATRIX=1        same as --run
#   S2_CI_WORKERS=N       worker count (default 3)
#   JAVA_TOOL_OPTIONS     forwarded to every run (any pre-existing value is kept)
#
# Results by default come from `scripts/bench.sh`, which writes per-cell logs under
# /tmp/s2-bench-<net>-<workers>.log and results under results/local-<net>-<workers>.
set -uo pipefail
cd "$(git rev-parse --show-toplevel)"

DEFAULT_NETWORKS="s2-triangle s2-line s2-ospf s2-ospf-bgp s2-redist s2-agg s2-static s2-external"
NETWORKS="$DEFAULT_NETWORKS"
WORKERS="${S2_CI_WORKERS:-3}"
MODES="default owned"
RUN=0
LIST=0

usage() {
  sed -n '2,26p' "$0"
}

BASE_JAVA_TOOL_OPTIONS="${JAVA_TOOL_OPTIONS:-}"

while [[ $# -gt 0 ]]; do
  case "$1" in
    --run) RUN=1 ;;
    --list) LIST=1 ;;
    --workers) WORKERS="${2:?--workers needs a value}"; shift ;;
    --workers=*) WORKERS="${1#*=}" ;;
    --networks) NETWORKS="${2:?--networks needs a value}"; shift ;;
    --networks=*) NETWORKS="${1#*=}" ;;
    --modes) MODES="${2:?--modes needs a value}"; shift ;;
    --modes=*) MODES="${1#*=}" ;;
    -h|--help) usage; exit 0 ;;
    *) echo "unknown argument: $1" >&2; usage; exit 2 ;;
  esac
  shift
done

if [[ "$LIST" == 1 ]]; then
  echo "networks: $NETWORKS"
  echo "workers:  $WORKERS"
  echo "modes:    $MODES"
  exit 0
fi

if [[ "$RUN" != 1 && "${S2_CI_MATRIX:-0}" != 1 ]]; then
  cat >&2 <<'EOF'
refusing to run: the CI matrix launches many full multi-JVM runs and is slow.
Pass --run (or set S2_CI_MATRIX=1) to acknowledge. Use --list to preview the matrix.
EOF
  exit 2
fi

echo "== S2 CI matrix: workers=${WORKERS} modes='${MODES}' =="
echo "== networks: ${NETWORKS} =="

summary_rows=()
pass=0
fail=0

for mode in $MODES; do
  case "$mode" in
    default) JAVA_TOOL_OPTIONS="$BASE_JAVA_TOOL_OPTIONS" ;;
    owned)   JAVA_TOOL_OPTIONS="$BASE_JAVA_TOOL_OPTIONS -Ds2.ownedDataplane=true" ;;
    *) echo "unknown mode '$mode' (expected default or owned)" >&2; exit 2 ;;
  esac
  export JAVA_TOOL_OPTIONS

  for net in $NETWORKS; do
    log="/tmp/s2-ci-matrix-${mode}-${net}-${WORKERS}.log"
    printf '== %-7s %-14s workers=%s -> %s\n' "$mode" "$net" "$WORKERS" "$log"
    scripts/bench.sh "$WORKERS" "$net" >"$log" 2>&1
    row="$(grep -E "^\| ${net} \| ${WORKERS} \|" "$log" | tail -1)"
    result="$(printf '%s\n' "$row" | awk -F'|' 'NF>1 {gsub(/^ +| +$/, "", $4); print $4}')"
    result="${result:-NO_RESULT}"
    if [[ "$result" == "MATCH" ]]; then
      pass=$((pass + 1))
      status="PASS"
    else
      fail=$((fail + 1))
      status="FAIL"
    fi
    summary_rows+=("$(printf '%-8s %-14s %-9s %s' "$mode" "$net" "$result" "$status")")
  done
done

echo
echo "== summary =="
printf '%-8s %-14s %-9s %s\n' "MODE" "NETWORK" "RESULT" "STATUS"
for row in "${summary_rows[@]}"; do
  printf '%s\n' "$row"
done
echo
echo "MATCH: ${pass}/$((pass + fail)) cell(s), failed: ${fail}"

if [[ "$fail" -gt 0 ]]; then
  echo "CI matrix FAILED: ${fail} cell(s) did not MATCH." >&2
  exit 1
fi
echo "CI matrix passed: all cells MATCH."
