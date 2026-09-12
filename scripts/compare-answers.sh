#!/usr/bin/env bash
# Assert the 1-Pod and 3-Pod runs both matched vanilla Batfish, i.e. distribution
# did not change the verification result.
set -euo pipefail

cd "$(git rev-parse --show-toplevel)"
A="results/k8s-controller-1pod.log"
B="results/k8s-controller-3pod.log"
[[ -f "$A" && -f "$B" ]] || { echo "run scripts/k8s-demo.sh 1 and 3 first" >&2; exit 1; }

status() { grep -oE "S2 (MATCH|DIFF)[^(]*" "$1" | tail -1; }
symbolic() { grep -oE "symbolic=(MATCH|DIFF)" "$1" | tail -1; }

sa="$(status "$A")"
sb="$(status "$B")"
ya="$(symbolic "$A")"
yb="$(symbolic "$B")"
echo "1 Pod:  ${sa:-<none>} ${ya:-<none>}"
echo "3 Pods: ${sb:-<none>} ${yb:-<none>}"

if grep -q "ribs=MATCH reachability=MATCH symbolic=MATCH" "$A" \
   && grep -q "ribs=MATCH reachability=MATCH symbolic=MATCH" "$B"; then
  echo "IDENTICAL: 1-Pod and 3-Pod both match vanilla (ribs + reachability + symbolic)."
else
  echo "MISMATCH: expected both runs to MATCH (ribs + reachability + symbolic)." >&2
  exit 1
fi
