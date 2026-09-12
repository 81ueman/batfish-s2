#!/usr/bin/env bash
# Assert the 1-Pod and 3-Pod runs both matched vanilla Batfish, i.e. distribution
# did not change the verification result.
set -euo pipefail

cd "$(git rev-parse --show-toplevel)"
A="results/k8s-controller-1pod.log"
B="results/k8s-controller-3pod.log"
[[ -f "$A" && -f "$B" ]] || { echo "run scripts/k8s-demo.sh 1 and 3 first" >&2; exit 1; }

summary() { grep -oE "ribs=[A-Z]+ reachability=[A-Z]+ answer=[A-Z]+" "$1" | tail -1; }

sa="$(summary "$A")"
sb="$(summary "$B")"
echo "1 Pod:  ${sa:-<none>}"
echo "3 Pods: ${sb:-<none>}"

if [[ "$sa" == "ribs=MATCH reachability=MATCH answer=MATCH" \
   && "$sb" == "ribs=MATCH reachability=MATCH answer=MATCH" ]]; then
  echo "IDENTICAL: 1-Pod and 3-Pod both match vanilla (ribs + reachability + reachability answer)."
else
  echo "MISMATCH: expected both runs to MATCH (ribs + reachability + answer)." >&2
  exit 1
fi
