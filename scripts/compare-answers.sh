#!/usr/bin/env bash
# Assert the 1-Pod and 3-Pod runs both matched vanilla Batfish, i.e. distribution
# did not change the verification result.
set -euo pipefail

cd "$(git rev-parse --show-toplevel)"
NETWORK="${1:-s2-triangle}"
# The MATCH line is produced by the verify role now (the controller only collects results).
A="results/k8s-verifier-${NETWORK}-1pod.log"
B="results/k8s-verifier-${NETWORK}-3pod.log"
[[ -f "$A" && -f "$B" ]] || {
  echo "run 'scripts/k8s-demo.sh 1 ${NETWORK}' and 'scripts/k8s-demo.sh 3 ${NETWORK}' first" >&2
  exit 1
}

summary() {
  grep -oE "ribs=[A-Z]+ reachability=[A-Z]+ symbolic=[A-Z]+ answer=[A-Z]+" "$1" | tail -1
}

EXPECTED="ribs=MATCH reachability=MATCH symbolic=MATCH answer=MATCH"
sa="$(summary "$A")"
sb="$(summary "$B")"
echo "1 Pod:  ${sa:-<none>}"
echo "3 Pods: ${sb:-<none>}"

if [[ "$sa" == "$EXPECTED" && "$sb" == "$EXPECTED" ]]; then
  echo "IDENTICAL: 1-Pod and 3-Pod both match vanilla (ribs + reachability + symbolic states + answer)."
else
  echo "MISMATCH: expected both runs to MATCH (ribs + reachability + symbolic + answer)." >&2
  exit 1
fi
