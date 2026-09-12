#!/usr/bin/env bash
# Compare controller output between the 1-Pod and 3-Pod runs.
#
# Normalizes volatile fields (timestamps, runtimes, memory) and diffs the rest.
# Expected: no differences => distributing did not change the verification
# result.
set -euo pipefail

cd "$(git rev-parse --show-toplevel)"
A="results/controller-1pod.log"
B="results/controller-3pod.log"
[[ -f "$A" && -f "$B" ]] || { echo "run scripts/k8s-demo.sh 1 and 3 first" >&2; exit 1; }

normalize() {
  sed -E \
    -e 's/^[0-9]{4}-[0-9]{2}-[0-9]{2}[ T][0-9:.]+//' \
    -e 's/[0-9]+(\.[0-9]+)?s\b/<TIME>/g' \
    -e 's/[0-9]+ GB\b/<MEM>/g' \
    -e 's/\b[0-9a-f]{8,}\b/<ID>/g' \
    "$1"
}

if diff <(normalize "$A") <(normalize "$B") > results/diff.txt; then
  echo "IDENTICAL: 1-Pod and 3-Pod produce the same verification result."
else
  echo "DIFFERENT: see results/diff.txt"
  head -100 results/diff.txt
  exit 1
fi
