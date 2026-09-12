#!/usr/bin/env bash
# Run the S2 1-Pod or 3-Pod demo on the local cluster and capture the
# controller output for comparison.
#
# Usage: scripts/k8s-demo.sh <1|3>
set -euo pipefail

WORKERS="${1:?usage: k8s-demo.sh <1|3>}"
case "$WORKERS" in 1|3) ;; *) echo "workers must be 1 or 3" >&2; exit 2;; esac

cd "$(git rev-parse --show-toplevel)"
OVERLAY="k8s/overlays/${WORKERS}pod"
RESULTS="results/controller-${WORKERS}pod.log"
mkdir -p results

echo "== resetting namespace s2 =="
kubectl delete namespace s2 --ignore-not-found --wait=true

echo "== applying ${OVERLAY} =="
kubectl apply -k "$OVERLAY"

echo "== waiting for controller =="
kubectl -n s2 wait --for=condition=ready pod -l app=s2-controller --timeout=600s

echo "== waiting for orchestrator job =="
kubectl -n s2 wait --for=condition=complete job/s2-orchestrate --timeout=1800s \
  || { kubectl -n s2 logs job/s2-orchestrate || true; exit 1; }

echo "== capturing controller log -> ${RESULTS} =="
kubectl -n s2 logs deploy/s2-controller > "$RESULTS"
kubectl -n s2 logs job/s2-orchestrate | tee "results/orchestrator-${WORKERS}pod.log"
echo "done: ${RESULTS}"
