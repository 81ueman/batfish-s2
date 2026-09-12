#!/usr/bin/env bash
# Run the S2 1-Pod or 3-Pod demo on the local cluster and capture the controller output.
# Usage: scripts/k8s-demo.sh <1|3>
set -euo pipefail

W="${1:?usage: k8s-demo.sh <1|3>}"
case "$W" in 1|3) ;; *) echo "workers must be 1 or 3" >&2; exit 2;; esac

cd "$(git rev-parse --show-toplevel)"
mkdir -p results

echo "== resetting namespace s2 =="
kubectl delete namespace s2 --ignore-not-found --wait=true

echo "== applying k8s/overlays/${W}pod =="
kubectl apply -k "k8s/overlays/${W}pod"

echo "== waiting for controller job =="
if ! kubectl -n s2 wait --for=condition=complete job/s2-controller --timeout=1800s; then
  kubectl -n s2 describe job/s2-controller || true
  kubectl -n s2 logs job/s2-controller || true
  exit 1
fi

kubectl -n s2 logs job/s2-controller | tee "results/k8s-controller-${W}pod.log"
echo "done: results/k8s-controller-${W}pod.log"
