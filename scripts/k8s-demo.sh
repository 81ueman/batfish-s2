#!/usr/bin/env bash
# Run the S2 1-Pod or 3-Pod demo on the local cluster and capture the controller output.
# Usage: scripts/k8s-demo.sh <1|3> [network]
#   network defaults to s2-triangle; any snapshot under networks/ baked into the image works
#   (e.g. s2-ospf, s2-ospf-bgp, s2-redist, s2-line).
set -euo pipefail

W="${1:?usage: k8s-demo.sh <1|3> [network]}"
NETWORK="${2:-s2-triangle}"
case "$W" in 1|3) ;; *) echo "workers must be 1 or 3" >&2; exit 2;; esac

cd "$(git rev-parse --show-toplevel)"
mkdir -p results

echo "== resetting namespace s2 =="
kubectl delete namespace s2 --ignore-not-found --wait=true

echo "== applying k8s/overlays/${W}pod (network=${NETWORK}) =="
# Render the overlay and substitute the snapshot name so we do not need one overlay per network.
rendered="$(kubectl kustomize "k8s/overlays/${W}pod" | sed "s/s2-triangle/${NETWORK}/g")"
# Optionally pin the partition scheme (default 'auto' from the manifests):
#   S2_PARTITION=METIS scripts/k8s-demo.sh 3
if [[ -n "${S2_PARTITION:-}" ]]; then
  rendered="$(printf '%s\n' "$rendered" | sed "s/-Ds2.partition=auto/-Ds2.partition=${S2_PARTITION}/g")"
fi
printf '%s\n' "$rendered" | kubectl apply -f -

echo "== waiting for controller job =="
if ! kubectl -n s2 wait --for=condition=complete job/s2-controller --timeout=1800s; then
  kubectl -n s2 describe job/s2-controller || true
  kubectl -n s2 logs job/s2-controller || true
  exit 1
fi

echo "== waiting for verifier job =="
if ! kubectl -n s2 wait --for=condition=complete job/s2-verifier --timeout=1800s; then
  kubectl -n s2 describe job/s2-verifier || true
  kubectl -n s2 logs job/s2-verifier || true
  exit 1
fi

kubectl -n s2 logs job/s2-controller | tee "results/k8s-controller-${NETWORK}-${W}pod.log"
kubectl -n s2 logs job/s2-verifier | tee "results/k8s-verifier-${NETWORK}-${W}pod.log"
echo "done: results/k8s-controller-${NETWORK}-${W}pod.log results/k8s-verifier-${NETWORK}-${W}pod.log"
