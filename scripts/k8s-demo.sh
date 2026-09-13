#!/usr/bin/env bash
# Run the S2 1-Pod or 3-Pod demo on the local cluster and capture the controller output.
# Usage: scripts/k8s-demo.sh <1|3> [network]
#   network defaults to s2-triangle; any snapshot under networks/ baked into the image works
#   (e.g. s2-ospf, s2-ospf-bgp, s2-redist, s2-line).
set -euo pipefail

W="${1:?usage: k8s-demo.sh <N> [network]  (N has k8s/overlays/<N>pod)}"
NETWORK="${2:-s2-triangle}"
if [[ ! -d "k8s/overlays/${W}pod" ]]; then
  echo "no k8s/overlays/${W}pod; available: $(ls k8s/overlays | sed 's/pod$//' | tr '\n' ' ')" >&2
  exit 2
fi

cd "$(git rev-parse --show-toplevel)"
mkdir -p results

echo "== resetting namespace s2 =="
# Delete without --wait, then wait with a bound: the shared RWO PVC's pvc-protection finalizer can
# otherwise stall namespace termination indefinitely (which would hang the next run's apply).
kubectl delete namespace s2 --ignore-not-found --wait=false
for _ in $(seq 1 90); do
  kubectl get namespace s2 >/dev/null 2>&1 || break
  # Clear the PVC finalizer if termination is stuck on it.
  kubectl -n s2 patch pvc --all --type=merge -p '{"metadata":{"finalizers":null}}' >/dev/null 2>&1 || true
  sleep 2
done
if kubectl get namespace s2 >/dev/null 2>&1; then
  echo "== namespace s2 still terminating; force-finalizing =="
  kubectl get namespace s2 -o json 2>/dev/null \
    | python3 -c 'import sys,json;o=json.load(sys.stdin);o["spec"]["finalizers"]=[];print(json.dumps(o))' 2>/dev/null \
    | kubectl replace --raw /api/v1/namespaces/s2/finalize -f - >/dev/null 2>&1 || true
  sleep 3
fi
if kubectl get namespace s2 >/dev/null 2>&1; then
  echo "ERROR: namespace s2 could not be deleted" >&2
  exit 1
fi

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
