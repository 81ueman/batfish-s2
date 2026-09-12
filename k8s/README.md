# Local Kubernetes demo (OrbStack)

Verifies that running S2 with **1 worker Pod** and with **3 worker Pods** gives
the same verification result for the same snapshot.

## Prerequisites

* OrbStack Kubernetes enabled (`orb config set k8s.enable true`, then restart).
  `kubectl get nodes` should show the `orbstack` node.
* The S2 module compiles (see `docs/s2-port/PORTING.md`; work in progress).
* Image built: `scripts/build-s2.sh --image`.

## How the pieces fit

| Object | Role |
| --- | --- |
| `Deployment/s2-controller` + `Service/s2-controller:4090` | S2 Controller (parse, partition, orchestrate) |
| `StatefulSet/s2-worker` + headless `Service/s2-worker` | N S2 workers; Pod DNS `s2-worker-<i>.s2-worker.s2.svc.cluster.local` |
| `Job/s2-orchestrate` | Sends S2's text commands to start workers then controller |
| `scripts/orchestrate.sh` | Protocol driver baked into the image |

The network being verified is partitioned across workers (this is what
"分ける" means here). Controller and workers are separate Pods; cross-Pod route
and packet exchange uses S2 sidecars over gRPC.

## Run

```sh
scripts/k8s-demo.sh 1
scripts/k8s-demo.sh 3
scripts/compare-answers.sh
```

`k8s/overlays/1pod` and `k8s/overlays/3pod` differ only in the worker replica
count and the `WORKERS` value passed to the orchestrator.

## Notes

* The bundled snapshot is `networks/bgp-resolution-loop` (a 3-router NX-OS eBGP
  topology) baked into the image at `/s2/inputs/`.
* `shard 1` is used initially; prefix sharding is an orthogonal memory
  optimization and is not required for the equality check on a tiny network.
* Worker Pods are separate Kubernetes Pods but share the cluster network; the
  "split" is the network *model* partition, not a NetworkPolicy.
