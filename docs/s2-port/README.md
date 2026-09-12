# S2 re-implementation on latest upstream Batfish

Clean, from-scratch implementation of **S2: A Distributed Configuration Verifier
for Hyper-Scale Networks** (SIGCOMM'25) on top of current upstream Batfish.

The old copy-based port is preserved on branch `s2-copied-old` and the reference
repo `~/ghq/github.com/81ueman/s2-reference` is used as a design reference only.

## Repos / branches

| Thing | Where |
| --- | --- |
| Upstream Batfish base | `batfish/batfish` master `2a513d0` |
| This work | branch `s2` |
| Old copy-based port | branch `s2-copied-old` |
| Reference implementation | `~/ghq/github.com/81ueman/s2-reference` |
| Paper | `nv-papers/papers/s2-2025.pdf` |

## Design (see `DESIGN.md`)

Batfish's BGP iteration pulls a neighbor's advertisements with

```java
remoteProcess.getOutgoingRoutesForEdge(edgeId, nodes, bgpTopology, nc, isNewSession);
```

so distribution needs no RIB surgery:

* Each worker builds a `Node` for every switch.
* Owned switches are **real**; the rest are **shadow** nodes that delegate their
  virtual routers to the owning worker's real node.
* The engine iterates only real routers (`iterationVirtualRouters` hook) but sees
  shadows for neighbor lookups and dataplane construction.
* Convergence is decided **globally** (`S2Cluster` + barrier), matching S2's
  controller-level fixpoint.

### Minimal core hooks (3 files)

| File | Change |
| --- | --- |
| `Node` | drop `final` |
| `IncrementalBdpEngine` | `public`; `newNode`, `iterationVirtualRouters`, `protected nextDataplane`, `protected hasNotReachedRoutingFixedPoint` |
| `BgpRoutingProcess` | `public`; `getOutgoingRoutesForEdge` protected |

### New module `//projects/s2`

* `DistributedNode` — real/shadow node
* `NetworkPartitioner` — balanced hostname→worker assignment
* `S2BdpEngine` — engine over `DistributedNode`s, real-only iteration, global convergence
* `S2Cluster` — shared global convergence check

## Milestones

- [x] **M1** single JVM, in-process: 1 and 3 logical workers produce main RIBs
  **identical** to vanilla Batfish (`S2DistributedControlPlaneTest`).
- [x] **M2** remote route exchange over a real sidecar socket with Java
  serialization; 1 and 3 workers still match (`S2RemoteSidecarTest`, 18 RPCs).
- [x] **M3** Kubernetes 1 Pod vs 3 Pods. Controller Job + worker StatefulSet;
  both runs report `S2 MATCH` vs vanilla and `scripts/compare-answers.sh`
  confirms they are identical.
- [x] **M4** FIB distribution + data-plane check. Each worker fetches the owning
  workers' main RIBs over the sidecar into its shadows, so every worker builds a
  complete forwarding analysis; dataplane-level BGP reachability is re-enabled and
  a traceroute-based reachability digest is computed on the distributed
  dataplane. Both ribs and reachability match vanilla for 1 and 3 workers/Pods.

M4 limitation: the reachability digest is computed per worker over the assembled
dataplane (FIBs are distributed, forwarding is not). The paper's fully distributed
symbolic DPV (BDD port predicates forwarded across workers via InterWorkerTransition)
is still future work.

## Running the demos

Local multi-process (one JVM per worker):

```sh
bazel build //projects/s2:s2_main_deploy.jar
scripts/local-demo.sh 1
scripts/local-demo.sh 3
```

OrbStack Kubernetes (controller + 1 or 3 worker Pods):

```sh
scripts/build-s2.sh --image        # builds s2:local
scripts/k8s-demo.sh 1
scripts/k8s-demo.sh 3
scripts/compare-answers.sh
```

## Test snapshot

`networks/s2-triangle/configs/{r1,r2,r3}` is a static eBGP triangle (also copied
under `projects/s2/src/test/resources/...`). A loop testrig is unsuitable: vanilla
Batfish itself does not converge on it.

## Layout added by this work

```
projects/s2/          # our implementation + tests
networks/s2-triangle/ # demo snapshot
docker/, k8s/, scripts/, docs/s2-port/
```
