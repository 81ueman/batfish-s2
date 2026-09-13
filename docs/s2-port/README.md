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
| `IncrementalBdpEngine` | `public`; `newNode`, `iterationVirtualRouters`, `protected nextDataplane`, `protected hasNotReachedRoutingFixedPoint`, and the synchronization hooks `synchronizeWorkers`, `exchangeIterationHashCode`, `hasReachedTopologyFixedPoint`, `hasNotReachedIgpFixedPoint`, `initialSchedule` |
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

## M5 — distributed symbolic DPV

Decomposed into slices:

- [x] **Slice 1** BDD serialization primitive: `net.sf.javabdd.BDDTransfer` round-trips
  a BDD between two `JFactory` instances (`BddTransferTest`). This is what lets a
  symbolic packet cross a worker boundary.
- [x] **Slice 2** Cross-worker transition: `InterWorkerTransition` applies the
  wrapped transition locally and ships a non-empty resulting BDD to the worker
  owning the far state, returning zero so the local fixpoint does not consume it.
  `S2BddSidecar` transfers it over a real socket. `InterWorkerTransitionTest`
  verifies the symbolic packet crosses the boundary and round-trips exactly.
- [x] **Slice 3/4** Distributed BDD reachability fixpoint. State expressions are
  partitioned by owning hostname; each worker runs the forward fixpoint over its
  own states and ships crossing BDDs to the owner over `S2BddSidecar`, in
  barrier-synchronized rounds until global quiescence. `DistributedReachabilityTest`
  verifies that the per-state reachable BDDs for **1 and 3 workers equal Batfish's
  local `computeForwardReachableStates()`** (BDD equality via the codec + `biimp`).

The symbolic fixpoint is now wired into the multi-process/k8s runner: after the
control plane converges and FIBs are distributed, each worker builds its
`BDDReachabilityAnalysis`, runs its share of the distributed forward fixpoint with
`S2BddSidecar` transfer, and the controller compares the per-state reachable BDDs
against vanilla. Verified locally and on OrbStack Kubernetes for 1 and 3 Pods
(`ribs=MATCH reachability=MATCH symbolic=MATCH`).

The controller also evaluates the result at the **public API level**: it combines
the workers' backward-reachable BDDs and produces Batfish's reachability answer
(`BDDReachabilityUtils.constructFlows`), then compares the concrete flow set with
vanilla. Verified locally and on OrbStack Kubernetes for 1 and 3 Pods
(`ribs=MATCH reachability=MATCH symbolic=MATCH answer=MATCH`).

- [x] **Slice 5** Per-worker-local graph generation (paper §3.1/§4.3). Each worker
  builds its `BDDReachabilityAnalysis` with an `OwnedForwardingAnalysis` over its own
  switches, keeps only edges into its own states, and pulls the boundary edges
  (`PreOutEdgePostNat(src remote) -> PreInInterface(dst local)`) from the owner with a
  portable transition codec (`TransitionTransfer`). Verified per-state against the
  full graph for 1 and 3 workers (`ScaledReachabilityTest`) and on Kubernetes
  (`ribs=MATCH reachability=MATCH symbolic=MATCH answer=MATCH`). See `M5-SCALE.md`.

This closes the scalability gap: the symbolic edge table per worker now shrinks with
the number of workers, not just the fixpoint.

## Running the demos

Local multi-process (one JVM per worker):

```sh
bazel build //projects/s2:s2_main_deploy.jar
scripts/local-demo.sh 1
scripts/local-demo.sh 3
scripts/local-demo.sh 3 s2-line   # optional second arg selects the network
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
Batfish itself does not converge on it. `networks/s2-line/configs/{r1..r6}` is a
6-node static eBGP line used for the M5 symbolic scale evidence and for the
multi-hop distributed-control-plane regression test; it matches vanilla at 1, 3,
and 6 workers. `networks/s2-ospf/configs/{r1..r4}` is a 4-node OSPF line and
`networks/s2-ospf-bgp/configs/{r1,r2,r3}` mixes eBGP with OSPF; both are supported
(and tested) in the multi-process runner at 1 and 3 workers. EIGRP/IS-IS/RIP are not
distributed and are rejected for `>1` worker.

## Layout added by this work

```
projects/s2/          # our implementation + tests
networks/s2-triangle/ # 3-node demo snapshot
networks/s2-line/     # 6-node scale snapshot
networks/s2-ospf/     # 4-node OSPF snapshot
networks/s2-ospf-bgp/ # eBGP + OSPF snapshot
docker/, k8s/, scripts/, docs/s2-port/
```
