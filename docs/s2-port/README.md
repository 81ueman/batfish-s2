# S2 re-implementation on latest upstream Batfish

Clean, from-scratch implementation of **S2: A Distributed Configuration Verifier
for Hyper-Scale Networks** (SIGCOMM'25) on top of current upstream Batfish.

An earlier copy-based port was superseded by this from-scratch implementation. The paper authors'
open-source implementation [`XJTU-NetVerify/s2`](https://github.com/XJTU-NetVerify/s2) (Apache-2.0)
is used as a design reference only; none of its code is copied into this work.

## Repos / branches

| Thing | Where |
| --- | --- |
| Upstream Batfish base | `batfish/batfish` master `2a513d0` |
| This work | branch `master` |
| Reference implementation | [`XJTU-NetVerify/s2`](https://github.com/XJTU-NetVerify/s2) (Apache-2.0, design reference only) |
| Paper | *S2: A Distributed Configuration Verifier for Hyper-Scale Networks* (SIGCOMM'25) |

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

### Core hooks

The distribution needs a small set of hooks in shared Batfish (all default to stock behavior):

| File | Change |
| --- | --- |
| `Node` | drop `final` |
| `IncrementalBdpEngine` | `public`; `newNode`, `iterationVirtualRouters`, `protected nextDataplane`, `hasNotReachedRoutingFixedPoint`, the synchronization hooks `synchronizeWorkers`, `exchangeIterationHashCode`, `hasReachedTopologyFixedPoint`, `hasNotReachedIgpFixedPoint`, `initialSchedule`; `dataPlaneNodes` (final-dataplane scope) and the EGP schedule hooks `runEgpFixpoint` / `reconcileEgpSchedule` / `ospfInternalSchedule` |
| `BgpRoutingProcess` | `public`; `getOutgoingRoutesForEdge` protected; `appointed`/`restageExternalAdvertisements` (prefix sharding) |
| `VirtualRouter` | `initStubFib` (config-only remote FIB), `initForEgpPrefixRound` / `drainBgpRoutes` / `restoreBgpRoutes` (prefix sharding) |
| `BDDReachabilityAnalysisFactory` | nullable `localNodes` (owned-only factory scoping) |
| `PrefixSpace` | gated positive-only memo (`-Ds2.prefixSpacePositiveCacheOnly`) |
| `OspfRoutingProcess` | gated `EnqueueProvider` (distributed OSPF messages) |

### New module `//projects/s2`

* `DistributedNode` — real/shadow node
* `S2BdpEngine` — engine over `DistributedNode`s, real-only iteration, global convergence
* `S2Cluster` — shared global convergence check
* `partition/` — pluggable `NodePartitioner` schemes + `CommunicationGraph` / `NodeWeights`
* `PrefixSharder` / `PrefixDependencyGraph` / `PrefixShardCountSelector` — control-plane prefix sharding
* `RemoteNodeDescriptor` — lightweight remote (shadow) config
* `S2ReachabilityWorker` / `S2BddSidecar` / `InterWorkerTransition` / `TransitionTransfer` — distributed symbolic DPV

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

M4's "FIBs are distributed, forwarding is not" gave way to the distributed symbolic DPV in M5
below. The per-worker reachability digest is now optional: in owned/descriptor mode the controller
implies forwarding equality from the exact RIB match (see `M5-SCALE.md` and `OPS.md`).

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
`S2BddSidecar` transfer, and the verify role compares the per-state reachable BDDs
against vanilla. Verified locally and on OrbStack Kubernetes for 1 and 3 Pods
(`ribs=MATCH reachability=MATCH symbolic=MATCH`).

The verify role also evaluates the result at the **public API level**: it combines
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

> **Naming note.** "M5" here is the README milestone (distributed symbolic DPV, done). `REMAINING.md`
> also uses "M5" for a *memory* task (dataplane prefix sharding / on-disk RIB+FIB), which was
> **dropped** — see `REMAINING.md` A5.

## Post-M5 work (scale / memory)

Added after the distributed DPV (defaults noted; see `M5-SCALE.md`, `OPS.md`, `REMAINING.md`):

* **Controller-shipped configs** (default on): the controller parses once and ships the parsed
  configs, so workers do not re-parse the snapshot (`-Ds2.noShipConfigs=true` reproduces the old
  per-worker parse; this was the parse-dominated floor).
* **Owned-only dataplane** (default on): workers build full RIBs/FIBs only for owned nodes; shadows
  get a config-only stub FIB (`VirtualRouter.initStubFib`). Falls back to full configs for
  tracks / VXLAN / tunnel / IPsec. Disable with `-Ds2.ownedDataplane=false`.
* **Descriptor shadows** (default on): remote nodes are materialized from a lightweight
  `RemoteNodeDescriptor` (drops remote ACL / policy / route-map bodies). Disable with
  `-Ds2.descriptorShadows=false`.
* **Node partitioner** (default **`WEIGHTED_LPT_FM`**): pluggable `RANDOM` / `NAME_ORDERED` /
  `WEIGHTED_LPT_FM` / `GREEDY_REGION` / `METIS` / `AUTO`, computed once on the controller and shipped
  to workers (`-Ds2.partition=<scheme>`). Measured per-scheme peaks in `PARTITIONING-PLAN.md` 6.13.
* **Control-plane prefix sharding + DPDG**: `S2_PREFIX_SHARDS=N` or `=auto` (deterministic shard
  count from the prefix dependency graph), plus the aggregate / static / external-announcement prefix
  closure (see `PARTITIONING-PLAN.md`).
* **Controller / verifier split (A7)**: the controller is a lightweight coordinator; a separate
  `verify` role (a new JVM locally, the `s2-verifier` Job on Kubernetes) runs the vanilla dataplane
  and reference BDD analysis. This cut the `s2-mega` controller peak from ~1326 to ~529 MiB.
* **k8s multi-Pod scale-out**: `k8s/overlays/{1,3,6,8,16}pod` and `scripts/k8s-demo.sh <N>`;
  `s2-fat4` at 6 / 8 / 16 and `s2-mega` at 8 / 16 all `MATCH`.
* **Drop-in engine** (`-dataplaneengine=s2`): S2 is also a Batfish `DataPlanePlugin`, so the
  distributed engine is selected like the stock `ibdp` engine — same snapshot input, same question
  engine/REST/pybatfish, `-s2workers N`, and no verify step. The global data plane is assembled
  lazily per host. See [`PLUGIN.md`](PLUGIN.md).

## Running the demos

Local multi-process (one JVM per worker, plus a separate verify JVM):

```sh
bazel build //projects/s2:s2_main_deploy.jar
scripts/local-demo.sh 1
scripts/local-demo.sh 3
scripts/local-demo.sh 3 s2-line   # optional second arg selects the network
```

OrbStack Kubernetes (controller Job + N worker Pods + verifier Job):

```sh
scripts/build-s2.sh --image        # builds s2:local
scripts/k8s-demo.sh 1              # 1 / 3 / 6 / 8 / 16 worker Pods
scripts/k8s-demo.sh 3
scripts/compare-answers.sh
scripts/k8s-demo.sh 6 s2-fat4      # multi-Pod scale-out on a DCN
```

The controller is a **lightweight coordinator**: it parses the snapshot, resolves
the partition, ships configs, collects the workers' RIBs/digest/symbolic BDDs, and
writes them to `$S2_OUTPUT_DIR/worker-results-<W>.bin`. It no longer computes the
vanilla single-machine dataplane or the reference BDD analysis. The `verify` role
(`S2Main verify <network> <numWorkers>`, a new JVM locally and the `s2-verifier`
Job on Kubernetes) reads that file, reproduces the reference computation, and
writes `result-<W>worker.txt` with the same `S2 MATCH (...)` line. The two k8s
Jobs share a small `ReadWriteOnce` PVC at `/s2/shared` (`S2_OUTPUT_DIR`); the
verifier waits for the controller's results, so no ordering is required. This is
what lets the controller's heap drop far below a worker's (A7 in `REMAINING.md`,
resources in `OPS.md`).

## Test snapshot

`networks/s2-triangle/configs/{r1,r2,r3}` is a static eBGP triangle (also copied
under `projects/s2/src/test/resources/...`). A loop testrig is unsuitable: vanilla
Batfish itself does not converge on it. `networks/s2-line/configs/{r1..r6}` is a
6-node static eBGP line used for the M5 symbolic scale evidence and for the
multi-hop distributed-control-plane regression test; it matches vanilla at 1, 3,
and 6 workers. `networks/s2-ospf/configs/{r1..r4}` is a 4-node OSPF line,
`networks/s2-ospf-bgp/configs/{r1,r2,r3}` mixes eBGP with OSPF, and
`networks/s2-redist/configs/{r1..r4}` exercises OSPF<->BGP redistribution; all three are
supported (and tested) in the multi-process runner and on Kubernetes at 1 and 3 worker
Pods. EIGRP/IS-IS/RIP are not distributed and are rejected for `>1` worker.

## Layout added by this work

```
projects/s2/          # our implementation + tests
networks/s2-triangle/ # 3-node demo snapshot
networks/s2-line/     # 6-node scale snapshot
networks/s2-ospf/     # 4-node OSPF snapshot
networks/s2-ospf-bgp/ # eBGP + OSPF snapshot
networks/s2-redist/   # OSPF<->BGP redistribution snapshot
networks/s2-agg/      # eBGP with a BGP aggregate (prefix-sharding closure test)
networks/s2-static/   # static route redistributed into BGP (redistribution-closure test)
networks/s2-external/ # external BGP announcement into BGP (external-closure test)
networks/s2-acl/      # 6-node eBGP line with 3000-line ACLs (descriptor/ACL-heavy testbed)
networks/s2-fat2/     # generated 5-switch FatTree k=2 (DCN)
networks/s2-fat4/     # generated 20-switch FatTree k=4 (DCN)
networks/s2-fat6/     # generated 45-switch FatTree k=6 (DCN, larger)
networks/s2-hub/      # hub + 8 spokes (WAN star / route-reflector-like)
networks/s2-genline/  # generated 4-switch eBGP line (generator smoke test)
scripts/gen-topology.py    # generate FatTree/line/hub testbeds
scripts/bench.sh           # network x workers matrix -> metrics table
scripts/bench-table.sh     # cached size-ladder x mode -> markdown table
scripts/partition-metrics.py  # imbalance / weighted cut for a network
scripts/calibrate-weights.py  # node-weight features / fit / cost-aware imbalance
scripts/shard-sweep.sh     # prefix-shard-count sweep (peak vs N)
scripts/ci.sh              # CI entry point (unit / --upstream / --matrix)
scripts/ci-matrix.sh       # opt-in demo matrix
k8s/overlays/{1,3,6,8,16}pod  # worker replica counts (scripts/k8s-demo.sh <N>)
docs/s2-port/OPS.md        # k8s resources, default -Xmx, METIS
docs/s2-port/PLUGIN.md     # S2 as a Batfish dataplane engine (-dataplaneengine=s2)
docs/s2-port/RESULTS.md    # consolidated drop-in/pool results (memory B.6, k8s B.7)
networks/s2-big-bgp/  # 6-node eBGP line with 192 prefixes (prefix-sharding measurement)
networks/s2-big2/     # 10-node eBGP line with 640 prefixes (larger prefix-sharding measurement)
networks/s2-huge/     # 8-node eBGP line with 2048 prefixes
networks/s2-mega/     # 16-node eBGP line with 4096 prefixes
networks/s2-giga/     # 16-node eBGP line with 32768 prefixes (largest snapshot)
docker/, k8s/, scripts/, docs/s2-port/
```

## Remaining work

See [`REMAINING.md`](REMAINING.md) for the now-small open-item list (the bulk of the memory work is
done: config shipping, owned-only dataplane, descriptor shadows, node partitioner, prefix sharding,
controller/verifier split; the dataplane on-disk-RIB task "M5" was dropped),
[`PARTITIONING-PLAN.md`](PARTITIONING-PLAN.md) for the partitioner / prefix-sharding evaluation, and
[`M5-SCALE.md`](M5-SCALE.md) for the memory/scale measurements.

## License

This work — the `//projects/s2` module, the S2 runner and scripts, and the S2 documentation under
`docs/s2-port/` — is released under the **Apache License, Version 2.0**, the same license as
Batfish itself; see the repository root [`LICENSE`](../../LICENSE). The new S2 source files carry an
`SPDX-License-Identifier: Apache-2.0` header.

## Acknowledgements

* Built on [Batfish](https://github.com/batfish/batfish) (Apache-2.0).
* Design follows *S2: A Distributed Configuration Verifier for Hyper-Scale Networks* (SIGCOMM'25)
  and its open-source implementation [`XJTU-NetVerify/s2`](https://github.com/XJTU-NetVerify/s2)
  (Apache-2.0), which was consulted for design only; no reference code is copied here. An earlier
  copy-based port was superseded by this from-scratch implementation.

