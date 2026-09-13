# M5 scale-out — per-worker local graph generation

Status: **implemented and verified.** Each worker now generates only the reachability
edges whose source it owns and pulls the boundary edges into its own states from
their owners. The paper's §3.1/§4.3 scale property holds: per-worker edge tables
shrink with the number of workers (the fixpoint was already distributed).

## Verified

* **M1–M5 all green.** OrbStack Kubernetes, 1 Pod and 3 Pods both report
  `ribs=MATCH reachability=MATCH symbolic=MATCH answer=MATCH` against vanilla Batfish
  (`scripts/k8s-demo.sh 1|3`, `scripts/compare-answers.sh`).
* The scaled backward fixpoint equals the full-graph fixpoint **per state** for
  1 and 3 workers (`ScaledReachabilityTest`), on the `s2-triangle` snapshot. The runner
  now performs the same strict per-state comparison itself (`symbolic=MATCH`).
* Multi-process runner (`scripts/local-demo.sh`) matches for 1 and 3 workers (verified
  repeatedly on the 3-worker triangle: 5/5).
* `networks/s2-line` (6-node static eBGP line) matches vanilla for 1, 3, and 6 workers,
  and is covered by `S2DistributedControlPlaneTest#testMultiHopLineMatchesVanilla`.
* The runner reports per-worker peak heap (`S2ControlMessages.Result.peakHeapBytes`,
  printed by the controller and written to `result-<N>worker.txt`).

## Design (approved and implemented)

1. **Direction: backward** (Batfish's answer path / `getIngressLocationReachableBDDs`).
2. **Local generation.** Each worker builds its `BDDReachabilityAnalysisFactory` with
   an `OwnedForwardingAnalysis` (its own hostnames only), so it generates transitions
   only for locally owned sources. The worker keeps only edges whose **post** state it
   owns — remote-remote and local→remote edges are never traversed locally and are
   dropped (`S2ReachabilityWorker`).
3. **Boundary edge pull.** The only edges crossing a worker boundary are
   `PreOutEdgePostNat(src, dst) -> PreInInterface(dst)`. A worker asks each peer for
   its edges whose source the peer owns and whose destination the requester owns
   (`S2Messages.BoundaryEdgesRequest`), reconstructs the transitions in its own BDD
   factory and adds them to its reverse-edge map. A controller-synchronized barrier
   runs once so every worker has published its analysis before any pulls.
4. **Transitions: only portable types.** `TransitionTransfer` serializes
   `Identity`, `Zero`, `Constraint`, `EraseAndSet`, `Composite`, and `Or` (with their
   BDDs via `BDDTransfer`); engine-backed transitions (source/last-hop managers) are
   rejected. The reachability graph without firewall sessions only uses the portable
   set on boundary edges.
5. The runner uses the scaled backward path directly (there is no separate full-graph
   fallback any more).

## Concurrency fix found while verifying

`S2BddSidecar` used to deserialize the incoming BDD **on the sidecar thread** into the
worker's `JFactory`, which is not thread-safe and raced with the worker's own fixpoint.
The sidecar now hands the raw payload to the worker, which deserializes it on its own
thread (`S2ReachabilityWorker.drainInbox`). The same fix was applied to the test
harnesses (and their inboxes were made thread-safe).

## Scale evidence

`S2Main` prints, per worker, the number of locally generated and pulled edges. On the
6-node `s2-line` snapshot (static eBGP):

| workers | worker edge counts (local + pulled) |
| --- | --- |
| 1 | 265 + 0 |
| 3 | 128+3, 68+4, 55+3 |

The workers in the 3-worker run generate 128/68/55 local edges and pull only 3–4 each,
i.e. each worker's table is a fraction of the 265-edge single-worker table — the M5
scale property. The 6-worker `s2-line` run (1 node per worker) also matches vanilla and
reports 93+1, 34+2, 34+2, 34+2, 34+2, 21+1.

And on the 3-node `s2-triangle`:

| workers | worker edge counts (local + pulled) |
| --- | --- |
| 1 | 151 + 0 |
| 3 | 75+2, 34+2, 34+2 |

Note: each worker additionally holds the few terminal-state edges (owner `""`, assigned
to worker 0), which is why worker 0's count is larger.

## Distributed control-plane synchronization (found while verifying)

Multi-hop topologies exposed three ordering bugs in the distributed control plane (all
before any symbolic reachability), which are now fixed:

1. **Phase barriers.** The stock engine relies on `parallelStream().forEach(...)` phases
   completing before the next begins, so no node writes state another node reads. In a
   distributed run a worker could run `endOfEgpInnerRound` (which overwrites the
   neighbor-visible BGP deltas) while a peer was still pulling them, making convergence
   order-dependent. `IncrementalBdpEngine` now calls a `synchronizeWorkers()` hook at
   each such boundary; `S2BdpEngine` makes it a global barrier.
2. **Global topology convergence.** The outer topology fixed-point check was local, so
   one worker could start another topology iteration while peers exited. The
   `hasReachedTopologyFixedPoint(local)` hook makes it a global AND.
3. **Global oscillation detection.** Each worker computed its own iteration hashcode for
   schedule selection, so they could switch to `NODE_SERIALIZED` at different iterations
   and desynchronize the barriers. The `exchangeIterationHashCode(local)` hook sums the
   hashes across workers (`S2Coordinator.sumAll`), so the switch is cluster-wide. The
   remote controller implements this with a `SumRequest`/`SumResponse` round.
4. **Deterministic schedule.** The default `NODE_COLORED` schedule colors a worker's own
   (partially shadowed) BGP topology, so different workers can get a different number of
   color classes and therefore a different number of schedule steps — which breaks any
   per-step phase barrier. `S2BdpEngine.initialSchedule` starts from `ALL` (one step);
   the oscillation fallback `NODE_SERIALIZED` has one step per node. Both have a step
   count that is identical across workers.
5. **IGP phases (OSPF / EIGRP / IS-IS / RIP).** The same phase barriers are applied to
   the IGP portions of the engine (`initForIgpComputation`, `initOspfInternalRoutes`,
   `initRipInternalRoutes`, and the EIGRP/IS-IS/OSPF-external phases in
   `computeDependentRoutesIteration`), their convergence loops are made global via the
   `hasNotReachedIgpFixedPoint` hook, and the OSPF internal schedule is `ALL`
   (deterministic).

With these, `s2-line` matches vanilla at 1, 3, and 6 workers and multi-worker runs no
longer hang.

## Distributed OSPF

OSPF is now distributed in the multi-process runner too. The wiring mirrors BGP's shadow
delegation:

* A shadow VR is never iterated, but a real OSPF process looks its neighbors up by
  process name and **pushes** messages into the neighbor's queues
  (`OspfRoutingProcess.getNeighborProcess` → `enqueueMessagesIntra/Inter/Type1/Type2`).
* `VirtualRouter.initShadowOspfProcesses` gives each shadow the (inert) OSPF process
  objects, and `DistributedNode.installRemoteOspfProviders` installs a
  `RemoteOspfEnqueueProvider` on them. That provider serializes the would-be enqueue and
  ships it over the sidecar; the owner's `S2SidecarHandlers` enqueues it on the real
  process. The queues are `ConcurrentLinkedQueue`, so the sidecar thread is safe.
* `OspfRoutingProcess.EnqueueProvider` is the pluggable hook; `OspfTopology.EdgeId` is
  now `Serializable`.

**Redistribution** is covered too. `networks/s2-redist` has OSPF->BGP redistribution on
one border router (r2) and BGP->OSPF redistribution (with `subnets`) on another (r3), so
an OSPF-only router (r4) learns a remote loopback as an OSPF external type-2 route.
`testRedistributionProducesRoutes` asserts both directions actually produce routes;
`testRedistributionMatchesVanilla` and `testRemoteRedistributionSidecar` assert the
distributed result equals vanilla at 1 and 3 workers.

Verified end to end: the multi-process local runner and OrbStack Kubernetes (1 and 3
worker Pods) both report `ribs=MATCH reachability=MATCH symbolic=MATCH answer=MATCH` for
`s2-ospf`, `s2-ospf-bgp`, and `s2-redist`. `scripts/k8s-demo.sh <1|3> [network]` and
`scripts/compare-answers.sh [network]` take the snapshot name.

**Protocol scope note.** Only eBGP and OSPF are distributed. EIGRP, IS-IS, and RIP are
not (they have different cross-node shapes: EIGRP/IS-IS also push into a neighbor's
structures, RIP pulls the neighbor's RIP RIB). A multi-worker run of a snapshot using any
of them fails fast with a clear message instead of an NPE
(`S2Main.assertDistributedProtocolsSupported`). Since those protocols have no shadow
delegation, leaving them unimplemented is fine as long as the snapshots use only BGP and
OSPF — mixed BGP+OSPF networks are fully supported.

## Control-plane prefix sharding (B)

`S2_PREFIX_SHARDS=N` (or `-Ds2.prefixShards=N`, default 1) shards the **control-plane BGP
computation** by destination prefix (this is the S2 paper's prefix sharding):

- The engine runs the EGP fixpoint once per prefix round. Each round appoints a shard
  (`BgpRoutingProcess.appointed`) so only that shard's prefixes are originated, advertised,
  and merged; the round also seeds `_mainRibDeltaPrevRound` from the shard's routes so a
  prefix that only appeared in the first round's delta is still originated
  (`VirtualRouter.initForEgpPrefixRound`).
- With `-Ds2.prefixShardExternalize=true`, each round's BGP routes are serialized off the
  RIB (`BgpRoutingProcess.drainV4Routes`) and dropped, then restored after all rounds, so
  only one shard's BGP RIB is live at a time.
- The union over shards equals the unsharded result; verified by
  `S2DistributedControlPlaneTest#testPrefixShardingMatchesVanilla` and by the demos.

Measured on `networks/s2-big-bgp` (6-router eBGP line, 192 origination prefixes, 3
workers):

| shards | live BGP routes / worker / round | max peak heap / worker |
| --- | --- | --- |
| 1 | 384 | 426.8 MiB |
| 4 | 96 | 323.1 MiB |
| 8 | 48 | 291.6 MiB |
| 16 | 24 | 334.2 MiB |

Measured on larger topologies (all runs
`ribs=MATCH reachability=MATCH symbolic=MATCH answer=MATCH`):

`networks/s2-big2` (10-router eBGP line, 640 origination prefixes, 3 workers):

| shards | live BGP routes / worker / round | max peak heap / worker |
| --- | --- | --- |
| 1 | 1920 (total) | 649.6 MiB |
| 8 | 320 | 478.1 MiB |
| 16 | 160 | 510.8 MiB |

`networks/s2-huge` (8-router eBGP line, 2048 prefixes, 3 workers):

| shards | max peak heap / worker |
| --- | --- |
| 1 | 689.9 MiB |
| 8 (+externalize) | 530.1 MiB |

`networks/s2-mega` (16-router eBGP line, 4096 prefixes, 3 workers):

| shards | max peak heap / worker |
| --- | --- |
| 1 | 2090.8 MiB |
| 8 (+externalize) | 873.4 MiB |

The live BGP RIB is bounded to roughly `total/N`. The peak-heap reduction grows with the
prefix count: ~23% at 640-2048 prefixes but **~58% at 4096 prefixes** (2.09 → 0.87 GiB),
because the BGP fixpoint/RIB becomes a larger fraction of the peak. So B is the right lever
for large BGP tables.

### Where the remaining peak goes (bottleneck)

Phase-level peak heap (`S2BdpEngine.reportPhase`) on `s2-mega`, worker 0:

| phase | baseline | B (8 shards + externalize) |
| --- | --- | --- |
| after IGP / init | 517.0 MiB | 564.4 MiB |
| after EGP iteration 1 | 1033.0 MiB | 668.4 MiB |
| after nextDataplane 1 | 1360.4 MiB | 668.4 MiB |

Baseline worst worker 2090.8 MiB, B 873.4 MiB. So B removes most of the EGP +
dataplane-construction part (worker 0: 1360 → 668 MiB). Adding a heap cap on top:

| configuration (`s2-mega`, 3 workers) | max peak heap / worker | result |
| --- | --- | --- |
| baseline | 2090.8 MiB | MATCH |
| B (8 shards + externalize) | 873.4 MiB | MATCH |
| B + `-Xmx1g` | 595.4 MiB | MATCH |

That is a **~72% worst-worker peak reduction** at 4096 prefixes, still matching vanilla.

**What the remaining floor is.** Phase attribution (added `reportPhase`) shows the peak is
already reached at the very first phase, right after the snapshot is parsed:

| phase (`s2-big2`, worker 0) | peak heap |
| --- | --- |
| after snapshot load | 289.2 MiB |
| after building nodes | 289.2 MiB |
| … after EGP / nextDataplane | 318.5 / 390.5 MiB |

So the floor is **the per-worker parse of the full snapshot** (every worker holds all
configs, real and shadow). It is *transient*: forcing a GC after the control plane leaves
only ~40 MiB live, and B + `-Xmx1g` completes far below the uncapped peak. Consequences:

* The floor is not reduced by sharding; the effective lever is GC headroom (heap cap).
* A true architectural reduction would need to cut transient allocations in the parser /
  FIB builder (upstream Batfish) or avoid parsing the full snapshot on every worker — a
  much larger change than prefix sharding.

A query/header-space variant (sharding the reachability query by destination prefix) was
also prototyped and measured: it shrank the symbolic result ~20% but saturated and did not
reduce peak heap, so it was removed in favor of this control-plane variant.

### Memory attribution and peak reduction

Forcing a GC right after the control plane on `s2-big2` shows the **retained set is small**
(live ~40 MiB control plane + ~50 MiB reachability/BDD) while the **peak is ~550-580 MiB**:
the peak is dominated by *transient* control-plane allocation (route/delta/FIB objects),
not retained data. Consequences:

* Prefix sharding (B) bounds the live BGP RIB and lowers the peak ~20-25%, but cannot go
  much further by itself because the retained prefix state is a small fraction of the peak.
* The effective lever for the transient part is GC headroom. Capping the worker heap
  (`-Xmx512m`) already lowers the peak, and combining it with B:

  | configuration (s2-big2, 3 workers) | max peak heap / worker | result |
  | --- | --- | --- |
  | baseline | 580.2 MiB | MATCH |
  | B (8 shards + externalize) | 478.1 MiB | MATCH |
  | `-Xmx512m` | 439.8 MiB | MATCH |
  | B + `-Xmx512m` | 338.6 MiB | MATCH |

  Together they cut the worst-worker peak ~42% while still matching vanilla. A `System.gc()`
  hint each BGP iteration did **not** reliably help (GC timing is noisy).

## Controller-shipped configurations (per-worker parse elimination)

The attribution above showed the floor is the per-worker parse/load of the full snapshot.
The controller already parses the snapshot, so it now Java-serializes the parsed
configurations and ships them in the `Start` control message; each worker deserializes them
and builds its snapshot with `S2Snapshot.fromConfigs` (`BatfishTestUtils.getBatfish(configs)`
installs them directly, no ANTLR and no on-disk snapshot). A worker that does not receive
the configs falls back to parsing from disk.

Effect (`s2-mega`, 16-router eBGP line, 4096 prefixes, 3 workers, all
`ribs=MATCH reachability=MATCH symbolic=MATCH answer=MATCH`):

| configuration | max peak heap / worker |
| --- | --- |
| per-worker parse (before) | 2090.8 MiB |
| controller-shipped configs | **436.4 MiB** |

That is ~79% lower, and the first-phase peak (`after building nodes`) drops from ~517 MiB
to ~168 MiB. With this floor removed, prefix sharding (B) no longer moves the *max* peak
at this size (it is within noise); B remains the lever when the BGP RIB itself dominates.
Verified on OrbStack too (1 and 3 Pods MATCH).

Set `-Ds2.noShipConfigs=true` on the controller to reproduce the per-worker-parse path.

### Largest snapshot (`networks/s2-giga`, 32768 prefixes)

`networks/s2-giga` is a 16-router eBGP line with 2048 loopbacks each (32768 origination
prefixes). Run with `-Xmx4g`, 3 workers, all MATCH:

| configuration | max peak heap / worker |
| --- | --- |
| per-worker parse (before) | 2513.1 MiB |
| controller-shipped configs | 2226.1 MiB |
| controller-shipped + B (8 shards) | 2363.7 MiB |

At 32768 prefixes the peak is now dominated by the *retained* dataplane/analysis (RIBs,
FIBs, forward/symbolic structures), so eliminating the parse transient only helps ~11%,
and prefix sharding (B) no longer helps (its serialization/GC overhead slightly exceeds
its RIB bound). Both mechanisms remain useful but their payoff depends on which component
dominates: parse transient (config shipping), BGP RIB (B), or retained dataplane (needs the
descriptor/owned-only work).

## Where the floor lives, across scales

Same workload shape (eBGP line, origination prefixes grown), 3 workers:

| network | prefixes | parse transient | config shipping | B prefix shards | worst-worker peak |
| --- | --- | --- | --- | --- | --- |
| `s2-big2` | 640 | dominant | 580.2 -> 164.7 MiB | helps | 164.7 MiB |
| `s2-mega` | 4096 | large | 2090.8 -> 436.4 MiB | marginal | 436.4 MiB |
| `s2-giga` | 32768 | small share | 2513.1 -> 2226.1 MiB | no gain | 2226.1 MiB |

Phase peaks at 32768 prefixes (worker 0, per-worker-parse path, cumulative
`MemoryPoolMXBean` peaks so the deltas are indicative, not live sizes):

* `after building nodes` (configs + node skeletons): 1174.2 MiB
* `after EGP iteration 1` (BGP RIBs): 1558.6 MiB (+~385)
* `after nextDataplane 1` (FIB construction): 2335.5 MiB (+~777)
* final peak: 2513.1 MiB

So as prefixes grow the bottleneck migrates from *parsing the snapshot* (fixed cost per
config, small at large prefix counts) to the *full dataplane every worker retains*. Each
worker currently builds and keeps, for **all** nodes (owned and shadow):

* a main RIB and a FIB per node — `S2BdpEngine.nextDataplane` calls `computeFib` on every
  node's virtual routers, and the returned `DataPlane` holds every node's tables;
* a global traceroute digest over every pair of loopbacks (`reachabilityDigest`), which needs
  all FIBs to run;
* a full `BDDReachabilityAnalysisFactory` over all configs — remote edges are then suppressed
  by `OwnedForwardingAnalysis`, but the factory still builds each remote config's ACL /
  transformation / source structures.

## Reduction roadmap

Ordered by expected payoff at scale. Every item should stay behind a default-off switch until
it is verified to match vanilla.

1. **Owned-only dataplane (FIB / main RIB).** Build and retain FIBs and main RIBs only for
   owned nodes. The symbolic analysis is already owned-scoped (`OwnedForwardingAnalysis` +
   pulled boundary edges) and `ribsOf` already filters to owned. Biggest lever: drops
   ~(1 - owned/total) of the retained FIB+RIB, i.e. ~2/3 at 16 nodes over 3 workers.
   *Not yet implemented — see the finding below.* Restricting a worker to owned FIBs was tried
   and fails in `ForwardingAnalysisImpl`: `computeArpTrueEdgeDestIp` /
   `computeArpTrueEdgeNextHopIp` look up `arpReplies.get(receiver.getHostname())` for every L3
   topology neighbor, and `arpReplies` / `routableIps` / `_vrfForwardingBehavior` are all
   computed from the FIBs of *all* configs. An owned→remote edge still needs the remote
   interface's ARP state (the source worker computes the transition it ships), so dropping
   remote FIBs NPEs. The real prerequisite is a forwarding-analysis change that supplies
   remote ARP/forwarding state without full remote FIBs — e.g. per-remote-interface
   "connected/local-only" stub FIBs — verified against the full computation. The other
   consumers to address alongside it: `reachabilityDigest` (a verification artifact — move to
   the controller or derive from the exact RIB match), `nextTopologyContext`'s global prune
   (`initBgpTopology` reachability — already disabled/unused when the check is off; plus
   VXLAN/IPsec/tunnel), and `IpsRoutedOutInterfacesFactory`.

2. **Scope the BDD factory to owned configs.** Add an optional `Set<String> localNodes` so
   `computeAclBDDs`, `computeTransformationRanges`, `BDDOutgoingOriginalFlowFilterManager`,
   `BDDSourceManager` and `LastHopOutgoingInterfaceManager` skip remote nodes. Cuts the
   analysis transient and retained structures that `OwnedForwardingAnalysis` hides but does not
   avoid.

3. **Release the full dataplane before the symbolic phase.** After extracting owned RIBs and
   owned FIBs, drop the `DataPlane` reference so the symbolic phase does not carry remote
   tables. Cheap; helps the symbolic peak (item 1 is what helps the dataplane-loop peak).

4. **Lightweight remote descriptor.** Replace each remote `Configuration` with a descriptor
   holding only what edge generation needs (interface name / addresses / L3 flags / OSPF
   settings, BGP peer config), dropping remote ACL / policy / route-map / community bodies.
   Payoff tracks policy size, not prefix count; it is also the prerequisite for not holding
   remote configs at all.

5. **Prefix-shard the dataplane (on-demand RIB / FIB).** Extend B from the control-plane BGP
   RIB to the main RIB / FIB: build and serialize one prefix shard at a time and page the rest
   (the paper's on-disk RIBs). Reduces the peak when the FIB dominates; a large shared-code
   change.

6. **Partitioning and worker count.** `NetworkPartitioner` is a balanced round-robin; a graph
   partitioner (the paper's expert / METIS) would cut boundary edges and balance per-worker RIB
   size. More workers is the blunt version (retained ~ 1/numWorkers). Orthogonal, cheap to try,
   but it scales out rather than down.

7. **Heap cap (`-Xmx`).** Already used; bounds transient headroom, not the retained floor.

## Known residual

On a **cyclic equal-cost** topology (e.g. a 6-node ring), the distributed BGP fixpoint
can occasionally pick a different valid route than single-machine Batfish where two
paths tie on AS-path length. That is BGP multiple-fixed-point / tie-break-order
nondeterminism, not a hang or a scheduling bug; the handoff's acyclic topologies are
unaffected. Matching Batfish's tie-breaking exactly in a distributed setting is
follow-up work.

## Commands

```sh
# unit tests
bazel test //projects/s2:s2_tests

# local multi-process (workers, optional network; defaults to s2-triangle)
bazel build //projects/s2:s2_main_deploy.jar
scripts/local-demo.sh 1
scripts/local-demo.sh 3
scripts/local-demo.sh 3 s2-line
scripts/local-demo.sh 3 s2-ospf
scripts/local-demo.sh 3 s2-ospf-bgp   # eBGP + OSPF
scripts/local-demo.sh 3 s2-redist     # OSPF<->BGP redistribution
# control-plane prefix sharding (B): N rounds + each round's BGP RIB externalized
JAVA_TOOL_OPTIONS=-Ds2.prefixShardExternalize=true S2_PREFIX_SHARDS=8 scripts/local-demo.sh 3 s2-big-bgp
JAVA_TOOL_OPTIONS=-Ds2.prefixShardExternalize=true S2_PREFIX_SHARDS=8 scripts/local-demo.sh 3 s2-big2
JAVA_TOOL_OPTIONS=-Ds2.prefixShardExternalize=true S2_PREFIX_SHARDS=8 scripts/local-demo.sh 3 s2-mega

# largest snapshot (32768 prefixes); bound the heap
JAVA_TOOL_OPTIONS=-Xmx4g scripts/local-demo.sh 3 s2-giga
# reproduce the per-worker snapshot parse (disable controller-shipped configs)
JAVA_TOOL_OPTIONS=-Xmx4g scripts/local-demo.sh 3 s2-giga -Ds2.noShipConfigs=true

# Kubernetes (OrbStack) — <workers> [network]
scripts/build-s2.sh --image
scripts/k8s-demo.sh 1
scripts/k8s-demo.sh 3
scripts/compare-answers.sh
scripts/k8s-demo.sh 3 s2-ospf
scripts/compare-answers.sh s2-ospf
scripts/k8s-demo.sh 3 s2-ospf-bgp
scripts/k8s-demo.sh 3 s2-redist
```

## Key files

* Runner: `projects/s2/src/main/java/org/batfish/dataplane/ibdp/S2Main.java`
* Backward worker: `S2ReachabilityWorker.java`
* Local forwarding view: `OwnedForwardingAnalysis.java`
* Boundary edges: `S2SidecarHandlers.java`, `S2Messages.java`, `S2StateHosts.java`
* OSPF delegation: `RemoteOspfEnqueueProvider.java`, `OspfRoutingProcess.EnqueueProvider`,
  `DistributedNode.installRemoteOspfProviders`, `VirtualRouter.initShadowOspfProcesses`
* BDD transport: `S2BddSidecar.java`
* BDD codec: `projects/bdd/src/main/java/net/sf/javabdd/BDDTransfer.java`
* Transition codec: `projects/batfish/.../transition/TransitionTransfer.java`
* Tests: `ScaledReachabilityTest`, `TransitionTransferTest`, `DistributedReachabilityTest`,
  `S2DistributedControlPlaneTest` (`testMultiHopLineMatchesVanilla`, `testOspfMatchesVanilla`,
  `testOspfBgpMatchesVanilla`), `S2RemoteSidecarTest` (`testRemoteOspfSidecar`,
  `testRemoteOspfBgpSidecar`)
