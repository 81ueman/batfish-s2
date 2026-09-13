# S2 port — remaining work

Living list of what is left, what it would buy, and how to verify it. Companion to
`M5-SCALE.md` (measurements) and `README.md` (milestones).

## Status snapshot

Working and verified end-to-end (local multi-process and OrbStack Kubernetes for 1 and 3
workers): distributed BGP + OSPF + OSPF<->BGP redistribution control plane, distributed symbolic
DPV, and the public-API answer check. `ribs=MATCH reachability=MATCH symbolic=MATCH answer=MATCH`.

Knobs added for scale work:

| knob | effect | default |
| --- | --- | --- |
| (none) | controller ships parsed configs; workers do not re-parse | on |
| `-Ds2.noShipConfigs=true` | worker parses the snapshot itself (reproduce the old floor) | off |
| `-Ds2.ownedDataplane=true` | worker keeps full RIBs/FIBs only for owned nodes (remote get stub FIBs) | off |
| `S2_PREFIX_SHARDS=N` | control-plane (BGP RIB) prefix sharding, N rounds | 1 (off) |
| `-Ds2.prefixShardExternalize=true` | serialize each shard's BGP RIB between rounds | off |

Headline peak-heap per worker (3 workers, `-Xmx4g`):

| network | prefixes | per-worker parse | config shipping | shipping + owned |
| --- | --- | --- | --- | --- |
| `s2-mega` | 4096 | 2090.8 MiB | 436.4 MiB | **301.1 MiB** |
| `s2-giga` | 32768 | 2513.1 MiB | 2226.1 MiB | **1938.1 MiB** |

At 32768 prefixes the bottleneck has moved off parsing and off the dataplane FIBs: the owned run's
phase peaks are `building nodes` 413 MiB, `EGP iteration 1` 1595 MiB, `nextDataplane 1` 1740 MiB.
The remaining cost is the held configurations and the BGP control-plane transient. The FIB axis is
considered done for now.

## A. Memory / scale (ranked by expected payoff)

### A1. Remote configuration descriptor
* **What:** replace each remote `Configuration` with a descriptor holding only what edge
  generation needs (interface name / addresses / L3 flags / OSPF settings, BGP peer config),
  dropping remote ACL / policy / route-map / community bodies.
* **Why:** workers currently hold *every* config in full; `s2-giga` spends 413 MiB at
  `after building nodes`. Payoff tracks policy size, so it is largest on ACL/policy-heavy
  networks and modest on the eBGP line networks used so far.
* **How:** add a serializable `RemoteNodeDescriptor` built by the controller and shipped in
  `S2ControlMessages.Start` (like the configs today); workers materialize a lightweight
  `Configuration`/node view for shadows. Enumerate the exact factory/dataplane fields first
  (`BDDReachabilityAnalysisFactory`, `ForwardingAnalysisImpl`, topology providers).
* **Risk:** medium-high. The shadow must satisfy interface/topology lookups and the stub-FIB
  path; remote ACL bodies must never be needed for local edges (they are suppressed by
  `OwnedForwardingAnalysis`, but the factory still iterates all configs today — see A3).
* **Verify:** add an ACL-heavy snapshot; assert `MATCH` and measure the drop in the
  `after building nodes` phase (phase hook already exists: `S2BdpEngine.reportPhase`).

### A2. Control-plane transient reduction
* **What:** shrink the BGP delta/queue transient during the EGP fixpoint (the `EGP iteration 1`
  peak of +877..1182 MiB at `s2-giga`).
* **Why:** this is now the single largest jump in the largest run.
* **How (candidates):** reduce `RouteAdvertisement` copying/sanitization; bound/drain delta
  queues more aggressively; revisit the `Schedule` (currently forced to `ALL` for cross-worker
  determinism) to lower per-step fan-out; tune `-Xmx`/GC. Prefix sharding (B) was tried and does
  not help at this scale (`owned + B(8)` = 2324 MiB > owned 1938 MiB) because each worker already
  owns few routers and the shard externalization adds its own transient.
* **Risk:** medium. Anything touching iteration order/scheduling must preserve global
  convergence and determinism (the barrier hooks in `S2BdpEngine`).
* **Verify:** `s2-giga` MATCH plus phase-peak comparison; unit suite for convergence.

### A3. Scope the BDD factory to owned configs (`#2`)
* **What:** add an optional `Set<String> localNodes` to `BDDReachabilityAnalysisFactory` so
  `computeAclBDDs`, `computeTransformationRanges`, `BDDOutgoingOriginalFlowFilterManager`,
  `BDDSourceManager`, `LastHopOutgoingInterfaceManager`, and the `_configs.keySet()` iterations in
  `generateEdges` / `bddReachabilityAnalysis` skip remote nodes.
* **Why:** `OwnedForwardingAnalysis` hides remote forwarding edges but the factory still builds
  per-config ACL/source/transformation structures for remote nodes. Real at large ACL-heavy scale;
  small on the current line networks.
* **How:** thread `localNodes` (nullable, default all = stock) through the constructor and the
  final-node / disposition-edge generation; keep remote nodes reachable as edge *targets*.
* **Risk:** medium; core class used by all of Batfish. Must keep stock behavior identical when
  `localNodes == null`.
* **Verify:** existing reachability tests; `symbolic=MATCH` on the demo matrix.

### A4. Harden the owned-only dataplane
* **What:** extend `-Ds2.ownedDataplane` to the cases it does not cover today:
  `TrackReachability` (needs remote FIBs in `nextTrackReachabilityResults`), VXLAN / IPsec /
  tunnel pruning (uses `TracerouteEngineImpl` over the stub dataplane), and the dataplane-level
  BGP session reachability check currently disabled.
* **Why:** needed before owned mode can become a default for general networks.
* **How:** provide the minimal remote state each consumer needs:
  * tracks: evaluate on the owner and exchange, or reconstruct remote FIBs only when tracks exist;
  * prune: disable/skip when the corresponding topologies are empty, otherwise distribute;
  * BGP reachability: exchange the minimal remote forwarding facts, or keep it off and document.
* **Risk:** medium. Keep behind the switch until each case matches.
* **Verify:** add snapshots with a `TrackReachability`, a VXLAN/IPsec case, and a BGP session that
  requires a reachability check.

### A5. Dataplane prefix sharding / on-disk RIB+FIB
* **What:** extend prefix sharding from the control-plane BGP RIB to the main RIB/FIB: build and
  serialize one prefix shard at a time and page the rest (the paper's on-disk RIBs).
* **Why:** reduces the peak only when the FIB dominates, which it no longer does after A4's owned
  mode on the current networks. Deferred.
* **Risk:** high (large shared-code change; on-demand FIB lookup).

### A6. Partitioning and worker count
* **What:** replace `NetworkPartitioner`'s balanced round-robin with a graph partitioner (the
  paper's expert scheme / METIS); optionally expose worker count.
* **Why:** cuts cross-worker boundary edges (symbolic) and balances the per-worker control-plane
  load. Note it does **not** reduce the per-worker floor: every worker still holds all configs and
  (unless owned mode is on) all FIBs. Orthogonal, cheap to try.

## B. Correctness / generality

* **B1. Cyclic equal-cost BGP tie-break nondeterminism.** On a cyclic equal-cost topology the
  distributed BGP fixpoint can pick a different (valid) route than single-machine Batfish. Known
  residual, documented in `M5-SCALE.md`; handoff topologies are unaffected.
* **B2. EIGRP / IS-IS / RIP** are intentionally not distributed and are rejected for `>1` worker.
* **B3. Owned-mode non-covered cases** — see A4.
* **B4. Controller-side digest for owned mode.** Today, with `-Ds2.ownedDataplane`, workers return
  an empty digest and the controller *implies* forwarding equality from the exact RIB match. A
  stronger check would reconstruct a dataplane from the union of the workers' delivered RIBs on
  the controller and run the traceroute digest there, so the forwarding result is checked directly.

## C. Operations / packaging

* **C1. Defaults.** Config shipping is on. Decide whether to make owned mode (and maybe prefix
  sharding) default after A4 lands; otherwise keep them opt-in and documented.
* **C2. Kubernetes resources.** Set worker/controller memory requests and limits from the measured
  peaks (e.g. `s2-giga` at `-Xmx4g`), and pick a default `-Xmx`. Rebuild the image
  (`scripts/build-s2.sh --image`) after any runner change and re-run `scripts/k8s-demo.sh` +
  `scripts/compare-answers.sh`.
* **C3. CI matrix.** Run the full local demo matrix on every change:
  `s2-triangle`, `s2-line`, `s2-ospf`, `s2-ospf-bgp`, `s2-redist`, `s2-mega`, `s2-giga` (the last
  two with `-Xmx4g`), each in both default and `-Ds2.ownedDataplane=true` modes.
* **C4. Benchmark/reporting.** The phase attribution (`S2BdpEngine.reportPhase`) and per-worker
  peak reporting already exist; consider a script that runs the size ladder and emits the table in
  `M5-SCALE.md` automatically.

## Reference: where the seams are

| Area | File |
| --- | --- |
| Runner / controller / worker / config shipping / digest | `projects/s2/src/main/java/org/batfish/dataplane/ibdp/S2Main.java` |
| Owned mode, stub FIBs, schedule/barriers | `projects/s2/src/main/java/org/batfish/dataplane/ibdp/S2BdpEngine.java` |
| Final-dataplane node hook | `IncrementalBdpEngine.dataPlaneNodes` (`projects/batfish/.../ibdp/IncrementalBdpEngine.java`) |
| Stub FIB init | `VirtualRouter.initStubFib` (`projects/batfish/.../ibdp/VirtualRouter.java`) |
| Factory per-config scope | `projects/batfish/src/main/java/org/batfish/bddreachability/BDDReachabilityAnalysisFactory.java` |
| Forwarding analysis (ARP / unownedArpIps) | `projects/common/src/main/java/org/batfish/datamodel/ForwardingAnalysisImpl.java` |
| Phase peaks / measurements | `docs/s2-port/M5-SCALE.md` |
