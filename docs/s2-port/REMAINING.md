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
| `-Ds2.descriptorShadows=true` | remote (shadow) nodes built from a lightweight descriptor (drops ACL/policy/route-map/community bodies) | off |
| `-Ds2.rpcStats=false` | disable the per-worker sidecar RPC/byte summary | on (prints) |
| `-Ds2.partition=<scheme>` | node→worker partitioner: RANDOM (default) / NAME_ORDERED / WEIGHTED_LPT_FM / GREEDY_REGION / METIS (real `gpmetis -ptype=rb -ufactor=1`; fallback if `gpmetis` absent) | RANDOM |
| `-Ds2.prefixSpacePositiveCacheOnly=true` | memoize only positive `PrefixSpace.containsPrefix` results (cuts the EGP transient; pure memoization) | off |
| `S2_PREFIX_SHARDS=N` | control-plane (BGP RIB) prefix sharding, N rounds | 1 (off) |
| `S2_PREFIX_SHARDS=auto` | auto shard count from the DPDG component weights (alias `-Ds2.prefixShardCount=auto`) | — |
| `-Ds2.prefixShardBudgetMiB=M` | per-shard live-BGP-RIB budget used by `auto` (smaller ⇒ more shards, cap 16) | 192 |
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

Descriptor mode (`-Ds2.descriptorShadows`, opt-in) further trims remote policy bodies. On the
ACL-heavy `networks/s2-acl` (3000-line ACLs), owned+descriptor drops the `after building nodes`
phase from 125.7 to 81.4 MiB and the max worker peak from 183.9 to 154.7 MiB; on `s2-mega` it is
modest (~10%/~6%) because its configs are mostly loopback interfaces, which must be retained.

## Consolidated task index (with dependencies)

Unified view across this file and `PARTITIONING-PLAN.md`. `←` depends on, `⇄` competes with,
(independent) otherwise. Detail sections A/B/C follow below.

### Base (do first)

| id | task | notes |
| --- | --- | --- |
| **P0** | measurement + testbed infrastructure | **Generator added**: `scripts/gen-topology.py` (`fattree --k`, `line --nodes`, `--originate`). **Metrics/baseline added**: `scripts/bench.sh "<workers>" "<networks>"` runs the matrix and emits a table (result / max peak MiB / controller MiB / engine s / wall s); phase output now carries elapsed time (`S2 phase ... t=..s`). **Finding**: FatTree eBGP with k>=4 is tie-unstable (multiple equal-cost BGP fixed points -> `ribs=DIFF` even at 1 worker; reachability/symbolic/answer MATCH), consistent with C1, so MATCH-verified partition evaluation must use tie-stable topologies (`line`, `fattree --k 2`) or a deterministic variant. `networks/s2-fat4` (20 switches) is a k=4 DCN testbed for throughput/memory. **Partition-quality metrics added**: `scripts/partition-metrics.py` (node weights, imbalance max/mean, weighted cut; default assignment ports `NetworkPartitioner`). **Boundary RPC counters added**: the sidecars print a per-worker `S2 rpc-stats` stderr summary at end of run. **CI matrix added**: `scripts/ci-matrix.sh` (opt-in; default/owned modes). **Ops notes added**: `docs/s2-port/OPS.md`. Still pending: weight calibration (O6). |
| **C-PFX** | prefix closure fix | **Done.** Aggregates: universe inclusion + co-sharding with the prefixes they cover (`networks/s2-agg`, `testPrefixShardingWithAggregateMatchesVanilla`). Redistribution: static and kernel route networks added (`networks/s2-static`, `testPrefixShardingWithRedistributedStaticMatchesVanilla`). External announcements: runner loads `external_bgp_announcements.json`, the controller ships them (`Start.externalAdverts`), the universe includes their networks, and they are re-staged each shard round (`BgpRoutingProcess.restageExternalAdvertisements`) (`networks/s2-external`, `testPrefixShardingWithExternalAnnouncementMatchesVanilla`, `shards=1..4`). Prerequisite for (B)/DPDG. |

### Partitioning (see `PARTITIONING-PLAN.md`)

| id | task | depends |
| --- | --- | --- |
| **P2** | node→worker partitioner plugin (RANDOM / NAME_ORDERED / WEIGHTED_LPT_FM / GREEDY_REGION / METIS); controller computes and distributes the assignment | **Done**: new `.../ibdp/partition/` package + `NodePartitioner`, union graph/`NodeWeights`, `-Ds2.partition` (default RANDOM unchanged), assignment shipped in `Start.assignment`; metrics/eval in `PARTITIONING-PLAN.md` §6.6 (pre-`gpmetis`) and §6.7 (real METIS). P0 (weights) |
| **P3** | PrefixDependencyGraph (closure + DPDG + weighted WCC-LPT) | **Done**: `PrefixDependencyGraph.java` + `PrefixSharder` rewrite (weighted WCC-LPT, degenerate fallback); `PrefixSharderTest` extended |
| **P-X** | shard-count selection | **Done**: `S2_PREFIX_SHARDS=auto` (`PrefixShardCountSelector`) picks N deterministically from the DPDG component weights under a per-shard budget (`-Ds2.prefixShardBudgetMiB`, default 192, cap 16); `scripts/shard-sweep.sh` + `M5-SCALE.md` record peak-vs-N |

### Memory

| id | task | section | depends / competes |
| --- | --- | --- | --- |
| **M1** | remote configuration descriptor | A1 | **Done (opt-in)**: `-Ds2.descriptorShadows=true`, `RemoteNodeDescriptor`; ACL-heavy testbed `networks/s2-acl` |
| **M2** | control-plane transient reduction | A2 | **Done**: root cause was an unbounded negative-result memo in `PrefixSpace.containsPrefix` (O(N^2) per router); opt-in positive-only cache (`-Ds2.prefixSpacePositiveCacheOnly`) cuts the s2-giga EGP peak ~40-48%; default off (stock unchanged) |
| **M3** | BDD factory owned scoping | A3 | **Done**: nullable `localNodes` on `BDDReachabilityAnalysisFactory`, wired from `S2Main` |
| **M4** | owned-mode hardening (Track / VXLAN / tunnel / BGP reachability) | A4 | **Done**: owned mode falls back to full configs when tracks/VNIs/tunnel/IPsec are present; BGP reachability disabled in owned mode |
| **M5** | dataplane prefix sharding / on-disk RIB+FIB | A5 | deferred |

### Correctness / generality — section B

- **C1** cyclic equal-cost BGP tie-break nondeterminism (known residual)
- **C2** EIGRP/IS-IS/RIP out of scope
- **C3** controller-side digest for owned mode — **Done (documented)**: the RIB ⇒ forwarding implication (and its descriptor-mode variant) is documented in `S2Main`.

### Operations / packaging — section C

- **O1** defaults; **O2** k8s resources / `-Xmx`; **O3** CI demo matrix; **O4** benchmark automation;
  **O5** METIS in the eval environment — **Done**: `gpmetis` (METIS 5.1.0) is installed in the eval
  environment and the real sweep is recorded in `PARTITIONING-PLAN.md` §6.7; `MetisPartitioner` keeps
  the pure-Java fallback and passes `-ptype=rb -ufactor=1`. **O6** partitioner weight calibration
  (← P0): **v1 shipped**
  (`NodeWeights`, documented additive feature sum; also added `--weights` to
  `scripts/partition-metrics.py`); fitting the coefficients to single-worker phase peaks is still
  pending; **O7** docs sync.

### Dependency graph

```
P0 ────┬─> P2  (evaluate H1 with owned on/off)
       ├─> O6
       └─> M2  (measure, then choose vs P3)
C-PFX ─> P3 ─> P-X
M1 (independent)             => lowers M3's value
M4 (owned opt-in done)       => defaulting owned (O1); makes P2's memory payoff general
M2 ⇄ P3                      (both target T_w)
```

### Recommended order

Done (merged): C-PFX, P3, P-X, P2, M1, M2, M3, M4, C3, P0 (except weight calibration O6).

1. **O6** weight calibration (v1 weights exist; calibrate against single-worker phase peaks)
2. **O1** defaults (decide whether to make owned / descriptor / positive-cache default) / **O3**/**O4** CI+bench automation
3. **M5** (deferred)
4. **C1** (FatTree tie-stability) if MATCH-verified DCN partition evaluation is required

(O5 METIS install is done — see the sweep in `PARTITIONING-PLAN.md` §6.7.)

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
