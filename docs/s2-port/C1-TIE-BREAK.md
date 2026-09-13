# C1 — FatTree eBGP equal-cost tie nondeterminism

Status: **resolved by default.** The S2 engine now uses vanilla's deterministic
`NODE_COLORED` schedule, so `networks/s2-fat4` matches vanilla by default at 1 and 3
workers; `-Ds2.egpSchedule=ALL` remains the escape hatch. Cross-worker coloring
consistency is guaranteed (full node set + config-derived topology) and enforced by a
cluster-wide fingerprint check that falls back to `ALL` instead of deadlocking.

## TL;DR

`networks/s2-fat4` (20-switch FatTree k=4, a distinct eBGP AS per switch) has many pairs of
genuinely equal-cost BGP routes. Two order-sensitive rules in `BgpRib`/`RibTree` decide them:

1. **`comparePreference` rejects equal-length, different-AS-path routes as "less preferable".**
   After weight, local preference, AS-path length, origin type, eBGP-over-iBGP, MED, IGP cost, and
   cluster list all tie, `compareRouteAsPath` compares the paths under `EXACT_PATH` (the default
   multipath-equivalence mode). Two different neighbors always advertise different first ASes, so the
   paths are unequal and it returns `< 0`; on a non-multipath RIB (`max-paths 1`, the default) that
   `-1` is returned directly. `RibTree.mergeRoute` treats `< 0` as "less preferred" and keeps the
   route already in the RIB — so **the first-arrived route wins and the later equal-cost one is
   rejected**.
2. **`bestPathComparator`/`ARRIVAL_ORDER`.** When the AS paths *are* exactly equal (or the RIB is
   multipath), selection falls through to the tie-breaker; the Cisco default is
   `BgpTieBreaker.ARRIVAL_ORDER`, whose `_logicalArrivalTime` is again assigned in merge order.

Either way the winning route is the one merged **first**. (Forcing the alternative `ROUTER_ID`
tie-breaker does *not* remove the nondeterminism — see evidence — which confirms rule 1 is what fires
on this topology.)

That makes the fixed point a function of the message-passing order:

* **Vanilla Batfish** uses `Schedule.NODE_COLORED`. Color classes are independent sets of the BGP
  topology, processed one class at a time, so a node always reads a neighbor's advertisement in a
  fixed order and never concurrently. Deterministic (verified: two runs byte-identical).
* **The S2 engine** historically forced `Schedule.ALL`: one step, every node pulling in the same
  concurrent round (`S2BdpEngine.initialSchedule`). On a cyclic equal-cost topology this is
  **run-to-run nondeterministic even in the stock engine** (verified: two stock-`ALL` runs differ),
  and it can select a different — still valid — equal-cost fixed point than vanilla.

So the C1 residual was not just "ALL is a different deterministic schedule"; **ALL itself races on
the route-arrival order**, which is why `ribs=DIFF` appeared even at 1 worker.

Fix (now the default): give the S2 engine the same deterministic schedule vanilla uses —
`Schedule.NODE_COLORED`, returned by `S2BdpEngine.initialSchedule()`. With it, the S2 engine
matches vanilla at 1 and 3 workers. `-Ds2.egpSchedule=ALL` restores the historical single-round
schedule. The C1 concern that motivated `ALL` (different workers coloring different topologies)
is handled as described in "The fix" below.

## Topology and the disagreeing routes

`networks/s2-fat4`: 4 core (`sw0..sw3`, AS65000-65003), 8 aggregation (`sw4..sw11`), 8 edge
(`sw12..sw19`), every link /30, every switch eBGP with its own AS and originating one or more
`/32` loopbacks. There is no IGP, so all loopback routes reach a switch over multiple equal-AS-path-
length eBGP paths through the Clos.

Concrete disagreement (from the 1-worker local demo, `vanilla` vs the S2 engine). On `sw2`, prefix
`10.0.4.1/32` (core `sw3`'s loopback), the two candidates are:

| attribute | vanilla picked | S2 (`ALL`) picked |
| --- | --- | --- |
| `_asPath` | `[65011, 65003]` | `[65005, 65003]` |
| `_nextHop` | `10.1.30.1` | `10.1.18.1` |
| `_receivedFrom` | `10.1.30.1` (`sw11`) | `10.1.18.1` (`sw5`) |
| `_originatorIp` | `10.0.12.1` (`sw11`) | `10.0.6.1` (`sw5`) |
| `_localPreference` | 100 | 100 |
| `_med` | 0 | 0 |
| `_weight` | 0 | 0 |
| `_originType` / `_protocol` | EGP / BGP | EGP / BGP |

`sw2` is directly connected to `sw5` and `sw11`; both advertise `sw3`'s loopback with an AS path
of length 2. The routes tie on weight, local preference, aggregate preference, AS-path length,
origin type, eBGP-over-iBGP, MED, IGP cost to next hop, and cluster-list length. The next check,
`compareRouteAsPath`, sees two unequal AS paths under `EXACT_PATH` and returns `< 0`:

```java
// BgpRib.comparePreference (tail)
result = compareRouteAsPath(lhs, rhs); // EXACT_PATH: paths differ -> -1
if (result != 0 || isMultipath()) {
  return result;                       // non-multipath RIB: return -1
}
return bestPathComparator(lhs, rhs);
```

`RibTree.mergeRoute` interprets that `-1` as "less preferable than the route already present" and
drops it, so the route that arrived first is retained. That is why vanilla can keep the *worse*
`originatorIp` (`10.0.12.1` rather than `10.0.6.1`): the later route never gets to the
`originatorIp`/`ReceivedFrom` tie-breakers. (`_process.getTieBreaker()` is unset for these configs —
Cisco IOS only sets `ROUTER_ID` for `bgp bestpath compare-routerid`, which is absent — so the
`bestPathComparator` reached when AS paths *are* equal uses `ARRIVAL_ORDER`, i.e. arrival order
again.)

There are dozens of such pairs; a single 1-worker run reports ~66 differing routes across the 20
switches (the exact set varies run to run because `ALL` is nondeterministic — see below).

## Why the schedules differ

* **Vanilla (`NODE_COLORED`).** `NodeColoredSchedule` colors the BGP/OSPF adjacency graph; nodes in
  one color class have no mutual adjacency. `runEgpFixpoint` executes one color class per schedule
  step and only then calls `endOfEgpInnerRound` (which publishes each node's best-path delta).
  A node in class *c+1* therefore reads its neighbors' deltas in a fixed, sequenced order, and no
  two adjacent nodes ever run concurrently. The result is deterministic.

* **S2 (`ALL`).** `MaxParallelSchedule` puts every node in a single step. All nodes run
  `bgpIteration` in one `parallelStream` phase: each calls `startOfInnerRound` (snapshot) and then
  `processBgpV4UnicastMessages` (pull + merge), while its neighbors are doing the same. Two effects:
  1. Equal-cost advertisements that NODE_COLORED would deliver in different steps are now merged in
     *the same* round, so which one gets the lower arrival clock is decided by the concurrent
     execution order rather than by the schedule.
  2. The engine's own VR order is randomized (`StreamUtil.toListInRandomOrder(...,
     ThreadLocalRandom)` in `IncrementalBdpEngine.computeDataPlane`), and the per-node `vrs` order
     affects the parallel phase interleaving. Nothing in `ALL` pins the arrival order back down.

  `S2BdpEngine` adds global barriers (`synchronizeWorkers`) at phase boundaries, but a barrier is
  not a per-node sequencing: it does not stop two adjacent nodes from running in the same step. It
  makes workers agree on *when* a round ends, not on the *order inside* a round.

The M5 note ("`ALL` gives one step, identical across workers") is true but not sufficient: step
*count* is deterministic while route *arrival order* within a step is not.

## Evidence

### 1. End to end (multi-process runner)

```sh
bazel build //projects/s2:s2_main_deploy.jar
S2_BASE_PORT=19300 scripts/local-demo.sh 1 s2-fat4
```

Observed (default S2 engine, i.e. `Schedule.ALL`):

```
S2 DIFF (1 workers): ribs=DIFF reachability=MATCH symbolic=MATCH answer=MATCH, ...
```

`ribs=DIFF` at **1 worker**; the reachability/symbolic/answer checks still match. The controller
prints the full vanilla and distributed RIB maps, from which the `sw2` `10.0.4.1/32` pair above was
extracted.

### 2. Isolated, reproducible (`S2FatTreeTieBreakTest`)

```sh
bazel test //projects/s2:s2_tests \
  --test_filter=org.batfish.dataplane.ibdp.S2FatTreeTieBreakTest --test_output=all
```

The test runs, each on a **freshly parsed snapshot** (so no cross-run config mutation):
vanilla (public API), the stock engine forced to `ALL`, the stock engine forced to `NODE_COLORED`
(twice each for determinism), and the unmodified S2 engine (whose default is now `NODE_COLORED`)
twice at 1 worker and once at 3 workers. Observed:

```
vanilla == stock(NODE_COLORED):    true
stock(NODE_COLORED) run1 == run2:  true
vanilla == stock(ALL):             false
stock(ALL) run1 == run2:           false     <-- ALL is nondeterministic by itself
s2(1w, default) run1 == run2:      true      <-- default is NODE_COLORED
vanilla == s2(1w, default):        true      <-- the fix
vanilla == s2(3w, default):        true      <-- the fix, distributed
stock(ALL, ROUTER_ID) run1==run2:  false     <-- the other tie-breaker does not pin it down
vanilla == stock(ALL, ROUTER_ID):  false
  (hundreds of differing routes; the exact set varies run to run)
```

This separates the facts:
* schedule choice causes the fixed point to differ (`vanilla != stock(ALL)`);
* `ALL` is not merely "different" — it is not reproducible run to run, which is why even a
  1-worker S2 run under `ALL` cannot be compared against vanilla;
* forcing the alternative `ROUTER_ID` tie-breaker (`bestPathComparator` prefers the lower
  `originatorIp`) still leaves `ALL` nondeterministic. `ROUTER_ID` is only consulted after the
  `compareRouteAsPath` check, which preempts it for equal-length, different-AS-path routes; so the
  order sensitivity is the first-arrival rule, not the choice of `bestPathComparator`.

The test asserts the stable facts (`vanilla == stock(NODE_COLORED)`, the two `NODE_COLORED` runs are
equal, `vanilla != stock(ALL)`) and the fix (`vanilla == s2(1w/3w, default)`); the `stock(ALL)`
nondeterminism lines are printed, not asserted, to avoid a flaky test.

### 3. The fix end to end (multi-process runner)

By default (no property), `s2-fat4` now matches vanilla:

```sh
JAVA_TOOL_OPTIONS=-Xmx4g S2_BASE_PORT=20100 scripts/local-demo.sh 1 s2-fat4
JAVA_TOOL_OPTIONS=-Xmx4g S2_BASE_PORT=20110 scripts/local-demo.sh 3 s2-fat4
```

Observed:

```
S2 MATCH (1 workers): ribs=MATCH reachability=MATCH symbolic=MATCH answer=MATCH
S2 MATCH (3 workers): ribs=MATCH reachability=MATCH symbolic=MATCH answer=MATCH
```

The escape hatch still restores the old behavior (and the old nondeterminism):

```sh
JAVA_TOOL_OPTIONS="-Xmx4g -Ds2.egpSchedule=ALL" S2_BASE_PORT=20500 scripts/local-demo.sh 1 s2-fat4
-> S2 DIFF (1 workers): ribs=DIFF reachability=DIFF symbolic=MATCH answer=MATCH
```

No regression on the tie-stable demos (3 workers unless noted), all with the default schedule:

```
s2-triangle, s2-line, s2-ospf, s2-ospf-bgp, s2-redist, s2-big2, s2-mega  -> MATCH
s2-triangle, s2-line (6 workers)                                          -> MATCH
```

## The fix

Two parts, both in the engine (no controller/protocol change):

1. **Default to vanilla's schedule.** `S2BdpEngine.initialSchedule()` now returns
   `Schedule.NODE_COLORED`. `IncrementalBdpEngine.runEgpFixpoint` still honors the
   override property, so `-Ds2.egpSchedule=ALL` is the escape hatch:

   ```java
   String scheduleOverride = System.getProperty("s2.egpSchedule");
   Schedule currentSchedule =
       scheduleOverride == null ? initialSchedule() : Schedule.valueOf(scheduleOverride);
   ```

2. **Make cross-worker coloring safe by construction.** Right after the schedule is
   computed, `runEgpFixpoint` calls a new engine hook
   `reconcileEgpSchedule(Schedule, List<Map<String, Node>>)` (default: return the
   schedule unchanged). `S2BdpEngine` overrides it to fingerprint the ordered color
   classes and exchange both the fingerprint and the worker count with the coordinator
   (`S2Coordinator.sumAll`). If `sum(fingerprint) == fingerprint * workers` on every
   worker, they all colored identically and the schedule is kept; otherwise every worker
   sees the disagreement and falls back cluster-wide to the single-step `ALL`, whose step
   count is worker-independent. A coloring mismatch therefore cannot desynchronize the
   per-step barriers and hang the run.

Why the coloring is in fact identical across workers (so the fallback should never fire):

* Every worker is built over the **full node set**: `S2Main` constructs a `DistributedNode`
  (real or shadow) for every host in `snap.configs`, and
  `IncrementalBdpEngine.computeDataPlane` builds its `nodes` map from all `configurations`.
* `NodeColoredSchedule` colors the `BGP` + `OSPF` topology of the `TopologyContext`
  computed in `nextTopologyContext` from that same full config set. In descriptor-shadow
  mode the reduced remote configs still carry interfaces/addresses and the BGP and OSPF
  processes the topology read needs (`RemoteNodeDescriptorTest#testDescriptorKeepsTopology`
  covers L3/OSPF/IP ownership), and BGP session establishment runs with reachability
  checks off, so the graph is the same on every worker.
* The coloring algorithm defaults to deterministic `SATURATION`; given the same graph the
  color classes (and their order) are identical.

`S2FatTreeTieBreakTest` covers the mechanics:

* `testDefaultScheduleReproducesVanilla` asserts `vanilla == s2(1w)` and `vanilla == s2(3w)`
  with the unmodified engine (the 3-worker run also exercises the new exchange barriers and
  would hang on a mismatch).
* `testAllScheduleEscapeHatch` sets `-Ds2.egpSchedule=ALL` and asserts the result differs
  from vanilla.
* `testScheduleIsTheCause` keeps the stock-engine evidence (`vanilla == stock(NODE_COLORED)`,
  `vanilla != stock(ALL)`).

`bazel test //projects/s2:s2_tests` passes (78 tests).

## Recommended path to full determinism

1. **Short term (done).** `Schedule.NODE_COLORED` is the S2 default; `ALL` is the opt-out.
2. **Cluster-wide consistency (done).** `reconcileEgpSchedule` fingerprints the schedule and
   falls back cluster-wide to `ALL` on disagreement, so a difference cannot deadlock.
3. **Robust version (future).** Have the controller compute the coloring once from the full
   snapshot and ship the color classes in `Start` (like the node→worker `assignment`), so the
   schedule cannot depend on per-worker topology at all and the fallback is unnecessary. This
   is still the recommended long-term design and is the only way to keep `NODE_COLORED` if a
   future feature ever makes a worker's derived topology diverge.
4. **Do not try to fix this with the tie-breaker.** Forcing `ROUTER_ID`/`ReceivedFrom` does not
   even make `ALL` deterministic here (the equal-length-AS-path check in `comparePreference`
   runs first), and changing it would alter Batfish's vendor-faithful behavior without
   reproducing vanilla's choice. Schedule sequencing is the correct lever.

## Residual / not addressed

* `NODE_COLORED` uses more schedule steps than `ALL`, so the EGP transient may grow (relevant
  to the A2 memory work). On `s2-fat4` the added cost is small; no memory numbers were collected
  here.
* The fingerprint guard falls back to `ALL` if a future topology ever makes workers color
  differently; that fallback would reintroduce run-to-run nondeterminism on a cyclic
  equal-cost topology. No such divergence has been observed (the full-node-set/topology
  invariant holds today), and the controller-shipped coloring (#3 above) is the robust
  long-term fix.
* Other vendors' tie-breakers (`ROUTER_ID` for `bgp bestpath compare-routerid`, etc.) do not
  change the conclusion: on this topology the equal-length-AS-path `comparePreference` check
  decides before any `bestPathComparator` tie-breaker is reached.

## Key files

* Schedule choice: `projects/batfish/.../dataplane/ibdp/IncrementalBdpEngine.java`
  (`runEgpFixpoint`, `reconcileEgpSchedule`), `.../schedule/NodeColoredSchedule.java`,
  `.../S2BdpEngine.java` (`initialSchedule`, `reconcileEgpSchedule`, `scheduleFingerprint`)
* Tie-break: `projects/batfish/.../dataplane/rib/BgpRib.java` (`bestPathComparator`,
  `mergeRouteGetDelta`/`_logicalArrivalTime`), `.../BgpRoutingProcess.java` (default
  `ARRIVAL_ORDER`)
* Evidence: `projects/s2/src/test/java/org/batfish/dataplane/ibdp/S2FatTreeTieBreakTest.java`,
  testrig `projects/s2/src/test/resources/org/batfish/dataplane/testrigs/s2-fat4`,
  `networks/s2-fat4`
