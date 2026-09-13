# C1 — FatTree eBGP equal-cost tie nondeterminism

Status: **root cause identified with evidence; a small, default-off fix is implemented and
verified** on `networks/s2-fat4` at 1 and 3 workers.

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
* **The S2 engine** forces `Schedule.ALL`: one step, every node pulling in the same concurrent
  round (`S2BdpEngine.initialSchedule`). On a cyclic equal-cost topology this is **run-to-run
  nondeterministic even in the stock engine** (verified: two stock-`ALL` runs differ), and it can
  select a different — still valid — equal-cost fixed point than vanilla.

So the C1 residual is not just "ALL is a different deterministic schedule"; **ALL itself races on
the route-arrival order**, which is why `ribs=DIFF` appears even at 1 worker.

Fix: give the S2 engine the same deterministic schedule vanilla uses —
`-Ds2.egpSchedule=NODE_COLORED` (default off). With it, the S2 engine matches vanilla at 1 and 3
workers.

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
(twice each for determinism), the S2 engine (twice, 1 worker), and an S2 engine variant using
`NODE_COLORED` at 1 and 3 workers. Observed:

```
vanilla == stock(NODE_COLORED):    true
stock(NODE_COLORED) run1 == run2:  true
vanilla == stock(ALL):             false
stock(ALL) run1 == run2:           false     <-- ALL is nondeterministic by itself
s2(1w, ALL) run1 == run2:          false
vanilla == s2(1w, NODE_COLORED):   true      <-- the fix
vanilla == s2(3w, NODE_COLORED):   true      <-- the fix, distributed
vanilla == s2(3w, ALL):            false
stock(ALL, ROUTER_ID) run1==run2:  false     <-- the other tie-breaker does not pin it down
vanilla == stock(ALL, ROUTER_ID):  false
  (66 differing routes)
```

This separates the facts:
* schedule choice causes the fixed point to differ (`vanilla != stock(ALL)`);
* `ALL` is not merely "different" — it is not reproducible run to run, which is why even a
  1-worker S2 run cannot be compared against vanilla;
* forcing the alternative `ROUTER_ID` tie-breaker (`bestPathComparator` prefers the lower
  `originatorIp`) still leaves `ALL` nondeterministic. `ROUTER_ID` is only consulted after the
  `compareRouteAsPath` check, which preempts it for equal-length, different-AS-path routes; so the
  order sensitivity is the first-arrival rule, not the choice of `bestPathComparator`.

The test asserts the stable facts (`vanilla == stock(NODE_COLORED)`, the two `NODE_COLORED` runs are
equal, `vanilla != stock(ALL)`) and the fix (`vanilla == s2(1w/3w, NODE_COLORED)`); the
nondeterminism lines are printed, not asserted, to avoid a flaky test.

### 3. The fix end to end (multi-process runner)

```sh
JAVA_TOOL_OPTIONS=-Ds2.egpSchedule=NODE_COLORED S2_BASE_PORT=19350 scripts/local-demo.sh 1 s2-fat4
JAVA_TOOL_OPTIONS=-Ds2.egpSchedule=NODE_COLORED S2_BASE_PORT=19370 scripts/local-demo.sh 3 s2-fat4
```

Observed:

```
S2 MATCH (1 workers): ribs=MATCH reachability=MATCH symbolic=MATCH answer=MATCH
S2 MATCH (3 workers): ribs=MATCH reachability=MATCH symbolic=MATCH answer=MATCH
```

No regression on the existing demos with the flag off (default behavior unchanged by the added
property read):

```
S2_BASE_PORT=19390 scripts/local-demo.sh 3 s2-line   -> S2 MATCH (3 workers): ribs=MATCH ...
S2_BASE_PORT=19410 scripts/local-demo.sh 1 s2-fat4   -> S2 DIFF  (1 workers): ribs=DIFF ...
```

The second run confirms the residual is still there by default; the first confirms the engine change
did not disturb a tie-stable snapshot.

## The fix

`IncrementalBdpEngine.runEgpFixpoint` now honors an optional system property:

```java
String scheduleOverride = System.getProperty("s2.egpSchedule");
Schedule currentSchedule =
    scheduleOverride == null ? initialSchedule() : Schedule.valueOf(scheduleOverride);
```

* Default `null` ⇒ `initialSchedule()`, so stock Batfish and the current S2 behavior are unchanged.
* `-Ds2.egpSchedule=NODE_COLORED` makes the S2 engine use vanilla's deterministic schedule.

Why this is safe for the S2 engine: every worker builds the schedule from the same full node map
(all real + shadow nodes) and the same topology context, so `NodeColoredSchedule` produces the same
color classes and therefore the same number of per-step barriers on every worker. (This is the
opposite direction from the M5 #4 decision to force `ALL`; that decision was motivated by the risk
of *differing* per-worker colorings. With the current runner the node set and topology are global,
and the test validates the barriers line up at 3 workers. If a future topology made a worker's
BGP topology differ, the flag should be turned off or the coloring shipped by the controller — see
below.)

The test `S2FatTreeTieBreakTest#testEgpScheduleOverrideReproducesVanilla` sets the property around
the unmodified S2 engine and asserts `vanilla == s2(1w)` and `vanilla == s2(3w)`.

`bazel test //projects/s2:s2_tests` passes with the change (all existing tests plus the two new
ones). Because the flag is default off, the existing demo matrix is unaffected; the demos can be
re-run with `-Ds2.egpSchedule=NODE_COLORED` to opt in.

## Recommended path to full determinism

1. **Short term (done).** `-Ds2.egpSchedule=NODE_COLORED`, opt-in, verified on the FatTree.
2. **Default it for cyclic equal-cost topologies.** Once the "colors are identical cluster-wide"
   invariant is checked (assert all workers agree on `scheduleSteps.size()` and, ideally, on the
   class contents for each step), make the distributed engine prefer `NODE_COLORED` by default and
   keep `ALL` for acyclic/tie-free snapshots where it is faster.
3. **Robust version.** Have the controller compute the coloring once from the full snapshot and
   ship the color assignment in `Start` (like the node→worker `assignment`), so it cannot depend on
   per-worker topology at all. This removes the M5 #4 risk entirely and is the recommended
   long-term design.
4. **Do not try to fix this with the tie-breaker.** Forcing `ROUTER_ID`/`ReceivedFrom` does not even
   make `ALL` deterministic here (the equal-length-AS-path check in `comparePreference` runs first),
   and changing it would alter Batfish's vendor-faithful behavior without reproducing vanilla's
   choice. Schedule sequencing is the correct lever.

## Residual / not addressed

* The flag helps only when all workers see the same node set and topology. Enforcing that invariant
  (assertion or controller-shipped coloring) is future work; until then the flag is opt-in.
* `NODE_COLORED` uses more schedule steps than `ALL`, so the EGP transient may grow (relevant to the
  A2 memory work). On `s2-fat4` the added cost is small; no memory numbers were collected here.
* Other vendors' tie-breakers (`ROUTER_ID` for `bgp bestpath compare-routerid`, etc.) do not change
  the conclusion: on this topology the equal-length-AS-path `comparePreference` check decides before
  any `bestPathComparator` tie-breaker is reached.

## Key files

* Schedule choice: `projects/batfish/.../dataplane/ibdp/IncrementalBdpEngine.java#runEgpFixpoint`
  (`s2.egpSchedule`), `.../schedule/NodeColoredSchedule.java`, `.../S2BdpEngine.java#initialSchedule`
* Tie-break: `projects/batfish/.../dataplane/rib/BgpRib.java` (`bestPathComparator`,
  `mergeRouteGetDelta`/`_logicalArrivalTime`), `.../BgpRoutingProcess.java` (default
  `ARRIVAL_ORDER`)
* Evidence: `projects/s2/src/test/java/org/batfish/dataplane/ibdp/S2FatTreeTieBreakTest.java`,
  testrig `projects/s2/src/test/resources/org/batfish/dataplane/testrigs/s2-fat4`,
  `networks/s2-fat4`
