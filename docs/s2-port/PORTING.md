# S2 port — remaining work (checkpoint)

`bazel build //projects/distributed:distributed` → 76 errors across ~17 files.

## Already applied to upstream Batfish (core)

* `final` → non-final: `Node`, `VirtualRouter`, `BgpRoutingProcess`,
  `OspfRoutingProcess`, `IncrementalBdpEngine`, `IncrementalDataPlanePlugin`,
  `Bgpv4Rib`, `TracerouteAnswerer`, plus `RouteAdvertisement`.
* `public` on `BgpRoutingProcess`, `OspfRoutingProcess`, `IncrementalBdpEngine`.
* Visibility `private` → `protected`: `BgpRoutingProcess` RIB/field set,
  `VirtualRouter` (`_name`, `_node`, `_ospfProcesses`, `_vrf`),
  `PartialDataplane` (fields + constructor).
* `Serializable`: `RouteAdvertisement` (+ Jackson annotations and
  `PROP_ROUTE`/`PROP_REASON`), `TraceDagImpl`, `BgpSessionProperties`,
  `IngressLocation` (via symbolic), and many `datamodel.flow.*` classes.
* Symbolic BUILD visibility made public.
* BDD serialization support: `BDD.getIndex()`, `JFactory.makeBDD`/`BDDImpl`
  public, `BDDImpl.getIndex()`, `JFactory.bdd_nodecount` public, new
  `net.sf.javabdd.BDDTransfer`.
* New `org.batfish.bddreachability.BDDReachabilityAnswerElement`.

## Remaining error groups

1. **`DistributedBdpEngine` (18) / `CentralizedBdpEngine` (12)**
   Upstream rewrote the iteration loop of `IncrementalBdpEngine`. The reference
   subclasses it and overrides internals (`_settings`, `_numIterations`,
   `getScheduleName()`, `IbdpSchedule`, `computeIterationStatistics`,
   `MAX_TOPOLOGY_ITERATIONS`). Need to re-subclass against the current
   `IncrementalBdpEngine` iteration API (see upstream
   `projects/batfish/src/main/java/org/batfish/dataplane/ibdp/IncrementalBdpEngine.java`).

2. **`DbfCombinedBgpv4Rib` (14) / `DbfBgpv4Rib` (2)**
   Upstream changed `BgpRib`: `_logicalClock` is now `long[]`, `_bestRibs`
   element type, `bestPathComparator` visibility, `Bgpv4Rib` constructor now
   takes a `ResolutionRestriction`, and `_allRoutes` was renamed. The custom
   S2 RIB (used for memory-efficient prefix sharding) must be reworked against
   the new `Bgpv4Rib`/`BgpRib`.

3. **`TopologyIterator` (6)**
   `IncrementalBdpEngine.collectTrackRoutes`,
   `nextTrackReachabilityResultsByHostname`, `nextTrackRoutesByHostname`,
   `nextTrackMethodEvaluatorProvider` were renamed/moved upstream. Update the
   calls or the visibility.

4. **`TracerouteWorkerSidecar` (3), `DistributedFlowTracer` (1)**
   `org.batfish.question.traceroute.TracerouteAnswererHelper` no longer exists
   and `FlowTracer` methods changed. Traceroute is **not** needed for the
   minimal reachability milestone — consider excluding
   `DistributedTraceroute*`/`TracerouteWorkerSidecar`/`DistributedFlowTracer`
   from the first milestone build.

5. **`BatfishUtils` (2)**
   Upstream `makeTestrigCache()` returns Guava `Cache`; reference expected
   Caffeine. Switch the distributed `BatfishUtils` field/param type to Guava
   `com.google.common.cache.Cache`.

6. **`ArpReplies` (3) / `PartialForwardingAnalysis` (2)**
   `ForwardingAnalysisImpl.computeMatchingIps`, `computeRoutesWithNextHop`,
   `computeIpsRoutedOutInterfaces` changed visibility/signature. Check the
   current `ForwardingAnalysisImpl` and adapt (the reference had copied an
   `ArpReplies` helper).

7. **`Worker` (3)**
   `VirtualRouter._independentRib` no longer exists and
   `ospfIteration`/`bgpIteration` signatures changed. Adapt the worker's
   iteration dispatch.

8. **`CentralizedDataPlanePlugin` / `DistributedDataPlanePlugin` /
   `DistributedDataPlane` / `NodeWrapper` / `CentralizedNode` (8)**
   Constructor/visibility drift on `IncrementalDataPlanePlugin` and `Node`;
   low-risk once the larger items land.

## Suggested next steps

1. Reduce scope: build a first milestone without traceroute/multipath/loop/OSPF
   files, so only the BGP + reachability path must compile. Keep the excluded
   files for later.
2. Port `IncrementalBdpEngine` subclassing first (blocks 30 errors).
3. Port/adjust the RIB (blocks 16 errors).
4. Fix `BatfishUtils` cache type (2 errors) and the engine/worker casts.
5. Get a single-JVM equivalence run working with `TestRunner` on a tiny
   snapshot (vanilla Batfish vs S2 same result).
6. Only then build images and run the 1-vs-3 Pod comparison.

## Why results should be identical

S2 keeps a *real* node for each locally-hosted switch and a *shadow* node for
every remote switch. A shadow node relays route exchange to the real node on
another worker via the sidecar, so switch models remain unaware of placement.
The distributed fixpoint therefore computes the same RIBs/FIBs as Batfish; the
1-vs-3 Pod check is exactly this invariant.
