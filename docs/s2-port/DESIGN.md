# S2 re-implementation design (clean, from scratch)

We implement S2 ourselves against current upstream Batfish. An earlier copy-based
port was superseded by this work, and the reference implementation
[`XJTU-NetVerify/s2`](https://github.com/XJTU-NetVerify/s2) is used only as a design reference.

## Core idea

Batfish's BGP iteration on a node pulls routes from each neighbor with:

```java
// BgpRoutingProcess.prepareV4UnicastMessages(...)
BgpRoutingProcess remoteProcess = getNeighborBgpProcess(remoteConfigId, nodes);
remoteProcess.getOutgoingRoutesForEdge(edgeId, nodes, bgpTopology, nc, isNewSession);
```

`getNeighborBgpProcess` resolves the neighbor through
`allNodes.get(host).getVirtualRouterOrThrow(vrf).getBgpRoutingProcess()`.

So distribution is achievable without touching the RIB internals at all:

* Every worker builds a `Node` for **all** configurations.
* A node whose configuration is assigned to this worker is **real**.
* Other nodes are **shadow**: their `VirtualRouter`s exist so neighbor lookup
  succeeds, but their `BgpRoutingProcess` is a `ShadowBgpRoutingProcess` that
  serves `getOutgoingRoutesForEdge` by calling the owning worker (in-process for
  M1, gRPC sidecar for M2+).
* `DistributedNode.getVirtualRouters()` returns **only real VRs** (empty for a
  shadow node). Batfish's engine iterates exactly this collection, so only real
  switches are simulated, while `getVirtualRouterOrThrow()` (which uses the
  internal map) still finds shadows for route lookups.

This makes the framework decoupled from the switch model exactly as the paper
describes: the real process runs stock Batfish logic; only the *route pull*
crosses the worker boundary.

## Required (minimal) core hooks

| File | Change | Why |
| --- | --- | --- |
| `IncrementalBdpEngine` | add `Node newNode(Configuration)` factory; use `this::newNode` | let us build `DistributedNode`s |
| `Node` | drop `final` | subclass `DistributedNode` |
| `BgpRoutingProcess` | make `public` non-final, `getOutgoingRoutesForEdge` `protected` | override for shadow nodes |

Our classes live in package `org.batfish.dataplane.ibdp` (in a new Bazel
package) so they can use package-private members (`_bgpRoutingProcess`,
`getBgpRoutingProcess()`, the `BgpRoutingProcess` constructor, `isDirty()`,
`bgpIteration()`, ...).

## Milestones

**M1 — single JVM, in-process sidecar, control-plane equality.**
* Partition N configs across W logical workers (`RandomPartitioner`).
* Build per-worker `DistributedNode` maps; shadows resolve via a shared registry.
* Run `IncrementalBdpEngine.computeDataPlane` on each worker.
* Compare every real node's BGP RIB with vanilla Batfish's RIB. Expect identical.

**M2 — separate processes + gRPC sidecar.**
* Same model, but `ShadowBgpRoutingProcess` calls a sidecar that serializes the
  `EdgeId` request and remote process identity over gRPC.
* Worker runs as its own JVM.

**M3 — Kubernetes 1 vs 3 Pods.**
* `docker/Dockerfile.s2`, `k8s/overlays/{1pod,3pod}`.
* Re-run the same snapshot and diff the controller output.

## Reachability

For the first milestone the equality check is on BGP RIBs/FIBs, which is exactly
the invariant S2 relies on. Distributed data-plane reachability (BDD forwarding
across shadow ports) is a later milestone; it can reuse the same shadow/relay
pattern on `Transition`s.

## Why the result is unchanged

Only the *placement* of switches changes. Each real process computes with the
same inputs it would have in a single machine: the neighbor's advertisements are
either local or fetched verbatim from the owning worker. Hence the distributed
fixpoint reaches the same RIBs/FIBs, and the 1-Pod vs 3-Pod check validates it.
