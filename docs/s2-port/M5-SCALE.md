# M5 scale-out — per-worker local graph generation

Status: **implemented and verified.** Each worker now generates only the reachability
edges whose source it owns and pulls the boundary edges into its own states from
their owners. The paper's §3.1/§4.3 scale property holds: per-worker edge tables
shrink with the number of workers (the fixpoint was already distributed).

## Verified

* **M1–M5 all green.** OrbStack Kubernetes, 1 Pod and 3 Pods both report
  `ribs=MATCH reachability=MATCH answer=MATCH` against vanilla Batfish
  (`scripts/k8s-demo.sh 1|3`, `scripts/compare-answers.sh`).
* The scaled backward fixpoint equals the full-graph fixpoint **per state** for
  1 and 3 workers (`ScaledReachabilityTest`), on the `s2-triangle` snapshot.
* Multi-process runner (`scripts/local-demo.sh`) matches for 1 and 3 workers (verified
  repeatedly on the 3-worker triangle: 5/5).
* `networks/s2-line` (6-node static eBGP line) matches vanilla for 1, 3, and 6 workers,
  and is covered by `S2DistributedControlPlaneTest#testMultiHopLineMatchesVanilla`.

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

With these, `s2-line` matches vanilla at 1, 3, and 6 workers and multi-worker runs no
longer hang.

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

# Kubernetes (OrbStack)
scripts/build-s2.sh --image
scripts/k8s-demo.sh 1
scripts/k8s-demo.sh 3
scripts/compare-answers.sh
```

## Key files

* Runner: `projects/s2/src/main/java/org/batfish/dataplane/ibdp/S2Main.java`
* Backward worker: `S2ReachabilityWorker.java`
* Local forwarding view: `OwnedForwardingAnalysis.java`
* Boundary edges: `S2SidecarHandlers.java`, `S2Messages.java`, `S2StateHosts.java`
* BDD transport: `S2BddSidecar.java`
* BDD codec: `projects/bdd/src/main/java/net/sf/javabdd/BDDTransfer.java`
* Transition codec: `projects/batfish/.../transition/TransitionTransfer.java`
* Tests: `ScaledReachabilityTest`, `TransitionTransferTest`, `DistributedReachabilityTest`
