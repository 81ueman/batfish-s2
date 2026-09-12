# M5 scale-out — work in progress handoff

Where we are on the last piece (per-worker-local graph generation, paper §3.1/§4.3).

## Verified and done

* **M1–M5 all green.** On OrbStack Kubernetes, 1 Pod and 3 Pods both report
  `ribs=MATCH reachability=MATCH answer=MATCH` against vanilla Batfish
  (`scripts/k8s-demo.sh 1|3`, `scripts/compare-answers.sh`).
* Public-API reachability answer (concrete flow set) verified end to end
  (commit `0a35485`).

## The scale problem

Every worker builds a `BDDReachabilityAnalysisFactory` from **all** configs and
the **full** `ForwardingAnalysis`, so it generates the **whole** edge table.
Only the fixpoint is distributed, so per-worker memory/cost does not scale with
the number of workers. The paper and the reference (`PartialForwardingAnalysis` +
`processEdges` + `finalizeAnalysisService`) make each worker hold only its owned
switches' part.

## Experiment that failed (and why)

`OwnedForwardingAnalysis` (restrict `ForwardingAnalysis` to owned hostnames) +
`S2ForwardReachabilityWorker` (forward fixpoint, local-source edges):

* 1 worker: `scaled=MATCH` (edges=151)
* 3 workers: `scaled=DIFF` (edges=333)

Reason: Batfish generates some cross-node transitions using the **far endpoint's**
forwarding behavior, so restricting one side makes the boundary edge disappear on
both sides. Local generation alone is insufficient; boundary edges must be handled
explicitly.

## Approved design (proceed with this)

1. **Direction: backward** (same path as Batfish's answer/`getIngressLocationReachableBDDs`).
2. **Boundary: local generation + pull missing inter-worker edges** from the owner
   (reference `finalizeAnalysisService` / `PULL_INTER_WORKER_EDGES`).
3. **Transitions: serialize only the types that appear on boundary edges**
   (`Constraint`, `EraseAndSet` first).
4. **Unify on backward**; the runner stays on the verified full-graph path until
   the scaled path is verified.

## Implemented WIP (not wired into the runner)

* `projects/s2/.../OwnedForwardingAnalysis.java` — `ForwardingAnalysis` restricted
  to owned hostnames.
* `projects/s2/.../S2ForwardReachabilityWorker.java` — forward distributed fixpoint
  (used only by the failed experiment; keep for reference).
* `projects/batfish/.../bddreachability/transition/TransitionTransfer.java` —
  serialize/deserialize `Constraint`/`EraseAndSet` (with BDDs) via `BDDTransfer`.
* The backward worker `S2ReachabilityWorker` (used by the runner) stays as is.

## Next steps

1. **Boundary edge pull**
   * Add a sidecar request: for cross-worker edges whose `dst` is owned by the
     requester (in backward, edges `PreOutEdgePostNat(src remote) ->
     PreInInterface(dst local)`), return `src`, `dst`, and
     `TransitionTransfer.save(transition)`.
   * On the requester, `TransitionTransfer.load` into its own `JFactory` and add
     the edge to its analysis. Since `BDDReachabilityAnalysis` builds its edge
     table in the constructor, either rebuild it with the pulled edges or make the
     worker merge them into its own edge map (the worker already copies edges into
     `_reverseEdges`). The simplest correct path: build the per-worker analysis
     with `OwnedForwardingAnalysis`, then append pulled boundary edges to the
     worker's edge map before running.
   * Only `Constraint` and `EraseAndSet` are handled initially; extend
     `TransitionTransfer` (and its `TransitionVisitor`) if others appear.
2. **Drop remote-remote edges** from each worker's retained table
   (`processEdges`-style filter; both endpoints remote ⇒ never used locally).
3. **Verify** 1 vs 3 Pods still give `ribs` / `reachability` / `answer` MATCH and
   report per-worker `symbolicEdges` (already printed by the runner).
4. **Enlarge the topology** to 4–6 nodes (static eBGP) and record per-worker edge
   counts / memory as the scale evidence.

## Commands

```sh
# unit tests
bazel test //projects/s2:s2_tests

# local multi-process (1 or 3 workers, separate JVMs)
bazel build //projects/s2:s2_main_deploy.jar
scripts/local-demo.sh 1
scripts/local-demo.sh 3

# Kubernetes (OrbStack)
scripts/build-s2.sh --image
scripts/k8s-demo.sh 1
scripts/k8s-demo.sh 3
scripts/compare-answers.sh
```

## Key files

* Runner: `projects/s2/src/main/java/org/batfish/dataplane/ibdp/S2Main.java`
* Backward worker: `S2ReachabilityWorker.java`
* Sidecars: `S2SidecarServer.java`, `S2SidecarHandlers.java`, `S2BddSidecar.java`
* BDD codec: `projects/bdd/src/main/java/net/sf/javabdd/BDDTransfer.java`
* Transition codec: `projects/batfish/.../transition/TransitionTransfer.java`
