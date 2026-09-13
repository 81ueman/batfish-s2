# S2 as a Batfish dataplane engine (vanilla UX)

Goal: make S2 usable exactly like stock Batfish — same snapshot input, same question engine, same
REST / pybatfish API, no S2-specific runner, no verify step, no required flags. This document tracks
that work. The distributed runner (`S2Main`, `scripts/local-demo.sh`, the k8s Jobs) remains for
scale experiments; this is the "drop-in engine" path.

## What works today

S2 is registered as a Batfish **`DataPlanePlugin`** (`S2DataPlanePlugin`,
`@AutoService(Plugin.class)`, engine name `s2`). `Batfish` discovers it with the other plugins
(`loadPlugins()`), so it is selected like any engine:

```sh
# coordinator / allinone / worker, exactly as for the stock ibdp engine
batfish -dataplaneengine=s2 -s2workers=8 ...
# or via batfish.properties:  dataplaneengine=s2 / s2workers=8
# thin launcher:
scripts/s2-batfish.sh ...           # allinone with -dataplaneengine=s2
S2_WORKERS=4 scripts/s2-batfish.sh -snapshotdir /path/to/snapshots
```

* **Default is unchanged.** `dataplaneengine` defaults to `ibdp`, so stock Batfish never runs S2.
* **Same input/output.** The plugin builds the same topology context as `IncrementalDataPlanePlugin`,
  partitions the snapshot across `N` workers (owned nodes real, the rest shadowed), runs them
  concurrently, and returns a global `DataPlane` the normal question engine consumes.
* **No S2 API.** Questions are answered by the stock answerers; pybatfish/REST are untouched.
  `S2RoutesQuestionTest` asserts the stock `RoutesAnswerer`/`RoutesQuestion` answer identically to
  `ibdp` with `-dataplaneengine=s2 -s2workers=3`.
* **Lazy global data plane.** `S2LazyDataPlane` resolves each host's data-plane slice through a
  pluggable `S2HostSlices` source, lazily: a point/row lookup (a node-scoped question) is served
  from the owning host's slice; only whole-network iteration materializes the union. The in-process
  source extracts slices from the per-worker data planes; a directory-backed source (the Kubernetes
  shared-PVC model) reads a host on demand. This is the seam the remote worker pool plugs into.
* **No verify step.** The `S2Main verify` role (and `scripts/*`) is a runner/CI concern; the engine
  path does not need it.
* **Protocol fallback.** Snapshots that use EIGRP/IS-IS/RIP (not distributed) automatically run on a
  single worker, so the engine is a drop-in for any snapshot.
* **Persistent pool.** With `-s2controllerhost` / `-s2controllerport` the engine ships each snapshot
  to a long-lived controller service and its worker pool, then serves questions lazily from the
  per-host slices the workers wrote; see "Persistent pool" below.

> Note: `scripts/s2-batfish.sh` works because `//projects/allinone:allinone_main` now bundles the S2
> plugin (`//projects/s2:s2`), so the engine is discoverable exactly like stock `ibdp`.

## Settings

| Setting | Default | Meaning |
| --- | --- | --- |
| `dataplaneengine` | `ibdp` | Selects the engine; set to `s2` to use S2. |
| `s2workers` | `0` (auto) | S2 workers (partition shards) for the in-process engine. Auto = `availableProcessors` capped by the node count. Ignored when a controller host is configured (the pool size is fixed there). |
| `s2storedataplane` | `true` | Persist the data plane to disk. Set `false` for a lazy/remote data plane (kept in memory only). |
| `s2slicedir` | `""` | Directory for per-host data-plane slices. With `s2controllerhost` it is the base directory the pool writes into and the engine reads from; alone it means "serve from these pre-written slices". |
| `s2controllerhost` | `""` | Host of the persistent S2 controller service. When set, the engine ships each snapshot to the pool instead of computing in-process. |
| `s2controllerport` | `0` | Port of the persistent S2 controller service (required with `s2controllerhost`). |

`Settings.getS2Workers()` / `setS2Workers`, `getS2StoreDataPlane()` / `setS2StoreDataPlane`,
`getS2SliceDir()` / `setS2SliceDir`, `getS2ControllerHost()` / `setS2ControllerHost`,
`getS2ControllerPort()` / `setS2ControllerPort`; CLI `-s2workers`, `-s2storedataplane`,
`-s2slicedir`, `-s2controllerhost`, `-s2controllerport`, or the same keys in
`batfish.properties`.

## Kubernetes integration (persistent worker pool)

Chosen model: a **persistent pool** (StatefulSet + headless Service) the engine connects to, rather
than per-snapshot Jobs — no scheduling wait and it matches normal Batfish worker services.

```
pybatfish ── init_snapshot / question ──▶ Batfish (coordinator/worker JVM)
                                             │ computeDataPlane(snapshot)
                                             ▼
                                  S2DataPlanePlugin (engine JVM)
                                    1. read snapshot (configs/topology)
                                    2. discover the pool (Service DNS) → N
                                    3. partition nodes across workers
                                    4. ship configs/descriptors to each worker
                                    5. distributed control-plane fixpoint over RPC
                                    6. each worker persists its owned per-host
                                       data-plane slices to shared storage
                                    7. return a lazy global DataPlane
                                             │
                                             ▼
                              question engine (stock answerers)
                                - node-scoped  → fetch that host's slice
                                - whole-network→ materialize, or distributed execution (BDD)
```

* **Deployment:** Batfish (Deployment/Job) + `s2-workers` StatefulSet + headless Service + a shared
  PVC (snapshot input + per-host data-plane blocks). `N = kubectl scale` (pool size). Resources per
  `OPS.md` (worker request 1Gi / limit 6Gi / `-Xmx4g`).
* **Control plane = live RPC.** The fixpoint reuses the runner's controller/sidecar protocol
  (`S2ControlMessages`, `S2ControllerServer`, `S2SidecarServer`); the worker-side network barrier
  `S2RemoteCoordinator` already exists (the plugin reuses it; in-process runs use `S2Cluster`).
* **Data plane = per-host blocks.** Each worker writes its owned nodes' slices to shared storage
  (the same per-host granularity as `PerHostDataPlane`); the engine's lazy `DataPlane` reads a host's
  block on demand (with an LRU). This is what makes a data plane larger than one JVM answerable.
* **Failure/consistency:** a worker dying mid-fixpoint fails that snapshot's run (retry); slices are
  keyed by snapshot id so a version is pinned.

### Two-stage flow (implemented seam)

The engine can serve the data plane **from per-host slices produced by an out-of-process pool**,
which is the concrete Kubernetes step:

1. **Produce.** The S2 worker pool computes the fixpoint and each worker writes its owned hosts'
   slices to the shared volume (`S2_SLICE_DIR`, default `<S2_OUTPUT_DIR>/slices`; the `s2-worker`
   StatefulSet mounts the PVC there). `S2Main worker` (one-shot) and `S2Main worker-service`
   (persistent) both do this.
2. **Serve.** A Batfish engine runs with `-dataplaneengine=s2 -s2slicedir=/s2/shared/slices
   -s2storedataplane=false`; questions are answered lazily from the slices (`S2DirectoryHostSlices` →
   `S2LazyDataPlane`), so only the touched hosts are read. Verified by
   `S2DataPlanePluginTest#testS2EngineServesFromSliceDirectory`.

### Persistent pool (choice A, implemented)

The two stages are automated by a long-lived controller service and worker services, so a
persistent pool serves many snapshots:

```sh
# 1. Controller service: accepts N worker services, then one compute request per snapshot.
S2Main controller-service <numWorkers> <controllerPort>

# 2. Worker services: register once, then serve snapshots until shut down.
S2Main worker-service <workerId> <controllerHost> <controllerPort> <sidecarPort> [advertisedHost]

# 3. Drive the pool. Locally, the smoke client ships one snapshot and compares to vanilla:
S2Main pool-compute <network> <controllerPort>
#    or, on a real engine, set the controller host so the plugin drives the pool:
batfish -dataplaneengine=s2 -s2controllerhost=HOST -s2controllerport=PORT \
        -s2slicedir=/s2/shared/slices -s2storedataplane=false ...
```

* **Protocol reuse.** The pool reuses the runner's controller/sidecar protocol
  (`S2ControlMessages` / `S2ControllerServer` / `S2SidecarServer`) and its round/sum barriers
  (`S2RoundBarrier` / `S2SumBarrier`); the worker-side barrier is still `S2RemoteCoordinator`. No
  new barrier.
* **Engines.** `S2ControllerService` accepts N workers once (each advertises its sidecar endpoint),
  then per snapshot computes the partition, ships the payload, drives the existing fixpoint, and
  returns once the workers wrote their slices. Requests are serialized and tagged with a monotonic
  `runId` so a straggler from a failed run cannot be mistaken for the next snapshot.
  `S2WorkerService` swaps its route-sidecar handler per snapshot and loops.
* **Slice GC.** The engine creates a unique per-snapshot directory under `s2slicedir`, registers it
  for recursive deletion on JVM shutdown (`S2DirectoryHostSlices.deleteRecursively`), and the
  `pool-compute` client deletes its temporary directory when done.
* **Forwarding exactness.** A remote shadow's FIB is a stub, so the two cross-node FIB inputs of the
  forwarding analysis are recovered by a one-shot post-convergence exchange: each worker ships the
  unowned ARP IPs and the exact per-interface ARP replies it can compute from its owned nodes' full
  FIBs, the coordinator unions them across the cluster, and each worker rebuilds its final
  forwarding analysis with the global state. Owned-only mode is therefore forwarding-exact, and
  `S2WorkerService` runs it by default (per-worker memory scales with the owned node count);
  `-Ds2.ownedDataplane=false` restores the full-dataplane-per-worker behavior.
* **Kubernetes.** `k8s/pool/` (separate from the one-shot `k8s/base` runner) has a persistent
  `s2-controller` Deployment + Service, an `s2-worker` StatefulSet in worker-service mode, a
  ReadWriteMany `s2-slices` PVC shared by the workers and the engine, and an `s2-engine` Deployment
  running allinone with `-dataplaneengine=s2 -s2controllerhost ... -s2storedataplane=false`.
* **Local verification.** `scripts/s2-pool-demo.sh <N> [network]` starts the controller service and
  N worker services as processes and runs `pool-compute` (expects `ribs=MATCH forwarding=MATCH`);
  `S2PoolServiceTest` does the same in-JVM and also drives the real plugin, serving two snapshots
  through one pool.

One-time build for the engine image: `bazel build //projects/allinone:allinone_main_deploy.jar`
then `cp -f bazel-bin/projects/allinone/allinone_main_deploy.jar docker/` and
`docker build -f docker/Dockerfile.s2-engine -t s2-engine:local .` (the deploy jar now bundles the
S2 plugin, so `-dataplaneengine=s2` is discoverable like any stock engine).

## Scale verification

* **Memory.** With in-process workers the lazy views only bound the *container* memory (the node data
  is in the same JVM). The real win needs remote workers (above). Measurement plan: for a large
  snapshot, compare peak heap for a node-scoped question (`routes` on one node) vs a whole-network
  question, with `-dataplaneengine=s2` and `-s2storedataplane=false`; the node-scoped run should pull
  only the touched hosts. Runner-scale baselines are in `M5-SCALE.md`.
* **Kubernetes.** `scripts/k8s-demo.sh` covers the one-shot `k8s/base` runner path.
  `k8s/pool/` is the persistent pool: apply it, then drive the `s2-engine` Deployment (or a local
  engine) with `-dataplaneengine=s2 -s2controllerhost=s2-controller.s2-pool.svc.cluster.local
  -s2controllerport=4090 -s2storedataplane=false` and compare a question against `ibdp` on the
  same snapshot. `kubectl scale statefulset/s2-worker --replicas=N` grows the pool.

### Local k8s bring-up (OrbStack, validated)

`k8s/pool` was brought up on a single-node OrbStack cluster:
- The `s2-controller` Deployment registered all 3 `s2-worker` services (via the headless
  `s2-worker` Service DNS) and the `s2-engine` Deployment came up healthy.
- On a single-node cluster with only the RWO `local-path` provisioner, the `s2-slices` PVC must be
  `ReadWriteOnce` (multiple pods on the same node share it); use `ReadWriteMany` for multi-node.
- The engine must run as a service: `scripts/entrypoint-engine.sh` passes `-runclient false` (the
  allinone client otherwise requires a command file and exits immediately).
- Driving a question end-to-end needs a pybatfish/REST client against the `s2-engine` Service
  (coordinator port 9997); the compute path itself is already verified by `s2-pool-demo.sh` and
  `S2PoolServiceTest`.

## Status

Done: engine registration/selection, `-s2workers` (explicit + auto), distributed compute + lazy
global data plane, stock-question equivalence, protocol fallback, `s2storedataplane`, launcher, the
pluggable per-host slice source (`S2HostSlices`: in-process + directory-backed), **slice production
by the worker pool** (`S2_SLICE_DIR`; the k8s `s2-worker` StatefulSet mounts it), **serving from
those slices** (`-s2slicedir`, lazily), and the **persistent controller/worker service pool**
(choice A): `controller-service` / `worker-service` roles, the engine client and settings
(`s2controllerhost` / `s2controllerport`), slice GC, `k8s/pool/`, and the local
`S2PoolServiceTest` + `scripts/s2-pool-demo.sh` verification.

Next: pool discovery/auto-`N` from the Service, per-snapshot slice cleanup keyed on snapshot ids
(currently JVM-exit GC), and the scale verification above.

## Running the local pool

```sh
bazel build //projects/s2:s2_main_deploy.jar
S2_INPUT_DIR=$PWD/networks scripts/s2-pool-demo.sh 3 s2-triangle
# -> S2 pool-compute s2-triangle (3 workers): ribs=MATCH forwarding=MATCH
```

`S2PoolServiceTest` is the same flow in-JVM (it also drives the real `S2DataPlanePlugin` through the
pool and serves two snapshots from one worker pool):

```sh
bazel test //projects/s2:s2_tests --test_filter=org.batfish.dataplane.ibdp.S2PoolServiceTest
```
