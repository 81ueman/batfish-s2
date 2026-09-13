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

## Settings

| Setting | Default | Meaning |
| --- | --- | --- |
| `dataplaneengine` | `ibdp` | Selects the engine; set to `s2` to use S2. |
| `s2workers` | `0` (auto) | S2 workers (partition shards). Auto = pool size (once a remote pool exists), else `availableProcessors` capped by the node count. An explicit value is honored. |
| `s2storedataplane` | `true` | Persist the data plane to disk. Set `false` for a lazy/remote data plane (kept in memory only). |

`Settings.getS2Workers()` / `setS2Workers`, `getS2StoreDataPlane()` / `setS2StoreDataPlane`; CLI
`-s2workers` / `-s2storedataplane`, or the same keys in `batfish.properties`.

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
   StatefulSet mounts the PVC there). `S2Main worker` does this now.
2. **Serve.** A Batfish engine runs with `-dataplaneengine=s2 -s2slicedir=/s2/shared/slices
   -s2storedataplane=false`; questions are answered lazily from the slices (`S2DirectoryHostSlices` →
   `S2LazyDataPlane`), so only the touched hosts are read. Verified by
   `S2DataPlanePluginTest#testS2EngineServesFromSliceDirectory`.

### What must be added to fully automate this

1. **Persistent controller service** (choice A): a long-lived `s2-controller` Deployment + Service the
   worker pool connects to and the engine drives per snapshot. The worker-side network barrier
   (`S2RemoteCoordinator`) and the controller/sidecar servers already exist from the runner.
2. **Worker service** — a long-lived Pod that joins the pool, accepts a snapshot assignment, runs its
   shard, writes its slices, and waits for the next snapshot.
3. **Pool discovery + settings** — headless Service (DNS) + a settings key (e.g. `s2workerpool`); auto
   `N` from the pool size.
4. **Slice GC** — clean the snapshot's slice directory after the run.

## Scale verification

* **Memory.** With in-process workers the lazy views only bound the *container* memory (the node data
  is in the same JVM). The real win needs remote workers (above). Measurement plan: for a large
  snapshot, compare peak heap for a node-scoped question (`routes` on one node) vs a whole-network
  question, with `-dataplaneengine=s2` and `-s2storedataplane=false`; the node-scoped run should pull
  only the touched hosts. Runner-scale baselines are in `M5-SCALE.md`.
* **Kubernetes.** `scripts/k8s-demo.sh` covers the runner path today; the plugin path needs the
  worker pool. Once the pool exists, run a Batfish Job with `-dataplaneengine=s2` and
  `-s2workerpool s2-workers:PORT` and compare a question against `ibdp` on the same snapshot.

## Status

Done: engine registration/selection, `-s2workers` (explicit + auto), distributed compute + lazy
global data plane, stock-question equivalence, protocol fallback, `s2storedataplane`, launcher, the
pluggable per-host slice source (`S2HostSlices`: in-process + directory-backed), **slice production
by the worker pool** (`S2_SLICE_DIR`; the k8s `s2-worker` StatefulSet mounts it), and **serving from
those slices** (`-s2slicedir`, lazily).

Next (large): automate the two stages with a **persistent controller service** (choice A) — the
engine drives the pool per snapshot while the worker-side barrier/servers are reused — plus pool
discovery/auto-`N`, slice GC, and the scale verification above.
