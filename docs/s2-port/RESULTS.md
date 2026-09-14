# S2 drop-in engine + pool — results

Consolidated results for the "use S2 as a Batfish dataplane engine" work (`-dataplaneengine=s2`),
including the scale/memory measurements (B.6) and the Kubernetes end-to-end (B.7). Details:
[`PLUGIN.md`](PLUGIN.md), [`B6-B7-VERIFICATION.md`](B6-B7-VERIFICATION.md), [`M5-SCALE.md`](M5-SCALE.md).

## What was built (merged: PRs #3–#16 on `master`)

- **`S2DataPlanePlugin`** (`@AutoService(Plugin.class)`, engine `s2`): selected exactly like stock
  `ibdp`; default engine unchanged. Settings `-s2workers` (0 = auto), `-s2storedataplane`,
  `-s2slicedir`, `-s2controllerhost`/`-s2controllerport`.
- **Distributed compute**: node partition → N workers (owned nodes real, rest shadowed) → global
  `DataPlane`, assembled **lazily** per host (`S2LazyDataPlane` over `S2HostSlices`).
- **Persistent pool (choice A)**: `S2Main controller-service` / `worker-service` roles; the engine
  drives the controller per snapshot; each worker writes its owned hosts' **per-host slices** to a
  shared volume (`S2_SLICE_DIR`), and the engine serves from them (`S2DirectoryHostSlices`).
- **DPV-lite (owned-only forwarding exact)**: two one-shot post-convergence cluster exchanges
  (`unionUnownedArpIps` + `unionArpReplies`) supply the only cross-node `ForwardingAnalysis` inputs,
  so owned-only is forwarding-exact and the worker-service defaults to it.
- **Launcher** `scripts/s2-batfish.sh`; **k8s** `k8s/pool/` (controller Deployment+Service, worker
  StatefulSet, RWX slice PVC, engine Deployment) + `k8s/pool/engine-query.yaml` Job; scripts
  `s2-pool-demo.sh`, `s2-engine-query.sh`; `S2Main vanilla <net>` baseline role.

## Correctness

- Stock `dataplaneengine=ibdp` (upstream) byte-for-byte unchanged (all new params default to null).
- Owned-only equals vanilla on **ribs, BGP routes, FIB keys, `getVrfForwardingBehavior()`, and
  `getArpReplies()`** (tests `S2RemoteSidecarTest`, `S2DistributedControlPlaneTest`).
- Stock question path: `RoutesAnswerer`/`RoutesQuestion` answer identically with `-s2workers=3`
  (`S2RoutesQuestionTest`).
- Stock suites pass: `//projects/batfish/.../dataplane/ibdp:tests`,
  `//projects/common/.../datamodel:tests`; runner and pool demos all `MATCH`.

## B.6 — memory

### Method / caveats

- **vanilla** = stock `ibdp` engine in **one JVM** (parse + compute), via `S2Main vanilla <net>`.
- **S2 per-worker** = the runner's reported per-worker peak (owned-only default, configs shipped by
  the controller, so workers do not parse).
- `peakHeapBytes()` reports the **sum of each heap pool's peak used** — an overestimate of the
  simultaneous heap peak (Eden/Old peaks do not coincide) and sensitive to `-Xmx`. Judge a budget by
  `-Xmx`, and compare numbers produced the same way.

### One worker vs vanilla (all `MATCH`)

| network | prefixes | vanilla `ibdp` (1 JVM) | S2 W=3 max/worker | S2 W=6 max/worker |
| --- | --- | --- | --- | --- |
| `s2-big2` | 640 | 547.6 MiB | 209.5 | 182.7 |
| `s2-mega` | 4096 | 867.1 MiB | 381.5 | 372.2 |
| `s2-giga` | 32768 | 4480.0 MiB | 1625.4 | 1202.8 |
| `s2-giga2` | 65552 | 12182.0 MiB | 4844 | 3306 |
| `s2-giga4` | 131088 | 31796.1 MiB | (not run locally) | |

A worker's peak is **~60–73% below vanilla** and decreases with W (with a floor from the per-worker
fixed/global data). `s2-giga2` used `-Xmx6g` for the S2 runs; the rest used the default heap.

### Node scaling (FatTree, ~5 prefixes/node, `MATCH`)

Growing the **node count** keeps each worker's peak far below vanilla (runner per-worker, includes
the distributed BDD reachability phase):

| network | nodes | vanilla `ibdp` (1 JVM) | S2 W=3 max/worker | S2 W=6 max/worker |
| --- | --- | --- | --- | --- |
| `s2-clos6` | 45 | 1457.9 MiB | 215.8 | 180.6 |
| `s2-clos8` | 80 | 2312.0 MiB | 328.6 | 273.7 |

### Per-node heap-budget demo (`s2-giga2`, this PC)

| process | budget | result |
| --- | --- | --- |
| vanilla `ibdp` (1 JVM) | `-Xmx4g` | **OOM** |
| vanilla `ibdp` (1 JVM) | `-Xmx6g` | **OOM** |
| vanilla `ibdp` (1 JVM) | `-Xmx8g` | completes |
| S2 controller (parse+ship) | `-Xmx4g` | completes (2230.6 MiB) |
| S2 W=6, each worker | `-Xmx4g` | completes (max 2734.8 MiB) |

With a **6 GiB-per-process budget**, vanilla cannot compute `s2-giga2` while S2 can (controller and
every worker fit).

### Single host vs multiple nodes (invariant)

On one host, S2's **total** across workers is **≥ vanilla's single-JVM peak** (same total work + the
controller + cross-worker exchanges). Distribution lowers the **per-process** peak, which only helps
when workers run on **separate nodes**. Raising a hypervisor/VM memory (e.g. OrbStack) only
repartitions the host's RAM, so it does not change this. The invariant to optimize is *per-worker
peak*.

### Engine-side laziness

A node-scoped question reads only the touched hosts' slices; whole-network iteration is the only case
that materializes the union. Structurally proven: `S2HostSlicesTest#testPointLookupsResolveOnlyOwningHost`
uses a slice source that throws for any non-target host.

### owned-only vs full (secondary)

Comparable per-worker (within a few %; the DPV exchanges add overhead on route-light nets). The win
that matters is per-worker vs vanilla above.

## B.7 — Kubernetes end-to-end

- **Local (process-level)**: `allinone -dataplaneengine=s2` against a controller-service + 3
  worker-services → controller `done (3 workers, 307.7 MiB)`, worker wrote slices, engine returned
  `IncrementalBdpAnswerElement status=SUCCESS` (`scripts/s2-engine-query.sh`).
- **In-cluster (single-node OrbStack)**: `k8s/pool` + `k8s/pool/engine-query.yaml` Job → controller
  registered 3 workers via the headless Service, `done (3 workers, 486.4 MiB)`, worker wrote slices
  to the shared PVC, Job `Complete` with the dataplane answer.
- Single-node clusters with only the RWO `local-path` provisioner need the PVC as `ReadWriteOnce`;
  multi-node needs `ReadWriteMany`.
- **In-cluster large snapshot (`s2-giga2`, 65552 prefixes, 16 nodes)**: the engine Job's
  `generate-dataplane` returned `SUCCESS`; the controller reported
  `done (3 workers, 10415.7 MiB total peak heap)`; the three worker pods' compute-phase peaks were
  2832 / 3091 / **3258 MiB** (each worker owns ~5 nodes), and worker-0 wrote slices for 5 hosts to the
  shared PVC. So an in-cluster worker pod needs **~3.3 GiB** for a snapshot whose stock single-JVM
  peak is ~12 GiB (measured in a 24 GiB single-node OrbStack cluster).

## Scale reference

| network | routers | origination prefixes | per-router BGP table | config |
| --- | --- | --- | --- | --- |
| `s2-big2` | 10 | 640 | ~640 | — |
| `s2-huge` | 8 | 2048 | ~2048 | — |
| `s2-mega` | 16 | 4096 | ~4096 | — |
| `s2-giga` | 16 | 32768 | ~32768 | — |
| `s2-giga2` | 16 | 65552 | ~65552 | 6.9 MB |
| `s2-giga4` | 16 | 131088 | ~131088 | 14 MB |
| `s2-clos6` | 45 | ~225 | ~5 | — |
| `s2-clos8` | 80 | ~400 | ~5 | — |

`s2-giga2`/`s2-giga4` are generated with `scripts/gen-topology.py line --nodes 16 --originate
{4096,8192}` (not committed; the generator was fixed to allow `--originate > 254`). Route counts are
mid-ISP scale; the node count (16) is small.

## Reproduce

```sh
# Correctness
bazel test //projects/s2:s2_tests //projects/s2:s2_tests_pmd

# One worker vs vanilla (peaks)
for W in 3 6; do S2_BASE_PORT=$((18000+W)) scripts/local-demo.sh "$W" s2-giga; done
S2_INPUT_DIR=$PWD/networks java -jar bazel-bin/projects/s2/s2_main_deploy.jar vanilla s2-giga

# Pool
scripts/s2-pool-demo.sh 3 s2-triangle       # ribs=MATCH forwarding=MATCH
scripts/s2-engine-query.sh 3 s2-triangle    # engine -> pool end-to-end

# Kubernetes (single node: RWO PVC)
kubectl kustomize k8s/pool | sed 's/ReadWriteMany/ReadWriteOnce/' | kubectl apply -f -
kubectl apply -f k8s/pool/engine-query.yaml
```
