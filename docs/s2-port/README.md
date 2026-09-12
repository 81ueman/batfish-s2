# S2 port onto latest upstream Batfish

This directory tracks our effort to re-implement **S2: A Distributed Configuration
Verifier for Hyper-Scale Networks** (SIGCOMM'25) on top of **current upstream
Batfish**, then validate that distributing verification across several
Kubernetes Pods does not change the verification result compared to a single Pod.

## Repos involved

| Repo | Path | Role |
| --- | --- | --- |
| upstream Batfish | `batfish-s2/` (this repo, branch `s2`) | Base we modify |
| S2 reference | `~/ghq/github.com/81ueman/s2-reference` | Old fork of Batfish (base commit `0ee9162`, 2023-06-13) |
| S2 paper | `nv-papers/papers/s2-2025.pdf` | Design reference |

The S2 reference adds `projects/distributed` (89 Java files) and touches 98
files across Batfish core (~1,286 insertions). Its base is ~3 years older than
upstream `master`, so the port is a mixture of *applying S2's intent* and
*adapting to renamed/moved APIs*.

## Approach

Compile-driven port:

1. Copy `projects/distributed` into this repo.
2. Wire it into the current build: `projects/common` (renamed from
   `projects/batfish-common-protocol`), gRPC added as an isolated Maven repo
   (`grpc_maven`) so the main Batfish lockfile is untouched.
3. Apply the "core" part of the S2 patch (visibility `private`→`protected`/
   `public`, `final`→non-final, `Serializable`), rewriting
   `projects/batfish-common-protocol/` → `projects/common/`.
4. Iterate `bazel build //projects/distributed:distributed` until it compiles,
   adapting where Batfish APIs changed. Newer upstream is the source of truth.
5. Containerize controller/worker and run on local Kubernetes (OrbStack).
6. Compare 1-worker vs 3-worker output for the same snapshot.

A reconstructed full patch is saved at
`/private/var/folders/.../opencode/s2-full.patch` (regenerate with
`scripts/make-s2-patch.sh`).

## Status (checkpoint)

* ✅ Upstream Batfish builds on this machine (`bazel build
  //projects/allinone:allinone_main`, ~70s warm).
* ✅ `projects/distributed` copied and wired; gRPC deps resolve.
* ✅ Core visibility/serialization patch largely applied.
* ✅ BDD-layer additions (`BDD.getIndex()`, `JFactory.makeBDD`/`BDDImpl`
  visibility, `getIndex`, `bdd_nodecount`) applied.
* 🔄 `//projects/distributed:distributed` **does not compile yet**: 76 errors in
  ~17 files, concentrated in:
  * `DistributedBdpEngine` / `CentralizedBdpEngine` (upstream reworked the
    engine iteration API),
  * `DbfCombinedBgpv4Rib` / `DbfBgpv4Rib` (upstream RIB internals changed),
  * `TopologyIterator` (upstream moved/renamed `IncrementalBdpEngine` track
    helpers),
  * `TracerouteWorkerSidecar` (`TracerouteAnswererHelper` no longer exists),
  * `BatfishUtils` (upstream now uses Guava `Cache`, reference used Caffeine),
  * assorted `ForwardingAnalysisImpl`/`Ospf`/`VirtualRouter` method visibility.
* ⏳ Kubernetes manifests and equivalence harness are scaffolded (see
  `k8s/`, `scripts/`) but not yet runnable until the module compiles.

See `docs/s2-port/PORTING.md` for the per-file remaining work.

## Kubernetes validation plan

* One controller Pod + N worker Pods (N = 1 and N = 3).
* The verified network model is partitioned across workers (`random`/`expert`
  partition scheme initially; METIS later), i.e. "splitting the network" means
  **splitting the network being verified**, not isolating Pods with
  NetworkPolicy.
* Controller and workers are separate Pods; cross-Pod route/packet exchange goes
  through S2 sidecars over gRPC.
* Same snapshot is verified in both configurations; the controller's RIB/FIB and
  reachability answer are diffed. Expect: identical results.

## Layout added by this work

```
projects/distributed/     # ported S2 module
docker/Dockerfile.s2      # controller/worker image
k8s/                      # namespace, controller, worker StatefulSet, jobs
scripts/                  # patch regeneration, build, k8s apply/compare
docs/s2-port/             # this documentation
```
