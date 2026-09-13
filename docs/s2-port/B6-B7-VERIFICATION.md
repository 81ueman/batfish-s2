# B.6 / B.7 verification notes

Working notes for the two scale/verification items on the S2 drop-in engine
(`-dataplaneengine=s2`). Keep this file updated as results land.

## B.7 — engine drives the pool (end-to-end)

**Local (process-level), validated 2026-09-14.** The engine (allinone, `-dataplaneengine=s2`) was run
against a persistent pool (1 controller-service + 3 worker-services) on one machine:

```
S2_BASE_PORT=19490 (pool on 127.0.0.1)  engine args:
  -dataplaneengine=s2 -s2controllerhost=127.0.0.1 -s2controllerport=19490
  -s2storedataplane=false -s2slicedir=<dir>
```

Client command file (no question template needed):

```
init-network n1
init-snapshot <repo>/networks/s2-triangle s2-triangle
generate-dataplane
```

Observed:
- controller-service: `snapshot <id> partition scheme=WEIGHTED_LPT_FM workers=3 nodes=3` →
  `descriptor shadows on (3 full configs + 3 descriptors)` →
  `snapshot <id> done (3 workers, 307.7 MiB total peak heap)`.
- worker-service 0: `snapshot 1 wrote slices for 1 owned hosts to <dir>/snapshot-<id>`.
- engine: `generate-dataplane` returned `IncrementalBdpAnswerElement` with `status=SUCCESS`.

So the full chain works: **allinone engine → S2 plugin → controller-service → worker-services →
per-host slices → engine serves lazily** (the engine deletes its per-snapshot slice dir on JVM exit).

Script: `scripts/s2-engine-query.sh` (process-level).

**Kubernetes (single-node OrbStack), validated 2026-09-14.** `k8s/pool/` (engine + controller +
worker StatefulSet + shared PVC) was brought up, then `k8s/pool/engine-query.yaml` — a one-off Job
running the engine image (allinone) with `-runclient true -cmdfile /s2/query.txt` and
`-dataplaneengine=s2 -s2controllerhost=s2-controller.s2-pool.svc.cluster.local -s2controllerport=4090
-s2storedataplane=false -s2slicedir=/s2/shared/slices`. The image bakes the snapshot (`networks/`) and
the command file.

Observed:
- controller-service: `snapshot <id> partition scheme=WEIGHTED_LPT_FM workers=3 nodes=3` →
  `descriptor shadows on (3 full configs + 3 descriptors)` →
  `snapshot <id> done (3 workers, 486.4 MiB total peak heap)`.
- worker-0: `wrote slices for 1 owned hosts to /s2/shared/slices/snapshot-<id>` (shared PVC).
- the Job: `Complete`, log ends with `IncrementalBdpAnswerElement ... status=SUCCESS`.

So the same chain works **in-cluster**: engine Job → S2 plugin → controller Service → worker pods →
per-host slices on the PVC → engine serves lazily. (Single-node clusters with only the RWO
`local-path` provisioner need the PVC as `ReadWriteOnce`; multi-node needs `ReadWriteMany`.)

## B.6 — memory

Two questions to answer:

1. **Per-worker memory** with `owned-only` vs full dataplane. The pool's controller log reports the
   workers' combined peak per snapshot (`... done (N workers, X MiB total peak heap)`), so this is a
   matter of running the pool twice (`-Ds2.ownedDataplane=true|false`) per network and comparing.
   Baselines: `M5-SCALE.md` (e.g. `s2-giga` 1938.1 MiB per worker with owned-only + descriptors).
2. **Engine-side laziness**: a node-scoped question should read only the touched hosts' slices; a
   whole-network question materializes the union. Measure the engine's peak heap for
   `routes` on one node vs all nodes with `-s2storedataplane=false`.

### Per-worker peak vs vanilla (upstream `ibdp`) — measured 2026-09-14

The metric that matters for "can a huge network run distributed" is **one worker's peak vs stock
Batfish in one JVM**. `S2Main vanilla <network>` (PR #14) runs the stock `ibdp` engine in a single
JVM (parse + compute) and prints its peak; the S2 per-worker peaks are from the runner/pool
(owned-only, controller-shipped configs). All runs `MATCH`.

| network | prefixes | vanilla `ibdp` (1 JVM) | S2 W=3 max/worker | S2 W=6 max/worker |
| --- | --- | --- | --- | --- |
| `s2-big2` | 640 | 547.6 MiB | 209.5 | 182.7 |
| `s2-mega` | 4096 | 867.1 MiB | 381.5 | 372.2 |
| `s2-giga` | 32768 | 4480.0 MiB | 1625.4 | 1202.8 |
| `s2-giga2` | 65552 | 12182.0 MiB | 4844 (controller 2698) | 3306 |

Per-worker is **60–73% below vanilla** and decreases with W (with a floor set by the per-worker
fixed/global data: all configs + the cluster-wide ARP-replies exchange). `s2-giga4` (131088
prefixes) vanilla peaks at **31796 MiB** in one JVM — beyond a normal node — while S2's per-worker
share is `~(N×P)/W`, i.e. a few GiB.

### Single host vs multiple nodes

On **one** host, S2's total across workers is **≥ vanilla's single-JVM peak** (same total work, plus
the controller and the cross-worker exchanges). So distributing does **not** let a single machine run
a network that vanilla OOMs on: it lowers the **per-process** peak, which only helps when the workers
run on **separate nodes**. Raising OrbStack's memory only repartitions the host's RAM into the single
Linux VM, so it does not change this. The invariant to measure is therefore *per-worker peak*, which
is what the table above shows.

### owned-only vs full (secondary)

The pool now defaults to owned-only (forwarding-exact via the distributed `unownedArpIps` + remote
`arpReplies` exchanges). Compared with full mode on the same pool, per-worker peaks are comparable
(within a few %; one `s2-mega` run had owned lower, route-light/DCN re-runs roughly equal, with the
exchange overhead). The win that matters is per-worker vs vanilla, above.

Engine-side laziness (question-scoped): **structurally proven**, not just measured.
`S2HostSlicesTest#testPointLookupsResolveOnlyOwningHost` uses a slice source that **throws for any
host other than the target** and asserts that a node-scoped access (`getRibs().row(host)`,
`getFibs().get(host)`, the forwarding maps' `get(host)`) succeeds — i.e. it never touches the other
hosts' slices. `S2DataPlanePluginTest#testS2EngineServesFromSliceDirectory` then shows the engine
serving a snapshot from a slice directory matches vanilla. So a node-scoped question reads only the
touched hosts; a whole-network question is the only case that materializes the union. A heap-based
node-scoped-vs-whole-network delta is environment-dependent and left as a follow-up.
