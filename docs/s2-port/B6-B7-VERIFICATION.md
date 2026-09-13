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

### Worker memory: owned-only vs full (measured 2026-09-14)

`task/s2-dpv` made owned-only forwarding-exact, so the pool now defaults to it. The controller
reports the workers' combined peak per snapshot. On `s2-mega` (16 nodes / 4096 prefixes) with 3
worker-services:

| mode | ribs | forwarding | total peak (3 workers) |
| --- | --- | --- | --- |
| owned-only (now default) | MATCH | MATCH | **1054.7 MiB** |
| full (`-Ds2.ownedDataplane=false`) | MATCH | MATCH | 1125.6 MiB |

Owned-only is forwarding-exact (distributed `unownedArpIps` + remote `arpReplies`) and no higher
than full here (~6% lower). The gap grows with how much of a worker's materialized FIB/ARP state
belongs to nodes it does not own (denser / role-diverse fabrics).

Engine-side laziness (question-scoped) is still to be measured.
