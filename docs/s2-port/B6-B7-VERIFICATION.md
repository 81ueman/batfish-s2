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

Script: `scripts/s2-engine-query.sh` (process-level). The Kubernetes counterpart is `k8s/pool/`
(engine Deployment + controller + worker StatefulSet + shared PVC), brought up on OrbStack: the
controller registered all 3 workers via the headless Service and the engine came up healthy; driving
a question there needs a client (pybatfish or a command-file Job) against the `s2-engine` Service.

## B.6 — memory

Two questions to answer:

1. **Per-worker memory** with `owned-only` vs full dataplane. The pool's controller log reports the
   workers' combined peak per snapshot (`... done (N workers, X MiB total peak heap)`), so this is a
   matter of running the pool twice (`-Ds2.ownedDataplane=true|false`) per network and comparing.
   Baselines: `M5-SCALE.md` (e.g. `s2-giga` 1938.1 MiB per worker with owned-only + descriptors).
2. **Engine-side laziness**: a node-scoped question should read only the touched hosts' slices; a
   whole-network question materializes the union. Measure the engine's peak heap for
   `routes` on one node vs all nodes with `-s2storedataplane=false`.

Pending: the owned-only *forwarding-exact* fix (distributed `unownedArpIps`) must land before the
owned-vs-full comparison is meaningful, since the pool currently defaults to the full dataplane
(a remote shadow's stub FIB otherwise changes owned nodes' forwarding analysis).
