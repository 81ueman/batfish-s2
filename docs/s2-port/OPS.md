# S2 operations: resources, heap, and the evaluation tooling

Operational defaults for running the distributed S2 runner (local or Kubernetes), and how to
install the tools the partition evaluation needs. Companion to `M5-SCALE.md` (measurements),
`PARTITIONING-PLAN.md` (§6 evaluation), and `REMAINING.md` (ops items O2/O3/O5/O6).

## Default heap (`-Xmx`)

The runner is spawned as one JVM per process (controller + `N` workers). Set the heap via the
standard `JAVA_TOOL_OPTIONS` environment variable:

```sh
JAVA_TOOL_OPTIONS=-Xmx4g scripts/local-demo.sh 3 s2-giga
```

Recommended values, from the measured per-worker peaks in `M5-SCALE.md` (3 workers):

| snapshot | prefixes | worst-worker peak (`-Xmx4g`) | `-Xmx` |
| --- | --- | --- | --- |
| demo matrix (`s2-triangle` … `s2-redist`, `s2-agg`, `s2-static`, `s2-external`) | < 100 | < 250 MiB | `2g` |
| `s2-big2` | 640 | 164.7 MiB | `2g` |
| `s2-mega` | 4096 | 436.4 MiB (owned 301.1) | `2g`–`4g` |
| `s2-giga` | 32768 | 2226.1 MiB (owned 1938.1; parse path 2513.1) | **`4g`** |

* **Default: `-Xmx4g`.** It covers the largest verified snapshot (`s2-giga`) with headroom. The
  measured peak there is ~2.5 GiB, so 4g leaves room for non-heap (metaspace, code cache,
  thread stacks) and GC churn.
* Use `-Xmx2g` for the demo matrix and anything up to a few hundred MiB; the smaller cap lowers
  the *observed* peak (the metric is used-heap peak) by forcing GC earlier, e.g. `s2-mega` at
  `-Xmx1g` measured 595.4 MiB with prefix sharding (`M5-SCALE.md`).
* The peak is transient control-plane/FIB allocation, not retained data, so raising `-Xmx` from
  2g to 4g does not change the retained floor — it only gives transient headroom.

The local scripts forward `JAVA_TOOL_OPTIONS` to every JVM:

```sh
JAVA_TOOL_OPTIONS="-Xmx4g" scripts/local-demo.sh 3 s2-mega
scripts/bench.sh "3" "s2-mega s2-giga"          # honours JAVA_TOOL_OPTIONS too
```

## S2 memory-feature defaults (O1)

The S2 memory features are **on by default** as of 2026-09-13. They are S2-only (read in
`S2BdpEngine`/`S2Main`) or, for the memo, set by the runner, so **stock Batfish is
unaffected**. Each has a documented `=false` override:

| feature | default | disable | notes |
| --- | --- | --- | --- |
| owned-only dataplane | **on** | `-Ds2.ownedDataplane=false` | `S2BdpEngine`/`S2Main` (S2-only). Remote nodes get stub FIBs; a run automatically falls back to full RIBs/FIBs for tracks / VNI / IPsec / tunnel. |
| descriptor shadows | **on** | `-Ds2.descriptorShadows=false` | `S2Main` (S2-only), multi-worker only, and gated by `descriptorShadowsSafe` (same tracks / VNI / tunnel / IPsec fallback: the controller then ships full configs). |
| positive-only `PrefixSpace` memo | off in shared code, **on in the runner** | `-Ds2.prefixSpacePositiveCacheOnly=false` | pure memoization, never changes results; `scripts/local-demo.sh` exports it and the k8s worker manifest carries it in `JAVA_TOOL_OPTIONS`. |
| node→worker partitioner | **`auto` in the runner**, `RANDOM` in shared code | `-Ds2.partition=RANDOM` (or any scheme) | `AUTO` classifies DCN/WAN and picks METIS when `gpmetis` is installed, else NAME_ORDERED / WEIGHTED_LPT_FM; the controller logs the selection (`scheme=METIS (requested=AUTO, shape=...)`). A later user `-D` overrides the runner default. |
| prefix sharding | off (`S2_PREFIX_SHARDS` unset) | — | unchanged. |

Two node-weight model flags are also available (both gated off / evaluation-only; the partitioner
is unaffected unless used): `-Ds2.nodeWeightsRoleScale=true` enables the O6-residual adaptive
role-level peer scaling (improves `s2-fat4` W=3 cost imbalance 1.061→1.047, other testbeds
unchanged; `PARTITIONING-PLAN.md` §6.10), and `-Ds2.nodeWeightsPeerScale=<int>` pins the BGP peer
coefficient for calibration sweeps.

The runner flag is prepended to any existing `JAVA_TOOL_OPTIONS`, so a later user `-D` wins:

```sh
# default: owned + descriptor shadows + positive cache
scripts/local-demo.sh 3 s2-mega
# restore the pre-O1 full dataplane / full remote configs
JAVA_TOOL_OPTIONS="-Ds2.ownedDataplane=false -Ds2.descriptorShadows=false" \
  scripts/local-demo.sh 3 s2-ospf-bgp
JAVA_TOOL_OPTIONS="-Ds2.prefixSpacePositiveCacheOnly=false" scripts/local-demo.sh 3 s2-triangle
```

**Verification implication.** With owned or descriptor shadows on, a worker holds no complete
remote FIB (stub FIBs / reduced configs), so it cannot run the global per-worker traceroute
digest. The controller therefore does not require a worker digest; it asserts forwarding
equality from the **exact RIB match** (each worker returns its owned nodes' complete final
main RIBs, checked against vanilla) plus the **distributed symbolic reachability**
comparison and the public-API **answer** check (`ribs=MATCH symbolic=MATCH answer=MATCH`).
The digest check returns when both are disabled with the `=false` flags above (C3).

## Kubernetes resource requests/limits

`k8s/base/{controller,worker}.yaml` set the defaults; `k8s/overlays/{1,3}pod` only change the
worker replica count and `WORKERS`.

| container | request | limit | heap | why |
| --- | --- | --- | --- | --- |
| `worker` | `memory: 2Gi`, `cpu: 1` | `memory: 6Gi` | `-Xmx4g` | measured worst-worker peak 2.5 GiB at `s2-giga`; limit = heap + ~2 GiB non-heap/GC headroom |
| `controller` | `memory: 2Gi`, `cpu: 1` | `memory: 6Gi` | `-Xmx4g` | also builds the vanilla dataplane and the reference reachability analysis, so it uses the worker heap budget |

Notes:

* The request (2Gi) reflects a typical steady working set; the limit (6Gi) is what prevents an
  OOM-kill on the large snapshots. CPU requests are 1 because the run is mostly single-threaded
  per phase (the fixpoint barriers serialize workers).
* The heap is set by the `JAVA_TOOL_OPTIONS` env in both base manifests, so it is visible and
  overridable (`kubectl set env` / `kubectl edit` / an overlay patch). The worker value also
  carries the O1 runner default `-Ds2.prefixSpacePositiveCacheOnly=true`; both manifests carry the
  partitioner runner default `-Ds2.partition=auto` (append `-Ds2.prefixSpacePositiveCacheOnly=false`
  or `-Ds2.partition=<scheme>` to disable / pin). The JVM would otherwise derive its
  max heap from the limit (~1.5 GiB at 6Gi), which is too small for `s2-giga`.
* The entrypoints still pass `-XX:-UseCompressedOops` (as measured). Do not remove it when
  comparing against the `M5-SCALE.md` numbers.
* `scripts/k8s-demo.sh <1|3> [network]` renders the overlays and substitutes the snapshot name;
  `scripts/compare-answers.sh [network]` asserts the 1-Pod and 3-Pod results both MATCH.

## Boundary RPC counters (P0)

The sidecars count RPCs and wire bytes process-wide and print one summary line to **stderr** per
worker JVM when the route sidecar closes (end of the run):

```
S2 rpc-stats pid=12345 route.served=... served.reqBytes=... served.respBytes=... \
  served.boundary=... served.boundaryEdges=... route.sent=... sent.reqBytes=... \
  sent.respBytes=... sent.boundary=... sent.boundaryEdges=... \
  bdd.received=... received.reqBytes=... received.respBytes=... \
  bdd.sent=... sent.reqBytes=... sent.respBytes=...
```

* `route.*` is the BGP/OSPF/main-RIB/boundary sidecar (`S2SidecarServer`/`S2SidecarClient`);
  `bdd.*` is the symbolic `S2BddSidecar`.
* `served` = RPCs this worker answered; `sent` = RPCs this worker initiated (pulled from peers).
  Bytes are counted at the socket stream (one request and one response per connection), so they
  are exact and include the Java serialization stream header.
* `boundary`/`boundaryEdges` isolate the cross-worker `BoundaryEdgesRequest` — the symbolic
  boundary pull that the partitioner is meant to minimize.
* Disable with `-Ds2.rpcStats=false`. Each worker writes its own summary to its own
  `worker-<i>.log` (`scripts/local-demo.sh`) so the per-worker boundary cost is directly visible.

## Partition-quality metrics (P0)

`scripts/partition-metrics.py` reports node weights and the weighted cut for a snapshot and an
assignment (default: a faithful Python port of `NetworkPartitioner`, seed 0):

```sh
scripts/partition-metrics.py networks/s2-triangle
scripts/partition-metrics.py --workers 3 --seed 0 networks/s2-fat4
scripts/partition-metrics.py --workers 3 --assignment assign.txt networks/s2-line
```

See the module docstring for the assignment-file format and `--edge-weights`. Dependency-free
(stdlib only); it does not need METIS.

## Benchmark table generation (O4)

`scripts/bench-table.sh` runs a parameterized size ladder x mode(s) through `scripts/bench.sh`
and emits the markdown metrics table used in `M5-SCALE.md`:

| network | prefixes | mode | max peak MiB | controller MiB | engine s (w0) | wall s |

Each cell is `(network, workers, mode, S2_PREFIX_SHARDS)` and is cached in
`results/bench-table.cache.tsv`, so the table can be regenerated **incrementally**: a re-run skips
cells it has already measured (cached cells whose result was not `MATCH` are retried unless
`--keep-failures`). `--list` prints the plan — every cell with its cache state — without running
anything.

```sh
# preview the default ladder (s2-big-bgp s2-big2 s2-huge s2-mega s2-giga) in default+owned
scripts/bench-table.sh --list

# a couple of small cells (this is the acceptance demo)
S2_BASE_PORT=19000 scripts/bench-table.sh \
  --ladder "s2-triangle s2-line" --modes "default" --workers 3

# the full ladder: forward the heap, or sweep shard counts
JAVA_TOOL_OPTIONS=-Xmx4g scripts/bench-table.sh --workers 3 --modes "default full"
scripts/bench-table.sh --workers 3 --shards "1 8" --modes "default"
```

The base `JAVA_TOOL_OPTIONS` and the per-cell `S2_PREFIX_SHARDS` are forwarded to every
`bench.sh`/`local-demo.sh` run; a cell whose shard count is not 1 is labelled `<mode>+B<n>` in the
`mode` column. `prefixes` is the origination (loopback) count, auto-detected from
`networks/<net>/configs` and overridable with `--ladder "s2-big2:640 ..."`. Raw per-cell
`bench.sh` output is kept under `results/bench-table-logs/`. Sample output from the 2-cell run:

```markdown
| network | prefixes | mode | max peak MiB | controller MiB | engine s (w0) | wall s |
| --- | --- | --- | --- | --- | --- | --- |
| s2-triangle | 3 | default | 141.4 | 248.0 | 3.3 | 8 |
| s2-line | 6 | default | 140.0 | 266.3 | 3.3 | 8 |
```

Numbers are host- and JVM-dependent; use them for relative comparisons, not as absolutes.

## CI entry point (O3)

`scripts/ci.sh` is the conservative CI entry point. By default it runs only the unit tests; the
demo matrix is opt-in:

```sh
scripts/ci.sh                              # bazel test //projects/s2:s2_tests
scripts/ci.sh --list                       # print the plan, run nothing
scripts/ci.sh --matrix                     # unit tests, then the demo matrix
scripts/ci.sh --matrix --workers 3 --networks "s2-triangle s2-ospf"
S2_CI_MATRIX=1 scripts/ci.sh               # same as --matrix (still runs the unit tests)
```

`.github/workflows/s2-ci.yml` wraps it as a **manual-only** (`workflow_dispatch`) workflow: the
default run does the unit tests, and ticking `run_matrix` (plus an optional worker count) adds the
demo matrix on a second job. It never runs on push/PR.

The matrix itself is `scripts/ci-matrix.sh`: it runs the full tie-stable demo matrix
(`s2-triangle s2-line s2-ospf s2-ospf-bgp s2-redist s2-agg s2-static s2-external`) at 3 workers
in both the new default mode (O1: owned + descriptor shadows) and the pre-O1 `full` mode
(`-Ds2.ownedDataplane=false -Ds2.descriptorShadows=false`), and prints a pass/fail summary grepped
from the `MATCH` result. It is **opt-in** because it is slow:

```sh
scripts/ci-matrix.sh --run                 # run the matrix
scripts/ci-matrix.sh --list                # print the matrix, run nothing
scripts/ci-matrix.sh --run --workers 3 --networks "s2-triangle s2-ospf"
S2_CI_MATRIX=1 scripts/ci-matrix.sh        # same as --run
```

(`s2-mega`/`s2-giga` are intentionally excluded: they need `-Xmx4g` and are covered by the
benchmark commands in `M5-SCALE.md`.)

## METIS (O5)

The `METIS` partitioner is an external binary, so it is not needed to build or run S2 today; it
is needed to evaluate the paper's partitioner (P2/P4). Install it where `gpmetis` is on `PATH`:

```sh
# macOS
brew install metis
# Debian/Ubuntu
sudo apt-get install metis
```

Verify and locate it:

```sh
gpmetis -version        # or: command -v gpmetis
```

Current status: `gpmetis` (METIS 5.1.0) **is installed** in the evaluation environment, so the
`METIS` scheme in `PARTITIONING-PLAN.md` is evaluated for real (§6.7 there). `MetisPartitioner`
passes `-ptype=rb -ufactor=1` and still falls back to a pure-Java scheme when the binary is absent,
so no build or stock run depends on it. `scripts/partition-metrics.py` itself has no METIS
dependency. Where METIS cannot be installed, use `WEIGHTED_LPT_FM`/`GREEDY_REGION` (pure Java) for
the comparison and record the substitution in the evaluation write-up. The container image does
**not** currently bake in METIS; if the evaluation is to run on Kubernetes, add `metis` to
`docker/Dockerfile.s2`.
