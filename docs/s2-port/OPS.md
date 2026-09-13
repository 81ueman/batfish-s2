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
JAVA_TOOL_OPTIONS="-Xmx4g -Ds2.ownedDataplane=true" scripts/local-demo.sh 3 s2-mega
scripts/bench.sh "3" "s2-mega s2-giga"          # honours JAVA_TOOL_OPTIONS too
```

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
* The heap is set by the `JAVA_TOOL_OPTIONS: "-Xmx4g"` env in both base manifests, so it is
  visible and overridable (`kubectl set env` / `kubectl edit` / an overlay patch). The JVM would
  otherwise derive its max heap from the limit (~1.5 GiB at 6Gi), which is too small for
  `s2-giga`.
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

## CI demo matrix (O3)

`scripts/ci-matrix.sh` runs the full tie-stable demo matrix
(`s2-triangle s2-line s2-ospf s2-ospf-bgp s2-redist s2-agg s2-static s2-external`) at 3 workers
in both default and `-Ds2.ownedDataplane=true` modes, and prints a pass/fail summary grepped from
the `MATCH` result. It is **opt-in** because it is slow:

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

Current status: `gpmetis` is **not installed** in the evaluation environment, which is why the
`METIS` scheme in `PARTITIONING-PLAN.md` §3.3 is listed as "external, fall back to a pure-Java
scheme when absent". `scripts/partition-metrics.py` itself has no METIS dependency. If METIS
cannot be installed, use `WEIGHTED_LPT_FM`/`GREEDY_REGION` (pure Java) for the comparison and
record the substitution in the evaluation write-up. The container image does **not** currently
bake in METIS; if the evaluation is to run on Kubernetes, add `metis` to `docker/Dockerfile.s2`.
