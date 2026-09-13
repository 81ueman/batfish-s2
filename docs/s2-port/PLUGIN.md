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
```

* **Default is unchanged.** `dataplaneengine` defaults to `ibdp`, so stock Batfish never runs S2.
* **Same input/output.** The plugin builds the same topology context as `IncrementalDataPlanePlugin`,
  partitions the snapshot across `N` workers (owned nodes real, the rest shadowed), runs them
  concurrently, and returns a global `DataPlane` the normal question engine consumes.
* **No S2 API.** Questions are answered by the stock answerers; pybatfish/REST are untouched.
  `S2RoutesQuestionTest` asserts the stock `RoutesAnswerer`/`RoutesQuestion` answer identically to
  `ibdp` with `-dataplaneengine=s2 -s2workers=3`.
* **Lazy global data plane.** `S2LazyDataPlane` unions the per-worker owned data planes lazily: a
  point/row lookup (a node-scoped question) is served from the owning worker; only whole-network
  iteration materializes the union. See "Scaling out" below.
* **No verify step.** The `S2Main verify` role (and `scripts/*`) is a runner/CI concern; the engine
  path does not need it.

## Settings

| Setting | Default | Meaning |
| --- | --- | --- |
| `dataplaneengine` | `ibdp` | Selects the engine; set to `s2` to use S2. |
| `s2workers` | `1` | Number of S2 workers (partition shards) per data plane computation. |

`Settings.getS2Workers()` / `setS2Workers(int)`; CLI `-s2workers`, or `s2workers=` in
`batfish.properties`.

## Scaling out (memory)

The engine currently runs the `N` workers **in-process** (threads). That parallelizes CPU and bounds
the *container* memory (the lazy views avoid materializing a second copy), but the node data still
lives in one JVM, so it does not by itself make a topology that does not fit one JVM runnable.

The plan (see `REMAINING.md`) is to make the lazy views fetch a host's slice from its **owner over
the network**, with the workers running as a remote pool (Kubernetes) and the engine picking `N` from
the pool. Then a node-scoped question only pulls the nodes it touches, and a whole-network question
is executed across the owners (the paper's distributed DPV, which the runner already does for BDD
reachability). The `S2LazyDataPlane` seam is where that remote source plugs in.

## Remaining work

1. **Remote worker pool (Kubernetes).** Back `S2LazyDataPlane`'s per-host access with a fetch from
   the owning worker; manage the pool (scale, health) as deployment state, not a user flag. `N`
   defaults to the pool size.
2. **Auto worker count.** Default `s2workers` to a sensible value (e.g. pool size, else
   `availableProcessors` capped by node count) so no flag is needed.
3. **Protocol coverage.** EIGRP / IS-IS / RIP are not distributed by the runner; fall back to the
   single-JVM engine for those snapshots (or implement them) so the engine is a true drop-in.
4. **Storage.** `Batfish.saveDataPlane` materializes per host and writes each host separately (already
   per-host granularity); confirm it stays bounded with the lazy views and, if needed, bypass it for
   the remote case.
5. **Launcher.** A thin `s2-batfish` wrapper that only sets `dataplaneengine=s2` (+ pool settings).

## Status

Done: engine registration/selection, `-s2workers` setting, distributed compute + lazy global data
plane, stock-question equivalence. Next: remote pool (Kubernetes), auto count, protocol fallback.
