# S2 — remaining work after M1

## Done (M1)

`//projects/s2:s2_tests` proves the distributed control plane:

* partition 3 switches across 1 and 3 logical workers
* one Batfish engine per worker; shadows delegate to the owning worker's real
  node
* global (controller-level) convergence check via `S2Cluster`
* assert each real node's main RIB equals vanilla Batfish's RIB

Both `workers=1` and `workers=3` match. Core hooks are limited to `Node`,
`IncrementalBdpEngine`, `BgpRoutingProcess` (see `README.md`).

## Next: M2 — separate processes + gRPC sidecar

Currently a shadow node delegates by holding the real node's `VirtualRouter` in
the same JVM. M2 replaces that delegation with an RPC:

1. Controller parses configs, partitions, and sends each worker its assignment.
2. Worker builds `DistributedNode`s; shadow nodes get an RPC-backed
   `BgpRoutingProcess` that implements `getOutgoingRoutesForEdge` by calling the
   owning worker's sidecar.
3. Cross-worker gRPC channel (Batfish already added `grpc_maven` in
   `MODULE.bazel`). Serialize the edge id + hostname/vrf request; return the
   advertisement stream (Java serialization initially).
4. Global convergence stays barrier-based but crosses processes (controller
   coordinates rounds).

Open questions: serialization format for `RouteAdvertisement<Bgpv4Route>`
(Java serialization vs JSON), and how the controller learns "all workers
converged" (a round RPC is the simplest).

## After M2: M3 — Kubernetes

* `docker/Dockerfile.s2` builds controller/worker image from the runner jar.
* `k8s/overlays/{1pod,3pod}` run the same snapshot with 1 vs 3 worker Pods.
* `scripts/k8s-demo.sh N` + `scripts/compare-answers.sh` diff controller output.

## Not yet implemented (later milestones)

* Distributed data-plane verification (BDD forwarding across shadow ports):
  M1/M2 compare control-plane RIBs/FIBs, which is S2's core invariant. Full
  reachability across partitions is a later step.
* OSPF / EVPN / prefix sharding / METIS partitioner / oscillation handling.
