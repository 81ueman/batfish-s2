# S2 × Kubernetes: query flow

How a normal Batfish query is served by the S2 distributed engine on Kubernetes. **Control plane**
(the BGP/OSPF fixpoint) is live RPC between the engine and the worker pool; the **data plane**
(per-host RIB/FIB/forwarding slices) is written to shared storage and read lazily, so the global
data plane never has to fit in one JVM.

```mermaid
flowchart TD
  user(["User / pybatfish"]) -->|"init_snapshot / question"| bf["Batfish coordinator<br/>(engine JVM)"]
  bf -->|"computeDataPlane(snapshot)"| plugin["S2DataPlanePlugin<br/>(engine = s2)"]

  subgraph K8S["Kubernetes"]
    direction TB
    svc["headless Service<br/>s2-workers"]
    pool["StatefulSet s2-workers<br/>(N worker Pods)"]
    pvc[("Shared PVC<br/>snapshot + per-host slices")]
    svc -. "DNS discovery" .-> pool
  end

  plugin -->|"1. discover pool (DNS) → N"| svc
  plugin -->|"2. partition nodes + ship configs"| pool
  plugin <-->|"3. control-plane fixpoint (RPC barriers)"| pool
  pool -->|"4. write owned per-host slices"| pvc
  plugin -->|"5. return lazy global DataPlane"| bf

  bf --> q{"Stock question engine"}
  q -->|"node-scoped question"| lazy["S2LazyDataPlane<br/>fetch touched hosts only"]
  lazy -->|"lazy per-host read"| pvc
  q -->|"whole-network question"| mat["materialize union<br/>or distributed BDD"]

  classDef k8s fill:#e8f0fe,stroke:#4285f4,color:#111;
  classDef ctrl fill:#fff4e5,stroke:#ff9800,color:#111;
  classDef data fill:#e6f4ea,stroke:#34a853,color:#111;
  class svc,pool k8s;
  class plugin ctrl;
  class pvc data;
```

- **Control plane = live RPC.** The fixpoint reuses the runner's controller/sidecar protocol
  (`S2ControlMessages` / `S2ControllerServer` / `S2SidecarServer`); the in-process `S2Cluster`
  barriers become a network `S2Coordinator`.
- **Data plane = per-host slices.** Each worker writes its owned nodes' slices to the shared PVC
  (one block per host, the same granularity as `PerHostDataPlane`); `S2LazyDataPlane` reads a host on
  demand (LRU).
- **Scaling.** `kubectl scale statefulset/s2-workers --replicas=N`; the engine uses
  `N = min(pool size, node count)` (`s2workers=0` = auto).
- **Failure/consistency.** A worker dying mid-fixpoint fails that run (retry); slices are keyed by
  snapshot id, so a version is pinned.
