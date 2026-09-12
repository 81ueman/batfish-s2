# S2 on Kubernetes — architecture

```
                 +------------------------------+
                 |  Job/s2-orchestrate          |
                 |  scripts/orchestrate.sh      |
                 +--------------+---------------+
                                | TCP text commands
              +-----------------+------------------+
              |                                    |
   +----------v-----------+          +-------------v-------------+
   | Deployment           |          | StatefulSet               |
   |   s2-controller:4090 |          |   s2-worker-0..N:4091     |
   | Parser + Partitioner |          | real/shadow Node models   |
   | CPO + DPO + Sidecar  |<-------->| Sidecar (gRPC)            |
   +----------------------+   gRPC   +---------------------------+
                                |
                     (workers also talk to each other
                      directly over gRPC for route/packet
                      exchange through shadow nodes)
```

## Why splitting the model is transparent

For each switch, one worker hosts the **real** node; every other worker hosts a
**shadow** node. A shadow node has the same interface as a real node but relays
calls to the real node over the sidecar. Switch models therefore never observe
placement, and the distributed fixpoint computes the same RIBs/FIBs as the
single-machine Batfish engine.

## What changes with N

Only the partition/assignment of switches to workers changes. The verified
property (e.g. all-pairs reachability) and the resulting forwarding state must
not. The 1-Pod vs 3-Pod diff is exactly this invariant.

## Protocol (from the S2 reference)

Worker: `router <controller-ip> <worker-ip> <worker-id> shard <n>`
Controller: `controller <network> partition-scheme <scheme> shard <n>
reachability <n> <worker-ip> <worker-id> ...`

`S2Runner` accepts a second connection to `stop` and report peak memory.
