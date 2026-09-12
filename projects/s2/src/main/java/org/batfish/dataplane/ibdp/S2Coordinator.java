package org.batfish.dataplane.ibdp;

/**
 * Decides whether the whole cluster has reached a routing fixed point. Implemented in-JVM by {@link
 * S2Cluster} (milestone 1/2 tests) and remotely by the controller (milestone 3, separate Pods).
 */
public interface S2Coordinator {
  /**
   * Report this worker's local convergence state for the current round and return the global result
   * (true if any worker still has changes). Blocks until every worker has reported the same round.
   */
  boolean roundCheck(boolean localDirty);
}
