// SPDX-License-Identifier: Apache-2.0
package org.batfish.dataplane.ibdp;

/**
 * Cluster-wide coordination for the distributed dataplane computation. Implemented in-JVM by {@link
 * S2Cluster} (milestone 1/2 tests) and remotely by the controller (milestone 3, separate Pods).
 */
public interface S2Coordinator {
  /**
   * Report this worker's local convergence state for the current round and return the global result
   * (true if any worker still has changes). Blocks until every worker has reported the same round.
   */
  boolean roundCheck(boolean localDirty);

  /**
   * Exchange a per-worker value for the current computation round and return the sum across all
   * workers. Used to make oscillation detection and schedule selection global: workers that only
   * see their own switches would otherwise pick different schedules and desynchronize.
   */
  int sumAll(int localValue);
}
