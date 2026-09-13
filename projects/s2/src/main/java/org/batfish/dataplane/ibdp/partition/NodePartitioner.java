// SPDX-License-Identifier: Apache-2.0

package org.batfish.dataplane.ibdp.partition;

import java.util.Map;

/**
 * Assigns each router (hostname) of a snapshot to a worker.
 *
 * <p>Every scheme is deterministic for a fixed {@code seed}: the controller computes the assignment
 * exactly once and ships it to the workers, which never recompute it (see {@code S2Main} and {@code
 * S2ControlMessages.Start}). Schemes must therefore not depend on any worker-local state, and a
 * scheme that uses randomness must confine it to the controller.
 *
 * @see PartitionScheme
 */
@FunctionalInterface
public interface NodePartitioner {

  /**
   * Partition {@code graph}'s nodes across {@code numWorkers} workers.
   *
   * @param graph the union communication graph, including per-node weights and per-edge estimated
   *     exchange costs
   * @param numWorkers number of workers, {@code >= 1}
   * @param seed deterministic seed; schemes that use randomness must derive all randomness from it
   * @return hostname -&gt; worker index in {@code [0, numWorkers)}
   */
  Map<String, Integer> partition(CommunicationGraph graph, int numWorkers, long seed);
}
