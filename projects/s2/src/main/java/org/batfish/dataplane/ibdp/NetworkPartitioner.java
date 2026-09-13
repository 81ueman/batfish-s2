// SPDX-License-Identifier: Apache-2.0
package org.batfish.dataplane.ibdp;

import java.util.Map;
import java.util.Set;
import org.batfish.dataplane.ibdp.partition.RandomPartitioner;

/**
 * Assigns switches (hostnames) to workers.
 *
 * <p>This is the historical default partitioner: a deterministic balanced round-robin over a
 * seed-derived hash shuffle. It is kept for source compatibility and is now a thin delegate to
 * {@link RandomPartitioner}, the {@code RANDOM} scheme of the pluggable partitioner in {@code
 * org.batfish.dataplane.ibdp.partition}. New code should select a scheme with {@code
 * -Ds2.partition=<scheme>} and let the controller compute and ship the assignment.
 */
public final class NetworkPartitioner {

  private static final RandomPartitioner RANDOM = new RandomPartitioner();

  private NetworkPartitioner() {}

  /**
   * @param hostnames all switch hostnames in the snapshot
   * @param numWorkers number of workers (>= 1)
   * @param seed deterministic shuffle seed
   * @return hostname -&gt; worker index, with the load as balanced as possible
   */
  public static Map<String, Integer> partition(Set<String> hostnames, int numWorkers, long seed) {
    return RANDOM.partition(hostnames, numWorkers, seed);
  }
}
