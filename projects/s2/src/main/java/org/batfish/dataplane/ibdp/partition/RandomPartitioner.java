// SPDX-License-Identifier: Apache-2.0

package org.batfish.dataplane.ibdp.partition;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * The historical S2 partitioner: sort the hostnames by a seed-derived 64-bit hash and deal them
 * round-robin. It ignores topology and weights, so it balances the node <em>count</em> but produces
 * the worst possible cut. Kept as the default so stock demos and the {@code partition-metrics.py}
 * baseline are byte-for-byte unchanged.
 *
 * <p>This is the single source of truth for the algorithm; {@code NetworkPartitioner} delegates to
 * it and {@code scripts/partition-metrics.py} mirrors it.
 */
public final class RandomPartitioner implements NodePartitioner {

  /** Golden-ratio odd multiplier from {@code NetworkPartitioner.mix}. */
  private static final long GOLDEN = 0x9E3779B97F4A7C15L;

  /** Murmur-style finalizer multiplier from {@code NetworkPartitioner.mix}. */
  private static final long FINALIZER = 0xFF51AFD7ED558CCDL;

  @Override
  public Map<String, Integer> partition(CommunicationGraph graph, int numWorkers, long seed) {
    return partition(graph.nodes(), numWorkers, seed);
  }

  /**
   * The raw algorithm over a hostname collection, so callers that do not have a graph (the legacy
   * {@code NetworkPartitioner.partition} signature, and its Python port) can reproduce it exactly.
   */
  public Map<String, Integer> partition(Iterable<String> hostnames, int numWorkers, long seed) {
    if (numWorkers < 1) {
      throw new IllegalArgumentException("numWorkers must be >= 1");
    }
    List<String> sorted = new ArrayList<>();
    hostnames.forEach(sorted::add);
    // Deterministic shuffle: sort by a hash derived from the seed so results are reproducible.
    sorted.sort(Comparator.comparingLong(h -> mix(seed, h)));
    Map<String, Integer> assignment = new HashMap<>();
    for (int i = 0; i < sorted.size(); i++) {
      assignment.put(sorted.get(i), i % numWorkers);
    }
    return assignment;
  }

  /** Seed-mixed 64-bit hash of a hostname (matches {@code NetworkPartitioner.mix}). */
  static long mix(long seed, String hostname) {
    long h = seed * GOLDEN + hostname.hashCode();
    h ^= (h >>> 33);
    h *= FINALIZER;
    h ^= (h >>> 33);
    return h;
  }
}
