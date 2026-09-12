package org.batfish.dataplane.ibdp;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Assigns switches (hostnames) to workers.
 *
 * <p>Milestone 1 uses a balanced random-ish assignment. Later milestones can add the expert/METIS
 * schemes from the paper. Deterministic for a fixed seed.
 */
public final class NetworkPartitioner {

  private NetworkPartitioner() {}

  /**
   * @param hostnames all switch hostnames in the snapshot
   * @param numWorkers number of workers (>= 1)
   * @param seed deterministic shuffle seed
   * @return hostname -&gt; worker index, with the load as balanced as possible
   */
  public static Map<String, Integer> partition(Set<String> hostnames, int numWorkers, long seed) {
    if (numWorkers < 1) {
      throw new IllegalArgumentException("numWorkers must be >= 1");
    }
    List<String> sorted = new ArrayList<>(hostnames);
    // Deterministic shuffle: sort by a hash derived from the seed so results are reproducible.
    sorted.sort(Comparator.comparingLong(h -> mix(seed, h)));
    Map<String, Integer> assignment = new HashMap<>();
    for (int i = 0; i < sorted.size(); i++) {
      assignment.put(sorted.get(i), i % numWorkers);
    }
    return assignment;
  }

  private static long mix(long seed, String hostname) {
    long h = seed * 0x9E3779B97F4A7C15L + hostname.hashCode();
    h ^= (h >>> 33);
    h *= 0xFF51AFD7ED558CCDL;
    h ^= (h >>> 33);
    return h;
  }
}
