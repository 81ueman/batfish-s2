// SPDX-License-Identifier: Apache-2.0
package org.batfish.dataplane.ibdp.partition;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeSet;

/**
 * The paper's expert scheme: hostnames in ascending order, dealt round-robin across workers. On a
 * DCN whose names encode the tier (e.g. {@code core1}, {@code agg2}, {@code edge3}) this keeps
 * tiers spread evenly and is close to METIS without an external solver (port plan &sect;3.3).
 */
public final class NameOrderedPartitioner implements NodePartitioner {

  @Override
  public Map<String, Integer> partition(CommunicationGraph graph, int numWorkers, long seed) {
    if (numWorkers < 1) {
      throw new IllegalArgumentException("numWorkers must be >= 1");
    }
    List<String> sorted = new ArrayList<>(new TreeSet<>(graph.nodes()));
    Map<String, Integer> assignment = new HashMap<>();
    for (int i = 0; i < sorted.size(); i++) {
      assignment.put(sorted.get(i), i % numWorkers);
    }
    return assignment;
  }
}
