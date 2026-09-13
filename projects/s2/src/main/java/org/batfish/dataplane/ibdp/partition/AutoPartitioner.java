// SPDX-License-Identifier: Apache-2.0
package org.batfish.dataplane.ibdp.partition;

import java.util.Map;

/**
 * The {@link PartitionScheme#AUTO} partitioner: classify the graph with {@link AutoSchemeSelector}
 * and delegate to the selected concrete scheme. Used when a caller wants a {@link NodePartitioner}
 * for the {@code AUTO} scheme directly (the {@code S2Main} controller resolves once via {@link
 * AutoSchemeSelector#select} so it can log the selection and use the concrete scheme).
 */
public final class AutoPartitioner implements NodePartitioner {

  @Override
  public Map<String, Integer> partition(CommunicationGraph graph, int numWorkers, long seed) {
    return AutoSchemeSelector.select(graph).partitioner().partition(graph, numWorkers, seed);
  }
}
