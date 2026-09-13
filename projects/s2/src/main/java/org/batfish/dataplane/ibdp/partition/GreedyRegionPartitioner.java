// SPDX-License-Identifier: Apache-2.0

package org.batfish.dataplane.ibdp.partition;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.Set;

/**
 * Seed k-center followed by least-loaded neighbor region growing (the port plan's WAN candidate,
 * &sect;3.3). It favors locality, which matters when the BGP session graph does not follow L3 links
 * (route reflectors, multi-hop iBGP).
 *
 * <ol>
 *   <li><b>Seeds:</b> farthest-point k-center. The first seed is the heaviest node (ties by
 *       hostname); each next seed maximizes the shortest-path distance to the current seeds (ties
 *       by weight then hostname).
 *   <li><b>Regions:</b> grow each seed's region outward. Nodes are processed by increasing distance
 *       from the seed set; each joins the adjacent region with the least current load (ties by
 *       region index), or, if it has no assigned neighbor yet, the region of its nearest seed.
 * </ol>
 *
 * <p>Deterministic: every comparison has an explicit tie-break and no randomness is used.
 */
public final class GreedyRegionPartitioner implements NodePartitioner {

  @Override
  public Map<String, Integer> partition(CommunicationGraph graph, int numWorkers, long seed) {
    if (numWorkers < 1) {
      throw new IllegalArgumentException("numWorkers must be >= 1");
    }
    List<String> nodes = new ArrayList<>(graph.nodes());
    Map<String, Integer> assignment = new HashMap<>();
    if (nodes.isEmpty()) {
      return assignment;
    }
    if (numWorkers >= nodes.size()) {
      for (int i = 0; i < nodes.size(); i++) {
        assignment.put(nodes.get(i), i);
      }
      return assignment;
    }
    List<String> seeds = pickSeeds(graph, nodes, numWorkers);
    RegionBfs bfs = new RegionBfs(graph, nodes, seeds);
    long[] loads = new long[numWorkers];
    for (int i = 0; i < seeds.size(); i++) {
      assignment.put(seeds.get(i), i);
      loads[i] += graph.weight(seeds.get(i));
    }
    List<String> rest = new ArrayList<>();
    for (String node : nodes) {
      if (!assignment.containsKey(node)) {
        rest.add(node);
      }
    }
    rest.sort(
        Comparator.comparingInt((String h) -> bfs.distance(h))
            .thenComparing(Comparator.comparingInt((String h) -> graph.weight(h)).reversed())
            .thenComparing(Comparator.naturalOrder()));
    for (String node : rest) {
      int region = -1;
      long bestLoad = Long.MAX_VALUE;
      for (Map.Entry<String, Integer> e : graph.neighbors(node).entrySet()) {
        Integer neighborRegion = assignment.get(e.getKey());
        if (neighborRegion == null) {
          continue;
        }
        long load = loads[neighborRegion];
        if (region < 0 || load < bestLoad || (load == bestLoad && neighborRegion < region)) {
          region = neighborRegion;
          bestLoad = load;
        }
      }
      if (region < 0) {
        Integer nearest = bfs.nearestSeed(node);
        if (nearest != null) {
          region = nearest;
        } else {
          // Disconnected component with no seed: put it on the least-loaded worker.
          region = 0;
          for (int w = 1; w < numWorkers; w++) {
            if (loads[w] < loads[region]) {
              region = w;
            }
          }
        }
      }
      assignment.put(node, region);
      loads[region] += graph.weight(node);
    }
    return assignment;
  }

  /** Farthest-point k-center seeds (see the class doc). */
  private static List<String> pickSeeds(CommunicationGraph graph, List<String> nodes, int k) {
    List<String> byWeight = new ArrayList<>(nodes);
    byWeight.sort(
        Comparator.comparingInt((String h) -> graph.weight(h))
            .reversed()
            .thenComparing(Comparator.naturalOrder()));
    List<String> seeds = new ArrayList<>();
    seeds.add(byWeight.get(0));
    while (seeds.size() < k) {
      RegionBfs bfs = new RegionBfs(graph, nodes, seeds);
      String best = null;
      int bestDistance = -1;
      int bestWeight = -1;
      for (String node : nodes) {
        if (seeds.contains(node)) {
          continue;
        }
        int distance = bfs.distance(node);
        // Unreachable nodes (distance -1) sort last.
        if (best == null
            || distance > bestDistance
            || (distance == bestDistance && graph.weight(node) > bestWeight)
            || (distance == bestDistance
                && graph.weight(node) == bestWeight
                && node.compareTo(best) < 0)) {
          best = node;
          bestDistance = distance;
          bestWeight = graph.weight(node);
        }
      }
      seeds.add(best);
    }
    return seeds;
  }

  /**
   * Multi-source BFS from the seeds. {@link #distance} is the shortest-path distance to the nearest
   * seed ({@code -1} if unreachable); {@link #nearestSeed} is the seed index minimizing (distance,
   * seed index).
   */
  private static final class RegionBfs {
    private final Map<String, Integer> _distance = new HashMap<>();
    private final Map<String, Integer> _nearest = new HashMap<>();

    RegionBfs(CommunicationGraph graph, List<String> nodes, List<String> seeds) {
      for (String node : nodes) {
        _distance.put(node, -1);
      }
      // Priority queue over (distance, seed index); unweighted BFS with an explicit tie-break.
      PriorityQueue<long[]> queue =
          new PriorityQueue<>(
              Comparator.<long[]>comparingLong(a -> a[0]).thenComparingLong(a -> a[1]));
      Map<String, Integer> nodeIndex = new HashMap<>();
      for (int i = 0; i < nodes.size(); i++) {
        nodeIndex.put(nodes.get(i), i);
      }
      for (int s = 0; s < seeds.size(); s++) {
        String seed = seeds.get(s);
        _distance.put(seed, 0);
        _nearest.put(seed, s);
        queue.add(new long[] {0, s, nodeIndex.get(seed)});
      }
      Set<String> settled = new HashSet<>();
      while (!queue.isEmpty()) {
        long[] entry = queue.poll();
        int distance = (int) entry[0];
        int seedIndex = (int) entry[1];
        String node = nodes.get((int) entry[2]);
        if (!settled.add(node)) {
          continue;
        }
        _distance.put(node, distance);
        _nearest.put(node, seedIndex);
        for (String neighbor : graph.neighbors(node).keySet()) {
          if (settled.contains(neighbor)) {
            continue;
          }
          queue.add(new long[] {distance + 1L, seedIndex, nodeIndex.get(neighbor)});
        }
      }
    }

    int distance(String node) {
      return _distance.get(node);
    }

    /** The nearest seed index, or {@code null} if {@code node} is unreachable from every seed. */
    Integer nearestSeed(String node) {
      return _nearest.get(node);
    }
  }
}
