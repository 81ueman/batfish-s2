// SPDX-License-Identifier: Apache-2.0

package org.batfish.dataplane.ibdp.partition;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Weight-descending LPT followed by a bounded FM/KL-style refinement under a load cap, the port
 * plan's general-purpose default candidate (&sect;3.3).
 *
 * <p>The paper's objective is balance first and cut second (memory is the bottleneck), so this
 * scheme:
 *
 * <ol>
 *   <li>assigns nodes largest-weight-first to the currently lightest worker (LPT / list
 *       scheduling);
 *   <li>then reduces the cut with single-node moves and pairwise swaps that never push a target
 *       worker above a load cap.
 * </ol>
 *
 * <p>The cap is {@code max(maxNodeWeight, ceil(LOAD_CAP_FACTOR * totalWeight / numWorkers))}. The
 * {@code maxNodeWeight} floor keeps the problem feasible when one router is heavier than an even
 * share; the {@code LOAD_CAP_FACTOR} (10%) leaves just enough slack for cut-improving moves without
 * letting balance degrade. Refinement is bounded by {@link #MAX_REFINE_PASSES}, {@link
 * #MAX_SWAP_ROUNDS} and {@link #MAX_SWAP_NODES} so runtime stays predictable on large snapshots.
 *
 * <p>Deterministic: every list is sorted by hostname (and weight) and no randomness is used.
 */
public final class WeightedLptFmPartitioner implements NodePartitioner {

  /** Target per-worker load may exceed the even share by this factor during refinement. */
  public static final double LOAD_CAP_FACTOR = 1.10;

  /** Maximum single-node-move passes. */
  public static final int MAX_REFINE_PASSES = 4;

  /** Maximum best-swap rounds. */
  public static final int MAX_SWAP_ROUNDS = 3;

  /** Maximum boundary nodes considered per swap round (highest incident cut first). */
  public static final int MAX_SWAP_NODES = 512;

  @Override
  public Map<String, Integer> partition(CommunicationGraph graph, int numWorkers, long seed) {
    if (numWorkers < 1) {
      throw new IllegalArgumentException("numWorkers must be >= 1");
    }
    List<String> order = new ArrayList<>(graph.nodes());
    order.sort(
        Comparator.comparingInt((String h) -> graph.weight(h))
            .reversed()
            .thenComparing(Comparator.naturalOrder()));
    Map<String, Integer> assignment = new HashMap<>();
    long[] loads = new long[numWorkers];
    for (String host : order) {
      int best = 0;
      for (int w = 1; w < numWorkers; w++) {
        if (loads[w] < loads[best]) {
          best = w;
        }
      }
      assignment.put(host, best);
      loads[best] += graph.weight(host);
    }
    long cap = loadCap(graph, numWorkers);
    refineByMoves(graph, numWorkers, assignment, loads, cap);
    refineBySwaps(graph, numWorkers, assignment, loads, cap);
    return assignment;
  }

  /** The target load cap (see the class doc). */
  static long loadCap(CommunicationGraph graph, int numWorkers) {
    long total = graph.totalWeight();
    long evenShare = (total + numWorkers - 1) / numWorkers;
    long scaled = (long) Math.ceil(LOAD_CAP_FACTOR * evenShare);
    int maxNode = 0;
    for (String node : graph.nodes()) {
      maxNode = Math.max(maxNode, graph.weight(node));
    }
    return Math.max(maxNode, scaled);
  }

  /** Move single nodes to reduce the cut while respecting the cap. */
  private static void refineByMoves(
      CommunicationGraph graph,
      int numWorkers,
      Map<String, Integer> assignment,
      long[] loads,
      long cap) {
    for (int pass = 0; pass < MAX_REFINE_PASSES; pass++) {
      boolean changed = false;
      for (String node : CommunicationGraph.boundaryNodes(graph, assignment)) {
        int current = assignment.get(node);
        int nodeWeight = graph.weight(node);
        long bestGain = 0;
        int bestWorker = -1;
        for (int w = 0; w < numWorkers; w++) {
          if (w == current) {
            continue;
          }
          if (loads[w] + nodeWeight > cap) {
            continue;
          }
          long gain = moveGain(graph, assignment, node, current, w);
          if (gain > bestGain) {
            bestGain = gain;
            bestWorker = w;
          }
        }
        if (bestWorker >= 0) {
          loads[current] -= nodeWeight;
          loads[bestWorker] += nodeWeight;
          assignment.put(node, bestWorker);
          changed = true;
        }
      }
      if (!changed) {
        return;
      }
    }
  }

  /**
   * Swap pairs of boundary nodes when the swap reduces the cut and keeps both workers within the
   * cap. Only the {@link #MAX_SWAP_NODES} boundary nodes with the highest incident cut weight are
   * considered, so the pass is bounded.
   */
  private static void refineBySwaps(
      CommunicationGraph graph,
      int numWorkers,
      Map<String, Integer> assignment,
      long[] loads,
      long cap) {
    for (int round = 0; round < MAX_SWAP_ROUNDS; round++) {
      List<String> candidates = swapCandidates(graph, assignment);
      long bestDelta = 0;
      String bestU = null;
      String bestV = null;
      for (int i = 0; i < candidates.size(); i++) {
        String u = candidates.get(i);
        int au = assignment.get(u);
        int wu = graph.weight(u);
        for (int j = i + 1; j < candidates.size(); j++) {
          String v = candidates.get(j);
          int av = assignment.get(v);
          if (au == av) {
            continue;
          }
          int wv = graph.weight(v);
          if (loads[av] - wv + wu > cap || loads[au] - wu + wv > cap) {
            continue;
          }
          // Swapping u and v keeps their mutual edge cut, and each move's gain counts it, so
          // subtract the double count (2 * edge weight).
          long delta =
              moveGain(graph, assignment, u, au, av)
                  + moveGain(graph, assignment, v, av, au)
                  - 2L * graph.edgeWeight(u, v);
          if (delta > bestDelta) {
            bestDelta = delta;
            bestU = u;
            bestV = v;
          }
        }
      }
      if (bestU == null) {
        return;
      }
      int au = assignment.get(bestU);
      int av = assignment.get(bestV);
      int wu = graph.weight(bestU);
      int wv = graph.weight(bestV);
      loads[au] += wv - wu;
      loads[av] += wu - wv;
      assignment.put(bestU, av);
      assignment.put(bestV, au);
    }
  }

  /** Boundary nodes, highest incident cut weight first (ties by hostname), truncated. */
  private static List<String> swapCandidates(
      CommunicationGraph graph, Map<String, Integer> assignment) {
    List<String> boundary = new ArrayList<>(CommunicationGraph.boundaryNodes(graph, assignment));
    boundary.sort(
        Comparator.comparingLong((String h) -> incidentCutWeight(graph, assignment, h))
            .reversed()
            .thenComparing(Comparator.naturalOrder()));
    if (boundary.size() > MAX_SWAP_NODES) {
      return boundary.subList(0, MAX_SWAP_NODES);
    }
    return boundary;
  }

  /**
   * The cut-weight reduction from moving {@code node} from worker {@code from} to worker {@code to}
   * (positive means the cut shrinks).
   */
  static long moveGain(
      CommunicationGraph graph, Map<String, Integer> assignment, String node, int from, int to) {
    long gain = 0;
    for (Map.Entry<String, Integer> e : graph.neighbors(node).entrySet()) {
      int owner = assignment.get(e.getKey());
      if (owner == to) {
        // An edge that was cut becomes internal.
        gain += e.getValue();
      } else if (owner == from) {
        // An edge that was internal becomes cut.
        gain -= e.getValue();
      }
    }
    return gain;
  }

  /** Estimated exchange weight that {@code node}'s incident edges contribute to the cut. */
  private static long incidentCutWeight(
      CommunicationGraph graph, Map<String, Integer> assignment, String node) {
    long cut = 0;
    int owner = assignment.get(node);
    for (Map.Entry<String, Integer> e : graph.neighbors(node).entrySet()) {
      if (assignment.get(e.getKey()) != owner) {
        cut += e.getValue();
      }
    }
    return cut;
  }
}
