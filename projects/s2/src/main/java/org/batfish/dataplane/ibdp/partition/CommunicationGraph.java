// SPDX-License-Identifier: Apache-2.0

package org.batfish.dataplane.ibdp.partition;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.ImmutableSortedSet;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import org.batfish.datamodel.BgpPeerConfigId;
import org.batfish.datamodel.Configuration;
import org.batfish.datamodel.Edge;
import org.batfish.datamodel.bgp.BgpTopology;
import org.batfish.datamodel.ospf.OspfNeighborConfigId;
import org.batfish.datamodel.ospf.OspfTopology;
import org.batfish.dataplane.ibdp.TopologyContext;

/**
 * The union communication graph of an S2 snapshot, used by the node &rarr; worker partitioners.
 *
 * <p>Vertices are router hostnames; a vertex is weighted by {@link NodeWeights} (an estimate of its
 * control-plane load). Undirected edges are the union of three topologies (port plan &sect;3.1):
 *
 * <ul>
 *   <li>L3 adjacencies ({@link TopologyContext#getLayer3Topology()});
 *   <li>BGP sessions ({@link BgpTopology}), which is the essential graph on WANs where multi-hop
 *       iBGP sessions do not follow L3 links;
 *   <li>OSPF adjacencies ({@link TopologyContext#getOspfTopology()}).
 * </ul>
 *
 * <p>An edge's weight is an estimate of the exchange volume across it. In v1 an edge's weight is
 * the number of distinct topologies that connect the pair (1 for an L3-only or BGP-only or
 * OSPF-only adjacency, up to 3 when all three agree). Topology edge sets are directed and list both
 * orientations of a link; the bitmask below makes the undirected edge weight independent of that
 * duplication. This is deliberately simple and documented rather than calibrated; the partition
 * objective weights balance first and cut second, and the paper finds the cut has little effect on
 * peak memory (&sect;5.6).
 *
 * <p>Building is deterministic: vertices and neighbor lists are sorted by hostname.
 */
public final class CommunicationGraph {

  /** One unit per distinct topology connecting a pair (see the class doc). */
  public static final int DEFAULT_EDGE_WEIGHT = 1;

  /** Bit for an L3 adjacency in the per-pair topology mask. */
  private static final int L3 = 1;

  /** Bit for a BGP session in the per-pair topology mask. */
  private static final int BGP = 2;

  /** Bit for an OSPF adjacency in the per-pair topology mask. */
  private static final int OSPF = 4;

  private final ImmutableSortedSet<String> _nodes;
  private final ImmutableMap<String, Integer> _weights;
  private final ImmutableMap<String, ImmutableMap<String, Integer>> _adjacency;

  /**
   * The bare undirected BGP session graph (hostname to neighbor hostnames), a subset of {@link
   * #_adjacency}. Exposed separately so {@link AutoSchemeSelector} can tell a BGP overlay that does
   * not follow the IGP (route reflectors / multi-hop iBGP) from a DCN where BGP sessions track the
   * L3 links.
   */
  private final ImmutableMap<String, ImmutableSet<String>> _bgpAdjacency;

  private CommunicationGraph(
      ImmutableSortedSet<String> nodes,
      ImmutableMap<String, Integer> weights,
      ImmutableMap<String, ImmutableMap<String, Integer>> adjacency,
      ImmutableMap<String, ImmutableSet<String>> bgpAdjacency) {
    _nodes = nodes;
    _weights = weights;
    _adjacency = adjacency;
    _bgpAdjacency = bgpAdjacency;
  }

  /** All router hostnames, in ascending order. */
  public Set<String> nodes() {
    return _nodes;
  }

  /** The estimated load weight of {@code node}. */
  public int weight(String node) {
    return _weights.getOrDefault(node, 0);
  }

  /** The estimated load weights, keyed by hostname. */
  public Map<String, Integer> weights() {
    return _weights;
  }

  /** Sum of all node weights. */
  public long totalWeight() {
    long total = 0;
    for (int w : _weights.values()) {
      total += w;
    }
    return total;
  }

  /**
   * The estimated exchange weight of the undirected edge between {@code a} and {@code b}, or {@code
   * 0} if they are not adjacent.
   */
  public int edgeWeight(String a, String b) {
    ImmutableMap<String, Integer> neighbors = _adjacency.get(a);
    return neighbors == null ? 0 : neighbors.getOrDefault(b, 0);
  }

  /**
   * The neighbors of {@code node} with their estimated edge weights, in ascending hostname order.
   */
  public Map<String, Integer> neighbors(String node) {
    return _adjacency.getOrDefault(node, ImmutableMap.of());
  }

  /** The BGP session neighbors of {@code node}, in ascending hostname order. */
  public Set<String> bgpNeighbors(String node) {
    return _bgpAdjacency.getOrDefault(node, ImmutableSet.of());
  }

  /** Whether {@code a} and {@code b} have a BGP session (the bare session graph). */
  public boolean hasBgpSession(String a, String b) {
    return _bgpAdjacency.getOrDefault(a, ImmutableSet.of()).contains(b);
  }

  /**
   * Whether the undirected edge {@code a}--{@code b} exists in the union graph but is <em>not</em>
   * a BGP session (an L3 and/or OSPF adjacency only).
   */
  public boolean hasNonBgpEdge(String a, String b) {
    return edgeWeight(a, b) > 0 && !hasBgpSession(a, b);
  }

  /** The total estimated exchange weight of all edges (each undirected edge counted once). */
  public long totalEdgeWeight() {
    long total = 0;
    for (ImmutableMap<String, Integer> neighbors : _adjacency.values()) {
      for (int w : neighbors.values()) {
        total += w;
      }
    }
    return total / 2;
  }

  /**
   * A synthetic graph for unit tests: {@code weights} is the node set and weights, {@code
   * adjacency} the symmetric edge weights. No BGP session graph is attached (the union edges are
   * treated as non-BGP), so the graph is undirected only; use the three-argument overload to test
   * the BGP-overlay heuristic.
   */
  @com.google.common.annotations.VisibleForTesting
  static CommunicationGraph forTesting(
      Map<String, Integer> weights, Map<String, Map<String, Integer>> adjacency) {
    return forTesting(weights, adjacency, Map.of());
  }

  /**
   * A synthetic graph with an explicit BGP session graph, for the {@link AutoSchemeSelector} tests.
   * The BGP adjacency is filtered to known nodes and symmetrized. Both maps use symmetric
   * undirected edge weights.
   */
  @com.google.common.annotations.VisibleForTesting
  static CommunicationGraph forTesting(
      Map<String, Integer> weights,
      Map<String, Map<String, Integer>> adjacency,
      Map<String, Map<String, Integer>> bgpAdjacency) {
    ImmutableSortedSet<String> nodes =
        ImmutableSortedSet.copyOf(Comparator.naturalOrder(), weights.keySet());
    ImmutableMap.Builder<String, Integer> weightBuilder = ImmutableMap.builder();
    ImmutableMap.Builder<String, ImmutableMap<String, Integer>> adjacencyBuilder =
        ImmutableMap.builder();
    Map<String, Set<String>> bgpSets = new TreeMap<>();
    for (String node : nodes) {
      weightBuilder.put(node, weights.getOrDefault(node, 0));
      Map<String, Integer> neighbors = new java.util.TreeMap<>();
      Map<String, Integer> edges = adjacency.get(node);
      if (edges != null) {
        for (Map.Entry<String, Integer> e : edges.entrySet()) {
          if (weights.containsKey(e.getKey())) {
            neighbors.put(e.getKey(), e.getValue());
          }
        }
      }
      adjacencyBuilder.put(node, ImmutableMap.copyOf(neighbors));
      bgpSets.put(node, new TreeSet<>());
    }
    for (String node : nodes) {
      Map<String, Integer> edges = bgpAdjacency.get(node);
      if (edges == null) {
        continue;
      }
      for (String neighbor : edges.keySet()) {
        if (!weights.containsKey(neighbor) || node.equals(neighbor)) {
          continue;
        }
        bgpSets.get(node).add(neighbor);
        bgpSets.get(neighbor).add(node);
      }
    }
    ImmutableMap.Builder<String, ImmutableSet<String>> bgpBuilder = ImmutableMap.builder();
    for (String node : nodes) {
      bgpBuilder.put(node, ImmutableSet.copyOf(bgpSets.get(node)));
    }
    return new CommunicationGraph(
        nodes, weightBuilder.build(), adjacencyBuilder.build(), bgpBuilder.build());
  }

  /** Build the union graph for a snapshot. */
  public static CommunicationGraph build(
      Map<String, Configuration> configs,
      TopologyContext topologyContext,
      BgpTopology bgpTopology) {
    MutableAdjacency adjacency = new MutableAdjacency(configs.keySet());
    // The bare BGP session graph (undirected), used by the v2 full-table node-weight correction.
    Map<String, Set<String>> bgpAdjacency = new TreeMap<>();
    for (String node : configs.keySet()) {
      bgpAdjacency.put(node, new TreeSet<>());
    }
    // L3 adjacencies.
    for (Edge edge : topologyContext.getLayer3Topology().getEdges()) {
      adjacency.add(edge.getNode1(), edge.getNode2(), L3);
    }
    // BGP sessions.
    for (com.google.common.graph.EndpointPair<BgpPeerConfigId> pair :
        bgpTopology.getGraph().edges()) {
      String source = pair.source().getHostname();
      String target = pair.target().getHostname();
      adjacency.add(source, target, BGP);
      if (!source.equals(target)
          && bgpAdjacency.containsKey(source)
          && bgpAdjacency.containsKey(target)) {
        bgpAdjacency.get(source).add(target);
        bgpAdjacency.get(target).add(source);
      }
    }
    // OSPF adjacencies.
    OspfTopology ospf = topologyContext.getOspfTopology();
    for (OspfTopology.EdgeId edgeId : ospf.edges()) {
      OspfNeighborConfigId tail = edgeId.getTail();
      OspfNeighborConfigId head = edgeId.getHead();
      adjacency.add(tail.getHostname(), head.getHostname(), OSPF);
    }
    Map<String, Integer> weights = NodeWeights.compute(configs, bgpAdjacency);
    return adjacency.toGraph(weights, bgpAdjacency);
  }

  /** Accumulates the per-pair topology mask deterministically. */
  private static final class MutableAdjacency {
    private final Set<String> _nodes;
    private final Map<String, Map<String, Integer>> _mask = new TreeMap<>();

    MutableAdjacency(Set<String> nodes) {
      _nodes = nodes;
      for (String node : nodes) {
        _mask.put(node, new TreeMap<>());
      }
    }

    void add(String a, String b, int topologyBit) {
      if (a == null || b == null || a.equals(b)) {
        return;
      }
      if (!_nodes.contains(a) || !_nodes.contains(b)) {
        // Topology can mention nodes that are not configurations (should not happen); ignore.
        return;
      }
      _mask.get(a).merge(b, topologyBit, (x, y) -> x | y);
      _mask.get(b).merge(a, topologyBit, (x, y) -> x | y);
    }

    CommunicationGraph toGraph(
        Map<String, Integer> weights, Map<String, Set<String>> bgpAdjacency) {
      ImmutableSortedSet<String> nodes =
          ImmutableSortedSet.copyOf(Comparator.naturalOrder(), _nodes);
      ImmutableMap.Builder<String, Integer> weightBuilder = ImmutableMap.builder();
      for (String node : nodes) {
        weightBuilder.put(node, weights.getOrDefault(node, 0));
      }
      ImmutableMap.Builder<String, ImmutableMap<String, Integer>> adjacencyBuilder =
          ImmutableMap.builder();
      ImmutableMap.Builder<String, ImmutableSet<String>> bgpBuilder = ImmutableMap.builder();
      for (String node : nodes) {
        Map<String, Integer> neighbors = new TreeMap<>();
        for (Map.Entry<String, Integer> e : _mask.getOrDefault(node, Map.of()).entrySet()) {
          neighbors.put(e.getKey(), Integer.bitCount(e.getValue()) * DEFAULT_EDGE_WEIGHT);
        }
        adjacencyBuilder.put(node, ImmutableMap.copyOf(neighbors));
        Set<String> bgp = new TreeSet<>();
        for (String neighbor : bgpAdjacency.getOrDefault(node, Set.of())) {
          if (_nodes.contains(neighbor) && !neighbor.equals(node)) {
            bgp.add(neighbor);
          }
        }
        bgpBuilder.put(node, ImmutableSet.copyOf(bgp));
      }
      return new CommunicationGraph(
          nodes, weightBuilder.build(), adjacencyBuilder.build(), bgpBuilder.build());
    }
  }

  /** The set of nodes with at least one neighbor in a different worker, in ascending order. */
  static List<String> boundaryNodes(CommunicationGraph graph, Map<String, Integer> assignment) {
    List<String> boundary = new ArrayList<>();
    for (String node : graph.nodes()) {
      int owner = assignment.get(node);
      for (Map.Entry<String, Integer> e : graph.neighbors(node).entrySet()) {
        if (assignment.get(e.getKey()) != owner) {
          boundary.add(node);
          break;
        }
      }
    }
    return boundary;
  }

  /** Compute the cut weight of an assignment (sum of edge weights crossing a worker boundary). */
  public static long cutWeight(CommunicationGraph graph, Map<String, Integer> assignment) {
    Map<String, Integer> edgeSeen = new HashMap<>();
    long cut = 0;
    for (String u : graph.nodes()) {
      int uOwner = assignment.get(u);
      for (Map.Entry<String, Integer> e : graph.neighbors(u).entrySet()) {
        String v = e.getKey();
        if (uOwner == assignment.get(v)) {
          continue;
        }
        // Count each undirected edge once: only when u < v.
        if (u.compareTo(v) < 0) {
          cut += e.getValue();
        }
      }
    }
    return cut;
  }

  /** The per-worker node weights of an assignment, indexed by worker. */
  public static long[] loads(
      CommunicationGraph graph, Map<String, Integer> assignment, int numWorkers) {
    long[] loads = new long[numWorkers];
    for (String node : graph.nodes()) {
      loads[assignment.get(node)] += graph.weight(node);
    }
    return loads;
  }

  /** Build an assignment from a hostname -&gt; worker index map, validating the range. */
  public static Map<String, Integer> canonicalAssignment(
      Map<String, Integer> assignment, int numWorkers) {
    Map<String, Integer> canonical = new TreeMap<>();
    for (Map.Entry<String, Integer> e : assignment.entrySet()) {
      int worker = e.getValue();
      if (worker < 0 || worker >= numWorkers) {
        throw new IllegalStateException(
            "partitioner assigned " + e.getKey() + " to out-of-range worker " + worker);
      }
      canonical.put(e.getKey(), worker);
    }
    return canonical;
  }
}
