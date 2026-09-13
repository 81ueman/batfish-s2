// SPDX-License-Identifier: Apache-2.0

package org.batfish.dataplane.ibdp;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.Deque;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import org.batfish.datamodel.BgpProcess;
import org.batfish.datamodel.Configuration;
import org.batfish.datamodel.Interface;
import org.batfish.datamodel.KernelRoute;
import org.batfish.datamodel.Prefix;
import org.batfish.datamodel.PrefixRange;
import org.batfish.datamodel.StaticRoute;
import org.batfish.datamodel.Vrf;

/**
 * The S2 prefix dependency graph (DPDG): a directed graph whose nodes are destination prefixes and
 * whose edges encode that one prefix's computation depends on another's. Prefix sharding is only
 * safe if every weakly connected component (WCC) of this graph is appointed as a unit, because a
 * route for one prefix can be generated, activated, or suppressed based on the presence of another.
 *
 * <p>Edges currently modeled, all between an aggregate network and the more-specific prefixes it
 * covers:
 *
 * <ul>
 *   <li>{@link EdgeKind#AGGREGATE}: a BGP aggregate is generated only while at least one covered
 *       more-specific is present, so it depends on its contributors.
 *   <li>{@link EdgeKind#SUMMARY_ONLY}: a summary-only (suppressing) aggregate also determines
 *       whether the covered more-specifics are advertised, so the specific depends on the aggregate
 *       (the relation is symmetric for co-sharding).
 *   <li>{@link EdgeKind#NESTED_AGGREGATE}: BgpRoutingProcess funnels a more-specific aggregate's
 *       potential contributors into a more-general aggregate, so nested aggregates depend on each
 *       other even when no non-aggregate prefix sits strictly between them.
 * </ul>
 *
 * <p>Conditional advertisement would be another edge kind, but the vendor-independent model does
 * not represent it (routing policies are opaque), so it cannot be discovered here and is
 * intentionally not modeled.
 *
 * <p>The graph is built deterministically from the snapshot: node and edge iteration use natural
 * prefix ordering and no randomness.
 */
final class PrefixDependencyGraph {

  /** Why two prefixes must be co-sharded. */
  enum EdgeKind {
    /** A BGP aggregate depends on a covered more-specific contributor. */
    AGGREGATE,
    /** A summary-only aggregate suppresses the covered more-specifics it covers. */
    SUMMARY_ONLY,
    /** A more-general aggregate funnels the contributors of a more-specific aggregate. */
    NESTED_AGGREGATE,
  }

  /** A directed, typed dependency between two prefixes. */
  record Edge(Prefix tail, Prefix head, EdgeKind kind) {}

  /** A BGP aggregate network plus whether it suppresses the more-specifics it covers. */
  private record AggregateInfo(Prefix network, boolean suppresses) {}

  private static final Comparator<Prefix> PREFIX_ORDER = Comparator.naturalOrder();

  private final Set<Prefix> _nodes;
  private final List<Edge> _edges;
  private final Map<Prefix, Integer> _weights;

  private PrefixDependencyGraph(Set<Prefix> nodes, List<Edge> edges, Map<Prefix, Integer> weights) {
    _nodes = ImmutableSet.copyOf(nodes);
    _edges = ImmutableList.copyOf(edges);
    _weights = ImmutableMap.copyOf(weights);
  }

  /**
   * Builds the DPDG over the sharding universe of {@code configs} (see {@link
   * PrefixSharder#queryPrefixes}) plus any {@code extraPrefixes} (e.g. external BGP announcements).
   */
  static PrefixDependencyGraph build(
      Map<String, Configuration> configs, Collection<Prefix> extraPrefixes) {
    Set<Prefix> nodes = new LinkedHashSet<>(PrefixSharder.queryPrefixes(configs, extraPrefixes));
    List<Prefix> sortedNodes = new ArrayList<>(nodes);
    sortedNodes.sort(PREFIX_ORDER);

    List<AggregateInfo> aggregates = aggregateInfos(configs);
    Set<Edge> edgeSet = new LinkedHashSet<>();
    for (AggregateInfo aggregate : aggregates) {
      if (!nodes.contains(aggregate.network())) {
        continue;
      }
      for (Prefix covered : sortedNodes) {
        if (covered.equals(aggregate.network()) || !aggregate.network().containsPrefix(covered)) {
          continue;
        }
        edgeSet.add(new Edge(aggregate.network(), covered, EdgeKind.AGGREGATE));
        if (aggregate.suppresses()) {
          edgeSet.add(new Edge(aggregate.network(), covered, EdgeKind.SUMMARY_ONLY));
        }
      }
    }
    // Nested aggregates: a more-general aggregate depends on every more-specific aggregate it
    // covers, even if that aggregate's own contributors are not in the universe.
    List<Prefix> aggregatePrefixes = new ArrayList<>();
    for (AggregateInfo aggregate : aggregates) {
      if (nodes.contains(aggregate.network()) && !aggregatePrefixes.contains(aggregate.network())) {
        aggregatePrefixes.add(aggregate.network());
      }
    }
    aggregatePrefixes.sort(PREFIX_ORDER);
    for (Prefix general : aggregatePrefixes) {
      for (Prefix specific : aggregatePrefixes) {
        if (!general.equals(specific) && general.containsPrefix(specific)) {
          edgeSet.add(new Edge(general, specific, EdgeKind.NESTED_AGGREGATE));
        }
      }
    }

    List<Edge> edges = new ArrayList<>(edgeSet);
    edges.sort(
        Comparator.comparing((Edge e) -> e.tail(), PREFIX_ORDER)
            .thenComparing(e -> e.head(), PREFIX_ORDER)
            .thenComparing(e -> e.kind().ordinal()));
    TreeSet<Prefix> nodeSet = new TreeSet<>(PREFIX_ORDER);
    nodeSet.addAll(nodes);
    return new PrefixDependencyGraph(nodeSet, edges, computeWeights(configs, nodes));
  }

  /** All aggregates across the snapshot, with their suppression status. */
  private static List<AggregateInfo> aggregateInfos(Map<String, Configuration> configs) {
    List<AggregateInfo> aggregates = new ArrayList<>();
    for (Configuration c : configs.values()) {
      for (Vrf vrf : c.getVrfs().values()) {
        BgpProcess proc = vrf.getBgpProcess();
        if (proc == null) {
          continue;
        }
        proc.getAggregates()
            .values()
            .forEach(
                aggregate ->
                    aggregates.add(
                        new AggregateInfo(
                            aggregate.getNetwork(), aggregate.getSuppressionPolicy() != null)));
      }
    }
    return aggregates;
  }

  /**
   * Estimate each prefix's route/memory contribution. The paper weights a WCC by the size of the
   * propagation closure of its prefixes; without the BGP session graph here we approximate that by
   * the number of router processes that locally source the prefix (connected address, BGP
   * origination space, aggregate, or redistributed static/kernel route). Every such source is a
   * distinct RIB entry, and on a connected session graph the total RIB usage grows with this count,
   * so it is a strict improvement over treating every prefix as weight one.
   */
  private static Map<Prefix, Integer> computeWeights(
      Map<String, Configuration> configs, Set<Prefix> nodes) {
    Map<Prefix, Integer> weights = new HashMap<>();
    for (Prefix node : nodes) {
      weights.put(node, 1);
    }
    for (Configuration c : configs.values()) {
      Set<Prefix> localSources = new HashSet<>();
      for (Interface i : c.getAllInterfaces().values()) {
        if (i.getConcreteAddress() != null) {
          localSources.add(i.getConcreteAddress().getPrefix());
        }
      }
      for (Vrf vrf : c.getVrfs().values()) {
        BgpProcess proc = vrf.getBgpProcess();
        if (proc != null) {
          proc.getOriginationSpace().getPrefixRanges().stream()
              .map(PrefixRange::getPrefix)
              .forEach(localSources::add);
          localSources.addAll(proc.getUnconditionalNetworkStatements());
          localSources.addAll(proc.getAggregates().keySet());
        }
        vrf.getStaticRoutes().stream().map(StaticRoute::getNetwork).forEach(localSources::add);
        vrf.getKernelRoutes().stream().map(KernelRoute::getNetwork).forEach(localSources::add);
      }
      for (Prefix source : localSources) {
        if (weights.containsKey(source)) {
          weights.merge(source, 1, Integer::sum);
        }
      }
    }
    return weights;
  }

  /** All prefix nodes of the graph, in ascending prefix order. */
  Set<Prefix> nodes() {
    return _nodes;
  }

  /** All dependency edges, in a deterministic order (by tail, then head, then kind). */
  List<Edge> edges() {
    return _edges;
  }

  /** The estimated route/memory weight of a single prefix node. */
  @VisibleForTesting
  int prefixWeight(Prefix prefix) {
    return _weights.getOrDefault(prefix, 1);
  }

  /** The estimated route/memory weight of a whole component: the sum over its prefix nodes. */
  int componentWeight(Collection<Prefix> component) {
    int weight = 0;
    for (Prefix prefix : component) {
      weight += prefixWeight(prefix);
    }
    return weight;
  }

  /**
   * The weakly connected components of the graph, each returned as an ascending set of prefixes.
   * Components are ordered by their smallest prefix. Deterministic for a fixed snapshot.
   */
  List<Set<Prefix>> weaklyConnectedComponents() {
    Map<Prefix, TreeSet<Prefix>> adjacency = new TreeMap<>(PREFIX_ORDER);
    for (Prefix node : _nodes) {
      adjacency.put(node, new TreeSet<>(PREFIX_ORDER));
    }
    for (Edge edge : _edges) {
      adjacency.get(edge.tail()).add(edge.head());
      adjacency.get(edge.head()).add(edge.tail());
    }
    Set<Prefix> visited = new HashSet<>();
    List<Set<Prefix>> components = new ArrayList<>();
    for (Prefix start : adjacency.keySet()) {
      if (!visited.add(start)) {
        continue;
      }
      TreeSet<Prefix> component = new TreeSet<>(PREFIX_ORDER);
      Deque<Prefix> frontier = new ArrayDeque<>();
      frontier.add(start);
      while (!frontier.isEmpty()) {
        Prefix current = frontier.poll();
        component.add(current);
        for (Prefix neighbor : adjacency.get(current)) {
          if (visited.add(neighbor)) {
            frontier.add(neighbor);
          }
        }
      }
      components.add(component);
    }
    return components;
  }
}
