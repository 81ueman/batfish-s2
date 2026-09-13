// SPDX-License-Identifier: Apache-2.0
package org.batfish.dataplane.ibdp;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.ToIntFunction;
import org.batfish.datamodel.BgpProcess;
import org.batfish.datamodel.Configuration;
import org.batfish.datamodel.Interface;
import org.batfish.datamodel.KernelRoute;
import org.batfish.datamodel.Prefix;
import org.batfish.datamodel.PrefixRange;
import org.batfish.datamodel.PrefixSpace;
import org.batfish.datamodel.StaticRoute;
import org.batfish.datamodel.Vrf;

/**
 * Splits a snapshot's destination prefix space into shards for S2 prefix sharding (the
 * control-plane BGP variant): each shard is appointed for one EGP prefix round so only that shard's
 * BGP routes are materialized at a time.
 *
 * <p>The shards partition the prefixes that can be originated (interface addresses, BGP origination
 * networks, unconditional network statements, BGP aggregate networks, static and kernel route
 * networks, and external BGP announcements). Prefixes that depend on one another are modeled by a
 * {@link PrefixDependencyGraph}; each weakly connected component of that graph is assigned as a
 * unit to a shard, so an aggregate always stays with the prefixes it covers and with nested
 * aggregates.
 *
 * <p>Components are assigned to the shards by weighted LPT: largest estimated route/memory
 * contribution first to the currently lightest shard. The weight is an estimate of the propagation
 * closure (see {@link PrefixDependencyGraph#componentWeight}), not the raw prefix count, so a small
 * component that is originated by many routers is not underestimated.
 */
final class PrefixSharder {

  private PrefixSharder() {}

  /** All prefixes of interest for sharding (see the class doc). */
  static List<Prefix> queryPrefixes(Map<String, Configuration> configs) {
    return queryPrefixes(configs, ImmutableList.of());
  }

  /**
   * All prefixes of interest for sharding: connected addresses, BGP origination networks,
   * unconditional network statements, aggregates, static and kernel route networks (redistribution
   * sources) and any {@code extraPrefixes} (e.g. external BGP announcements). All of them are
   * subject to appointment, so all must be in the universe.
   */
  static List<Prefix> queryPrefixes(
      Map<String, Configuration> configs, Collection<Prefix> extraPrefixes) {
    Set<Prefix> prefixes = new LinkedHashSet<>();
    for (Configuration c : configs.values()) {
      for (Interface i : c.getAllInterfaces().values()) {
        if (i.getConcreteAddress() != null) {
          prefixes.add(i.getConcreteAddress().getPrefix());
        }
      }
      for (Vrf vrf : c.getVrfs().values()) {
        BgpProcess proc = vrf.getBgpProcess();
        if (proc != null) {
          proc.getOriginationSpace().getPrefixRanges().stream()
              .map(PrefixRange::getPrefix)
              .forEach(prefixes::add);
          prefixes.addAll(proc.getUnconditionalNetworkStatements());
          // Aggregates are generated from other routes, but must themselves be appointed in some
          // shard; otherwise they are filtered out of every round.
          prefixes.addAll(proc.getAggregates().keySet());
        }
        // Prefixes redistributed into BGP from this VRF's main RIB.
        vrf.getStaticRoutes().stream().map(StaticRoute::getNetwork).forEach(prefixes::add);
        vrf.getKernelRoutes().stream().map(KernelRoute::getNetwork).forEach(prefixes::add);
      }
    }
    prefixes.addAll(extraPrefixes);
    return new ArrayList<>(prefixes);
  }

  /**
   * Partition the snapshot's prefixes into at most {@code n} shards. Every weakly connected
   * component of the prefix dependency graph is assigned as a unit (largest estimated weight first)
   * to the currently lightest shard.
   */
  static List<PrefixSpace> shards(Map<String, Configuration> configs, int n) {
    return shards(configs, ImmutableList.of(), n);
  }

  /** As {@link #shards(Map, int)}, additionally including {@code extraPrefixes} in the universe. */
  static List<PrefixSpace> shards(
      Map<String, Configuration> configs, Collection<Prefix> extraPrefixes, int n) {
    return shards(PrefixDependencyGraph.build(configs, extraPrefixes), n);
  }

  /** Assigns the components of {@code graph} to at most {@code n} shards. */
  @VisibleForTesting
  static List<PrefixSpace> shards(PrefixDependencyGraph graph, int n) {
    List<Set<Prefix>> components = graph.weaklyConnectedComponents();
    Set<Prefix> allPrefixes = graph.nodes();
    if (components.size() == 1 && allPrefixes.size() > 1) {
      // Degenerate DPDG: a single dependency closure (e.g. a 0.0.0.0/0 aggregate) covers the whole
      // universe, so no split is possible without dropping routes. Fall back to one shard; the
      // caller treats a single-element list as sharding disabled.
      System.err.printf(
          "S2 prefix sharding: degenerate prefix dependency graph (one component over all %d"
              + " prefixes); falling back to a single shard%n",
          allPrefixes.size());
      return ImmutableList.of(prefixSpace(allPrefixes));
    }
    return assignComponents(components, graph::componentWeight, n);
  }

  /**
   * Assign whole components to at most {@code n} shards, largest estimated weight first to the
   * currently-lightest shard (list scheduling / LPT). Ties are broken by the component's smallest
   * prefix, so the result is deterministic without a random shuffle. A component's weight is its
   * estimated route/memory contribution, not its prefix count.
   */
  @VisibleForTesting
  static List<PrefixSpace> assignComponents(
      List<Set<Prefix>> components, ToIntFunction<Set<Prefix>> weigher, int n) {
    List<PrefixSpace> shards = new ArrayList<>();
    if (n <= 1 || components.isEmpty()) {
      PrefixSpace all = new PrefixSpace();
      components.forEach(c -> c.forEach(all::addPrefix));
      shards.add(all);
      return shards;
    }
    int k = Math.min(n, components.size());
    for (int i = 0; i < k; i++) {
      shards.add(new PrefixSpace());
    }
    long[] loads = new long[k];
    List<Set<Prefix>> sortedComponents = new ArrayList<>(components);
    sortedComponents.sort(
        Comparator.<Set<Prefix>>comparingInt(weigher::applyAsInt)
            .reversed()
            .thenComparing(c -> c.iterator().next(), Comparator.naturalOrder()));
    for (Set<Prefix> component : sortedComponents) {
      int best = 0;
      for (int i = 1; i < k; i++) {
        if (loads[i] < loads[best]) {
          best = i;
        }
      }
      component.forEach(shards.get(best)::addPrefix);
      loads[best] += weigher.applyAsInt(component);
    }
    return shards;
  }

  private static PrefixSpace prefixSpace(Collection<Prefix> prefixes) {
    PrefixSpace space = new PrefixSpace();
    prefixes.forEach(space::addPrefix);
    return space;
  }
}
