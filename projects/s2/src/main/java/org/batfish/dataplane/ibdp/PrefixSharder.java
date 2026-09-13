// SPDX-License-Identifier: Apache-2.0
package org.batfish.dataplane.ibdp;

import com.google.common.collect.ImmutableList;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
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
 * networks, unconditional network statements and BGP aggregate networks). Prefixes that depend on
 * one another -- an aggregate and the prefixes it covers -- stay in the same shard, because an
 * aggregate is only generated while one of its more-specifics is present.
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
   * Partition the snapshot's prefixes into at most {@code n} shards. An aggregate and the prefixes
   * it covers stay in the same shard; groups are assigned largest-first (LPT) to the lightest
   * shard.
   */
  static List<PrefixSpace> shards(Map<String, Configuration> configs, int n) {
    return shards(configs, ImmutableList.of(), n);
  }

  /** As {@link #shards(Map, int)}, additionally including {@code extraPrefixes} in the universe. */
  static List<PrefixSpace> shards(
      Map<String, Configuration> configs, Collection<Prefix> extraPrefixes, int n) {
    List<Prefix> prefixes = queryPrefixes(configs, extraPrefixes);
    List<List<Prefix>> groups = dependencyGroups(prefixes, aggregatePrefixes(configs));
    return assignGroups(groups, n);
  }

  /** The networks of all BGP aggregates across the snapshot. */
  private static List<Prefix> aggregatePrefixes(Map<String, Configuration> configs) {
    Set<Prefix> aggregates = new LinkedHashSet<>();
    for (Configuration c : configs.values()) {
      for (Vrf vrf : c.getVrfs().values()) {
        if (vrf.getBgpProcess() != null) {
          aggregates.addAll(vrf.getBgpProcess().getAggregates().keySet());
        }
      }
    }
    return new ArrayList<>(aggregates);
  }

  /** Union-find grouping: each aggregate is unioned with every prefix it covers. */
  private static List<List<Prefix>> dependencyGroups(
      List<Prefix> prefixes, List<Prefix> aggregates) {
    List<Prefix> sorted = new ArrayList<>(prefixes);
    sorted.sort(Comparator.comparing(Prefix::toString));
    Map<Prefix, Integer> index = new HashMap<>();
    for (int i = 0; i < sorted.size(); i++) {
      index.put(sorted.get(i), i);
    }
    int[] parent = new int[sorted.size()];
    for (int i = 0; i < parent.length; i++) {
      parent[i] = i;
    }
    for (Prefix aggregate : aggregates) {
      Integer root = index.get(aggregate);
      if (root == null) {
        continue;
      }
      for (int j = 0; j < sorted.size(); j++) {
        Prefix p = sorted.get(j);
        if (!p.equals(aggregate) && aggregate.containsPrefix(p)) {
          union(parent, root, j);
        }
      }
    }
    Map<Integer, List<Prefix>> byRoot = new TreeMap<>();
    for (int i = 0; i < sorted.size(); i++) {
      byRoot.computeIfAbsent(find(parent, i), k -> new ArrayList<>()).add(sorted.get(i));
    }
    return new ArrayList<>(byRoot.values());
  }

  private static int find(int[] parent, int x) {
    while (parent[x] != x) {
      parent[x] = parent[parent[x]];
      x = parent[x];
    }
    return x;
  }

  private static void union(int[] parent, int a, int b) {
    int ra = find(parent, a);
    int rb = find(parent, b);
    if (ra != rb) {
      parent[rb] = ra;
    }
  }

  /**
   * Assign whole groups to at most {@code n} shards, largest group first to the currently-lightest
   * shard (list scheduling). Deterministic for a fixed input.
   */
  private static List<PrefixSpace> assignGroups(List<List<Prefix>> groups, int n) {
    List<PrefixSpace> shards = new ArrayList<>();
    if (n <= 1 || groups.isEmpty()) {
      PrefixSpace all = new PrefixSpace();
      groups.forEach(g -> g.forEach(all::addPrefix));
      shards.add(all);
      return shards;
    }
    int k = Math.min(n, groups.size());
    for (int i = 0; i < k; i++) {
      shards.add(new PrefixSpace());
    }
    int[] sizes = new int[k];
    List<List<Prefix>> sortedGroups = new ArrayList<>(groups);
    sortedGroups.sort(
        Comparator.<List<Prefix>>comparingInt(g -> -g.size())
            .thenComparing(g -> g.get(0).toString()));
    for (List<Prefix> group : sortedGroups) {
      int best = 0;
      for (int i = 1; i < k; i++) {
        if (sizes[i] < sizes[best]) {
          best = i;
        }
      }
      group.forEach(shards.get(best)::addPrefix);
      sizes[best] += group.size();
    }
    return shards;
  }
}
