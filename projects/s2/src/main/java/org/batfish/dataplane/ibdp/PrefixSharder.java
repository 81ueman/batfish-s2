package org.batfish.dataplane.ibdp;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import javax.annotation.Nullable;
import org.batfish.datamodel.AclIpSpace;
import org.batfish.datamodel.Configuration;
import org.batfish.datamodel.Interface;
import org.batfish.datamodel.IpSpace;
import org.batfish.datamodel.Prefix;
import org.batfish.datamodel.PrefixRange;
import org.batfish.datamodel.PrefixSpace;
import org.batfish.datamodel.UniverseIpSpace;
import org.batfish.datamodel.Vrf;

/**
 * Splits the destination prefix space of a snapshot into query shards for prefix-sharded
 * distributed reachability (S2 prefix sharding, orthogonal to the switch partition).
 *
 * <p>The shards partition the prefixes that can appear as accepted destinations (interface
 * addresses and BGP origination networks). Each shard is run as its own backward reachability
 * fixpoint with the query restricted to that shard's destinations; the union over shards equals the
 * full query because backward reachability is linear in the query BDD.
 */
final class PrefixSharder {

  private PrefixSharder() {}

  /** Prefixes of interest: connected interface addresses plus BGP origination networks. */
  static List<Prefix> queryPrefixes(Map<String, Configuration> configs) {
    Set<Prefix> prefixes = new LinkedHashSet<>();
    for (Configuration c : configs.values()) {
      for (Interface i : c.getAllInterfaces().values()) {
        if (i.getConcreteAddress() != null) {
          prefixes.add(i.getConcreteAddress().getPrefix());
        }
      }
      for (Vrf vrf : c.getVrfs().values()) {
        if (vrf.getBgpProcess() != null) {
          vrf.getBgpProcess().getOriginationSpace().getPrefixRanges().stream()
              .map(PrefixRange::getPrefix)
              .forEach(prefixes::add);
        }
      }
    }
    return new ArrayList<>(prefixes);
  }

  /** The union of {@code prefixes} as an {@link IpSpace}; the full query space. */
  static IpSpace querySpace(List<Prefix> prefixes) {
    if (prefixes.isEmpty()) {
      return UniverseIpSpace.INSTANCE;
    }
    IpSpace space =
        AclIpSpace.union(prefixes.stream().map(Prefix::toIpSpace).collect(Collectors.toList()));
    return space == null ? UniverseIpSpace.INSTANCE : space;
  }

  /**
   * Partition {@code prefixes} into at most {@code n} balanced shards (round-robin over a
   * deterministic ordering), each returned as an {@link IpSpace}. With {@code n <= 1} the single
   * shard is the full query space.
   */
  static List<IpSpace> shard(List<Prefix> prefixes, int n) {
    if (n <= 1 || prefixes.isEmpty()) {
      return List.of(querySpace(prefixes));
    }
    List<Prefix> sorted = new ArrayList<>(prefixes);
    sorted.sort(Comparator.comparing(Prefix::toString));
    List<List<Prefix>> groups = new ArrayList<>(n);
    for (int i = 0; i < n; i++) {
      groups.add(new ArrayList<>());
    }
    for (int i = 0; i < sorted.size(); i++) {
      groups.get(i % n).add(sorted.get(i));
    }
    List<IpSpace> shards = new ArrayList<>();
    for (List<Prefix> group : groups) {
      if (!group.isEmpty()) {
        IpSpace space = querySpace(group);
        if (space != null) {
          shards.add(space);
        }
      }
    }
    return shards;
  }

  /** The union of the shard spaces; equal to {@link #querySpace(List)}. */
  static @Nullable IpSpace union(List<IpSpace> shards) {
    return AclIpSpace.union(shards);
  }

  /**
   * Like {@link #shard}, but returns {@link PrefixSpace}s (for the control-plane BGP prefix
   * sharding hook, which appoints a prefix space per round).
   */
  static List<PrefixSpace> prefixSpaces(List<Prefix> prefixes, int n) {
    List<Prefix> sorted = new ArrayList<>(prefixes);
    sorted.sort(Comparator.comparing(Prefix::toString));
    int groups = (n <= 1 || sorted.isEmpty()) ? 1 : n;
    List<PrefixSpace> shards = new ArrayList<>();
    for (int i = 0; i < groups; i++) {
      shards.add(new PrefixSpace());
    }
    for (int i = 0; i < sorted.size(); i++) {
      shards.get(i % groups).addPrefix(sorted.get(i));
    }
    return shards;
  }
}
