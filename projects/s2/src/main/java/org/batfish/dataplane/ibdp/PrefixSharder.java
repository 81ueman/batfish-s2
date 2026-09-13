package org.batfish.dataplane.ibdp;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.batfish.datamodel.Configuration;
import org.batfish.datamodel.Interface;
import org.batfish.datamodel.Prefix;
import org.batfish.datamodel.PrefixRange;
import org.batfish.datamodel.PrefixSpace;
import org.batfish.datamodel.Vrf;

/**
 * Splits a snapshot's destination prefix space into shards for S2 prefix sharding (the
 * control-plane BGP variant): each shard is appointed for one EGP prefix round so only that shard's
 * BGP routes are materialized at a time.
 *
 * <p>The shards partition the prefixes that can be originated (interface addresses and BGP
 * origination networks).
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

  /**
   * Partition {@code prefixes} into at most {@code n} balanced shards (round-robin over a
   * deterministic ordering) as {@link PrefixSpace}s. With {@code n <= 1} there is a single shard.
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
