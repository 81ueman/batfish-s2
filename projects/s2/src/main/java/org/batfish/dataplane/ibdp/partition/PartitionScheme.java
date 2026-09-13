// SPDX-License-Identifier: Apache-2.0
package org.batfish.dataplane.ibdp.partition;

import java.util.Locale;

/**
 * The selectable node &rarr; worker partition schemes (port plan &sect;3.3).
 *
 * <p>Selected on the controller with {@code -Ds2.partition=<scheme>} (case-insensitive). The
 * default is {@link #RANDOM}, which reproduces the historical deterministic hash-shuffle
 * round-robin exactly so stock demos are unchanged. Workers never consult this property: the
 * controller ships the computed assignment in {@code S2ControlMessages.Start}.
 */
public enum PartitionScheme {
  /**
   * Historical deterministic balanced round-robin (hash-shuffled). Default; load-balanced, worst
   * cut.
   */
  RANDOM(new RandomPartitioner()),

  /** Expert scheme: hostnames in ascending order, dealt round-robin. Strong on named DCNs. */
  NAME_ORDERED(new NameOrderedPartitioner()),

  /**
   * Weight-descending LPT then a bounded FM/KL refinement under a load cap. General-purpose
   * default.
   */
  WEIGHTED_LPT_FM(new WeightedLptFmPartitioner()),

  /** Seed k-center followed by least-loaded neighbor region growing. Favors locality (WAN). */
  GREEDY_REGION(new GreedyRegionPartitioner()),

  /**
   * External {@code gpmetis -seed=...}; falls back to {@link #WEIGHTED_LPT_FM} when unavailable.
   */
  METIS(new MetisPartitioner());

  /** System property that selects the scheme. */
  public static final String PROPERTY = "s2.partition";

  private final NodePartitioner _partitioner;

  PartitionScheme(NodePartitioner partitioner) {
    _partitioner = partitioner;
  }

  /** The {@link NodePartitioner} implementing this scheme. */
  public NodePartitioner partitioner() {
    return _partitioner;
  }

  /** Parse a scheme name (case-insensitive), or {@code null} if unknown. */
  public static PartitionScheme tryParse(String value) {
    if (value == null || value.trim().isEmpty()) {
      return null;
    }
    try {
      return valueOf(value.trim().toUpperCase(Locale.ROOT));
    } catch (IllegalArgumentException e) {
      return null;
    }
  }

  /**
   * The scheme selected by {@code -Ds2.partition=<scheme>}, defaulting to {@link #RANDOM}. An
   * unknown value fails fast rather than silently changing the partition.
   */
  public static PartitionScheme fromSystemProperties() {
    String value = System.getProperty(PROPERTY);
    if (value == null || value.trim().isEmpty()) {
      return RANDOM;
    }
    PartitionScheme scheme = tryParse(value);
    if (scheme == null) {
      throw new IllegalArgumentException(
          "unknown "
              + PROPERTY
              + " value '"
              + value
              + "'; expected one of RANDOM, NAME_ORDERED, WEIGHTED_LPT_FM, GREEDY_REGION, METIS");
    }
    return scheme;
  }
}
