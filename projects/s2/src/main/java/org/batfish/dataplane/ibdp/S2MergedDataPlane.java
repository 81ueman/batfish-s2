// SPDX-License-Identifier: Apache-2.0

package org.batfish.dataplane.ibdp;

import com.google.common.collect.HashBasedTable;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableTable;
import com.google.common.collect.Table;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;
import javax.annotation.Nonnull;
import org.batfish.datamodel.Bgpv4Route;
import org.batfish.datamodel.DataPlane;
import org.batfish.datamodel.EvpnRoute;
import org.batfish.datamodel.Fib;
import org.batfish.datamodel.FinalMainRib;
import org.batfish.datamodel.ForwardingAnalysis;
import org.batfish.datamodel.ForwardingAnalysisImpl;
import org.batfish.datamodel.IpSpace;
import org.batfish.datamodel.Prefix;
import org.batfish.datamodel.VrfForwardingBehavior;
import org.batfish.datamodel.vxlan.Layer2Vni;
import org.batfish.datamodel.vxlan.Layer3Vni;

/**
 * A {@link DataPlane} assembled from the per-worker data planes of a distributed S2 run.
 *
 * <p>In the default owned-only mode each worker computes full RIBs/FIBs only for the nodes it owns,
 * so the workers' data planes are disjoint and their union is the global data plane. (If owned mode
 * is disabled every worker computes the whole data plane, in which case the union is idempotent.)
 * This is the naive, fully-materialized assembly; a lazy per-node data plane replaces it when the
 * global result does not fit in one JVM.
 */
final class S2MergedDataPlane implements DataPlane {

  private final @Nonnull Table<String, String, Set<Bgpv4Route>> _bgpRoutes;
  private final @Nonnull Table<String, String, Set<Bgpv4Route>> _bgpBackupRoutes;
  private final @Nonnull Table<String, String, Set<EvpnRoute<?, ?>>> _evpnRoutes;
  private final @Nonnull Table<String, String, Set<EvpnRoute<?, ?>>> _evpnBackupRoutes;
  private final @Nonnull Map<String, Map<String, Fib>> _fibs;
  private final @Nonnull ForwardingAnalysis _forwardingAnalysis;
  private final @Nonnull Table<String, String, Set<Layer2Vni>> _layer2Vnis;
  private final @Nonnull Table<String, String, Set<Layer3Vni>> _layer3Vnis;
  private final @Nonnull SortedMap<String, SortedMap<String, Map<Prefix, Map<String, Set<String>>>>>
      _prefixTracingInfoSummary;
  private final @Nonnull Table<String, String, FinalMainRib> _ribs;

  private S2MergedDataPlane(
      Table<String, String, Set<Bgpv4Route>> bgpRoutes,
      Table<String, String, Set<Bgpv4Route>> bgpBackupRoutes,
      Table<String, String, Set<EvpnRoute<?, ?>>> evpnRoutes,
      Table<String, String, Set<EvpnRoute<?, ?>>> evpnBackupRoutes,
      Map<String, Map<String, Fib>> fibs,
      ForwardingAnalysis forwardingAnalysis,
      Table<String, String, Set<Layer2Vni>> layer2Vnis,
      Table<String, String, Set<Layer3Vni>> layer3Vnis,
      SortedMap<String, SortedMap<String, Map<Prefix, Map<String, Set<String>>>>>
          prefixTracingInfoSummary,
      Table<String, String, FinalMainRib> ribs) {
    _bgpRoutes = bgpRoutes;
    _bgpBackupRoutes = bgpBackupRoutes;
    _evpnRoutes = evpnRoutes;
    _evpnBackupRoutes = evpnBackupRoutes;
    _fibs = fibs;
    _forwardingAnalysis = forwardingAnalysis;
    _layer2Vnis = layer2Vnis;
    _layer3Vnis = layer3Vnis;
    _prefixTracingInfoSummary = prefixTracingInfoSummary;
    _ribs = ribs;
  }

  /** Union the per-worker data planes into one global data plane. */
  static @Nonnull DataPlane of(List<DataPlane> parts) {
    Table<String, String, Set<Bgpv4Route>> bgpRoutes = HashBasedTable.create();
    Table<String, String, Set<Bgpv4Route>> bgpBackupRoutes = HashBasedTable.create();
    Table<String, String, Set<EvpnRoute<?, ?>>> evpnRoutes = HashBasedTable.create();
    Table<String, String, Set<EvpnRoute<?, ?>>> evpnBackupRoutes = HashBasedTable.create();
    Table<String, String, Set<Layer2Vni>> layer2Vnis = HashBasedTable.create();
    Table<String, String, Set<Layer3Vni>> layer3Vnis = HashBasedTable.create();
    Table<String, String, FinalMainRib> ribs = HashBasedTable.create();
    Map<String, Map<String, Fib>> fibs = new TreeMap<>();
    SortedMap<String, SortedMap<String, Map<Prefix, Map<String, Set<String>>>>> prefixTracing =
        new TreeMap<>();
    Map<String, Map<String, IpSpace>> arpReplies = new HashMap<>();
    Map<String, Map<String, VrfForwardingBehavior>> vrfForwardingBehavior = new HashMap<>();
    for (DataPlane dp : parts) {
      copyInto(dp.getBgpRoutes(), bgpRoutes);
      copyInto(dp.getBgpBackupRoutes(), bgpBackupRoutes);
      copyInto(dp.getEvpnRoutes(), evpnRoutes);
      copyInto(dp.getEvpnBackupRoutes(), evpnBackupRoutes);
      copyInto(dp.getLayer2Vnis(), layer2Vnis);
      copyInto(dp.getLayer3Vnis(), layer3Vnis);
      copyInto(dp.getRibs(), ribs);
      fibs.putAll(dp.getFibs());
      prefixTracing.putAll(dp.getPrefixTracingInfoSummary());
      arpReplies.putAll(dp.getForwardingAnalysis().getArpReplies());
      vrfForwardingBehavior.putAll(dp.getForwardingAnalysis().getVrfForwardingBehavior());
    }
    return new S2MergedDataPlane(
        ImmutableTable.copyOf(bgpRoutes),
        ImmutableTable.copyOf(bgpBackupRoutes),
        ImmutableTable.copyOf(evpnRoutes),
        ImmutableTable.copyOf(evpnBackupRoutes),
        ImmutableMap.copyOf(fibs),
        ForwardingAnalysisImpl.of(arpReplies, vrfForwardingBehavior),
        ImmutableTable.copyOf(layer2Vnis),
        ImmutableTable.copyOf(layer3Vnis),
        prefixTracing,
        ImmutableTable.copyOf(ribs));
  }

  private static <R, C, V> void copyInto(Table<R, C, V> source, Table<R, C, V> target) {
    source
        .cellSet()
        .forEach(cell -> target.put(cell.getRowKey(), cell.getColumnKey(), cell.getValue()));
  }

  @Override
  public @Nonnull Table<String, String, Set<Bgpv4Route>> getBgpRoutes() {
    return _bgpRoutes;
  }

  @Override
  public @Nonnull Table<String, String, Set<Bgpv4Route>> getBgpBackupRoutes() {
    return _bgpBackupRoutes;
  }

  @Override
  public @Nonnull Table<String, String, Set<EvpnRoute<?, ?>>> getEvpnRoutes() {
    return _evpnRoutes;
  }

  @Override
  public @Nonnull Table<String, String, Set<EvpnRoute<?, ?>>> getEvpnBackupRoutes() {
    return _evpnBackupRoutes;
  }

  @Override
  public @Nonnull Map<String, Map<String, Fib>> getFibs() {
    return _fibs;
  }

  @Override
  public @Nonnull ForwardingAnalysis getForwardingAnalysis() {
    return _forwardingAnalysis;
  }

  @Override
  public @Nonnull Table<String, String, Set<Layer2Vni>> getLayer2Vnis() {
    return _layer2Vnis;
  }

  @Override
  public @Nonnull Table<String, String, Set<Layer3Vni>> getLayer3Vnis() {
    return _layer3Vnis;
  }

  @Override
  public @Nonnull SortedMap<String, SortedMap<String, Map<Prefix, Map<String, Set<String>>>>>
      getPrefixTracingInfoSummary() {
    return _prefixTracingInfoSummary;
  }

  @Override
  public @Nonnull Table<String, String, FinalMainRib> getRibs() {
    return _ribs;
  }
}
