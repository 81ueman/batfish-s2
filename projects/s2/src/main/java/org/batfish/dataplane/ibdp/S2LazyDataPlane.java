// SPDX-License-Identifier: Apache-2.0

package org.batfish.dataplane.ibdp;

import com.google.common.collect.HashBasedTable;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.ImmutableTable;
import com.google.common.collect.Table;
import com.google.common.collect.Table.Cell;
import java.util.AbstractMap;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.function.Function;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import org.batfish.datamodel.Bgpv4Route;
import org.batfish.datamodel.DataPlane;
import org.batfish.datamodel.EvpnRoute;
import org.batfish.datamodel.Fib;
import org.batfish.datamodel.FinalMainRib;
import org.batfish.datamodel.ForwardingAnalysis;
import org.batfish.datamodel.IpSpace;
import org.batfish.datamodel.Prefix;
import org.batfish.datamodel.VrfForwardingBehavior;
import org.batfish.datamodel.vxlan.Layer2Vni;
import org.batfish.datamodel.vxlan.Layer3Vni;
import org.batfish.storage.HostDataPlaneSlice;

/**
 * A {@link DataPlane} assembled lazily from the per-host slices of a distributed S2 run.
 *
 * <p>In the default owned-only mode each worker computes full RIBs/FIBs only for the nodes it owns,
 * so the worker slices are disjoint and their union is the global data plane. Node-local access (a
 * specific row/host, which is what a node-scoped stock answerer uses) is resolved directly from the
 * owning host's slice without materializing the others; whole-network iteration falls back to a
 * cached materialized union.
 *
 * <p>The slices come from a pluggable {@link S2HostSlices}: today the in-process workers, and once
 * the workers are remote (Kubernetes), a shared-storage source that fetches a host's slice on
 * demand. That is what makes a data plane that does not fit in one JVM answerable.
 */
final class S2LazyDataPlane implements DataPlane {

  private final @Nonnull S2HostSlices _slices;

  private transient @Nullable SortedMap<
          String, SortedMap<String, Map<Prefix, Map<String, Set<String>>>>>
      _prefixTracingInfoSummary;

  private S2LazyDataPlane(S2HostSlices slices) {
    _slices = slices;
  }

  static @Nonnull DataPlane of(S2HostSlices slices) {
    return new S2LazyDataPlane(slices);
  }

  @Override
  public @Nonnull Table<String, String, Set<Bgpv4Route>> getBgpRoutes() {
    return new S2LazyTable<>(_slices, HostDataPlaneSlice::getBgpRoutes);
  }

  @Override
  public @Nonnull Table<String, String, Set<Bgpv4Route>> getBgpBackupRoutes() {
    return new S2LazyTable<>(_slices, HostDataPlaneSlice::getBgpBackupRoutes);
  }

  @Override
  public @Nonnull Table<String, String, Set<EvpnRoute<?, ?>>> getEvpnRoutes() {
    return new S2LazyTable<>(_slices, HostDataPlaneSlice::getEvpnRoutes);
  }

  @Override
  public @Nonnull Table<String, String, Set<EvpnRoute<?, ?>>> getEvpnBackupRoutes() {
    return new S2LazyTable<>(_slices, HostDataPlaneSlice::getEvpnBackupRoutes);
  }

  @Override
  public @Nonnull Map<String, Map<String, Fib>> getFibs() {
    return new S2LazyMap<>(_slices, HostDataPlaneSlice::getFibs);
  }

  @Override
  public @Nonnull ForwardingAnalysis getForwardingAnalysis() {
    return new S2LazyForwardingAnalysis(_slices);
  }

  @Override
  public @Nonnull Table<String, String, Set<Layer2Vni>> getLayer2Vnis() {
    return new S2LazyTable<>(_slices, HostDataPlaneSlice::getLayer2Vnis);
  }

  @Override
  public @Nonnull Table<String, String, Set<Layer3Vni>> getLayer3Vnis() {
    return new S2LazyTable<>(_slices, HostDataPlaneSlice::getLayer3Vnis);
  }

  @Override
  public @Nonnull SortedMap<String, SortedMap<String, Map<Prefix, Map<String, Set<String>>>>>
      getPrefixTracingInfoSummary() {
    // Route tracing is off by default and the summary is small; union it once, on demand.
    SortedMap<String, SortedMap<String, Map<Prefix, Map<String, Set<String>>>>> summary =
        _prefixTracingInfoSummary;
    if (summary == null) {
      SortedMap<String, SortedMap<String, Map<Prefix, Map<String, Set<String>>>>> merged =
          new TreeMap<>();
      for (String host : _slices.hosts()) {
        HostDataPlaneSlice slice = _slices.get(host);
        if (slice != null) {
          merged.put(host, slice.getPrefixTracingInfoSummary());
        }
      }
      summary = merged;
      _prefixTracingInfoSummary = summary;
    }
    return summary;
  }

  @Override
  public @Nonnull Table<String, String, FinalMainRib> getRibs() {
    return new S2LazyTable<>(_slices, HostDataPlaneSlice::getRibs);
  }

  /**
   * A read-only {@link Table} keyed by host, whose per-column content comes from the owning host's
   * slice. Point/row lookups resolve only the addressed host; aggregate views fall back to a cached
   * materialized union.
   */
  private static final class S2LazyTable<C, V> implements Table<String, C, V> {
    private final @Nonnull S2HostSlices _slices;
    private final @Nonnull Function<HostDataPlaneSlice, Map<C, V>> _byHost;
    private transient @Nullable Table<String, C, V> _materialized;

    S2LazyTable(S2HostSlices slices, Function<HostDataPlaneSlice, Map<C, V>> byHost) {
      _slices = slices;
      _byHost = byHost;
    }

    /** The addressed host's column map, or {@code null} if it has no slice. */
    private @Nullable Map<C, V> rowOf(Object rowKey) {
      if (!(rowKey instanceof String)) {
        return null;
      }
      HostDataPlaneSlice slice = _slices.get((String) rowKey);
      return slice == null ? null : _byHost.apply(slice);
    }

    @Override
    public boolean contains(Object rowKey, Object columnKey) {
      Map<C, V> row = rowOf(rowKey);
      return row != null && row.containsKey(columnKey);
    }

    @Override
    public boolean containsRow(Object rowKey) {
      Map<C, V> row = rowOf(rowKey);
      return row != null && !row.isEmpty();
    }

    @Override
    public boolean containsColumn(Object columnKey) {
      return materialized().containsColumn(columnKey);
    }

    @Override
    public boolean containsValue(Object value) {
      return materialized().containsValue(value);
    }

    @Override
    public @Nullable V get(Object rowKey, Object columnKey) {
      Map<C, V> row = rowOf(rowKey);
      return row == null ? null : row.get(columnKey);
    }

    @Override
    public boolean isEmpty() {
      return size() == 0;
    }

    @Override
    public int size() {
      return materialized().size();
    }

    @Override
    public void clear() {
      throw new UnsupportedOperationException("S2 data plane is read-only");
    }

    @Override
    public V put(String rowKey, C columnKey, V value) {
      throw new UnsupportedOperationException("S2 data plane is read-only");
    }

    @Override
    public void putAll(Table<? extends String, ? extends C, ? extends V> table) {
      throw new UnsupportedOperationException("S2 data plane is read-only");
    }

    @Override
    public V remove(Object rowKey, Object columnKey) {
      throw new UnsupportedOperationException("S2 data plane is read-only");
    }

    @Override
    public Map<C, V> row(String rowKey) {
      Map<C, V> row = rowOf(rowKey);
      return row == null ? ImmutableMap.of() : ImmutableMap.copyOf(row);
    }

    @Override
    public Set<C> columnKeySet() {
      return materialized().columnKeySet();
    }

    @Override
    public Map<String, Map<C, V>> rowMap() {
      return materialized().rowMap();
    }

    @Override
    public Map<C, Map<String, V>> columnMap() {
      return materialized().columnMap();
    }

    @Override
    public Set<Cell<String, C, V>> cellSet() {
      return materialized().cellSet();
    }

    @Override
    public Set<String> rowKeySet() {
      return materialized().rowKeySet();
    }

    @Override
    public Map<String, V> column(C columnKey) {
      return materialized().column(columnKey);
    }

    @Override
    public Collection<V> values() {
      return materialized().values();
    }

    @Override
    public boolean equals(Object obj) {
      return materialized().equals(obj);
    }

    @Override
    public int hashCode() {
      return materialized().hashCode();
    }

    @Override
    public String toString() {
      return materialized().toString();
    }

    private Table<String, C, V> materialized() {
      Table<String, C, V> materialized = _materialized;
      if (materialized == null) {
        Table<String, C, V> union = HashBasedTable.create();
        for (String host : _slices.hosts()) {
          HostDataPlaneSlice slice = _slices.get(host);
          if (slice != null) {
            _byHost.apply(slice).forEach((column, value) -> union.put(host, column, value));
          }
        }
        materialized = ImmutableTable.copyOf(union);
        _materialized = materialized;
      }
      return materialized;
    }
  }

  /**
   * A read-only {@link Map} keyed by host, whose values come from the owning host's slice. Point
   * lookups resolve only the addressed host; aggregate views fall back to a materialized union.
   */
  private static final class S2LazyMap<V> extends AbstractMap<String, V> {
    private final @Nonnull S2HostSlices _slices;
    private final @Nonnull Function<HostDataPlaneSlice, V> _byHost;
    private transient @Nullable Map<String, V> _materialized;

    S2LazyMap(S2HostSlices slices, Function<HostDataPlaneSlice, V> byHost) {
      _slices = slices;
      _byHost = byHost;
    }

    @Override
    public @Nullable V get(Object key) {
      if (!(key instanceof String)) {
        return null;
      }
      HostDataPlaneSlice slice = _slices.get((String) key);
      return slice == null ? null : _byHost.apply(slice);
    }

    @Override
    public boolean containsKey(Object key) {
      return key instanceof String && _slices.get((String) key) != null;
    }

    @Override
    public Set<String> keySet() {
      return ImmutableSet.copyOf(_slices.hosts());
    }

    @Override
    public Set<Entry<String, V>> entrySet() {
      return materialized().entrySet();
    }

    @Override
    public int size() {
      return materialized().size();
    }

    private Map<String, V> materialized() {
      Map<String, V> materialized = _materialized;
      if (materialized == null) {
        Map<String, V> union = new LinkedHashMap<>();
        for (String host : _slices.hosts()) {
          HostDataPlaneSlice slice = _slices.get(host);
          if (slice != null) {
            union.put(host, _byHost.apply(slice));
          }
        }
        materialized = union;
        _materialized = materialized;
      }
      return materialized;
    }
  }

  /** The per-host forwarding analysis assembled from the host slices. */
  private static final class S2LazyForwardingAnalysis implements ForwardingAnalysis {
    private final @Nonnull S2HostSlices _slices;

    S2LazyForwardingAnalysis(S2HostSlices slices) {
      _slices = slices;
    }

    @Override
    public Map<String, Map<String, IpSpace>> getArpReplies() {
      return new S2LazyMap<>(_slices, HostDataPlaneSlice::getArpReplies);
    }

    @Override
    public @Nonnull Map<String, Map<String, VrfForwardingBehavior>> getVrfForwardingBehavior() {
      return new S2LazyMap<>(_slices, HostDataPlaneSlice::getVrfForwardingBehavior);
    }
  }
}
