// SPDX-License-Identifier: Apache-2.0

package org.batfish.dataplane.ibdp;

import com.google.common.collect.HashBasedTable;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.ImmutableTable;
import com.google.common.collect.Table;
import com.google.common.collect.Table.Cell;
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
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
import org.batfish.datamodel.IpSpace;
import org.batfish.datamodel.Prefix;
import org.batfish.datamodel.VrfForwardingBehavior;
import org.batfish.datamodel.vxlan.Layer2Vni;
import org.batfish.datamodel.vxlan.Layer3Vni;

/**
 * A {@link DataPlane} assembled lazily from the per-worker data planes of a distributed S2 run.
 *
 * <p>In the default owned-only mode each worker computes full RIBs/FIBs only for the nodes it owns,
 * so the workers' data planes are disjoint and their union is the global data plane. Node-local
 * access (a specific row/host, which is what a node-scoped stock answerer uses) is served directly
 * from the owning worker without materializing the other nodes; whole-network iteration falls back
 * to a cached materialized union.
 *
 * <p>Today the parts are the in-process workers, so this bounds the <em>container</em> memory but
 * not the node data (which lives in the same JVM). When the workers become remote (Kubernetes), the
 * same views fetch a host's slice from its owner on demand, which is what makes a data plane that
 * does not fit in one JVM possible.
 */
final class S2LazyDataPlane implements DataPlane {

  private final @Nonnull List<DataPlane> _parts;

  private S2LazyDataPlane(List<DataPlane> parts) {
    _parts = parts;
  }

  static @Nonnull DataPlane of(List<DataPlane> parts) {
    return new S2LazyDataPlane(parts);
  }

  @Override
  public @Nonnull Table<String, String, Set<Bgpv4Route>> getBgpRoutes() {
    List<Table<String, String, Set<Bgpv4Route>>> tables = new ArrayList<>();
    _parts.forEach(p -> tables.add(p.getBgpRoutes()));
    return new S2LazyTable<>(tables);
  }

  @Override
  public @Nonnull Table<String, String, Set<Bgpv4Route>> getBgpBackupRoutes() {
    List<Table<String, String, Set<Bgpv4Route>>> tables = new ArrayList<>();
    _parts.forEach(p -> tables.add(p.getBgpBackupRoutes()));
    return new S2LazyTable<>(tables);
  }

  @Override
  public @Nonnull Table<String, String, Set<EvpnRoute<?, ?>>> getEvpnRoutes() {
    List<Table<String, String, Set<EvpnRoute<?, ?>>>> tables = new ArrayList<>();
    _parts.forEach(p -> tables.add(p.getEvpnRoutes()));
    return new S2LazyTable<>(tables);
  }

  @Override
  public @Nonnull Table<String, String, Set<EvpnRoute<?, ?>>> getEvpnBackupRoutes() {
    List<Table<String, String, Set<EvpnRoute<?, ?>>>> tables = new ArrayList<>();
    _parts.forEach(p -> tables.add(p.getEvpnBackupRoutes()));
    return new S2LazyTable<>(tables);
  }

  @Override
  public @Nonnull Map<String, Map<String, Fib>> getFibs() {
    List<Map<String, Map<String, Fib>>> maps = new ArrayList<>();
    _parts.forEach(p -> maps.add(p.getFibs()));
    return new S2LazyMap<>(maps);
  }

  @Override
  public @Nonnull ForwardingAnalysis getForwardingAnalysis() {
    List<ForwardingAnalysis> analyses = new ArrayList<>();
    _parts.forEach(p -> analyses.add(p.getForwardingAnalysis()));
    return new S2LazyForwardingAnalysis(analyses);
  }

  @Override
  public @Nonnull Table<String, String, Set<Layer2Vni>> getLayer2Vnis() {
    List<Table<String, String, Set<Layer2Vni>>> tables = new ArrayList<>();
    _parts.forEach(p -> tables.add(p.getLayer2Vnis()));
    return new S2LazyTable<>(tables);
  }

  @Override
  public @Nonnull Table<String, String, Set<Layer3Vni>> getLayer3Vnis() {
    List<Table<String, String, Set<Layer3Vni>>> tables = new ArrayList<>();
    _parts.forEach(p -> tables.add(p.getLayer3Vnis()));
    return new S2LazyTable<>(tables);
  }

  @Override
  public @Nonnull SortedMap<String, SortedMap<String, Map<Prefix, Map<String, Set<String>>>>>
      getPrefixTracingInfoSummary() {
    // Route tracing is off by default and the summary is small; union it eagerly.
    SortedMap<String, SortedMap<String, Map<Prefix, Map<String, Set<String>>>>> merged =
        new TreeMap<>();
    _parts.forEach(p -> merged.putAll(p.getPrefixTracingInfoSummary()));
    return merged;
  }

  @Override
  public @Nonnull Table<String, String, FinalMainRib> getRibs() {
    List<Table<String, String, FinalMainRib>> tables = new ArrayList<>();
    _parts.forEach(p -> tables.add(p.getRibs()));
    return new S2LazyTable<>(tables);
  }

  /**
   * A read-only {@link Table} that is the union of several tables. Point/row lookups resolve
   * directly against the owning table; aggregate views fall back to a cached materialized union.
   */
  private static final class S2LazyTable<R, C, V> implements Table<R, C, V> {
    private final @Nonnull List<Table<R, C, V>> _tables;
    private transient Table<R, C, V> _materialized;

    S2LazyTable(List<Table<R, C, V>> tables) {
      _tables = tables;
    }

    @Override
    public boolean contains(Object rowKey, Object columnKey) {
      for (Table<R, C, V> table : _tables) {
        if (table.contains(rowKey, columnKey)) {
          return true;
        }
      }
      return false;
    }

    @Override
    public boolean containsRow(Object rowKey) {
      for (Table<R, C, V> table : _tables) {
        if (table.containsRow(rowKey)) {
          return true;
        }
      }
      return false;
    }

    @Override
    public boolean containsColumn(Object columnKey) {
      for (Table<R, C, V> table : _tables) {
        if (table.containsColumn(columnKey)) {
          return true;
        }
      }
      return false;
    }

    @Override
    public boolean containsValue(Object value) {
      return materialized().containsValue(value);
    }

    @Override
    public V get(Object rowKey, Object columnKey) {
      for (Table<R, C, V> table : _tables) {
        if (table.contains(rowKey, columnKey)) {
          return table.get(rowKey, columnKey);
        }
      }
      return null;
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
    public V put(R rowKey, C columnKey, V value) {
      throw new UnsupportedOperationException("S2 data plane is read-only");
    }

    @Override
    public void putAll(Table<? extends R, ? extends C, ? extends V> table) {
      throw new UnsupportedOperationException("S2 data plane is read-only");
    }

    @Override
    public V remove(Object rowKey, Object columnKey) {
      throw new UnsupportedOperationException("S2 data plane is read-only");
    }

    @Override
    public Map<C, V> row(R rowKey) {
      Map<C, V> merged = null;
      for (Table<R, C, V> table : _tables) {
        if (table.containsRow(rowKey)) {
          if (merged == null) {
            merged = new LinkedHashMap<>(table.row(rowKey));
          } else {
            merged.putAll(table.row(rowKey));
          }
        }
      }
      return merged == null ? ImmutableMap.of() : merged;
    }

    @Override
    public Set<C> columnKeySet() {
      return materialized().columnKeySet();
    }

    @Override
    public Map<R, Map<C, V>> rowMap() {
      return materialized().rowMap();
    }

    @Override
    public Map<C, Map<R, V>> columnMap() {
      return materialized().columnMap();
    }

    @Override
    public Set<Cell<R, C, V>> cellSet() {
      return materialized().cellSet();
    }

    @Override
    public Set<R> rowKeySet() {
      Set<R> rows = new LinkedHashSet<>();
      _tables.forEach(t -> rows.addAll(t.rowKeySet()));
      return ImmutableSet.copyOf(rows);
    }

    @Override
    public Map<R, V> column(C columnKey) {
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

    private Table<R, C, V> materialized() {
      if (_materialized == null) {
        Table<R, C, V> union = HashBasedTable.create();
        _tables.forEach(union::putAll);
        _materialized = ImmutableTable.copyOf(union);
      }
      return _materialized;
    }
  }

  /** A read-only {@link Map} that is the union of several maps, resolved lazily per key. */
  private static final class S2LazyMap<K, V> extends AbstractMap<K, V> {
    private final @Nonnull List<Map<K, V>> _maps;
    private transient Map<K, V> _materialized;

    S2LazyMap(List<Map<K, V>> maps) {
      _maps = maps;
    }

    @Override
    public V get(Object key) {
      for (Map<K, V> map : _maps) {
        if (map.containsKey(key)) {
          return map.get(key);
        }
      }
      return null;
    }

    @Override
    public boolean containsKey(Object key) {
      for (Map<K, V> map : _maps) {
        if (map.containsKey(key)) {
          return true;
        }
      }
      return false;
    }

    @Override
    public Set<K> keySet() {
      Set<K> keys = new LinkedHashSet<>();
      _maps.forEach(m -> keys.addAll(m.keySet()));
      return ImmutableSet.copyOf(keys);
    }

    @Override
    public Set<Entry<K, V>> entrySet() {
      return materialized().entrySet();
    }

    @Override
    public int size() {
      return materialized().size();
    }

    private Map<K, V> materialized() {
      if (_materialized == null) {
        Map<K, V> union = new LinkedHashMap<>();
        _maps.forEach(union::putAll);
        _materialized = union;
      }
      return _materialized;
    }
  }

  /** The union of several per-worker forwarding analyses, resolved lazily per host. */
  private static final class S2LazyForwardingAnalysis implements ForwardingAnalysis {
    private final @Nonnull List<ForwardingAnalysis> _parts;

    S2LazyForwardingAnalysis(List<ForwardingAnalysis> parts) {
      _parts = parts;
    }

    @Override
    public Map<String, Map<String, IpSpace>> getArpReplies() {
      List<Map<String, Map<String, IpSpace>>> maps = new ArrayList<>();
      _parts.forEach(p -> maps.add(p.getArpReplies()));
      return new S2LazyMap<>(maps);
    }

    @Override
    public @Nonnull Map<String, Map<String, VrfForwardingBehavior>> getVrfForwardingBehavior() {
      List<Map<String, Map<String, VrfForwardingBehavior>>> maps = new ArrayList<>();
      _parts.forEach(p -> maps.add(p.getVrfForwardingBehavior()));
      return new S2LazyMap<>(maps);
    }
  }
}
