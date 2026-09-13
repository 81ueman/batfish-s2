package org.batfish.storage;

import com.google.common.collect.ImmutableMap;
import java.io.Serializable;
import java.util.Map;
import java.util.Set;
import java.util.SortedMap;
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
 * Public, serializable view of a single host's slice of the data plane.
 *
 * <p>Batfish already serializes the data plane one host at a time as {@link PerHostDataPlane}
 * (package-private) inside {@link FileBasedStorage#storeDataPlane}. This class re-exposes that
 * exact granularity as a public type, so a data-plane consumer can resolve a host's content from a
 * pluggable (possibly remote) source without materializing the whole data plane.
 *
 * <p>This is purely additive: the on-disk format, the stock {@code ibdp} engine, and the default
 * behavior are unchanged.
 */
public final class HostDataPlaneSlice implements Serializable {

  private static final long serialVersionUID = 1L;

  private final @Nonnull PerHostDataPlane _slice;

  private HostDataPlaneSlice(@Nonnull PerHostDataPlane slice) {
    _slice = slice;
  }

  /**
   * The hosts that have a data-plane slice in {@code dataPlane}. Mirrors {@link
   * FileBasedStorage#storeDataPlane}, which keys the per-host slices on the FIB hosts.
   */
  public static @Nonnull Set<String> hosts(DataPlane dataPlane) {
    return dataPlane.getFibs().keySet();
  }

  /**
   * Extract one host's slice from a whole data plane, mirroring how {@link
   * FileBasedStorage#storeDataPlane} reads the per-host content: the host's rows of the route/VNI
   * tables, its FIBs, its ARP-reply and VRF-forwarding state, its prefix-tracing summary, and its
   * main RIBs.
   */
  public static @Nonnull HostDataPlaneSlice extract(DataPlane dataPlane, String host) {
    ForwardingAnalysis forwardingAnalysis = dataPlane.getForwardingAnalysis();
    return new HostDataPlaneSlice(
        new PerHostDataPlane(
            dataPlane.getBgpRoutes().row(host),
            dataPlane.getBgpBackupRoutes().row(host),
            dataPlane.getEvpnRoutes().row(host),
            dataPlane.getEvpnBackupRoutes().row(host),
            dataPlane.getFibs().get(host),
            forwardingAnalysis.getArpReplies().getOrDefault(host, ImmutableMap.of()),
            forwardingAnalysis.getVrfForwardingBehavior().getOrDefault(host, ImmutableMap.of()),
            dataPlane.getLayer2Vnis().row(host),
            dataPlane.getLayer3Vnis().row(host),
            dataPlane.getPrefixTracingInfoSummary().get(host),
            dataPlane.getRibs().row(host)));
  }

  public @Nonnull Map<String, Set<Bgpv4Route>> getBgpRoutes() {
    return _slice.getBgpRoutes();
  }

  public @Nonnull Map<String, Set<Bgpv4Route>> getBgpBackupRoutes() {
    return _slice.getBgpBackupRoutes();
  }

  public @Nonnull Map<String, Set<EvpnRoute<?, ?>>> getEvpnRoutes() {
    return _slice.getEvpnRoutes();
  }

  public @Nonnull Map<String, Set<EvpnRoute<?, ?>>> getEvpnBackupRoutes() {
    return _slice.getEvpnBackupRoutes();
  }

  public @Nonnull Map<String, Fib> getFibs() {
    return _slice.getFibs();
  }

  public @Nonnull Map<String, IpSpace> getArpReplies() {
    return _slice.getArpReplies();
  }

  public @Nonnull Map<String, VrfForwardingBehavior> getVrfForwardingBehavior() {
    return _slice.getVrfForwardingBehavior();
  }

  public @Nonnull Map<String, Set<Layer2Vni>> getLayer2Vnis() {
    return _slice.getLayer2Vnis();
  }

  public @Nonnull Map<String, Set<Layer3Vni>> getLayer3Vnis() {
    return _slice.getLayer3Vnis();
  }

  public @Nonnull SortedMap<String, Map<Prefix, Map<String, Set<String>>>>
      getPrefixTracingInfoSummary() {
    return _slice.getPrefixTracingInfoSummary();
  }

  public @Nonnull Map<String, FinalMainRib> getRibs() {
    return _slice.getRibs();
  }
}
