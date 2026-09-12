package org.batfish.dataplane.ibdp;

import java.util.Map;
import java.util.stream.Stream;
import org.batfish.datamodel.Bgpv4Route;
import org.batfish.datamodel.NetworkConfigurations;
import org.batfish.datamodel.bgp.BgpTopology;
import org.batfish.dataplane.rib.RouteAdvertisement;

/**
 * Strategy for producing a BGP process's outgoing advertisements for an edge.
 *
 * <p>Stock Batfish computes these locally (see {@link BgpRoutingProcess#getOutgoingRoutesForEdge}).
 * S2 installs an implementation on <b>shadow</b> processes that fetches them from the owning worker
 * over the sidecar, so remote switches can be represented without simulating them locally.
 */
@FunctionalInterface
public interface OutgoingRoutesProvider {
  Stream<RouteAdvertisement<Bgpv4Route>> getOutgoingRoutesForEdge(
      BgpRoutingProcess process,
      BgpTopology.EdgeId edge,
      Map<String, Node> allNodes,
      BgpTopology bgpTopology,
      NetworkConfigurations networkConfigurations,
      boolean isNewSession);
}
