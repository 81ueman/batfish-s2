package org.batfish.dataplane.ibdp;

import java.util.Map;
import java.util.stream.Stream;
import org.batfish.datamodel.Bgpv4Route;
import org.batfish.datamodel.NetworkConfigurations;
import org.batfish.datamodel.bgp.BgpTopology;
import org.batfish.dataplane.rib.RouteAdvertisement;

/** {@link OutgoingRoutesProvider} for a shadow process that lives on another worker. */
final class RemoteOutgoingRoutesProvider implements OutgoingRoutesProvider {

  private final S2SidecarClient _client;
  private final S2WorkerEndpoint _owner;
  private final String _hostname;
  private final String _vrf;

  RemoteOutgoingRoutesProvider(
      S2SidecarClient client, S2WorkerEndpoint owner, String hostname, String vrf) {
    _client = client;
    _owner = owner;
    _hostname = hostname;
    _vrf = vrf;
  }

  @Override
  public Stream<RouteAdvertisement<Bgpv4Route>> getOutgoingRoutesForEdge(
      BgpRoutingProcess process,
      BgpTopology.EdgeId edge,
      Map<String, Node> allNodes,
      BgpTopology bgpTopology,
      NetworkConfigurations networkConfigurations,
      boolean isNewSession) {
    return _client
        .fetch(_owner, new S2Messages.RoutesRequest(_hostname, _vrf, edge, isNewSession))
        .stream();
  }
}
