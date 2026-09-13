package org.batfish.dataplane.ibdp;

import java.util.List;
import org.batfish.datamodel.OspfExternalType1Route;
import org.batfish.datamodel.OspfExternalType2Route;
import org.batfish.datamodel.OspfInterAreaRoute;
import org.batfish.datamodel.OspfIntraAreaRoute;
import org.batfish.datamodel.ospf.OspfTopology.EdgeId;
import org.batfish.dataplane.rib.RouteAdvertisement;

/**
 * S2 shadow-side {@link OspfRoutingProcess.EnqueueProvider}: forwards OSPF messages that the real
 * neighbor process would have enqueued onto itself to the owning worker over the sidecar, where the
 * real process enqueues them locally.
 */
final class RemoteOspfEnqueueProvider implements OspfRoutingProcess.EnqueueProvider {

  private final S2SidecarClient _client;
  private final S2WorkerEndpoint _owner;
  private final String _hostname;
  private final String _vrf;
  private final String _process;

  RemoteOspfEnqueueProvider(
      S2SidecarClient client, S2WorkerEndpoint owner, String hostname, String vrf, String process) {
    _client = client;
    _owner = owner;
    _hostname = hostname;
    _vrf = vrf;
    _process = process;
  }

  @Override
  public void enqueueIntra(EdgeId edge, List<RouteAdvertisement<OspfIntraAreaRoute>> routes) {
    _client.call(_owner, new S2Messages.OspfIntraRequest(_hostname, _vrf, _process, edge, routes));
  }

  @Override
  public void enqueueInter(EdgeId edge, List<RouteAdvertisement<OspfInterAreaRoute>> routes) {
    _client.call(_owner, new S2Messages.OspfInterRequest(_hostname, _vrf, _process, edge, routes));
  }

  @Override
  public void enqueueType1(EdgeId edge, List<RouteAdvertisement<OspfExternalType1Route>> routes) {
    _client.call(_owner, new S2Messages.OspfType1Request(_hostname, _vrf, _process, edge, routes));
  }

  @Override
  public void enqueueType2(EdgeId edge, List<RouteAdvertisement<OspfExternalType2Route>> routes) {
    _client.call(_owner, new S2Messages.OspfType2Request(_hostname, _vrf, _process, edge, routes));
  }
}
