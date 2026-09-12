package org.batfish.dataplane.ibdp;

import java.util.List;
import org.batfish.datamodel.AnnotatedRoute;
import org.batfish.datamodel.AbstractRoute;
import org.batfish.datamodel.NetworkConfigurations;
import org.batfish.datamodel.bgp.BgpTopology;
import org.batfish.dataplane.rib.Rib;

/** Shared sidecar handler that answers both route-exchange and main-RIB (FIB) requests. */
final class S2SidecarHandlers {

  private S2SidecarHandlers() {}

  static S2SidecarServer.Handler forWorker(
      java.util.Map<String, Node> nodeMap,
      BgpTopology bgpTopology,
      NetworkConfigurations networkConfigurations) {
    return request -> {
      if (request instanceof S2Messages.RoutesRequest) {
        S2Messages.RoutesRequest routesRequest = (S2Messages.RoutesRequest) request;
        BgpRoutingProcess process =
            nodeMap
                .get(routesRequest.hostname)
                .getVirtualRouterOrThrow(routesRequest.vrf)
                .getBgpRoutingProcess();
        return new S2Messages.RoutesResponse(
            process
                .getOutgoingRoutesForEdge(
                    routesRequest.edgeId,
                    nodeMap,
                    bgpTopology,
                    networkConfigurations,
                    routesRequest.isNewSession)
                .toList());
      }
      if (request instanceof S2Messages.MainRibRequest) {
        S2Messages.MainRibRequest ribRequest = (S2Messages.MainRibRequest) request;
        Rib rib =
            nodeMap
                .get(ribRequest.hostname)
                .getVirtualRouterOrThrow(ribRequest.vrf)
                .getMainRib();
        List<AnnotatedRoute<AbstractRoute>> routes =
            new java.util.ArrayList<>(rib.getRoutes());
        return new S2Messages.MainRibResponse(routes);
      }
      throw new IllegalArgumentException("Unknown sidecar request " + request.getClass());
    };
  }
}
