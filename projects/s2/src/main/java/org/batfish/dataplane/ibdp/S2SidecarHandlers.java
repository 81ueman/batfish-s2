// SPDX-License-Identifier: Apache-2.0
package org.batfish.dataplane.ibdp;

import com.google.common.collect.Table;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.function.Supplier;
import org.batfish.bddreachability.BDDReachabilityAnalysis;
import org.batfish.bddreachability.transition.TransitionTransfer;
import org.batfish.datamodel.AbstractRoute;
import org.batfish.datamodel.AnnotatedRoute;
import org.batfish.datamodel.NetworkConfigurations;
import org.batfish.datamodel.bgp.BgpTopology;
import org.batfish.dataplane.rib.Rib;
import org.batfish.symbolic.state.StateExpr;

/**
 * Shared sidecar handler that answers route-exchange, main-RIB (FIB), and boundary-edge requests.
 */
final class S2SidecarHandlers {

  private S2SidecarHandlers() {}

  static S2SidecarServer.Handler forWorker(
      java.util.Map<String, Node> nodeMap,
      BgpTopology bgpTopology,
      NetworkConfigurations networkConfigurations) {
    return forWorker(nodeMap, bgpTopology, networkConfigurations, Set.of(), () -> null);
  }

  static S2SidecarServer.Handler forWorker(
      java.util.Map<String, Node> nodeMap,
      BgpTopology bgpTopology,
      NetworkConfigurations networkConfigurations,
      Set<String> ownedHosts,
      Supplier<BDDReachabilityAnalysis> localAnalysis) {
    S2SidecarServer.Handler boundary = forBoundaryEdges(ownedHosts, localAnalysis);
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
            nodeMap.get(ribRequest.hostname).getVirtualRouterOrThrow(ribRequest.vrf).getMainRib();
        List<AnnotatedRoute<AbstractRoute>> routes = new java.util.ArrayList<>(rib.getRoutes());
        return new S2Messages.MainRibResponse(routes);
      }
      if (request instanceof S2Messages.BoundaryEdgesRequest) {
        return boundary.handle(request);
      }
      // Cross-worker OSPF messages: a shadow process forwards what it would have enqueued on the
      // real neighbor; the owner enqueues it locally.
      if (request instanceof S2Messages.OspfIntraRequest) {
        S2Messages.OspfIntraRequest r = (S2Messages.OspfIntraRequest) request;
        ospfProcess(nodeMap, r).enqueueMessagesIntra(r.edge, r.routes.stream());
        return new S2Messages.Ack();
      }
      if (request instanceof S2Messages.OspfInterRequest) {
        S2Messages.OspfInterRequest r = (S2Messages.OspfInterRequest) request;
        ospfProcess(nodeMap, r).enqueueMessagesInter(r.edge, r.routes);
        return new S2Messages.Ack();
      }
      if (request instanceof S2Messages.OspfType1Request) {
        S2Messages.OspfType1Request r = (S2Messages.OspfType1Request) request;
        ospfProcess(nodeMap, r).enqueueMessagesType1(r.edge, r.routes);
        return new S2Messages.Ack();
      }
      if (request instanceof S2Messages.OspfType2Request) {
        S2Messages.OspfType2Request r = (S2Messages.OspfType2Request) request;
        ospfProcess(nodeMap, r).enqueueMessagesType2(r.edge, r.routes);
        return new S2Messages.Ack();
      }
      throw new IllegalArgumentException("Unknown sidecar request " + request.getClass());
    };
  }

  private static OspfRoutingProcess ospfProcess(
      java.util.Map<String, Node> nodeMap, S2Messages.OspfEnqueueRequest request) {
    OspfRoutingProcess process =
        nodeMap
            .get(request.hostname)
            .getVirtualRouterOrThrow(request.vrf)
            .getOspfProcesses()
            .get(request.process);
    if (process == null) {
      throw new IllegalStateException(
          "No OSPF process " + request.process + " on " + request.hostname + "/" + request.vrf);
    }
    return process;
  }

  /**
   * Serves {@link S2Messages.BoundaryEdgesRequest}: returns this worker's reachability edges whose
   * source is owned locally and whose destination is owned by the requester. In the backward
   * fixpoint the requester cannot generate these (their transition depends on the source node's
   * forwarding behavior), so it pulls them here and reconstructs the transition in its own BDD
   * factory.
   */
  static S2SidecarServer.Handler forBoundaryEdges(
      Set<String> ownedHosts, Supplier<BDDReachabilityAnalysis> localAnalysis) {
    return request -> {
      S2Messages.BoundaryEdgesRequest boundaryRequest = (S2Messages.BoundaryEdgesRequest) request;
      BDDReachabilityAnalysis analysis = localAnalysis.get();
      if (analysis == null) {
        throw new IllegalStateException(
            "Boundary edge request before local analysis was ready; server owns "
                + ownedHosts
                + " requester owns "
                + boundaryRequest.ownedHosts);
      }
      List<S2Messages.SerializedEdge> edges = new ArrayList<>();
      try {
        for (Table.Cell<StateExpr, StateExpr, org.batfish.bddreachability.transition.Transition>
            cell : analysis.getForwardEdgeTable().cellSet()) {
          StateExpr pre = cell.getRowKey();
          StateExpr post = cell.getColumnKey();
          String preHost = S2StateHosts.hostname(pre);
          String postHost = S2StateHosts.hostname(post);
          if (postHost.isEmpty() || !boundaryRequest.ownedHosts.contains(postHost)) {
            continue;
          }
          if (!ownedHosts.contains(preHost)) {
            // Only serve edges this worker generated (its own source); peers own the rest.
            continue;
          }
          edges.add(
              new S2Messages.SerializedEdge(pre, post, TransitionTransfer.save(cell.getValue())));
        }
      } catch (IOException e) {
        throw new RuntimeException("Failed to serialize a boundary edge transition", e);
      }
      return new S2Messages.BoundaryEdgesResponse(edges);
    };
  }
}
