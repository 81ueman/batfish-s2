// SPDX-License-Identifier: Apache-2.0

package org.batfish.dataplane.ibdp;

import java.util.List;
import java.util.Map;
import org.batfish.dataplane.rib.Rib;

/**
 * Populates shadow nodes' main RIBs with the owning workers' real routes, so each worker can build
 * a complete forwarding analysis (FIBs) — the FIB-distribution half of S2.
 */
public final class ShadowMainRibSync implements Runnable {

  private final Map<String, DistributedNode> _nodes;
  private final Map<String, Integer> _assignment;
  private final int _workerId;
  private final List<S2WorkerEndpoint> _endpoints;
  private final S2SidecarClient _client;

  public ShadowMainRibSync(
      Map<String, DistributedNode> nodes,
      Map<String, Integer> assignment,
      int workerId,
      List<S2WorkerEndpoint> endpoints,
      S2SidecarClient client) {
    _nodes = nodes;
    _assignment = assignment;
    _workerId = workerId;
    _endpoints = endpoints;
    _client = client;
  }

  @Override
  public void run() {
    for (String host : _nodes.keySet()) {
      if (_assignment.get(host) == _workerId) {
        continue;
      }
      DistributedNode shadow = _nodes.get(host);
      S2WorkerEndpoint owner = _endpoints.get(_assignment.get(host));
      for (String vrf : shadow.getConfiguration().getVrfs().keySet()) {
        S2Messages.MainRibResponse response =
            (S2Messages.MainRibResponse)
                _client.call(owner, new S2Messages.MainRibRequest(host, vrf));
        Rib rib = shadow.getVirtualRouterOrThrow(vrf).getMainRib();
        response.routes.forEach(rib::mergeRoute);
      }
    }
  }
}
