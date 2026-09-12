package org.batfish.dataplane.ibdp;

import java.util.Map;
import org.batfish.datamodel.Configuration;

/**
 * {@link IncrementalBdpEngine} that builds {@link DistributedNode}s supplied by the S2 controller
 * instead of plain {@link Node}s.
 *
 * <p>The engine logic is otherwise stock Batfish. Only the set of virtual routers it iterates
 * differs, because {@link DistributedNode#getVirtualRouters()} hides shadow nodes.
 */
public class S2BdpEngine extends IncrementalBdpEngine {

  private final Map<String, DistributedNode> _nodes;

  public S2BdpEngine(IncrementalDataPlaneSettings settings, Map<String, DistributedNode> nodes) {
    super(settings);
    _nodes = nodes;
  }

  @Override
  Node newNode(Configuration configuration) {
    DistributedNode node = _nodes.get(configuration.getHostname());
    if (node == null) {
      throw new IllegalStateException("No S2 node prepared for " + configuration.getHostname());
    }
    return node;
  }
}
