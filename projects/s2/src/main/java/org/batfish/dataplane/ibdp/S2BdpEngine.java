package org.batfish.dataplane.ibdp;

import com.google.common.collect.ImmutableList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.SortedMap;
import org.batfish.common.topology.IpOwners;
import org.batfish.datamodel.Configuration;

/**
 * {@link IncrementalBdpEngine} that builds {@link DistributedNode}s supplied by the S2 controller
 * instead of plain {@link Node}s, and simulates only the nodes this worker owns.
 */
public class S2BdpEngine extends IncrementalBdpEngine {

  /**
   * Serializes dataplane construction across engines running in the same JVM. Shadow nodes delegate
   * to real routers owned by other engines, and {@link VirtualRouter#computeFib()} transiently nulls
   * the FIB, so concurrent construction could observe a null FIB.
   */
  private static final Object DATAPLANE_LOCK = new Object();

  private final Map<String, DistributedNode> _nodes;
  private final S2Coordinator _coordinator;

  public S2BdpEngine(
      IncrementalDataPlaneSettings settings,
      Map<String, DistributedNode> nodes,
      S2Coordinator coordinator) {
    super(settings);
    _nodes = nodes;
    _coordinator = coordinator;
  }

  /**
   * S2 convergence is global: a worker must keep computing until no real router anywhere is dirty,
   * otherwise it may stop before a remote worker has finished propagating its routes.
   */
  @Override
  protected boolean hasNotReachedRoutingFixedPoint(List<VirtualRouter> vrs) {
    return _coordinator.roundCheck(super.hasNotReachedRoutingFixedPoint(vrs));
  }

  @Override
  Node newNode(Configuration configuration) {
    DistributedNode node = _nodes.get(configuration.getHostname());
    if (node == null) {
      throw new IllegalStateException("No S2 node prepared for " + configuration.getHostname());
    }
    return node;
  }

  /** Only real nodes are simulated locally; shadows are visible for lookups but inert. */
  @Override
  Collection<VirtualRouter> iterationVirtualRouters(Node node) {
    return ((DistributedNode) node).isShadow() ? ImmutableList.of() : node.getVirtualRouters();
  }

  @Override
  protected boolean checkBgpSessionReachability() {
    // Shadow nodes do not yet receive remote FIBs, so dataplane-level reachability checks would
    // wrongly prune sessions. Sessions are established from configuration + L3 adjacency.
    return false;
  }

  @Override
  protected PartialDataplane nextDataplane(
      TopologyContext currentTopologyContext,
      SortedMap<String, Node> nodes,
      List<VirtualRouter> vrs,
      IpOwners currentIpOwners) {
    synchronized (DATAPLANE_LOCK) {
      // Ensure every visible router (including those delegated through shadows) has a FIB before the
      // forwarding analysis is built over all nodes.
      nodes.values().stream()
          .flatMap(n -> n.getVirtualRouters().stream())
          .forEach(VirtualRouter::computeFib);
      return super.nextDataplane(currentTopologyContext, nodes, vrs, currentIpOwners);
    }
  }
}
