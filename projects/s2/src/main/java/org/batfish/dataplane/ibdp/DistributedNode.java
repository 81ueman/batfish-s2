package org.batfish.dataplane.ibdp;

import com.google.common.collect.ImmutableList;
import java.util.Collection;
import org.batfish.datamodel.Configuration;

/**
 * A {@link Node} that participates in S2's distributed computation.
 *
 * <p>Exactly one worker "owns" a given switch and simulates it as a <b>real</b> node. Every other
 * worker builds the same node as a <b>shadow</b>. A shadow node still exposes its virtual routers
 * to neighbor lookup ({@link #getVirtualRouterOrThrow}), but returns an empty collection from {@link
 * #getVirtualRouters()}, so Batfish's engine never simulates it. Its BGP process is patched (see
 * {@link #wireShadowBgpProcessesFrom}) to delegate route lookups to the real process on the owning
 * worker.
 */
public class DistributedNode extends Node {

  private final boolean _shadow;

  public DistributedNode(Configuration configuration, boolean shadow) {
    super(configuration);
    _shadow = shadow;
  }

  public boolean isShadow() {
    return _shadow;
  }

  /** Only real nodes are iterated by the data-plane engine; shadow nodes are inert. */
  @Override
  Collection<VirtualRouter> getVirtualRouters() {
    return _shadow ? ImmutableList.of() : super.getVirtualRouters();
  }

  /**
   * Replace this (shadow) node's BGP processes with the corresponding real processes from the owning
   * worker, so a neighbor lookup ({@code getOutgoingRoutesForEdge}) yields the real advertisements.
   *
   * <p>In milestone 1 (single JVM) the real process object is shared directly. Milestone 2 replaces
   * this with a sidecar RPC, which is why the call boundary is isolated here.
   */
  public void wireShadowBgpProcessesFrom(DistributedNode realNode) {
    if (!_shadow) {
      throw new IllegalStateException("wireShadowBgpProcessesFrom called on a real node");
    }
    for (String vrf : realNode.getConfiguration().getVrfs().keySet()) {
      VirtualRouter realVr = realNode.getVirtualRouterOrThrow(vrf);
      VirtualRouter shadowVr = getVirtualRouterOrThrow(vrf);
      shadowVr._bgpRoutingProcess = realVr.getBgpRoutingProcess();
    }
  }
}
