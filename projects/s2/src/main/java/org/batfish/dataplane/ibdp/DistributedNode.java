package org.batfish.dataplane.ibdp;

import java.util.Collection;
import java.util.Optional;
import javax.annotation.Nullable;
import org.batfish.datamodel.Configuration;

/**
 * A {@link Node} that participates in S2's distributed computation.
 *
 * <p>Exactly one worker "owns" a switch and simulates it as a <b>real</b> node. Every other worker
 * holds a <b>shadow</b> node for the same switch. A shadow node delegates its virtual routers to
 * the owning worker's real node, so:
 *
 * <ul>
 *   <li>route lookups by neighbors ({@link #getVirtualRouterOrThrow}) return the real BGP process;
 *   <li>dataplane construction (FIBs, forwarding analysis) sees the real forwarding state;
 *   <li>but the data-plane engine must not <i>iterate</i> a shadow. That is decided by {@link
 *       S2BdpEngine#iterationVirtualRouters}, which returns no routers for shadows.
 * </ul>
 */
public class DistributedNode extends Node {

  /** Non-null iff this node is a shadow; points at the real node on the owning worker. */
  private final @Nullable DistributedNode _real;

  private DistributedNode(Configuration configuration, @Nullable DistributedNode real) {
    super(configuration);
    _real = real;
  }

  /** A node this worker owns and simulates. */
  public static DistributedNode real(Configuration configuration) {
    return new DistributedNode(configuration, null);
  }

  /** A node owned by another worker; delegates to {@code real}. */
  public static DistributedNode shadowOf(DistributedNode real) {
    return new DistributedNode(real.getConfiguration(), real);
  }

  public boolean isShadow() {
    return _real != null;
  }

  @Override
  Collection<VirtualRouter> getVirtualRouters() {
    return isShadow() ? _real.getVirtualRouters() : super.getVirtualRouters();
  }

  @Override
  Optional<VirtualRouter> getVirtualRouter(String vrfName) {
    return isShadow() ? _real.getVirtualRouter(vrfName) : super.getVirtualRouter(vrfName);
  }

  @Override
  VirtualRouter getVirtualRouterOrThrow(String vrfName) {
    return isShadow() ? _real.getVirtualRouterOrThrow(vrfName) : super.getVirtualRouterOrThrow(vrfName);
  }
}
