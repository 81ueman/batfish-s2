package org.batfish.dataplane.ibdp;

import java.util.Collection;
import java.util.Optional;
import javax.annotation.Nullable;
import org.batfish.datamodel.Configuration;
import org.batfish.datamodel.ospf.OspfTopology;

/**
 * A {@link Node} that participates in S2's distributed computation.
 *
 * <p>Exactly one worker "owns" a switch and simulates it (a <b>real</b> node). Other workers hold a
 * <b>shadow</b> node representing it. Shadow nodes are never iterated ({@link
 * S2BdpEngine#iterationVirtualRouters}), but must still answer neighbor lookups and appear in the
 * dataplane.
 *
 * <p>Two shadow flavors exist:
 *
 * <ul>
 *   <li>{@link #shadowOf} (milestone 1, single JVM): delegate directly to the owning worker's real
 *       node object.
 *   <li>{@link #shadow} (milestone 2, separate processes): keep local (empty) routers and install a
 *       {@link RemoteOutgoingRoutesProvider} that fetches advertisements over the sidecar.
 * </ul>
 */
public class DistributedNode extends Node {

  private final boolean _owns;

  /** Non-null only for the in-JVM (milestone 1) shadow that delegates to a real node object. */
  private final @Nullable DistributedNode _delegate;

  private DistributedNode(
      Configuration configuration, boolean owns, @Nullable DistributedNode delegate) {
    super(configuration);
    _owns = owns;
    _delegate = delegate;
  }

  /** A node this worker owns and simulates. */
  public static DistributedNode real(Configuration configuration) {
    return new DistributedNode(configuration, true, null);
  }

  /** Milestone-1 shadow: delegate to the real node in the same JVM. */
  public static DistributedNode shadowOf(DistributedNode real) {
    return new DistributedNode(real.getConfiguration(), false, real);
  }

  /** Milestone-2 shadow: local empty routers, advertisements fetched over the sidecar. */
  public static DistributedNode shadow(Configuration configuration) {
    return new DistributedNode(configuration, false, null);
  }

  public boolean isShadow() {
    return !_owns;
  }

  /** Install sidecar-backed providers on this shadow's BGP processes. */
  public void installRemoteBgpProviders(S2SidecarClient client, S2WorkerEndpoint owner) {
    if (!isShadow() || _delegate != null) {
      throw new IllegalStateException("installRemoteBgpProviders requires a remote shadow node");
    }
    String hostname = getConfiguration().getHostname();
    for (String vrf : getConfiguration().getVrfs().keySet()) {
      BgpRoutingProcess process = getVirtualRouterOrThrow(vrf).getBgpRoutingProcess();
      if (process != null) {
        process.setOutgoingRoutesProvider(
            new RemoteOutgoingRoutesProvider(client, owner, hostname, vrf));
      }
    }
  }

  /** Install sidecar-backed sinks on this shadow's OSPF processes. */
  public void installRemoteOspfProviders(
      S2SidecarClient client, S2WorkerEndpoint owner, OspfTopology topology) {
    if (!isShadow() || _delegate != null) {
      throw new IllegalStateException("installRemoteOspfProviders requires a remote shadow node");
    }
    String hostname = getConfiguration().getHostname();
    for (String vrf : getConfiguration().getVrfs().keySet()) {
      VirtualRouter vr = getVirtualRouterOrThrow(vrf);
      vr.initShadowOspfProcesses(topology);
      vr.getOspfProcesses()
          .forEach(
              (processName, process) ->
                  process.setEnqueueProvider(
                      new RemoteOspfEnqueueProvider(client, owner, hostname, vrf, processName)));
    }
  }

  @Override
  Collection<VirtualRouter> getVirtualRouters() {
    return _delegate != null ? _delegate.getVirtualRouters() : super.getVirtualRouters();
  }

  @Override
  Optional<VirtualRouter> getVirtualRouter(String vrfName) {
    return _delegate != null
        ? _delegate.getVirtualRouter(vrfName)
        : super.getVirtualRouter(vrfName);
  }

  @Override
  VirtualRouter getVirtualRouterOrThrow(String vrfName) {
    return _delegate != null
        ? _delegate.getVirtualRouterOrThrow(vrfName)
        : super.getVirtualRouterOrThrow(vrfName);
  }
}
