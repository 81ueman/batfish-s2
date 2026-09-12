package org.batfish.dataplane.ibdp;

import java.util.List;
import java.util.concurrent.CyclicBarrier;

/**
 * Shared state for a set of S2 worker engines running in the same JVM.
 *
 * <p>In S2 the controller decides convergence for the whole network, not per worker. Engines run
 * their route-computation rounds in lockstep and only stop when no real router anywhere is dirty.
 */
public final class S2Cluster implements S2Coordinator {

  private final List<VirtualRouter> _realVirtualRouters;
  private final CyclicBarrier _iterationBarrier;

  public S2Cluster(List<VirtualRouter> realVirtualRouters, int workers) {
    _realVirtualRouters = realVirtualRouters;
    _iterationBarrier = new CyclicBarrier(workers);
  }

  @Override
  public boolean roundCheck(boolean localDirty) {
    try {
      _iterationBarrier.await();
    } catch (Exception e) {
      throw new RuntimeException("S2 worker synchronization failed", e);
    }
    return anyDirty();
  }

  /** Global fixed-point check: is any real router still dirty? */
  private boolean anyDirty() {
    return _realVirtualRouters.parallelStream().anyMatch(VirtualRouter::isDirty);
  }
}
