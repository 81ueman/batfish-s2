// SPDX-License-Identifier: Apache-2.0
package org.batfish.dataplane.ibdp;

import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Shared state for a set of S2 worker engines running in the same JVM.
 *
 * <p>In S2 the controller decides convergence for the whole network, not per worker. Engines run
 * their route-computation rounds in lockstep and only stop when no real router anywhere is dirty.
 * Like the remote controller, this coordinator takes each worker's local dirty flag and returns the
 * global OR for the round; it never inspects the virtual routers itself (doing so is only valid at
 * the end of a round, while this is also used as a mid-round phase barrier).
 */
public final class S2Cluster implements S2Coordinator {

  private final CyclicBarrier _barrier;
  private final AtomicBoolean _anyDirty = new AtomicBoolean();
  private volatile boolean _result;

  private final CyclicBarrier _sumBarrier;
  private final AtomicInteger _sum = new AtomicInteger();
  private volatile int _sumResult;

  public S2Cluster(int workers) {
    _barrier =
        new CyclicBarrier(
            workers,
            () -> {
              _result = _anyDirty.get();
              _anyDirty.set(false);
            });
    _sumBarrier = new CyclicBarrier(workers, () -> _sumResult = _sum.getAndSet(0));
  }

  @Override
  public boolean roundCheck(boolean localDirty) {
    if (localDirty) {
      _anyDirty.set(true);
    }
    try {
      _barrier.await();
    } catch (Exception e) {
      throw new RuntimeException("S2 worker synchronization failed", e);
    }
    return _result;
  }

  @Override
  public int sumAll(int localValue) {
    _sum.addAndGet(localValue);
    try {
      _sumBarrier.await();
    } catch (Exception e) {
      throw new RuntimeException("S2 worker synchronization failed", e);
    }
    return _sumResult;
  }
}
