// SPDX-License-Identifier: Apache-2.0

package org.batfish.dataplane.ibdp;

import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Barrier that sums every worker's contribution for the round and returns the total.
 *
 * <p>Extracted from {@code S2ControllerServer} so the one-shot runner and the persistent controller
 * service share exactly the same synchronization semantics.
 */
final class S2SumBarrier {

  private final AtomicInteger _sum = new AtomicInteger();
  private final CyclicBarrier _barrier;
  private volatile int _result;

  S2SumBarrier(int numWorkers) {
    _barrier = new CyclicBarrier(numWorkers, () -> _result = _sum.getAndSet(0));
  }

  int check(int value) {
    _sum.addAndGet(value);
    try {
      _barrier.await();
    } catch (Exception e) {
      throw new RuntimeException("S2 sum synchronization failed", e);
    }
    return _result;
  }
}
