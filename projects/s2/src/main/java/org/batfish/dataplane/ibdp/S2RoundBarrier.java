// SPDX-License-Identifier: Apache-2.0

package org.batfish.dataplane.ibdp;

import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Round barrier shared by all workers: every worker reports a boolean each round and the barrier
 * action computes the OR for that round before any worker proceeds to the next round. Using a
 * {@link CyclicBarrier} (not wait/notify) avoids the cross-round race where a fast worker starts
 * round N+1 before a slow worker has left round N.
 *
 * <p>Extracted from {@code S2ControllerServer} so the one-shot runner and the persistent controller
 * service share exactly the same synchronization semantics.
 */
final class S2RoundBarrier {

  private final AtomicBoolean _anyDirty = new AtomicBoolean();
  private final CyclicBarrier _barrier;
  private volatile boolean _result;

  S2RoundBarrier(int numWorkers) {
    _barrier =
        new CyclicBarrier(
            numWorkers,
            () -> {
              _result = _anyDirty.get();
              _anyDirty.set(false);
            });
  }

  boolean check(boolean dirty) {
    if (dirty) {
      _anyDirty.set(true);
    }
    try {
      _barrier.await();
    } catch (Exception e) {
      throw new RuntimeException("S2 round synchronization failed", e);
    }
    return _result;
  }
}
