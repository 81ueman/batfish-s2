// SPDX-License-Identifier: Apache-2.0

package org.batfish.dataplane.ibdp;

import com.google.common.collect.ImmutableSet;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CyclicBarrier;
import org.batfish.datamodel.Ip;

/**
 * Barrier that unions every worker's set contribution for the round and returns the total.
 *
 * <p>Extracted from {@code S2ControllerServer} so the one-shot runner and the persistent controller
 * service share exactly the same synchronization semantics. Unlike {@link S2SumBarrier} (which sums
 * integers) this accumulates a set; the union is computed in the barrier action and cleared before
 * any worker is released, so the next round starts empty.
 */
final class S2UnownedArpIpsBarrier {

  private final Set<Ip> _union = ConcurrentHashMap.newKeySet();
  private final CyclicBarrier _barrier;
  private volatile Set<Ip> _result;

  S2UnownedArpIpsBarrier(int numWorkers) {
    _barrier =
        new CyclicBarrier(
            numWorkers,
            () -> {
              _result = ImmutableSet.copyOf(_union);
              _union.clear();
            });
  }

  Set<Ip> check(Set<Ip> local) {
    _union.addAll(local);
    try {
      _barrier.await();
    } catch (Exception e) {
      throw new RuntimeException("S2 unowned ARP IP synchronization failed", e);
    }
    return _result;
  }
}
