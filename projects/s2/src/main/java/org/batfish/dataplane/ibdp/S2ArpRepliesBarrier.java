// SPDX-License-Identifier: Apache-2.0

package org.batfish.dataplane.ibdp;

import com.google.common.collect.ImmutableMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CyclicBarrier;
import org.batfish.datamodel.IpSpace;

/**
 * Barrier that merges every worker's per-node ARP replies for the round and returns the result.
 *
 * <p>The map-valued counterpart of {@link S2UnownedArpIpsBarrier}: the merge is computed in the
 * barrier action and cleared before any worker is released, so the next round starts empty.
 */
final class S2ArpRepliesBarrier {

  private final Map<String, Map<String, IpSpace>> _arpReplies = new ConcurrentHashMap<>();
  private final CyclicBarrier _barrier;
  private volatile Map<String, Map<String, IpSpace>> _result;

  S2ArpRepliesBarrier(int numWorkers) {
    _barrier =
        new CyclicBarrier(
            numWorkers,
            () -> {
              _result = ImmutableMap.copyOf(_arpReplies);
              _arpReplies.clear();
            });
  }

  Map<String, Map<String, IpSpace>> check(Map<String, Map<String, IpSpace>> local) {
    _arpReplies.putAll(local);
    try {
      _barrier.await();
    } catch (Exception e) {
      throw new RuntimeException("S2 ARP reply synchronization failed", e);
    }
    return _result;
  }
}
