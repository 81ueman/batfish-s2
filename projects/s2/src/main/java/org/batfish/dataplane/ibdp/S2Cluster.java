// SPDX-License-Identifier: Apache-2.0

package org.batfish.dataplane.ibdp;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import org.batfish.datamodel.Ip;
import org.batfish.datamodel.IpSpace;

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

  private final CyclicBarrier _unionBarrier;
  private final Set<Ip> _union = ConcurrentHashMap.newKeySet();
  private volatile Set<Ip> _unionResult;

  private final CyclicBarrier _arpRepliesBarrier;
  private final Map<String, Map<String, IpSpace>> _arpReplies = new ConcurrentHashMap<>();
  private volatile Map<String, Map<String, IpSpace>> _arpRepliesResult;

  public S2Cluster(int workers) {
    _barrier =
        new CyclicBarrier(
            workers,
            () -> {
              _result = _anyDirty.get();
              _anyDirty.set(false);
            });
    _sumBarrier = new CyclicBarrier(workers, () -> _sumResult = _sum.getAndSet(0));
    _unionBarrier =
        new CyclicBarrier(
            workers,
            () -> {
              _unionResult = ImmutableSet.copyOf(_union);
              _union.clear();
            });
    _arpRepliesBarrier =
        new CyclicBarrier(
            workers,
            () -> {
              _arpRepliesResult = ImmutableMap.copyOf(_arpReplies);
              _arpReplies.clear();
            });
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

  @Override
  public Set<Ip> unionUnownedArpIps(Set<Ip> local) {
    _union.addAll(local);
    try {
      _unionBarrier.await();
    } catch (Exception e) {
      throw new RuntimeException("S2 worker synchronization failed", e);
    }
    return _unionResult;
  }

  @Override
  public Map<String, Map<String, IpSpace>> unionArpReplies(
      Map<String, Map<String, IpSpace>> local) {
    _arpReplies.putAll(local);
    try {
      _arpRepliesBarrier.await();
    } catch (Exception e) {
      throw new RuntimeException("S2 worker synchronization failed", e);
    }
    return _arpRepliesResult;
  }
}
