// SPDX-License-Identifier: Apache-2.0
package org.batfish.dataplane.ibdp;

import com.google.common.collect.ImmutableMap;
import java.util.Map;
import java.util.Set;
import org.batfish.datamodel.ForwardingAnalysis;
import org.batfish.datamodel.IpSpace;
import org.batfish.datamodel.VrfForwardingBehavior;

/**
 * A {@link ForwardingAnalysis} view restricted to a worker's owned hostnames.
 *
 * <p>Passing this to {@code BDDReachabilityAnalysisFactory} makes each worker generate reachability
 * transitions only for the switches it owns (S2's scale-out design). Cross-worker edges still appear
 * (their source is local); their targets are owned by other workers and are reached by shipping the
 * BDD over the sidecar.
 */
public final class OwnedForwardingAnalysis implements ForwardingAnalysis {

  private static final long serialVersionUID = 1L;

  private final ForwardingAnalysis _delegate;
  private final Set<String> _ownedHosts;

  public OwnedForwardingAnalysis(ForwardingAnalysis delegate, Set<String> ownedHosts) {
    _delegate = delegate;
    _ownedHosts = ownedHosts;
  }

  @Override
  public Map<String, Map<String, IpSpace>> getArpReplies() {
    ImmutableMap.Builder<String, Map<String, IpSpace>> builder = ImmutableMap.builder();
    _delegate
        .getArpReplies()
        .forEach(
            (hostname, replies) -> {
              if (_ownedHosts.contains(hostname)) {
                builder.put(hostname, replies);
              }
            });
    return builder.build();
  }

  @Override
  public Map<String, Map<String, VrfForwardingBehavior>> getVrfForwardingBehavior() {
    ImmutableMap.Builder<String, Map<String, VrfForwardingBehavior>> builder =
        ImmutableMap.builder();
    _delegate
        .getVrfForwardingBehavior()
        .forEach(
            (hostname, behavior) -> {
              if (_ownedHosts.contains(hostname)) {
                builder.put(hostname, behavior);
              }
            });
    return builder.build();
  }
}
