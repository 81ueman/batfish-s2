// SPDX-License-Identifier: Apache-2.0

package org.batfish.dataplane.ibdp;

import java.util.Map;
import java.util.Set;
import org.batfish.datamodel.Ip;
import org.batfish.datamodel.IpSpace;

/**
 * Cluster-wide coordination for the distributed dataplane computation. Implemented in-JVM by {@link
 * S2Cluster} (milestone 1/2 tests) and remotely by the controller (milestone 3, separate Pods).
 */
public interface S2Coordinator {
  /**
   * Report this worker's local convergence state for the current round and return the global result
   * (true if any worker still has changes). Blocks until every worker has reported the same round.
   */
  boolean roundCheck(boolean localDirty);

  /**
   * Exchange a per-worker value for the current computation round and return the sum across all
   * workers. Used to make oscillation detection and schedule selection global: workers that only
   * see their own switches would otherwise pick different schedules and desynchronize.
   */
  int sumAll(int localValue);

  /**
   * Union this worker's set with every other worker's set and return the result. Called at most
   * once per snapshot, after convergence: a worker that only builds full FIBs for its owned nodes
   * combines the unowned ARP IPs it computed from those FIBs with the other workers' contributions
   * to recover the exact cluster-wide set. Blocks until every worker has contributed.
   */
  Set<Ip> unionUnownedArpIps(Set<Ip> local);

  /**
   * Merge this worker's per-node ARP replies with every other worker's and return the result.
   * Called at most once per snapshot, after convergence, right after {@link
   * #unionUnownedArpIps(Set)}: a worker that only builds full FIBs for its owned nodes contributes
   * its owned nodes' exact ARP replies so every worker ends up with the full-FIB replies for all
   * nodes (see {@code S2BdpEngine#computeFinalArpReplies}). Blocks until every worker has
   * contributed.
   */
  Map<String, Map<String, IpSpace>> unionArpReplies(Map<String, Map<String, IpSpace>> local);
}
