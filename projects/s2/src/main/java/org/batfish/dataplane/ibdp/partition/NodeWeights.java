// SPDX-License-Identifier: Apache-2.0
package org.batfish.dataplane.ibdp.partition;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.batfish.datamodel.BgpProcess;
import org.batfish.datamodel.Configuration;
import org.batfish.datamodel.Interface;
import org.batfish.datamodel.IpAccessList;
import org.batfish.datamodel.Vrf;
import org.batfish.datamodel.routing_policy.RoutingPolicy;

/**
 * Node-weight model for the S2 node &rarr; worker partitioner (O6 in the port plan).
 *
 * <p>The weight estimates a router's contribution to the control-plane load (retained RIB/FIB and
 * the transient BDD/queue cost). It is an additive feature sum over the parsed configuration;
 * before simulation the real per-router route count is unknown, so this is a static proxy.
 *
 * <p>The coefficients are deliberately coarse (this is the plan's {@code v1} static estimate; a
 * proper calibration against single-worker phase peaks, O6, is future work):
 *
 * <ul>
 *   <li>{@link #INTERFACE} per interface holding a concrete address: connected/interface routes,
 *       ARP and FIB entries.
 *   <li>{@link #BGP_PEER} per BGP neighbor (active + passive + interface): per-session state, an
 *       Adj-RIB-In and propagation work.
 *   <li>{@link #ORIGINATION_PREFIX} per prefix in the BGP origination space or an unconditional
 *       network statement, plus per configured aggregate: locally sourced routes that enter the
 *       dispersed RIB.
 *   <li>{@link #ACL_LINE} per access-list line and per routing-policy statement: proxies the BDD
 *       transition and forwarding cost.
 *   <li>{@link #STATIC_ROUTE} per static route: a retained main-RIB entry that can be
 *       redistributed.
 *   <li>{@link #VRF} per VRF: per-VRF processes and tables.
 * </ul>
 *
 * <p>The weight is the sum of these counts, so it is also exactly the feature count: the existing
 * {@code scripts/partition-metrics.py} baseline weight ({@code interfaces + peers + origination
 * prefixes}) is the same quantity restricted to the first three terms. This keeps the metrics
 * script and the Java partitioner comparable while making the ACL/policy and redistribution axes
 * explicit.
 */
public final class NodeWeights {

  /** Weight per interface with a concrete address. */
  public static final int INTERFACE = 1;

  /** Weight per BGP neighbor. */
  public static final int BGP_PEER = 1;

  /** Weight per BGP-originated prefix (network statement, redistribution range, aggregate). */
  public static final int ORIGINATION_PREFIX = 1;

  /** Weight per access-list line or routing-policy statement. */
  public static final int ACL_LINE = 1;

  /** Weight per static route. */
  public static final int STATIC_ROUTE = 1;

  /** Weight per VRF. */
  public static final int VRF = 1;

  private NodeWeights() {}

  /** Compute the node weight of every configuration, keyed by hostname. */
  public static Map<String, Integer> compute(Map<String, Configuration> configs) {
    Map<String, Integer> weights = new HashMap<>();
    for (Configuration c : configs.values()) {
      weights.put(c.getHostname(), compute(c));
    }
    return weights;
  }

  /** Compute the node weight of a single configuration (see the class doc for the coefficients). */
  public static int compute(Configuration c) {
    int weight = 0;
    for (Interface i : c.getAllInterfaces().values()) {
      if (i.getConcreteAddress() != null) {
        weight += INTERFACE;
      }
    }
    for (IpAccessList acl : c.getIpAccessLists().values()) {
      weight += ACL_LINE * acl.getLines().size();
    }
    for (RoutingPolicy policy : c.getRoutingPolicies().values()) {
      weight += ACL_LINE * policy.getStatements().size();
    }
    for (Vrf vrf : c.getVrfs().values()) {
      weight += VRF;
      weight += STATIC_ROUTE * vrf.getStaticRoutes().size();
      BgpProcess proc = vrf.getBgpProcess();
      if (proc != null) {
        weight += BGP_PEER * proc.getActiveNeighbors().size();
        weight += BGP_PEER * proc.getPassiveNeighbors().size();
        weight += BGP_PEER * proc.getInterfaceNeighbors().size();
        weight += ORIGINATION_PREFIX * proc.getOriginationSpace().getPrefixRanges().size();
        weight += ORIGINATION_PREFIX * proc.getUnconditionalNetworkStatements().size();
        weight += ORIGINATION_PREFIX * proc.getAggregates().size();
      }
    }
    return weight;
  }

  /**
   * The per-node weight breakdown for logging/calibration, as an ordered list of {@code
   * "term=count"} strings.
   */
  public static List<String> describe(Configuration c) {
    int interfaces = 0;
    int aclLines = 0;
    int policyLines = 0;
    int staticRoutes = 0;
    int vrfs = 0;
    int peers = 0;
    int originationPrefixes = 0;
    for (Interface i : c.getAllInterfaces().values()) {
      if (i.getConcreteAddress() != null) {
        interfaces++;
      }
    }
    for (IpAccessList acl : c.getIpAccessLists().values()) {
      aclLines += acl.getLines().size();
    }
    for (RoutingPolicy policy : c.getRoutingPolicies().values()) {
      policyLines += policy.getStatements().size();
    }
    for (Vrf vrf : c.getVrfs().values()) {
      vrfs++;
      staticRoutes += vrf.getStaticRoutes().size();
      BgpProcess proc = vrf.getBgpProcess();
      if (proc != null) {
        peers +=
            proc.getActiveNeighbors().size()
                + proc.getPassiveNeighbors().size()
                + proc.getInterfaceNeighbors().size();
        originationPrefixes +=
            proc.getOriginationSpace().getPrefixRanges().size()
                + proc.getUnconditionalNetworkStatements().size()
                + proc.getAggregates().size();
      }
    }
    List<String> terms = new ArrayList<>();
    terms.add("interfaces=" + interfaces);
    terms.add("peers=" + peers);
    terms.add("originationPrefixes=" + originationPrefixes);
    terms.add("aclLines=" + aclLines);
    terms.add("policyStatements=" + policyLines);
    terms.add("staticRoutes=" + staticRoutes);
    terms.add("vrfs=" + vrfs);
    return terms;
  }
}
