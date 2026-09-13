// SPDX-License-Identifier: Apache-2.0
package org.batfish.dataplane.ibdp.partition;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Deque;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
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
 * <p><b>Calibration (O6).</b> The coefficients are fitted against measured per-node main-RIB route
 * counts (the dominant retained {@code R_w} term) from 13 testbeds, using within-network centering
 * so the fit sees the node-to-node variation the partitioner actually balances. The fitted
 * within-network ratios are {@code interfaces : peers : static = 0.68 : 1.81 : 0.99}, integerized
 * to {@code 1 : 3 : 1}; {@code originationPrefixes} and the generated-policy count fit to 0 (see
 * their constants). The {@code ACL_LINE} term is retained but cannot be identified from the RIB
 * measurements. See {@code scripts/calibrate-weights.py} and {@code
 * docs/s2-port/PARTITIONING-PLAN.md} &sect;6.7 for the methodology and the measured before/after
 * correlation. The model remains a pure, deterministic function of the parsed configuration.
 *
 * <p>The features are:
 *
 * <ul>
 *   <li>{@link #INTERFACE} per interface holding a concrete address: connected/interface routes,
 *       ARP and FIB entries.
 *   <li>{@link #BGP_PEER} per BGP neighbor (active + passive + interface): per-session state, an
 *       Adj-RIB-In and propagation work.
 *   <li>{@link #ORIGINATION_PREFIX} per prefix in the BGP origination space or an unconditional
 *       network statement, plus per configured aggregate: locally sourced routes that enter the
 *       dispersed RIB.
 *   <li>{@link #ACL_LINE} per explicit access-list line: proxies the BDD transition and forwarding
 *       cost.
 *   <li>{@link #POLICY_STATEMENT} per generated routing-policy statement (see the constant).
 *   <li>{@link #STATIC_ROUTE} per static route: a retained main-RIB entry that can be
 *       redistributed.
 *   <li>{@link #VRF} per VRF: per-VRF processes and tables.
 * </ul>
 *
 * <p><b>Topology correction (v2, {@value #V2_PROPERTY}).</b> The additive feature model above is a
 * static proxy; it cannot express the absolute <em>magnitude</em> of a router's retained RIB,
 * because before simulation the route count is unknown. The dominant term in the measured main RIB
 * is the <em>full table</em>: on a connected BGP domain every router ends up holding (a best route
 * for) every prefix originated anywhere in its domain. The correction estimates that full table
 * directly from the BGP session graph. For a router {@code v}, {@code fullTableRoutes(v)} is the
 * number of prefixes originated by any router in {@code v}'s BGP connected component (its
 * propagation closure); {@code v} receives a best route for each of them. The corrected weight is
 *
 * <pre>{@code weight(v) = baseWeight(v) + V2_FULL_TABLE_WEIGHT * fullTableRoutes(v)}</pre>
 *
 * <p>Adding a router's full-table size in route units is the part the config features miss. On a
 * DCN the whole fabric is one BGP component, so every router receives the same full table: the term
 * is a large common load, and it compresses the feature-only core/edge weight ratio (peers give
 * core/agg a 1.5x feature ratio on FatTree) toward the measured cost ratio (~1.10, because the full
 * table dominates). On a WAN with several BGP components, or a router that sees only a partial
 * table, routers with a larger propagation closure are weighted proportionally more. The correction
 * is deterministic and a pure function of the configs plus the BGP session graph.
 *
 * <p>It is <em>off by default</em> (it changes partition assignments, so stock demos are unchanged)
 * and enabled with {@code -Ds2.nodeWeightsV2=true}. The coefficient is {@link
 * #V2_FULL_TABLE_WEIGHT}; the evaluation-only {@value #V2_SCALE_PROPERTY} override exists so
 * calibration can sweep it without a rebuild.
 *
 * <p><b>Role-level peer scaling (O6 residual, {@value #ROLE_SCALE_PROPERTY}).</b> The calibrated
 * {@link #BGP_PEER} coefficient is a single fixed multiplier fitted across all networks, but the
 * measured core:edge cost ratio it should reproduce differs by shape. It is gated off by default;
 * when enabled, {@link #adaptivePeerCoefficient} drops the peer term on networks whose busiest tier
 * already has at least as many interfaces as their least-connected tier, and keeps it only when the
 * peer term is needed to rank the roles (see the method and {@code
 * docs/s2-port/PARTITIONING-PLAN.md} &sect;6.10). The {@value #PEER_SCALE_PROPERTY} override, when
 * set, takes precedence and pins the coefficient for evaluation.
 *
 * <p>Calibration instrumentation: when the {@value #DUMP_PROPERTY} system property names a file,
 * the {@link #compute(Map, Map)} overload writes a TSV of the per-node feature counts, the
 * full-table route estimate ({@value #CLOSURE_COLUMN}) and the effective weight there. It is off by
 * default and has no effect on the weight.
 */
public final class NodeWeights {

  /** Weight per interface with a concrete address. */
  public static final int INTERFACE = 1;

  /** Weight per BGP neighbor. */
  public static final int BGP_PEER = 3;

  /**
   * Weight per BGP-originated prefix (network statement, redistribution range, aggregate).
   *
   * <p>Calibrated to 0: within a network the measured per-node main-RIB route count is essentially
   * independent of how many prefixes the node itself originates (every other node carries them
   * too), while the cross-network scale is a network-level effect the partitioner does not need.
   */
  public static final int ORIGINATION_PREFIX = 0;

  /**
   * Weight per explicit access-list line. Not identifiable from the per-node RIB measurements (the
   * one ACL-heavy testbed, {@code s2-acl}, has the same ACL size on every node), so it is retained
   * at 1: the {@code after building nodes} peak jumps 59&rarr;121 MiB for the same route counts
   * when 3000-line ACLs are present, so ACLs are real foreground work.
   */
  public static final int ACL_LINE = 1;

  /**
   * Weight per generated routing-policy statement. Calibrated to 0: Batfish generates one policy
   * per BGP peer/network, so the statement count is collinear with {@link #BGP_PEER} and {@link
   * #ORIGINATION_PREFIX} and adds no measured within-network RIB signal. (It was the dominant term
   * in v1 and is what made the FatTree core and edge weigh the same.)
   */
  public static final int POLICY_STATEMENT = 0;

  /** Weight per static route. */
  public static final int STATIC_ROUTE = 1;

  /** Weight per VRF. */
  public static final int VRF = 1;

  /** System property that enables the v2 topology (full-table) correction. Off by default. */
  public static final String V2_PROPERTY = "s2.nodeWeightsV2";

  /**
   * Evaluation-only override for the {@link #BGP_PEER} coefficient, used by the O6 cost-aware sweep
   * (and by {@code scripts/calibrate-weights.py} style experiments) without a rebuild. Unset in
   * normal use. A value of 0 drops the peer term, which the §6.9 residual analysis compares against
   * the calibrated model.
   */
  public static final String PEER_SCALE_PROPERTY = "s2.nodeWeightsPeerScale";

  /**
   * System property that enables the adaptive role-level peer scaling (O6 residual). Off by
   * default: it changes partition assignments in a way validated only on the O6 testbeds, so it is
   * gated like the v2 topology correction. When enabled, the peer coefficient is chosen per network
   * from the degree/interface structure instead of the fixed {@link #BGP_PEER} (see {@link
   * #adaptivePeerCoefficient}).
   */
  public static final String ROLE_SCALE_PROPERTY = "s2.nodeWeightsRoleScale";

  /**
   * Weight per full-table route in the v2 topology correction: a router adds {@code
   * V2_FULL_TABLE_WEIGHT * fullTableRoutes(v)} to its base weight. The calibrated base coefficients
   * are on the order of the number of BGP peers (1&ndash;3), while a FatTree full table is tens of
   * routes; a small multiplier is enough to make the common full-table term dominant, which is what
   * flattens the feature-only core/edge ratio. Tuned on the O6 testbeds (see {@code
   * docs/s2-port/PARTITIONING-PLAN.md} &sect;6.9).
   */
  public static final int V2_FULL_TABLE_WEIGHT = 2;

  /**
   * Evaluation-only override for {@link #V2_FULL_TABLE_WEIGHT} ({@value}). Used by {@code
   * scripts/calibrate-weights.py} to sweep the coefficient without rebuilding; unset in normal use.
   */
  public static final String V2_SCALE_PROPERTY = "s2.nodeWeightsV2Scale";

  /** The dump column holding the estimated full-table route count (see the class doc). */
  public static final String CLOSURE_COLUMN = "bgpClosure";

  /** System property naming a file to dump the per-node feature counts to (calibration only). */
  public static final String DUMP_PROPERTY = "s2.nodeWeightsDump";

  private NodeWeights() {}

  /** The raw feature counts of one router, before the coefficients are applied. */
  public static final class Features {
    public final int interfaces;
    public final int peers;
    public final int originationPrefixes;
    public final int aclLines;
    public final int policyStatements;
    public final int staticRoutes;
    public final int vrfs;

    Features(
        int interfaces,
        int peers,
        int originationPrefixes,
        int aclLines,
        int policyStatements,
        int staticRoutes,
        int vrfs) {
      this.interfaces = interfaces;
      this.peers = peers;
      this.originationPrefixes = originationPrefixes;
      this.aclLines = aclLines;
      this.policyStatements = policyStatements;
      this.staticRoutes = staticRoutes;
      this.vrfs = vrfs;
    }

    /** The feature column names, in the same order as the TSV dumped by {@link #DUMP_PROPERTY}. */
    public static List<String> columnNames() {
      return List.of(
          "interfaces",
          "peers",
          "originationPrefixes",
          "aclLines",
          "policyStatements",
          "staticRoutes",
          "vrfs");
    }

    /** The feature values, in the same order as {@link #columnNames()}. */
    public List<Integer> values() {
      return List.of(
          interfaces, peers, originationPrefixes, aclLines, policyStatements, staticRoutes, vrfs);
    }
  }

  /** Extract the raw feature counts of a configuration. */
  public static Features features(Configuration c) {
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
    return new Features(
        interfaces, peers, originationPrefixes, aclLines, policyLines, staticRoutes, vrfs);
  }

  /** Apply the calibrated coefficients to a feature vector. */
  public static int weightOf(Features f) {
    return weightOf(f, peerCoefficient());
  }

  /** Apply the calibrated coefficients to a feature vector with an explicit peer coefficient. */
  public static int weightOf(Features f, int peerCoefficient) {
    return INTERFACE * f.interfaces
        + peerCoefficient * f.peers
        + ORIGINATION_PREFIX * f.originationPrefixes
        + ACL_LINE * f.aclLines
        + POLICY_STATEMENT * f.policyStatements
        + STATIC_ROUTE * f.staticRoutes
        + VRF * f.vrfs;
  }

  /**
   * The BGP peer coefficient, honoring the evaluation-only {@link #PEER_SCALE_PROPERTY} override.
   * This single-node form cannot apply the adaptive {@link #ROLE_SCALE_PROPERTY} rule (which needs
   * the whole population); see {@link #effectivePeerCoefficient}.
   */
  public static int peerCoefficient() {
    String override = System.getProperty(PEER_SCALE_PROPERTY);
    if (override == null || override.isEmpty()) {
      return BGP_PEER;
    }
    return Integer.parseInt(override.trim());
  }

  /** Whether the adaptive role-level peer scaling is enabled ({@value #ROLE_SCALE_PROPERTY}). */
  public static boolean roleScaleEnabled() {
    return Boolean.parseBoolean(System.getProperty(ROLE_SCALE_PROPERTY, "false"));
  }

  /**
   * The peer coefficient the calibrated model uses for a population of routers: the {@link
   * #PEER_SCALE_PROPERTY} override if set, else the adaptive {@link #ROLE_SCALE_PROPERTY} rule if
   * enabled, else the fixed {@link #BGP_PEER}.
   */
  static int effectivePeerCoefficient(Collection<Features> features) {
    String override = System.getProperty(PEER_SCALE_PROPERTY);
    if (override != null && !override.isEmpty()) {
      return Integer.parseInt(override.trim());
    }
    if (!roleScaleEnabled()) {
      return BGP_PEER;
    }
    return adaptivePeerCoefficient(features);
  }

  /**
   * The O6-residual role-level rule. On a network whose busiest BGP tier (most peers) also has at
   * least as many interfaces as its least-connected tier, the interface term already ranks the
   * roles and adding the peer term only over-spreads the weights toward the paper's 2:1 FatTree
   * ratio instead of the measured ~1.1:1, so the peer term is dropped (coefficient 0). When the
   * busiest tier has <em>fewer</em> interfaces (e.g. the {@code s2-fat2} edge tier), the peer term
   * is needed to keep the core above the edge and the calibrated {@link #BGP_PEER} is used. If
   * every router has the same peer count there is no role signal, and the peer term is dropped as
   * well. Deterministic; see {@code PARTITIONING-PLAN.md} &sect;6.10 for the measured evaluation.
   */
  static int adaptivePeerCoefficient(Collection<Features> features) {
    if (features.isEmpty()) {
      return 0;
    }
    int maxPeers = Integer.MIN_VALUE;
    int minPeers = Integer.MAX_VALUE;
    for (Features f : features) {
      maxPeers = Math.max(maxPeers, f.peers);
      minPeers = Math.min(minPeers, f.peers);
    }
    if (maxPeers == minPeers) {
      return 0;
    }
    double highInterfaces = meanInterfacesAtPeers(features, maxPeers);
    double lowInterfaces = meanInterfacesAtPeers(features, minPeers);
    return highInterfaces < lowInterfaces ? BGP_PEER : 0;
  }

  /** Mean interface count of the routers with exactly {@code peers} BGP neighbors. */
  private static double meanInterfacesAtPeers(Collection<Features> features, int peers) {
    int count = 0;
    int total = 0;
    for (Features f : features) {
      if (f.peers == peers) {
        count++;
        total += f.interfaces;
      }
    }
    return count == 0 ? 0.0 : total / (double) count;
  }

  /** Compute the base (feature-only) node weight of every configuration, keyed by hostname. */
  public static Map<String, Integer> compute(Map<String, Configuration> configs) {
    Map<String, Integer> weights = baseWeights(configs);
    dumpIfRequested(configs, weights, Map.of());
    return weights;
  }

  /**
   * The base (feature-only) weight of every configuration, using {@link #effectivePeerCoefficient}
   * for the population (so the adaptive role rule sees the whole network).
   */
  private static Map<String, Integer> baseWeights(Map<String, Configuration> configs) {
    Map<String, Features> byHost = new TreeMap<>();
    for (Configuration c : configs.values()) {
      byHost.put(c.getHostname(), features(c));
    }
    int peerCoefficient = effectivePeerCoefficient(byHost.values());
    Map<String, Integer> weights = new HashMap<>();
    for (Map.Entry<String, Features> e : byHost.entrySet()) {
      weights.put(e.getKey(), weightOf(e.getValue(), peerCoefficient));
    }
    return weights;
  }

  /**
   * Compute the effective node weight of every configuration, applying the v2 topology correction
   * (see the class doc) against the BGP session graph when {@link #V2_PROPERTY} is enabled.
   *
   * @param bgpAdjacency the undirected BGP session graph (hostname to neighbor hostnames)
   */
  public static Map<String, Integer> compute(
      Map<String, Configuration> configs, Map<String, ? extends Collection<String>> bgpAdjacency) {
    Map<String, Integer> base = baseWeights(configs);
    Map<String, Integer> fullTableRoutes = fullTableRoutes(configs, bgpAdjacency);
    int scale = v2Enabled() ? v2FullTableWeight() : 0;
    Map<String, Integer> weights = new HashMap<>();
    for (String host : base.keySet()) {
      weights.put(host, base.get(host) + scale * fullTableRoutes.getOrDefault(host, 0));
    }
    dumpIfRequested(configs, weights, fullTableRoutes);
    return weights;
  }

  /** Compute the node weight of a single configuration (see the class doc for the coefficients). */
  public static int compute(Configuration c) {
    return weightOf(features(c));
  }

  /** Whether the v2 topology (full-table) correction is enabled ({@value #V2_PROPERTY}). */
  public static boolean v2Enabled() {
    return Boolean.parseBoolean(System.getProperty(V2_PROPERTY, "false"));
  }

  /** The v2 full-table route coefficient, honoring the evaluation-only override. */
  public static int v2FullTableWeight() {
    String override = System.getProperty(V2_SCALE_PROPERTY);
    if (override == null || override.isEmpty()) {
      return V2_FULL_TABLE_WEIGHT;
    }
    return Integer.parseInt(override.trim());
  }

  /**
   * The estimated size of the full table each router receives: for every router, the number of
   * prefixes originated by routers in its BGP connected component. {@code bgpAdjacency} is the
   * (undirected) BGP session graph; neighbors outside {@code configs} are ignored, and a router
   * with no session forms a singleton component. Deterministic: components are explored in hostname
   * order.
   */
  public static Map<String, Integer> fullTableRoutes(
      Map<String, Configuration> configs, Map<String, ? extends Collection<String>> bgpAdjacency) {
    Map<String, Integer> own = new HashMap<>();
    for (Configuration c : configs.values()) {
      own.put(c.getHostname(), features(c).originationPrefixes);
    }
    Map<String, Integer> result = new HashMap<>();
    Set<String> visited = new HashSet<>();
    for (String start : new TreeSet<>(configs.keySet())) {
      if (!visited.add(start)) {
        continue;
      }
      Deque<String> queue = new ArrayDeque<>();
      queue.add(start);
      Set<String> component = new TreeSet<>();
      while (!queue.isEmpty()) {
        String node = queue.remove();
        component.add(node);
        Collection<String> neighbors = bgpAdjacency.get(node);
        if (neighbors == null) {
          continue;
        }
        for (String neighbor : neighbors) {
          if (configs.containsKey(neighbor) && visited.add(neighbor)) {
            queue.add(neighbor);
          }
        }
      }
      int total = 0;
      for (String node : component) {
        total += own.getOrDefault(node, 0);
      }
      for (String node : component) {
        result.put(node, total);
      }
    }
    return result;
  }

  /**
   * Write the per-node feature counts to the file named by {@link #DUMP_PROPERTY}, if set. Used by
   * {@code scripts/calibrate-weights.py} through the offline {@code S2Main partition} role. The
   * dump is sorted by hostname so it is deterministic; it is never written unless the property is
   * set.
   */
  private static void dumpIfRequested(
      Map<String, Configuration> configs,
      Map<String, Integer> weights,
      Map<String, Integer> fullTableRoutes) {
    String path = System.getProperty(DUMP_PROPERTY);
    if (path == null || path.isEmpty()) {
      return;
    }
    List<String> hosts = new ArrayList<>(configs.keySet());
    hosts.sort(null);
    StringBuilder sb = new StringBuilder();
    sb.append("hostname\t")
        .append(String.join("\t", Features.columnNames()))
        .append('\t')
        .append(CLOSURE_COLUMN)
        .append("\tweight\n");
    for (String host : hosts) {
      Features f = features(configs.get(host));
      sb.append(host);
      for (int v : f.values()) {
        sb.append('\t').append(v);
      }
      sb.append('\t').append(fullTableRoutes.getOrDefault(host, 0));
      sb.append('\t').append(weights.getOrDefault(host, 0)).append('\n');
    }
    try {
      Files.writeString(Paths.get(path), sb.toString());
    } catch (IOException e) {
      throw new UncheckedIOException("could not write node-weight dump " + path, e);
    }
  }

  /**
   * The per-node weight breakdown for logging/calibration, as an ordered list of {@code
   * "term=count"} strings.
   */
  public static List<String> describe(Configuration c) {
    Features f = features(c);
    List<String> terms = new ArrayList<>();
    terms.add("interfaces=" + f.interfaces);
    terms.add("peers=" + f.peers);
    terms.add("originationPrefixes=" + f.originationPrefixes);
    terms.add("aclLines=" + f.aclLines);
    terms.add("policyStatements=" + f.policyStatements);
    terms.add("staticRoutes=" + f.staticRoutes);
    terms.add("vrfs=" + f.vrfs);
    return terms;
  }
}
