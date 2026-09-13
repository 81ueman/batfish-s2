// SPDX-License-Identifier: Apache-2.0

package org.batfish.dataplane.ibdp.partition;

import java.util.Locale;
import java.util.Set;
import java.util.TreeSet;
import java.util.regex.Pattern;

/**
 * The heuristic behind {@link PartitionScheme#AUTO} (port plan &sect;3.4): pick a partition scheme
 * from the shape of the union communication graph, without a human choosing per network.
 *
 * <p>The plan's guidance is that a DCN (FatTree/Clos/spine-leaf) and a WAN/ISP are best served by
 * different schemes. The classification here is deliberately cheap, deterministic, and based only
 * on the already-built {@link CommunicationGraph}:
 *
 * <ol>
 *   <li><b>Hierarchical names.</b> If at least half the hostnames carry a DCN tier token ({@code
 *       core}, {@code agg}/{@code aggr}, {@code spine}, {@code leaf}, {@code tor}, {@code pod},
 *       {@code fabric}, {@code tier}, {@code edge}) the network is a DCN. Generated testbeds name
 *       every switch {@code swN}, so this rarely fires alone.
 *   <li><b>BGP overlay.</b> If at least a quarter of the union edges are BGP <em>only</em> (a BGP
 *       session with no L3/OSPF adjacency under it) the network is a WAN/ISP: multi-hop iBGP and
 *       route reflectors make the session graph denser than the IGP, which is exactly where the
 *       union graph's BGP edges, not the L3 links, carry the exchange ({@code PARTITIONING-PLAN}
 *       &sect;3.1). A BGP-only edge is a BGP session whose union edge has weight 1 (no L3/OSPF
 *       adjacency under it).
 *   <li><b>Regular-degree fabric.</b> Otherwise a fabric with a regular degree distribution &mdash;
 *       at most three distinct degrees, an average degree of at least 2, at least two distinct
 *       degrees, and a quarter of the nodes (at least three) of degree &ge; 3 &mdash; is a DCN. A
 *       FatTree k=4 has 60% degree-4 nodes; lines, rings, and small trees are too sparse and land
 *       in the WAN branch.
 *   <li>Everything else is a WAN.
 * </ol>
 *
 * <p><b>Scheme choice.</b> METIS is the plan's quality reference and the best cut-to-balance
 * trade-off measured on the testbeds ({@code PARTITIONING-PLAN} &sect;6.7), so when {@code gpmetis}
 * is installed the selection is always {@link PartitionScheme#METIS} (which itself falls back to
 * {@link PartitionScheme#WEIGHTED_LPT_FM} if the binary later disappears). Without {@code gpmetis}
 * the classification decides the pure-Java fallback: {@link PartitionScheme#NAME_ORDERED} for a DCN
 * (tier names keep tiers spread evenly) and {@link PartitionScheme#WEIGHTED_LPT_FM} for a WAN
 * (balance-first with a cut-aware refinement, and no dependence on name conventions).
 *
 * <p><b>Limits.</b> The heuristic is intentionally coarse and is not a correctness mechanism: every
 * scheme yields a valid assignment, so a misclassification only changes partition quality. It can
 * be fooled by (a) DCNs with no tier naming and an irregular degree distribution, (b) dense regular
 * WANs, and (c) hostnames containing a tier token by coincidence (matched only as a whole lowercase
 * word, so {@code monitor} does not match {@code tor}). Extending it (e.g. bisection estimates,
 * clustering) is future work; the selected shape and reason are logged so a run is self-describing.
 */
public final class AutoSchemeSelector {

  /** The two coarse shapes the heuristic distinguishes. */
  public enum Shape {
    /** A regular, hierarchical fabric (FatTree/Clos/spine-leaf) whose BGP tracks the L3 links. */
    DCN,
    /** A WAN/ISP: a sparse or irregular L3 graph, or a BGP overlay that does not follow it. */
    WAN
  }

  /** DCN tier tokens matched as whole lowercase words inside a hostname. */
  private static final Pattern DCN_TIER_TOKEN =
      Pattern.compile(
          "(?<![a-z])(core|agg|aggr|aggregation|spine|leaf|tor|pod|fabric|tier|edge)(?![a-z])");

  /** A hostname fraction at or above this makes the network a DCN by naming alone. */
  static final double MIN_DCN_NAME_FRACTION = 0.5;

  /** Union-edge BGP-only fraction at or above this makes the network a WAN. */
  static final double MIN_BGP_OVERLAY_EDGE_FRACTION = 0.25;

  /** At most this many distinct union degrees for the regular-degree fabric test. */
  static final int MAX_DCN_DISTINCT_DEGREES = 3;

  /** Minimum average union degree for the regular-degree fabric test. */
  static final double MIN_DCN_AVERAGE_DEGREE = 2.0;

  /** A node is "high degree" in the fabric test at or above this degree. */
  static final int DCN_HIGH_DEGREE = 3;

  /** At least this fraction of nodes must be high-degree for the fabric test. */
  static final double MIN_DCN_HIGH_DEGREE_FRACTION = 0.25;

  /** At least this many nodes must be high-degree (so tiny graphs cannot trigger it). */
  static final int MIN_DCN_HIGH_DEGREE_COUNT = 3;

  private AutoSchemeSelector() {}

  /** A resolved selection: the concrete scheme plus why it was chosen. */
  public static final class Selection {
    private final PartitionScheme _scheme;
    private final Shape _shape;
    private final boolean _metisAvailable;
    private final String _reason;

    private Selection(PartitionScheme scheme, Shape shape, boolean metisAvailable, String reason) {
      _scheme = scheme;
      _shape = shape;
      _metisAvailable = metisAvailable;
      _reason = reason;
    }

    /** The concrete scheme to run (never {@link PartitionScheme#AUTO}). */
    public PartitionScheme scheme() {
      return _scheme;
    }

    /** The classified shape, or {@code null} when the requested scheme was explicit. */
    public Shape shape() {
      return _shape;
    }

    /** Whether {@code gpmetis} was found; meaningless for an explicit scheme. */
    public boolean metisAvailable() {
      return _metisAvailable;
    }

    /** A one-line, deterministic explanation for logs. */
    public String describe() {
      if (_shape == null) {
        return "explicit scheme=" + _scheme;
      }
      return String.format(
          "shape=%s (%s), gpmetis=%s", _shape, _reason, _metisAvailable ? "available" : "absent");
    }
  }

  /**
   * Resolve the scheme a run should use. An explicit (non-{@code AUTO}) request is returned as-is
   * without classifying or probing for METIS; {@link PartitionScheme#AUTO} is classified and
   * resolved.
   */
  public static Selection select(PartitionScheme requested, CommunicationGraph graph) {
    if (requested != PartitionScheme.AUTO) {
      return new Selection(requested, null, false, "explicit");
    }
    Shape shape = classify(graph);
    boolean metisAvailable = MetisPartitioner.isAvailable();
    PartitionScheme scheme;
    if (metisAvailable) {
      scheme = PartitionScheme.METIS;
    } else {
      scheme = shape == Shape.DCN ? PartitionScheme.NAME_ORDERED : PartitionScheme.WEIGHTED_LPT_FM;
    }
    String reason = classifyReason(graph);
    return new Selection(scheme, shape, metisAvailable, reason);
  }

  /** The concrete scheme {@code AUTO} resolves to for {@code graph} (see {@link #select}). */
  public static PartitionScheme select(CommunicationGraph graph) {
    return select(PartitionScheme.AUTO, graph).scheme();
  }

  /** Classify {@code graph} as {@link Shape#DCN} or {@link Shape#WAN} (see the class doc). */
  public static Shape classify(CommunicationGraph graph) {
    Set<String> nodes = graph.nodes();
    if (nodes.isEmpty()) {
      return Shape.WAN;
    }
    if (isDcnByNames(nodes)) {
      return Shape.DCN;
    }
    if (isBgpOverlay(graph)) {
      return Shape.WAN;
    }
    if (isRegularDegreeFabric(graph, nodes.size())) {
      return Shape.DCN;
    }
    return Shape.WAN;
  }

  /** Whether at least {@link #MIN_DCN_NAME_FRACTION} of hostnames carry a DCN tier token. */
  private static boolean isDcnByNames(Set<String> nodes) {
    int named = 0;
    for (String node : nodes) {
      if (DCN_TIER_TOKEN.matcher(node.toLowerCase(Locale.ROOT)).find()) {
        named++;
      }
    }
    return named >= Math.ceil(MIN_DCN_NAME_FRACTION * nodes.size());
  }

  /** Whether at least {@link #MIN_BGP_OVERLAY_EDGE_FRACTION} of union edges are BGP-only. */
  private static boolean isBgpOverlay(CommunicationGraph graph) {
    long unionEdges = 0;
    long bgpOnlyEdges = 0;
    for (String u : graph.nodes()) {
      for (String v : graph.neighbors(u).keySet()) {
        if (u.compareTo(v) >= 0) {
          continue;
        }
        unionEdges++;
        if (graph.hasBgpSession(u, v) && graph.edgeWeight(u, v) == 1) {
          bgpOnlyEdges++;
        }
      }
    }
    return unionEdges > 0 && bgpOnlyEdges >= Math.ceil(MIN_BGP_OVERLAY_EDGE_FRACTION * unionEdges);
  }

  /** The regular-degree fabric test (see the class doc). */
  private static boolean isRegularDegreeFabric(CommunicationGraph graph, int numNodes) {
    Set<Integer> distinctDegrees = new TreeSet<>();
    long totalDegree = 0;
    int highDegree = 0;
    for (String node : graph.nodes()) {
      int degree = graph.neighbors(node).size();
      distinctDegrees.add(degree);
      totalDegree += degree;
      if (degree >= DCN_HIGH_DEGREE) {
        highDegree++;
      }
    }
    int highDegreeFloor =
        Math.max(
            MIN_DCN_HIGH_DEGREE_COUNT, (int) Math.ceil(MIN_DCN_HIGH_DEGREE_FRACTION * numNodes));
    return distinctDegrees.size() >= 2
        && distinctDegrees.size() <= MAX_DCN_DISTINCT_DEGREES
        && totalDegree / (double) numNodes >= MIN_DCN_AVERAGE_DEGREE
        && highDegree >= highDegreeFloor;
  }

  /** The human-readable reason for the current classification (for logs). */
  private static String classifyReason(CommunicationGraph graph) {
    Set<String> nodes = graph.nodes();
    if (nodes.isEmpty()) {
      return "empty graph";
    }
    if (isDcnByNames(nodes)) {
      return "hierarchical names";
    }
    if (isBgpOverlay(graph)) {
      return "BGP overlay denser than L3";
    }
    if (isRegularDegreeFabric(graph, nodes.size())) {
      return "regular-degree fabric";
    }
    return "sparse/irregular L3";
  }
}
