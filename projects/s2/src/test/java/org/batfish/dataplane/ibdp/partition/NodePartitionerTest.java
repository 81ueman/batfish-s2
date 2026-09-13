// SPDX-License-Identifier: Apache-2.0

package org.batfish.dataplane.ibdp.partition;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.lessThanOrEqualTo;
import static org.junit.Assume.assumeTrue;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.ImmutableSortedSet;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import org.batfish.datamodel.AclLine;
import org.batfish.datamodel.BgpProcess;
import org.batfish.datamodel.ConcreteInterfaceAddress;
import org.batfish.datamodel.Configuration;
import org.batfish.datamodel.ConfigurationFormat;
import org.batfish.datamodel.Edge;
import org.batfish.datamodel.ExprAclLine;
import org.batfish.datamodel.Interface;
import org.batfish.datamodel.InterfaceType;
import org.batfish.datamodel.Ip;
import org.batfish.datamodel.IpAccessList;
import org.batfish.datamodel.LineAction;
import org.batfish.datamodel.Prefix;
import org.batfish.datamodel.PrefixRange;
import org.batfish.datamodel.PrefixSpace;
import org.batfish.datamodel.StaticRoute;
import org.batfish.datamodel.Topology;
import org.batfish.datamodel.Vrf;
import org.batfish.datamodel.acl.AclLineMatchExprs;
import org.batfish.datamodel.bgp.BgpTopology;
import org.batfish.datamodel.collections.NodeInterfacePair;
import org.batfish.dataplane.ibdp.NetworkPartitioner;
import org.batfish.dataplane.ibdp.TopologyContext;
import org.junit.Test;

/** Tests for the node &rarr; worker partition schemes and the union communication graph. */
public class NodePartitionerTest {

  /** A synthetic graph: {@code weights} sets the nodes and weights, {@code edges} are symmetric. */
  private static CommunicationGraph graph(Map<String, Integer> weights, Object[][] edges) {
    Map<String, Map<String, Integer>> adjacency = new HashMap<>();
    for (String node : weights.keySet()) {
      adjacency.put(node, new HashMap<>());
    }
    for (Object[] edge : edges) {
      String a = (String) edge[0];
      String b = (String) edge[1];
      int w = (Integer) edge[2];
      adjacency.get(a).put(b, w);
      adjacency.get(b).put(a, w);
    }
    return CommunicationGraph.forTesting(weights, adjacency);
  }

  /** The historical default must be byte-for-byte preserved. */
  @Test
  public void testRandomMatchesLegacyNetworkPartitioner() {
    Set<String> hosts = ImmutableSet.of("r1", "r2", "r3", "r4", "r5", "r6", "r7");
    Map<String, Integer> legacy = NetworkPartitioner.partition(hosts, 3, 0L);
    RandomPartitioner random = new RandomPartitioner();
    assertThat(random.partition(hosts, 3, 0L), equalTo(legacy));
    assertThat(
        random.partition(
            graph(
                ImmutableMap.of("r1", 1, "r2", 2, "r3", 3, "r4", 4, "r5", 5, "r6", 6, "r7", 7),
                new Object[0][]),
            3,
            0L),
        equalTo(legacy));
  }

  @Test
  public void testRandomIsDeterministicForSeed() {
    RandomPartitioner random = new RandomPartitioner();
    Set<String> hosts = ImmutableSet.of("a", "b", "c", "d", "e");
    assertThat(random.partition(hosts, 2, 7L), equalTo(random.partition(hosts, 2, 7L)));
  }

  @Test
  public void testNameOrderedDealsSortedRoundRobin() {
    CommunicationGraph g = graph(ImmutableMap.of("c", 1, "a", 1, "b", 1, "d", 1), new Object[0][]);
    assertThat(
        new NameOrderedPartitioner().partition(g, 2, 0L),
        equalTo(ImmutableMap.of("a", 0, "b", 1, "c", 0, "d", 1)));
  }

  /**
   * On a 4-node line with equal weights and 2 workers the balanced cut is 1. LPT alone would put
   * {a1,a3} | {a2,a4} (cut 3); the FM refinement must reach the optimum.
   */
  @Test
  public void testWeightedLptFmReducesCutOnLine() {
    Map<String, Integer> weights = ImmutableMap.of("a1", 1, "a2", 1, "a3", 1, "a4", 1);
    CommunicationGraph g =
        graph(
            weights,
            new Object[][] {
              {"a1", "a2", 1}, {"a2", "a3", 1}, {"a3", "a4", 1},
            });
    WeightedLptFmPartitioner partitioner = new WeightedLptFmPartitioner();
    Map<String, Integer> assignment = partitioner.partition(g, 2, 0L);
    assertThat(CommunicationGraph.cutWeight(g, assignment), equalTo(1L));
    long[] loads = CommunicationGraph.loads(g, assignment, 2);
    assertThat(Math.abs(loads[0] - loads[1]), lessThanOrEqualTo(1L));
    // Deterministic.
    assertThat(partitioner.partition(g, 2, 0L), equalTo(assignment));
  }

  /** A graph with disconnected components: every node is still assigned in range. */
  @Test
  public void testWeightedLptFmHandlesDisconnectedGraph() {
    Map<String, Integer> weights = ImmutableMap.of("a", 5, "b", 1, "c", 1, "d", 1);
    CommunicationGraph g = graph(weights, new Object[][] {{"a", "b", 1}});
    Map<String, Integer> assignment = new WeightedLptFmPartitioner().partition(g, 3, 0L);
    assertThat(assignment.keySet(), equalTo(weights.keySet()));
    for (int worker : assignment.values()) {
      assertThat(worker >= 0 && worker < 3, is(true));
    }
  }

  /**
   * Two dense communities joined by a single edge, 2 workers: GREEDY_REGION should keep each
   * community together (cut 1) by growing from one seed per community.
   */
  @Test
  public void testGreedyRegionKeepsCommunitiesTogether() {
    Map<String, Integer> weights =
        ImmutableMap.of("a1", 1, "a2", 1, "a3", 1, "b1", 1, "b2", 1, "b3", 1);
    CommunicationGraph g =
        graph(
            weights,
            new Object[][] {
              {"a1", "a2", 1},
              {"a2", "a3", 1},
              {"a1", "a3", 1},
              {"b1", "b2", 1},
              {"b2", "b3", 1},
              {"b1", "b3", 1},
              {"a1", "b1", 1},
            });
    Map<String, Integer> assignment = new GreedyRegionPartitioner().partition(g, 2, 0L);
    assertThat(CommunicationGraph.cutWeight(g, assignment), equalTo(1L));
    assertThat(assignment.get("a1"), equalTo(assignment.get("a2")));
    assertThat(assignment.get("a1"), equalTo(assignment.get("a3")));
    assertThat(assignment.get("b1"), equalTo(assignment.get("b2")));
    assertThat(assignment.get("b1"), equalTo(assignment.get("b3")));
  }

  /** METIS must degrade to WEIGHTED_LPT_FM when the binary is unavailable. */
  @Test
  public void testMetisFallsBackWhenBinaryMissing() {
    Map<String, Integer> weights = ImmutableMap.of("a", 3, "b", 2, "c", 2, "d", 1);
    CommunicationGraph g =
        graph(weights, new Object[][] {{"a", "b", 1}, {"b", "c", 1}, {"c", "d", 1}});
    String previous = System.getProperty(MetisPartitioner.METIS_PATH_PROPERTY);
    System.setProperty(MetisPartitioner.METIS_PATH_PROPERTY, "/nonexistent/gpmetis-for-s2-test");
    try {
      assertThat(
          new MetisPartitioner().partition(g, 2, 0L),
          equalTo(new WeightedLptFmPartitioner().partition(g, 2, 0L)));
    } finally {
      if (previous == null) {
        System.clearProperty(MetisPartitioner.METIS_PATH_PROPERTY);
      } else {
        System.setProperty(MetisPartitioner.METIS_PATH_PROPERTY, previous);
      }
    }
  }

  /**
   * The gpmetis graph file must use the METIS adjacency order {@code vwgt nbr ewgt} (a vertex
   * weight followed, per neighbor, by the 1-based neighbor then the edge weight). Writing {@code
   * vwgt ewgt nbr} instead makes METIS parse the unit edge weights as neighbor ids (self-loops), so
   * it drops the real edges and the weighted balance silently degrades. This locks the format down.
   */
  @Test
  public void testMetisGraphFormat() throws IOException {
    Map<String, Integer> weights = ImmutableMap.of("a", 20, "b", 20, "c", 20);
    CommunicationGraph g = graph(weights, new Object[][] {{"a", "b", 1}, {"b", "c", 2}});
    Path file = Files.createTempFile("s2-metis-format", ".graph");
    try {
      MetisPartitioner.writeGraph(g, new ArrayList<>(g.nodes()), file);
      assertThat(
          Files.readAllLines(file, StandardCharsets.UTF_8),
          equalTo(ImmutableList.of("3 2 11 1", "20 2 1", "20 1 1 3 2", "20 2 2")));
    } finally {
      Files.deleteIfExists(file);
    }
  }

  /**
   * End-to-end check that the real {@code gpmetis} balances weighted vertices: a 6-vertex line with
   * uniform weight 20 and 3 workers must split 2/2/2 (load 40 each). Skipped when {@code gpmetis}
   * is not installed, so the suite still passes in environments without METIS.
   */
  @Test
  public void testMetisBalancesUniformWeightLine() {
    assumeTrue("gpmetis not installed", gpmetisAvailable());
    Map<String, Integer> weights = new HashMap<>();
    Object[][] edges = new Object[5][];
    for (int i = 1; i <= 6; i++) {
      weights.put("v" + i, 20);
      if (i < 6) {
        edges[i - 1] = new Object[] {"v" + i, "v" + (i + 1), 1};
      }
    }
    CommunicationGraph g = graph(weights, edges);
    Map<String, Integer> assignment = new MetisPartitioner().partition(g, 3, 0L);
    assertThat(assignment.keySet(), equalTo(weights.keySet()));
    long[] loads = CommunicationGraph.loads(g, assignment, 3);
    for (long load : loads) {
      assertThat(load, equalTo(40L));
    }
    assertThat(CommunicationGraph.cutWeight(g, assignment), equalTo(2L));
  }

  /** Whether the default {@code gpmetis} executable can be started. */
  private static boolean gpmetisAvailable() {
    String metis = System.getProperty(MetisPartitioner.METIS_PATH_PROPERTY, "gpmetis");
    try {
      Process process =
          new ProcessBuilder(metis)
              .redirectOutput(ProcessBuilder.Redirect.DISCARD)
              .redirectError(ProcessBuilder.Redirect.DISCARD)
              .start();
      process.waitFor();
      return true;
    } catch (IOException e) {
      return false;
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      return false;
    }
  }

  @Test
  public void testPartitionSchemeParse() {
    assertThat(
        PartitionScheme.tryParse("weighted_lpt_fm"), equalTo(PartitionScheme.WEIGHTED_LPT_FM));
    assertThat(PartitionScheme.tryParse("MetIs"), equalTo(PartitionScheme.METIS));
    assertThat(PartitionScheme.tryParse("auto"), equalTo(PartitionScheme.AUTO));
    assertThat(PartitionScheme.tryParse("AUTO"), equalTo(PartitionScheme.AUTO));
    assertThat(PartitionScheme.tryParse("bogus"), equalTo(null));
    assertThat(PartitionScheme.tryParse(null), equalTo(null));

    String previous = System.getProperty(PartitionScheme.PROPERTY);
    try {
      System.clearProperty(PartitionScheme.PROPERTY);
      assertThat(PartitionScheme.fromSystemProperties(), equalTo(PartitionScheme.WEIGHTED_LPT_FM));
      System.setProperty(PartitionScheme.PROPERTY, "greedy_region");
      assertThat(PartitionScheme.fromSystemProperties(), equalTo(PartitionScheme.GREEDY_REGION));
      System.setProperty(PartitionScheme.PROPERTY, "auto");
      assertThat(PartitionScheme.fromSystemProperties(), equalTo(PartitionScheme.AUTO));
    } finally {
      if (previous == null) {
        System.clearProperty(PartitionScheme.PROPERTY);
      } else {
        System.setProperty(PartitionScheme.PROPERTY, previous);
      }
    }
  }

  /**
   * A deliberate, regular, branching fabric with no tier names classifies as a DCN: a FatTree has a
   * concentrated degree distribution and a substantial high-degree tier. A line/ring does not.
   */
  @Test
  public void testAutoClassifiesRegularDegreeFabricAsDcn() {
    // Two 4-node dense groups joined by two cross edges: degrees are 3 and 4, average 3.5, and
    // every node is high-degree (>= 3).
    Map<String, Integer> weights = new HashMap<>();
    Object[][] edges = {
      {"a1", "a2", 1}, {"a1", "a3", 1}, {"a1", "a4", 1},
      {"a2", "a3", 1}, {"a2", "a4", 1}, {"a3", "a4", 1},
      {"b1", "b2", 1}, {"b1", "b3", 1}, {"b1", "b4", 1},
      {"b2", "b3", 1}, {"b2", "b4", 1}, {"b3", "b4", 1},
      {"a1", "b1", 1}, {"a2", "b2", 1},
    };
    for (String node : ImmutableList.of("a1", "a2", "a3", "a4", "b1", "b2", "b3", "b4")) {
      weights.put(node, 1);
    }
    assertThat(
        AutoSchemeSelector.classify(graph(weights, edges)), equalTo(AutoSchemeSelector.Shape.DCN));
  }

  /** A line and a ring are too sparse (and have no high-degree tier): WAN. */
  @Test
  public void testAutoClassifiesSparseLineAsWan() {
    Map<String, Integer> weights = ImmutableMap.of("a", 1, "b", 1, "c", 1, "d", 1);
    CommunicationGraph line =
        graph(weights, new Object[][] {{"a", "b", 1}, {"b", "c", 1}, {"c", "d", 1}});
    assertThat(AutoSchemeSelector.classify(line), equalTo(AutoSchemeSelector.Shape.WAN));
  }

  /** Hierarchical hostnames are enough to call a DCN even when the graph itself is sparse. */
  @Test
  public void testAutoClassifiesDcnByTierNames() {
    Map<String, Integer> weights = ImmutableMap.of("core1", 1, "agg1", 1, "spine1", 1, "edge1", 1);
    CommunicationGraph g =
        graph(
            weights,
            new Object[][] {{"core1", "agg1", 1}, {"agg1", "spine1", 1}, {"spine1", "edge1", 1}});
    assertThat(AutoSchemeSelector.classify(g), equalTo(AutoSchemeSelector.Shape.DCN));
    // A coincidence inside a longer word must not match (monitor != tor).
    Map<String, Integer> monitor = ImmutableMap.of("monitor1", 1, "monitor2", 1);
    assertThat(
        AutoSchemeSelector.classify(graph(monitor, new Object[][] {{"monitor1", "monitor2", 1}})),
        equalTo(AutoSchemeSelector.Shape.WAN));
  }

  /**
   * A BGP overlay that does not follow the IGP (route reflectors / multi-hop iBGP) is a WAN even
   * when the L3 graph is a line: most union edges are then BGP-only.
   */
  @Test
  public void testAutoClassifiesBgpOverlayAsWan() {
    List<String> nodes = ImmutableList.of("a", "b", "c", "d", "e", "f");
    Map<String, Integer> weights = new HashMap<>();
    Map<String, Map<String, Integer>> union = new HashMap<>();
    Map<String, Map<String, Integer>> bgp = new HashMap<>();
    for (String n : nodes) {
      weights.put(n, 1);
      union.put(n, new HashMap<>());
      bgp.put(n, new HashMap<>());
    }
    // Every pair is a BGP session; only consecutive pairs have an L3 link under it (edge weight 2).
    for (int i = 0; i < nodes.size(); i++) {
      for (int j = i + 1; j < nodes.size(); j++) {
        String u = nodes.get(i);
        String v = nodes.get(j);
        int w = (j == i + 1) ? 2 : 1;
        union.get(u).put(v, w);
        union.get(v).put(u, w);
        bgp.get(u).put(v, 1);
        bgp.get(v).put(u, 1);
      }
    }
    CommunicationGraph g = CommunicationGraph.forTesting(weights, union, bgp);
    assertThat(g.hasBgpSession("a", "f"), is(true));
    assertThat(g.bgpNeighbors("a").size(), equalTo(5));
    assertThat(AutoSchemeSelector.classify(g), equalTo(AutoSchemeSelector.Shape.WAN));
  }

  /** AUTO resolves a DCN to WEIGHTED_LPT_FM (the default; the shape is only logged). */
  @Test
  public void testAutoResolvesDcnToWeightedLptFm() {
    Map<String, Integer> weights = ImmutableMap.of("core1", 1, "agg1", 1, "spine1", 1, "edge1", 1);
    CommunicationGraph g =
        graph(
            weights,
            new Object[][] {{"core1", "agg1", 1}, {"agg1", "spine1", 1}, {"spine1", "edge1", 1}});
    withMetisUnavailable(
        () ->
            assertThat(PartitionScheme.AUTO.resolve(g), equalTo(PartitionScheme.WEIGHTED_LPT_FM)));
  }

  /** Without {@code gpmetis} a WAN also resolves to WEIGHTED_LPT_FM. */
  @Test
  public void testAutoResolvesWanToWeightedLptFmWithoutMetis() {
    Map<String, Integer> weights = ImmutableMap.of("a", 3, "b", 2, "c", 2, "d", 1);
    CommunicationGraph g =
        graph(weights, new Object[][] {{"a", "b", 1}, {"b", "c", 1}, {"c", "d", 1}});
    withMetisUnavailable(
        () ->
            assertThat(PartitionScheme.AUTO.resolve(g), equalTo(PartitionScheme.WEIGHTED_LPT_FM)));
  }

  /** An explicit scheme never re-resolves (and never probes for METIS). */
  @Test
  public void testExplicitSchemeResolvesToItself() {
    Map<String, Integer> weights = ImmutableMap.of("a", 1, "b", 1);
    CommunicationGraph g = graph(weights, new Object[][] {{"a", "b", 1}});
    assertThat(PartitionScheme.RANDOM.resolve(g), equalTo(PartitionScheme.RANDOM));
    assertThat(PartitionScheme.GREEDY_REGION.resolve(g), equalTo(PartitionScheme.GREEDY_REGION));
    AutoSchemeSelector.Selection explicit =
        AutoSchemeSelector.select(PartitionScheme.NAME_ORDERED, g);
    assertThat(explicit.scheme(), equalTo(PartitionScheme.NAME_ORDERED));
    assertThat(explicit.shape(), equalTo(null));
  }

  /** The AUTO partitioner delegates to the concrete scheme it resolves to. */
  @Test
  public void testAutoPartitionerDelegatesToResolvedScheme() {
    Map<String, Integer> weights = ImmutableMap.of("a", 3, "b", 2, "c", 2, "d", 1);
    CommunicationGraph g =
        graph(weights, new Object[][] {{"a", "b", 1}, {"b", "c", 1}, {"c", "d", 1}});
    withMetisUnavailable(
        () -> {
          PartitionScheme resolved = PartitionScheme.AUTO.resolve(g);
          assertThat(
              new AutoPartitioner().partition(g, 2, 0L),
              equalTo(resolved.partitioner().partition(g, 2, 0L)));
        });
  }

  /**
   * Even when {@code gpmetis} is installed, AUTO resolves to WEIGHTED_LPT_FM (METIS is explicit).
   */
  @Test
  public void testAutoResolvesToWeightedLptFmWithMetis() {
    Map<String, Integer> weights = ImmutableMap.of("a", 1, "b", 1, "c", 1, "d", 1);
    CommunicationGraph g = graph(weights, new Object[][] {{"a", "b", 1}, {"b", "c", 1}});
    assertThat(PartitionScheme.AUTO.resolve(g), equalTo(PartitionScheme.WEIGHTED_LPT_FM));
  }

  /** Run {@code body} with the METIS path forced to a nonexistent binary. */
  private static void withMetisUnavailable(Runnable body) {
    String previous = System.getProperty(MetisPartitioner.METIS_PATH_PROPERTY);
    System.setProperty(
        MetisPartitioner.METIS_PATH_PROPERTY, "/nonexistent/gpmetis-for-s2-auto-test");
    try {
      body.run();
    } finally {
      restoreProperty(MetisPartitioner.METIS_PATH_PROPERTY, previous);
    }
  }

  /**
   * The union graph includes L3 adjacencies, dedupes the two directed orientations of a link, and
   * takes node weights from the config features.
   */
  @Test
  public void testCommunicationGraphBuildsUnionAndDedupes() {
    Configuration r1 = configWithInterface("r1", "i1", "10.0.0.1/30");
    Configuration r2 = configWithInterface("r2", "i2", "10.0.0.2/30");
    Edge forward = new Edge(NodeInterfacePair.of("r1", "i1"), NodeInterfacePair.of("r2", "i2"));
    Edge reverse = new Edge(NodeInterfacePair.of("r2", "i2"), NodeInterfacePair.of("r1", "i1"));
    Topology topology = new Topology(new TreeSet<>(ImmutableList.of(forward, reverse)));
    TopologyContext tc = TopologyContext.builder().setLayer3Topology(topology).build();
    CommunicationGraph g =
        CommunicationGraph.build(ImmutableMap.of("r1", r1, "r2", r2), tc, BgpTopology.EMPTY);
    assertThat(g.nodes(), equalTo(ImmutableSet.of("r1", "r2")));
    assertThat(g.edgeWeight("r1", "r2"), equalTo(1));
    // One interface with a concrete address + one VRF.
    assertThat(g.weight("r1"), equalTo(NodeWeights.INTERFACE + NodeWeights.VRF));
    assertThat(g.weight("r2"), equalTo(NodeWeights.INTERFACE + NodeWeights.VRF));
  }

  /** Node weights count interfaces, ACL lines, static routes and VRFs (O6 static model). */
  @Test
  public void testNodeWeightsCountsStaticFeatures() {
    Configuration c = configWithInterface("r1", "i1", "10.0.0.1/30");
    AclLine permit =
        ExprAclLine.builder()
            .setAction(LineAction.PERMIT)
            .setMatchCondition(AclLineMatchExprs.TRUE)
            .build();
    AclLine deny =
        ExprAclLine.builder()
            .setAction(LineAction.DENY)
            .setMatchCondition(AclLineMatchExprs.TRUE)
            .build();
    c.setIpAccessLists(
        ImmutableMap.of(
            "acl", IpAccessList.builder().setName("acl").setLines(permit, deny).build()));
    Vrf vrf = c.getVrfs().get(Configuration.DEFAULT_VRF_NAME);
    vrf.setStaticRoutes(
        ImmutableSortedSet.of(
            StaticRoute.testBuilder().setNetwork(Prefix.parse("192.0.2.0/24")).build(),
            StaticRoute.testBuilder().setNetwork(Prefix.parse("198.51.100.0/24")).build()));
    int expected =
        NodeWeights.INTERFACE
            + 2 * NodeWeights.ACL_LINE
            + 2 * NodeWeights.STATIC_ROUTE
            + NodeWeights.VRF;
    assertThat(NodeWeights.compute(c), equalTo(expected));
    assertThat(NodeWeights.compute(ImmutableMap.of("r1", c)).get("r1"), equalTo(expected));
  }

  /** {@code compute(c)} is exactly {@code weightOf(features(c))}. */
  @Test
  public void testComputeEqualsWeightOfFeatures() {
    Configuration c = configWithInterface("r1", "i1", "10.0.0.1/30");
    assertThat(NodeWeights.compute(c), equalTo(NodeWeights.weightOf(NodeWeights.features(c))));
  }

  /**
   * The calibrated model must rank the measured FatTree shapes correctly: the core/aggregation
   * layer (more BGP peers) carries more routes than the edge layer even though the edge layer
   * originates more prefixes. v1 (every coefficient = 1) weighed the k=4 layers the same and
   * inverted the k=2 layers.
   */
  @Test
  public void testCalibratedModelRanksFatTreeCoreAboveEdge() {
    // k=4 shape: same interfaces and generated policies; core 4 peers / 1 prefix, edge 2 peers /
    // 3 prefixes. Measured main-RIB routes: 44 vs 40.
    NodeWeights.Features core = new NodeWeights.Features(5, 4, 1, 0, 9, 0, 1);
    NodeWeights.Features edge = new NodeWeights.Features(5, 2, 3, 0, 9, 0, 1);
    assertThat(NodeWeights.weightOf(core), greaterThan(NodeWeights.weightOf(edge)));
  }

  /**
   * Generated routing-policy statements are collinear with the BGP peer/prefix structure and add no
   * measured within-network RIB signal, so the calibrated model gives them zero weight. They were
   * the dominant term in v1 and caused the FatTree inversion.
   */
  @Test
  public void testCalibratedModelIgnoresGeneratedPolicyStatements() {
    NodeWeights.Features withPolicies = new NodeWeights.Features(3, 2, 1, 0, 262, 0, 1);
    NodeWeights.Features withoutPolicies = new NodeWeights.Features(3, 2, 1, 0, 0, 0, 1);
    assertThat(NodeWeights.weightOf(withPolicies), equalTo(NodeWeights.weightOf(withoutPolicies)));
  }

  /**
   * A line endpoint vs interior: the interior has one more interface and one more peer, and its
   * measured main RIB is 2 routes larger; the calibrated model must rank it higher.
   */
  @Test
  public void testCalibratedModelRanksLineInteriorAboveEndpoint() {
    NodeWeights.Features endpoint = new NodeWeights.Features(2, 1, 1, 0, 6, 0, 1);
    NodeWeights.Features interior = new NodeWeights.Features(3, 2, 1, 0, 7, 0, 1);
    assertThat(NodeWeights.weightOf(interior), greaterThan(NodeWeights.weightOf(endpoint)));
  }

  /**
   * The v2 full-table estimate is the number of prefixes originated anywhere in the node's BGP
   * connected component (its propagation closure). A node with no session forms a singleton
   * component and only counts its own origination.
   */
  @Test
  public void testFullTableRoutesPerBgpComponent() {
    Map<String, Configuration> configs =
        ImmutableMap.of(
            "a", configWithBgp("a", 2),
            "b", configWithBgp("b", 1),
            "c", configWithBgp("c", 1),
            "d", configWithBgp("d", 0));
    Map<String, Set<String>> adjacency =
        ImmutableMap.of(
            "a", ImmutableSet.of("b"),
            "b", ImmutableSet.of("a", "c"),
            "c", ImmutableSet.of("b"),
            "d", ImmutableSet.of());
    assertThat(
        NodeWeights.fullTableRoutes(configs, adjacency),
        equalTo(ImmutableMap.of("a", 4, "b", 4, "c", 4, "d", 0)));
  }

  /** The v2 correction is off unless {@code -Ds2.nodeWeightsV2=true}. */
  @Test
  public void testV2CorrectionDisabledByDefault() {
    Map<String, Configuration> configs =
        ImmutableMap.of("a", configWithBgp("a", 2), "b", configWithBgp("b", 1));
    Map<String, Set<String>> adjacency =
        ImmutableMap.of("a", ImmutableSet.of("b"), "b", ImmutableSet.of("a"));
    String previous = System.getProperty(NodeWeights.V2_PROPERTY);
    try {
      System.clearProperty(NodeWeights.V2_PROPERTY);
      assertThat(NodeWeights.compute(configs, adjacency), equalTo(NodeWeights.compute(configs)));
    } finally {
      restoreProperty(NodeWeights.V2_PROPERTY, previous);
    }
  }

  /** With v2 enabled the weight is the base weight plus the full-table route term. */
  @Test
  public void testV2CorrectionAddsFullTableWeight() {
    Map<String, Configuration> configs =
        ImmutableMap.of(
            "a", configWithBgp("a", 2),
            "b", configWithBgp("b", 1),
            "d", configWithBgp("d", 0));
    Map<String, Set<String>> adjacency =
        ImmutableMap.of(
            "a", ImmutableSet.of("b"),
            "b", ImmutableSet.of("a"),
            "d", ImmutableSet.of());
    Map<String, Integer> base = NodeWeights.compute(configs);
    String previous = System.getProperty(NodeWeights.V2_PROPERTY);
    String previousScale = System.getProperty(NodeWeights.V2_SCALE_PROPERTY);
    try {
      System.setProperty(NodeWeights.V2_PROPERTY, "true");
      System.setProperty(NodeWeights.V2_SCALE_PROPERTY, "2");
      Map<String, Integer> corrected = NodeWeights.compute(configs, adjacency);
      // Component {a,b} originates 3 prefixes; d is a singleton with none.
      assertThat(corrected.get("a"), equalTo(base.get("a") + 2 * 3));
      assertThat(corrected.get("b"), equalTo(base.get("b") + 2 * 3));
      assertThat(corrected.get("d"), equalTo(base.get("d")));
      // Deterministic.
      assertThat(NodeWeights.compute(configs, adjacency), equalTo(corrected));
    } finally {
      restoreProperty(NodeWeights.V2_PROPERTY, previous);
      restoreProperty(NodeWeights.V2_SCALE_PROPERTY, previousScale);
    }
  }

  /**
   * The adaptive role rule: drop the peer term when the busiest tier has at least as many
   * interfaces as the least-connected tier (s2-fat4-like), keep it when the busiest tier has fewer
   * (s2-fat2-like), and drop it when every router has the same peer count.
   */
  @Test
  public void testAdaptivePeerCoefficient() {
    // s2-fat4: core/agg (4 peers, 5 interfaces), edge (2 peers, 5 interfaces) -> equal, drop peers.
    assertThat(
        NodeWeights.adaptivePeerCoefficient(
            ImmutableList.of(
                new NodeWeights.Features(5, 4, 1, 0, 9, 0, 1),
                new NodeWeights.Features(5, 2, 3, 0, 9, 0, 1))),
        equalTo(0));
    // s2-fat2: core/agg (2 peers, 3 interfaces), edge (1 peer, 4 interfaces) -> keep peers.
    assertThat(
        NodeWeights.adaptivePeerCoefficient(
            ImmutableList.of(
                new NodeWeights.Features(3, 2, 1, 0, 7, 0, 1),
                new NodeWeights.Features(4, 1, 3, 0, 8, 0, 1))),
        equalTo(NodeWeights.BGP_PEER));
    // Uniform peers: no role signal.
    assertThat(
        NodeWeights.adaptivePeerCoefficient(
            ImmutableList.of(
                new NodeWeights.Features(3, 2, 1, 0, 7, 0, 1),
                new NodeWeights.Features(4, 2, 3, 0, 8, 0, 1))),
        equalTo(0));
  }

  /**
   * The same role rule fires on the two shapes added for the §6.12 generalization check: a k=6
   * FatTree (core/agg 6 peers / 7 interfaces, edge 3 peers / 6 interfaces) and a
   * hub/route-reflector star (hub 8 peers / 9 interfaces, leaf 1 peer / 4 interfaces). Both have a
   * busier tier with at least as many interfaces, so the peer term is dropped.
   */
  @Test
  public void testAdaptivePeerCoefficientOnFat6AndHubShapes() {
    assertThat(
        NodeWeights.adaptivePeerCoefficient(
            ImmutableList.of(
                new NodeWeights.Features(7, 6, 1, 0, 11, 0, 1),
                new NodeWeights.Features(6, 3, 3, 0, 14, 0, 1))),
        equalTo(0));
    assertThat(
        NodeWeights.adaptivePeerCoefficient(
            ImmutableList.of(
                new NodeWeights.Features(9, 8, 1, 0, 16, 0, 1),
                new NodeWeights.Features(4, 1, 3, 0, 8, 0, 1))),
        equalTo(0));
  }

  /** Role scaling is off by default; the peer-scale override wins when set. */
  @Test
  public void testEffectivePeerCoefficientPrecedence() {
    java.util.List<NodeWeights.Features> fat4 =
        ImmutableList.of(
            new NodeWeights.Features(5, 4, 1, 0, 9, 0, 1),
            new NodeWeights.Features(5, 2, 3, 0, 9, 0, 1));
    String previousRole = System.getProperty(NodeWeights.ROLE_SCALE_PROPERTY);
    String previousPeer = System.getProperty(NodeWeights.PEER_SCALE_PROPERTY);
    try {
      System.clearProperty(NodeWeights.ROLE_SCALE_PROPERTY);
      System.clearProperty(NodeWeights.PEER_SCALE_PROPERTY);
      assertThat(NodeWeights.effectivePeerCoefficient(fat4), equalTo(NodeWeights.BGP_PEER));
      System.setProperty(NodeWeights.ROLE_SCALE_PROPERTY, "true");
      assertThat(NodeWeights.effectivePeerCoefficient(fat4), equalTo(0));
      System.setProperty(NodeWeights.PEER_SCALE_PROPERTY, "2");
      assertThat(NodeWeights.effectivePeerCoefficient(fat4), equalTo(2));
    } finally {
      restoreProperty(NodeWeights.ROLE_SCALE_PROPERTY, previousRole);
      restoreProperty(NodeWeights.PEER_SCALE_PROPERTY, previousPeer);
    }
  }

  private static void restoreProperty(String name, String previous) {
    if (previous == null) {
      System.clearProperty(name);
    } else {
      System.setProperty(name, previous);
    }
  }

  private static Configuration configWithBgp(String hostname, int originatedPrefixes) {
    Configuration c = configWithInterface(hostname, "i1", "10.0.0.1/30");
    Vrf vrf = c.getVrfs().get(Configuration.DEFAULT_VRF_NAME);
    BgpProcess process = BgpProcess.testBgpProcess(Ip.parse("1.1.1.1"));
    PrefixSpace space = new PrefixSpace();
    for (int i = 0; i < originatedPrefixes; i++) {
      space.addPrefixRange(PrefixRange.fromPrefix(Prefix.parse("10." + i + ".0.0/24")));
    }
    process.setOriginationSpace(space);
    vrf.setBgpProcess(process);
    return c;
  }

  private static Configuration configWithInterface(
      String hostname, String ifaceName, String address) {
    Configuration c =
        Configuration.builder()
            .setHostname(hostname)
            .setConfigurationFormat(ConfigurationFormat.CISCO_IOS)
            .build();
    Vrf vrf = Vrf.builder().setName(Configuration.DEFAULT_VRF_NAME).setOwner(c).build();
    Interface.builder()
        .setName(ifaceName)
        .setOwner(c)
        .setVrf(vrf)
        .setType(InterfaceType.PHYSICAL)
        .setAddress(ConcreteInterfaceAddress.parse(address))
        .build();
    return c;
  }
}
