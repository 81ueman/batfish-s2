// SPDX-License-Identifier: Apache-2.0
package org.batfish.dataplane.ibdp.partition;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.lessThanOrEqualTo;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.ImmutableSortedSet;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import org.batfish.datamodel.AclLine;
import org.batfish.datamodel.ConcreteInterfaceAddress;
import org.batfish.datamodel.Configuration;
import org.batfish.datamodel.ConfigurationFormat;
import org.batfish.datamodel.Edge;
import org.batfish.datamodel.ExprAclLine;
import org.batfish.datamodel.Interface;
import org.batfish.datamodel.InterfaceType;
import org.batfish.datamodel.IpAccessList;
import org.batfish.datamodel.LineAction;
import org.batfish.datamodel.Prefix;
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

  @Test
  public void testPartitionSchemeParse() {
    assertThat(
        PartitionScheme.tryParse("weighted_lpt_fm"), equalTo(PartitionScheme.WEIGHTED_LPT_FM));
    assertThat(PartitionScheme.tryParse("MetIs"), equalTo(PartitionScheme.METIS));
    assertThat(PartitionScheme.tryParse("bogus"), equalTo(null));
    assertThat(PartitionScheme.tryParse(null), equalTo(null));

    String previous = System.getProperty(PartitionScheme.PROPERTY);
    try {
      System.clearProperty(PartitionScheme.PROPERTY);
      assertThat(PartitionScheme.fromSystemProperties(), equalTo(PartitionScheme.RANDOM));
      System.setProperty(PartitionScheme.PROPERTY, "greedy_region");
      assertThat(PartitionScheme.fromSystemProperties(), equalTo(PartitionScheme.GREEDY_REGION));
    } finally {
      if (previous == null) {
        System.clearProperty(PartitionScheme.PROPERTY);
      } else {
        System.setProperty(PartitionScheme.PROPERTY, previous);
      }
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
