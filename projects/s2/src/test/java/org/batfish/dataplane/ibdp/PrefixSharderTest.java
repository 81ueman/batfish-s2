// SPDX-License-Identifier: Apache-2.0
package org.batfish.dataplane.ibdp;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.ToIntFunction;
import org.batfish.datamodel.BgpProcess;
import org.batfish.datamodel.Configuration;
import org.batfish.datamodel.Prefix;
import org.batfish.datamodel.PrefixSpace;
import org.batfish.datamodel.bgp.BgpAggregate;
import org.batfish.main.Batfish;
import org.batfish.main.BatfishTestUtils;
import org.batfish.main.TestrigText;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

/** Unit tests for the prefix-sharding universe and dependency grouping ({@link PrefixSharder}). */
public class PrefixSharderTest {

  private static final String TRIANGLE = "org/batfish/dataplane/testrigs/s2-triangle";
  private static final List<String> TRIANGLE_CONFIGS = ImmutableList.of("r1", "r2", "r3");
  private static final String AGG = "org/batfish/dataplane/testrigs/s2-agg";
  private static final List<String> AGG_CONFIGS = ImmutableList.of("r1", "r2", "r3");

  private static final Prefix AGGREGATE = Prefix.parse("2.128.0.0/16");
  private static final Prefix SPECIFIC_1 = Prefix.parse("2.128.1.0/24");
  private static final Prefix SPECIFIC_2 = Prefix.parse("2.128.2.0/24");

  @Rule public TemporaryFolder _folder = new TemporaryFolder();

  private Map<String, Configuration> load(String testrig, List<String> configs) throws Exception {
    Batfish batfish =
        BatfishTestUtils.getBatfishFromTestrigText(
            TestrigText.builder().setConfigurationFiles(testrig, configs).build(), _folder);
    return batfish.loadConfigurations(batfish.getSnapshot());
  }

  private static BgpProcess bgpProcess(Map<String, Configuration> configs, String host) {
    return configs.get(host).getVrfs().get(Configuration.DEFAULT_VRF_NAME).getBgpProcess();
  }

  private static Set<Prefix> componentContaining(List<Set<Prefix>> components, Prefix prefix) {
    return components.stream()
        .filter(c -> c.contains(prefix))
        .findFirst()
        .orElseThrow(() -> new AssertionError("no component contains " + prefix));
  }

  /** An extra prefix (e.g. an external BGP announcement) must land in some shard. */
  @Test
  public void testExtraPrefixesIncluded() throws Exception {
    Map<String, Configuration> configs = load(TRIANGLE, TRIANGLE_CONFIGS);
    Prefix external = Prefix.parse("203.0.113.0/24");
    List<PrefixSpace> shards = PrefixSharder.shards(configs, ImmutableList.of(external), 3);
    assertThat(
        "external prefix must be appointed in some shard",
        shards.stream().anyMatch(s -> s.containsPrefix(external)),
        is(true));
  }

  /** An aggregate and a prefix it covers must be assigned to the same shard. */
  @Test
  public void testAggregateAndCoveredPrefixShareShard() throws Exception {
    Map<String, Configuration> configs = load(AGG, AGG_CONFIGS);
    List<PrefixSpace> shards = PrefixSharder.shards(configs, ImmutableList.of(), 3);
    assertThat(
        "aggregate and its more-specific must be co-sharded",
        shards.stream().anyMatch(s -> s.containsPrefix(AGGREGATE) && s.containsPrefix(SPECIFIC_1)),
        is(true));
  }

  /**
   * The DPDG puts an aggregate and every more-specific it covers in one weakly connected component.
   */
  @Test
  public void testDependencyGraphGroupsAggregateWithCoveredPrefixes() throws Exception {
    Map<String, Configuration> configs = load(AGG, AGG_CONFIGS);
    PrefixDependencyGraph graph = PrefixDependencyGraph.build(configs, ImmutableList.of());
    Set<Prefix> component = componentContaining(graph.weaklyConnectedComponents(), AGGREGATE);
    assertThat(
        "aggregate and both covered more-specifics form one component",
        component.containsAll(ImmutableSet.of(AGGREGATE, SPECIFIC_1, SPECIFIC_2)),
        is(true));
  }

  /** A summary-only aggregate records an explicit suppression dependency on its contributors. */
  @Test
  public void testSummaryOnlyEdgeRecorded() throws Exception {
    Map<String, Configuration> configs = load(AGG, AGG_CONFIGS);
    PrefixDependencyGraph graph = PrefixDependencyGraph.build(configs, ImmutableList.of());
    Set<Prefix> suppressed =
        graph.edges().stream()
            .filter(e -> e.kind() == PrefixDependencyGraph.EdgeKind.SUMMARY_ONLY)
            .filter(e -> e.tail().equals(AGGREGATE))
            .map(PrefixDependencyGraph.Edge::head)
            .collect(ImmutableSet.toImmutableSet());
    assertThat(
        "summary-only aggregate suppresses both covered more-specifics",
        suppressed,
        equalTo(ImmutableSet.of(SPECIFIC_1, SPECIFIC_2)));
  }

  /** A more-general aggregate and a more-specific aggregate must not be split across shards. */
  @Test
  public void testNestedAggregatesCoSharded() throws Exception {
    Map<String, Configuration> configs = load(AGG, AGG_CONFIGS);
    Prefix general = Prefix.parse("2.0.0.0/8");
    bgpProcess(configs, "r2").addAggregate(BgpAggregate.of(general, null, null, null));
    List<PrefixSpace> shards = PrefixSharder.shards(configs, ImmutableList.of(), 3);
    assertThat(
        "nested aggregates and the covered more-specifics must be co-sharded",
        shards.stream()
            .anyMatch(
                s ->
                    s.containsPrefix(general)
                        && s.containsPrefix(AGGREGATE)
                        && s.containsPrefix(SPECIFIC_1)),
        is(true));
  }

  /**
   * A single component covering the whole universe cannot be split; sharding degrades to one shard.
   */
  @Test
  public void testDegenerateGraphFallsBackToSingleShard() throws Exception {
    Map<String, Configuration> configs = load(AGG, AGG_CONFIGS);
    Prefix defaultRoute = Prefix.ZERO;
    bgpProcess(configs, "r2").addAggregate(BgpAggregate.of(defaultRoute, null, null, null));
    PrefixDependencyGraph graph = PrefixDependencyGraph.build(configs, ImmutableList.of());
    assertThat(
        "a default-route aggregate connects the whole universe",
        graph.weaklyConnectedComponents().size(),
        is(1));
    List<PrefixSpace> shards = PrefixSharder.shards(configs, ImmutableList.of(), 4);
    assertThat("degenerate graph falls back to a single shard", shards.size(), is(1));
    assertThat(
        "the single shard still contains the default route and a specific",
        shards.get(0).containsPrefix(defaultRoute) && shards.get(0).containsPrefix(SPECIFIC_1),
        is(true));
  }

  /**
   * Weighted LPT isolates a small but heavy component, where raw prefix-count LPT would co-locate
   * it with another component.
   */
  @Test
  public void testWeightedLptIsolatesHeavyComponent() {
    Prefix heavyPrefix = Prefix.parse("10.0.0.0/8");
    Set<Prefix> heavy = ImmutableSet.of(heavyPrefix);
    Set<Prefix> lightA =
        ImmutableSet.of(
            Prefix.parse("20.0.1.0/24"),
            Prefix.parse("20.0.2.0/24"),
            Prefix.parse("20.0.3.0/24"),
            Prefix.parse("20.0.4.0/24"));
    Set<Prefix> lightB =
        ImmutableSet.of(
            Prefix.parse("30.0.1.0/24"),
            Prefix.parse("30.0.2.0/24"),
            Prefix.parse("30.0.3.0/24"),
            Prefix.parse("30.0.4.0/24"));
    ToIntFunction<Set<Prefix>> weigher = c -> c.contains(heavyPrefix) ? 100 : c.size();
    List<PrefixSpace> shards =
        PrefixSharder.assignComponents(ImmutableList.of(heavy, lightA, lightB), weigher, 2);
    PrefixSpace heavyShard =
        shards.stream()
            .filter(s -> s.containsPrefix(heavyPrefix))
            .findFirst()
            .orElseThrow(() -> new AssertionError("heavy prefix not appointed"));
    assertThat(
        "the heavy component is alone in its shard",
        heavyShard.containsPrefix(Prefix.parse("20.0.1.0/24")),
        is(false));
    assertThat(
        "the other shard carries both light components",
        shards.stream()
            .filter(s -> !s.containsPrefix(heavyPrefix))
            .anyMatch(
                s ->
                    s.containsPrefix(Prefix.parse("20.0.1.0/24"))
                        && s.containsPrefix(Prefix.parse("30.0.1.0/24"))),
        is(true));
  }

  /** The DPDG weight is an estimate of route contribution, not the raw prefix count. */
  @Test
  public void testComponentWeightExceedsPrefixCount() throws Exception {
    Map<String, Configuration> configs = load(AGG, AGG_CONFIGS);
    PrefixDependencyGraph graph = PrefixDependencyGraph.build(configs, ImmutableList.of());
    Set<Prefix> component = componentContaining(graph.weaklyConnectedComponents(), AGGREGATE);
    assertThat(
        "aggregate component weight exceeds its node count",
        graph.componentWeight(component) > component.size(),
        is(true));
  }

  /** Sharding is deterministic across repeated invocations on the same snapshot. */
  @Test
  public void testShardsDeterministic() throws Exception {
    Map<String, Configuration> configs = load(AGG, AGG_CONFIGS);
    List<PrefixSpace> first = PrefixSharder.shards(configs, ImmutableList.of(), 3);
    List<PrefixSpace> second = PrefixSharder.shards(configs, ImmutableList.of(), 3);
    assertThat("shards are deterministic", second, equalTo(first));
    for (PrefixSpace shard : first) {
      assertThat("no shard is empty", shard.getPrefixRanges().isEmpty(), is(false));
    }
    int components =
        PrefixDependencyGraph.build(configs, ImmutableList.of()).weaklyConnectedComponents().size();
    assertThat("one shard per component up to n", first.size(), is(Math.min(3, components)));
    // Every universe prefix is appointed at least once.
    Set<Prefix> universe = ImmutableSet.copyOf(PrefixSharder.queryPrefixes(configs));
    for (Prefix prefix : universe) {
      assertThat(
          "universe prefix " + prefix + " is appointed",
          first.stream().anyMatch(s -> s.containsPrefix(prefix)),
          is(true));
    }
  }
}
