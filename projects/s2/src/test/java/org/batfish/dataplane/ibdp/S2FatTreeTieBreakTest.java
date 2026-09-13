// SPDX-License-Identifier: Apache-2.0

package org.batfish.dataplane.ibdp;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

import com.google.common.collect.HashBasedTable;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Table;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import org.batfish.common.NetworkSnapshot;
import org.batfish.common.plugin.DataPlanePlugin.ComputeDataPlaneResult;
import org.batfish.common.topology.IpOwners;
import org.batfish.common.topology.TopologyProvider;
import org.batfish.datamodel.BgpAdvertisement;
import org.batfish.datamodel.BgpProcess;
import org.batfish.datamodel.BgpTieBreaker;
import org.batfish.datamodel.Configuration;
import org.batfish.datamodel.DataPlane;
import org.batfish.datamodel.FinalMainRib;
import org.batfish.datamodel.isis.IsisTopology;
import org.batfish.dataplane.ibdp.schedule.IbdpSchedule.Schedule;
import org.batfish.main.Batfish;
import org.batfish.main.BatfishTestUtils;
import org.batfish.main.TestrigText;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

/**
 * Root-cause characterization for the C1 residual: {@code networks/s2-fat4}, a 20-switch FatTree
 * k=4 with eBGP on every switch, has multiple equal-cost BGP fixed points. Vanilla Batfish uses the
 * {@link Schedule#NODE_COLORED} schedule; the S2 distributed engine forces {@link Schedule#ALL}.
 *
 * <p>Selection among the equal-cost routes is order-sensitive: {@code BgpRib.comparePreference}
 * returns {@code < 0} for equal-length, different-AS-path routes under the default {@code
 * EXACT_PATH} multipath mode, so {@code RibTree.mergeRoute} keeps whichever route arrived first;
 * when AS paths are exactly equal the default {@code ARRIVAL_ORDER} tie-breaker compares the
 * merge-time arrival clock. The schedule therefore determines the fixed point. This test proves
 * that causally: it compares vanilla, the stock engine forced to {@code ALL}, the stock engine
 * forced to {@code NODE_COLORED}, and the S2 engine.
 */
public class S2FatTreeTieBreakTest {

  private static final String TESTRIG = "org/batfish/dataplane/testrigs/s2-fat4";
  private static final List<String> CONFIGS =
      ImmutableList.of(
          "sw0", "sw1", "sw2", "sw3", "sw4", "sw5", "sw6", "sw7", "sw8", "sw9", "sw10", "sw11",
          "sw12", "sw13", "sw14", "sw15", "sw16", "sw17", "sw18", "sw19");

  @Rule public TemporaryFolder _folder = new TemporaryFolder();

  private record Setups(
      SortedMap<String, Configuration> configs,
      Set<BgpAdvertisement> adverts,
      TopologyContext tc,
      IpOwners ipOwners,
      IncrementalDataPlaneSettings settings) {}

  /**
   * Fresh Batfish/config context; each engine run gets its own so cross-run mutation cannot leak.
   */
  private Setups freshSetups() throws Exception {
    Batfish batfish =
        BatfishTestUtils.getBatfishFromTestrigText(
            TestrigText.builder().setConfigurationFiles(TESTRIG, CONFIGS).build(), _folder);
    NetworkSnapshot snapshot = batfish.getSnapshot();
    SortedMap<String, Configuration> configs = batfish.loadConfigurations(snapshot);
    Set<BgpAdvertisement> adverts = batfish.loadExternalBgpAnnouncements(snapshot, configs);
    TopologyProvider tp = batfish.getTopologyProvider();
    TopologyContext tc =
        TopologyContext.builder()
            .setIpsecTopology(tp.getInitialIpsecTopology(snapshot))
            .setIsisTopology(
                IsisTopology.initIsisTopology(configs, tp.getInitialLayer3Topology(snapshot)))
            .setLayer3Topology(tp.getInitialLayer3Topology(snapshot))
            .setLayer1Topologies(tp.getLayer1Topologies(snapshot))
            .setL3Adjacencies(tp.getInitialL3Adjacencies(snapshot))
            .setOspfTopology(tp.getInitialOspfTopology(snapshot))
            .setTunnelTopology(tp.getInitialTunnelTopology(snapshot))
            .build();
    return new Setups(
        configs,
        adverts,
        tc,
        tp.getInitialIpOwners(snapshot),
        new IncrementalDataPlaneSettings(batfish.getSettingsConfiguration()));
  }

  private Map<String, Map<String, Set<String>>> computeVanilla() throws Exception {
    Batfish batfish =
        BatfishTestUtils.getBatfishFromTestrigText(
            TestrigText.builder().setConfigurationFiles(TESTRIG, CONFIGS).build(), _folder);
    NetworkSnapshot snapshot = batfish.getSnapshot();
    batfish.computeDataPlane(snapshot);
    return canonical(batfish.loadDataPlane(snapshot));
  }

  @Test
  public void testScheduleIsTheCause() throws Exception {
    Map<String, Map<String, Set<String>>> vanilla = computeVanilla();

    Map<String, Map<String, Set<String>>> stockColored1 = runStock(Schedule.NODE_COLORED);
    Map<String, Map<String, Set<String>>> stockColored2 = runStock(Schedule.NODE_COLORED);
    Map<String, Map<String, Set<String>>> stockAll1 = runStock(Schedule.ALL);
    Map<String, Map<String, Set<String>>> stockAll2 = runStock(Schedule.ALL);
    // With NODE_COLORED now the default, an unmodified S2 engine is the fix.
    Map<String, Map<String, Set<String>>> s2A = runS2(1);
    Map<String, Map<String, Set<String>>> s2B = runS2(1);
    Map<String, Map<String, Set<String>>> s2Default3 = runS2(3);
    // Diagnostic: does the alternative bestPathComparator (prefer lower originatorIp) pin the
    // result down? It only affects the comparator, not the earlier EXACT_PATH AS-path check.
    Map<String, Map<String, Set<String>>> routerId1 =
        runStockWithTieBreaker(Schedule.ALL, BgpTieBreaker.ROUTER_ID);
    Map<String, Map<String, Set<String>>> routerId2 =
        runStockWithTieBreaker(Schedule.ALL, BgpTieBreaker.ROUTER_ID);

    System.out.println("=== s2-fat4 BGP tie-break evidence ===");
    System.out.println("vanilla == stock(NODE_COLORED):    " + vanilla.equals(stockColored1));
    System.out.println("stock(NODE_COLORED) run1 == run2:  " + stockColored1.equals(stockColored2));
    System.out.println("vanilla == stock(ALL):             " + vanilla.equals(stockAll1));
    System.out.println("stock(ALL) run1 == run2:           " + stockAll1.equals(stockAll2));
    System.out.println("s2(1w, default) run1 == run2:      " + s2A.equals(s2B));
    System.out.println("vanilla == s2(1w, default):        " + vanilla.equals(s2A));
    System.out.println("vanilla == s2(3w, default):        " + vanilla.equals(s2Default3));
    System.out.println("stock(ALL, ROUTER_ID) run1==run2:  " + routerId1.equals(routerId2));
    System.out.println("vanilla == stock(ALL, ROUTER_ID):  " + vanilla.equals(routerId1));
    printDiffSummary("vanilla vs stock(ALL)", vanilla, stockAll1);

    // Vanilla's NODE_COLORED schedule is deterministic and reproduces vanilla.
    assertThat(vanilla.equals(stockColored1), is(true));
    assertThat(stockColored1.equals(stockColored2), is(true));
    // ...and ordering-sensitive: the same network under Schedule.ALL selects a different fixed
    // point (and is itself not run-to-run stable on this cyclic equal-cost topology).
    assertThat(
        "vanilla and stock(ALL) must differ on this topology",
        vanilla.equals(stockAll1),
        is(false));
    // The S2 default (NODE_COLORED) is deterministic and matches vanilla at 1 and 3 workers; the
    // 3-worker assertion is also the cross-worker consistency check: a coloring mismatch would
    // have misaligned the per-step barriers and hung the test rather than returned.
    assertThat(s2A.equals(s2B), is(true));
    assertThat(vanilla.equals(s2A), is(true));
    assertThat(vanilla.equals(s2Default3), is(true));
  }

  /** The default (unmodified) S2 engine must reproduce vanilla at 1 and 3 workers. */
  @Test
  public void testDefaultScheduleReproducesVanilla() throws Exception {
    Map<String, Map<String, Set<String>>> vanilla = computeVanilla();
    assertThat(vanilla.equals(runS2(1)), is(true));
    assertThat(vanilla.equals(runS2(3)), is(true));
  }

  /**
   * {@code -Ds2.egpSchedule=ALL} remains the escape hatch: it restores the historical single-round
   * schedule, which on this cyclic equal-cost topology selects a different fixed point than
   * vanilla.
   */
  @Test
  public void testAllScheduleEscapeHatch() throws Exception {
    Map<String, Map<String, Set<String>>> vanilla = computeVanilla();
    System.setProperty("s2.egpSchedule", "ALL");
    try {
      Map<String, Map<String, Set<String>>> all = runS2(1);
      assertThat("ALL must still differ from vanilla on s2-fat4", vanilla.equals(all), is(false));
    } finally {
      System.clearProperty("s2.egpSchedule");
    }
  }

  private Map<String, Map<String, Set<String>>> runStock(Schedule schedule) throws Exception {
    Setups s = freshSetups();
    IncrementalDataPlaneSettings settings =
        new IncrementalDataPlaneSettings(s.settings().getConfig());
    settings.getConfig().setProperty("schedule", schedule.toString());
    ComputeDataPlaneResult result =
        new IncrementalBdpEngine(settings)
            .computeDataPlane(s.configs(), s.tc(), s.adverts(), s.ipOwners(), false);
    return canonical(result._dataPlane);
  }

  /** Stock engine with every BGP process forced to {@code tieBreaker}, for mechanism isolation. */
  private Map<String, Map<String, Set<String>>> runStockWithTieBreaker(
      Schedule schedule, BgpTieBreaker tieBreaker) throws Exception {
    Setups s = freshSetups();
    for (Configuration c : s.configs().values()) {
      BgpProcess p = c.getDefaultVrf().getBgpProcess();
      if (p != null) {
        p.setTieBreaker(tieBreaker);
      }
    }
    IncrementalDataPlaneSettings settings =
        new IncrementalDataPlaneSettings(s.settings().getConfig());
    settings.getConfig().setProperty("schedule", schedule.toString());
    ComputeDataPlaneResult result =
        new IncrementalBdpEngine(settings)
            .computeDataPlane(s.configs(), s.tc(), s.adverts(), s.ipOwners(), false);
    return canonical(result._dataPlane);
  }

  // The worker pool is shut down in the finally block below; PMD's CloseResource only recognizes
  // close()/try-with-resources, so suppress it here.
  @SuppressWarnings("PMD.CloseResource")
  private Map<String, Map<String, Set<String>>> runS2(int workers) throws Exception {
    Setups s = freshSetups();
    Map<String, Integer> assignment =
        NetworkPartitioner.partition(s.configs().keySet(), workers, 0L);
    Map<String, DistributedNode> realByHost = new HashMap<>();
    for (String host : s.configs().keySet()) {
      realByHost.put(host, DistributedNode.real(s.configs().get(host)));
    }
    S2Cluster cluster = new S2Cluster(workers);
    List<S2BdpEngine> engines = new ArrayList<>();
    for (int w = 0; w < workers; w++) {
      Map<String, DistributedNode> nodes = new HashMap<>();
      for (String host : s.configs().keySet()) {
        nodes.put(
            host,
            assignment.get(host) == w
                ? realByHost.get(host)
                : DistributedNode.shadowOf(realByHost.get(host)));
      }
      engines.add(new S2BdpEngine(s.settings(), nodes, cluster, null, ImmutableSet.of()));
    }
    ExecutorService pool = Executors.newFixedThreadPool(workers);
    try {
      List<Future<ComputeDataPlaneResult>> futures = new ArrayList<>();
      for (S2BdpEngine engine : engines) {
        futures.add(
            pool.submit(
                () ->
                    engine.computeDataPlane(
                        s.configs(), s.tc(), s.adverts(), s.ipOwners(), false)));
      }
      Table<String, String, FinalMainRib> merged = HashBasedTable.create();
      for (Future<ComputeDataPlaneResult> future : futures) {
        DataPlane dp = future.get()._dataPlane;
        dp.getRibs()
            .cellSet()
            .forEach(c -> merged.put(c.getRowKey(), c.getColumnKey(), c.getValue()));
      }
      return canonical(merged);
    } finally {
      pool.shutdownNow();
    }
  }

  private static Map<String, Map<String, Set<String>>> canonical(DataPlane dp) {
    Map<String, Map<String, Set<String>>> result = new TreeMap<>();
    dp.getRibs()
        .cellSet()
        .forEach(c -> put(result, c.getRowKey(), c.getColumnKey(), c.getValue().getRoutes()));
    return result;
  }

  private static Map<String, Map<String, Set<String>>> canonical(
      Table<String, String, FinalMainRib> table) {
    Map<String, Map<String, Set<String>>> result = new TreeMap<>();
    table
        .cellSet()
        .forEach(c -> put(result, c.getRowKey(), c.getColumnKey(), c.getValue().getRoutes()));
    return result;
  }

  private static void put(
      Map<String, Map<String, Set<String>>> result,
      String host,
      String vrf,
      Set<? extends Object> routes) {
    Set<String> names = new TreeSet<>();
    routes.forEach(r -> names.add(r.toString()));
    result.computeIfAbsent(host, h -> new TreeMap<>()).put(vrf, names);
  }

  /** Compact evidence: the differing (host, prefix) pairs and their AS paths. */
  private static void printDiffSummary(
      String label,
      Map<String, Map<String, Set<String>>> a,
      Map<String, Map<String, Set<String>>> b) {
    System.out.println("--- " + label + " ---");
    int n = 0;
    for (String host : a.keySet()) {
      for (String vrf : a.get(host).keySet()) {
        Set<String> onlyA = new TreeSet<>(a.get(host).get(vrf));
        onlyA.removeAll(b.getOrDefault(host, Map.of()).getOrDefault(vrf, Set.of()));
        Set<String> onlyB =
            new TreeSet<>(b.getOrDefault(host, Map.of()).getOrDefault(vrf, Set.of()));
        onlyB.removeAll(a.get(host).get(vrf));
        if (!onlyA.isEmpty() || !onlyB.isEmpty()) {
          System.out.printf("  %s/%s: %s%n", host, vrf, describe(onlyA, onlyB));
          n += onlyA.size() + onlyB.size();
        }
      }
    }
    System.out.println("  (" + n + " differing routes)");
  }

  private static String describe(Set<String> onlyA, Set<String> onlyB) {
    // route string -> (prefix, path) for compactness
    return "a=" + summarize(onlyA) + " b=" + summarize(onlyB);
  }

  private static String summarize(Set<String> routes) {
    List<String> out = new ArrayList<>();
    for (String r : routes) {
      String prefix = field(r, "_network=");
      String path = field(r, "_asPath=");
      out.add(prefix + " via " + path);
    }
    return out.toString();
  }

  private static String field(String route, String key) {
    int i = route.indexOf(key);
    if (i < 0) {
      return "?";
    }
    int start = i + key.length();
    int end = route.indexOf(',', start);
    return end < 0 ? route.substring(start) : route.substring(start, end);
  }
}
