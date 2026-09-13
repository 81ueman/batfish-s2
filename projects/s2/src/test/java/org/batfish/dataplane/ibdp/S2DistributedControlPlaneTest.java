// SPDX-License-Identifier: Apache-2.0

package org.batfish.dataplane.ibdp;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;

import com.google.common.collect.HashBasedTable;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Table;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SortedMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import org.batfish.common.NetworkSnapshot;
import org.batfish.common.plugin.DataPlanePlugin.ComputeDataPlaneResult;
import org.batfish.common.topology.IpOwners;
import org.batfish.common.topology.TopologyProvider;
import org.batfish.datamodel.BgpAdvertisement;
import org.batfish.datamodel.Bgpv4Route;
import org.batfish.datamodel.Configuration;
import org.batfish.datamodel.DataPlane;
import org.batfish.datamodel.FinalMainRib;
import org.batfish.datamodel.IpSpace;
import org.batfish.datamodel.Prefix;
import org.batfish.datamodel.RoutingProtocol;
import org.batfish.datamodel.VrfForwardingBehavior;
import org.batfish.datamodel.isis.IsisTopology;
import org.batfish.main.Batfish;
import org.batfish.main.BatfishTestUtils;
import org.batfish.main.TestrigText;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

/**
 * Milestone 1: prove that partitioning the network across N logical workers and running one Batfish
 * engine per worker (with shadow nodes backed by the owning worker's real process) produces exactly
 * the same main RIBs as vanilla single-machine Batfish.
 */
public class S2DistributedControlPlaneTest {

  private static final String TESTRIG = "org/batfish/dataplane/testrigs/s2-triangle";
  private static final List<String> CONFIGS = ImmutableList.of("r1", "r2", "r3");
  private static final String LINE_TESTRIG = "org/batfish/dataplane/testrigs/s2-line";
  private static final List<String> LINE_CONFIGS =
      ImmutableList.of("r1", "r2", "r3", "r4", "r5", "r6");
  private static final String OSPF_TESTRIG = "org/batfish/dataplane/testrigs/s2-ospf";
  private static final List<String> OSPF_CONFIGS = ImmutableList.of("r1", "r2", "r3", "r4");
  private static final String OSPF_BGP_TESTRIG = "org/batfish/dataplane/testrigs/s2-ospf-bgp";
  private static final List<String> OSPF_BGP_CONFIGS = ImmutableList.of("r1", "r2", "r3");
  private static final String REDIST_TESTRIG = "org/batfish/dataplane/testrigs/s2-redist";
  private static final List<String> REDIST_CONFIGS = ImmutableList.of("r1", "r2", "r3", "r4");
  private static final String AGG_TESTRIG = "org/batfish/dataplane/testrigs/s2-agg";
  private static final List<String> AGG_CONFIGS = ImmutableList.of("r1", "r2", "r3");
  private static final String STATIC_TESTRIG = "org/batfish/dataplane/testrigs/s2-static";
  private static final List<String> STATIC_CONFIGS = ImmutableList.of("r1", "r2");
  private static final String EXTERNAL_TESTRIG = "org/batfish/dataplane/testrigs/s2-external";
  private static final List<String> EXTERNAL_CONFIGS = ImmutableList.of("r1", "r2");
  private static final String TRACK_TESTRIG = "org/batfish/dataplane/testrigs/s2-track";
  private static final List<String> TRACK_CONFIGS = ImmutableList.of("r1", "r2");

  @Rule public TemporaryFolder _folder = new TemporaryFolder();

  @Test
  public void testOneAndThreeWorkersMatchVanilla() throws Exception {
    assertDistributedMatchesVanilla(TESTRIG, CONFIGS, new int[] {1, 3});
  }

  /**
   * Multi-hop static eBGP: regression test for the distributed phase barriers and the global
   * convergence/oscillation checks. Before those, the 3- and 6-worker runs were flaky or hung.
   */
  @Test
  public void testMultiHopLineMatchesVanilla() throws Exception {
    assertDistributedMatchesVanilla(LINE_TESTRIG, LINE_CONFIGS, new int[] {1, 3, 6});
  }

  /**
   * Regression test for the distributed IGP phase barriers: a multi-hop OSPF topology must produce
   * the same main RIBs as vanilla at 1 and 3 workers.
   */
  @Test
  public void testOspfMatchesVanilla() throws Exception {
    assertDistributedMatchesVanilla(OSPF_TESTRIG, OSPF_CONFIGS, new int[] {1, 3});
  }

  /** A network using both eBGP and OSPF must match vanilla at 1 and 3 workers. */
  @Test
  public void testOspfBgpMatchesVanilla() throws Exception {
    assertDistributedMatchesVanilla(OSPF_BGP_TESTRIG, OSPF_BGP_CONFIGS, new int[] {1, 3});
  }

  /**
   * Control-plane prefix sharding (S2): running the EGP fixpoint once per prefix shard, with each
   * shard's BGP routes externalized in between, must match vanilla at 1 and 3 workers.
   */
  @Test
  public void testPrefixShardingMatchesVanilla() throws Exception {
    System.setProperty("s2.prefixShards", "2");
    System.setProperty("s2.prefixShardExternalize", "true");
    try {
      assertDistributedMatchesVanilla(OSPF_BGP_TESTRIG, OSPF_BGP_CONFIGS, new int[] {1, 3});
    } finally {
      System.clearProperty("s2.prefixShards");
      System.clearProperty("s2.prefixShardExternalize");
    }
  }

  /**
   * Prefix sharding with a BGP aggregate: the aggregate and the prefixes it covers must stay in the
   * same shard, or the aggregate route disappears. Regression test for the prefix-closure bug
   * (`PARTITIONING-PLAN.md` section 4.5); naive round-robin with 3 shards splits the aggregate from
   * one of its more-specifics.
   */
  @Test
  public void testPrefixShardingWithAggregateMatchesVanilla() throws Exception {
    System.setProperty("s2.prefixShards", "3");
    System.setProperty("s2.prefixShardExternalize", "true");
    try {
      assertDistributedMatchesVanilla(AGG_TESTRIG, AGG_CONFIGS, new int[] {1, 3});
    } finally {
      System.clearProperty("s2.prefixShards");
      System.clearProperty("s2.prefixShardExternalize");
    }
  }

  /**
   * Prefix sharding with a static route redistributed into BGP: the static network is not a
   * connected address or a network statement, so it must be added to the sharding universe
   * explicitly or the redistributed route is lost.
   */
  @Test
  public void testPrefixShardingWithRedistributedStaticMatchesVanilla() throws Exception {
    System.setProperty("s2.prefixShards", "3");
    System.setProperty("s2.prefixShardExternalize", "true");
    try {
      assertDistributedMatchesVanilla(STATIC_TESTRIG, STATIC_CONFIGS, new int[] {1, 3});
    } finally {
      System.clearProperty("s2.prefixShards");
      System.clearProperty("s2.prefixShardExternalize");
    }
  }

  /**
   * Prefix sharding with external BGP announcements: their networks must be in the sharding
   * universe and they must be re-staged every round, or the announced route is lost. The in-process
   * engine receives them via {@code adverts}, as S2Main ships them to workers.
   */
  @Test
  public void testPrefixShardingWithExternalAnnouncementMatchesVanilla() throws Exception {
    System.setProperty("s2.prefixShards", "3");
    System.setProperty("s2.prefixShardExternalize", "true");
    try {
      assertDistributedMatchesVanilla(EXTERNAL_TESTRIG, EXTERNAL_CONFIGS, new int[] {1, 3}, true);
    } finally {
      System.clearProperty("s2.prefixShards");
      System.clearProperty("s2.prefixShardExternalize");
    }
  }

  /**
   * Auto shard-count selection ({@code S2_PREFIX_SHARDS=auto}): N is derived deterministically from
   * the prefix dependency graph. A tight budget forces more than one shard so the sharding path
   * (not just the unsharded one) is exercised, and the result must still match vanilla.
   */
  @Test
  public void testPrefixShardingAutoMatchesVanilla() throws Exception {
    System.setProperty("s2.prefixShards", "auto");
    System.setProperty("s2.prefixShardBudgetMiB", "2");
    System.setProperty("s2.prefixShardExternalize", "true");
    try {
      assertDistributedMatchesVanilla(OSPF_BGP_TESTRIG, OSPF_BGP_CONFIGS, new int[] {1, 3});
    } finally {
      System.clearProperty("s2.prefixShards");
      System.clearProperty("s2.prefixShardBudgetMiB");
      System.clearProperty("s2.prefixShardExternalize");
    }
  }

  /**
   * Auto selection with an aggregate: even when a tight budget shards the network, the aggregate
   * and the prefixes it covers must stay in one shard.
   */
  @Test
  public void testPrefixShardingAutoWithAggregateMatchesVanilla() throws Exception {
    System.setProperty("s2.prefixShards", "auto");
    System.setProperty("s2.prefixShardBudgetMiB", "3");
    System.setProperty("s2.prefixShardExternalize", "true");
    try {
      assertDistributedMatchesVanilla(AGG_TESTRIG, AGG_CONFIGS, new int[] {1, 3});
    } finally {
      System.clearProperty("s2.prefixShards");
      System.clearProperty("s2.prefixShardBudgetMiB");
      System.clearProperty("s2.prefixShardExternalize");
    }
  }

  /**
   * OSPF->BGP redistribution (on r2) and BGP->OSPF redistribution (on r3) must propagate correctly
   * across workers: r4 learns r1's loopback as an OSPF external route.
   */
  @Test
  public void testRedistributionMatchesVanilla() throws Exception {
    assertDistributedMatchesVanilla(REDIST_TESTRIG, REDIST_CONFIGS, new int[] {1, 3});
  }

  /**
   * Owned-only dataplane ({@code -Ds2.ownedDataplane}): a worker builds full RIBs/FIBs only for
   * owned nodes. On a network without tracks, VXLAN, IPsec, or tunnels it must still match vanilla.
   */
  @Test
  public void testOwnedDataplaneMatchesVanilla() throws Exception {
    assertDistributedOwnedMatchesVanilla(OSPF_BGP_TESTRIG, OSPF_BGP_CONFIGS, new int[] {1, 3});
  }

  /**
   * Owned-only dataplane hardening: a {@code TrackReachability} on a host that is a shadow on some
   * worker cannot be evaluated against that host's stub FIB. Owned mode must fall back to a full
   * dataplane for the whole run (so the tracked static route is computed correctly) instead of
   * silently deactivating it. Asserts the result still matches vanilla at 1 and 3 workers.
   */
  @Test
  public void testOwnedDataplaneWithTrackReachabilityMatchesVanilla() throws Exception {
    assertDistributedOwnedMatchesVanilla(TRACK_TESTRIG, TRACK_CONFIGS, new int[] {1, 3});
  }

  /** Owned-only dataplane must also match vanilla on a network with a BGP aggregate. */
  @Test
  public void testOwnedDataplaneWithAggregateMatchesVanilla() throws Exception {
    assertDistributedOwnedMatchesVanilla(AGG_TESTRIG, AGG_CONFIGS, new int[] {1, 3});
  }

  /**
   * Owned-only forwarding exactness on the in-process engine: each owned node's BGP routes, FIB
   * keys, and forwarding analysis (VRF forwarding behavior and ARP replies) must match vanilla at 1
   * and 3 workers.
   *
   * <p>Note: this harness's shadows delegate to the owner's real node, so remote FIBs are not stubs
   * and the stub-FIB gap is invisible here. {@link S2RemoteSidecarTest} runs the same check against
   * real (non-delegating) remote shadows, which is where the residual remote-ARP-reply dependency
   * actually appears.
   */
  @Test
  public void testOwnedDataplaneForwardingMatchesVanilla() throws Exception {
    assertDistributedOwnedForwardingMatchesVanilla(TESTRIG, CONFIGS, new int[] {1, 3});
    assertDistributedOwnedForwardingMatchesVanilla(
        OSPF_BGP_TESTRIG, OSPF_BGP_CONFIGS, new int[] {1, 3});
  }

  /** Sanity-check that the redistribution snapshot actually exercises both directions. */
  @Test
  public void testRedistributionProducesRoutes() throws Exception {
    Batfish batfish =
        BatfishTestUtils.getBatfishFromTestrigText(
            TestrigText.builder().setConfigurationFiles(REDIST_TESTRIG, REDIST_CONFIGS).build(),
            _folder);
    NetworkSnapshot snapshot = batfish.getSnapshot();
    batfish.computeDataPlane(snapshot);
    DataPlane dp = batfish.loadDataPlane(snapshot);
    Prefix r1Loopback = Prefix.parse("1.1.1.1/32");
    // r2 redistributes OSPF into BGP: r3 (eBGP) learns r1's loopback as a BGP route.
    boolean r3Bgp =
        dp.getRibs().get("r3", Configuration.DEFAULT_VRF_NAME).getRoutes().stream()
            .anyMatch(
                r -> r.getNetwork().equals(r1Loopback) && r.getProtocol() == RoutingProtocol.BGP);
    // r3 redistributes BGP into OSPF: r4 (OSPF only) learns it as an OSPF external route.
    boolean r4OspfExternal =
        dp.getRibs().get("r4", Configuration.DEFAULT_VRF_NAME).getRoutes().stream()
            .anyMatch(
                r ->
                    r.getNetwork().equals(r1Loopback)
                        && (r.getProtocol() == RoutingProtocol.OSPF_E2
                            || r.getProtocol() == RoutingProtocol.OSPF_E1));
    assertThat("r3 should learn 1.1.1.1/32 via OSPF->BGP redistribution", r3Bgp, is(true));
    assertThat("r4 should learn 1.1.1.1/32 via BGP->OSPF redistribution", r4OspfExternal, is(true));
  }

  private void assertDistributedMatchesVanilla(
      String testrig, List<String> testrigConfigs, int[] workerCounts) throws Exception {
    assertDistributedMatchesVanilla(testrig, testrigConfigs, workerCounts, false);
  }

  /**
   * Run the distributed workers with {@code -Ds2.ownedDataplane=true}. The S2 engine reads that
   * property when it is constructed, so set it only around the distributed runs (the vanilla
   * computation is unaffected either way).
   */
  private void assertDistributedOwnedMatchesVanilla(
      String testrig, List<String> testrigConfigs, int[] workerCounts) throws Exception {
    System.setProperty("s2.ownedDataplane", "true");
    try {
      assertDistributedMatchesVanilla(testrig, testrigConfigs, workerCounts, false);
    } finally {
      System.clearProperty("s2.ownedDataplane");
    }
  }

  /**
   * As {@link #assertDistributedOwnedMatchesVanilla} but also asserts that the owned nodes' BGP
   * routes, FIB keys, and forwarding analysis (VRF forwarding behavior and ARP replies) match
   * vanilla. This is the forwarding-exactness check for owned-only mode: its unowned-ARP-IP set is
   * recovered from the owned FIBs and unioned across workers rather than read off stub remote FIBs.
   */
  private void assertDistributedOwnedForwardingMatchesVanilla(
      String testrig, List<String> testrigConfigs, int[] workerCounts) throws Exception {
    System.setProperty("s2.ownedDataplane", "true");
    try {
      assertDistributedMatchesVanilla(testrig, testrigConfigs, workerCounts, false, true);
    } finally {
      System.clearProperty("s2.ownedDataplane");
    }
  }

  private void assertDistributedMatchesVanilla(
      String testrig, List<String> testrigConfigs, int[] workerCounts, boolean withAnnouncements)
      throws Exception {
    assertDistributedMatchesVanilla(
        testrig, testrigConfigs, workerCounts, withAnnouncements, false);
  }

  private void assertDistributedMatchesVanilla(
      String testrig,
      List<String> testrigConfigs,
      int[] workerCounts,
      boolean withAnnouncements,
      boolean checkForwarding)
      throws Exception {
    TestrigText.Builder testrigText =
        TestrigText.builder().setConfigurationFiles(testrig, testrigConfigs);
    if (withAnnouncements) {
      testrigText.setExternalBgpAnnouncements(testrig);
    }
    Batfish batfish = BatfishTestUtils.getBatfishFromTestrigText(testrigText.build(), _folder);
    NetworkSnapshot snapshot = batfish.getSnapshot();
    batfish.computeDataPlane(snapshot);
    DataPlane vanilla = batfish.loadDataPlane(snapshot);

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
    IpOwners ipOwners = tp.getInitialIpOwners(snapshot);
    IncrementalDataPlaneSettings settings =
        new IncrementalDataPlaneSettings(batfish.getSettingsConfiguration());

    for (int workers : workerCounts) {
      DistributedRun run = runDistributed(configs, adverts, tc, ipOwners, settings, workers);
      String context = "workers=" + workers;
      assertRibsEqual(vanilla, mergeRibs(run), context);
      if (checkForwarding) {
        assertOwnedBgpAndFibKeysEqual(vanilla, run, context);
        assertOwnedForwardingEqual(vanilla, run, context);
      }
    }
  }

  private static void assertOwnedBgpAndFibKeysEqual(
      DataPlane vanilla, DistributedRun run, String context) {
    Table<String, String, Set<Bgpv4Route>> mergedBgpRoutes = HashBasedTable.create();
    for (DataPlane dp : run.dataPlanes) {
      dp.getBgpRoutes()
          .cellSet()
          .forEach(
              cell -> mergedBgpRoutes.put(cell.getRowKey(), cell.getColumnKey(), cell.getValue()));
    }
    assertThat(
        String.format("%s: BGP routes differ", context),
        mergedBgpRoutes,
        equalTo(vanilla.getBgpRoutes()));
    for (DataPlane dp : run.dataPlanes) {
      assertThat(
          String.format("%s: FIB keys differ", context),
          dp.getFibs().keySet(),
          equalTo(vanilla.getFibs().keySet()));
    }
  }

  /**
   * Each host's forwarding analysis must come from the worker that owns it: other workers only
   * compute a stub FIB for it, so their view of it is not authoritative (and is allowed to differ).
   */
  private static void assertOwnedForwardingEqual(
      DataPlane vanilla, DistributedRun run, String context) {
    Map<String, Map<String, VrfForwardingBehavior>> vanillaVrfBehavior =
        vanilla.getForwardingAnalysis().getVrfForwardingBehavior();
    Map<String, Map<String, IpSpace>> vanillaArpReplies =
        vanilla.getForwardingAnalysis().getArpReplies();
    for (String host : vanillaVrfBehavior.keySet()) {
      DataPlane owner = run.dataPlanes.get(run.assignment.get(host));
      assertThat(
          String.format("%s: VRF forwarding behavior differs for %s", context, host),
          owner.getForwardingAnalysis().getVrfForwardingBehavior().get(host),
          equalTo(vanillaVrfBehavior.get(host)));
      assertThat(
          String.format("%s: ARP replies differ for %s", context, host),
          owner.getForwardingAnalysis().getArpReplies().get(host),
          equalTo(vanillaArpReplies.get(host)));
    }
  }

  // The worker pool is shut down in the finally block below; PMD's CloseResource only recognizes
  // close()/try-with-resources, so suppress it here.
  @SuppressWarnings("PMD.CloseResource")
  private static DistributedRun runDistributed(
      SortedMap<String, Configuration> configs,
      Set<BgpAdvertisement> adverts,
      TopologyContext tc,
      IpOwners ipOwners,
      IncrementalDataPlaneSettings settings,
      int workers)
      throws Exception {
    Map<String, Integer> assignment = NetworkPartitioner.partition(configs.keySet(), workers, 0L);

    // One real node per switch (owned by exactly one worker).
    Map<String, DistributedNode> realByHost = new HashMap<>();
    for (String host : configs.keySet()) {
      realByHost.put(host, DistributedNode.real(configs.get(host)));
    }
    S2Cluster cluster = new S2Cluster(workers);

    // External announcements are subject to prefix appointment, so include their networks in the
    // sharding universe (mirrors S2Main).
    Set<Prefix> externalAdvertPrefixes = new java.util.HashSet<>();
    for (BgpAdvertisement advert : adverts) {
      externalAdvertPrefixes.add(advert.getNetwork());
    }

    // Each worker gets a node for every switch: real if owned, shadow otherwise.
    List<S2BdpEngine> engines = new ArrayList<>();
    for (int w = 0; w < workers; w++) {
      Map<String, DistributedNode> nodes = new HashMap<>();
      for (String host : configs.keySet()) {
        if (assignment.get(host) == w) {
          nodes.put(host, realByHost.get(host));
        } else {
          nodes.put(host, DistributedNode.shadowOf(realByHost.get(host)));
        }
      }
      engines.add(new S2BdpEngine(settings, nodes, cluster, null, externalAdvertPrefixes));
    }

    // Run the workers concurrently. Each engine mutates only its own real nodes; shadow lookups
    // read the owning worker's real processes, mirroring Batfish's own parallel VR iteration.
    ExecutorService pool = Executors.newFixedThreadPool(workers);
    try {
      List<Future<ComputeDataPlaneResult>> futures = new ArrayList<>();
      for (S2BdpEngine engine : engines) {
        futures.add(
            pool.submit(() -> engine.computeDataPlane(configs, tc, adverts, ipOwners, false)));
      }
      List<DataPlane> dataPlanes = new ArrayList<>();
      for (Future<ComputeDataPlaneResult> future : futures) {
        dataPlanes.add(future.get()._dataPlane);
      }
      return new DistributedRun(assignment, dataPlanes);
    } finally {
      pool.shutdownNow();
    }
  }

  private static Table<String, String, FinalMainRib> mergeRibs(DistributedRun run) {
    Table<String, String, FinalMainRib> merged = HashBasedTable.create();
    for (DataPlane dp : run.dataPlanes) {
      dp.getRibs()
          .cellSet()
          .forEach(cell -> merged.put(cell.getRowKey(), cell.getColumnKey(), cell.getValue()));
    }
    return merged;
  }

  /** One distributed run's per-worker dataplanes and its node-to-worker assignment. */
  static final class DistributedRun {
    final Map<String, Integer> assignment;
    final List<DataPlane> dataPlanes;

    DistributedRun(Map<String, Integer> assignment, List<DataPlane> dataPlanes) {
      this.assignment = assignment;
      this.dataPlanes = dataPlanes;
    }
  }

  private static void assertRibsEqual(
      DataPlane vanilla, Table<String, String, FinalMainRib> distributed, String context) {
    for (Table.Cell<String, String, FinalMainRib> cell : vanilla.getRibs().cellSet()) {
      String host = cell.getRowKey();
      String vrf = cell.getColumnKey();
      FinalMainRib actual = distributed.get(host, vrf);
      assertThat(
          String.format("%s: RIB missing for %s/%s", context, host, vrf), actual, notNullValue());
      assertThat(
          String.format("%s: routes differ for %s/%s", context, host, vrf),
          actual.getRoutes(),
          equalTo(cell.getValue().getRoutes()));
    }
  }
}
