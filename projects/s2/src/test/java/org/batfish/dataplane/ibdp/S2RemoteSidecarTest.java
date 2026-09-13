// SPDX-License-Identifier: Apache-2.0

package org.batfish.dataplane.ibdp;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
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
import org.batfish.datamodel.NetworkConfigurations;
import org.batfish.datamodel.VrfForwardingBehavior;
import org.batfish.datamodel.bgp.BgpTopology;
import org.batfish.datamodel.isis.IsisTopology;
import org.batfish.main.Batfish;
import org.batfish.main.BatfishTestUtils;
import org.batfish.main.TestrigText;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

/**
 * Milestone 2: same as {@link S2DistributedControlPlaneTest}, but remote route lookups go through
 * the sidecar over a real socket with Java serialization instead of sharing objects in the JVM.
 */
public class S2RemoteSidecarTest {

  private static final String TESTRIG = "org/batfish/dataplane/testrigs/s2-triangle";
  private static final List<String> CONFIGS = ImmutableList.of("r1", "r2", "r3");
  private static final String OSPF_TESTRIG = "org/batfish/dataplane/testrigs/s2-ospf";
  private static final List<String> OSPF_CONFIGS = ImmutableList.of("r1", "r2", "r3", "r4");
  private static final String OSPF_BGP_TESTRIG = "org/batfish/dataplane/testrigs/s2-ospf-bgp";
  private static final List<String> OSPF_BGP_CONFIGS = ImmutableList.of("r1", "r2", "r3");
  private static final String REDIST_TESTRIG = "org/batfish/dataplane/testrigs/s2-redist";
  private static final List<String> REDIST_CONFIGS = ImmutableList.of("r1", "r2", "r3", "r4");

  @Rule public TemporaryFolder _folder = new TemporaryFolder();

  @Test
  public void testRemoteSidecarOneAndThreeWorkers() throws Exception {
    runAndAssert(TESTRIG, CONFIGS);
  }

  /** Exercises the sidecar-backed OSPF message forwarding (shadow enqueue -> owner). */
  @Test
  public void testRemoteOspfSidecar() throws Exception {
    runAndAssert(OSPF_TESTRIG, OSPF_CONFIGS);
  }

  /** A network using both eBGP and OSPF must match vanilla across the distributed workers. */
  @Test
  public void testRemoteOspfBgpSidecar() throws Exception {
    runAndAssert(OSPF_BGP_TESTRIG, OSPF_BGP_CONFIGS);
  }

  /** OSPF<->BGP redistribution must propagate correctly over the sidecar. */
  @Test
  public void testRemoteRedistributionSidecar() throws Exception {
    runAndAssert(REDIST_TESTRIG, REDIST_CONFIGS);
  }

  /**
   * Owned-only forwarding exactness on real (non-delegating) remote shadows: a shadow's FIB is a
   * stub, so a worker cannot derive the network's unowned ARP IPs or the remote nodes' ARP replies
   * from its own FIBs alone. The S2 engine recovers both from the owned FIBs, unions them across
   * workers, and rebuilds its final forwarding analysis. Each owned node's BGP routes, FIB keys,
   * and forwarding analysis (VRF forwarding behavior and ARP replies) must then match vanilla at 1
   * and 3 workers.
   *
   * <p>This is the faithful harness: the in-JVM delegating-shadow harness shares the owner's real
   * node, so its remote FIBs are not stubs and it cannot reproduce the gap.
   */
  @Test
  public void testRemoteOwnedForwardingMatchesVanilla() throws Exception {
    System.setProperty("s2.ownedDataplane", "true");
    try {
      assertForwardingMatchesVanilla(TESTRIG, CONFIGS);
      assertForwardingMatchesVanilla(OSPF_BGP_TESTRIG, OSPF_BGP_CONFIGS);
    } finally {
      System.clearProperty("s2.ownedDataplane");
    }
  }

  /** The same forwarding check in full-dataplane mode (every worker pulls remote RIBs). */
  @Test
  public void testRemoteFullForwardingMatchesVanilla() throws Exception {
    System.setProperty("s2.ownedDataplane", "false");
    try {
      assertForwardingMatchesVanilla(TESTRIG, CONFIGS);
      assertForwardingMatchesVanilla(OSPF_BGP_TESTRIG, OSPF_BGP_CONFIGS);
    } finally {
      System.clearProperty("s2.ownedDataplane");
    }
  }

  private void runAndAssert(String testrig, List<String> testrigConfigs) throws Exception {
    RemoteInputs in = loadInputs(testrig, testrigConfigs);
    for (int workers : new int[] {1, 3}) {
      RemoteRun run = runRemote(in, workers);
      assertRibsEqual(in.vanilla, mergeRibs(run), "workers=" + workers);
    }
  }

  private void assertForwardingMatchesVanilla(String testrig, List<String> testrigConfigs)
      throws Exception {
    RemoteInputs in = loadInputs(testrig, testrigConfigs);
    for (int workers : new int[] {1, 3}) {
      RemoteRun run = runRemote(in, workers);
      String context = "workers=" + workers;
      assertRibsEqual(in.vanilla, mergeRibs(run), context);
      assertOwnedBgpAndFibKeysEqual(in.vanilla, run, context);
      assertOwnedForwardingEqual(in.vanilla, run, context);
    }
  }

  private RemoteInputs loadInputs(String testrig, List<String> testrigConfigs) throws Exception {
    Batfish batfish =
        BatfishTestUtils.getBatfishFromTestrigText(
            TestrigText.builder().setConfigurationFiles(testrig, testrigConfigs).build(), _folder);
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
    return new RemoteInputs(
        vanilla,
        configs,
        adverts,
        tc,
        tp.getInitialIpOwners(snapshot),
        tp.getBgpTopology(snapshot),
        NetworkConfigurations.of(configs),
        new IncrementalDataPlaneSettings(batfish.getSettingsConfiguration()));
  }

  /** Vanilla data plane plus the inputs the distributed harness ships to the workers. */
  private static final class RemoteInputs {
    final DataPlane vanilla;
    final SortedMap<String, Configuration> configs;
    final Set<BgpAdvertisement> adverts;
    final TopologyContext tc;
    final IpOwners ipOwners;
    final BgpTopology bgpTopology;
    final NetworkConfigurations nc;
    final IncrementalDataPlaneSettings settings;

    RemoteInputs(
        DataPlane vanilla,
        SortedMap<String, Configuration> configs,
        Set<BgpAdvertisement> adverts,
        TopologyContext tc,
        IpOwners ipOwners,
        BgpTopology bgpTopology,
        NetworkConfigurations nc,
        IncrementalDataPlaneSettings settings) {
      this.vanilla = vanilla;
      this.configs = configs;
      this.adverts = adverts;
      this.tc = tc;
      this.ipOwners = ipOwners;
      this.bgpTopology = bgpTopology;
      this.nc = nc;
      this.settings = settings;
    }
  }

  // The sidecar servers and the worker pool are shut down together in the finally block below;
  // PMD's CloseResource only recognizes close()/try-with-resources, so suppress it here.
  @SuppressWarnings("PMD.CloseResource")
  private static RemoteRun runRemote(RemoteInputs in, int workers) throws Exception {
    Map<String, Integer> assignment =
        NetworkPartitioner.partition(in.configs.keySet(), workers, 0L);

    // 1. Build nodes (real for owned, M2 shadow otherwise) without providers yet.
    List<Map<String, DistributedNode>> workerNodes = new ArrayList<>();
    for (int w = 0; w < workers; w++) {
      Map<String, DistributedNode> nodes = new HashMap<>();
      for (String host : in.configs.keySet()) {
        if (assignment.get(host) == w) {
          nodes.put(host, DistributedNode.real(in.configs.get(host)));
        } else {
          nodes.put(host, DistributedNode.shadow(in.configs.get(host)));
        }
      }
      workerNodes.add(nodes);
    }
    S2Cluster cluster = new S2Cluster(workers);

    // 2. Start one sidecar per worker, serving that worker's real processes.
    S2SidecarClient client = new S2SidecarClient();
    List<S2SidecarServer> servers = new ArrayList<>();
    List<S2WorkerEndpoint> endpoints = new ArrayList<>();
    for (int w = 0; w < workers; w++) {
      Map<String, Node> nodeMap = new HashMap<>(workerNodes.get(w));
      S2SidecarServer server =
          new S2SidecarServer(0, S2SidecarHandlers.forWorker(nodeMap, in.bgpTopology, in.nc));
      server.start();
      servers.add(server);
      endpoints.add(new S2WorkerEndpoint("127.0.0.1", server.getPort()));
    }

    // 3. Install sidecar-backed providers on every shadow.
    for (int w = 0; w < workers; w++) {
      for (String host : in.configs.keySet()) {
        if (assignment.get(host) != w) {
          DistributedNode shadow = workerNodes.get(w).get(host);
          S2WorkerEndpoint owner = endpoints.get(assignment.get(host));
          shadow.installRemoteBgpProviders(client, owner);
          shadow.installRemoteOspfProviders(client, owner, in.tc.getOspfTopology());
        }
      }
    }

    // 4. Run engines concurrently with global convergence.
    List<S2BdpEngine> engines = new ArrayList<>();
    for (int w = 0; w < workers; w++) {
      ShadowMainRibSync shadowSync =
          new ShadowMainRibSync(workerNodes.get(w), assignment, w, endpoints, client);
      engines.add(new S2BdpEngine(in.settings, workerNodes.get(w), cluster, shadowSync));
    }
    ExecutorService pool = Executors.newFixedThreadPool(workers);
    try {
      List<Future<ComputeDataPlaneResult>> futures = new ArrayList<>();
      for (S2BdpEngine engine : engines) {
        futures.add(
            pool.submit(
                () -> engine.computeDataPlane(in.configs, in.tc, in.adverts, in.ipOwners, false)));
      }
      List<DataPlane> dataPlanes = new ArrayList<>();
      for (Future<ComputeDataPlaneResult> future : futures) {
        dataPlanes.add(future.get()._dataPlane);
      }
      return new RemoteRun(assignment, dataPlanes);
    } finally {
      pool.shutdownNow();
      servers.forEach(S2SidecarServer::close);
    }
  }

  /** One distributed run's per-worker dataplanes and its node-to-worker assignment. */
  private static final class RemoteRun {
    final Map<String, Integer> assignment;
    final List<DataPlane> dataPlanes;

    RemoteRun(Map<String, Integer> assignment, List<DataPlane> dataPlanes) {
      this.assignment = assignment;
      this.dataPlanes = dataPlanes;
    }
  }

  private static Table<String, String, FinalMainRib> mergeRibs(RemoteRun run) {
    Table<String, String, FinalMainRib> merged = HashBasedTable.create();
    for (DataPlane dp : run.dataPlanes) {
      dp.getRibs()
          .cellSet()
          .forEach(cell -> merged.put(cell.getRowKey(), cell.getColumnKey(), cell.getValue()));
    }
    return merged;
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

  private static void assertOwnedBgpAndFibKeysEqual(
      DataPlane vanilla, RemoteRun run, String context) {
    for (Table.Cell<String, String, Set<Bgpv4Route>> cell : vanilla.getBgpRoutes().cellSet()) {
      DataPlane owner = run.dataPlanes.get(run.assignment.get(cell.getRowKey()));
      assertThat(
          String.format(
              "%s: BGP routes differ for %s/%s", context, cell.getRowKey(), cell.getColumnKey()),
          owner.getBgpRoutes().get(cell.getRowKey(), cell.getColumnKey()),
          equalTo(cell.getValue()));
    }
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
  private static void assertOwnedForwardingEqual(DataPlane vanilla, RemoteRun run, String context) {
    Map<String, Map<String, VrfForwardingBehavior>> vanillaVrfBehavior =
        vanilla.getForwardingAnalysis().getVrfForwardingBehavior();
    Map<String, Map<String, IpSpace>> vanillaArpReplies =
        vanilla.getForwardingAnalysis().getArpReplies();
    for (String host : vanillaVrfBehavior.keySet()) {
      DataPlane owner = run.dataPlanes.get(run.assignment.get(host));
      assertThat(
          String.format("%s: ARP replies differ for %s", context, host),
          owner.getForwardingAnalysis().getArpReplies().get(host),
          equalTo(vanillaArpReplies.get(host)));
      assertThat(
          String.format("%s: VRF forwarding behavior differs for %s", context, host),
          owner.getForwardingAnalysis().getVrfForwardingBehavior().get(host),
          equalTo(vanillaVrfBehavior.get(host)));
    }
  }
}
