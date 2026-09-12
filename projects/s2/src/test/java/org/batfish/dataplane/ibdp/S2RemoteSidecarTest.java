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
import org.batfish.datamodel.Configuration;
import org.batfish.datamodel.DataPlane;
import org.batfish.datamodel.FinalMainRib;
import org.batfish.datamodel.NetworkConfigurations;
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

  @Rule public TemporaryFolder _folder = new TemporaryFolder();

  @Test
  public void testRemoteSidecarOneAndThreeWorkers() throws Exception {
    Batfish batfish =
        BatfishTestUtils.getBatfishFromTestrigText(
            TestrigText.builder().setConfigurationFiles(TESTRIG, CONFIGS).build(), _folder);
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
    BgpTopology bgpTopology = tp.getBgpTopology(snapshot);
    NetworkConfigurations nc = NetworkConfigurations.of(configs);
    IncrementalDataPlaneSettings settings =
        new IncrementalDataPlaneSettings(batfish.getSettingsConfiguration());

    for (int workers : new int[] {1, 3}) {
      Table<String, String, FinalMainRib> distributed =
          runRemote(
              configs, adverts, tc, ipOwners, bgpTopology, nc, settings, workers);
      assertRibsEqual(vanilla, distributed, "workers=" + workers);
    }
  }

  private static Table<String, String, FinalMainRib> runRemote(
      SortedMap<String, Configuration> configs,
      Set<BgpAdvertisement> adverts,
      TopologyContext tc,
      IpOwners ipOwners,
      BgpTopology bgpTopology,
      NetworkConfigurations nc,
      IncrementalDataPlaneSettings settings,
      int workers)
      throws Exception {
    Map<String, Integer> assignment = NetworkPartitioner.partition(configs.keySet(), workers, 0L);

    // 1. Build nodes (real for owned, M2 shadow otherwise) without providers yet.
    List<Map<String, DistributedNode>> workerNodes = new ArrayList<>();
    List<DistributedNode> realNodes = new ArrayList<>();
    for (int w = 0; w < workers; w++) {
      Map<String, DistributedNode> nodes = new HashMap<>();
      for (String host : configs.keySet()) {
        if (assignment.get(host) == w) {
          DistributedNode node = DistributedNode.real(configs.get(host));
          realNodes.add(node);
          nodes.put(host, node);
        } else {
          nodes.put(host, DistributedNode.shadow(configs.get(host)));
        }
      }
      workerNodes.add(nodes);
    }
    List<VirtualRouter> allRealVrs =
        realNodes.stream().flatMap(n -> n.getVirtualRouters().stream()).toList();
    S2Cluster cluster = new S2Cluster(allRealVrs, workers);

    // 2. Start one sidecar per worker, serving that worker's real processes.
    S2SidecarClient client = new S2SidecarClient();
    List<S2SidecarServer> servers = new ArrayList<>();
    List<S2WorkerEndpoint> endpoints = new ArrayList<>();
    for (int w = 0; w < workers; w++) {
      Map<String, Node> nodeMap = new HashMap<>(workerNodes.get(w));
      S2SidecarServer server =
          new S2SidecarServer(0, S2SidecarHandlers.forWorker(nodeMap, bgpTopology, nc));
      server.start();
      servers.add(server);
      endpoints.add(new S2WorkerEndpoint("127.0.0.1", server.getPort()));
    }

    // 3. Install sidecar-backed providers on every shadow.
    for (int w = 0; w < workers; w++) {
      for (String host : configs.keySet()) {
        if (assignment.get(host) != w) {
          DistributedNode shadow = workerNodes.get(w).get(host);
          shadow.installRemoteBgpProviders(client, endpoints.get(assignment.get(host)));
        }
      }
    }

    // 4. Run engines concurrently with global convergence.
    List<S2BdpEngine> engines = new ArrayList<>();
    for (int w = 0; w < workers; w++) {
      ShadowMainRibSync shadowSync =
          new ShadowMainRibSync(workerNodes.get(w), assignment, w, endpoints, client);
      engines.add(new S2BdpEngine(settings, workerNodes.get(w), cluster, shadowSync));
    }
    ExecutorService pool = Executors.newFixedThreadPool(workers);
    try {
      List<Future<ComputeDataPlaneResult>> futures = new ArrayList<>();
      for (S2BdpEngine engine : engines) {
        futures.add(
            pool.submit(() -> engine.computeDataPlane(configs, tc, adverts, ipOwners, false)));
      }
      Table<String, String, FinalMainRib> merged = HashBasedTable.create();
      for (int w = 0; w < futures.size(); w++) {
        DataPlane dp = futures.get(w).get()._dataPlane;
        for (Table.Cell<String, String, FinalMainRib> cell : dp.getRibs().cellSet()) {
          // Each engine's dataplane includes shadow nodes too; only trust owned hosts.
          if (assignment.get(cell.getRowKey()) == w) {
            merged.put(cell.getRowKey(), cell.getColumnKey(), cell.getValue());
          }
        }
      }
      return merged;
    } finally {
      pool.shutdownNow();
      servers.forEach(S2SidecarServer::close);
    }
  }

  private static void assertRibsEqual(
      DataPlane vanilla, Table<String, String, FinalMainRib> distributed, String context) {
    for (Table.Cell<String, String, FinalMainRib> cell : vanilla.getRibs().cellSet()) {
      String host = cell.getRowKey();
      String vrf = cell.getColumnKey();
      FinalMainRib actual = distributed.get(host, vrf);
      assertThat(
          String.format("%s: RIB missing for %s/%s", context, host, vrf),
          actual,
          notNullValue());
      assertThat(
          String.format("%s: routes differ for %s/%s", context, host, vrf),
          actual.getRoutes(),
          equalTo(cell.getValue().getRoutes()));
    }
  }
}
