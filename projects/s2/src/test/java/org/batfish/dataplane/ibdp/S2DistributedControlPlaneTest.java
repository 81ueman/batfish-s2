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

  @Rule public TemporaryFolder _folder = new TemporaryFolder();

  @Test
  public void testOneAndThreeWorkersMatchVanilla() throws Exception {
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
    IncrementalDataPlaneSettings settings =
        new IncrementalDataPlaneSettings(batfish.getSettingsConfiguration());

    for (int workers : new int[] {1, 3}) {
      Table<String, String, FinalMainRib> distributed =
          runDistributed(configs, adverts, tc, ipOwners, settings, workers);
      assertRibsEqual(vanilla, distributed, "workers=" + workers);
    }
  }

  private static Table<String, String, FinalMainRib> runDistributed(
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
    List<VirtualRouter> allRealVrs =
        realByHost.values().stream().flatMap(n -> n.getVirtualRouters().stream()).toList();
    S2Cluster cluster = new S2Cluster(allRealVrs, workers);

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
      engines.add(new S2BdpEngine(settings, nodes, cluster, null));
    }

    // Run the workers concurrently. Each engine mutates only its own real nodes; shadow lookups
    // read the owning worker's real processes, mirroring Batfish's own parallel VR iteration.
    ExecutorService pool = Executors.newFixedThreadPool(workers);
    try {
      List<Future<ComputeDataPlaneResult>> futures = new ArrayList<>();
      for (S2BdpEngine engine : engines) {
        futures.add(
            pool.submit(
                () -> engine.computeDataPlane(configs, tc, adverts, ipOwners, false)));
      }
      Table<String, String, FinalMainRib> merged = HashBasedTable.create();
      for (Future<ComputeDataPlaneResult> future : futures) {
        DataPlane dp = future.get()._dataPlane;
        dp.getRibs()
            .cellSet()
            .forEach(
                cell -> merged.put(cell.getRowKey(), cell.getColumnKey(), cell.getValue()));
      }
      return merged;
    } finally {
      pool.shutdownNow();
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
