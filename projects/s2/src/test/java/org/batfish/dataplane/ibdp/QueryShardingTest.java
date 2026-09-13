package org.batfish.dataplane.ibdp;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CyclicBarrier;
import net.sf.javabdd.BDD;
import net.sf.javabdd.JFactory;
import org.batfish.bddreachability.BDDReachabilityAnalysis;
import org.batfish.bddreachability.BDDReachabilityAnalysisFactory;
import org.batfish.bddreachability.IpsRoutedOutInterfacesFactory;
import org.batfish.common.NetworkSnapshot;
import org.batfish.common.bdd.BDDPacket;
import org.batfish.datamodel.Configuration;
import org.batfish.datamodel.DataPlane;
import org.batfish.datamodel.Interface;
import org.batfish.datamodel.IpSpace;
import org.batfish.datamodel.UniverseIpSpace;
import org.batfish.main.Batfish;
import org.batfish.main.BatfishTestUtils;
import org.batfish.main.TestrigText;
import org.batfish.specifier.InterfaceLocation;
import org.batfish.specifier.IpSpaceAssignment;
import org.batfish.symbolic.state.Query;
import org.batfish.symbolic.state.StateExpr;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

/**
 * Verifies S2 prefix sharding on the symbolic side: running the backward fixpoint once per
 * destination-prefix shard and unioning the per-state BDDs equals running it once over the full
 * query space.
 */
public class QueryShardingTest {

  private static final String TESTRIG = "org/batfish/dataplane/testrigs/s2-triangle";

  @Rule public TemporaryFolder _folder = new TemporaryFolder();

  @Test
  public void testShardUnionMatchesFull() throws Exception {
    Batfish batfish =
        BatfishTestUtils.getBatfishFromTestrigText(
            TestrigText.builder().setConfigurationFiles(TESTRIG, "r1", "r2", "r3").build(),
            _folder);
    NetworkSnapshot snapshot = batfish.getSnapshot();
    batfish.computeDataPlane(snapshot);
    DataPlane dp = batfish.loadDataPlane(snapshot);
    Map<String, Configuration> configs = batfish.loadConfigurations(snapshot);

    IpSpace querySpace = PrefixSharder.querySpace(PrefixSharder.queryPrefixes(configs));
    BDDReachabilityAnalysis analysis = buildAnalysis(configs, dp, querySpace);
    JFactory factory = (JFactory) analysis.getBDDPacket().getFactory();

    Map<String, Integer> partition = new HashMap<>();
    for (String host : configs.keySet()) {
      partition.put(host, 0);
    }
    S2ReachabilityWorker worker =
        new S2ReachabilityWorker(
            0, partition, analysis, new S2BddSidecar.Client[] {null}, new BarrierCoordinator());
    Map<StateExpr, BDD> full = worker.run();

    // Run the same fixpoint once per shard and union.
    List<IpSpace> shards = PrefixSharder.shard(PrefixSharder.queryPrefixes(configs), 3);
    assertThat("expected more than one shard", shards.size() > 1, is(true));
    Map<StateExpr, BDD> union = new HashMap<>();
    for (IpSpace shard : shards) {
      BDD root =
          analysis
              .getQueryHeaderSpaceBdd()
              .and(analysis.getBDDPacket().getDstIpSpaceToBDD().visit(shard));
      for (Map.Entry<StateExpr, BDD> e : worker.run(root).entrySet()) {
        union.merge(e.getKey(), e.getValue(), (a, b) -> a.or(b));
      }
    }

    assertThat(
        "shard union state set differs from full", full.keySet().equals(union.keySet()), is(true));
    for (Map.Entry<StateExpr, BDD> e : full.entrySet()) {
      BDD actual = union.get(e.getKey());
      assertThat(
          "state " + e.getKey(), actual != null && actual.biimp(e.getValue()).isOne(), is(true));
    }
    // Query root sanity: the union covers the same query space.
    assertThat(union.containsKey(Query.INSTANCE), is(true));
  }

  private static BDDReachabilityAnalysis buildAnalysis(
      Map<String, Configuration> configs, DataPlane dp, IpSpace querySpace) {
    BDDPacket packet = new BDDPacket();
    BDDReachabilityAnalysisFactory factory =
        new BDDReachabilityAnalysisFactory(
            packet,
            configs,
            dp.getForwardingAnalysis(),
            new IpsRoutedOutInterfacesFactory(dp.getFibs()),
            false,
            false);
    IpSpaceAssignment.Builder builder = IpSpaceAssignment.builder();
    for (Configuration c : configs.values()) {
      for (Interface i : c.getAllInterfaces().values()) {
        if (i.getActive()) {
          builder.assign(
              new InterfaceLocation(c.getHostname(), i.getName()), UniverseIpSpace.INSTANCE);
        }
      }
    }
    return factory.bddReachabilityAnalysis(
        builder.build(),
        org.batfish.datamodel.acl.AclLineMatchExprs.matchDst(querySpace),
        new HashSet<>(),
        new HashSet<>(),
        configs.keySet(),
        java.util.Set.of(org.batfish.datamodel.FlowDisposition.ACCEPTED));
  }

  /** Single-worker {@link S2Coordinator}: barrier over one party. */
  private static final class BarrierCoordinator implements S2Coordinator {
    private final CyclicBarrier _barrier = new CyclicBarrier(1);

    @Override
    public boolean roundCheck(boolean localDirty) {
      try {
        _barrier.await();
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
      return localDirty;
    }

    @Override
    public int sumAll(int localValue) {
      return localValue;
    }
  }
}
