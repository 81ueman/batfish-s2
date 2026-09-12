package org.batfish.dataplane.ibdp;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicBoolean;
import net.sf.javabdd.BDD;
import net.sf.javabdd.BDDTransfer;
import net.sf.javabdd.JFactory;
import org.batfish.bddreachability.BDDReachabilityAnalysis;
import org.batfish.bddreachability.BDDReachabilityAnalysisFactory;
import org.batfish.bddreachability.IpsRoutedOutInterfacesFactory;
import org.batfish.bddreachability.transition.TransitionTransfer;
import org.batfish.common.NetworkSnapshot;
import org.batfish.common.bdd.BDDPacket;
import org.batfish.datamodel.Configuration;
import org.batfish.datamodel.DataPlane;
import org.batfish.datamodel.ForwardingAnalysis;
import org.batfish.datamodel.Interface;
import org.batfish.datamodel.UniverseIpSpace;
import org.batfish.main.Batfish;
import org.batfish.main.BatfishTestUtils;
import org.batfish.main.TestrigText;
import org.batfish.specifier.InterfaceLocation;
import org.batfish.specifier.IpSpaceAssignment;
import org.batfish.symbolic.state.StateExpr;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

/**
 * Milestone 5 scale-out: each worker generates only the reachability edges whose source it owns
 * ({@link OwnedForwardingAnalysis}) and pulls the boundary edges into its own states from the peers
 * that own their sources ({@link S2SidecarHandlers#forBoundaryEdges}). The backward fixpoint over
 * this partial graph must equal the fixpoint over the full graph, for 1 and 3 workers.
 */
public class ScaledReachabilityTest {

  private static final String TESTRIG = "org/batfish/dataplane/testrigs/s2-triangle";

  @Rule public TemporaryFolder _folder = new TemporaryFolder();

  @Test
  public void testScaledMatchesFull() throws Exception {
    Batfish batfish =
        BatfishTestUtils.getBatfishFromTestrigText(
            TestrigText.builder().setConfigurationFiles(TESTRIG, "r1", "r2", "r3").build(),
            _folder);
    NetworkSnapshot snapshot = batfish.getSnapshot();
    batfish.computeDataPlane(snapshot);
    DataPlane dp = batfish.loadDataPlane(snapshot);
    Map<String, Configuration> configs = batfish.loadConfigurations(snapshot);

    for (int workers : new int[] {1, 3}) {
      JFactory referenceFactory = (JFactory) new BDDPacket().getFactory();
      Map<StateExpr, BDD> full = runDistributed(configs, dp, workers, false, referenceFactory);
      Map<StateExpr, BDD> scaled = runDistributed(configs, dp, workers, true, referenceFactory);
      assertThat(
          String.format("workers=%d: state sets differ", workers),
          full.keySet().equals(scaled.keySet()),
          is(true));
      for (Map.Entry<StateExpr, BDD> entry : full.entrySet()) {
        BDD actual = scaled.get(entry.getKey());
        assertThat(
            String.format("workers=%d: state %s differs", workers, entry.getKey()),
            actual != null && actual.biimp(entry.getValue()).isOne(),
            is(true));
      }
    }
  }

  private static Map<StateExpr, BDD> runDistributed(
      Map<String, Configuration> configs,
      DataPlane dp,
      int workers,
      boolean scaled,
      JFactory referenceFactory)
      throws Exception {
    Map<String, Integer> partition =
        NetworkPartitioner.partition(new HashSet<>(configs.keySet()), workers, 0L);
    List<Set<String>> ownedHosts = new ArrayList<>();
    for (int w = 0; w < workers; w++) {
      int worker = w;
      Set<String> hosts = new HashSet<>();
      partition.forEach(
          (host, owner) -> {
            if (owner == worker) {
              hosts.add(host);
            }
          });
      ownedHosts.add(hosts);
    }

    // Analyses are built up front. The scaled variant restricts each worker to the switches it
    // owns (local source edges only); boundary edges into its states are pulled below.
    List<BDDReachabilityAnalysis> analyses = new ArrayList<>();
    for (int w = 0; w < workers; w++) {
      analyses.add(
          buildAnalysis(configs, dp, assignment(configs), scaled ? ownedHosts.get(w) : null));
    }

    List<S2ReachabilityWorker[]> holders = new ArrayList<>();
    List<S2BddSidecar> servers = new ArrayList<>();
    List<S2WorkerEndpoint> endpoints = new ArrayList<>();
    for (int w = 0; w < workers; w++) {
      S2ReachabilityWorker[] holder = new S2ReachabilityWorker[1];
      holders.add(holder);
      S2BddSidecar server =
          new S2BddSidecar(0, (state, payload) -> holder[0].receive(state, payload));
      server.start();
      servers.add(server);
      endpoints.add(new S2WorkerEndpoint("127.0.0.1", server.getPort()));
    }

    BarrierCoordinator coordinator = new BarrierCoordinator(workers);
    List<S2ReachabilityWorker> workerList = new ArrayList<>();
    for (int w = 0; w < workers; w++) {
      S2BddSidecar.Client[] clients = new S2BddSidecar.Client[workers];
      for (int t = 0; t < workers; t++) {
        clients[t] = new S2BddSidecar.Client(endpoints.get(t));
      }
      S2ReachabilityWorker worker =
          new S2ReachabilityWorker(w, partition, analyses.get(w), clients, coordinator);
      holders.get(w)[0] = worker;
      workerList.add(worker);
    }

    if (scaled) {
      // Pull boundary edges directly from the owning workers' boundary handler (the same code the
      // sidecar serves over the wire), reconstructing transitions in each requester's factory.
      for (int w = 0; w < workers; w++) {
        JFactory factory = (JFactory) analyses.get(w).getBDDPacket().getFactory();
        for (int p = 0; p < workers; p++) {
          if (p == w) {
            continue;
          }
          final int peer = p;
          S2Messages.BoundaryEdgesResponse response =
              (S2Messages.BoundaryEdgesResponse)
                  S2SidecarHandlers.forBoundaryEdges(ownedHosts.get(peer), () -> analyses.get(peer))
                      .handle(new S2Messages.BoundaryEdgesRequest(ownedHosts.get(w)));
          for (S2Messages.SerializedEdge edge : response.edges) {
            workerList
                .get(w)
                .addEdge(
                    edge.preState,
                    edge.postState,
                    TransitionTransfer.load(factory, edge.transition));
          }
        }
      }
    }

    ExecutorService pool = Executors.newFixedThreadPool(workers);
    try {
      List<Future<Map<StateExpr, BDD>>> futures = new ArrayList<>();
      for (S2ReachabilityWorker worker : workerList) {
        futures.add(
            pool.submit(
                () -> {
                  try {
                    return worker.run();
                  } catch (Throwable t) {
                    t.printStackTrace();
                    throw t;
                  }
                }));
      }
      Map<StateExpr, BDD> result = new HashMap<>();
      for (Future<Map<StateExpr, BDD>> future : futures) {
        for (Map.Entry<StateExpr, BDD> entry : future.get().entrySet()) {
          String payload = new BDDTransfer().save(entry.getValue());
          result.put(entry.getKey(), new BDDTransfer().load(referenceFactory, payload));
        }
      }
      return result;
    } finally {
      pool.shutdownNow();
      servers.forEach(S2BddSidecar::close);
    }
  }

  private static IpSpaceAssignment assignment(Map<String, Configuration> configs) {
    IpSpaceAssignment.Builder builder = IpSpaceAssignment.builder();
    for (Configuration c : configs.values()) {
      for (Interface i : c.getAllInterfaces().values()) {
        if (i.getActive()) {
          builder.assign(
              new InterfaceLocation(c.getHostname(), i.getName()), UniverseIpSpace.INSTANCE);
        }
      }
    }
    return builder.build();
  }

  private static BDDReachabilityAnalysis buildAnalysis(
      Map<String, Configuration> configs,
      DataPlane dp,
      IpSpaceAssignment assignment,
      Set<String> ownedHosts) {
    BDDPacket packet = new BDDPacket();
    ForwardingAnalysis forwardingAnalysis =
        ownedHosts == null
            ? dp.getForwardingAnalysis()
            : new OwnedForwardingAnalysis(dp.getForwardingAnalysis(), ownedHosts);
    BDDReachabilityAnalysisFactory factory =
        new BDDReachabilityAnalysisFactory(
            packet,
            configs,
            forwardingAnalysis,
            new IpsRoutedOutInterfacesFactory(dp.getFibs()),
            false,
            false);
    return factory.bddReachabilityAnalysis(assignment);
  }

  /**
   * In-process {@link S2Coordinator}: global OR of the round's dirty flags, barrier-synchronized.
   */
  private static final class BarrierCoordinator implements S2Coordinator {
    private final CyclicBarrier _barrier;
    private final AtomicBoolean _anyDirty = new AtomicBoolean();
    private volatile boolean _result;

    BarrierCoordinator(int workers) {
      _barrier =
          new CyclicBarrier(
              workers,
              () -> {
                _result = _anyDirty.get();
                _anyDirty.set(false);
              });
    }

    @Override
    public boolean roundCheck(boolean localDirty) {
      if (localDirty) {
        _anyDirty.set(true);
      }
      try {
        _barrier.await(60, java.util.concurrent.TimeUnit.SECONDS);
      } catch (Exception e) {
        throw new RuntimeException("S2 worker synchronization failed", e);
      }
      return _result;
    }
  }
}
