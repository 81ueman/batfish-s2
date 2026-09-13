// SPDX-License-Identifier: Apache-2.0
package org.batfish.dataplane.ibdp;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

import com.google.common.collect.Table;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Queue;
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
import org.batfish.bddreachability.transition.Transition;
import org.batfish.common.NetworkSnapshot;
import org.batfish.common.bdd.BDDPacket;
import org.batfish.datamodel.Configuration;
import org.batfish.datamodel.DataPlane;
import org.batfish.datamodel.Interface;
import org.batfish.datamodel.UniverseIpSpace;
import org.batfish.main.Batfish;
import org.batfish.main.BatfishTestUtils;
import org.batfish.main.TestrigText;
import org.batfish.specifier.InterfaceLocation;
import org.batfish.specifier.IpSpaceAssignment;
import org.batfish.symbolic.state.EdgeStateExpr;
import org.batfish.symbolic.state.InterfaceStateExpr;
import org.batfish.symbolic.state.NodeStateExpr;
import org.batfish.symbolic.state.PacketPolicyAction;
import org.batfish.symbolic.state.PacketPolicyStatement;
import org.batfish.symbolic.state.StateExpr;
import org.batfish.symbolic.state.VrfStateExpr;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

/**
 * Milestone 5: distributed symbolic reachability. Each worker runs the BDD forward fixpoint over
 * only the state expressions it owns; BDDs that cross a worker boundary are serialized and shipped
 * over {@link S2BddSidecar}. The per-state reachable BDDs must equal Batfish's local ({@code
 * computeForwardReachableStates}) result.
 */
public class DistributedReachabilityTest {

  private static final String TESTRIG = "org/batfish/dataplane/testrigs/s2-triangle";

  @Rule public TemporaryFolder _folder = new TemporaryFolder();

  @Test
  public void testOneAndThreeWorkersMatchLocal() throws Exception {
    Batfish batfish =
        BatfishTestUtils.getBatfishFromTestrigText(
            TestrigText.builder().setConfigurationFiles(TESTRIG, "r1", "r2", "r3").build(),
            _folder);
    NetworkSnapshot snapshot = batfish.getSnapshot();
    batfish.computeDataPlane(snapshot);
    DataPlane dp = batfish.loadDataPlane(snapshot);

    Map<String, Configuration> configs = batfish.loadConfigurations(snapshot);
    IpSpaceAssignment assignment = interfaceAssignment(configs);

    BDDReachabilityAnalysis reference = buildAnalysis(configs, dp, assignment);
    Map<StateExpr, BDD> local = reference.computeForwardReachableStates();
    JFactory referenceFactory = (JFactory) reference.getBDDPacket().getFactory();

    for (int workers : new int[] {1, 3}) {
      Map<StateExpr, BDD> distributed =
          runDistributed(configs, dp, assignment, workers, referenceFactory);
      for (Map.Entry<StateExpr, BDD> entry : local.entrySet()) {
        BDD actual = distributed.get(entry.getKey());
        assertThat(
            String.format("workers=%d: missing/not equal state %s", workers, entry.getKey()),
            actual != null && actual.biimp(entry.getValue()).isOne(),
            is(true));
      }
    }
  }

  private static IpSpaceAssignment interfaceAssignment(Map<String, Configuration> configs) {
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
      Map<String, Configuration> configs, DataPlane dp, IpSpaceAssignment assignment) {
    BDDPacket packet = new BDDPacket();
    BDDReachabilityAnalysisFactory factory =
        new BDDReachabilityAnalysisFactory(
            packet,
            configs,
            dp.getForwardingAnalysis(),
            new IpsRoutedOutInterfacesFactory(dp.getFibs()),
            false,
            false);
    return factory.bddReachabilityAnalysis(assignment);
  }

  private static Map<StateExpr, BDD> runDistributed(
      Map<String, Configuration> configs,
      DataPlane dp,
      IpSpaceAssignment assignment,
      int workers,
      JFactory referenceFactory)
      throws Exception {
    Map<String, Integer> partition =
        NetworkPartitioner.partition(new HashSet<>(configs.keySet()), workers, 0L);

    List<Worker> workerList = new ArrayList<>();
    for (int w = 0; w < workers; w++) {
      BDDReachabilityAnalysis analysis = buildAnalysis(configs, dp, assignment);
      workerList.add(
          new Worker(w, analysis, partition, (JFactory) analysis.getBDDPacket().getFactory()));
    }

    List<S2BddSidecar> servers = new ArrayList<>();
    List<S2WorkerEndpoint> endpoints = new ArrayList<>();
    for (int w = 0; w < workers; w++) {
      Worker worker = workerList.get(w);
      S2BddSidecar server =
          new S2BddSidecar(0, (state, payload) -> worker.inbox.add(new Inbox(state, payload)));
      server.start();
      servers.add(server);
      endpoints.add(new S2WorkerEndpoint("127.0.0.1", server.getPort()));
    }
    for (Worker worker : workerList) {
      S2BddSidecar.Client[] clients = new S2BddSidecar.Client[workers];
      for (int t = 0; t < workers; t++) {
        clients[t] = new S2BddSidecar.Client(endpoints.get(t));
      }
      worker.clients = clients;
    }

    ExecutorService pool = Executors.newFixedThreadPool(workers);
    try {
      S2Coordinator coordinator = new BarrierCoordinator(workers);
      List<Future<?>> futures = new ArrayList<>();
      for (Worker worker : workerList) {
        futures.add(pool.submit(() -> worker.run(coordinator)));
      }
      for (Future<?> future : futures) {
        future.get();
      }
      Map<StateExpr, BDD> result = new HashMap<>();
      for (Worker worker : workerList) {
        for (Map.Entry<StateExpr, BDD> e : worker.reachable.entrySet()) {
          String payload = new BDDTransfer().save(e.getValue());
          result.put(e.getKey(), new BDDTransfer().load(referenceFactory, payload));
        }
      }
      return result;
    } finally {
      pool.shutdownNow();
      servers.forEach(S2BddSidecar::close);
    }
  }

  private record Inbox(StateExpr state, String payload) {}

  private record Outbound(StateExpr state, BDD bdd) {}

  static final class Worker {
    final int id;
    final JFactory factory;
    final Map<String, Integer> partition;
    final Table<StateExpr, StateExpr, Transition> edges;
    final Map<StateExpr, BDD> reachable = new HashMap<>();
    final Queue<StateExpr> worklist = new ArrayDeque<>();
    final java.util.concurrent.ConcurrentLinkedQueue<Inbox> inbox =
        new java.util.concurrent.ConcurrentLinkedQueue<>();
    final Map<Integer, List<Outbound>> pending = new HashMap<>();
    S2BddSidecar.Client[] clients;

    Worker(
        int id,
        BDDReachabilityAnalysis analysis,
        Map<String, Integer> partition,
        JFactory factory) {
      this.id = id;
      this.factory = factory;
      this.partition = partition;
      this.edges = analysis.getForwardEdgeTable();
      for (StateExpr state : analysis.getIngressLocationStates()) {
        if (owner(state) == id) {
          reachable.put(state, factory.one());
          worklist.add(state);
        }
      }
    }

    int owner(StateExpr state) {
      String host = ownerHostname(state);
      return host == null || host.isEmpty() ? 0 : partition.getOrDefault(host, 0);
    }

    void processLocal() {
      while (!worklist.isEmpty()) {
        StateExpr state = worklist.poll();
        BDD in = reachable.get(state);
        for (Map.Entry<StateExpr, Transition> edge : edges.row(state).entrySet()) {
          StateExpr post = edge.getKey();
          BDD out = edge.getValue().transitForward(in);
          if (out.isZero()) {
            continue;
          }
          if (owner(post) == id) {
            merge(post, out);
          } else {
            pending
                .computeIfAbsent(owner(post), k -> new ArrayList<>())
                .add(new Outbound(post, out));
          }
        }
      }
    }

    boolean merge(StateExpr state, BDD add) {
      BDD old = reachable.get(state);
      if (old == null) {
        reachable.put(state, add);
        worklist.add(state);
        return true;
      }
      BDD merged = old.or(add);
      if (!merged.equals(old)) {
        reachable.put(state, merged);
        worklist.add(state);
        return true;
      }
      return false;
    }

    boolean flushOutbox() {
      boolean any = false;
      for (Map.Entry<Integer, List<Outbound>> e : pending.entrySet()) {
        for (Outbound out : e.getValue()) {
          clients[e.getKey()].transit(out.state, out.bdd);
          any = true;
        }
      }
      pending.clear();
      return any;
    }

    boolean drainInbox() {
      boolean changed = false;
      Inbox message;
      while ((message = inbox.poll()) != null) {
        try {
          changed |= merge(message.state, new BDDTransfer().load(factory, message.payload));
        } catch (java.io.IOException e) {
          throw new RuntimeException(e);
        }
      }
      processLocal();
      return changed;
    }

    void run(S2Coordinator coordinator) {
      boolean done = false;
      while (!done) {
        processLocal();
        coordinator.roundCheck(false);
        boolean sent = flushOutbox();
        boolean anySent = coordinator.roundCheck(sent);
        boolean changed = drainInbox();
        boolean anyChanged = coordinator.roundCheck(changed);
        done = !anySent && !anyChanged;
      }
    }
  }

  /**
   * In-process {@link S2Coordinator}: global OR of the round's dirty flags, barrier-synchronized so
   * every worker stops on the same round.
   */
  static final class BarrierCoordinator implements S2Coordinator {
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

    @Override
    public int sumAll(int localValue) {
      return localValue;
    }
  }

  private static String ownerHostname(StateExpr state) {
    if (state instanceof NodeStateExpr) {
      return ((NodeStateExpr) state).getHostname();
    }
    if (state instanceof InterfaceStateExpr) {
      return ((InterfaceStateExpr) state).getHostname();
    }
    if (state instanceof VrfStateExpr) {
      return ((VrfStateExpr) state).getHostname();
    }
    if (state instanceof EdgeStateExpr) {
      return ((EdgeStateExpr) state).getSrcNode();
    }
    if (state instanceof PacketPolicyStatement) {
      return ((PacketPolicyStatement) state).getHostname();
    }
    if (state instanceof PacketPolicyAction) {
      return ((PacketPolicyAction) state).getHostname();
    }
    return "";
  }
}
