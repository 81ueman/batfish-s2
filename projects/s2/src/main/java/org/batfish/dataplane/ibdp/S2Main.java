package org.batfish.dataplane.ibdp;

import static org.batfish.datamodel.acl.AclLineMatchExprs.matchDst;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.lang.management.ManagementFactory;
import java.lang.management.MemoryPoolMXBean;
import java.lang.management.MemoryType;
import java.lang.management.MemoryUsage;
import java.net.Socket;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.concurrent.atomic.AtomicReference;
import net.sf.javabdd.BDD;
import net.sf.javabdd.BDDTransfer;
import net.sf.javabdd.JFactory;
import org.batfish.bddreachability.BDDReachabilityAnalysis;
import org.batfish.bddreachability.BDDReachabilityAnalysisFactory;
import org.batfish.bddreachability.BDDReachabilityUtils;
import org.batfish.bddreachability.IpsRoutedOutInterfacesFactory;
import org.batfish.bddreachability.transition.TransitionTransfer;
import org.batfish.common.bdd.BDDPacket;
import org.batfish.datamodel.AbstractRoute;
import org.batfish.datamodel.Configuration;
import org.batfish.datamodel.DataPlane;
import org.batfish.datamodel.FinalMainRib;
import org.batfish.datamodel.Flow;
import org.batfish.datamodel.FlowDisposition;
import org.batfish.datamodel.ForwardingAnalysis;
import org.batfish.datamodel.Interface;
import org.batfish.datamodel.Ip;
import org.batfish.datamodel.IpProtocol;
import org.batfish.datamodel.IpSpace;
import org.batfish.datamodel.Prefix;
import org.batfish.datamodel.UniverseIpSpace;
import org.batfish.datamodel.Vrf;
import org.batfish.datamodel.flow.Trace;
import org.batfish.dataplane.TracerouteEngineImpl;
import org.batfish.specifier.InterfaceLocation;
import org.batfish.specifier.IpSpaceAssignment;
import org.batfish.symbolic.state.StateExpr;

/**
 * Runnable entry point for the multi-process S2 demo.
 *
 * <pre>
 *   S2Main controller &lt;network&gt; &lt;numWorkers&gt; &lt;endpointsCsv&gt; &lt;controllerPort&gt;
 *   S2Main worker &lt;network&gt; &lt;workerId&gt; &lt;numWorkers&gt; &lt;controllerHost&gt; &lt;controllerPort&gt; &lt;sidecarPort&gt;
 * </pre>
 *
 * <p>Configs are read from {@code $S2_INPUT_DIR/&lt;network&gt;/configs} (default {@code
 * /s2/inputs}); results are written under {@code $S2_OUTPUT_DIR} (default {@code /s2/outputs}).
 */
public final class S2Main {

  /**
   * Worker BDD sidecars listen at route sidecar port + this offset (kept out of the route range).
   */
  private static final int BDD_PORT_OFFSET = 1000;

  private S2Main() {}

  public static void main(String[] args) throws Exception {
    if (args.length == 0) {
      throw new IllegalArgumentException("usage: S2Main controller|worker ...");
    }
    switch (args[0]) {
      case "controller":
        runController(args);
        break;
      case "worker":
        runWorker(args);
        break;
      default:
        throw new IllegalArgumentException("unknown role " + args[0]);
    }
  }

  private static Path inputDir() {
    return Paths.get(System.getenv().getOrDefault("S2_INPUT_DIR", "/s2/inputs"));
  }

  private static Path outputDir() throws IOException {
    Path dir = Paths.get(System.getenv().getOrDefault("S2_OUTPUT_DIR", "/s2/outputs"));
    Files.createDirectories(dir);
    return dir;
  }

  /** Number of destination-prefix query shards (S2 prefix sharding); default 1 (no sharding). */
  private static int queryShardCount() {
    return Math.max(1, Integer.parseInt(System.getenv().getOrDefault("S2_SHARDS", "1")));
  }

  /**
   * Shadow nodes delegate BGP and OSPF; EIGRP/IS-IS/RIP are not distributed, so a multi-worker run
   * of a snapshot that uses them would hit a null shadow process. Fail with a clear message.
   */
  private static void assertDistributedProtocolsSupported(S2Snapshot snap, int numWorkers) {
    if (numWorkers <= 1) {
      return;
    }
    for (Configuration c : snap.configs.values()) {
      for (Vrf vrf : c.getVrfs().values()) {
        if (!vrf.getEigrpProcesses().isEmpty()
            || vrf.getIsisProcess() != null
            || vrf.getRipProcess() != null) {
          throw new UnsupportedOperationException(
              "Multi-worker distributed routing supports only eBGP and OSPF: "
                  + c.getHostname()
                  + " uses EIGRP/IS-IS/RIP. Run with 1 worker or use the in-process "
                  + "S2DistributedControlPlaneTest.");
        }
      }
    }
  }

  /** Sum of the peak used bytes across all heap memory pools (for scale reporting). */
  private static long peakHeapBytes() {
    long total = 0;
    for (MemoryPoolMXBean pool : ManagementFactory.getMemoryPoolMXBeans()) {
      if (pool.getType() == MemoryType.HEAP) {
        MemoryUsage peak = pool.getPeakUsage();
        if (peak != null) {
          total += peak.getUsed();
        }
      }
    }
    return total;
  }

  // ---------------------------------------------------------------- controller

  private static void runController(String[] args) throws Exception {
    String network = args[1];
    int numWorkers = Integer.parseInt(args[2]);
    List<S2WorkerEndpoint> endpoints = parseEndpoints(args[3]);
    int port = Integer.parseInt(args[4]);

    S2Snapshot snap = S2Snapshot.load(inputDir().resolve(network).resolve("configs"));
    assertDistributedProtocolsSupported(snap, numWorkers);
    snap.batfish.computeDataPlane(snap.snapshot);
    DataPlane vanilla = snap.batfish.loadDataPlane(snap.snapshot);
    Map<String, Map<String, Set<String>>> vanillaRibs = canonical(ribsOf(vanilla, null, null));

    // Prefix sharding: partition the destination prefix space. shardCount()==1 keeps the full space
    // in a single shard (the historical behavior); >1 splits it so each worker only holds one
    // shard's BDDs at a time.
    List<Prefix> queryPrefixes = PrefixSharder.queryPrefixes(snap.configs);
    List<IpSpace> queryShards = PrefixSharder.shard(queryPrefixes, queryShardCount());
    IpSpace querySpace = PrefixSharder.union(queryShards);
    System.out.printf(
        "S2 prefix sharding: %d shard(s) over %d prefixes%n",
        queryShards.size(), queryPrefixes.size());

    try (S2ControllerServer server =
        new S2ControllerServer(port, numWorkers, endpoints, queryShards)) {
      server.start();
      System.out.printf(
          "S2 controller listening on %d, waiting for %d workers%n", port, numWorkers);
      Map<Integer, S2ControlMessages.Result> results = server.awaitResults(3600);
      Map<String, Map<String, List<AbstractRoute>>> merged = new TreeMap<>();
      for (S2ControlMessages.Result workerResult : results.values()) {
        workerResult.ribs.forEach(
            (host, byVrf) -> merged.computeIfAbsent(host, h -> new TreeMap<>()).putAll(byVrf));
      }
      Map<String, Map<String, Set<String>>> distributedRibs = canonical(merged);
      boolean match = vanillaRibs.equals(distributedRibs);
      Map<String, String> vanillaReach = reachabilityDigest(vanilla, snap);
      boolean reachMatch = true;
      for (S2ControlMessages.Result workerResult : results.values()) {
        reachMatch &= workerResult.reachability.equals(vanillaReach);
      }

      // Distributed symbolic reachability (M5). First compare the workers' reachable BDDs
      // state-by-state against the reference. This is strict: the answer check below re-runs the
      // fixpoint on the full reference graph (seeded with the distributed result), so it could mask
      // a worker that dropped states.
      BDDReachabilityAnalysis referenceAnalysis =
          buildReachabilityAnalysis(snap, vanilla, null, querySpace);
      JFactory referenceFactory = (JFactory) referenceAnalysis.getBDDPacket().getFactory();
      // Each (worker, shard) contributed a partial per-state map; OR them together. The union over
      // a partition of the query equals the full query's reachable states.
      Map<StateExpr, BDD> mergedReachable = new HashMap<>();
      for (Map<StateExpr, String> shardResult : server.getShardResults()) {
        for (Map.Entry<StateExpr, String> e : shardResult.entrySet()) {
          BDD bdd = new BDDTransfer().load(referenceFactory, e.getValue());
          mergedReachable.merge(e.getKey(), bdd, (a, b) -> a.or(b));
        }
      }
      Map<StateExpr, BDD> referenceReachable = referenceAnalysis.computeReverseReachableStates();
      boolean symbolicMatch = mergedReachable.keySet().equals(referenceReachable.keySet());
      StateExpr firstSymbolicDiff = null;
      if (symbolicMatch) {
        for (Map.Entry<StateExpr, BDD> e : referenceReachable.entrySet()) {
          BDD actual = mergedReachable.get(e.getKey());
          if (actual == null || !actual.biimp(e.getValue()).isOne()) {
            symbolicMatch = false;
            firstSymbolicDiff = e.getKey();
            break;
          }
        }
      }

      // Public API level: turn the reachable BDDs into concrete flows and compare with vanilla.
      Set<Flow> distributedFlows =
          BDDReachabilityUtils.constructFlows(
              referenceAnalysis.getBDDPacket(),
              referenceAnalysis.getIngressLocationReachableBDDs(mergedReachable));
      Set<Flow> vanillaFlows =
          BDDReachabilityUtils.constructFlows(
              referenceAnalysis.getBDDPacket(),
              referenceAnalysis.getIngressLocationReachableBDDs());
      boolean answerMatch = distributedFlows.equals(vanillaFlows);

      // Per-worker peak heap (scale evidence).
      long totalPeakHeapBytes = 0;
      for (S2ControlMessages.Result workerResult : results.values()) {
        totalPeakHeapBytes += workerResult.peakHeapBytes;
      }
      long controllerPeakHeapBytes = peakHeapBytes();
      for (S2ControlMessages.Result workerResult : results.values()) {
        System.out.printf(
            "worker %d peak heap %.1f MiB%n",
            workerResult.workerId, workerResult.peakHeapBytes / 1048576.0);
      }

      Path out = outputDir().resolve("result-" + numWorkers + "worker.txt");
      StringBuilder report = new StringBuilder();
      report.append("network=").append(network).append('\n');
      report.append("workers=").append(numWorkers).append('\n');
      report.append("hosts=").append(distributedRibs.size()).append('\n');
      report.append(match ? "RESULT=MATCH\n" : "RESULT=DIFF\n");
      report.append(reachMatch ? "REACHABILITY=MATCH\n" : "REACHABILITY=DIFF\n");
      report.append(symbolicMatch ? "SYMBOLIC=MATCH\n" : "SYMBOLIC=DIFF\n");
      report.append(answerMatch ? "ANSWER=MATCH\n" : "ANSWER=DIFF\n");
      report.append("--- per-worker peak heap (MiB) ---\n");
      for (S2ControlMessages.Result workerResult : results.values()) {
        report
            .append("worker ")
            .append(workerResult.workerId)
            .append(": ")
            .append(String.format("%.1f", workerResult.peakHeapBytes / 1048576.0))
            .append('\n');
      }
      report.append(String.format("total workers: %.1f%n", totalPeakHeapBytes / 1048576.0));
      report.append(String.format("controller: %.1f%n", controllerPeakHeapBytes / 1048576.0));
      report.append("--- distributed ---\n").append(distributedRibs);
      Files.writeString(out, report.toString());
      boolean allMatch = match && reachMatch && symbolicMatch && answerMatch;
      System.out.printf(
          "S2 %s (%d workers): ribs=%s reachability=%s symbolic=%s answer=%s, wrote %s%n",
          allMatch ? "MATCH" : "DIFF",
          numWorkers,
          match ? "MATCH" : "DIFF",
          reachMatch ? "MATCH" : "DIFF",
          symbolicMatch ? "MATCH" : "DIFF",
          answerMatch ? "MATCH" : "DIFF",
          out);
      if (!allMatch) {
        System.out.println("vanilla ribs: " + vanillaRibs);
        System.out.println("distributed:  " + distributedRibs);
        System.out.println("vanilla reach: " + vanillaReach);
        results.values().forEach(r -> System.out.println("worker reach: " + r.reachability));
        if (!symbolicMatch) {
          System.out.printf(
              "symbolic: reference states=%d distributed states=%d firstDiff=%s%n",
              referenceReachable.size(), mergedReachable.size(), firstSymbolicDiff);
        }
      }
    }
  }

  // -------------------------------------------------------------------- worker

  private static void runWorker(String[] args) throws Exception {
    String network = args[1];
    int workerId = Integer.parseInt(args[2]);
    int numWorkers = Integer.parseInt(args[3]);
    String controllerHost = args[4];
    int controllerPort = Integer.parseInt(args[5]);
    int sidecarPort = Integer.parseInt(args[6]);

    S2Snapshot snap = S2Snapshot.load(inputDir().resolve(network).resolve("configs"));
    assertDistributedProtocolsSupported(snap, numWorkers);
    Map<String, Integer> assignment =
        NetworkPartitioner.partition(snap.configs.keySet(), numWorkers, 0L);
    Set<String> ownedHosts = new HashSet<>();
    assignment.forEach(
        (host, owner) -> {
          if (owner == workerId) {
            ownedHosts.add(host);
          }
        });

    Map<String, DistributedNode> nodes = new HashMap<>();
    for (String host : snap.configs.keySet()) {
      nodes.put(
          host,
          assignment.get(host) == workerId
              ? DistributedNode.real(snap.configs.get(host))
              : DistributedNode.shadow(snap.configs.get(host)));
    }

    Map<String, Node> nodeMap = new HashMap<>(nodes);
    List<org.batfish.datamodel.BgpAdvertisement> adverts =
        new ArrayList<>(snap.batfish.loadExternalBgpAnnouncements(snap.snapshot, snap.configs));

    AtomicReference<BDDReachabilityAnalysis> localAnalysisRef = new AtomicReference<>();
    try (S2SidecarServer sidecar =
        new S2SidecarServer(
            sidecarPort,
            S2SidecarHandlers.forWorker(
                nodeMap,
                snap.bgpTopology,
                snap.networkConfigurations,
                ownedHosts,
                localAnalysisRef::get))) {
      sidecar.start();

      try (Socket socket = connectWithRetry(controllerHost, controllerPort)) {
        ObjectOutputStream out = new ObjectOutputStream(socket.getOutputStream());
        out.flush();
        ObjectInputStream in = new ObjectInputStream(socket.getInputStream());
        out.writeObject(new S2ControlMessages.Register(workerId));
        out.flush();
        S2ControlMessages.Start start = (S2ControlMessages.Start) in.readObject();

        S2SidecarClient client = new S2SidecarClient();
        for (String host : snap.configs.keySet()) {
          if (assignment.get(host) != workerId) {
            DistributedNode shadow = nodes.get(host);
            S2WorkerEndpoint owner = start.endpoints.get(assignment.get(host));
            shadow.installRemoteBgpProviders(client, owner);
            shadow.installRemoteOspfProviders(
                client, owner, snap.topologyContext.getOspfTopology());
          }
        }

        S2RemoteCoordinator coordinator = new S2RemoteCoordinator(out, in);
        ShadowMainRibSync shadowSync =
            new ShadowMainRibSync(nodes, assignment, workerId, start.endpoints, client);
        S2BdpEngine engine = new S2BdpEngine(snap.settings(), nodes, coordinator, shadowSync);
        DataPlane dp =
            engine.computeDataPlane(
                    snap.configs,
                    snap.topologyContext,
                    new java.util.HashSet<>(adverts),
                    snap.ipOwners,
                    false)
                ._dataPlane;

        // Distributed symbolic reachability (M5) over the converged dataplane. Each worker builds
        // only its own switches' edges (OwnedForwardingAnalysis); the boundary edges into its
        // states are pulled from the peers that own their sources. The query is prefix-sharded
        // (S2 prefix sharding): the controller sends the destination-prefix shards and each shard
        // is
        // run as its own fixpoint so only one shard's BDDs are live at a time.
        IpSpace querySpace = PrefixSharder.union(start.queryShards);
        BDDReachabilityAnalysis analysis =
            buildReachabilityAnalysis(snap, dp, ownedHosts, querySpace);
        localAnalysisRef.set(analysis);
        JFactory factory = (JFactory) analysis.getBDDPacket().getFactory();

        // Make sure every worker has published its analysis before anyone pulls boundary edges.
        coordinator.roundCheck(false);

        List<S2Messages.SerializedEdge> pulledEdges = new ArrayList<>();
        for (int t = 0; t < numWorkers; t++) {
          if (t == workerId) {
            continue;
          }
          S2Messages.BoundaryEdgesResponse response =
              (S2Messages.BoundaryEdgesResponse)
                  client.call(
                      start.endpoints.get(t), new S2Messages.BoundaryEdgesRequest(ownedHosts));
          pulledEdges.addAll(response.edges);
        }

        S2BddSidecar.Client[] bddClients = new S2BddSidecar.Client[numWorkers];
        for (int t = 0; t < numWorkers; t++) {
          S2WorkerEndpoint routeEndpoint = start.endpoints.get(t);
          bddClients[t] =
              new S2BddSidecar.Client(
                  new S2WorkerEndpoint(
                      routeEndpoint.getHost(), routeEndpoint.getPort() + BDD_PORT_OFFSET));
        }
        S2ReachabilityWorker[] holder = new S2ReachabilityWorker[1];
        System.err.printf(
            "worker %d starting BDD sidecar on %d%n", workerId, sidecarPort + BDD_PORT_OFFSET);
        try (S2BddSidecar bddSidecar =
            new S2BddSidecar(
                sidecarPort + BDD_PORT_OFFSET,
                (state, payload) -> {
                  if (holder[0] != null) {
                    holder[0].receive(state, payload);
                  }
                })) {
          bddSidecar.start();
          holder[0] =
              new S2ReachabilityWorker(workerId, assignment, analysis, bddClients, coordinator);
          for (S2Messages.SerializedEdge edge : pulledEdges) {
            holder[0].addEdge(
                edge.preState, edge.postState, TransitionTransfer.load(factory, edge.transition));
          }
          System.out.printf(
              "S2 worker %d generated %d local symbolic edges, pulled %d boundary edges, %d query"
                  + " shard(s)%n",
              workerId,
              holder[0].edgeCount() - pulledEdges.size(),
              pulledEdges.size(),
              start.queryShards.size());
          // One fixpoint per query shard; serialize and ship the shard's results, then drop them so
          // only one shard's BDDs are live at a time (this is the memory saving).
          long peakResultNodes = 0;
          long peakFactoryNodes = 0;
          for (IpSpace shard : start.queryShards) {
            BDD shardRoot =
                analysis
                    .getQueryHeaderSpaceBdd()
                    .and(analysis.getBDDPacket().getDstIpSpaceToBDD().visit(shard));
            Map<StateExpr, BDD> reachable = holder[0].run(shardRoot);
            peakResultNodes = Math.max(peakResultNodes, factory.nodeCount(reachable.values()));
            Map<StateExpr, String> symbolicSerializedShard = new HashMap<>();
            for (Map.Entry<StateExpr, BDD> e : reachable.entrySet()) {
              symbolicSerializedShard.put(e.getKey(), new BDDTransfer().save(e.getValue()));
            }
            out.writeObject(new S2ControlMessages.ShardResult(workerId, symbolicSerializedShard));
            out.flush();
            System.gc();
            holder[0].collectGarbage();
            peakFactoryNodes = Math.max(peakFactoryNodes, factory.getNodeNum());
          }
          System.out.printf(
              "S2 worker %d peak result BDD nodes %d (factory %d)%n",
              workerId, peakResultNodes, peakFactoryNodes);
        }
        Map<StateExpr, String> symbolicSerialized = new HashMap<>();

        long peakHeapBytes = peakHeapBytes();
        out.writeObject(
            new S2ControlMessages.Result(
                workerId,
                ribsOf(dp, assignment, workerId),
                reachabilityDigest(dp, snap),
                symbolicSerialized,
                peakHeapBytes));
        out.flush();
        System.out.printf(
            "S2 worker %d done (peak heap %.1f MiB)%n", workerId, peakHeapBytes / 1048576.0);
      }
    }
  }

  // ------------------------------------------------------------------- helpers

  /**
   * Build Batfish's BDD reachability analysis over the given dataplane (all interface sources).
   *
   * <p>When {@code ownedHosts} is non-null the forwarding analysis is restricted to those switches
   * so the worker generates only locally-owned edges; cross-worker edges into owned states are
   * pulled from their owners. {@code querySpace} is the destination prefix space the query is
   * restricted to (the union of the prefix shards).
   */
  private static BDDReachabilityAnalysis buildReachabilityAnalysis(
      S2Snapshot snap, DataPlane dp, Set<String> ownedHosts, IpSpace querySpace) {
    BDDPacket packet = new BDDPacket();
    ForwardingAnalysis forwardingAnalysis =
        ownedHosts == null
            ? dp.getForwardingAnalysis()
            : new OwnedForwardingAnalysis(dp.getForwardingAnalysis(), ownedHosts);
    BDDReachabilityAnalysisFactory factory =
        new BDDReachabilityAnalysisFactory(
            packet,
            snap.configs,
            forwardingAnalysis,
            new IpsRoutedOutInterfacesFactory(dp.getFibs()),
            false,
            false);
    IpSpaceAssignment.Builder builder = IpSpaceAssignment.builder();
    for (Configuration c : snap.configs.values()) {
      for (Interface i : c.getAllInterfaces().values()) {
        if (i.getActive()) {
          builder.assign(
              new InterfaceLocation(c.getHostname(), i.getName()), UniverseIpSpace.INSTANCE);
        }
      }
    }
    return factory.bddReachabilityAnalysis(
        builder.build(),
        matchDst(querySpace),
        Set.of(),
        Set.of(),
        snap.configs.keySet(),
        Set.of(FlowDisposition.ACCEPTED));
  }

  /** Data-plane check: traceroute between every pair of loopback addresses, as dispositions. */
  private static Map<String, String> reachabilityDigest(DataPlane dp, S2Snapshot snap) {
    Map<String, Ip> ips = new TreeMap<>();
    snap.configs.forEach(
        (host, configuration) -> {
          Ip ip = nodeIp(configuration);
          if (ip != null) {
            ips.put(host, ip);
          }
        });
    Set<Flow> flows = new HashSet<>();
    for (String src : ips.keySet()) {
      for (String dst : ips.keySet()) {
        if (src.equals(dst)) {
          continue;
        }
        flows.add(
            Flow.builder()
                .setIngressNode(src)
                .setIngressVrf(Configuration.DEFAULT_VRF_NAME)
                .setSrcIp(ips.get(src))
                .setDstIp(ips.get(dst))
                .setIpProtocol(IpProtocol.TCP)
                .setSrcPort(12345)
                .setDstPort(80)
                .build());
      }
    }
    TracerouteEngineImpl engine =
        new TracerouteEngineImpl(dp, snap.topologyContext.getLayer3Topology(), snap.configs);
    Map<Flow, List<Trace>> traces = engine.computeTraces(flows, false);
    Map<String, String> digest = new TreeMap<>();
    traces.forEach(
        (flow, list) ->
            digest.put(
                flow.getIngressNode() + "->" + flow.getDstIp(),
                list.isEmpty() ? "NO_TRACE" : list.get(0).getDisposition().name()));
    return digest;
  }

  private static Ip nodeIp(Configuration configuration) {
    for (Interface iface : configuration.getAllInterfaces().values()) {
      if (iface.getName().startsWith("Loopback") && iface.getConcreteAddress() != null) {
        return iface.getConcreteAddress().getIp();
      }
    }
    return null;
  }

  private static Socket connectWithRetry(String host, int port) throws InterruptedException {
    for (int attempt = 1; ; attempt++) {
      try {
        return new Socket(host, port);
      } catch (IOException e) {
        if (attempt >= 120) {
          throw new RuntimeException("could not connect to controller " + host + ":" + port, e);
        }
        Thread.sleep(1000);
      }
    }
  }

  private static List<S2WorkerEndpoint> parseEndpoints(String csv) {
    List<S2WorkerEndpoint> endpoints = new ArrayList<>();
    for (String part : csv.split(",")) {
      String[] hostPort = part.trim().split(":");
      endpoints.add(new S2WorkerEndpoint(hostPort[0], Integer.parseInt(hostPort[1])));
    }
    return endpoints;
  }

  /** Convert a dataplane's RIBs; if {@code assignment}/{@code workerId} given, keep owned hosts. */
  private static Map<String, Map<String, List<AbstractRoute>>> ribsOf(
      DataPlane dp, Map<String, Integer> assignment, Integer workerId) {
    Map<String, Map<String, List<AbstractRoute>>> result = new TreeMap<>();
    for (com.google.common.collect.Table.Cell<String, String, FinalMainRib> cell :
        dp.getRibs().cellSet()) {
      if (assignment != null && !assignment.get(cell.getRowKey()).equals(workerId)) {
        continue;
      }
      result
          .computeIfAbsent(cell.getRowKey(), h -> new TreeMap<>())
          .put(cell.getColumnKey(), new ArrayList<>(cell.getValue().getRoutes()));
    }
    return result;
  }

  private static Map<String, Map<String, Set<String>>> canonical(
      Map<String, Map<String, List<AbstractRoute>>> ribs) {
    Map<String, Map<String, Set<String>>> result = new TreeMap<>();
    ribs.forEach(
        (host, byVrf) -> {
          Map<String, Set<String>> vrfMap = new TreeMap<>();
          byVrf.forEach(
              (vrf, routes) -> {
                Set<String> names = new TreeSet<>();
                routes.forEach(r -> names.add(r.toString()));
                vrfMap.put(vrf, names);
              });
          result.put(host, vrfMap);
        });
    return result;
  }
}
