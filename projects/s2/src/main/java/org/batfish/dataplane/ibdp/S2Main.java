package org.batfish.dataplane.ibdp;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
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
import net.sf.javabdd.BDD;
import net.sf.javabdd.BDDTransfer;
import net.sf.javabdd.JFactory;
import org.batfish.bddreachability.BDDReachabilityAnalysis;
import org.batfish.bddreachability.BDDReachabilityAnalysisFactory;
import org.batfish.bddreachability.BDDReachabilityUtils;
import org.batfish.bddreachability.IpsRoutedOutInterfacesFactory;
import org.batfish.common.bdd.BDDPacket;
import org.batfish.datamodel.AbstractRoute;
import org.batfish.datamodel.Configuration;
import org.batfish.datamodel.DataPlane;
import org.batfish.datamodel.FinalMainRib;
import org.batfish.datamodel.Flow;
import org.batfish.datamodel.Interface;
import org.batfish.datamodel.Ip;
import org.batfish.datamodel.IpProtocol;
import org.batfish.datamodel.UniverseIpSpace;
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

  /** Worker BDD sidecars listen at route sidecar port + this offset (kept out of the route range). */
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

  // ---------------------------------------------------------------- controller

  private static void runController(String[] args) throws Exception {
    String network = args[1];
    int numWorkers = Integer.parseInt(args[2]);
    List<S2WorkerEndpoint> endpoints = parseEndpoints(args[3]);
    int port = Integer.parseInt(args[4]);

    S2Snapshot snap = S2Snapshot.load(inputDir().resolve(network).resolve("configs"));
    snap.batfish.computeDataPlane(snap.snapshot);
    DataPlane vanilla = snap.batfish.loadDataPlane(snap.snapshot);
    Map<String, Map<String, Set<String>>> vanillaRibs = canonical(ribsOf(vanilla, null, null));

    try (S2ControllerServer server = new S2ControllerServer(port, numWorkers, endpoints)) {
      server.start();
      System.out.printf("S2 controller listening on %d, waiting for %d workers%n", port, numWorkers);
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

      // Distributed symbolic reachability (M5), evaluated at the public API level: combine the
      // workers' reachable BDDs, turn them into the reachability answer (concrete flows), and
      // compare with vanilla Batfish's answer.
      BDDReachabilityAnalysis referenceAnalysis = buildReachabilityAnalysis(snap, vanilla);
      JFactory referenceFactory = (JFactory) referenceAnalysis.getBDDPacket().getFactory();
      Map<StateExpr, BDD> mergedReachable = new HashMap<>();
      for (S2ControlMessages.Result workerResult : results.values()) {
        for (Map.Entry<StateExpr, String> e : workerResult.symbolicReachable.entrySet()) {
          mergedReachable.put(e.getKey(), new BDDTransfer().load(referenceFactory, e.getValue()));
        }
      }
      Set<Flow> distributedFlows =
          BDDReachabilityUtils.constructFlows(
              referenceAnalysis.getBDDPacket(),
              referenceAnalysis.getIngressLocationReachableBDDs(mergedReachable));
      Set<Flow> vanillaFlows =
          BDDReachabilityUtils.constructFlows(
              referenceAnalysis.getBDDPacket(), referenceAnalysis.getIngressLocationReachableBDDs());
      boolean answerMatch = distributedFlows.equals(vanillaFlows);

      Path out = outputDir().resolve("result-" + numWorkers + "worker.txt");
      StringBuilder report = new StringBuilder();
      report.append("network=").append(network).append('\n');
      report.append("workers=").append(numWorkers).append('\n');
      report.append("hosts=").append(distributedRibs.size()).append('\n');
      report.append(match ? "RESULT=MATCH\n" : "RESULT=DIFF\n");
      report.append(reachMatch ? "REACHABILITY=MATCH\n" : "REACHABILITY=DIFF\n");
      report.append(answerMatch ? "ANSWER=MATCH\n" : "ANSWER=DIFF\n");
      report.append("--- distributed ---\n").append(distributedRibs);
      Files.writeString(out, report.toString());
      boolean allMatch = match && reachMatch && answerMatch;
      System.out.printf(
          "S2 %s (%d workers): ribs=%s reachability=%s answer=%s, wrote %s%n",
          allMatch ? "MATCH" : "DIFF",
          numWorkers,
          match ? "MATCH" : "DIFF",
          reachMatch ? "MATCH" : "DIFF",
          answerMatch ? "MATCH" : "DIFF",
          out);
      if (!allMatch) {
        System.out.println("vanilla ribs: " + vanillaRibs);
        System.out.println("distributed:  " + distributedRibs);
        System.out.println("vanilla reach: " + vanillaReach);
        results.values().forEach(r -> System.out.println("worker reach: " + r.reachability));
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
    Map<String, Integer> assignment =
        NetworkPartitioner.partition(snap.configs.keySet(), numWorkers, 0L);

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

    try (S2SidecarServer sidecar =
        new S2SidecarServer(
            sidecarPort,
            S2SidecarHandlers.forWorker(
                nodeMap, snap.bgpTopology, snap.networkConfigurations))) {
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
            shadow.installRemoteBgpProviders(client, start.endpoints.get(assignment.get(host)));
          }
        }

        S2RemoteCoordinator coordinator = new S2RemoteCoordinator(out, in);
        ShadowMainRibSync shadowSync =
            new ShadowMainRibSync(nodes, assignment, workerId, start.endpoints, client);
        S2BdpEngine engine = new S2BdpEngine(snap.settings(), nodes, coordinator, shadowSync);
        DataPlane dp =
            engine
                .computeDataPlane(
                    snap.configs, snap.topologyContext, new java.util.HashSet<>(adverts),
                    snap.ipOwners, false)
                ._dataPlane;

        // Distributed symbolic reachability (M5) over the converged dataplane.
        BDDReachabilityAnalysis analysis = buildReachabilityAnalysis(snap, dp);
        JFactory factory = (JFactory) analysis.getBDDPacket().getFactory();
        S2BddSidecar.Client[] bddClients = new S2BddSidecar.Client[numWorkers];
        for (int t = 0; t < numWorkers; t++) {
          S2WorkerEndpoint routeEndpoint = start.endpoints.get(t);
          bddClients[t] =
              new S2BddSidecar.Client(
                  new S2WorkerEndpoint(
                      routeEndpoint.getHost(), routeEndpoint.getPort() + BDD_PORT_OFFSET));
        }
        Map<StateExpr, String> symbolicSerialized = new HashMap<>();
        S2ReachabilityWorker[] holder = new S2ReachabilityWorker[1];
        System.err.printf("worker %d starting BDD sidecar on %d%n", workerId, sidecarPort + BDD_PORT_OFFSET);
        try (S2BddSidecar bddSidecar =
            new S2BddSidecar(
                sidecarPort + BDD_PORT_OFFSET,
                factory,
                (state, bdd) -> {
                  if (holder[0] != null) {
                    holder[0].receive(state, bdd);
                  }
                })) {
          bddSidecar.start();
          holder[0] =
              new S2ReachabilityWorker(workerId, assignment, analysis, bddClients, coordinator);
          Map<StateExpr, BDD> reachable = holder[0].run();
          for (Map.Entry<StateExpr, BDD> e : reachable.entrySet()) {
            symbolicSerialized.put(e.getKey(), new BDDTransfer().save(e.getValue()));
          }
        }

        out.writeObject(
            new S2ControlMessages.Result(
                workerId,
                ribsOf(dp, assignment, workerId),
                reachabilityDigest(dp, snap),
                symbolicSerialized));
        out.flush();
        System.out.printf("S2 worker %d done%n", workerId);
      }
    }
  }

  // ------------------------------------------------------------------- helpers

  /** Build Batfish's BDD reachability analysis over the given dataplane (all interface sources). */
  private static BDDReachabilityAnalysis buildReachabilityAnalysis(S2Snapshot snap, DataPlane dp) {
    BDDPacket packet = new BDDPacket();
    BDDReachabilityAnalysisFactory factory =
        new BDDReachabilityAnalysisFactory(
            packet,
            snap.configs,
            dp.getForwardingAnalysis(),
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
    return factory.bddReachabilityAnalysis(builder.build());
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
