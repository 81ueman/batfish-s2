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
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import org.batfish.datamodel.AbstractRoute;
import org.batfish.datamodel.DataPlane;
import org.batfish.datamodel.FinalMainRib;

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
      Map<Integer, Map<String, Map<String, List<AbstractRoute>>>> results =
          server.awaitResults(3600);
      Map<String, Map<String, List<AbstractRoute>>> merged = new TreeMap<>();
      for (Map<String, Map<String, List<AbstractRoute>>> workerRibs : results.values()) {
        workerRibs.forEach((host, byVrf) -> merged.computeIfAbsent(host, h -> new TreeMap<>()).putAll(byVrf));
      }
      Map<String, Map<String, Set<String>>> distributedRibs = canonical(merged);
      boolean match = vanillaRibs.equals(distributedRibs);
      Path out = outputDir().resolve("result-" + numWorkers + "worker.txt");
      StringBuilder report = new StringBuilder();
      report.append("network=").append(network).append('\n');
      report.append("workers=").append(numWorkers).append('\n');
      report.append("hosts=").append(distributedRibs.size()).append('\n');
      report.append(match ? "RESULT=MATCH\n" : "RESULT=DIFF\n");
      report.append("--- distributed ---\n").append(distributedRibs);
      Files.writeString(out, report.toString());
      System.out.printf("S2 %s (%d workers), wrote %s%n", match ? "MATCH" : "DIFF", numWorkers, out);
      if (!match) {
        System.out.println("vanilla:     " + vanillaRibs);
        System.out.println("distributed: " + distributedRibs);
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
            request -> {
              BgpRoutingProcess process =
                  nodeMap
                      .get(request.hostname)
                      .getVirtualRouterOrThrow(request.vrf)
                      .getBgpRoutingProcess();
              return new S2Messages.RoutesResponse(
                  process
                      .getOutgoingRoutesForEdge(
                          request.edgeId,
                          nodeMap,
                          snap.bgpTopology,
                          snap.networkConfigurations,
                          request.isNewSession)
                      .toList());
            })) {
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

        S2BdpEngine engine =
            new S2BdpEngine(snap.settings(), nodes, new S2RemoteCoordinator(out, in));
        DataPlane dp =
            engine
                .computeDataPlane(
                    snap.configs, snap.topologyContext, new java.util.HashSet<>(adverts),
                    snap.ipOwners, false)
                ._dataPlane;
        out.writeObject(
            new S2ControlMessages.Result(workerId, ribsOf(dp, assignment, workerId)));
        out.flush();
        System.out.printf("S2 worker %d done%n", workerId);
      }
    }
  }

  // ------------------------------------------------------------------- helpers

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
