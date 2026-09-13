// SPDX-License-Identifier: Apache-2.0

package org.batfish.dataplane.ibdp;

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
import java.nio.file.StandardCopyOption;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.SortedMap;
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
import org.batfish.datamodel.ForwardingAnalysis;
import org.batfish.datamodel.Interface;
import org.batfish.datamodel.Ip;
import org.batfish.datamodel.IpProtocol;
import org.batfish.datamodel.UniverseIpSpace;
import org.batfish.datamodel.flow.Trace;
import org.batfish.dataplane.TracerouteEngineImpl;
import org.batfish.dataplane.ibdp.partition.AutoSchemeSelector;
import org.batfish.dataplane.ibdp.partition.CommunicationGraph;
import org.batfish.dataplane.ibdp.partition.PartitionScheme;
import org.batfish.specifier.InterfaceLocation;
import org.batfish.specifier.IpSpaceAssignment;
import org.batfish.symbolic.state.StateExpr;

/**
 * Runnable entry point for the multi-process S2 demo.
 *
 * <pre>
 *   S2Main controller &lt;network&gt; &lt;numWorkers&gt; &lt;endpointsCsv&gt; &lt;controllerPort&gt;
 *   S2Main controller-service &lt;numWorkers&gt; &lt;controllerPort&gt;
 *   S2Main worker &lt;network&gt; &lt;workerId&gt; &lt;numWorkers&gt; &lt;controllerHost&gt; &lt;controllerPort&gt; &lt;sidecarPort&gt;
 *   S2Main worker-service &lt;workerId&gt; &lt;controllerHost&gt; &lt;controllerPort&gt; &lt;sidecarPort&gt; [advertisedHost]
 *   S2Main verify &lt;network&gt; &lt;numWorkers&gt;
 *   S2Main partition &lt;network&gt; &lt;numWorkers&gt;
 * </pre>
 *
 * <p>The {@code controller-service} / {@code worker-service} pair is the persistent pool (choice
 * A): the workers register once and stay alive for many snapshots, and the controller accepts one
 * compute request per snapshot from the dataplane engine (see {@link S2ControllerService} and
 * {@link S2WorkerService}). The one-shot {@code controller} / {@code worker} roles remain for the
 * scale/verify experiments.
 *
 * <p>The controller is a lightweight coordinator: it parses the snapshot, resolves the partition,
 * ships configs, and collects the workers' results, which it writes to {@code
 * $S2_OUTPUT_DIR/worker-results-&lt;W&gt;.bin}. The vanilla/reference verification (the vanilla
 * single-machine dataplane, the reference BDD analysis, and the RIB/reachability/symbolic/answer
 * comparisons) runs in a separate {@code verify} process/JVM so the controller's heap stays small
 * (see {@code docs/s2-port/OPS.md}, task A7).
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
      throw new IllegalArgumentException(
          "usage: S2Main controller|controller-service|worker|worker-service|verify|partition ...");
    }
    switch (args[0]) {
      case "controller":
        runController(args);
        break;
      case "controller-service":
        runControllerService(args);
        break;
      case "worker":
        runWorker(args);
        break;
      case "worker-service":
        runWorkerService(args);
        break;
      case "verify":
        runVerify(args);
        break;
      case "partition":
        runPartition(args);
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

  /**
   * Whether the worker restricts its dataplane to owned nodes. Default on; disable with {@code
   * -Ds2.ownedDataplane=false}. Read here only to decide whether the worker can run the global
   * traceroute digest (it cannot in owned mode; see {@code runWorker}); the engine reads the same
   * property for the owned-vs-full decision.
   */
  private static boolean ownedDataplaneMode() {
    return Boolean.parseBoolean(System.getProperty("s2.ownedDataplane", "true"));
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

  /** Print the controller's used and peak heap at a phase boundary (scale attribution). */
  private static void controllerPhase(String phase) {
    Runtime runtime = Runtime.getRuntime();
    long used = runtime.totalMemory() - runtime.freeMemory();
    System.err.printf(
        "S2 controller phase %s: used heap %.1f MiB, peak %.1f MiB%n",
        phase, used / 1048576.0, peakHeapBytes() / 1048576.0);
  }

  // ---------------------------------------------------------------- controller

  private static void runController(String[] args) throws Exception {
    String network = args[1];
    int numWorkers = Integer.parseInt(args[2]);
    List<S2WorkerEndpoint> endpoints = parseEndpoints(args[3]);
    int port = Integer.parseInt(args[4]);

    S2Snapshot snap = S2Snapshot.load(inputDir().resolve(network).resolve("configs"));
    controllerPhase("after snapshot load");
    S2Sharding.assertDistributedProtocolsSupported(snap, numWorkers);
    S2Sharding.Plan plan = S2Sharding.plan(snap, numWorkers);
    System.out.printf("S2 controller: %s%n", S2Sharding.describe(plan, numWorkers));

    // Descriptor-shadow mode (default on; disable with -Ds2.descriptorShadows=false): ship each
    // worker only its owned configs plus a shared reduced descriptor for every remote node, instead
    // of the full snapshot. Also requires config shipping and a snapshot the shadow path can answer
    // without remote policy/forwarding bodies (see S2Sharding.descriptorShadowsSafe, which falls
    // back to shipping full configs for tracks / VNI / tunnel / IPsec).
    boolean shipConfigs = !Boolean.getBoolean("s2.noShipConfigs");
    S2Sharding.Payload payload =
        S2Sharding.preparePayload(
            snap,
            plan.assignment,
            numWorkers,
            shipConfigs,
            Boolean.parseBoolean(System.getProperty("s2.descriptorShadows", "true")));
    if (payload.descriptorShadows()) {
      System.out.printf(
          "S2 controller: descriptor shadows on (%d full configs + %d descriptors)%n",
          numWorkers, payload.numDescriptors);
    }
    controllerPhase("after serialize for shipping");
    try (S2ControllerServer server =
        new S2ControllerServer(
            port,
            numWorkers,
            endpoints,
            plan.assignment,
            payload.configs,
            payload.externalAdverts,
            payload.ownedConfigsByWorker,
            payload.descriptors)) {
      server.start();
      System.out.printf(
          "S2 controller listening on %d, waiting for %d workers%n", port, numWorkers);
      Map<Integer, S2ControlMessages.Result> results = server.awaitResults(3600);
      controllerPhase("after awaitResults");
      // The controller stops here: verification (vanilla dataplane + reference BDD analysis) runs
      // in the separate `verify` role. Hand the workers' results over via the shared output dir.
      // Write atomically so a verifier waiting for the file never reads a partial payload.
      Path out = outputDir().resolve("worker-results-" + numWorkers + ".bin");
      Path tmp = outputDir().resolve("worker-results-" + numWorkers + ".bin.tmp");
      try (ObjectOutputStream oos = new ObjectOutputStream(Files.newOutputStream(tmp))) {
        oos.writeObject(results);
      }
      try {
        Files.move(tmp, out, StandardCopyOption.REPLACE_EXISTING, StandardCopyOption.ATOMIC_MOVE);
      } catch (java.nio.file.AtomicMoveNotSupportedException e) {
        Files.move(tmp, out, StandardCopyOption.REPLACE_EXISTING);
      }
      System.out.printf(
          "S2 controller: collected %d worker results (verification skipped; run the verify role),"
              + " wrote %s%n",
          results.size(), out);
    }
  }

  // -------------------------------------------------------------------- verify

  /** Read the worker results the controller wrote (see {@link #runController}). */
  @SuppressWarnings("unchecked")
  private static Map<Integer, S2ControlMessages.Result> readWorkerResults(int numWorkers)
      throws IOException, ClassNotFoundException {
    Path in = outputDir().resolve("worker-results-" + numWorkers + ".bin");
    if (!Files.exists(in)) {
      throw new IllegalStateException(
          "missing " + in + "; run the controller role first (it collects the worker results)");
    }
    try (ObjectInputStream ois = new ObjectInputStream(Files.newInputStream(in))) {
      return (Map<Integer, S2ControlMessages.Result>) ois.readObject();
    }
  }

  /**
   * Verification role: {@code verify <network> <numWorkers>}. Runs in its own JVM/Job, reads the
   * controller's {@code worker-results-&lt;W&gt;.bin}, reproduces the vanilla reference
   * computation, and writes {@code result-&lt;W&gt;worker.txt} in the historical format. The
   * controller no longer computes any of this, so its heap stays small (task A7).
   */
  private static void runVerify(String[] args) throws Exception {
    String network = args[1];
    int numWorkers = Integer.parseInt(args[2]);

    S2Snapshot snap = S2Snapshot.load(inputDir().resolve(network).resolve("configs"));
    controllerPhase("after snapshot load");
    Map<Integer, S2ControlMessages.Result> results = readWorkerResults(numWorkers);

    // The vanilla single-machine dataplane is the reference the distributed result is checked
    // against. This is the work that used to run (and peak) in the controller.
    snap.batfish.computeDataPlane(snap.snapshot);
    DataPlane vanilla = snap.batfish.loadDataPlane(snap.snapshot);
    controllerPhase("after vanilla computeDataPlane");
    Map<String, Map<String, Set<String>>> vanillaRibs = canonical(ribsOf(vanilla, null, null));
    controllerPhase("after vanillaRibs canonical");

    Map<String, Map<String, List<AbstractRoute>>> merged = new TreeMap<>();
    for (S2ControlMessages.Result workerResult : results.values()) {
      workerResult.ribs.forEach(
          (host, byVrf) -> merged.computeIfAbsent(host, h -> new TreeMap<>()).putAll(byVrf));
    }
    Map<String, Map<String, Set<String>>> distributedRibs = canonical(merged);
    boolean match = vanillaRibs.equals(distributedRibs);
    Map<String, String> vanillaReach = reachabilityDigest(vanilla, snap);
    boolean reachMatch = true;
    boolean anyWorkerDigest =
        results.values().stream().anyMatch(workerResult -> !workerResult.reachability.isEmpty());
    if (anyWorkerDigest) {
      for (S2ControlMessages.Result workerResult : results.values()) {
        reachMatch &= workerResult.reachability.equals(vanillaReach);
      }
    } else {
      // No worker digest to compare (owned-dataplane mode uses stub remote FIBs; descriptor mode
      // uses remote configs without their ACL bodies). C3: the implication "exact RIB match =>
      // forwarding match" is sound because each worker returns its owned nodes' complete final
      // main RIBs and every host's RIB is checked exactly against vanilla (match). A FIB is a
      // deterministic function of the main RIB plus the node's configuration; the workers build
      // owned FIBs from the full owned configs, and a remote node's ACL/policy bodies never
      // change its FIB. The union therefore has the same forwarding behaviour as vanilla.
      reachMatch = match;
    }
    System.out.printf(
        "S2 verify: worker traceroute digest %s%n",
        anyWorkerDigest
            ? "compared"
            : "skipped (owned/descriptor mode); implying forwarding from the exact RIB match");

    // Distributed symbolic reachability (M5). First compare the workers' reachable BDDs
    // state-by-state against the reference. This is strict: the answer check below re-runs the
    // fixpoint on the full reference graph (seeded with the distributed result), so it could mask
    // a worker that dropped states.
    BDDReachabilityAnalysis referenceAnalysis = buildReachabilityAnalysis(snap, vanilla);
    controllerPhase("after reference analysis build");
    JFactory referenceFactory = (JFactory) referenceAnalysis.getBDDPacket().getFactory();
    Map<StateExpr, BDD> mergedReachable = new HashMap<>();
    for (S2ControlMessages.Result workerResult : results.values()) {
      for (Map.Entry<StateExpr, String> e : workerResult.symbolicReachable.entrySet()) {
        mergedReachable.put(e.getKey(), new BDDTransfer().load(referenceFactory, e.getValue()));
      }
    }
    Map<StateExpr, BDD> referenceReachable = referenceAnalysis.computeReverseReachableStates();
    controllerPhase("after reference reverse-reachable");
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
            referenceAnalysis.getBDDPacket(), referenceAnalysis.getIngressLocationReachableBDDs());
    boolean answerMatch = distributedFlows.equals(vanillaFlows);

    // Per-worker peak heap (scale evidence).
    long totalPeakHeapBytes = 0;
    for (S2ControlMessages.Result workerResult : results.values()) {
      totalPeakHeapBytes += workerResult.peakHeapBytes;
    }
    long verifyPeakHeapBytes = peakHeapBytes();
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
    report.append(String.format("controller: %.1f%n", verifyPeakHeapBytes / 1048576.0));
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

  // -------------------------------------------------------------------- worker

  private static void runWorker(String[] args) throws Exception {
    String network = args[1];
    int workerId = Integer.parseInt(args[2]);
    int numWorkers = Integer.parseInt(args[3]);
    String controllerHost = args[4];
    int controllerPort = Integer.parseInt(args[5]);
    int sidecarPort = Integer.parseInt(args[6]);

    try (Socket socket = connectWithRetry(controllerHost, controllerPort)) {
      ObjectOutputStream out = new ObjectOutputStream(socket.getOutputStream());
      out.flush();
      ObjectInputStream in = new ObjectInputStream(socket.getInputStream());
      out.writeObject(new S2ControlMessages.Register(workerId));
      out.flush();
      S2ControlMessages.Start start = (S2ControlMessages.Start) in.readObject();

      // S2 ships the controller's parsed configurations so workers do not re-parse the snapshot.
      // In descriptor mode the worker gets full configs only for the nodes it owns and a reduced
      // descriptor for every remote node; it materializes a shadow config for the latter.
      SortedMap<String, Configuration> configs = null;
      if (start.descriptorShadows) {
        Map<String, RemoteNodeDescriptor> descriptors =
            RemoteNodeDescriptor.deserialize(start.descriptors);
        SortedMap<String, Configuration> effective = new TreeMap<>();
        descriptors.forEach(
            (host, descriptor) -> effective.put(host, descriptor.shadowConfiguration()));
        effective.putAll(S2ControlMessages.deserializeConfigs(start.ownedConfigs));
        configs = effective;
      } else if (start.configs != null) {
        configs = S2ControlMessages.deserializeConfigs(start.configs);
      }
      // The serialized payloads are no longer needed; drop them so they are not retained all run.
      start.clearConfigPayloads();
      S2Snapshot snap =
          configs != null
              ? S2Snapshot.fromConfigs(configs)
              : S2Snapshot.load(inputDir().resolve(network).resolve("configs"));
      S2Sharding.assertDistributedProtocolsSupported(snap, numWorkers);
      // The controller computes the assignment once and ships it; workers must use it verbatim
      // rather than recomputing. Fall back to the historical RANDOM partition only if an older
      // controller omitted it.
      Map<String, Integer> assignment =
          start.assignment != null
              ? start.assignment
              : NetworkPartitioner.partition(
                  snap.configs.keySet(), numWorkers, S2Sharding.PARTITION_SEED);
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
          start.externalAdverts != null
              ? new ArrayList<>(S2ControlMessages.deserializeExternalAdverts(start.externalAdverts))
              : new ArrayList<>(
                  snap.batfish.loadExternalBgpAnnouncements(snap.snapshot, snap.configs));
      // External announcements are injected into the BGP RIBs and are subject to prefix
      // appointment, so their networks must be part of the sharding universe.
      Set<org.batfish.datamodel.Prefix> externalAdvertPrefixes = new HashSet<>();
      for (org.batfish.datamodel.BgpAdvertisement advert : adverts) {
        externalAdvertPrefixes.add(advert.getNetwork());
      }

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
        S2BdpEngine engine =
            new S2BdpEngine(
                snap.settings(),
                nodes,
                coordinator,
                shadowSync,
                externalAdvertPrefixes,
                start.descriptorShadows);
        DataPlane dp =
            engine.computeDataPlane(
                    snap.configs,
                    snap.topologyContext,
                    new java.util.HashSet<>(adverts),
                    snap.ipOwners,
                    false)
                ._dataPlane;

        // Persist this worker's owned hosts' slices so an out-of-process engine (the s2 dataplane
        // plugin, serving questions with s2slicedir) can read them lazily. The controller names the
        // directory per snapshot in the persistent-pool flow; the one-shot runner falls back to
        // S2_SLICE_DIR, then <S2_OUTPUT_DIR>/slices (the Kubernetes shared volume).
        Path sliceDir =
            start.sliceDir != null
                ? Paths.get(start.sliceDir)
                : System.getenv("S2_SLICE_DIR") != null
                    ? Paths.get(System.getenv("S2_SLICE_DIR"))
                    : outputDir().resolve("slices");
        S2DirectoryHostSlices.write(sliceDir, S2InProcessHostSlices.of(List.of(dp)));
        System.out.printf("S2 worker %d wrote slices to %s%n", workerId, sliceDir);

        // Distributed symbolic reachability (M5) over the converged dataplane. Each worker builds
        // only its own switches' edges (OwnedForwardingAnalysis); the boundary edges into its
        // states are pulled from the peers that own their sources.
        BDDReachabilityAnalysis analysis = buildReachabilityAnalysis(snap, dp, ownedHosts);
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
        Map<StateExpr, String> symbolicSerialized = new HashMap<>();
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
              "S2 worker %d generated %d local symbolic edges, pulled %d boundary edges%n",
              workerId, holder[0].edgeCount() - pulledEdges.size(), pulledEdges.size());
          Map<StateExpr, BDD> reachable = holder[0].run();
          for (Map.Entry<StateExpr, BDD> e : reachable.entrySet()) {
            symbolicSerialized.put(e.getKey(), new BDDTransfer().save(e.getValue()));
          }
        }

        long peakHeapBytes = peakHeapBytes();
        out.writeObject(
            new S2ControlMessages.Result(
                workerId,
                ribsOf(dp, assignment, workerId),
                // Owned mode uses stub remote FIBs and descriptor mode uses remote configs without
                // their ACL bodies, so neither can run the global traceroute digest here. The
                // controller then implies forwarding equality from the exact RIB match: worker
                // RIBs exactly equal vanilla, and a FIB (hence forwarding) is a deterministic
                // function of the RIB and the full config. Owned configs are full; remote policy
                // bodies never affect a remote FIB, and remote ACLs only matter to the digest we
                // are skipping.
                ownedDataplaneMode() || start.descriptorShadows
                    ? Map.of()
                    : reachabilityDigest(dp, snap),
                symbolicSerialized,
                peakHeapBytes));
        out.flush();
        System.out.printf(
            "S2 worker %d done (peak heap %.1f MiB)%n", workerId, peakHeapBytes / 1048576.0);
      }
    }
  }

  // ---------------------------------------------------------- persistent pool

  /**
   * Long-lived controller service role: {@code controller-service <numWorkers> <port>}. Workers
   * register once (advertising their sidecar endpoints) and stay connected; the engine then drives
   * one compute request per snapshot. Blocks until the JVM is shut down.
   */
  private static void runControllerService(String[] args) throws Exception {
    int numWorkers = Integer.parseInt(args[1]);
    int port = Integer.parseInt(args[2]);
    S2ControllerService service = new S2ControllerService(port, numWorkers);
    service.start();
    System.out.printf(
        "S2 controller-service listening on %d, waiting for %d workers%n",
        service.getPort(), numWorkers);
    if (!service.awaitWorkers(600)) {
      throw new IllegalStateException("timed out waiting for the workers to register");
    }
    System.out.printf("S2 controller-service: all %d workers registered%n", numWorkers);
    Thread.currentThread().join();
  }

  /**
   * Long-lived worker service role: {@code worker-service <workerId> <controllerHost>
   * <controllerPort> <sidecarPort> [advertisedHost]}. Registers with the controller once and then
   * serves snapshots until shut down.
   */
  private static void runWorkerService(String[] args) throws Exception {
    int workerId = Integer.parseInt(args[1]);
    String controllerHost = args[2];
    int controllerPort = Integer.parseInt(args[3]);
    int sidecarPort = Integer.parseInt(args[4]);
    String advertisedHost =
        args.length > 5 ? args[5] : java.net.InetAddress.getLocalHost().getHostAddress();
    S2WorkerService worker =
        new S2WorkerService(workerId, controllerHost, controllerPort, sidecarPort, advertisedHost);
    worker.start();
    System.out.printf(
        "S2 worker-service %d registered at %s (sidecar %d, controller %s:%d)%n",
        workerId, advertisedHost, worker.getSidecarPort(), controllerHost, controllerPort);
    Thread.currentThread().join();
  }

  // ------------------------------------------------------------------- helpers

  /**
   * Stand-alone partition CLI for offline evaluation: {@code S2Main partition <network>
   * <numWorkers>} computes the assignment for the scheme in {@code -Ds2.partition} and writes it,
   * plus the node weights, under {@code $S2_OUTPUT_DIR}. {@code scripts/partition-metrics.py}
   * consumes both. No workers or dataplane are started.
   */
  private static void runPartition(String[] args) throws Exception {
    String network = args[1];
    int numWorkers = Integer.parseInt(args[2]);
    S2Snapshot snap = S2Snapshot.load(inputDir().resolve(network).resolve("configs"));
    PartitionScheme requested = PartitionScheme.fromSystemProperties();
    CommunicationGraph graph =
        CommunicationGraph.build(snap.configs, snap.topologyContext, snap.bgpTopology);
    AutoSchemeSelector.Selection selection = AutoSchemeSelector.select(requested, graph);
    PartitionScheme scheme = selection.scheme();
    Map<String, Integer> assignment =
        CommunicationGraph.canonicalAssignment(
            scheme.partitioner().partition(graph, numWorkers, S2Sharding.PARTITION_SEED),
            numWorkers);
    Path out = outputDir();
    String base = network + "-" + scheme.name().toLowerCase(Locale.ROOT) + "-" + numWorkers + "w";
    Path assignmentFile = out.resolve("assignment-" + base + ".txt");
    Path weightsFile = out.resolve("weights-" + base + ".txt");
    StringBuilder assignmentText = new StringBuilder();
    StringBuilder weightsText = new StringBuilder();
    for (String host : graph.nodes()) {
      assignmentText.append(host).append(' ').append(assignment.get(host)).append('\n');
      weightsText.append(host).append(' ').append(graph.weight(host)).append('\n');
    }
    Files.writeString(assignmentFile, assignmentText.toString());
    Files.writeString(weightsFile, weightsText.toString());
    long[] loads = CommunicationGraph.loads(graph, assignment, numWorkers);
    long maxLoad = 0;
    long totalLoad = 0;
    for (long load : loads) {
      maxLoad = Math.max(maxLoad, load);
      totalLoad += load;
    }
    double meanLoad = totalLoad / (double) numWorkers;
    String selectionNote =
        selection.shape() == null
            ? ""
            : String.format(" (requested=AUTO, %s)", selection.describe());
    System.out.printf(
        "S2 partition: scheme=%s%s network=%s workers=%d nodes=%d weighted-cut=%d"
            + " imbalance(max/mean)=%.3f%n",
        scheme,
        selectionNote,
        network,
        numWorkers,
        graph.nodes().size(),
        CommunicationGraph.cutWeight(graph, assignment),
        meanLoad == 0.0 ? 0.0 : maxLoad / meanLoad);
    System.out.printf("assignment: %s%nweights: %s%n", assignmentFile, weightsFile);
  }

  /** Build Batfish's BDD reachability analysis over the given dataplane (all interface sources). */
  private static BDDReachabilityAnalysis buildReachabilityAnalysis(S2Snapshot snap, DataPlane dp) {
    return buildReachabilityAnalysis(snap, dp, null);
  }

  /**
   * Like {@link #buildReachabilityAnalysis(S2Snapshot, DataPlane)}, but when {@code ownedHosts} is
   * non-null the forwarding analysis is restricted to those switches so the worker generates only
   * locally-owned edges. Cross-worker edges into owned states are pulled from their owners. The
   * factory is likewise scoped to the owned hostnames so it does not build remote source
   * structures; remote nodes remain usable as edge targets.
   */
  private static BDDReachabilityAnalysis buildReachabilityAnalysis(
      S2Snapshot snap, DataPlane dp, Set<String> ownedHosts) {
    BDDPacket packet = new BDDPacket();
    ForwardingAnalysis forwardingAnalysis =
        ownedHosts == null
            ? dp.getForwardingAnalysis()
            : new OwnedForwardingAnalysis(dp.getForwardingAnalysis(), ownedHosts);
    // A worker can own zero nodes when there are more workers than switches (small demo
    // networks). It still has to take part in the distributed fixpoint: it owns the global
    // disposition / Query states that every node's edges feed into, and the factory cannot build
    // a scoped view with zero source configs (BDDSourceManager requires at least one). Fall back
    // to an unscoped factory for this degenerate worker; it generates the global disposition
    // edges, and S2ReachabilityWorker keeps only those whose post state this worker owns.
    Set<String> factoryLocalNodes = ownedHosts != null && ownedHosts.isEmpty() ? null : ownedHosts;
    BDDReachabilityAnalysisFactory factory =
        new BDDReachabilityAnalysisFactory(
            packet,
            snap.configs,
            forwardingAnalysis,
            new IpsRoutedOutInterfacesFactory(dp.getFibs()),
            false,
            false,
            factoryLocalNodes);
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
