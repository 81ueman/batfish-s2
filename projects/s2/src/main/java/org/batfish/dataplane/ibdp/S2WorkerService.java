// SPDX-License-Identifier: Apache-2.0

package org.batfish.dataplane.ibdp;

import java.io.EOFException;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.Socket;
import java.net.SocketException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.concurrent.atomic.AtomicReference;
import org.batfish.datamodel.Configuration;
import org.batfish.datamodel.DataPlane;

/**
 * Long-lived worker service for the persistent S2 worker pool (choice A).
 *
 * <p>Registers with the controller once (advertising its route-sidecar endpoint), then loops: read
 * a {@link S2ControlMessages.Start} for one snapshot, run this worker's shard of the existing
 * distributed fixpoint (the same code path as the one-shot {@code S2Main worker}), write its owned
 * hosts' per-host slices to the directory the controller names, and report {@link
 * S2ControlMessages.Done}. It stays alive for the next snapshot instead of exiting.
 *
 * <p>The route sidecar is bound once and its handler is swapped per snapshot, so peers always find
 * a stable endpoint. The symbolic-BDD phase of the one-shot runner is skipped: the engine serves
 * questions from the slices, and the verify role is a runner concern.
 */
public final class S2WorkerService implements AutoCloseable {

  private final int _workerId;
  private final String _controllerHost;
  private final int _controllerPort;
  private final String _advertisedHost;
  private final S2SidecarServer _sidecar;

  private volatile Socket _socket;
  private volatile ObjectOutputStream _out;
  private volatile ObjectInputStream _in;
  private volatile boolean _closed;
  private volatile Thread _loopThread;

  public S2WorkerService(
      int workerId,
      String controllerHost,
      int controllerPort,
      int sidecarPort,
      String advertisedHost)
      throws IOException {
    _workerId = workerId;
    _controllerHost = controllerHost;
    _controllerPort = controllerPort;
    _advertisedHost = advertisedHost;
    _sidecar = new S2SidecarServer(sidecarPort, noSnapshotHandler());
    _sidecar.start();
  }

  public int getWorkerId() {
    return _workerId;
  }

  /** The actual route-sidecar port (meaningful when the configured port was 0/ephemeral). */
  public int getSidecarPort() {
    return _sidecar.getPort();
  }

  /**
   * Connect to the controller, register, and start serving snapshot requests in a daemon thread.
   * Synchronous registration means a caller can wait for the pool to be complete.
   */
  public void start() throws IOException {
    Socket socket = connectWithRetry(_controllerHost, _controllerPort);
    _socket = socket;
    _out = new ObjectOutputStream(socket.getOutputStream());
    _out.flush();
    _in = new ObjectInputStream(socket.getInputStream());
    _out.writeObject(
        new S2ControlMessages.Register(
            _workerId, new S2WorkerEndpoint(_advertisedHost, _sidecar.getPort())));
    _out.flush();
    Thread thread = new Thread(this::loop, "s2-worker-service-" + _workerId);
    thread.setDaemon(true);
    _loopThread = thread;
    thread.start();
  }

  private void loop() {
    while (!_closed) {
      Object message;
      try {
        message = _in.readObject();
      } catch (EOFException | SocketException e) {
        break;
      } catch (IOException | ClassNotFoundException e) {
        if (!_closed) {
          System.err.printf("S2 worker-service %d read failed: %s%n", _workerId, e);
        }
        break;
      }
      if (message instanceof S2ControlMessages.Shutdown) {
        break;
      }
      if (!(message instanceof S2ControlMessages.Start)) {
        System.err.printf(
            "S2 worker-service %d: ignoring unexpected message %s%n",
            _workerId, message.getClass());
        continue;
      }
      S2ControlMessages.Start start = (S2ControlMessages.Start) message;
      try {
        runSnapshot(start);
        _out.writeObject(new S2ControlMessages.Done(start.runId, _workerId, peakHeapBytes()));
        _out.flush();
      } catch (Throwable t) {
        t.printStackTrace();
        try {
          _out.writeObject(new S2ControlMessages.WorkerError(start.runId, _workerId, t.toString()));
          _out.flush();
        } catch (IOException e) {
          break;
        }
      } finally {
        // Release the snapshot's node graph; the next snapshot builds its own.
        _sidecar.setHandler(noSnapshotHandler());
      }
    }
  }

  private void runSnapshot(S2ControlMessages.Start start) throws Exception {
    SortedMap<String, Configuration> configs = configsFrom(start);
    S2Snapshot snap = S2Snapshot.fromConfigs(configs);
    int numWorkers = start.endpoints.size();
    S2Sharding.assertDistributedProtocolsSupported(snap, numWorkers);
    Map<String, Integer> assignment =
        start.assignment != null
            ? start.assignment
            : NetworkPartitioner.partition(
                snap.configs.keySet(), numWorkers, S2Sharding.PARTITION_SEED);
    Set<String> ownedHosts = new HashSet<>();
    assignment.forEach(
        (host, owner) -> {
          if (owner == _workerId) {
            ownedHosts.add(host);
          }
        });

    Map<String, DistributedNode> nodes = new HashMap<>();
    for (String host : snap.configs.keySet()) {
      nodes.put(
          host,
          assignment.get(host) == _workerId
              ? DistributedNode.real(snap.configs.get(host))
              : DistributedNode.shadow(snap.configs.get(host)));
    }
    Map<String, Node> nodeMap = new HashMap<>(nodes);

    java.util.List<org.batfish.datamodel.BgpAdvertisement> adverts =
        start.externalAdverts != null
            ? new ArrayList<>(S2ControlMessages.deserializeExternalAdverts(start.externalAdverts))
            : new ArrayList<>(
                snap.batfish.loadExternalBgpAnnouncements(snap.snapshot, snap.configs));
    Set<org.batfish.datamodel.Prefix> externalAdvertPrefixes = new HashSet<>();
    for (org.batfish.datamodel.BgpAdvertisement advert : adverts) {
      externalAdvertPrefixes.add(advert.getNetwork());
    }
    // Payloads are no longer needed; drop the serialized copies for the rest of the snapshot.
    start.clearConfigPayloads();

    AtomicReference<org.batfish.bddreachability.BDDReachabilityAnalysis> unusedAnalysis =
        new AtomicReference<>();
    _sidecar.setHandler(
        S2SidecarHandlers.forWorker(
            nodeMap,
            snap.bgpTopology,
            snap.networkConfigurations,
            ownedHosts,
            unusedAnalysis::get));

    S2SidecarClient client = new S2SidecarClient();
    for (String host : snap.configs.keySet()) {
      if (assignment.get(host) != _workerId) {
        DistributedNode shadow = nodes.get(host);
        S2WorkerEndpoint owner = start.endpoints.get(assignment.get(host));
        shadow.installRemoteBgpProviders(client, owner);
        shadow.installRemoteOspfProviders(client, owner, snap.topologyContext.getOspfTopology());
      }
    }

    S2RemoteCoordinator coordinator = new S2RemoteCoordinator(_out, _in, start.runId);
    ShadowMainRibSync shadowSync =
        new ShadowMainRibSync(nodes, assignment, _workerId, start.endpoints, client);
    // The pool's slices are served to questions that consult the forwarding analysis. A remote
    // shadow's FIB is a stub, so owned-only mode would give an ARP/forwarding view that differs
    // from stock Batfish; the worker service therefore defaults to the forwarding-exact full
    // dataplane (each worker pulls the remote RIBs via the shadow sync). -Ds2.ownedDataplane=true
    // restores owned-only mode for scale experiments that only inspect RIBs/FIBs.
    boolean ownedDataplane = Boolean.parseBoolean(System.getProperty("s2.ownedDataplane", "false"));
    S2BdpEngine engine =
        new S2BdpEngine(
            snap.settings(),
            nodes,
            coordinator,
            shadowSync,
            externalAdvertPrefixes,
            start.descriptorShadows,
            ownedDataplane);
    DataPlane dp =
        engine.computeDataPlane(
                snap.configs, snap.topologyContext, new HashSet<>(adverts), snap.ipOwners, false)
            ._dataPlane;

    Path sliceDir = start.sliceDir != null ? Paths.get(start.sliceDir) : defaultSliceDir();
    S2DirectoryHostSlices.write(
        sliceDir, S2InProcessHostSlices.of(java.util.List.of(dp), ownedHosts));
    System.out.printf(
        "S2 worker-service %d: snapshot %d wrote slices for %d owned hosts to %s%n",
        _workerId, start.runId, ownedHosts.size(), sliceDir);
  }

  private static SortedMap<String, Configuration> configsFrom(S2ControlMessages.Start start)
      throws Exception {
    if (start.descriptorShadows) {
      Map<String, RemoteNodeDescriptor> descriptors =
          RemoteNodeDescriptor.deserialize(start.descriptors);
      SortedMap<String, Configuration> effective = new TreeMap<>();
      descriptors.forEach(
          (host, descriptor) -> effective.put(host, descriptor.shadowConfiguration()));
      effective.putAll(S2ControlMessages.deserializeConfigs(start.ownedConfigs));
      return effective;
    }
    if (start.configs == null) {
      throw new IllegalStateException(
          "worker-service requires the controller to ship configs (s2.noShipConfigs unsupported)");
    }
    return S2ControlMessages.deserializeConfigs(start.configs);
  }

  private static Path defaultSliceDir() throws IOException {
    String sliceDirEnv = System.getenv("S2_SLICE_DIR");
    if (sliceDirEnv != null) {
      return Paths.get(sliceDirEnv);
    }
    Path dir =
        Paths.get(System.getenv().getOrDefault("S2_OUTPUT_DIR", "/s2/outputs")).resolve("slices");
    java.nio.file.Files.createDirectories(dir);
    return dir;
  }

  private static S2SidecarServer.Handler noSnapshotHandler() {
    return request -> {
      throw new IllegalStateException("no snapshot is running on this worker");
    };
  }

  private static long peakHeapBytes() {
    long total = 0;
    for (java.lang.management.MemoryPoolMXBean pool :
        java.lang.management.ManagementFactory.getMemoryPoolMXBeans()) {
      if (pool.getType() == java.lang.management.MemoryType.HEAP) {
        java.lang.management.MemoryUsage peak = pool.getPeakUsage();
        if (peak != null) {
          total += peak.getUsed();
        }
      }
    }
    return total;
  }

  private static Socket connectWithRetry(String host, int port) throws IOException {
    IOException last = null;
    for (int attempt = 1; attempt <= 120; attempt++) {
      try {
        return new Socket(host, port);
      } catch (IOException e) {
        last = e;
        try {
          Thread.sleep(1000);
        } catch (InterruptedException ie) {
          Thread.currentThread().interrupt();
          throw new IOException("interrupted connecting to controller " + host + ":" + port, ie);
        }
      }
    }
    throw new IOException("could not connect to controller " + host + ":" + port, last);
  }

  @Override
  public void close() {
    _closed = true;
    Thread loopThread = _loopThread;
    if (loopThread != null) {
      loopThread.interrupt();
    }
    Socket socket = _socket;
    if (socket != null) {
      try {
        socket.close();
      } catch (IOException e) {
        // ignore
      }
    }
    _sidecar.close();
  }
}
