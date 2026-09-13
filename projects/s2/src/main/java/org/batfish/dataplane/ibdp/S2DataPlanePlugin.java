// SPDX-License-Identifier: Apache-2.0

package org.batfish.dataplane.ibdp;

import com.google.auto.service.AutoService;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;
import org.apache.commons.configuration2.ImmutableConfiguration;
import org.batfish.common.NetworkSnapshot;
import org.batfish.common.plugin.DataPlanePlugin;
import org.batfish.common.plugin.Plugin;
import org.batfish.common.topology.TopologyProvider;
import org.batfish.config.Settings;
import org.batfish.datamodel.BgpAdvertisement;
import org.batfish.datamodel.Configuration;
import org.batfish.datamodel.DataPlane;
import org.batfish.datamodel.Prefix;
import org.batfish.datamodel.Vrf;
import org.batfish.datamodel.answers.IncrementalBdpAnswerElement;
import org.batfish.datamodel.isis.IsisTopology;

/**
 * Batfish dataplane engine that computes the data plane with the S2 distributed engine.
 *
 * <p>Registering this as a plugin makes S2 selectable exactly like the stock {@code ibdp} engine:
 * run Batfish with {@code -dataplaneengine s2} (or set it in {@code batfish.properties}) and the
 * normal question engine, REST API, and pybatfish work unchanged. This is the S2 equivalent of
 * {@link IncrementalDataPlanePlugin}; the stock engine remains the default, so vanilla Batfish is
 * unaffected unless this engine is explicitly selected.
 *
 * <p>The snapshot is partitioned across {@code N} in-process workers (each owns a subset of nodes
 * and shadows the rest), the workers run concurrently, and their owned data planes are assembled
 * into the global one lazily ({@link S2LazyDataPlane}): node-local access is served from the owning
 * worker and whole-network iteration falls back to a materialized union. This is what lets a data
 * plane that does not fit in one JVM be answered once the workers are remote (Kubernetes);
 * in-process it bounds the container memory.
 */
@AutoService(Plugin.class)
public final class S2DataPlanePlugin extends DataPlanePlugin {

  public static final String PLUGIN_NAME = "s2";

  private IncrementalDataPlaneSettings _settings;

  public S2DataPlanePlugin() {}

  @Override
  public ComputeDataPlaneResult computeDataPlane(NetworkSnapshot snapshot) {
    ImmutableConfiguration settings = _batfish.getSettingsConfiguration();
    String sliceDir = settings.getString(Settings.ARG_S2_SLICE_DIR, "");
    String controllerHost = settings.getString(Settings.ARG_S2_CONTROLLER_HOST, "");
    int controllerPort = settings.getInt(Settings.ARG_S2_CONTROLLER_PORT, 0);
    Map<String, Configuration> configurations = _batfish.loadConfigurations(snapshot);
    TopologyProvider topologyProvider = _batfish.getTopologyProvider();
    TopologyContext topologyContext =
        buildTopologyContext(snapshot, configurations, topologyProvider);
    Set<BgpAdvertisement> externalAdverts =
        _batfish.loadExternalBgpAnnouncements(snapshot, configurations);
    if (!controllerHost.isEmpty()) {
      if (!distributedProtocolsSupported(configurations)) {
        // EIGRP/IS-IS/RIP are not distributed; the pool cannot compute this snapshot. Fall back to
        // the in-process single-worker path below (equivalent to the stock engine).
        _logger.warn(
            "S2: snapshot uses EIGRP/IS-IS/RIP, which the worker pool cannot distribute;"
                + " ignoring s2controllerhost and computing in-process with 1 worker");
      } else {
        return computeViaController(
            snapshot,
            configurations,
            externalAdverts,
            topologyContext,
            controllerHost,
            controllerPort,
            sliceDir);
      }
    }
    if (!sliceDir.isEmpty()) {
      // An out-of-process S2 pool already produced the per-host slices (Kubernetes shared storage);
      // serve questions from them lazily instead of computing in-process.
      return computeFromSlices(topologyContext, Paths.get(sliceDir));
    }

    int numWorkers = resolveNumWorkers(configurations);
    Map<String, Integer> assignment =
        NetworkPartitioner.partition(configurations.keySet(), numWorkers, 0L);
    Map<String, DistributedNode> realByHost = new HashMap<>();
    configurations.values().forEach(c -> realByHost.put(c.getHostname(), DistributedNode.real(c)));

    S2Coordinator coordinator = new S2Cluster(numWorkers);
    Set<Prefix> externalAdvertPrefixes =
        externalAdverts.stream().map(BgpAdvertisement::getNetwork).collect(Collectors.toSet());
    List<S2BdpEngine> engines = new ArrayList<>();
    for (int w = 0; w < numWorkers; w++) {
      Map<String, DistributedNode> nodes = new HashMap<>();
      for (String host : configurations.keySet()) {
        nodes.put(
            host,
            assignment.get(host) == w
                ? realByHost.get(host)
                : DistributedNode.shadowOf(realByHost.get(host)));
      }
      engines.add(new S2BdpEngine(_settings, nodes, coordinator, null, externalAdvertPrefixes));
    }

    ComputeDataPlaneResult result =
        runDistributed(
            engines,
            configurations,
            topologyContext,
            externalAdverts,
            topologyProvider.getInitialIpOwners(snapshot));
    _logger.infof(
        "Generated S2 data-plane for snapshot:%s (workers=%d); iterations:%s",
        snapshot.getSnapshot(),
        numWorkers,
        ((IncrementalBdpAnswerElement) result._answerElement).getDependentRoutesIterations());
    return result;
  }

  /** Build the topology context the S2 engine needs, as {@link IncrementalDataPlanePlugin} does. */
  private static TopologyContext buildTopologyContext(
      NetworkSnapshot snapshot,
      Map<String, Configuration> configurations,
      TopologyProvider topologyProvider) {
    return TopologyContext.builder()
        .setIpsecTopology(topologyProvider.getInitialIpsecTopology(snapshot))
        .setIsisTopology(
            IsisTopology.initIsisTopology(
                configurations, topologyProvider.getInitialLayer3Topology(snapshot)))
        .setLayer3Topology(topologyProvider.getInitialLayer3Topology(snapshot))
        .setLayer1Topologies(topologyProvider.getLayer1Topologies(snapshot))
        .setL3Adjacencies(topologyProvider.getInitialL3Adjacencies(snapshot))
        .setOspfTopology(topologyProvider.getInitialOspfTopology(snapshot))
        .setTunnelTopology(topologyProvider.getInitialTunnelTopology(snapshot))
        .build();
  }

  /** Serve the data plane lazily from per-host slices written by an out-of-process S2 pool. */
  private ComputeDataPlaneResult computeFromSlices(TopologyContext topologyContext, Path sliceDir) {
    S2HostSlices slices;
    try {
      slices = S2DirectoryHostSlices.read(sliceDir);
    } catch (IOException e) {
      throw new UncheckedIOException("Failed to read S2 host slices from " + sliceDir, e);
    }
    _logger.infof(
        "S2: serving the data plane from %d host slices under %s", slices.hosts().size(), sliceDir);
    return new ComputeDataPlaneResult(
        new IncrementalBdpAnswerElement(), S2LazyDataPlane.of(slices), topologyContext);
  }

  /** Per-snapshot slice directories this JVM created, deleted on shutdown (slice GC). */
  private static final Set<Path> _sliceDirsForGc = ConcurrentHashMap.newKeySet();

  private static final AtomicBoolean _sliceGcHookRegistered = new AtomicBoolean();

  /**
   * Drive the persistent controller service (choice A): ship this snapshot to the pool, wait for
   * the workers to write their owned hosts' slices, then serve questions lazily from those slices.
   * The per-snapshot directory is registered for deletion on JVM shutdown so a shared volume does
   * not accumulate one directory per snapshot.
   */
  private ComputeDataPlaneResult computeViaController(
      NetworkSnapshot snapshot,
      Map<String, Configuration> configurations,
      Set<BgpAdvertisement> externalAdverts,
      TopologyContext topologyContext,
      String controllerHost,
      int controllerPort,
      String baseSliceDir) {
    if (controllerPort <= 0) {
      throw new IllegalStateException(
          "s2controllerhost is set but s2controllerport is not a valid port: " + controllerPort);
    }
    Path sliceDir;
    try {
      Path base =
          baseSliceDir.isEmpty()
              ? Files.createTempDirectory("s2-slices-")
              : Files.createDirectories(Paths.get(baseSliceDir));
      sliceDir = Files.createTempDirectory(base, "snapshot-");
    } catch (IOException e) {
      throw new UncheckedIOException("Failed to create the S2 slice directory", e);
    }
    registerSliceGc(sliceDir);
    try {
      S2ControllerClient client = new S2ControllerClient(controllerHost, controllerPort);
      S2ControlMessages.ComputeRequest request =
          new S2ControlMessages.ComputeRequest(
              S2ControlMessages.serializeConfigs(configurations),
              S2ControlMessages.serializeExternalAdverts(externalAdverts),
              sliceDir.toString(),
              snapshot.getSnapshot().getId());
      S2ControlMessages.ComputeResponse response = client.compute(request);
      if (!response.ok) {
        throw new IllegalStateException(
            "S2 controller failed for "
                + snapshot.getSnapshot()
                + " ("
                + controllerHost
                + ":"
                + controllerPort
                + "): "
                + response.message);
      }
      Path resultDir = response.sliceDir == null ? sliceDir : Paths.get(response.sliceDir);
      _logger.infof(
          "S2: controller %s:%d computed snapshot %s on %d workers; slices at %s",
          controllerHost, controllerPort, snapshot.getSnapshot(), response.numWorkers, resultDir);
      return computeFromSlices(topologyContext, resultDir);
    } catch (IOException | ClassNotFoundException e) {
      throw new IllegalStateException(
          "S2 controller request to " + controllerHost + ":" + controllerPort + " failed", e);
    }
  }

  private static void registerSliceGc(Path sliceDir) {
    if (_sliceGcHookRegistered.compareAndSet(false, true)) {
      Runtime.getRuntime()
          .addShutdownHook(new Thread(S2DataPlanePlugin::deleteSliceDirs, "s2-slice-gc"));
    }
    _sliceDirsForGc.add(sliceDir);
  }

  private static void deleteSliceDirs() {
    for (Path dir : _sliceDirsForGc) {
      try {
        S2DirectoryHostSlices.deleteRecursively(dir);
      } catch (IOException e) {
        // Best effort: the directory may be on a read-only or already-unmounted volume.
        System.err.printf("S2: failed to delete slice directory %s: %s%n", dir, e);
      }
    }
  }

  /** Run the workers concurrently and assemble their owned data planes lazily. */
  private ComputeDataPlaneResult runDistributed(
      List<S2BdpEngine> engines,
      Map<String, Configuration> configurations,
      TopologyContext topologyContext,
      Set<BgpAdvertisement> externalAdverts,
      org.batfish.common.topology.IpOwners ipOwners) {
    boolean retainAnnotated =
        _batfish.debugFlagEnabled(IncrementalDataPlanePlugin.DEBUG_FLAG_RETAIN_ANNOTATED_RIBS);
    ExecutorService pool = Executors.newFixedThreadPool(engines.size());
    try {
      List<Future<ComputeDataPlaneResult>> futures = new ArrayList<>();
      for (S2BdpEngine engine : engines) {
        futures.add(
            pool.submit(
                () ->
                    engine.computeDataPlane(
                        configurations,
                        topologyContext,
                        externalAdverts,
                        ipOwners,
                        retainAnnotated)));
      }
      List<ComputeDataPlaneResult> results = new ArrayList<>();
      for (Future<ComputeDataPlaneResult> future : futures) {
        results.add(future.get());
      }
      ComputeDataPlaneResult first = results.get(0);
      List<DataPlane> dataPlanes =
          results.stream().map(r -> r._dataPlane).collect(Collectors.toList());
      // In-process workers today; the remote worker pool swaps this for a shared-storage source.
      return new ComputeDataPlaneResult(
          first._answerElement,
          S2LazyDataPlane.of(S2InProcessHostSlices.of(dataPlanes)),
          first._topologies);
    } catch (InterruptedException | ExecutionException e) {
      Thread.currentThread().interrupt();
      throw new IllegalStateException("S2 distributed data plane computation failed", e);
    } finally {
      pool.shutdownNow();
    }
  }

  /**
   * Resolve the number of workers. An explicit {@code s2workers} value is honored; {@code 0} (the
   * default) means auto: the pool size when a remote pool is configured (future), else {@code
   * availableProcessors} capped by the node count. Multi-worker distribution supports only eBGP and
   * OSPF, so a snapshot that uses EIGRP/IS-IS/RIP falls back to a single worker (which is
   * equivalent to the stock engine).
   */
  private int resolveNumWorkers(Map<String, Configuration> configurations) {
    int nodes = Math.max(1, configurations.size());
    int requested = _batfish.getSettingsConfiguration().getInt(Settings.ARG_S2_WORKERS, 0);
    int numWorkers =
        requested > 0 ? requested : Math.min(Runtime.getRuntime().availableProcessors(), nodes);
    numWorkers = Math.max(1, Math.min(numWorkers, nodes));
    if (numWorkers > 1 && !distributedProtocolsSupported(configurations)) {
      _logger.warn(
          "S2: snapshot uses EIGRP/IS-IS/RIP, which are not distributed; falling back to 1 worker");
      numWorkers = 1;
    }
    return numWorkers;
  }

  /** Whether every VRF's routing can be distributed (only eBGP and OSPF are supported). */
  private static boolean distributedProtocolsSupported(Map<String, Configuration> configurations) {
    for (Configuration c : configurations.values()) {
      for (Vrf vrf : c.getVrfs().values()) {
        if (!vrf.getEigrpProcesses().isEmpty()
            || vrf.getIsisProcess() != null
            || vrf.getRipProcess() != null) {
          return false;
        }
      }
    }
    return true;
  }

  @Override
  protected void dataPlanePluginInitialize() {
    _settings = new IncrementalDataPlaneSettings(_batfish.getSettingsConfiguration());
  }

  @Override
  public String getName() {
    return PLUGIN_NAME;
  }
}
