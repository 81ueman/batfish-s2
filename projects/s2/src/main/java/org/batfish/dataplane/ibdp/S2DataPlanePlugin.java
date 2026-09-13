// SPDX-License-Identifier: Apache-2.0

package org.batfish.dataplane.ibdp;

import com.google.auto.service.AutoService;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.stream.Collectors;
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
    Map<String, Configuration> configurations = _batfish.loadConfigurations(snapshot);
    Set<BgpAdvertisement> externalAdverts =
        _batfish.loadExternalBgpAnnouncements(snapshot, configurations);

    TopologyProvider topologyProvider = _batfish.getTopologyProvider();
    TopologyContext topologyContext =
        TopologyContext.builder()
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
