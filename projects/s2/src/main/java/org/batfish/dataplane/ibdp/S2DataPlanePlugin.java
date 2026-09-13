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
import org.batfish.datamodel.BgpAdvertisement;
import org.batfish.datamodel.Configuration;
import org.batfish.datamodel.DataPlane;
import org.batfish.datamodel.Prefix;
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
 * and shadows the rest), the workers run concurrently, and their owned data planes are merged into
 * the global one. This is the naive, fully-materialized assembly; a lazy per-node data plane
 * replaces it when the global result does not fit in one JVM, and the worker pool will be able to
 * be remote instead of in-process.
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

    int numWorkers = Math.min(numWorkers(), Math.max(1, configurations.size()));
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

  /** Run the workers concurrently and merge their owned, global-assembled data planes. */
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
      return new ComputeDataPlaneResult(
          first._answerElement, S2MergedDataPlane.of(dataPlanes), first._topologies);
    } catch (InterruptedException | ExecutionException e) {
      Thread.currentThread().interrupt();
      throw new IllegalStateException("S2 distributed data plane computation failed", e);
    } finally {
      pool.shutdownNow();
    }
  }

  /** The requested number of workers (the {@link Settings#ARG_S2_WORKERS} setting, default 1). */
  private int numWorkers() {
    try {
      return Math.max(
          1,
          _batfish
              .getSettingsConfiguration()
              .getInt(org.batfish.config.Settings.ARG_S2_WORKERS, 1));
    } catch (RuntimeException e) {
      return 1;
    }
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
