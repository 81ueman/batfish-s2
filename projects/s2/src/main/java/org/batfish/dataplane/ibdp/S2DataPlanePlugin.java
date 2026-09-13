// SPDX-License-Identifier: Apache-2.0

package org.batfish.dataplane.ibdp;

import com.google.auto.service.AutoService;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import org.batfish.common.NetworkSnapshot;
import org.batfish.common.plugin.DataPlanePlugin;
import org.batfish.common.plugin.Plugin;
import org.batfish.common.topology.TopologyProvider;
import org.batfish.datamodel.BgpAdvertisement;
import org.batfish.datamodel.Configuration;
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
 * <p><b>Current increment.</b> This first version runs a single S2 worker ({@code W=1}) with every
 * node real, so its result is the assembled global data plane and is identical to vanilla. It
 * exists to prove the engine-selection and question-answering path end to end; the work
 * distribution ({@code W>1}, merging the per-worker owned data planes) and the lazy data plane
 * follow.
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

    // The first increment runs a single worker with every node real, so the engine returns the
    // assembled global data plane. Distribution (W>1, merging owned fragments) lands next.
    Map<String, DistributedNode> nodes = new HashMap<>();
    configurations.values().forEach(c -> nodes.put(c.getHostname(), DistributedNode.real(c)));
    S2Coordinator coordinator = new S2Cluster(1);
    Set<Prefix> externalAdvertPrefixes =
        externalAdverts.stream().map(BgpAdvertisement::getNetwork).collect(Collectors.toSet());
    S2BdpEngine engine =
        new S2BdpEngine(_settings, nodes, coordinator, null, externalAdvertPrefixes);

    ComputeDataPlaneResult result =
        engine.computeDataPlane(
            configurations,
            topologyContext,
            externalAdverts,
            topologyProvider.getInitialIpOwners(snapshot),
            _batfish.debugFlagEnabled(IncrementalDataPlanePlugin.DEBUG_FLAG_RETAIN_ANNOTATED_RIBS));
    _logger.infof(
        "Generated S2 data-plane for snapshot:%s; iterations:%s",
        snapshot.getSnapshot(),
        ((IncrementalBdpAnswerElement) result._answerElement).getDependentRoutesIterations());
    return result;
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
