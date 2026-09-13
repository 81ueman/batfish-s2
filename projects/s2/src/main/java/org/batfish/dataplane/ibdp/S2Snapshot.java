package org.batfish.dataplane.ibdp;

import com.google.common.collect.ImmutableMap;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.stream.Stream;
import org.batfish.common.NetworkSnapshot;
import org.batfish.common.topology.IpOwners;
import org.batfish.common.topology.TopologyProvider;
import org.batfish.datamodel.Configuration;
import org.batfish.datamodel.NetworkConfigurations;
import org.batfish.datamodel.bgp.BgpTopology;
import org.batfish.datamodel.bgp.BgpTopologyUtils;
import org.batfish.datamodel.isis.IsisTopology;
import org.batfish.main.Batfish;
import org.batfish.main.BatfishTestUtils;
import org.batfish.main.TestrigText;
import org.junit.rules.TemporaryFolder;

/**
 * Loads a S2 snapshot (a directory of vendor configs) and the inputs the dataplane engine needs.
 *
 * <p>For this prototype we reuse Batfish's test utilities to parse; a production runner would go
 * through the snapshot/coordinator API instead.
 */
public final class S2Snapshot {

  public final Batfish batfish;
  public final NetworkSnapshot snapshot;
  public final SortedMap<String, Configuration> configs;
  public final TopologyContext topologyContext;
  public final IpOwners ipOwners;
  public final BgpTopology bgpTopology;
  public final NetworkConfigurations networkConfigurations;

  private S2Snapshot(
      Batfish batfish,
      NetworkSnapshot snapshot,
      SortedMap<String, Configuration> configs,
      TopologyContext topologyContext,
      IpOwners ipOwners,
      BgpTopology bgpTopology,
      NetworkConfigurations networkConfigurations) {
    this.batfish = batfish;
    this.snapshot = snapshot;
    this.configs = configs;
    this.topologyContext = topologyContext;
    this.ipOwners = ipOwners;
    this.bgpTopology = bgpTopology;
    this.networkConfigurations = networkConfigurations;
  }

  public static S2Snapshot load(Path configsDir) throws IOException {
    TreeMap<String, byte[]> bytes = new TreeMap<>();
    try (Stream<Path> files = Files.list(configsDir)) {
      for (Path file : files.filter(Files::isRegularFile).toList()) {
        bytes.put(file.getFileName().toString(), Files.readAllBytes(file));
      }
    }
    TemporaryFolder folder = new TemporaryFolder();
    folder.create();
    Batfish batfish =
        BatfishTestUtils.getBatfishFromTestrigText(
            TestrigText.builder().setConfigurationBytes(bytes).build(), folder);
    NetworkSnapshot snapshot = batfish.getSnapshot();
    SortedMap<String, Configuration> configs = batfish.loadConfigurations(snapshot);
    return fromBatfish(batfish, configs);
  }

  /**
   * Like {@link #load}, but from already-parsed configurations (S2 ships the controller's parsed
   * configs to workers so they do not re-parse the snapshot). {@code BatfishTestUtils.getBatfish}
   * installs the configs directly, so topology is computed without ANTLR.
   */
  public static S2Snapshot fromConfigs(SortedMap<String, Configuration> configs)
      throws IOException {
    TemporaryFolder folder = new TemporaryFolder();
    folder.create();
    Batfish batfish = BatfishTestUtils.getBatfish(configs, folder);
    return fromBatfish(batfish, configs);
  }

  private static S2Snapshot fromBatfish(Batfish batfish, SortedMap<String, Configuration> configs) {
    NetworkSnapshot snapshot = batfish.getSnapshot();
    TopologyProvider tp = batfish.getTopologyProvider();
    TopologyContext topologyContext =
        TopologyContext.builder()
            .setIpsecTopology(tp.getInitialIpsecTopology(snapshot))
            .setIsisTopology(
                IsisTopology.initIsisTopology(configs, tp.getInitialLayer3Topology(snapshot)))
            .setLayer3Topology(tp.getInitialLayer3Topology(snapshot))
            .setLayer1Topologies(tp.getLayer1Topologies(snapshot))
            .setL3Adjacencies(tp.getInitialL3Adjacencies(snapshot))
            .setOspfTopology(tp.getInitialOspfTopology(snapshot))
            .setTunnelTopology(tp.getInitialTunnelTopology(snapshot))
            .build();
    IpOwners ipOwners = tp.getInitialIpOwners(snapshot);
    // Compute BGP topology without dataplane reachability checks, matching the S2 engine.
    BgpTopology bgpTopology =
        BgpTopologyUtils.initBgpTopology(
            configs,
            ipOwners.getIpVrfOwners(),
            false,
            false,
            null,
            ImmutableMap.of(),
            topologyContext.getL3Adjacencies());
    return new S2Snapshot(
        batfish,
        snapshot,
        configs,
        topologyContext,
        ipOwners,
        bgpTopology,
        NetworkConfigurations.of(configs));
  }

  public IncrementalDataPlaneSettings settings() {
    return new IncrementalDataPlaneSettings(batfish.getSettingsConfiguration());
  }
}
