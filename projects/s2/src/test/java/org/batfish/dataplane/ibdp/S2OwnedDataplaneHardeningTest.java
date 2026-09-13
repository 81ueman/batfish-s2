// SPDX-License-Identifier: Apache-2.0
package org.batfish.dataplane.ibdp;

import static org.batfish.datamodel.tracking.TrackMethods.reachability;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import java.util.HashMap;
import java.util.Map;
import org.batfish.common.NetworkSnapshot;
import org.batfish.common.topology.TunnelTopology;
import org.batfish.datamodel.BumTransportMethod;
import org.batfish.datamodel.Configuration;
import org.batfish.datamodel.ConfigurationFormat;
import org.batfish.datamodel.Ip;
import org.batfish.datamodel.Vrf;
import org.batfish.datamodel.collections.NodeInterfacePair;
import org.batfish.datamodel.tracking.TrackReachability;
import org.batfish.datamodel.vxlan.Layer2Vni;
import org.batfish.main.Batfish;
import org.batfish.main.BatfishTestUtils;
import org.batfish.main.TestrigText;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

/**
 * Focused tests for the owned-only dataplane hardening ({@code -Ds2.ownedDataplane}): the S2 engine
 * must turn owned mode off (rather than evaluate against stub remote FIBs) when the snapshot needs
 * complete remote FIBs — a {@link TrackReachability} on a remote node, a VXLAN VNI, or a non-empty
 * IPsec/tunnel topology.
 */
public class S2OwnedDataplaneHardeningTest {

  private static final String R1 = "r1";
  private static final String R2 = "r2";
  private static final String TRACK_TESTRIG = "org/batfish/dataplane/testrigs/s2-track";
  private static final ImmutableList<String> TRACK_CONFIGS = ImmutableList.of("r1", "r2");

  @Rule public TemporaryFolder _folder = new TemporaryFolder();

  /**
   * Guard against a vacuous end-to-end fallback test: the {@code s2-track} testrig must actually
   * convert to a {@link TrackReachability} tracking group.
   */
  @Test
  public void testTrackTestrigHasTrackReachability() throws Exception {
    Batfish batfish =
        BatfishTestUtils.getBatfishFromTestrigText(
            TestrigText.builder().setConfigurationFiles(TRACK_TESTRIG, TRACK_CONFIGS).build(),
            _folder);
    NetworkSnapshot snapshot = batfish.getSnapshot();
    Map<String, Configuration> configs = batfish.loadConfigurations(snapshot);
    boolean anyTrackReachability =
        configs.values().stream().anyMatch(S2BdpEngine::hasTrackReachability);
    assertThat("s2-track must define a TrackReachability", anyTrackReachability, is(true));
  }

  private static Configuration configWithTrack(String hostname) {
    Configuration c =
        Configuration.builder()
            .setHostname(hostname)
            .setConfigurationFormat(ConfigurationFormat.CISCO_IOS)
            .build();
    Vrf.builder().setName(Configuration.DEFAULT_VRF_NAME).setOwner(c).build();
    TrackReachability track =
        (TrackReachability) reachability(Ip.parse("10.0.12.2"), Configuration.DEFAULT_VRF_NAME);
    c.setTrackingGroups(ImmutableMap.of("1", track));
    return c;
  }

  private static Configuration plainConfig(String hostname) {
    Configuration c =
        Configuration.builder()
            .setHostname(hostname)
            .setConfigurationFormat(ConfigurationFormat.CISCO_IOS)
            .build();
    Vrf.builder().setName(Configuration.DEFAULT_VRF_NAME).setOwner(c).build();
    return c;
  }

  /** Build an engine with the given nodes; owned mode is requested via the system property. */
  private static S2BdpEngine engine(Map<String, DistributedNode> nodes) {
    return new S2BdpEngine(
        new IncrementalDataPlaneSettings(), nodes, new S2Cluster(2), null, ImmutableSet.of());
  }

  @Test
  public void testTrackOnRemoteShadowDisablesOwnedMode() {
    Configuration r1 = configWithTrack(R1);
    Configuration r2 = plainConfig(R2);
    Map<String, DistributedNode> nodes = new HashMap<>();
    // r1 (with the track) is remote on this worker: its FIB would only be a stub.
    nodes.put(R1, DistributedNode.shadow(r1));
    nodes.put(R2, DistributedNode.real(r2));

    System.setProperty("s2.ownedDataplane", "true");
    try {
      S2BdpEngine e = engine(nodes);
      e.prepareDataPlane(ImmutableMap.of(R1, r1, R2, r2), TopologyContext.builder().build());
      assertThat(
          "owned mode must be off when a remote node needs a full FIB for a track",
          e.canUseTracerouteForDataplaneTopologyPruning(),
          is(true));
      assertThat(
          "track evaluation must be allowed after the fallback",
          e.hasCompleteFibForTrackReachability(R1),
          is(true));
    } finally {
      System.clearProperty("s2.ownedDataplane");
    }
  }

  /**
   * The track fallback is snapshot-wide: even when the tracked node is owned on this worker, owned
   * mode is disabled so that all workers agree (they must also agree on the dataplane-level BGP
   * session reachability check, which owned mode skips).
   */
  @Test
  public void testTrackOnOwnedNodeStillDisablesOwnedMode() {
    Configuration r1 = configWithTrack(R1);
    Configuration r2 = plainConfig(R2);
    Map<String, DistributedNode> nodes = new HashMap<>();
    nodes.put(R1, DistributedNode.real(r1));
    nodes.put(R2, DistributedNode.shadow(r2));

    System.setProperty("s2.ownedDataplane", "true");
    try {
      S2BdpEngine e = engine(nodes);
      e.prepareDataPlane(ImmutableMap.of(R1, r1, R2, r2), TopologyContext.builder().build());
      assertThat(
          "owned mode is disabled snapshot-wide when any node has a TrackReachability",
          e.canUseTracerouteForDataplaneTopologyPruning(),
          is(true));
      assertThat(e.hasCompleteFibForTrackReachability(R1), is(true));
      assertThat(e.hasCompleteFibForTrackReachability(R2), is(true));
    } finally {
      System.clearProperty("s2.ownedDataplane");
    }
  }

  /**
   * O1: owned mode is on by default (no system property set), and remote shadows therefore have
   * stub FIBs.
   */
  @Test
  public void testOwnedModeIsDefaultOn() {
    Configuration r1 = plainConfig(R1);
    Configuration r2 = plainConfig(R2);
    Map<String, DistributedNode> nodes = new HashMap<>();
    nodes.put(R1, DistributedNode.real(r1));
    nodes.put(R2, DistributedNode.shadow(r2));

    System.clearProperty("s2.ownedDataplane");
    S2BdpEngine e = engine(nodes);
    e.prepareDataPlane(ImmutableMap.of(R1, r1, R2, r2), TopologyContext.builder().build());
    assertThat(
        "owned mode is on by default", e.canUseTracerouteForDataplaneTopologyPruning(), is(false));
    assertThat(
        "a remote node's FIB is a stub by default",
        e.hasCompleteFibForTrackReachability(R2),
        is(false));
  }

  /** {@code -Ds2.ownedDataplane=false} restores the pre-O1 full dataplane. */
  @Test
  public void testOwnedModeCanBeDisabled() {
    Configuration r1 = plainConfig(R1);
    Configuration r2 = plainConfig(R2);
    Map<String, DistributedNode> nodes = new HashMap<>();
    nodes.put(R1, DistributedNode.real(r1));
    nodes.put(R2, DistributedNode.shadow(r2));

    System.setProperty("s2.ownedDataplane", "false");
    try {
      S2BdpEngine e = engine(nodes);
      e.prepareDataPlane(ImmutableMap.of(R1, r1, R2, r2), TopologyContext.builder().build());
      assertThat(
          "owned mode is off when explicitly disabled",
          e.canUseTracerouteForDataplaneTopologyPruning(),
          is(true));
      assertThat(e.hasCompleteFibForTrackReachability(R2), is(true));
    } finally {
      System.clearProperty("s2.ownedDataplane");
    }
  }

  /** With no tracks, VNIs, or overlay topologies, owned mode stays on. */
  @Test
  public void testPlainSnapshotKeepsOwnedMode() {
    Configuration r1 = plainConfig(R1);
    Configuration r2 = plainConfig(R2);
    Map<String, DistributedNode> nodes = new HashMap<>();
    nodes.put(R1, DistributedNode.real(r1));
    nodes.put(R2, DistributedNode.shadow(r2));

    System.setProperty("s2.ownedDataplane", "true");
    try {
      S2BdpEngine e = engine(nodes);
      e.prepareDataPlane(ImmutableMap.of(R1, r1, R2, r2), TopologyContext.builder().build());
      assertThat(
          "owned mode stays on for a snapshot with no remote-FIB consumers",
          e.canUseTracerouteForDataplaneTopologyPruning(),
          is(false));
      assertThat(
          "an owned node's FIB is complete", e.hasCompleteFibForTrackReachability(R1), is(true));
      assertThat(
          "a remote node's FIB is a stub", e.hasCompleteFibForTrackReachability(R2), is(false));
    } finally {
      System.clearProperty("s2.ownedDataplane");
    }
  }

  @Test
  public void testConfiguredVniDisablesOwnedMode() {
    Configuration r1 = plainConfig(R1);
    r1.getDefaultVrf()
        .setLayer2Vnis(
            ImmutableSet.of(
                Layer2Vni.builder()
                    .setVni(10000)
                    .setVlan(10)
                    .setSrcVrf(Configuration.DEFAULT_VRF_NAME)
                    .setBumTransportMethod(BumTransportMethod.UNICAST_FLOOD_GROUP)
                    .build()));
    Configuration r2 = plainConfig(R2);
    Map<String, DistributedNode> nodes = new HashMap<>();
    nodes.put(R1, DistributedNode.shadow(r1));
    nodes.put(R2, DistributedNode.real(r2));

    System.setProperty("s2.ownedDataplane", "true");
    try {
      S2BdpEngine e = engine(nodes);
      e.prepareDataPlane(ImmutableMap.of(R1, r1, R2, r2), TopologyContext.builder().build());
      assertThat(
          "VXLAN VNI settings make owned mode unsafe",
          e.canUseTracerouteForDataplaneTopologyPruning(),
          is(true));
    } finally {
      System.clearProperty("s2.ownedDataplane");
    }
  }

  @Test
  public void testNonEmptyTunnelTopologyDisablesOwnedMode() {
    Configuration r1 = plainConfig(R1);
    Configuration r2 = plainConfig(R2);
    Map<String, DistributedNode> nodes = new HashMap<>();
    nodes.put(R1, DistributedNode.real(r1));
    nodes.put(R2, DistributedNode.shadow(r2));
    TopologyContext tc =
        TopologyContext.builder()
            .setTunnelTopology(
                TunnelTopology.builder()
                    .add(NodeInterfacePair.of(R1, "Tunnel1"), NodeInterfacePair.of(R2, "Tunnel1"))
                    .build())
            .build();

    System.setProperty("s2.ownedDataplane", "true");
    try {
      S2BdpEngine e = engine(nodes);
      e.prepareDataPlane(ImmutableMap.of(R1, r1, R2, r2), tc);
      assertThat(
          "a non-empty tunnel topology makes owned mode unsafe",
          e.canUseTracerouteForDataplaneTopologyPruning(),
          is(true));
    } finally {
      System.clearProperty("s2.ownedDataplane");
    }
  }
}
