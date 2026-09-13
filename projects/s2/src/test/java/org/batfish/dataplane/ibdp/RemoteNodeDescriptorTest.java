// SPDX-License-Identifier: Apache-2.0
package org.batfish.dataplane.ibdp;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import java.util.Map;
import java.util.SortedMap;
import java.util.TreeMap;
import org.batfish.common.NetworkSnapshot;
import org.batfish.common.topology.TopologyProvider;
import org.batfish.datamodel.Configuration;
import org.batfish.datamodel.ExprAclLine;
import org.batfish.datamodel.IpAccessList;
import org.batfish.datamodel.ospf.OspfTopology;
import org.batfish.datamodel.routing_policy.RoutingPolicy;
import org.batfish.datamodel.routing_policy.statement.Statements;
import org.batfish.main.Batfish;
import org.batfish.main.BatfishTestUtils;
import org.batfish.main.TestrigText;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

/**
 * Tests for {@link RemoteNodeDescriptor}: the reduced shadow configuration must drop the heavy
 * policy bodies while preserving everything the shadow path reads (interfaces / addresses, BGP and
 * OSPF processes), stay constructible, and serialize. The reduced configs must also yield the same
 * topology as the full configs.
 */
public class RemoteNodeDescriptorTest {

  private static final String OSPF_BGP_TESTRIG = "org/batfish/dataplane/testrigs/s2-ospf-bgp";
  private static final ImmutableList<String> OSPF_BGP_CONFIGS = ImmutableList.of("r1", "r2", "r3");

  @Rule public TemporaryFolder _folder = new TemporaryFolder();

  /**
   * Inject an ACL and a routing policy into a parsed config (the testrigs are policy-free), then
   * check the descriptor drops them and clears the by-name references into them, while keeping the
   * interface and the BGP/OSPF processes.
   */
  @Test
  public void testDescriptorTrimsPolicyBodiesAndKeepsShadowFields() throws Exception {
    Batfish batfish =
        BatfishTestUtils.getBatfishFromTestrigText(
            TestrigText.builder().setConfigurationFiles(OSPF_BGP_TESTRIG, OSPF_BGP_CONFIGS).build(),
            _folder);
    NetworkSnapshot snapshot = batfish.getSnapshot();
    SortedMap<String, Configuration> configs = batfish.loadConfigurations(snapshot);
    Configuration r1 = configs.get("r1");
    // Parsed configs hold immutable policy maps; reset to mutable ones so the builders (which
    // register into the owner) work.
    r1.setIpAccessLists(new java.util.HashMap<>());
    r1.setRoutingPolicies(new java.util.HashMap<>());

    IpAccessList acl =
        IpAccessList.builder()
            .setOwner(r1)
            .setName("acl-in")
            .setLines(ImmutableList.of(ExprAclLine.ACCEPT_ALL))
            .build();
    r1.getAllInterfaces().get("GigabitEthernet0/0").setIncomingFilter(acl);
    RoutingPolicy policy =
        RoutingPolicy.builder()
            .setOwner(r1)
            .setName("p")
            .setStatements(ImmutableList.of(Statements.ExitAccept.toStaticStatement()))
            .build();
    r1.getDefaultVrf().setResolutionPolicy("p");
    r1.getDefaultVrf().getBgpProcess().setNextHopIpResolverRestrictionPolicy("p");

    Configuration shadow = RemoteNodeDescriptor.of(r1).shadowConfiguration();

    assertThat("ACLs are dropped", shadow.getIpAccessLists().isEmpty(), is(true));
    assertThat("routing policies are dropped", shadow.getRoutingPolicies().isEmpty(), is(true));
    assertThat(
        "the interface's dangling ACL reference is cleared",
        shadow.getAllInterfaces().get("GigabitEthernet0/0").getIncomingFilter(),
        nullValue());
    assertThat(
        "the dangling VRF resolution policy is cleared",
        shadow.getDefaultVrf().getResolutionPolicy(),
        nullValue());
    assertThat(
        "the dangling BGP next-hop resolver policy is cleared",
        shadow.getDefaultVrf().getBgpProcess().getNextHopIpResolverRestrictionPolicy(),
        nullValue());
    assertThat(
        "interface addresses are preserved",
        shadow.getAllInterfaces().get("GigabitEthernet0/0").getConcreteAddress(),
        equalTo(r1.getAllInterfaces().get("GigabitEthernet0/0").getConcreteAddress()));
    assertThat("BGP process is preserved", shadow.getDefaultVrf().getBgpProcess(), notNullValue());
    assertThat(
        "OSPF process is preserved",
        shadow.getDefaultVrf().getOspfProcesses().isEmpty(),
        is(false));
    // The full config must be untouched (the controller keeps it for the vanilla reference).
    assertThat(r1.getIpAccessLists().containsKey("acl-in"), is(true));
    assertThat(r1.getRoutingPolicies().containsKey("p"), is(true));

    RemoteNodeDescriptor descriptor = RemoteNodeDescriptor.of(r1);
    byte[] payload = RemoteNodeDescriptor.serialize(ImmutableMap.of("r1", descriptor));
    Map<String, RemoteNodeDescriptor> roundTripped = RemoteNodeDescriptor.deserialize(payload);
    assertThat(roundTripped.containsKey("r1"), is(true));
    assertThat(roundTripped.get("r1").shadowConfiguration().getHostname(), equalTo("r1"));
  }

  /** A shadow node built from a reduced config must construct its virtual routers. */
  @Test
  public void testShadowNodeConstructsFromDescriptor() throws Exception {
    Batfish batfish =
        BatfishTestUtils.getBatfishFromTestrigText(
            TestrigText.builder().setConfigurationFiles(OSPF_BGP_TESTRIG, OSPF_BGP_CONFIGS).build(),
            _folder);
    NetworkSnapshot snapshot = batfish.getSnapshot();
    Configuration r1 = batfish.loadConfigurations(snapshot).get("r1");

    DistributedNode shadow =
        DistributedNode.shadow(RemoteNodeDescriptor.of(r1).shadowConfiguration());
    assertThat(shadow.isShadow(), is(true));
    VirtualRouter vr = shadow.getVirtualRouterOrThrow(Configuration.DEFAULT_VRF_NAME);
    assertThat(
        "the shadow's BGP process is available for remote providers",
        vr.getBgpRoutingProcess(),
        notNullValue());
    // A remote OSPF provider is installed by DistributedNode.installRemoteOspfProviders, which
    // first materializes the shadow OSPF processes from the config.
    vr.initShadowOspfProcesses(OspfTopology.EMPTY);
    assertThat(
        "the shadow's OSPF processes are materialized from the descriptor config",
        vr.getOspfProcesses().isEmpty(),
        is(false));
  }

  /**
   * Reduced configs must produce the same initial L3 / OSPF topologies and IP ownership as the full
   * configs; the shadow path depends on exactly these.
   */
  @Test
  public void testDescriptorKeepsTopology() throws Exception {
    Batfish full =
        BatfishTestUtils.getBatfishFromTestrigText(
            TestrigText.builder().setConfigurationFiles(OSPF_BGP_TESTRIG, OSPF_BGP_CONFIGS).build(),
            _folder);
    NetworkSnapshot fullSnapshot = full.getSnapshot();
    SortedMap<String, Configuration> fullConfigs = full.loadConfigurations(fullSnapshot);

    SortedMap<String, Configuration> reduced = new TreeMap<>();
    fullConfigs.forEach(
        (host, c) -> reduced.put(host, RemoteNodeDescriptor.of(c).shadowConfiguration()));
    Batfish reducedBatfish = BatfishTestUtils.getBatfish(reduced, _folder);
    NetworkSnapshot reducedSnapshot = reducedBatfish.getSnapshot();

    TopologyProvider fullTp = full.getTopologyProvider();
    TopologyProvider reducedTp = reducedBatfish.getTopologyProvider();
    assertThat(
        reducedTp.getInitialLayer3Topology(reducedSnapshot).getEdges(),
        equalTo(fullTp.getInitialLayer3Topology(fullSnapshot).getEdges()));
    assertThat(
        reducedTp.getInitialOspfTopology(reducedSnapshot).edges(),
        equalTo(fullTp.getInitialOspfTopology(fullSnapshot).edges()));
    assertThat(
        reducedTp.getInitialIpOwners(reducedSnapshot).getIpVrfOwners(),
        equalTo(fullTp.getInitialIpOwners(fullSnapshot).getIpVrfOwners()));
  }
}
