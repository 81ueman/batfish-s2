// SPDX-License-Identifier: Apache-2.0
package org.batfish.dataplane.ibdp;

import com.google.common.collect.ImmutableMap;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.util.Map;
import org.batfish.datamodel.BgpProcess;
import org.batfish.datamodel.Configuration;
import org.batfish.datamodel.Interface;
import org.batfish.datamodel.Vrf;

/**
 * Lightweight, serializable stand-in for a remote (shadow) node's {@link Configuration}.
 *
 * <p>A remote worker never simulates a shadow node, so it does not need the parts of the
 * configuration that only the dataplane/forwarding computation reads. The shadow path reads:
 *
 * <ul>
 *   <li>{@link S2Snapshot}: interface addresses / active / L3 flags / VRF, OSPF interface + process
 *       settings, BGP process + peers, and (for the config-derived stub FIB) static/kernel routes —
 *       all of which are needed to compute the L3/OSPF/BGP topologies and IP ownership.
 *   <li>{@code Node}/{@code VirtualRouter} construction: connected/local routes from interfaces,
 *       static/kernel routes, the BGP and OSPF processes (so the worker can install remote
 *       providers on them).
 *   <li>{@code BDDReachabilityAnalysisFactory}: interfaces, VRFs, transformations, and
 *       per-interface L3 flags. With {@link OwnedForwardingAnalysis} only edges whose source is
 *       owned are kept, so a remote config's ACL/policy bodies only ever feed edges the worker
 *       drops.
 * </ul>
 *
 * <p>The descriptor therefore keeps everything above and drops the heavy policy bodies: IP
 * access-lists, routing policies, route-filter lists, community / as-path sets and expressions,
 * packet policies, zones, MLAGs, IKE/IPsec, and generated reference books. Interface references to
 * dropped ACLs are cleared (they are resolved by name via {@code checkNotNull}, so a dangling name
 * would throw); a VRF's resolution policy and a BGP process's next-hop resolver restriction policy
 * are cleared for the same reason.
 *
 * <p>This is only sound while the snapshot has no feature that needs a remote node's full policy or
 * forwarding state on this worker (tracks, IPsec / tunnel / VXLAN reachability, the global
 * traceroute digest). {@code S2Main} gates descriptor mode on exactly those conditions and falls
 * back to full configs otherwise.
 */
final class RemoteNodeDescriptor implements Serializable {

  private static final long serialVersionUID = 1L;

  private final Configuration _shadowConfig;

  private RemoteNodeDescriptor(Configuration shadowConfig) {
    _shadowConfig = shadowConfig;
  }

  /**
   * Build a descriptor for {@code full}. The configuration is deep-copied first so the caller (the
   * controller) can keep using the full config for its vanilla reference.
   */
  static RemoteNodeDescriptor of(Configuration full) {
    Configuration shadow = deepCopy(full);
    shadow.setIpAccessLists(ImmutableMap.of());
    shadow.setRoutingPolicies(ImmutableMap.of());
    shadow.setRouteFilterLists(ImmutableMap.of());
    shadow.setCommunitySets(ImmutableMap.of());
    shadow.setCommunityMatchExprs(ImmutableMap.of());
    shadow.setCommunitySetExprs(ImmutableMap.of());
    shadow.setCommunitySetMatchExprs(ImmutableMap.of());
    shadow.setAsPathAccessLists(ImmutableMap.of());
    shadow.setAsPathExprs(ImmutableMap.of());
    shadow.setAsPathMatchExprs(ImmutableMap.of());
    shadow.setPacketPolicies(ImmutableMap.of());
    shadow.setZones(ImmutableMap.of());
    shadow.setMlags(ImmutableMap.of());
    shadow.setGeneratedReferenceBooks(ImmutableMap.of());
    shadow.setAuthenticationKeyChains(ImmutableMap.of());
    shadow.setIkePhase1Keys(ImmutableMap.of());
    shadow.setIkePhase1Policies(ImmutableMap.of());
    shadow.setIkePhase1Proposals(ImmutableMap.of());
    shadow.setIpsecPeerConfigs(ImmutableMap.of());
    shadow.setIpsecPhase2Policies(ImmutableMap.of());
    shadow.setIpsecPhase2Proposals(ImmutableMap.of());
    shadow.setIpSpaceMetadata(ImmutableMap.of());
    // Clear the by-name references into the collections just dropped, so nothing resolves a
    // dangling name later (Interface.getIpAccessList uses checkNotNull, VirtualRouter and
    // BgpRoutingProcess dereference the resolved policy).
    for (Interface iface : shadow.getAllInterfaces().values()) {
      iface.setInboundFilter(null);
      iface.setIncomingFilter(null);
      iface.setOutgoingFilter(null);
      iface.setOutgoingOriginalFlowFilter(null);
      iface.setPostTransformationIncomingFilter(null);
      iface.setPreTransformationOutgoingFilter(null);
      iface.setPacketPolicy(null);
    }
    for (Vrf vrf : shadow.getVrfs().values()) {
      vrf.setResolutionPolicy(null);
      BgpProcess bgp = vrf.getBgpProcess();
      if (bgp != null) {
        bgp.setNextHopIpResolverRestrictionPolicy(null);
      }
    }
    return new RemoteNodeDescriptor(shadow);
  }

  /** The reduced configuration to materialize as the shadow node's configuration. */
  Configuration shadowConfiguration() {
    return _shadowConfig;
  }

  static byte[] serialize(Map<String, RemoteNodeDescriptor> descriptors) throws IOException {
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    try (ObjectOutputStream oos = new ObjectOutputStream(baos)) {
      oos.writeObject(new java.util.TreeMap<>(descriptors));
    }
    return baos.toByteArray();
  }

  @SuppressWarnings("unchecked")
  static Map<String, RemoteNodeDescriptor> deserialize(byte[] payload)
      throws IOException, ClassNotFoundException {
    try (ObjectInputStream ois = new ObjectInputStream(new ByteArrayInputStream(payload))) {
      return (Map<String, RemoteNodeDescriptor>) ois.readObject();
    }
  }

  private static Configuration deepCopy(Configuration c) {
    try {
      ByteArrayOutputStream baos = new ByteArrayOutputStream();
      try (ObjectOutputStream oos = new ObjectOutputStream(baos)) {
        oos.writeObject(c);
      }
      try (ObjectInputStream ois =
          new ObjectInputStream(new ByteArrayInputStream(baos.toByteArray()))) {
        return (Configuration) ois.readObject();
      }
    } catch (IOException | ClassNotFoundException e) {
      throw new IllegalStateException("Failed to copy configuration " + c.getHostname(), e);
    }
  }
}
