// SPDX-License-Identifier: Apache-2.0

package org.batfish.dataplane.ibdp;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.SortedMap;
import java.util.TreeMap;
import org.batfish.datamodel.Configuration;
import org.batfish.datamodel.Vrf;
import org.batfish.dataplane.ibdp.partition.AutoSchemeSelector;
import org.batfish.dataplane.ibdp.partition.CommunicationGraph;
import org.batfish.dataplane.ibdp.partition.PartitionScheme;

/**
 * Shared snapshot-to-pool planning for the one-shot runner and the persistent controller service:
 * pick a partition scheme, compute the node &rarr; worker assignment once, and serialize the
 * payloads the workers need (full configs, or descriptor-shadow owned configs + shared
 * descriptors).
 */
final class S2Sharding {

  /** Fixed partition seed: the assignment is computed once on the controller and shipped. */
  static final long PARTITION_SEED = 0L;

  private S2Sharding() {}

  /** A computed partition: the assignment plus the metrics used to report the scheme selection. */
  static final class Plan {
    final Map<String, Integer> assignment;
    final CommunicationGraph graph;
    final PartitionScheme scheme;
    final AutoSchemeSelector.Selection selection;

    Plan(
        Map<String, Integer> assignment,
        CommunicationGraph graph,
        PartitionScheme scheme,
        AutoSchemeSelector.Selection selection) {
      this.assignment = assignment;
      this.graph = graph;
      this.scheme = scheme;
      this.selection = selection;
    }
  }

  static Plan plan(S2Snapshot snap, int numWorkers) {
    // The partition scheme is selected once on the controller and the assignment is computed here,
    // then shipped to every worker (workers never recompute it). The code default RANDOM reproduces
    // the historical hash-shuffle round-robin; the runner defaults -Ds2.partition=auto, which
    // resolves to a concrete scheme from the graph shape (see AutoSchemeSelector).
    PartitionScheme requested = PartitionScheme.fromSystemProperties();
    CommunicationGraph graph =
        CommunicationGraph.build(snap.configs, snap.topologyContext, snap.bgpTopology);
    AutoSchemeSelector.Selection selection = AutoSchemeSelector.select(requested, graph);
    PartitionScheme scheme = selection.scheme();
    Map<String, Integer> assignment =
        CommunicationGraph.canonicalAssignment(
            scheme.partitioner().partition(graph, numWorkers, PARTITION_SEED), numWorkers);
    return new Plan(assignment, graph, scheme, selection);
  }

  /** Human-readable scheme/weighted-cut/imbalance summary for a computed plan. */
  static String describe(Plan plan, int numWorkers) {
    long[] loads = CommunicationGraph.loads(plan.graph, plan.assignment, numWorkers);
    long maxLoad = 0;
    long totalLoad = 0;
    for (long load : loads) {
      maxLoad = Math.max(maxLoad, load);
      totalLoad += load;
    }
    double meanLoad = totalLoad / (double) numWorkers;
    String selectionNote =
        plan.selection.shape() == null
            ? ""
            : String.format(" (requested=AUTO, %s)", plan.selection.describe());
    return String.format(
        "partition scheme=%s%s workers=%d nodes=%d weighted-cut=%d imbalance(max/mean)=%.3f",
        plan.scheme,
        selectionNote,
        numWorkers,
        plan.graph.nodes().size(),
        CommunicationGraph.cutWeight(plan.graph, plan.assignment),
        meanLoad == 0.0 ? 0.0 : maxLoad / meanLoad);
  }

  /**
   * Shadow nodes delegate BGP and OSPF; EIGRP/IS-IS/RIP are not distributed, so a multi-worker run
   * of a snapshot that uses them would hit a null shadow process. Fail with a clear message.
   */
  static void assertDistributedProtocolsSupported(S2Snapshot snap, int numWorkers) {
    if (numWorkers <= 1) {
      return;
    }
    for (Configuration c : snap.configs.values()) {
      for (Vrf vrf : c.getVrfs().values()) {
        if (!vrf.getEigrpProcesses().isEmpty()
            || vrf.getIsisProcess() != null
            || vrf.getRipProcess() != null) {
          throw new UnsupportedOperationException(
              "Multi-worker distributed routing supports only eBGP and OSPF: "
                  + c.getHostname()
                  + " uses EIGRP/IS-IS/RIP. Run with 1 worker or use the in-process "
                  + "S2DistributedControlPlaneTest.");
        }
      }
    }
  }

  /**
   * Whether reduced shadow configs are safe for this snapshot. The descriptor drops remote ACL /
   * policy bodies, so it is only sound while nothing needs a remote node's full policy or
   * forwarding state on a non-owning worker:
   *
   * <ul>
   *   <li>tracks: a {@code TrackReachability} traceroutes through possibly-remote nodes;
   *   <li>IPsec / tunnel / VXLAN reachability: dataplane traceroutes prune the initial topology;
   *   <li>(the global traceroute digest is skipped in descriptor mode — see {@code runWorker}).
   * </ul>
   */
  static boolean descriptorShadowsSafe(S2Snapshot snap) {
    for (Configuration c : snap.configs.values()) {
      if (S2BdpEngine.hasTrackReachability(c)) {
        return false;
      }
      for (Vrf vrf : c.getVrfs().values()) {
        if (!vrf.getLayer2Vnis().isEmpty() || !vrf.getLayer3Vnis().isEmpty()) {
          return false;
        }
      }
    }
    TopologyContext tc = snap.topologyContext;
    return tc.getIpsecTopology().getGraph().edges().isEmpty()
        && tc.getTunnelTopology().getGraph().edges().isEmpty()
        && tc.getVxlanTopology().getGraph().edges().isEmpty();
  }

  /** The serialized payloads a {@link S2ControlMessages.Start} carries to the workers. */
  static final class Payload {
    final byte[] configs;
    final byte[] externalAdverts;
    final Map<Integer, byte[]> ownedConfigsByWorker;
    final byte[] descriptors;
    final int numDescriptors;

    Payload(
        byte[] configs,
        byte[] externalAdverts,
        Map<Integer, byte[]> ownedConfigsByWorker,
        byte[] descriptors,
        int numDescriptors) {
      this.configs = configs;
      this.externalAdverts = externalAdverts;
      this.ownedConfigsByWorker = ownedConfigsByWorker;
      this.descriptors = descriptors;
      this.numDescriptors = numDescriptors;
    }

    boolean descriptorShadows() {
      return descriptors != null;
    }
  }

  /**
   * Serialize the snapshot for shipping. In descriptor-shadow mode each worker gets only its owned
   * configs plus a shared reduced descriptor for the remote nodes; otherwise every worker gets the
   * full snapshot (or null when {@code shipConfigs} is false).
   */
  static Payload preparePayload(
      S2Snapshot snap,
      Map<String, Integer> assignment,
      int numWorkers,
      boolean shipConfigs,
      boolean descriptorShadowsRequested)
      throws IOException {
    byte[] serializedExternalAdverts =
        S2ControlMessages.serializeExternalAdverts(
            snap.batfish.loadExternalBgpAnnouncements(snap.snapshot, snap.configs));
    boolean descriptorShadows =
        shipConfigs && descriptorShadowsRequested && numWorkers > 1 && descriptorShadowsSafe(snap);
    if (descriptorShadows) {
      Map<Integer, SortedMap<String, Configuration>> ownedByWorker = new HashMap<>();
      Map<String, RemoteNodeDescriptor> descriptors = new TreeMap<>();
      for (Map.Entry<String, Configuration> e : snap.configs.entrySet()) {
        int owner = assignment.get(e.getKey());
        ownedByWorker.computeIfAbsent(owner, w -> new TreeMap<>()).put(e.getKey(), e.getValue());
        descriptors.put(e.getKey(), RemoteNodeDescriptor.of(e.getValue()));
      }
      // More workers than nodes is legal; make sure every worker has a (possibly empty) payload.
      for (int w = 0; w < numWorkers; w++) {
        ownedByWorker.computeIfAbsent(w, x -> new TreeMap<>());
      }
      Map<Integer, byte[]> ownedConfigsByWorker = new HashMap<>();
      for (Map.Entry<Integer, SortedMap<String, Configuration>> e : ownedByWorker.entrySet()) {
        ownedConfigsByWorker.put(e.getKey(), S2ControlMessages.serializeConfigs(e.getValue()));
      }
      return new Payload(
          null,
          serializedExternalAdverts,
          ownedConfigsByWorker,
          RemoteNodeDescriptor.serialize(descriptors),
          descriptors.size());
    }
    byte[] serializedConfigs =
        shipConfigs ? S2ControlMessages.serializeConfigs(snap.configs) : null;
    return new Payload(serializedConfigs, serializedExternalAdverts, null, null, 0);
  }
}
