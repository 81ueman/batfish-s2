package org.batfish.dataplane.ibdp;

import static java.util.stream.Collectors.toSet;
import static org.batfish.common.topology.TopologyUtil.computeLayer2Topology;
import static org.batfish.common.topology.TopologyUtil.computeLayer3Topology;
import static org.batfish.common.topology.TopologyUtil.computeRawLayer3Topology;
import static org.batfish.common.topology.TopologyUtil.pruneUnreachableTunnelEdges;
import static org.batfish.common.util.CollectionUtil.toImmutableSortedMap;
import static org.batfish.common.util.IpsecUtil.retainReachableIpsecEdges;
import static org.batfish.common.util.IpsecUtil.toEdgeSet;
import static org.batfish.common.util.StreamUtil.toListInRandomOrder;
import static org.batfish.datamodel.bgp.BgpTopologyUtils.initBgpTopology;
import static org.batfish.datamodel.vxlan.VxlanTopologyUtils.computeNextVxlanTopologyModuloReachability;
import static org.batfish.datamodel.vxlan.VxlanTopologyUtils.prunedVxlanTopology;
import static org.batfish.datamodel.vxlan.VxlanTopologyUtils.vxlanTopologyToLayer3Edges;
import static org.batfish.dataplane.ibdp.TrackReachabilityUtils.evaluateTrackReachability;
import static org.batfish.dataplane.rib.AbstractRib.importRib;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.ImmutableTable;
import com.google.common.collect.Sets;
import com.google.common.collect.Table;
import com.google.common.collect.Table.Cell;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.Set;
import java.util.SortedMap;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Stream;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.batfish.common.BdpOscillationException;
import org.batfish.common.plugin.DataPlanePlugin.ComputeDataPlaneResult;
import org.batfish.common.plugin.TracerouteEngine;
import org.batfish.common.topology.GlobalBroadcastNoPointToPoint;
import org.batfish.common.topology.HybridL3Adjacencies;
import org.batfish.common.topology.IpOwners;
import org.batfish.common.topology.L3Adjacencies;
import org.batfish.common.topology.Layer1Topologies;
import org.batfish.common.topology.Layer2Topology;
import org.batfish.common.topology.TunnelTopology;
import org.batfish.common.topology.broadcast.BroadcastL3Adjacencies;
import org.batfish.datamodel.AbstractRoute;
import org.batfish.datamodel.BgpAdvertisement;
import org.batfish.datamodel.Bgpv4Route;
import org.batfish.datamodel.Configuration;
import org.batfish.datamodel.Edge;
import org.batfish.datamodel.Fib;
import org.batfish.datamodel.InactiveReason;
import org.batfish.datamodel.IntegerSpace;
import org.batfish.datamodel.Interface;
import org.batfish.datamodel.InterfaceType;
import org.batfish.datamodel.Ip;
import org.batfish.datamodel.IsisRoute;
import org.batfish.datamodel.NetworkConfigurations;
import org.batfish.datamodel.PrefixSpace;
import org.batfish.datamodel.SwitchportMode;
import org.batfish.datamodel.Topology;
import org.batfish.datamodel.Vrf;
import org.batfish.datamodel.answers.IncrementalBdpAnswerElement;
import org.batfish.datamodel.bgp.BgpTopology;
import org.batfish.datamodel.eigrp.EigrpTopology;
import org.batfish.datamodel.eigrp.EigrpTopologyUtils;
import org.batfish.datamodel.ipsec.IpsecTopology;
import org.batfish.datamodel.ospf.OspfTopology;
import org.batfish.datamodel.tracking.GenericTrackMethodVisitor;
import org.batfish.datamodel.tracking.NegatedTrackMethod;
import org.batfish.datamodel.tracking.PreDataPlaneTrackMethodEvaluator;
import org.batfish.datamodel.tracking.TrackAll;
import org.batfish.datamodel.tracking.TrackInterface;
import org.batfish.datamodel.tracking.TrackMethodReference;
import org.batfish.datamodel.tracking.TrackReachability;
import org.batfish.datamodel.tracking.TrackRoute;
import org.batfish.datamodel.tracking.TrackTrue;
import org.batfish.datamodel.vxlan.Layer2Vni;
import org.batfish.datamodel.vxlan.VxlanNode;
import org.batfish.datamodel.vxlan.VxlanTopology;
import org.batfish.dataplane.TracerouteEngineImpl;
import org.batfish.dataplane.ibdp.DataplaneTrackEvaluator.DataPlaneTrackMethodEvaluatorProvider;
import org.batfish.dataplane.ibdp.TrackRouteUtils.GetRoutesForPrefix;
import org.batfish.dataplane.ibdp.schedule.IbdpSchedule;
import org.batfish.dataplane.ibdp.schedule.IbdpSchedule.Schedule;
import org.batfish.dataplane.rib.RibDelta;
import org.batfish.version.BatfishVersion;

/** Computes the entire dataplane by executing a fixed-point computation. */
public class IncrementalBdpEngine {

  private static final Logger LOGGER = LogManager.getLogger(IncrementalBdpEngine.class);

  /** Whether prefix-sharded rounds externalize (and free) each shard's BGP routes. */
  private static boolean externalize() {
    return Boolean.getBoolean("s2.prefixShardExternalize");
  }

  /**
   * Maximum amount of topology iterations to do before deciding that the dataplane computation
   * cannot converge (there is some sort of flap)
   */
  private static final int MAX_TOPOLOGY_ITERATIONS = 10;

  private int _numIterations;
  private final IncrementalDataPlaneSettings _settings;

  IncrementalBdpEngine(IncrementalDataPlaneSettings settings) {
    _settings = settings;
  }

  /**
   * Returns the {@link PartialDataplane} corresponding to the given topology and nodes. FIBs,
   * ForwardingAnalysis, and other internals are recomputed based on the updated state in the {@code
   * nodes} and {@code vrs}.
   */
  protected PartialDataplane nextDataplane(
      TopologyContext currentTopologyContext,
      SortedMap<String, Node> nodes,
      List<VirtualRouter> vrs,
      IpOwners currentIpOwners) {
    LOGGER.info("Updating dataplane");
    computeFibs(vrs);

    return PartialDataplane.builder()
        .setNodes(nodes)
        .setIpOwners(currentIpOwners)
        .setLayer3Topology(currentTopologyContext.getLayer3Topology())
        .setL3Adjacencies(currentTopologyContext.getL3Adjacencies())
        .build();
  }

  /**
   * The nodes included in the *final* dataplane result. Stock returns every node. S2's distributed
   * engine may return only its owned nodes so a worker does not retain remote final RIBs. The
   * intermediate partial dataplane still covers all nodes (remote ones may hold stub FIBs needed
   * for remote ARP state).
   */
  protected Map<String, Node> dataPlaneNodes(Map<String, Node> nodes) {
    return nodes;
  }

  /**
   * Called once, before any node is built, to let a subclass react to the snapshot about to be
   * computed. The default implementation does nothing, so stock behavior is unchanged.
   *
   * <p>The S2 engine uses this to turn off its owned-only dataplane when the snapshot contains
   * features that need complete remote FIBs (tracks, VXLAN/IPsec/tunnel reachability pruning),
   * rather than silently computing against stub remote FIBs.
   */
  protected void prepareDataPlane(
      Map<String, Configuration> configurations, TopologyContext initialTopologyContext) {}

  /**
   * Whether the partial dataplane used to compute the next topology has complete FIBs for every
   * node, so traceroute-based pruning of the IPsec/VXLAN/tunnel topologies is valid. The default is
   * {@code true}. The S2 owned-only dataplane returns {@code false} because remote nodes only have
   * stub FIBs.
   */
  protected boolean canUseTracerouteForDataplaneTopologyPruning() {
    return true;
  }

  /**
   * Whether the FIB for {@code hostname} in the partial dataplane is complete enough to evaluate a
   * {@link TrackReachability}. The default is {@code true}. The S2 owned-only dataplane returns
   * {@code false} for a remote (shadow) node, whose FIB is a config-derived stub; evaluating a
   * track against it would give a silently wrong answer.
   */
  protected boolean hasCompleteFibForTrackReachability(String hostname) {
    return true;
  }

  /**
   * Performs the iterative step in dataplane computations as topology changes.
   *
   * <p>The {@code currentTopologyContext} contains the connectivity learned so far in the network,
   * specifically for things like VXLAN, BGP, and others, and {@code nodes} contains the current
   * routing and forwarding tables.
   *
   * <p>Given these inputs, primarily the current Layer3 topology, the possible edges for each other
   * topology (obtained from {@code initialTopologyContext}) are pruned down based on which sessions
   * can be established given the current L3 topology and dataplane state. The resulting {@code
   * TopologyContext} for the next iteration of dataplane is returned.
   */
  protected TopologyContext nextTopologyContext(
      TopologyContext currentTopologyContext,
      PartialDataplane currentDataplane,
      TopologyContext initialTopologyContext,
      NetworkConfigurations networkConfigurations,
      Map<Ip, Map<String, Set<String>>> ipVrfOwners) {
    // Update topologies
    LOGGER.info("Updating dynamic topologies");

    Map<String, Configuration> configurations = networkConfigurations.getMap();
    // Traceroute-based pruning needs complete FIBs for every node. The S2 owned-only dataplane has
    // only stub FIBs for remote nodes, so a traceroute over it can give a wrong answer. In that
    // case we skip the prune (each prune is a no-op when its topology is empty anyway) and keep the
    // unpruned candidate edges, which is a conservative superset. A snapshot that actually needs
    // the
    // prune (non-empty topology / VNI settings) is detected up front by {@link #prepareDataPlane}.
    boolean canPruneWithTraceroute = canUseTracerouteForDataplaneTopologyPruning();
    TracerouteEngine trEngCurrentL3Topology = null;

    // IPsec
    LOGGER.info("Updating IPsec topology");
    // Note: this uses the initial context since it is pruning down the potential edges initially
    // established.
    IpsecTopology initialIpsecTopology = initialTopologyContext.getIpsecTopology();
    IpsecTopology newIpsecTopology;
    if (initialIpsecTopology.getGraph().edges().isEmpty()) {
      // Nothing to prune; skip building the traceroute engine.
      newIpsecTopology = initialIpsecTopology;
    } else if (!canPruneWithTraceroute) {
      LOGGER.warn(
          "Skipping IPsec topology pruning: the dataplane has incomplete (stub) FIBs, so a"
              + " traceroute-based prune would be unsound. Keeping the unpruned initial IPsec"
              + " topology.");
      newIpsecTopology = initialIpsecTopology;
    } else {
      trEngCurrentL3Topology =
          getOrCreateTracerouteEngine(
              trEngCurrentL3Topology, currentDataplane, currentTopologyContext, configurations);
      newIpsecTopology =
          retainReachableIpsecEdges(initialIpsecTopology, configurations, trEngCurrentL3Topology);
    }

    // VXLAN
    LOGGER.info("Updating VXLAN topology");
    VxlanTopology vxlanTopologyModuloReachability =
        computeNextVxlanTopologyModuloReachability(
            currentDataplane.getLayer2Vnis(), currentDataplane.getLayer3Vnis());
    VxlanTopology newVxlanTopology;
    if (vxlanTopologyModuloReachability.getGraph().edges().isEmpty()) {
      // Nothing to prune; skip building the traceroute engine.
      newVxlanTopology = vxlanTopologyModuloReachability;
    } else if (!canPruneWithTraceroute) {
      LOGGER.warn(
          "Skipping VXLAN topology pruning: the dataplane has incomplete (stub) FIBs, so a"
              + " traceroute-based prune would be unsound. Keeping the unpruned VXLAN topology.");
      newVxlanTopology = vxlanTopologyModuloReachability;
    } else {
      trEngCurrentL3Topology =
          getOrCreateTracerouteEngine(
              trEngCurrentL3Topology, currentDataplane, currentTopologyContext, configurations);
      newVxlanTopology =
          prunedVxlanTopology(
              vxlanTopologyModuloReachability, configurations, trEngCurrentL3Topology);
    }

    // Tunnel topology
    LOGGER.info("Updating Tunnel topology");
    // like IPsec, pruning initial tunnels
    TunnelTopology initialTunnelTopology = initialTopologyContext.getTunnelTopology();
    TunnelTopology newTunnelTopology;
    if (initialTunnelTopology.getGraph().edges().isEmpty()) {
      // Nothing to prune; skip building the traceroute engine.
      newTunnelTopology = initialTunnelTopology;
    } else if (!canPruneWithTraceroute) {
      LOGGER.warn(
          "Skipping tunnel topology pruning: the dataplane has incomplete (stub) FIBs, so a"
              + " traceroute-based prune would be unsound. Keeping the unpruned initial tunnel"
              + " topology.");
      newTunnelTopology = initialTunnelTopology;
    } else {
      trEngCurrentL3Topology =
          getOrCreateTracerouteEngine(
              trEngCurrentL3Topology, currentDataplane, currentTopologyContext, configurations);
      newTunnelTopology =
          pruneUnreachableTunnelEdges(
              initialTunnelTopology, networkConfigurations, trEngCurrentL3Topology);
    }

    // EIGRP topology
    LOGGER.info("Updating EIGRP topology");
    EigrpTopology newEigrpTopology =
        EigrpTopologyUtils.initEigrpTopology(
            configurations, currentTopologyContext.getLayer3Topology());

    // Initialize BGP topology
    LOGGER.info("Updating BGP topology");
    boolean checkBgpSessionReachability = checkBgpSessionReachability();
    if (checkBgpSessionReachability && trEngCurrentL3Topology == null) {
      // The session reachability check also needs the engine; build it if no prune did.
      trEngCurrentL3Topology =
          getOrCreateTracerouteEngine(
              trEngCurrentL3Topology, currentDataplane, currentTopologyContext, configurations);
    }
    BgpTopology newBgpTopology =
        initBgpTopology(
            configurations,
            ipVrfOwners,
            false,
            checkBgpSessionReachability,
            trEngCurrentL3Topology,
            currentDataplane.getFibs(),
            currentTopologyContext.getL3Adjacencies());

    // Update L3 adjacencies if necessary.
    L3Adjacencies newAdjacencies;
    if (!currentTopologyContext
        .getVxlanTopology()
        .getLayer2VniEdges()
        .collect(ImmutableSet.toImmutableSet())
        .equals(newVxlanTopology.getLayer2VniEdges().collect(ImmutableSet.toImmutableSet()))) {
      LOGGER.info("Updating Layer 3 adjacencies");
      if (L3Adjacencies.USE_NEW_METHOD) {
        newAdjacencies =
            BroadcastL3Adjacencies.create(
                initialTopologyContext.getLayer1Topologies(), newVxlanTopology, configurations);
      } else {
        Layer1Topologies topologies = initialTopologyContext.getLayer1Topologies();
        if (topologies.getCombinedL1().isEmpty()) {
          newAdjacencies = GlobalBroadcastNoPointToPoint.instance();
        } else {
          Layer2Topology l2 =
              computeLayer2Topology(
                  topologies.getActiveLogicalL1(), newVxlanTopology, configurations);
          newAdjacencies = HybridL3Adjacencies.create(topologies, l2, configurations);
        }
      }
    } else {
      newAdjacencies = currentTopologyContext.getL3Adjacencies();
    }

    // Layer-3
    Topology newLayer3Topology;
    if (!newIpsecTopology.equals(currentTopologyContext.getIpsecTopology())
        || !newTunnelTopology.equals(currentTopologyContext.getTunnelTopology())
        || !newAdjacencies.equals(currentTopologyContext.getL3Adjacencies())
        || !newVxlanTopology
            .getLayer3VniEdges()
            .collect(ImmutableSet.toImmutableSet())
            .equals(
                currentTopologyContext
                    .getVxlanTopology()
                    .getLayer3VniEdges()
                    .collect(ImmutableSet.toImmutableSet()))) {
      LOGGER.info("Updating Layer 3 topology");
      newLayer3Topology =
          computeLayer3Topology(
              computeRawLayer3Topology(newAdjacencies, configurations),
              // Overlay edges consist of "plain" tunnels and IPSec tunnels
              ImmutableSet.<Edge>builder()
                  .addAll(toEdgeSet(newIpsecTopology, configurations))
                  .addAll(newTunnelTopology.asEdgeSet())
                  .addAll(vxlanTopologyToLayer3Edges(newVxlanTopology, configurations))
                  .build());
    } else {
      newLayer3Topology = currentTopologyContext.getLayer3Topology();
    }

    return currentTopologyContext.toBuilder()
        .setBgpTopology(newBgpTopology)
        .setLayer3Topology(newLayer3Topology)
        .setL3Adjacencies(newAdjacencies)
        .setVxlanTopology(newVxlanTopology)
        .setIpsecTopology(newIpsecTopology)
        .setTunnelTopology(newTunnelTopology)
        .setEigrpTopology(newEigrpTopology)
        .build();
  }

  /**
   * Lazily build the traceroute engine used to prune the dynamic topologies, reusing it across the
   * IPsec/VXLAN/tunnel prunes within a single {@link #nextTopologyContext} call.
   */
  private static TracerouteEngine getOrCreateTracerouteEngine(
      @Nullable TracerouteEngine existing,
      PartialDataplane currentDataplane,
      TopologyContext currentTopologyContext,
      Map<String, Configuration> configurations) {
    if (existing != null) {
      return existing;
    }
    return new TracerouteEngineImpl(
        currentDataplane, currentTopologyContext.getLayer3Topology(), configurations);
  }

  /**
   * Update autostate for VLAN interfaces based on VXLAN L2 VNI edge changes. When a VLAN gains an
   * active L2 VNI edge (remote VTEP reachable), its IRB should be active even without physical
   * switchport members. When L2 VNI edges disappear and the VLAN has no physical members, the IRB
   * should be deactivated.
   *
   * @return the set of hostnames whose interface status changed (empty if no changes)
   */
  @VisibleForTesting
  static Set<String> updateVxlanAutostate(
      VxlanTopology oldVxlanTopology,
      VxlanTopology newVxlanTopology,
      Map<String, Configuration> configurations) {
    // Collect (hostname, VNI) pairs that have L2 VNI edges in each topology
    Set<VxlanNode> oldNodes = nodesWithLayer2VniEdges(oldVxlanTopology);
    Set<VxlanNode> newNodes = nodesWithLayer2VniEdges(newVxlanTopology);

    Set<VxlanNode> gained = Sets.difference(newNodes, oldNodes);
    Set<VxlanNode> lost = Sets.difference(oldNodes, newNodes);
    if (gained.isEmpty() && lost.isEmpty()) {
      return ImmutableSet.of();
    }

    NetworkConfigurations nc = NetworkConfigurations.of(configurations);
    ImmutableSet.Builder<String> affectedHostnames = ImmutableSet.builder();

    // Activate IRBs for VLANs that gained VXLAN connectivity
    for (VxlanNode node : gained) {
      Interface iface = findVlanInterfaceForVni(node, nc);
      if (iface != null
          && !iface.getActive()
          && iface.getInactiveReason() == InactiveReason.AUTOSTATE_FAILURE) {
        iface.reactivateForAutostate();
        affectedHostnames.add(node.getHostname());
      }
    }

    // Deactivate IRBs for VLANs that lost VXLAN connectivity (if no physical members)
    for (VxlanNode node : lost) {
      Interface iface = findVlanInterfaceForVni(node, nc);
      if (iface != null && iface.getActive() && iface.getAutoState()) {
        Configuration c = configurations.get(node.getHostname());
        int vlanNumber = iface.getVlan();
        if (c.getNormalVlanRange().contains(vlanNumber)
            && countPhysicalVlanMembers(c, vlanNumber) == 0) {
          iface.deactivate(InactiveReason.AUTOSTATE_FAILURE);
          affectedHostnames.add(node.getHostname());
        }
      }
    }

    return affectedHostnames.build();
  }

  /** Collect the set of VxlanNodes that participate in at least one L2 VNI edge. */
  private static Set<VxlanNode> nodesWithLayer2VniEdges(VxlanTopology topology) {
    ImmutableSet.Builder<VxlanNode> nodes = ImmutableSet.builder();
    topology
        .getLayer2VniEdges()
        .forEach(
            edge -> {
              nodes.add(edge.nodeU());
              nodes.add(edge.nodeV());
            });
    return nodes.build();
  }

  /**
   * Find the VLAN interface associated with a given VxlanNode's L2 VNI. Returns null if the VNI or
   * VLAN interface cannot be resolved.
   */
  private static @Nullable Interface findVlanInterfaceForVni(
      VxlanNode node, NetworkConfigurations nc) {
    Optional<Layer2Vni> vniOpt =
        nc.getVniSettings(node.getHostname(), node.getVni(), Vrf::getLayer2Vnis);
    if (vniOpt.isEmpty()) {
      return null;
    }
    int vlanNumber = vniOpt.get().getVlan();
    Optional<Configuration> cOpt = nc.get(node.getHostname());
    if (cOpt.isEmpty()) {
      return null;
    }
    // Find the VLAN interface for this VLAN number
    return cOpt.get().getAllInterfaces().values().stream()
        .filter(
            iface ->
                iface.getInterfaceType() == InterfaceType.VLAN
                    && Integer.valueOf(vlanNumber).equals(iface.getVlan()))
        .findFirst()
        .orElse(null);
  }

  /**
   * Count active physical switchport members for a given VLAN on a device. This mirrors the
   * member-counting logic in {@link org.batfish.main.Batfish#disableUnusableVlanInterfaces}.
   */
  private static int countPhysicalVlanMembers(Configuration c, int vlanNumber) {
    int count = 0;
    for (Interface iface : c.getActiveInterfaces().values()) {
      if (iface.getInterfaceType() == InterfaceType.VLAN) {
        continue;
      }
      if (iface.getSwitchportMode() == SwitchportMode.TRUNK) {
        IntegerSpace allowed = iface.getAllowedVlans();
        if (allowed.isEmpty() || allowed.contains(vlanNumber)) {
          count++;
          continue;
        }
        Integer nativeVlan = iface.getNativeVlan();
        if (nativeVlan != null && nativeVlan == vlanNumber) {
          count++;
        }
      } else if (iface.getSwitchportMode() == SwitchportMode.ACCESS) {
        Integer accessVlan = iface.getAccessVlan();
        if (accessVlan != null && accessVlan == vlanNumber) {
          count++;
        }
      }
    }
    return count;
  }

  /** Helper method used to sample the change in tracks across iterations. */
  @VisibleForTesting
  static <T> @Nonnull Optional<String> compareTracks(
      Table<String, T, Boolean> current, Table<String, T, Boolean> next) {
    if (current.equals(next)) {
      return Optional.empty();
    }
    Set<String> currentTrue =
        current.cellSet().stream()
            .filter(Cell::getValue)
            .map(c -> String.format("%s > %s", c.getRowKey(), c.getColumnKey()))
            .collect(toSet());
    Set<String> nextTrue =
        next.cellSet().stream()
            .filter(Cell::getValue)
            .map(c -> String.format("%s > %s", c.getRowKey(), c.getColumnKey()))
            .collect(toSet());
    List<String> gained = ImmutableList.copyOf(Sets.difference(nextTrue, currentTrue));
    List<String> lost = ImmutableList.copyOf(Sets.difference(currentTrue, nextTrue));
    if (gained.isEmpty()) {
      return Optional.ofNullable(
          String.format(
              "lost %d including %s", lost.size(), lost.size() > 3 ? lost.subList(0, 3) : lost));
    } else if (lost.isEmpty()) {
      return Optional.ofNullable(
          String.format(
              "gained %d including %s",
              gained.size(), gained.size() > 3 ? gained.subList(0, 3) : gained));
    }
    return Optional.ofNullable(
        String.format(
            "gained %d including %s, lost %d including %s",
            gained.size(),
            gained.size() > 3 ? gained.subList(0, 3) : gained,
            lost.size(),
            lost.size() > 3 ? lost.subList(0, 3) : lost));
  }

  /**
   * Factory for the {@link Node} model backing a configuration. Overridable so the S2 distributed
   * engine can substitute {@code DistributedNode}s without duplicating engine logic.
   */
  Node newNode(Configuration configuration) {
    return new Node(configuration);
  }

  /**
   * Virtual routers that this engine should iterate (route computation). Defaults to all routers on
   * the node. The S2 distributed engine overrides this so shadow nodes are visible to dataplane
   * construction (FIBs/forwarding analysis) but are not simulated locally.
   */
  Collection<VirtualRouter> iterationVirtualRouters(Node node) {
    return node.getVirtualRouters();
  }

  /**
   * Whether BGP session establishment verifies dataplane reachability. The S2 engine disables this
   * while shadow FIBs are not yet distributed; sessions are then established from configuration +
   * L3 adjacency (sufficient for directly-connected peering).
   */
  protected boolean checkBgpSessionReachability() {
    return true;
  }

  ComputeDataPlaneResult computeDataPlane(
      Map<String, Configuration> configurations,
      TopologyContext initialTopologyContext,
      Set<BgpAdvertisement> externalAdverts,
      IpOwners initialIpOwners) {
    return computeDataPlane(
        configurations, initialTopologyContext, externalAdverts, initialIpOwners, false);
  }

  ComputeDataPlaneResult computeDataPlane(
      Map<String, Configuration> configurations,
      TopologyContext initialTopologyContext,
      Set<BgpAdvertisement> externalAdverts,
      IpOwners initialIpOwners,
      boolean retainAnnotatedRibs) {
    LOGGER.info("Computing Data Plane using iBDP");

    // Let a subclass adjust how the dataplane will be computed before any node is built (e.g. the
    // S2 engine disables its owned-only dataplane when the snapshot needs state it cannot provide).
    prepareDataPlane(configurations, initialTopologyContext);

    Map<Ip, Map<String, Set<String>>> initialIpVrfOwners = initialIpOwners.getIpVrfOwners();

    // Generate our nodes, keyed by name, sorted for determinism
    SortedMap<String, Node> nodes =
        toImmutableSortedMap(configurations.values(), Configuration::getHostname, this::newNode);
    // A collection of all the virtual routers in random order enables parallelization across all
    // VRs, and likely spreads nodes with similar hostnames across different cores. In contrast,
    // nodes.values().parallelStream().flatMap(get vrs stream) is only node-parallel and clusters
    // nodes by hostname. See https://github.com/batfish/batfish/pull/7054 description.
    List<VirtualRouter> vrs =
        toListInRandomOrder(
            nodes.values().stream().flatMap(n -> iterationVirtualRouters(n).stream()));
    NetworkConfigurations networkConfigurations = NetworkConfigurations.of(configurations);
    reportPhase("after building nodes");

    /*
     * Run the data plane computation here:
     * - First, let the IGP routes converge
     * - Second, re-init BGP neighbors with reachability checks
     * - Third, let the EGP routes converge
     * - Finally, compute FIBs, return answer
     */
    IncrementalBdpAnswerElement answerElement = new IncrementalBdpAnswerElement();
    // TODO: eventually, IGP needs to be part of fixed-point below, because tunnels.
    computeIgpDataPlane(nodes, vrs, initialTopologyContext, networkConfigurations, answerElement);
    reportPhase("after IGP");

    LOGGER.info("Initialize virtual routers before topology fixed point");
    vrs.parallelStream()
        .forEach(
            vr -> vr.initForEgpComputationBeforeTopologyLoop(externalAdverts, initialIpVrfOwners));
    reportPhase("after initForEgpBeforeTopologyLoop");

    /*
     * Perform a fixed-point computation, in which every round the topology is updated based
     * on what we have learned in the previous round.
     */
    // Since the topology iterations are incremental, clear fields that are pruned to get the real
    // topology. They are not actually yet included in topologies.
    TopologyContext priorTopologyContext =
        initialTopologyContext.toBuilder()
            .setIpsecTopology(IpsecTopology.EMPTY)
            .setTunnelTopology(TunnelTopology.EMPTY)
            .setVxlanTopology(VxlanTopology.EMPTY)
            .build();
    PartialDataplane currentDataplane =
        nextDataplane(priorTopologyContext, nodes, vrs, initialIpOwners);
    reportPhase("after initial nextDataplane");

    TopologyContext currentTopologyContext =
        nextTopologyContext(
            priorTopologyContext,
            currentDataplane,
            initialTopologyContext,
            networkConfigurations,
            initialIpVrfOwners);
    Map<String, Collection<TrackRoute>> trackRoutesByHostname = collectTrackRoutes(configurations);
    Map<String, Collection<TrackReachability>> trackReachabilitiesByHostname =
        collectTrackReachabilities(configurations);
    Table<String, TrackReachability, Boolean> currentTrackReachabilityResults =
        nextTrackReachabilityResults(
            currentDataplane,
            currentTopologyContext,
            configurations,
            trackReachabilitiesByHostname);
    Table<String, TrackRoute, Boolean> currentTrackRouteResults =
        nextTrackRouteResults(trackRoutesByHostname, nodes);
    DataPlaneTrackMethodEvaluatorProvider currentTrackMethodEvaluatorProvider =
        nextTrackMethodEvaluatorProvider(currentTrackReachabilityResults, currentTrackRouteResults);
    DataPlaneIpOwners currentIpOwners =
        new DataPlaneIpOwners(
            configurations,
            currentTopologyContext.getL3Adjacencies(),
            currentTrackMethodEvaluatorProvider);
    int topologyIterations = 0;
    boolean converged = false;
    while (!converged && topologyIterations++ < MAX_TOPOLOGY_ITERATIONS) {
      LOGGER.info("Starting topology iteration {}", topologyIterations);
      boolean isOscillating =
          computeNonMonotonicPortionOfDataPlane(
              nodes,
              vrs,
              answerElement,
              currentTopologyContext,
              initialTopologyContext.getLayer3Topology(),
              currentIpOwners,
              networkConfigurations,
              currentTrackMethodEvaluatorProvider);
      if (isOscillating) {
        // If we are oscillating here, network has no stable solution.
        LOGGER.error("Network has no stable solution");
        throw new BdpOscillationException("Network has no stable solution");
      }
      reportPhase("after EGP iteration " + topologyIterations);

      updateLayer3Vnis(vrs);
      currentDataplane = null; // free the old one
      currentDataplane = nextDataplane(currentTopologyContext, nodes, vrs, currentIpOwners);
      reportPhase("after nextDataplane " + topologyIterations);
      TopologyContext nextTopologyContext =
          nextTopologyContext(
              currentTopologyContext,
              currentDataplane,
              initialTopologyContext,
              networkConfigurations,
              currentIpOwners.getIpVrfOwners());

      // Activate/deactivate IRBs based on L2 VNI edge changes (VXLAN-aware autostate)
      Set<String> autostateAffectedHostnames =
          updateVxlanAutostate(
              currentTopologyContext.getVxlanTopology(),
              nextTopologyContext.getVxlanTopology(),
              configurations);
      if (!autostateAffectedHostnames.isEmpty()) {
        // Recompute connected routes only for VRFs on affected nodes
        vrs.parallelStream()
            .filter(vr -> autostateAffectedHostnames.contains(vr.getHostname()))
            .forEach(VirtualRouter::updateConnectedAndLocalRoutesForAutostateChange);
      }

      Table<String, TrackReachability, Boolean> nextTrackReachabilityResults =
          nextTrackReachabilityResults(
              currentDataplane,
              currentTopologyContext,
              configurations,
              trackReachabilitiesByHostname);
      Table<String, TrackRoute, Boolean> nextTrackRouteResults =
          nextTrackRouteResults(trackRoutesByHostname, nodes);
      currentTrackMethodEvaluatorProvider =
          nextTrackMethodEvaluatorProvider(nextTrackReachabilityResults, nextTrackRouteResults);
      DataPlaneIpOwners nextIpOwners =
          new DataPlaneIpOwners(
              configurations,
              nextTopologyContext.getL3Adjacencies(),
              currentTrackMethodEvaluatorProvider);
      converged = true;
      if (!currentTopologyContext.equals(nextTopologyContext)) {
        converged = false;
        LOGGER.info("Topologies changed in this iteration");
      }
      Optional<String> reachabilityDiff =
          compareTracks(currentTrackReachabilityResults, nextTrackReachabilityResults);
      Optional<String> routesDiff = compareTracks(currentTrackRouteResults, nextTrackRouteResults);
      if (reachabilityDiff.isPresent() || routesDiff.isPresent()) {
        converged = false;
        LOGGER.info("Tracks changed in this iteration");
        reachabilityDiff.ifPresent(s -> LOGGER.info("Reachability tracks: {}", s));
        routesDiff.ifPresent(s -> LOGGER.info("Route tracks: {}", s));
      }
      if (!currentIpOwners.equals(nextIpOwners)) {
        converged = false;
        LOGGER.info("IP ownership changed in this iteration");
      }
      if (!autostateAffectedHostnames.isEmpty()) {
        converged = false;
        LOGGER.info("VXLAN autostate changed interface status in this iteration");
      }
      // A distributed engine must agree on convergence: if any worker still sees a topology change,
      // every worker has to run another topology iteration, or they desynchronize.
      converged = hasReachedTopologyFixedPoint(converged);
      currentTopologyContext = nextTopologyContext;
      currentTrackReachabilityResults = nextTrackReachabilityResults;
      currentTrackRouteResults = nextTrackRouteResults;
      currentIpOwners = nextIpOwners;
    }

    if (!converged) {
      LOGGER.error(
          "Could not reach a fixed point topology in {} iterations", MAX_TOPOLOGY_ITERATIONS);
      throw new BdpOscillationException(
          String.format(
              "Could not reach a fixed point topology in %d iterations", MAX_TOPOLOGY_ITERATIONS));
    }

    // Generate the answers from the computation, compute final FIBs
    // TODO: Properly finalize topologies, IpOwners, etc.
    LOGGER.info("Finalizing dataplane");
    answerElement.setVersion(BatfishVersion.getVersionStatic());
    IncrementalDataPlane finalDataplane =
        IncrementalDataPlane.builder()
            .setNodes(dataPlaneNodes(nodes))
            .setPartialDataplane(currentDataplane)
            .setRetainAnnotatedRibs(retainAnnotatedRibs)
            .build();
    return new IbdpResult(answerElement, finalDataplane, currentTopologyContext, nodes);
  }

  private @Nonnull Table<String, TrackRoute, Boolean> nextTrackRouteResults(
      Map<String, Collection<TrackRoute>> trackRoutesByHostname, SortedMap<String, Node> nodes) {
    ImmutableTable.Builder<String, TrackRoute, Boolean> trackRouteResults =
        ImmutableTable.builder();
    trackRoutesByHostname.forEach(
        (hostname, trackRoutes) -> {
          Node node = nodes.get(hostname);
          trackRoutes.forEach(
              trackRoute ->
                  trackRouteResults.put(
                      hostname, trackRoute, evaluateTrackRoute(trackRoute, node)));
        });
    return trackRouteResults.build();
  }

  /**
   * Returns map: hostname of config with at least one {@link TrackRoute} -> {@link TrackRoute}s in
   * that config.
   */
  private static @Nonnull Map<String, Collection<TrackReachability>> collectTrackReachabilities(
      Map<String, Configuration> configurations) {
    ImmutableMap.Builder<String, Collection<TrackReachability>> builder = ImmutableMap.builder();
    configurations.forEach(
        (hostname, c) -> {
          Collection<TrackReachability> trackReachabilities =
              c.getTrackingGroups().values().stream()
                  .flatMap(TRACK_REACHABILITY_COLLECTOR::visit)
                  .collect(ImmutableSet.toImmutableSet());
          if (!trackReachabilities.isEmpty()) {
            builder.put(hostname, trackReachabilities);
          }
        });
    return builder.build();
  }

  private static final TrackReachabilityCollector TRACK_REACHABILITY_COLLECTOR =
      new TrackReachabilityCollector();

  private static final class TrackReachabilityCollector
      implements GenericTrackMethodVisitor<Stream<TrackReachability>> {

    @Override
    public Stream<TrackReachability> visitNegatedTrackMethod(
        NegatedTrackMethod negatedTrackMethod) {
      return visit(negatedTrackMethod.getTrackMethod());
    }

    @Override
    public Stream<TrackReachability> visitTrackAll(TrackAll trackAll) {
      return trackAll.getConjuncts().stream().flatMap(this::visit);
    }

    @Override
    public Stream<TrackReachability> visitTrackInterface(TrackInterface trackInterface) {
      return Stream.of();
    }

    @Override
    public Stream<TrackReachability> visitTrackMethodReference(
        TrackMethodReference trackMethodReference) {
      // target will be found elsewhere
      return Stream.of();
    }

    @Override
    public Stream<TrackReachability> visitTrackReachability(TrackReachability trackReachability) {
      return Stream.of(trackReachability);
    }

    @Override
    public Stream<TrackReachability> visitTrackRoute(TrackRoute trackRoute) {
      return Stream.of();
    }

    @Override
    public Stream<TrackReachability> visitTrackTrue(TrackTrue trackTrue) {
      return Stream.of();
    }
  }

  /**
   * Returns map: hostname of config with at least one {@link TrackRoute} -> {@link TrackRoute}s in
   * that config.
   */
  private static @Nonnull Map<String, Collection<TrackRoute>> collectTrackRoutes(
      Map<String, Configuration> configurations) {
    ImmutableMap.Builder<String, Collection<TrackRoute>> builder = ImmutableMap.builder();
    configurations.forEach(
        (hostname, c) -> {
          Collection<TrackRoute> trackRoutes =
              c.getTrackingGroups().values().stream()
                  .flatMap(TRACK_ROUTE_COLLECTOR::visit)
                  .collect(ImmutableSet.toImmutableSet());
          if (!trackRoutes.isEmpty()) {
            builder.put(hostname, trackRoutes);
          }
        });
    return builder.build();
  }

  private static final TrackRouteCollector TRACK_ROUTE_COLLECTOR = new TrackRouteCollector();

  private static final class TrackRouteCollector
      implements GenericTrackMethodVisitor<Stream<TrackRoute>> {

    @Override
    public Stream<TrackRoute> visitNegatedTrackMethod(NegatedTrackMethod negatedTrackMethod) {
      return visit(negatedTrackMethod.getTrackMethod());
    }

    @Override
    public Stream<TrackRoute> visitTrackAll(TrackAll trackAll) {
      return trackAll.getConjuncts().stream().flatMap(this::visit);
    }

    @Override
    public Stream<TrackRoute> visitTrackInterface(TrackInterface trackInterface) {
      return Stream.of();
    }

    @Override
    public Stream<TrackRoute> visitTrackMethodReference(TrackMethodReference trackMethodReference) {
      // target will be found elsewhere
      return Stream.of();
    }

    @Override
    public Stream<TrackRoute> visitTrackReachability(TrackReachability trackReachability) {
      return Stream.of();
    }

    @Override
    public Stream<TrackRoute> visitTrackRoute(TrackRoute trackRoute) {
      return Stream.of(trackRoute);
    }

    @Override
    public Stream<TrackRoute> visitTrackTrue(TrackTrue trackTrue) {
      return Stream.of();
    }
  }

  /**
   * Create a provider for data-plane-based track evaluation, which depends in general on the
   * contents of FIBs and RIBs.
   *
   * <p>Evaluation is currently performed in the following places:
   *
   * <ul>
   *   <li>Constructor of {@link DataPlaneIpOwners}. This happens between iterations, so is thread
   *       safe with respect to RIBs and FIBs.
   *   <li>{@link VirtualRouter#activateStaticRoutes}. This happens during evaluation of a parallel
   *       stream over all {@link VirtualRouter}s that modifies RIBs. In order to achieve
   *       thread-safety in the case where a {@link org.batfish.datamodel.tracking.TrackRoute}
   *       depends on information in a different VRF than that containing the static route, the
   *       evaulator must have an immutable view of the RIB being inspected. So we should depend on
   *       the routes from the beginning of the iteration (note we are only able to supply FIBs from
   *       the beginning of an iteration anyway). Since saving routes of a VRF can be expensive, we
   *       instead use pre-evaluated {@link org.batfish.datamodel.tracking.TrackRoute} and {@link
   *       org.batfish.datamodel.tracking.TrackReachability} results here.
   * </ul>
   */
  private static @Nonnull DataPlaneTrackMethodEvaluatorProvider nextTrackMethodEvaluatorProvider(
      Table<String, TrackReachability, Boolean> trackReachabilityResults,
      Table<String, TrackRoute, Boolean> trackRouteResults) {
    return DataplaneTrackEvaluator.createTrackMethodEvaluatorProvider(
        trackReachabilityResults, trackRouteResults);
  }

  private @Nonnull Table<String, TrackReachability, Boolean> nextTrackReachabilityResults(
      PartialDataplane dp,
      TopologyContext topologyContext,
      Map<String, Configuration> configurations,
      Map<String, Collection<TrackReachability>> trackReachabilitiesByHostname) {
    TracerouteEngine tr =
        new TracerouteEngineImpl(dp, topologyContext.getLayer3Topology(), configurations);
    ImmutableTable.Builder<String, TrackReachability, Boolean> trackReachabilityResults =
        ImmutableTable.builder();
    trackReachabilitiesByHostname.forEach(
        (hostname, trackReachabilities) -> {
          Configuration config = configurations.get(hostname);
          Map<String, Fib> fibs = dp.getFibs().get(hostname);
          if (fibs == null) {
            // Should not happen for a full dataplane; a host with a track must have a FIB. Rather
            // than silently skipping the track (which would later NPE) or evaluating against
            // nothing, fail loudly.
            throw new IllegalStateException(
                String.format(
                    "Cannot evaluate TrackReachability for %s: no FIB present in the dataplane",
                    hostname));
          }
          if (!hasCompleteFibForTrackReachability(hostname)) {
            // Evaluating a track against a stub FIB would produce a silently wrong answer (e.g. it
            // would claim a destination is unreachable, deactivating tracked static routes). Fail
            // loudly instead of guessing; prepareDataPlane is responsible for not getting here.
            throw new IllegalStateException(
                String.format(
                    "Cannot evaluate TrackReachability for %s: its FIB in this dataplane is"
                        + " incomplete (a stub). Owned-only FIBs must be disabled when a remote"
                        + " host has a TrackReachability.",
                    hostname));
          }
          trackReachabilities.forEach(
              trackReachability ->
                  trackReachabilityResults.put(
                      hostname,
                      trackReachability,
                      evaluateTrackReachability(trackReachability, config, fibs, tr)));
        });
    return trackReachabilityResults.build();
  }

  @VisibleForTesting
  static boolean evaluateTrackRoute(TrackRoute trackRoute, Node node) {
    return switch (trackRoute.getRibType()) {
      case BGP ->
          TrackRouteUtils.evaluateTrackRoute(
              trackRoute,
              Optional.ofNullable(
                      node.getVirtualRouter(trackRoute.getVrf()).get().getBgpRoutingProcess())
                  .<GetRoutesForPrefix<Bgpv4Route>>map(brp -> brp._bgpv4Rib::getRoutes)
                  .orElse(TrackRouteUtils::emptyGetRoutesForPrefix));
      case MAIN ->
          TrackRouteUtils.evaluateTrackRoute(
              trackRoute, node.getVirtualRouter(trackRoute.getVrf()).get().getMainRib()::getRoutes);
    };
  }

  /**
   * Perform one iteration of the "dependent routes" dataplane computation. Dependent routes refers
   * to routes that could change because other routes have changed. For example, this includes:
   *
   * <ul>
   *   <li>static routes with next hop IP
   *   <li>aggregate routes
   *   <li>EGP routes (various protocols)
   * </ul>
   *
   * @param vrs virtual routers that are participating in the computation
   * @param iterationLabel iteration label (for stats tracking)
   * @param allNodes all nodes in the network (for correct neighbor referencing)
   */
  private void computeDependentRoutesIteration(
      List<VirtualRouter> vrs,
      String iterationLabel,
      Map<String, Node> allNodes,
      NetworkConfigurations networkConfigurations,
      DataPlaneTrackMethodEvaluatorProvider provider,
      int iteration) {
    LOGGER.info("{}: Compute dependent routes", iterationLabel);

    // No worker may start pulling this step's advertisements until every worker has finished the
    // previous step's writes (endOfEgpInnerRound).
    synchronizeWorkers();

    // Static nextHopIp routes
    LOGGER.info("{}: Recompute conditional static routes", iterationLabel);
    vrs.parallelStream()
        .forEach(vr -> vr.activateStaticRoutes(provider.forConfiguration(vr.getConfiguration())));

    // Generated/aggregate routes
    LOGGER.info("{}: Recompute aggregate/generated routes", iterationLabel);
    vrs.parallelStream().forEach(VirtualRouter::recomputeGeneratedRoutes);

    // EIGRP
    LOGGER.info("{}: Propagate EIGRP routes", iterationLabel);
    vrs.parallelStream().forEach(vr -> vr.eigrpIteration(allNodes, networkConfigurations));
    synchronizeWorkers();
    vrs.parallelStream().forEach(VirtualRouter::mergeEigrpRoutesToMainRib);

    // Re-initialize IS-IS exports.
    LOGGER.info("{}: Recompute IS-IS routes", iterationLabel);
    vrs.parallelStream()
        .forEach(vr -> vr.initIsisExports(iteration, allNodes, networkConfigurations));
    synchronizeWorkers();

    // IS-IS route propagation
    boolean isisChanged = true;
    int isisSubIterations = 0;
    while (isisChanged) {
      isisSubIterations++;
      LOGGER.info("{}: Recompute IS-IS routes: subIteration {}", iterationLabel, isisSubIterations);
      synchronizeWorkers();
      AtomicBoolean localIsisChanged = new AtomicBoolean(false);
      vrs.parallelStream()
          .forEach(
              vr -> {
                Entry<RibDelta<IsisRoute>, RibDelta<IsisRoute>> p =
                    vr.propagateIsisRoutes(networkConfigurations);
                if (p != null
                    && vr.unstageIsisRoutes(
                        allNodes, networkConfigurations, p.getKey(), p.getValue())) {
                  localIsisChanged.set(true);
                }
              });
      isisChanged = hasNotReachedIgpFixedPoint(localIsisChanged.get());
    }

    LOGGER.info("{}: Propagate OSPF external", iterationLabel);
    synchronizeWorkers();
    vrs.parallelStream().forEach(vr -> vr.ospfIteration(allNodes, networkConfigurations));
    synchronizeWorkers();
    vrs.parallelStream().forEach(VirtualRouter::mergeOspfRoutesToMainRib);

    computeIterationOfBgpRoutes(iterationLabel, allNodes, vrs, networkConfigurations);

    leakAcrossVrfs(vrs, iterationLabel);

    // Every node has finished pulling its neighbors' advertisements for this schedule step. A
    // distributed worker must not overwrite its neighbor-visible BGP deltas (endOfEgpInnerRound)
    // while a peer is still reading them, so synchronize all workers before this step's write.
    synchronizeWorkers();

    // Tell each VR that a BGP route computation inner round (schedule) has ended.
    vrs.parallelStream().forEach(VirtualRouter::endOfEgpInnerRound);
  }

  /**
   * Synchronization point between computation phases. The stock engine relies on each {@code
   * parallelStream().forEach(...)} phase completing before the next begins, so that no node writes
   * state another node reads. A distributed engine overriding this must make it a global barrier
   * across workers. The default implementation is a no-op.
   */
  protected void synchronizeWorkers() {}

  /**
   * Exchange a locally computed iteration hashcode for a cluster-wide one. The stock engine uses
   * the local hashcode for oscillation detection; a distributed engine must combine all workers'
   * hashes so that schedule changes stay synchronized. The default implementation returns the local
   * value.
   */
  protected int exchangeIterationHashCode(int localHashCode) {
    return localHashCode;
  }

  /**
   * Decide whether the topology fixed point has been reached. The stock engine uses the local
   * result; a distributed engine must combine all workers' results (fixed point only if all agree)
   * so that they run the same number of topology iterations.
   */
  protected boolean hasReachedTopologyFixedPoint(boolean localConverged) {
    return localConverged;
  }

  /**
   * The schedule to start the inner route-computation loop with. The stock engine uses the
   * configured schedule; a distributed engine should pick one whose step count does not depend on
   * per-worker state, or its phase barriers will not line up across workers.
   */
  protected Schedule initialSchedule() {
    return _settings.getScheduleName();
  }

  /**
   * The schedule for the OSPF internal convergence loop. The stock engine uses the configured
   * schedule, exactly as it did before the distributed hooks were introduced, so the loop converges
   * in the same number of iterations. A distributed engine may instead return {@link Schedule#ALL}
   * so every worker takes the same steps: a NODE_COLORED schedule colors the worker's own (possibly
   * shadowed) topology and could yield a different number of steps per worker, which would
   * desynchronize the phase barriers.
   */
  protected Schedule ospfInternalSchedule() {
    return _settings.getScheduleName();
  }

  /**
   * Verify that every worker will take the same schedule steps for the current round, and if not,
   * return a schedule whose step count cannot differ across workers.
   *
   * <p>A colored schedule is computed from the topology the engine holds. A distributed engine
   * whose workers hold different topologies could color differently, which would misalign the
   * per-step phase barriers ({@link #synchronizeWorkers()}) and hang the run. A distributed engine
   * must use this hook to either prove all workers agree or fall back to a schedule that has the
   * same number of steps everywhere (e.g. {@link Schedule#ALL}). The stock single-engine
   * computation returns {@code schedule} unchanged.
   *
   * @param schedule the schedule just computed
   * @param scheduleSteps the schedule's ordered steps (each a map of node names to nodes)
   * @return the schedule to use; may be a fallback that differs from {@code schedule}
   */
  protected Schedule reconcileEgpSchedule(
      Schedule schedule, List<Map<String, Node>> scheduleSteps) {
    return schedule;
  }

  /**
   * Prefix shards for the EGP computation (S2 prefix sharding). Empty means a single unsharded
   * pass. When non-empty, the EGP fixpoint runs once per shard with BGP restricted to that shard
   * and each shard's BGP routes are externalized before the next shard, so only one shard is live
   * at a time.
   */
  protected List<PrefixSpace> egpPrefixShards() {
    return ImmutableList.of();
  }

  /** Hook for phase-level reporting (e.g. peak memory) during dataplane computation. No-op. */
  protected void reportPhase(String phase) {}

  /**
   * Decide the IGP (OSPF/IS-IS/RIP) convergence condition cluster-wide. The stock engine keeps
   * iterating while the local dirty flag is set; a distributed engine must OR the flags of every
   * worker, or workers finish the IGP fixpoint after different numbers of iterations.
   */
  protected boolean hasNotReachedIgpFixedPoint(boolean localDirty) {
    return localDirty;
  }

  private static void updateLayer3Vnis(List<VirtualRouter> vrs) {
    LOGGER.info("Update learned VTEP IPs for Layer3Vnis");
    vrs.parallelStream().forEach(VirtualRouter::updateLayer3Vnis);
  }

  private static void computeIterationOfBgpRoutes(
      String iterationLabel,
      Map<String, Node> allNodes,
      List<VirtualRouter> vrs,
      NetworkConfigurations nc) {
    LOGGER.info("{}: Init for new BGP iteration", iterationLabel);
    vrs.parallelStream().forEach(vr -> vr.bgpIteration(allNodes, nc));
    LOGGER.info("{}: Init BGP generated/aggregate routes", iterationLabel);
    // first let's initialize nodes-level generated/aggregate routes
    vrs.parallelStream().forEach(VirtualRouter::initBgpAggregateRoutes);

    LOGGER.info("{}: Propagate BGP v4 routes", iterationLabel);

    // Merge BGP routes from BGP process into the main RIB
    vrs.parallelStream().forEach(VirtualRouter::mergeBgpRoutesToMainRib);
  }

  private static void queueRoutesForCrossVrfLeaking(List<VirtualRouter> vrs) {
    LOGGER.info("Queueing routes to leak across VRFs");
    vrs.parallelStream().forEach(VirtualRouter::queueCrossVrfImports);
  }

  private static void leakAcrossVrfs(List<VirtualRouter> vrs, String iterationLabel) {
    LOGGER.info("{}: Leaking routes across VRFs", iterationLabel);
    vrs.parallelStream().forEach(VirtualRouter::processCrossVrfRoutes);
  }

  /**
   * Run {@link VirtualRouter#computeFib} on all virtual routers
   *
   * @param vrs all virtual routers
   */
  private void computeFibs(List<VirtualRouter> vrs) {
    LOGGER.info("Compute FIBs");
    vrs.parallelStream().forEach(VirtualRouter::computeFib);
  }

  /**
   * Compute the IGP portion of the dataplane.
   *
   * @param nodes A dictionary of configuration-wrapping Bdp nodes keyed by name
   * @param topologyContext The topology context in which various adjacencies are stored
   * @param ae The output answer element in which to store a report of the computation. Also
   */
  private void computeIgpDataPlane(
      SortedMap<String, Node> nodes,
      List<VirtualRouter> vrs,
      TopologyContext topologyContext,
      NetworkConfigurations nc,
      IncrementalBdpAnswerElement ae) {
    LOGGER.info("Compute IGP");
    int numOspfInternalIterations;

    /*
     * For each virtual router, setup the initial easy-to-do routes, init protocol-based RIBs,
     * queue outgoing messages to neighbors
     */
    LOGGER.info("Initialize for IGP computation");
    vrs.parallelStream().forEach(vr -> vr.initForIgpComputation(topologyContext));
    // initForIgpComputation queues outgoing messages to neighbors; let every worker finish before
    // any worker starts consuming them.
    synchronizeWorkers();
    reportPhase("after initForIgpComputation");

    // Apply rib-groups sequentially to avoid concurrent writes to same destination RIB
    LOGGER.info("Apply rib-groups for IGP");
    vrs.stream().forEach(VirtualRouter::applyRibGroupsForIgp);
    reportPhase("after applyRibGroupsForIgp");

    // OSPF internal routes
    numOspfInternalIterations =
        initOspfInternalRoutes(nodes, topologyContext.getOspfTopology(), nc);

    // RIP internal routes
    initRipInternalRoutes(nodes, vrs, topologyContext.getLayer3Topology());

    // Activate static routes
    LOGGER.info("Compute static routes post IGP convergence");
    vrs.parallelStream()
        .forEach(
            vr -> {
              // Use static evaluator since we don't have dataplane yet
              vr.activateStaticRoutes(new PreDataPlaneTrackMethodEvaluator(vr.getConfiguration()));
            });

    // Set iteration stats in the answer
    ae.setOspfInternalIterations(numOspfInternalIterations);
  }

  /**
   * Compute the EGP portion of the route exchange. Must be called after IGP routing has converged.
   *
   * @param nodes A dictionary of configuration-wrapping Bdp nodes keyed by name
   * @param ae The output answer element in which to store a report of the computation. Also
   *     contains the current recovery iteration.
   * @param topologyContext The various network topologies
   * @return true iff the computation is oscillating
   */
  private boolean computeNonMonotonicPortionOfDataPlane(
      SortedMap<String, Node> nodes,
      List<VirtualRouter> vrs,
      IncrementalBdpAnswerElement ae,
      TopologyContext topologyContext,
      Topology initialLayer3Topology,
      IpOwners ipOwners,
      NetworkConfigurations networkConfigurations,
      DataPlaneTrackMethodEvaluatorProvider provider) {
    LOGGER.info("Compute EGP");
    /*
     * Initialize all routers and their message queues (can be done as parallel as possible)
     */
    LOGGER.info("Initialize virtual routers with updated topologies");
    vrs.parallelStream()
        .forEach(vr -> vr.initForEgpComputationWithNewTopology(topologyContext, provider));

    LOGGER.info("Compute HMM routes");
    Map<String, Map<String, Set<Ip>>> interfaceOwners = ipOwners.getInterfaceOwners(true);
    vrs.parallelStream().forEach(vr -> vr.computeHmmRoutes(initialLayer3Topology, interfaceOwners));

    LOGGER.info("Compute kernel routes");
    vrs.parallelStream()
        .forEach(vr -> vr.computeConditionalKernelRoutes(ipOwners.getIpVrfOwners()));

    /*
     * Setup maps to track iterations. We need this for oscillation detection.
     * Specifically, if we detect that an iteration hashcode (a hash of all the nodes' RIBs)
     * has been previously encountered, we switch our schedule to a more restrictive one.
     */

    // S2 prefix sharding: run the EGP fixpoint once per prefix shard (or a single unsharded pass
    // when
    // disabled), externalizing each shard's BGP routes in between so only one shard is live at a
    // time. The union over shards equals the unsharded result.
    List<PrefixSpace> prefixShards = egpPrefixShards();
    boolean sharded = prefixShards.size() > 1;
    List<List<byte[]>> cachedByVr = new ArrayList<>();
    if (sharded) {
      for (int i = 0; i < vrs.size(); i++) {
        cachedByVr.add(new ArrayList<>());
      }
    }
    int numRounds = sharded ? prefixShards.size() : 1;
    for (int round = 0; round < numRounds; round++) {
      PrefixSpace shard = sharded ? prefixShards.get(round) : null;
      if (sharded) {
        LOGGER.info("Prefix round {} of {}: {}", round + 1, numRounds, shard);
        appointPrefixSpace(vrs, shard);
        PrefixSpace roundShard = shard;
        vrs.parallelStream().forEach(vr -> vr.initForEgpPrefixRound(roundShard));
      }
      if (runEgpFixpoint(nodes, vrs, ae, topologyContext, networkConfigurations, provider)) {
        return true; // Found an oscillation
      }
      if (sharded) {
        System.err.printf(
            "S2 prefix sharding: round %d/%d live BGP routes %d%n",
            round + 1, numRounds, vrs.stream().mapToInt(vr -> vr.getBgpRoutes().size()).sum());
      }
      if (sharded && externalize()) {
        // Externalize this shard's BGP routes and drop them from the RIBs.
        for (int i = 0; i < vrs.size(); i++) {
          cachedByVr.get(i).add(serializeBgpRoutes(vrs.get(i).drainBgpRoutes()));
        }
        appointPrefixSpace(vrs, null);
      }
    }
    if (sharded && externalize()) {
      appointPrefixSpace(vrs, null);
      for (int i = 0; i < vrs.size(); i++) {
        for (byte[] payload : cachedByVr.get(i)) {
          vrs.get(i).restoreBgpRoutes(deserializeBgpRoutes(payload));
        }
      }
    }
    if (sharded) {
      System.err.printf(
          "S2 prefix sharding: %d rounds, total BGP routes %d%n",
          numRounds, vrs.stream().mapToInt(vr -> vr.getBgpRoutes().size()).sum());
    }

    ae.setDependentRoutesIterations(_numIterations);
    return false; // No oscillations
  }

  /**
   * Run the EGP fixpoint to convergence for the currently appointed prefix space. Returns true if
   * the network oscillates.
   */
  private boolean runEgpFixpoint(
      SortedMap<String, Node> nodes,
      List<VirtualRouter> vrs,
      IncrementalBdpAnswerElement ae,
      TopologyContext topologyContext,
      NetworkConfigurations networkConfigurations,
      DataPlaneTrackMethodEvaluatorProvider provider) {
    Map<Integer, SortedSet<Integer>> iterationsByHashCode = new HashMap<>();

    // C1: on a cyclic equal-cost topology the EGP fixed point depends on the schedule, because the
    // default ARRIVAL_ORDER BGP tie-breaker consumes the order in which equal-cost advertisements
    // are merged. Vanilla Batfish uses the deterministic NODE_COLORED schedule. The S2 engine
    // defaults to NODE_COLORED too (initialSchedule); -Ds2.egpSchedule=ALL is the escape hatch that
    // restores the historical single-round ALL schedule.
    String scheduleOverride = System.getProperty("s2.egpSchedule");
    Schedule currentSchedule =
        scheduleOverride == null ? initialSchedule() : Schedule.valueOf(scheduleOverride);
    // The node schedule depends on the nodes, the topology, and the schedule type. Within a round
    // only the type can change, on oscillation, so compute the schedule once per type.
    List<Map<String, Node>> scheduleSteps = null;
    Schedule scheduleStepsFor = null;

    // Go into iteration mode, until the routes converge (or oscillation is detected)
    do {
      _numIterations++;
      LOGGER.info("Iteration {} begins", _numIterations);
      if (scheduleSteps == null || scheduleStepsFor != currentSchedule) {
        LOGGER.info("Compute schedule");
        scheduleSteps =
            IbdpSchedule.getSchedule(_settings, currentSchedule, nodes, topologyContext)
                .getAllRemaining();
        // A distributed engine may have colored its own topology differently from its peers. Any
        // such disagreement would give the workers different numbers of steps, so their per-step
        // barriers would no longer line up. Reconcile before the first barrier of the round; a
        // fallback must be a schedule whose step count is the same on every worker.
        Schedule reconciled = reconcileEgpSchedule(currentSchedule, scheduleSteps);
        if (reconciled != currentSchedule) {
          LOGGER.warn(
              "EGP schedule {} is not consistent across workers; falling back to {}",
              currentSchedule,
              reconciled);
          currentSchedule = reconciled;
          scheduleSteps =
              IbdpSchedule.getSchedule(_settings, currentSchedule, nodes, topologyContext)
                  .getAllRemaining();
        }
        scheduleStepsFor = currentSchedule;
      }

      // (Re)initialization of dependent route calculation
      //  Since this is a local step, coloring not required.

      LOGGER.info("Re-Init for new route iteration");
      vrs.parallelStream().forEach(VirtualRouter::reinitForNewIteration);

      /*
      Redistribution: take all the routes merged into the main RIB during previous iteration
      and offer them to each routing process.

      This must be called before any `executeIteration` calls on any routing process.
      Since this is a local step, coloring not required.
      */
      LOGGER.info("Redistribute");
      vrs.parallelStream().forEach(VirtualRouter::redistribute);

      // Handle process-specific route resolution and cross-VRF leaking here too.
      vrs.parallelStream().forEach(VirtualRouter::updateResolvableRoutes);
      queueRoutesForCrossVrfLeaking(vrs);

      // compute dependent routes for each allowable set of nodes until we cover all nodes
      int nodeSet = 0;
      for (Map<String, Node> iterationNodes : scheduleSteps) {
        List<VirtualRouter> iterationVrs =
            toListInRandomOrder(
                iterationNodes.values().stream().flatMap(n -> iterationVirtualRouters(n).stream()));
        String iterationlabel = String.format("Iteration %d Schedule %d", _numIterations, nodeSet);
        computeDependentRoutesIteration(
            iterationVrs, iterationlabel, nodes, networkConfigurations, provider, _numIterations);
        ++nodeSet;
      }

      // All nodes have finished reading their neighbors' advertisements for this iteration. Let
      // every worker reach this point before anyone runs endOfEgpRound, which clears the neighbor-
      // visible main-RIB snapshots. Without this, a distributed worker could clear its snapshots
      // while a peer is still pulling them.
      synchronizeWorkers();

      // Tell each VR that a route computation round has ended.
      // This must be the last thing called on a VR in a routing round.
      vrs.parallelStream().forEach(VirtualRouter::endOfEgpRound);

      /*
       * Perform various bookkeeping at the end of the iteration:
       * - Collect sizes of certain RIBs this iteration
       * - Compute iteration hashcode
       * - Check for oscillations
       */
      computeIterationStatistics(vrs, ae, _numIterations);

      // This hashcode uniquely identifies the iteration (i.e., network state). A distributed engine
      // must make it global so that every worker detects oscillation at the same iteration and
      // therefore switches schedule together.
      int iterationHashCode = exchangeIterationHashCode(computeIterationHashCode(vrs));
      SortedSet<Integer> iterationsWithThisHashCode =
          iterationsByHashCode.computeIfAbsent(iterationHashCode, h -> new TreeSet<>());

      if (iterationsWithThisHashCode.isEmpty()) {
        iterationsWithThisHashCode.add(_numIterations);
      } else {
        // If oscillation detected, switch to a more restrictive schedule
        if (currentSchedule != Schedule.NODE_SERIALIZED) {
          LOGGER.debug(
              "Switching to a more restrictive schedule {}, iteration {}",
              Schedule.NODE_SERIALIZED,
              _numIterations);
          currentSchedule = Schedule.NODE_SERIALIZED;
        } else {
          return true; // Found an oscillation
        }
      }
    } while (hasNotReachedRoutingFixedPoint(vrs));

    return false; // No oscillations
  }

  /** Appoint (or clear, with null) the prefix space for every VR's BGP computation. */
  private static void appointPrefixSpace(List<VirtualRouter> vrs, PrefixSpace space) {
    vrs.parallelStream().forEach(vr -> vr.setAppointedPrefixSpace(space));
  }

  private static byte[] serializeBgpRoutes(Set<Bgpv4Route> routes) {
    try (ByteArrayOutputStream baos = new ByteArrayOutputStream();
        ObjectOutputStream oos = new ObjectOutputStream(baos)) {
      oos.writeObject(new HashSet<>(routes));
      oos.flush();
      return baos.toByteArray();
    } catch (IOException e) {
      throw new RuntimeException("Failed to externalize BGP routes", e);
    }
  }

  @SuppressWarnings("unchecked")
  private static Set<Bgpv4Route> deserializeBgpRoutes(byte[] payload) {
    try (ObjectInputStream ois = new ObjectInputStream(new ByteArrayInputStream(payload))) {
      return (Set<Bgpv4Route>) ois.readObject();
    } catch (IOException | ClassNotFoundException e) {
      throw new RuntimeException("Failed to restore BGP routes", e);
    }
  }

  /** Check if we have reached a routing fixed point */
  protected boolean hasNotReachedRoutingFixedPoint(List<VirtualRouter> vrs) {
    LOGGER.info("Iteration {}: Check if fixed point reached", _numIterations);
    return vrs.parallelStream().anyMatch(VirtualRouter::isDirty);
  }

  /**
   * Compute the hashcode that uniquely identifies the state of the network at a given iteration
   *
   * @param vrs all virtual routers in the network
   * @return integer hashcode
   */
  private int computeIterationHashCode(List<VirtualRouter> vrs) {
    LOGGER.info("Iteration {}: Compute hashCode", _numIterations);
    return vrs.parallelStream().mapToInt(VirtualRouter::computeIterationHashCode).sum();
  }

  private static void computeIterationStatistics(
      List<VirtualRouter> vrs, IncrementalBdpAnswerElement ae, int dependentRoutesIterations) {
    LOGGER.info("Iteration {}: Compute statistics", dependentRoutesIterations);
    int numBgpBestPathRibRoutes =
        vrs.parallelStream().mapToInt(VirtualRouter::getNumBgpBestPaths).sum();
    ae.getBgpBestPathRibRoutesByIteration().put(dependentRoutesIterations, numBgpBestPathRibRoutes);
    int numBgpMultipathRibRoutes =
        vrs.parallelStream().mapToInt(VirtualRouter::getNumBgpPaths).sum();
    ae.getBgpMultipathRibRoutesByIteration()
        .put(dependentRoutesIterations, numBgpMultipathRibRoutes);
    int numMainRibRoutes =
        vrs.parallelStream().mapToInt(vr -> vr.getMainRib().getNumRoutes()).sum();
    ae.getMainRibRoutesByIteration().put(dependentRoutesIterations, numMainRibRoutes);
  }

  /**
   * Return the main RIB routes for each node. Map structure: Hostname -&gt; VRF name -&gt; Set of
   * routes
   */
  @VisibleForTesting
  static SortedMap<String, SortedMap<String, Set<AbstractRoute>>> getRoutes(
      IncrementalDataPlane dp) {
    // Scan through all Nodes and their VRFs, retrieve main rib routes
    return toImmutableSortedMap(
        dp.getRibs().rowMap(),
        Entry::getKey,
        nodeEntry ->
            toImmutableSortedMap(
                nodeEntry.getValue(),
                Entry::getKey,
                vrfEntry -> ImmutableSet.copyOf(vrfEntry.getValue().getRoutes())));
  }

  private static final int MAX_OSPF_INTERNAL_ITERATIONS = 100000;

  /**
   * Run the IGP OSPF computation until convergence.
   *
   * @param allNodes list of nodes for which to initialize the OSPF routes
   * @param ospfTopology graph of OSPF adjacencies
   * @return the number of iterations it took for internal OSPF routes to converge
   */
  private int initOspfInternalRoutes(
      Map<String, Node> allNodes, OspfTopology ospfTopology, NetworkConfigurations nc) {
    int ospfInternalIterations = 0;
    boolean dirty = true;

    while (dirty) {
      ospfInternalIterations++;
      LOGGER.info("OSPF internal: Iteration {}", ospfInternalIterations);
      // The schedule is a hook: stock uses the configured schedule, while the distributed engine
      // uses a single-step schedule so every worker takes the same steps. NODE_COLORED colors the
      // worker's own (possibly shadowed) topology and can yield a different number of steps per
      // worker, which would desynchronize the phase barriers.
      IbdpSchedule schedule =
          IbdpSchedule.getSchedule(
              _settings,
              ospfInternalSchedule(),
              allNodes,
              TopologyContext.builder().setOspfTopology(ospfTopology).build());

      while (schedule.hasNext()) {
        Map<String, Node> scheduleNodes = schedule.next();
        List<VirtualRouter> scheduleVrs =
            toListInRandomOrder(
                scheduleNodes.values().stream().flatMap(n -> iterationVirtualRouters(n).stream()));
        synchronizeWorkers();
        scheduleVrs.parallelStream()
            .forEach(virtualRouter -> virtualRouter.ospfIteration(allNodes, nc));
        synchronizeWorkers();
        scheduleVrs.parallelStream().forEach(VirtualRouter::mergeOspfRoutesToMainRib);
      }
      boolean localDirty =
          allNodes.values().parallelStream()
              .flatMap(n -> iterationVirtualRouters(n).stream())
              .flatMap(vr -> vr.getOspfProcesses().values().stream())
              .anyMatch(OspfRoutingProcess::isDirty);
      dirty = hasNotReachedIgpFixedPoint(localDirty);
      if (ospfInternalIterations > MAX_OSPF_INTERNAL_ITERATIONS) {
        throw new BdpOscillationException(
            "OSPF did not converge after " + MAX_OSPF_INTERNAL_ITERATIONS + " iterations");
      }
    }
    return ospfInternalIterations;
  }

  /**
   * Run the IGP RIP computation until convergence
   *
   * @param nodes nodes for which to initialize the routes, keyed by name
   * @param topology network topology
   */
  private void initRipInternalRoutes(
      SortedMap<String, Node> nodes, List<VirtualRouter> vrs, Topology topology) {
    /*
     * Consider this method to be a simulation within a simulation. Since RIP routes are not
     * affected by other protocols, we propagate all RIP routes amongst the nodes prior to
     * processing other routing protocols (e.g., OSPF & BGP)
     */
    boolean ripInternalChanged = true;
    int ripInternalIterations = 0;
    while (ripInternalChanged) {
      ripInternalIterations++;
      LOGGER.info("RIP internal: Iteration {}", ripInternalIterations);
      synchronizeWorkers();
      AtomicBoolean localChanged = new AtomicBoolean(false);
      vrs.parallelStream()
          .forEach(
              vr -> {
                if (vr.propagateRipInternalRoutes(nodes, topology)) {
                  localChanged.set(true);
                }
              });
      synchronizeWorkers();
      LOGGER.info("Unstage RIP internal: Iteration {}", ripInternalIterations);
      vrs.parallelStream().forEach(VirtualRouter::unstageRipInternalRoutes);

      synchronizeWorkers();
      LOGGER.info("Import RIP internal: Iteration {}", ripInternalIterations);
      vrs.parallelStream()
          .forEach(
              vr -> {
                importRib(vr._ripRib, vr._ripInternalRib);
                importRib(vr.getMainRib(), vr._ripRib, vr.getName());
              });
      ripInternalChanged = hasNotReachedIgpFixedPoint(localChanged.get());
    }
  }
}
