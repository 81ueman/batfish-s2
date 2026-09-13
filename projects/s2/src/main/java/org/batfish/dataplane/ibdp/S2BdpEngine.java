// SPDX-License-Identifier: Apache-2.0

package org.batfish.dataplane.ibdp;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SortedMap;
import java.util.stream.Stream;
import javax.annotation.Nullable;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.batfish.common.topology.IpOwners;
import org.batfish.datamodel.Configuration;
import org.batfish.datamodel.Prefix;
import org.batfish.datamodel.PrefixSpace;
import org.batfish.datamodel.Vrf;
import org.batfish.datamodel.tracking.GenericTrackMethodVisitor;
import org.batfish.datamodel.tracking.NegatedTrackMethod;
import org.batfish.datamodel.tracking.TrackAll;
import org.batfish.datamodel.tracking.TrackInterface;
import org.batfish.datamodel.tracking.TrackMethodReference;
import org.batfish.datamodel.tracking.TrackReachability;
import org.batfish.datamodel.tracking.TrackRoute;
import org.batfish.datamodel.tracking.TrackTrue;
import org.batfish.dataplane.ibdp.schedule.IbdpSchedule.Schedule;

/**
 * {@link IncrementalBdpEngine} that builds {@link DistributedNode}s supplied by the S2 controller
 * instead of plain {@link Node}s, and simulates only the nodes this worker owns.
 */
public class S2BdpEngine extends IncrementalBdpEngine {

  private static final Logger LOGGER = LogManager.getLogger(S2BdpEngine.class);

  /**
   * Serializes dataplane construction across engines running in the same JVM. Shadow nodes delegate
   * to real routers owned by other engines, and {@link VirtualRouter#computeFib()} transiently
   * nulls the FIB, so concurrent construction could observe a null FIB.
   */
  private static final Object DATAPLANE_LOCK = new Object();

  private final Map<String, DistributedNode> _nodes;
  private final S2Coordinator _coordinator;
  private final @Nullable Runnable _shadowSync;

  /** Networks of external BGP announcements: injected into the RIBs and thus shard-appointed. */
  private final Set<Prefix> _externalAdvertPrefixes;

  /**
   * Whether a worker builds and retains full tables (RIBs, FIBs) only for the nodes it owns. Remote
   * (shadow) nodes get a cheap config-only "stub" FIB so the forwarding analysis can still compute
   * the cross-node ARP state, but no full remote routing table is ever built.
   *
   * <p><b>Default on.</b> This is an S2-only mode: it is read here (and in {@code S2Main}) so it
   * never affects stock Batfish. Disable with {@code -Ds2.ownedDataplane=false} to restore the old
   * full-dataplane-per-worker behavior.
   *
   * <p>This is the *requested* mode. It is turned off for a run (see {@link #prepareDataPlane})
   * when the snapshot contains features that need complete remote FIBs (tracks, VXLAN/IPsec/tunnel
   * reachability pruning), because owned-only FIBs would give silently wrong answers there.
   */
  private final boolean _ownedDataplaneRequested =
      Boolean.parseBoolean(System.getProperty("s2.ownedDataplane", "true"));

  /**
   * The effective owned mode for the current run. Equals {@link #_ownedDataplaneRequested} unless
   * {@link #prepareDataPlane} disabled it as an unsafe fallback.
   */
  private boolean _ownedDataplane = _ownedDataplaneRequested;

  /**
   * Whether this run materializes reduced shadow configs from remote descriptors (default on,
   * disable with {@code -Ds2.descriptorShadows=false}). Remote shadow configs then carry no ACL /
   * policy bodies, so consumers that need a remote node's full forwarding state (the
   * dataplane-level BGP session reachability check) are disabled; {@code S2Main} gates the mode on
   * the snapshot being free of tracks and IPsec / tunnel / VXLAN reachability.
   */
  private final boolean _descriptorShadows;

  public S2BdpEngine(
      IncrementalDataPlaneSettings settings,
      Map<String, DistributedNode> nodes,
      S2Coordinator coordinator,
      @Nullable Runnable shadowSync) {
    this(settings, nodes, coordinator, shadowSync, ImmutableSet.of(), false);
  }

  public S2BdpEngine(
      IncrementalDataPlaneSettings settings,
      Map<String, DistributedNode> nodes,
      S2Coordinator coordinator,
      @Nullable Runnable shadowSync,
      Set<Prefix> externalAdvertPrefixes) {
    this(settings, nodes, coordinator, shadowSync, externalAdvertPrefixes, false);
  }

  public S2BdpEngine(
      IncrementalDataPlaneSettings settings,
      Map<String, DistributedNode> nodes,
      S2Coordinator coordinator,
      @Nullable Runnable shadowSync,
      Set<Prefix> externalAdvertPrefixes,
      boolean descriptorShadows) {
    super(settings);
    _nodes = nodes;
    _coordinator = coordinator;
    _shadowSync = shadowSync;
    _externalAdvertPrefixes = externalAdvertPrefixes;
    _descriptorShadows = descriptorShadows;
  }

  @Override
  Node newNode(Configuration configuration) {
    DistributedNode node = _nodes.get(configuration.getHostname());
    if (node == null) {
      throw new IllegalStateException("No S2 node prepared for " + configuration.getHostname());
    }
    return node;
  }

  /** Only real nodes are simulated locally; shadows are visible for lookups but inert. */
  @Override
  Collection<VirtualRouter> iterationVirtualRouters(Node node) {
    return ((DistributedNode) node).isShadow() ? ImmutableList.of() : node.getVirtualRouters();
  }

  /**
   * S2 convergence is global: a worker must keep computing until no real router anywhere is dirty,
   * otherwise it may stop before a remote worker has finished propagating its routes.
   */
  @Override
  protected boolean hasNotReachedRoutingFixedPoint(List<VirtualRouter> vrs) {
    return _coordinator.roundCheck(super.hasNotReachedRoutingFixedPoint(vrs));
  }

  /**
   * The stock engine relies on phase boundaries (a {@code parallelStream} phase finishing before
   * the next begins) to order reads of a neighbor's state against writes to it. In a distributed
   * run a worker can otherwise, say, overwrite its neighbor-visible BGP deltas while a peer is
   * still pulling them, which makes convergence order-dependent. Make every such boundary a global
   * barrier.
   */
  @Override
  protected void synchronizeWorkers() {
    _coordinator.roundCheck(false);
  }

  /**
   * Oscillation detection must be cluster-wide: each worker only sees its own routers, so a purely
   * local hashcode would let one worker switch to a stricter schedule while others do not, which
   * desynchronizes the phase barriers above. Sum the hashes across workers instead.
   */
  @Override
  protected int exchangeIterationHashCode(int localHashCode) {
    return _coordinator.sumAll(localHashCode);
  }

  /**
   * Topology convergence is global too: a worker whose shadows look stale may want another topology
   * iteration after a peer has already exited. Treat the topology as converged only when every
   * worker says so.
   */
  @Override
  protected boolean hasReachedTopologyFixedPoint(boolean localConverged) {
    return !_coordinator.roundCheck(!localConverged);
  }

  /** IGP convergence (OSPF/IS-IS/RIP) is global for the same reason as the EGP fixed point. */
  @Override
  protected boolean hasNotReachedIgpFixedPoint(boolean localDirty) {
    return _coordinator.roundCheck(localDirty);
  }

  /**
   * Use the same deterministic schedule vanilla Batfish uses. The S2 engine previously started from
   * {@link Schedule#ALL} because a worker colored its own (partially shadowed) topology and workers
   * could then disagree on the number of color classes, misaligning the per-step phase barriers.
   * That is handled two ways now:
   *
   * <ol>
   *   <li>Every worker is built over the <em>full</em> node set (real + shadow) and computes its
   *       topology from that full config set, so {@link Schedule#NODE_COLORED}'s color classes are
   *       identical across workers.
   *   <li>{@link #reconcileEgpSchedule} fingerprints the schedule and exchanges it with the
   *       coordinator, falling back cluster-wide to {@link Schedule#ALL} if the workers ever
   *       disagree, so a mismatch can never deadlock the run.
   * </ol>
   *
   * <p>{@code -Ds2.egpSchedule=ALL} remains the escape hatch.
   */
  @Override
  protected Schedule initialSchedule() {
    return Schedule.NODE_COLORED;
  }

  /**
   * OSPF internal convergence must be phase-aligned across workers, so use the single-step {@link
   * Schedule#ALL}. Unlike the EGP loop, the OSPF internal loop has no schedule-reconciliation
   * fallback, and a worker colors its own (possibly shadowed) OSPF topology, so a NODE_COLORED
   * schedule could yield a different number of steps per worker and desynchronize the barriers.
   */
  @Override
  protected Schedule ospfInternalSchedule() {
    return Schedule.ALL;
  }

  /**
   * Make the EGP schedule safe across workers. Each worker colors the full node set + topology it
   * holds; that is the same on every worker today, but if a future topology made it differ the
   * per-step barriers would stop lining up and the run would hang. Fingerprint the ordered color
   * classes and exchange both the fingerprint and the worker count with the coordinator. If every
   * worker agrees, keep the schedule; otherwise fall back cluster-wide to the single-step {@link
   * Schedule#ALL}, whose step count is worker-independent.
   *
   * <p>All workers reach this at the same point in the round (the schedule is recomputed only on
   * the first iteration and on the synchronized oscillation switch), so the two exchange barriers
   * line up. The fingerprint is forced non-zero so a zero fingerprint cannot mask a disagreement.
   */
  @Override
  protected Schedule reconcileEgpSchedule(
      Schedule schedule, List<Map<String, Node>> scheduleSteps) {
    int fingerprint = scheduleFingerprint(scheduleSteps);
    int workers = _coordinator.sumAll(1);
    int sum = _coordinator.sumAll(fingerprint);
    return sum == fingerprint * workers ? schedule : Schedule.ALL;
  }

  /** Order-sensitive fingerprint of a schedule's per-step node-name groups. */
  private static int scheduleFingerprint(List<Map<String, Node>> scheduleSteps) {
    List<List<String>> groups = new ArrayList<>();
    for (Map<String, Node> step : scheduleSteps) {
      List<String> names = new ArrayList<>(step.keySet());
      Collections.sort(names);
      groups.add(names);
    }
    // Force the low bit so the fingerprint is never zero.
    return groups.hashCode() | 1;
  }

  @Override
  protected void reportPhase(String phase) {
    long peak = 0;
    for (java.lang.management.MemoryPoolMXBean pool :
        java.lang.management.ManagementFactory.getMemoryPoolMXBeans()) {
      if (pool.getType() == java.lang.management.MemoryType.HEAP) {
        java.lang.management.MemoryUsage usage = pool.getPeakUsage();
        if (usage != null) {
          peak += usage.getUsed();
        }
      }
    }
    System.err.printf(
        "S2 phase %s: peak heap %.1f MiB, t=%.1fs%n",
        phase,
        peak / 1048576.0,
        java.lang.management.ManagementFactory.getRuntimeMXBean().getUptime() / 1000.0);
  }

  private volatile List<PrefixSpace> _egpPrefixShards;

  /**
   * Control-plane prefix sharding (S2): with {@code S2_PREFIX_SHARDS=N > 1} (or {@code
   * -Ds2.prefixShards=N} / {@code -Ds2.prefixShardCount=N}), run the EGP fixpoint once per prefix
   * shard so only one shard's BGP RIB is live at a time. The shards are derived from the same
   * snapshot on every worker, so the rounds line up.
   *
   * <p>{@code S2_PREFIX_SHARDS=auto} instead selects N deterministically from the snapshot's prefix
   * dependency graph ({@link PrefixShardCountSelector}); see that class for the policy and its
   * budget knob.
   */
  @Override
  protected List<PrefixSpace> egpPrefixShards() {
    List<PrefixSpace> shards = _egpPrefixShards;
    if (shards == null) {
      synchronized (this) {
        shards = _egpPrefixShards;
        if (shards == null) {
          Map<String, Configuration> configs = new HashMap<>();
          _nodes.forEach((host, node) -> configs.put(host, node.getConfiguration()));
          String spec = prefixShardCountSpec();
          if (PrefixShardCountSelector.isAuto(spec)) {
            PrefixDependencyGraph graph =
                PrefixDependencyGraph.build(configs, _externalAdvertPrefixes);
            int n = PrefixShardCountSelector.select(graph);
            shards = n <= 1 ? ImmutableList.of() : PrefixSharder.shards(graph, n);
          } else {
            int n = parseShardCount(spec);
            shards =
                n <= 1
                    ? ImmutableList.of()
                    : PrefixSharder.shards(configs, _externalAdvertPrefixes, n);
          }
          _egpPrefixShards = shards;
        }
      }
    }
    return shards;
  }

  /**
   * The raw shard-count setting: {@code -Ds2.prefixShards} (original name), then the alias {@code
   * -Ds2.prefixShardCount}, then the {@code S2_PREFIX_SHARDS} environment variable, then {@code
   * "1"} (sharding off). A numeric value is a fixed shard count; {@code "auto"} selects it
   * deterministically from the prefix dependency graph.
   */
  private static String prefixShardCountSpec() {
    String spec = System.getProperty("s2.prefixShards");
    if (spec == null) {
      spec = System.getProperty("s2.prefixShardCount");
    }
    if (spec == null) {
      spec = System.getenv("S2_PREFIX_SHARDS");
    }
    return spec == null ? "1" : spec;
  }

  /** Parses a fixed shard count; values {@code <= 1} disable sharding. */
  private static int parseShardCount(String spec) {
    try {
      return Math.max(1, Integer.parseInt(spec.trim()));
    } catch (NumberFormatException e) {
      throw new IllegalArgumentException(
          "Invalid S2 prefix shard count '" + spec + "': expected a positive integer or \"auto\"",
          e);
    }
  }

  @Override
  protected PartialDataplane nextDataplane(
      TopologyContext currentTopologyContext,
      SortedMap<String, Node> nodes,
      List<VirtualRouter> vrs,
      IpOwners currentIpOwners) {
    synchronized (DATAPLANE_LOCK) {
      if (_ownedDataplane) {
        // Owned mode: a shadow only needs enough of a FIB for the forwarding analysis to compute
        // the cross-node ARP state (its interfaces' connected/local/static routes). Build that stub
        // instead of pulling and computing the remote node's full tables.
        nodes.forEach(
            (host, node) -> {
              boolean shadow = ((DistributedNode) node).isShadow();
              for (VirtualRouter vr : node.getVirtualRouters()) {
                if (shadow) {
                  vr.initStubFib();
                } else {
                  vr.computeFib();
                }
              }
            });
      } else {
        // Pull the owning workers' main RIBs into shadows so the forwarding analysis is complete.
        if (_shadowSync != null) {
          _shadowSync.run();
        }
        // Ensure every visible router has a FIB before the forwarding analysis is built over all
        // nodes.
        nodes.values().stream()
            .flatMap(n -> n.getVirtualRouters().stream())
            .forEach(VirtualRouter::computeFib);
      }
      return super.nextDataplane(currentTopologyContext, nodes, vrs, currentIpOwners);
    }
  }

  /**
   * Decide the effective owned mode for this run, before any node/dataplane is built.
   *
   * <p>Owned-only FIBs are unsafe whenever a computation needs a *remote* node's full forwarding
   * state in this worker. Two such consumers exist today:
   *
   * <ul>
   *   <li>{@code TrackReachability}: a track on a remote (shadow) host would be evaluated against
   *       that host's stub FIB, giving a silently wrong result and deactivating tracked routes.
   *   <li>IPsec / VXLAN / tunnel reachability pruning: the prune traceroutes between (possibly
   *       remote) endpoints; a stub FIB makes the prune unsound. VXLAN in particular can become
   *       non-empty at runtime purely from VNI settings, so detect configured VNIs up front.
   * </ul>
   *
   * <p>The decision is snapshot-wide, not per-worker: if any worker fell back while another stayed
   * owned, they would also disagree about the dataplane-level BGP session reachability check. So
   * when any of these is present we conservatively fall back to a full dataplane on every worker
   * and log why, rather than produce a silently wrong answer.
   */
  @Override
  protected void prepareDataPlane(
      Map<String, Configuration> configurations, TopologyContext initialTopologyContext) {
    if (!_ownedDataplaneRequested) {
      return;
    }
    String reason = ownedDataplaneUnsafeReason(configurations, initialTopologyContext);
    if (reason != null) {
      LOGGER.warn(
          "Disabling owned-only dataplane for this run (falling back to full RIBs/FIBs): {}",
          reason);
      _ownedDataplane = false;
    }
  }

  /**
   * Returns the reason owned-only FIBs are unsafe for this snapshot, or {@code null} if they are
   * fine. Only called when owned mode was requested.
   */
  private @Nullable String ownedDataplaneUnsafeReason(
      Map<String, Configuration> configurations, TopologyContext initialTopologyContext) {
    // 1. TrackReachability anywhere in the snapshot. A track on a remote (shadow) node would be
    //    evaluated against that node's stub FIB. We disable owned mode cluster-wide (not just on
    //    the worker that happens to shadow the tracked node) so every worker makes the same
    //    decision: in particular, the dataplane-level BGP session reachability check must not
    //    differ across workers.
    for (Configuration c : configurations.values()) {
      if (hasTrackReachability(c)) {
        return String.format("node %s has a TrackReachability", c.getHostname());
      }
    }
    // 2. VXLAN: VNI settings can yield VXLAN topology edges at runtime even if the initial VXLAN
    //    topology is empty, so any configured layer2/layer3 VNI counts.
    for (Configuration c : configurations.values()) {
      for (Vrf vrf : c.getVrfs().values()) {
        if (!vrf.getLayer2Vnis().isEmpty() || !vrf.getLayer3Vnis().isEmpty()) {
          return String.format("node %s defines VXLAN VNIs", c.getHostname());
        }
      }
    }
    // 3. IPsec and tunnel topologies are pruned from the initial (candidate) edges.
    if (!initialTopologyContext.getIpsecTopology().getGraph().edges().isEmpty()) {
      return "the snapshot has an IPsec topology";
    }
    if (!initialTopologyContext.getTunnelTopology().getGraph().edges().isEmpty()) {
      return "the snapshot has a tunnel topology";
    }
    if (!initialTopologyContext.getVxlanTopology().getGraph().edges().isEmpty()) {
      return "the snapshot has a VXLAN topology";
    }
    return null;
  }

  /**
   * In owned mode the partial dataplane has only stub FIBs for remote nodes, so traceroute-based
   * topology pruning is unsound; {@link #prepareDataPlane} ensures this is only reached when the
   * corresponding topologies are provably empty.
   */
  @Override
  protected boolean canUseTracerouteForDataplaneTopologyPruning() {
    return !_ownedDataplane;
  }

  /**
   * Owned mode's remote FIBs are stubs, so tracks must never be evaluated against them. This is a
   * defensive per-host check: {@link #prepareDataPlane} disables owned mode for the whole snapshot
   * as soon as any track exists, so in practice this only guards against a future caller that
   * re-enables owned mode while tracks are present.
   */
  @Override
  protected boolean hasCompleteFibForTrackReachability(String hostname) {
    if (!_ownedDataplane) {
      return true;
    }
    DistributedNode node = _nodes.get(hostname);
    return node == null || !node.isShadow();
  }

  /**
   * In owned mode the final dataplane result contains only this worker's nodes, so remote final
   * RIBs are never retained. The partial dataplane still covers all nodes (with stub FIBs).
   */
  @Override
  protected Map<String, Node> dataPlaneNodes(Map<String, Node> nodes) {
    if (!_ownedDataplane) {
      return nodes;
    }
    Map<String, Node> owned = new HashMap<>();
    nodes.forEach(
        (host, node) -> {
          if (!((DistributedNode) node).isShadow()) {
            owned.put(host, node);
          }
        });
    return owned;
  }

  /**
   * Owned mode has no full remote FIBs, so dataplane-level BGP session reachability checks are
   * skipped (sessions are established from configuration + L3 adjacency, sufficient for
   * directly-connected peering). This is only valid while owned mode is actually in effect: if
   * {@link #prepareDataPlane} disabled it, the check runs as in stock Batfish. Stock behavior keeps
   * the check.
   *
   * <p>Descriptor mode likewise skips it: a remote shadow config carries no ACL bodies, so a
   * reachability check that traverses a remote node could disagree with the full configuration.
   */
  @Override
  protected boolean checkBgpSessionReachability() {
    return !_ownedDataplane && !_descriptorShadows;
  }

  /** Whether any tracking group on {@code c} is (or contains) a {@link TrackReachability}. */
  static boolean hasTrackReachability(Configuration c) {
    return c.getTrackingGroups().values().stream()
        .flatMap(TRACK_REACHABILITY_COLLECTOR::visit)
        .findAny()
        .isPresent();
  }

  /** Extracts the {@link TrackReachability}s reachable from a track method. */
  private static final GenericTrackMethodVisitor<Stream<TrackReachability>>
      TRACK_REACHABILITY_COLLECTOR =
          new GenericTrackMethodVisitor<Stream<TrackReachability>>() {
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
              // The referenced method is inspected where it is defined.
              return Stream.of();
            }

            @Override
            public Stream<TrackReachability> visitTrackReachability(
                TrackReachability trackReachability) {
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
          };
}
