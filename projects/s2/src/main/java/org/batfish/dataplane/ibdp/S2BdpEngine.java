package org.batfish.dataplane.ibdp;

import com.google.common.collect.ImmutableList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.SortedMap;
import javax.annotation.Nullable;
import org.batfish.common.topology.IpOwners;
import org.batfish.datamodel.Configuration;
import org.batfish.datamodel.PrefixSpace;
import org.batfish.dataplane.ibdp.schedule.IbdpSchedule.Schedule;

/**
 * {@link IncrementalBdpEngine} that builds {@link DistributedNode}s supplied by the S2 controller
 * instead of plain {@link Node}s, and simulates only the nodes this worker owns.
 */
public class S2BdpEngine extends IncrementalBdpEngine {

  /**
   * Serializes dataplane construction across engines running in the same JVM. Shadow nodes delegate
   * to real routers owned by other engines, and {@link VirtualRouter#computeFib()} transiently
   * nulls the FIB, so concurrent construction could observe a null FIB.
   */
  private static final Object DATAPLANE_LOCK = new Object();

  private final Map<String, DistributedNode> _nodes;
  private final S2Coordinator _coordinator;
  private final @Nullable Runnable _shadowSync;

  public S2BdpEngine(
      IncrementalDataPlaneSettings settings,
      Map<String, DistributedNode> nodes,
      S2Coordinator coordinator,
      @Nullable Runnable shadowSync) {
    super(settings);
    _nodes = nodes;
    _coordinator = coordinator;
    _shadowSync = shadowSync;
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
   * Start with the {@link Schedule#ALL} schedule (one step) rather than the default {@code
   * NODE_COLORED} schedule. A worker computes its coloring from its own (partially shadowed)
   * topology, so different workers can get a different number of color classes; that would make the
   * per-step phase barriers line up incorrectly and deadlock. {@code ALL} has a single step and the
   * oscillation fallback {@code NODE_SERIALIZED} has one step per node, both of which are identical
   * across workers.
   */
  @Override
  protected Schedule initialSchedule() {
    return Schedule.ALL;
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
    System.err.printf("S2 phase %s: peak heap %.1f MiB%n", phase, peak / 1048576.0);
  }

  private volatile List<PrefixSpace> _egpPrefixShards;

  /**
   * Control-plane prefix sharding (S2): with {@code S2_PREFIX_SHARDS=N > 1}, run the EGP fixpoint
   * once per prefix shard so only one shard's BGP RIB is live at a time. The shards are derived
   * from the same snapshot on every worker, so the rounds line up.
   */
  @Override
  protected List<PrefixSpace> egpPrefixShards() {
    List<PrefixSpace> shards = _egpPrefixShards;
    if (shards == null) {
      synchronized (this) {
        shards = _egpPrefixShards;
        if (shards == null) {
          int n =
              Math.max(
                  1,
                  Integer.parseInt(
                      System.getProperty(
                          "s2.prefixShards",
                          System.getenv().getOrDefault("S2_PREFIX_SHARDS", "1"))));
          if (n <= 1) {
            shards = ImmutableList.of();
          } else {
            Map<String, Configuration> configs = new HashMap<>();
            _nodes.forEach((host, node) -> configs.put(host, node.getConfiguration()));
            shards = PrefixSharder.prefixSpaces(PrefixSharder.queryPrefixes(configs), n);
          }
          _egpPrefixShards = shards;
        }
      }
    }
    return shards;
  }

  @Override
  protected PartialDataplane nextDataplane(
      TopologyContext currentTopologyContext,
      SortedMap<String, Node> nodes,
      List<VirtualRouter> vrs,
      IpOwners currentIpOwners) {
    synchronized (DATAPLANE_LOCK) {
      // Pull the owning workers' main RIBs into shadows so the forwarding analysis is complete.
      if (_shadowSync != null) {
        _shadowSync.run();
      }
      // Ensure every visible router has a FIB before the forwarding analysis is built over all
      // nodes.
      nodes.values().stream()
          .flatMap(n -> n.getVirtualRouters().stream())
          .forEach(VirtualRouter::computeFib);
      return super.nextDataplane(currentTopologyContext, nodes, vrs, currentIpOwners);
    }
  }
}
