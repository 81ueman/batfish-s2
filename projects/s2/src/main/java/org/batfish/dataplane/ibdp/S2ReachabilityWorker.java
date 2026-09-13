package org.batfish.dataplane.ibdp;

import com.google.common.collect.Table;
import java.io.IOException;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import net.sf.javabdd.BDD;
import net.sf.javabdd.BDDTransfer;
import net.sf.javabdd.JFactory;
import org.batfish.bddreachability.BDDReachabilityAnalysis;
import org.batfish.bddreachability.transition.Transition;
import org.batfish.symbolic.state.EdgeStateExpr;
import org.batfish.symbolic.state.InterfaceStateExpr;
import org.batfish.symbolic.state.NodeStateExpr;
import org.batfish.symbolic.state.PacketPolicyAction;
import org.batfish.symbolic.state.PacketPolicyStatement;
import org.batfish.symbolic.state.Query;
import org.batfish.symbolic.state.StateExpr;
import org.batfish.symbolic.state.VrfStateExpr;

/**
 * One worker's part of the distributed symbolic reachability fixpoint (Batfish's backward
 * reachability).
 *
 * <p>Starts from the query header space at {@link Query} and propagates backward over the
 * reachability graph. Each worker processes only states on switches assigned to it; when a
 * predecessor state lives on another worker, the resulting BDD is serialized and shipped to that
 * worker over {@link S2BddSidecar}. Rounds are synchronized through {@link S2Coordinator}.
 */
public final class S2ReachabilityWorker {

  private final int _id;
  private final Map<String, Integer> _partition;
  private final JFactory _factory;
  private final Map<StateExpr, List<PreEdge>> _reverseEdges = new HashMap<>();
  private final S2BddSidecar.Client[] _clients;
  private final S2Coordinator _coordinator;

  private final Map<StateExpr, BDD> _reachable = new HashMap<>();
  private final Queue<StateExpr> _worklist = new ArrayDeque<>();
  private final List<Inbox> _inbox = new ArrayList<>();
  private final Map<Integer, List<Outbound>> _pending = new HashMap<>();

  /** The full query header space; shards further restrict this via {@link #run(BDD)}. */
  private final BDD _queryRoot;

  private int _edgeCount;

  private record PreEdge(StateExpr pre, Transition transition) {}

  private record Inbox(StateExpr state, String payload) {}

  private record Outbound(StateExpr state, BDD bdd) {}

  public S2ReachabilityWorker(
      int id,
      Map<String, Integer> partition,
      BDDReachabilityAnalysis analysis,
      S2BddSidecar.Client[] clients,
      S2Coordinator coordinator) {
    _id = id;
    _partition = partition;
    _factory = (JFactory) analysis.getBDDPacket().getFactory();
    _clients = clients;
    _coordinator = coordinator;
    // In the backward fixpoint only post states owned by this worker are ever resolved, so edges
    // whose destination is remote (including remote-remote edges) are dead weight. Keeping them out
    // is the per-worker scale saving. Boundary edges into local states are pulled from their owners
    // via addEdge once their transitions have been reconstructed locally.
    for (Table.Cell<StateExpr, StateExpr, Transition> cell :
        analysis.getForwardEdgeTable().cellSet()) {
      if (owner(cell.getColumnKey()) != id) {
        continue;
      }
      _reverseEdges
          .computeIfAbsent(cell.getColumnKey(), k -> new ArrayList<>())
          .add(new PreEdge(cell.getRowKey(), cell.getValue()));
      _edgeCount++;
    }
    _queryRoot = analysis.getQueryHeaderSpaceBdd().id();
  }

  /**
   * Adds a boundary edge pulled from the worker that owns {@code pre} (see {@code
   * S2Messages.BoundaryEdgesRequest}). Only relevant when the analysis was generated with an {@link
   * OwnedForwardingAnalysis}; otherwise the edge is already present.
   */
  public void addEdge(StateExpr pre, StateExpr post, Transition transition) {
    _reverseEdges.computeIfAbsent(post, k -> new ArrayList<>()).add(new PreEdge(pre, transition));
    _edgeCount++;
  }

  /** Number of reverse edges this worker holds (for scale reporting). */
  public int edgeCount() {
    return _edgeCount;
  }

  /**
   * Queues a serialized BDD received from another worker (called by the sidecar thread). The
   * payload is deserialized later on the worker's own thread, since {@link JFactory} is not
   * thread-safe.
   */
  public void receive(StateExpr state, String payload) {
    synchronized (_inbox) {
      _inbox.add(new Inbox(state, payload));
    }
  }

  /**
   * Runs the barrier-synchronized distributed fixpoint for the full query header space and returns
   * this worker's reachable BDDs.
   */
  public Map<StateExpr, BDD> run() {
    return run(_queryRoot);
  }

  /**
   * Runs the fixpoint for one query shard (a subset of the query header space), starting from
   * {@code queryRoot}. The caller is expected to serialize the result and drop it before the next
   * shard so that only one shard's BDDs are live at a time (the peak-memory saving of prefix
   * sharding).
   *
   * <p>Every worker must call this the same number of times in the same order, since each shard
   * runs its own barrier-synchronized fixpoint.
   */
  public Map<StateExpr, BDD> run(BDD queryRoot) {
    reset();
    if (owner(Query.INSTANCE) == _id) {
      _reachable.put(Query.INSTANCE, queryRoot.id());
      _worklist.add(Query.INSTANCE);
    }
    boolean done = false;
    while (!done) {
      processLocal();
      _coordinator.roundCheck(false);
      boolean sent = flushOutbox();
      boolean anySent = _coordinator.roundCheck(sent);
      boolean changed = drainInbox();
      boolean anyChanged = _coordinator.roundCheck(changed);
      done = !anySent && !anyChanged;
    }
    return new HashMap<>(_reachable);
  }

  private void reset() {
    _reachable.clear();
    _worklist.clear();
    _pending.clear();
    synchronized (_inbox) {
      _inbox.clear();
    }
  }

  /** Reclaim BDD nodes no longer referenced by the current shard's result. */
  public void collectGarbage() {
    _factory.runGC();
  }

  private int owner(StateExpr state) {
    String host = ownerHostname(state);
    return host == null || host.isEmpty() ? 0 : _partition.getOrDefault(host, 0);
  }

  private void processLocal() {
    while (!_worklist.isEmpty()) {
      StateExpr post = _worklist.poll();
      BDD postBdd = _reachable.get(post);
      for (PreEdge edge : _reverseEdges.getOrDefault(post, List.of())) {
        BDD out = edge.transition.transitBackward(postBdd);
        if (out.isZero()) {
          continue;
        }
        StateExpr pre = edge.pre;
        if (owner(pre) == _id) {
          merge(pre, out);
        } else {
          _pending.computeIfAbsent(owner(pre), k -> new ArrayList<>()).add(new Outbound(pre, out));
        }
      }
    }
  }

  private boolean merge(StateExpr state, BDD add) {
    BDD old = _reachable.get(state);
    if (old == null) {
      _reachable.put(state, add);
      _worklist.add(state);
      return true;
    }
    BDD merged = old.or(add);
    if (!merged.equals(old)) {
      _reachable.put(state, merged);
      _worklist.add(state);
      return true;
    }
    return false;
  }

  private boolean flushOutbox() {
    boolean any = false;
    for (Map.Entry<Integer, List<Outbound>> e : _pending.entrySet()) {
      for (Outbound out : e.getValue()) {
        _clients[e.getKey()].transit(out.state, out.bdd);
        any = true;
      }
    }
    _pending.clear();
    return any;
  }

  private boolean drainInbox() {
    boolean changed = false;
    synchronized (_inbox) {
      for (Inbox message : _inbox) {
        BDD bdd;
        try {
          bdd = new BDDTransfer().load(_factory, message.payload);
        } catch (IOException e) {
          throw new RuntimeException("Failed to deserialize a distributed BDD", e);
        }
        changed |= merge(message.state, bdd);
      }
      _inbox.clear();
    }
    processLocal();
    return changed;
  }

  private static String ownerHostname(StateExpr state) {
    if (state instanceof NodeStateExpr) {
      return ((NodeStateExpr) state).getHostname();
    }
    if (state instanceof InterfaceStateExpr) {
      return ((InterfaceStateExpr) state).getHostname();
    }
    if (state instanceof VrfStateExpr) {
      return ((VrfStateExpr) state).getHostname();
    }
    if (state instanceof EdgeStateExpr) {
      return ((EdgeStateExpr) state).getSrcNode();
    }
    if (state instanceof PacketPolicyStatement) {
      return ((PacketPolicyStatement) state).getHostname();
    }
    if (state instanceof PacketPolicyAction) {
      return ((PacketPolicyAction) state).getHostname();
    }
    return "";
  }
}
