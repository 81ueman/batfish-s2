package org.batfish.dataplane.ibdp;

import com.google.common.collect.Table;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import net.sf.javabdd.BDD;
import net.sf.javabdd.JFactory;
import org.batfish.bddreachability.BDDReachabilityAnalysis;
import org.batfish.bddreachability.transition.Transition;
import org.batfish.symbolic.state.EdgeStateExpr;
import org.batfish.symbolic.state.InterfaceStateExpr;
import org.batfish.symbolic.state.NodeStateExpr;
import org.batfish.symbolic.state.PacketPolicyAction;
import org.batfish.symbolic.state.PacketPolicyStatement;
import org.batfish.symbolic.state.StateExpr;
import org.batfish.symbolic.state.VrfStateExpr;

/**
 * One worker's part of the distributed symbolic reachability fixpoint.
 *
 * <p>It owns a subset of the reachability graph's state expressions (those on switches assigned to
 * this worker). It runs the forward BDD fixpoint over its own states; when a transition leaves the
 * worker, the resulting BDD is serialized and shipped to the owner over {@link S2BddSidecar}. Rounds
 * are synchronized with the other workers through {@link S2Coordinator}.
 */
public final class S2ReachabilityWorker {

  private final int _id;
  private final Map<String, Integer> _partition;
  private final JFactory _factory;
  private final Table<StateExpr, StateExpr, Transition> _edges;
  private final S2BddSidecar.Client[] _clients;
  private final S2Coordinator _coordinator;

  private final Map<StateExpr, BDD> _reachable = new HashMap<>();
  private final Queue<StateExpr> _worklist = new ArrayDeque<>();
  private final List<Inbox> _inbox = new ArrayList<>();
  private final Map<Integer, List<Outbound>> _pending = new HashMap<>();

  private record Inbox(StateExpr state, BDD bdd) {}

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
    _edges = analysis.getForwardEdgeTable();
    _clients = clients;
    _coordinator = coordinator;
    for (StateExpr state : analysis.getIngressLocationStates()) {
      if (owner(state) == id) {
        _reachable.put(state, _factory.one());
        _worklist.add(state);
      }
    }
  }

  /** Adds a BDD received from another worker (called by the sidecar). */
  public void receive(StateExpr state, BDD bdd) {
    synchronized (_inbox) {
      _inbox.add(new Inbox(state, bdd));
    }
  }

  /** Runs the barrier-synchronized distributed fixpoint and returns this worker's reachable BDDs. */
  public Map<StateExpr, BDD> run() {
    boolean done = false;
    while (!done) {
      processLocal();
      _coordinator.roundCheck(false); // barrier: everyone finished local work
      boolean sent = flushOutbox();
      boolean anySent = _coordinator.roundCheck(sent);
      boolean changed = drainInbox();
      boolean anyChanged = _coordinator.roundCheck(changed);
      done = !anySent && !anyChanged;
    }
    return _reachable;
  }

  private int owner(StateExpr state) {
    String host = ownerHostname(state);
    return host == null || host.isEmpty() ? 0 : _partition.getOrDefault(host, 0);
  }

  private void processLocal() {
    while (!_worklist.isEmpty()) {
      StateExpr state = _worklist.poll();
      BDD in = _reachable.get(state);
      for (Map.Entry<StateExpr, Transition> edge : _edges.row(state).entrySet()) {
        StateExpr post = edge.getKey();
        BDD out = edge.getValue().transitForward(in);
        if (out.isZero()) {
          continue;
        }
        if (owner(post) == _id) {
          merge(post, out);
        } else {
          _pending
              .computeIfAbsent(owner(post), k -> new ArrayList<>())
              .add(new Outbound(post, out));
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
        changed |= merge(message.state, message.bdd);
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
