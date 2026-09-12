package org.batfish.dataplane.ibdp;

import java.util.Objects;
import net.sf.javabdd.BDD;
import org.batfish.bddreachability.transition.Transition;
import org.batfish.bddreachability.transition.TransitionVisitor;
import org.batfish.symbolic.state.StateExpr;

/**
 * A {@link Transition} whose result leaves the owning worker. It applies the wrapped transition
 * locally and, if the result is non-empty, ships the resulting BDD to the worker that owns the end
 * state (forward) or start state (backward), returning zero so the local fixpoint does not consume
 * it. Mirrors S2's algorithm: each worker only continues symbolic forwarding for its own states.
 */
public final class InterWorkerTransition implements Transition {

  private static final long serialVersionUID = 1L;

  private final StateExpr _startState;
  private final StateExpr _endState;
  private final Transition _transition;
  private transient S2BddSidecar.Client _sidecar;

  public InterWorkerTransition(
      StateExpr startState,
      StateExpr endState,
      Transition transition,
      S2BddSidecar.Client sidecar) {
    _startState = startState;
    _endState = endState;
    _transition = transition;
    _sidecar = sidecar;
  }

  @Override
  public BDD transitForward(BDD bdd) {
    BDD result = _transition.transitForward(bdd);
    if (!result.isZero() && _sidecar != null) {
      _sidecar.transit(_endState, result);
    }
    return bdd.getFactory().zero();
  }

  @Override
  public BDD transitBackward(BDD bdd) {
    BDD result = _transition.transitBackward(bdd);
    if (!result.isZero() && _sidecar != null) {
      _sidecar.transit(_startState, result);
    }
    return bdd.getFactory().zero();
  }

  @Override
  public <T> T accept(TransitionVisitor<T> visitor) {
    return _transition.accept(visitor);
  }

  public Transition getTransition() {
    return _transition;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof InterWorkerTransition)) {
      return false;
    }
    InterWorkerTransition that = (InterWorkerTransition) o;
    return Objects.equals(_startState, that._startState)
        && Objects.equals(_endState, that._endState)
        && Objects.equals(_transition, that._transition);
  }

  @Override
  public int hashCode() {
    return Objects.hash(_startState, _endState, _transition);
  }

  @Override
  public String toString() {
    return "InterWorkerTransition{" + _startState + " -> " + _endState + ", " + _transition + '}';
  }
}
