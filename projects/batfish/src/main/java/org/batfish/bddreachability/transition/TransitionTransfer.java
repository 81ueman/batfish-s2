package org.batfish.bddreachability.transition;

import java.io.IOException;
import net.sf.javabdd.BDDFactory;
import net.sf.javabdd.BDDTransfer;
import net.sf.javabdd.JFactory;

/**
 * Serializes a {@link Transition} (and the BDDs it captures) to a portable string so an S2 worker
 * can pull a boundary edge from the worker that owns its source and reconstruct the transition in
 * its own {@link JFactory}.
 *
 * <p>Only the transition types that appear on cross-worker edges are supported for now: {@link
 * Constraint} and {@link EraseAndSet}. Add more as needed.
 */
public final class TransitionTransfer {

  private static final String SEP = "|";
  private static final String CONSTRAINT = "C";
  private static final String ERASE_AND_SET = "E";

  private TransitionTransfer() {}

  public static String save(Transition transition) throws IOException {
    BDDTransfer transfer = new BDDTransfer();
    if (transition instanceof Constraint) {
      return CONSTRAINT + SEP + transfer.save(((Constraint) transition).getConstraint());
    }
    if (transition instanceof EraseAndSet) {
      EraseAndSet eas = (EraseAndSet) transition;
      return ERASE_AND_SET
          + SEP
          + transfer.save(eas.getEraseVars())
          + SEP
          + transfer.save(eas.getSetValue());
    }
    throw new IllegalArgumentException("Unsupported transition " + transition.getClass());
  }

  public static Transition load(BDDFactory factory, String serialized) throws IOException {
    JFactory jfactory = (JFactory) factory;
    BDDTransfer transfer = new BDDTransfer();
    String[] parts = serialized.split("\\" + SEP);
    switch (parts[0]) {
      case CONSTRAINT:
        return new Constraint(transfer.load(jfactory, parts[1]));
      case ERASE_AND_SET:
        return new EraseAndSet(transfer.load(jfactory, parts[1]), transfer.load(jfactory, parts[2]));
      default:
        throw new IOException("Unknown transition payload " + parts[0]);
    }
  }
}
