package org.batfish.bddreachability.transition;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import net.sf.javabdd.BDDFactory;
import net.sf.javabdd.BDDTransfer;
import net.sf.javabdd.JFactory;

/**
 * Serializes a {@link Transition} (and the BDDs it captures) to a portable string so an S2 worker
 * can pull a boundary edge from the worker that owns its source and reconstruct the transition in
 * its own {@link JFactory}.
 *
 * <p>Supported transition types are the ones Batfish builds for a reachability graph without
 * firewall sessions: {@link Identity}, {@link Zero}, {@link Constraint}, {@link EraseAndSet},
 * {@link Composite}, and {@link Or}. Transitions that capture engine objects (source managers, last
 * hop managers, transformation pairings) are not portable and are rejected.
 */
public final class TransitionTransfer {

  private static final String IDENTITY = "I";
  private static final String ZERO = "Z";
  private static final String CONSTRAINT = "C";
  private static final String ERASE_AND_SET = "E";
  private static final String COMPOSITE = "M";
  private static final String OR = "O";
  private static final String SEP = ",";

  private TransitionTransfer() {}

  public static String save(Transition transition) throws IOException {
    StringBuilder builder = new StringBuilder();
    write(transition, builder);
    return builder.toString();
  }

  private static void write(Transition transition, StringBuilder builder) throws IOException {
    BDDTransfer transfer = new BDDTransfer();
    if (transition instanceof Identity) {
      builder.append(IDENTITY);
    } else if (transition instanceof Zero) {
      builder.append(ZERO);
    } else if (transition instanceof Constraint) {
      builder
          .append(CONSTRAINT)
          .append(SEP)
          .append(transfer.save(((Constraint) transition).getConstraint()));
    } else if (transition instanceof EraseAndSet) {
      EraseAndSet eas = (EraseAndSet) transition;
      builder
          .append(ERASE_AND_SET)
          .append(SEP)
          .append(transfer.save(eas.getEraseVars()))
          .append(SEP)
          .append(transfer.save(eas.getSetValue()));
    } else if (transition instanceof Composite) {
      writeAll(COMPOSITE, ((Composite) transition).getTransitions(), builder);
    } else if (transition instanceof Or) {
      writeAll(OR, ((Or) transition).getTransitions(), builder);
    } else {
      throw new IOException("Unsupported transition " + transition.getClass());
    }
  }

  private static void writeAll(String tag, List<Transition> transitions, StringBuilder builder)
      throws IOException {
    builder.append(tag).append(SEP).append(transitions.size());
    for (Transition transition : transitions) {
      builder.append(SEP);
      write(transition, builder);
    }
  }

  public static Transition load(BDDFactory factory, String serialized) throws IOException {
    JFactory jfactory = (JFactory) factory;
    String[] tokens = serialized.split(SEP, -1);
    int[] cursor = new int[1];
    Transition transition = read(jfactory, tokens, cursor);
    if (cursor[0] != tokens.length) {
      throw new IOException("Trailing tokens in transition payload");
    }
    return transition;
  }

  private static Transition read(JFactory factory, String[] tokens, int[] cursor)
      throws IOException {
    BDDTransfer transfer = new BDDTransfer();
    if (cursor[0] >= tokens.length) {
      throw new IOException("Truncated transition payload");
    }
    String tag = tokens[cursor[0]++];
    switch (tag) {
      case IDENTITY:
        return Identity.INSTANCE;
      case ZERO:
        return Zero.INSTANCE;
      case CONSTRAINT:
        return new Constraint(transfer.load(factory, next(tokens, cursor)));
      case ERASE_AND_SET:
        {
          String erase = next(tokens, cursor);
          String set = next(tokens, cursor);
          return new EraseAndSet(transfer.load(factory, erase), transfer.load(factory, set));
        }
      case COMPOSITE:
        return new Composite(readChildren(factory, tokens, cursor));
      case OR:
        return new Or(readChildren(factory, tokens, cursor));
      default:
        throw new IOException("Unknown transition tag " + tag);
    }
  }

  private static List<Transition> readChildren(JFactory factory, String[] tokens, int[] cursor)
      throws IOException {
    int count;
    try {
      count = Integer.parseInt(next(tokens, cursor));
    } catch (NumberFormatException e) {
      throw new IOException("Invalid child count in transition payload", e);
    }
    List<Transition> children = new ArrayList<>(count);
    for (int i = 0; i < count; i++) {
      children.add(read(factory, tokens, cursor));
    }
    return children;
  }

  private static String next(String[] tokens, int[] cursor) throws IOException {
    if (cursor[0] >= tokens.length) {
      throw new IOException("Truncated transition payload");
    }
    return tokens[cursor[0]++];
  }
}
