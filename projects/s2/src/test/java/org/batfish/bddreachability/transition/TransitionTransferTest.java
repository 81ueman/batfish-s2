// SPDX-License-Identifier: Apache-2.0

package org.batfish.bddreachability.transition;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;

import java.util.Arrays;
import java.util.List;
import net.sf.javabdd.JFactory;
import org.junit.Test;

/** Verifies that {@link TransitionTransfer} round-trips the transitions used on boundary edges. */
public class TransitionTransferTest {

  @Test
  public void testRoundTrip() throws Exception {
    JFactory source = (JFactory) JFactory.init(1000, 1000);
    source.setVarNum(4);
    JFactory target = (JFactory) JFactory.init(1000, 1000);
    target.setVarNum(4);

    List<Transition> transitions =
        Arrays.asList(
            Identity.INSTANCE,
            Zero.INSTANCE,
            Transitions.constraint(source.ithVar(0)),
            Transitions.eraseAndSet(source.ithVar(1), source.ithVar(2)),
            new Composite(
                Transitions.constraint(source.ithVar(0).and(source.nithVar(3))),
                Transitions.eraseAndSet(source.ithVar(1), source.ithVar(2))),
            new Or(
                Identity.INSTANCE, Transitions.constraint(source.ithVar(0).or(source.ithVar(3)))));

    for (Transition transition : transitions) {
      String serialized = TransitionTransfer.save(transition);
      Transition restored = TransitionTransfer.load(target, serialized);
      assertThat(transition.toString(), TransitionTransfer.save(restored), equalTo(serialized));
    }
  }

  @Test
  public void testUnsupportedTransitionRejected() {
    JFactory factory = (JFactory) JFactory.init(1000, 1000);
    factory.setVarNum(4);
    Transition unsupported =
        Transitions.reverse(Transitions.eraseAndSet(factory.ithVar(1), factory.ithVar(2)));
    boolean threw = false;
    try {
      TransitionTransfer.save(unsupported);
    } catch (java.io.IOException e) {
      threw = true;
    }
    assertThat(threw, is(true));
  }
}
