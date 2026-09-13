// SPDX-License-Identifier: Apache-2.0
package org.batfish.dataplane.ibdp;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;

import net.sf.javabdd.BDD;
import net.sf.javabdd.BDDTransfer;
import net.sf.javabdd.JFactory;
import org.junit.Test;

/** Round-trips a BDD between two factories, the primitive S2 uses to forward packets across workers. */
public class BddTransferTest {

  @Test
  public void testRoundTrip() throws Exception {
    JFactory f1 = (JFactory) JFactory.init(1000, 1000);
    f1.setVarNum(4);
    BDD b = f1.ithVar(0).and(f1.nithVar(1)).or(f1.ithVar(2).and(f1.ithVar(3)));

    String serialized = new BDDTransfer().save(b);

    JFactory f2 = (JFactory) JFactory.init(1000, 1000);
    BDD b2 = new BDDTransfer().load(f2, serialized);

    for (int assignment = 0; assignment < 16; assignment++) {
      assertThat(
          "assignment " + assignment,
          evaluate(f1, b, assignment),
          equalTo(evaluate(f2, b2, assignment)));
    }
  }

  private static boolean evaluate(JFactory factory, BDD bdd, int assignment) {
    BDD minterm = factory.one();
    for (int i = 0; i < 4; i++) {
      minterm.andWith(((assignment >> i) & 1) == 1 ? factory.ithVar(i) : factory.nithVar(i));
    }
    return !bdd.and(minterm).isZero();
  }
}
