// SPDX-License-Identifier: Apache-2.0
package org.batfish.dataplane.ibdp;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import net.sf.javabdd.BDD;
import net.sf.javabdd.BDDTransfer;
import net.sf.javabdd.JFactory;
import org.batfish.bddreachability.transition.Transition;
import org.batfish.bddreachability.transition.TransitionVisitor;
import org.batfish.symbolic.state.PreInInterface;
import org.batfish.symbolic.state.StateExpr;
import org.junit.Test;

/** Verifies a symbolic packet crosses a worker boundary via {@link InterWorkerTransition}. */
public class InterWorkerTransitionTest {

  @Test
  public void testShipsBddToRemoteWorker() throws Exception {
    JFactory senderFactory = (JFactory) JFactory.init(1000, 1000);
    senderFactory.setVarNum(4);
    JFactory receiverFactory = (JFactory) JFactory.init(1000, 1000);
    receiverFactory.setVarNum(4);

    CompletableFuture<BDD> received = new CompletableFuture<>();
    S2BddSidecar server =
        new S2BddSidecar(
            0,
            (state, payload) -> {
              try {
                received.complete(new BDDTransfer().load(receiverFactory, payload));
              } catch (Exception e) {
                received.completeExceptionally(e);
              }
            });
    server.start();
    try {
      S2BddSidecar.Client client =
          new S2BddSidecar.Client(new S2WorkerEndpoint("127.0.0.1", server.getPort()));

      StateExpr start = new PreInInterface("r1", "GigabitEthernet0/0");
      StateExpr end = new PreInInterface("r2", "GigabitEthernet0/0");
      Transition identity =
          new Transition() {
            @Override
            public BDD transitForward(BDD bdd) {
              return bdd.id();
            }

            @Override
            public BDD transitBackward(BDD bdd) {
              return bdd.id();
            }

            @Override
            public <T> T accept(TransitionVisitor<T> visitor) {
              return null;
            }
          };

      InterWorkerTransition transition = new InterWorkerTransition(start, end, identity, client);

      // A symbolic packet on the sending worker.
      BDD packet = senderFactory.ithVar(0).and(senderFactory.nithVar(1));

      BDD localResult = transition.transitForward(packet);
      assertThat("result must leave the worker", localResult.isZero(), is(true));

      BDD remotePacket = received.get(5, TimeUnit.SECONDS);
      for (int assignment = 0; assignment < 16; assignment++) {
        assertThat(
            "assignment " + assignment,
            evaluate(senderFactory, packet, assignment),
            equalTo(evaluate(receiverFactory, remotePacket, assignment)));
      }
    } finally {
      server.close();
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
