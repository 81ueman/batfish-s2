// SPDX-License-Identifier: Apache-2.0

package org.batfish.dataplane.ibdp;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;

/** Worker-side {@link S2Coordinator} that asks the controller for the global fixed-point result. */
final class S2RemoteCoordinator implements S2Coordinator {

  private final ObjectOutputStream _out;
  private final ObjectInputStream _in;
  private final int _runId;
  private int _round;

  S2RemoteCoordinator(ObjectOutputStream out, ObjectInputStream in) {
    this(out, in, 0);
  }

  S2RemoteCoordinator(ObjectOutputStream out, ObjectInputStream in, int runId) {
    _out = out;
    _in = in;
    _runId = runId;
  }

  @Override
  public synchronized boolean roundCheck(boolean localDirty) {
    try {
      _out.writeObject(new S2ControlMessages.RoundRequest(_runId, _round++, localDirty));
      _out.flush();
      S2ControlMessages.RoundResponse response = (S2ControlMessages.RoundResponse) _in.readObject();
      return response.globalDirty;
    } catch (IOException | ClassNotFoundException e) {
      throw new RuntimeException("S2 round check failed", e);
    }
  }

  @Override
  public synchronized int sumAll(int localValue) {
    try {
      _out.writeObject(new S2ControlMessages.SumRequest(_runId, localValue));
      _out.flush();
      S2ControlMessages.SumResponse response = (S2ControlMessages.SumResponse) _in.readObject();
      return response.sum;
    } catch (IOException | ClassNotFoundException e) {
      throw new RuntimeException("S2 sum exchange failed", e);
    }
  }
}
