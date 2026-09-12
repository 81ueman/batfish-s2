package org.batfish.dataplane.ibdp;

import java.io.Serializable;
import java.util.List;
import java.util.Map;
import org.batfish.datamodel.AbstractRoute;

/** Control-plane wire messages between S2 workers and the controller (milestone 3). */
final class S2ControlMessages {

  private S2ControlMessages() {}

  static final class Register implements Serializable {
    private static final long serialVersionUID = 1L;
    final int workerId;

    Register(int workerId) {
      this.workerId = workerId;
    }
  }

  static final class Start implements Serializable {
    private static final long serialVersionUID = 1L;
    final List<S2WorkerEndpoint> endpoints;

    Start(List<S2WorkerEndpoint> endpoints) {
      this.endpoints = endpoints;
    }
  }

  static final class RoundRequest implements Serializable {
    private static final long serialVersionUID = 1L;
    final int round;
    final boolean localDirty;

    RoundRequest(int round, boolean localDirty) {
      this.round = round;
      this.localDirty = localDirty;
    }
  }

  static final class RoundResponse implements Serializable {
    private static final long serialVersionUID = 1L;
    final boolean globalDirty;

    RoundResponse(boolean globalDirty) {
      this.globalDirty = globalDirty;
    }
  }

  /** A worker's final main RIBs (hostname -&gt; vrf -&gt; routes) and data-plane digest. */
  static final class Result implements Serializable {
    private static final long serialVersionUID = 1L;
    final int workerId;
    final Map<String, Map<String, List<AbstractRoute>>> ribs;
    final Map<String, String> reachability;

    Result(
        int workerId,
        Map<String, Map<String, List<AbstractRoute>>> ribs,
        Map<String, String> reachability) {
      this.workerId = workerId;
      this.ribs = ribs;
      this.reachability = reachability;
    }
  }
}
