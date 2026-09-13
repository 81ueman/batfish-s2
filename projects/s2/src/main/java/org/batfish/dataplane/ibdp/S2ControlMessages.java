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

    /**
     * Java-serialized {@code SortedMap<String, Configuration>} of all snapshot configs, produced by
     * the controller so workers do not re-parse. May be null (fall back to parsing on the worker).
     */
    final byte[] configs;

    Start(List<S2WorkerEndpoint> endpoints, byte[] configs) {
      this.endpoints = endpoints;
      this.configs = configs;
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

  /** Contribute this worker's value to the current round's global sum. */
  static final class SumRequest implements Serializable {
    private static final long serialVersionUID = 1L;
    final int value;

    SumRequest(int value) {
      this.value = value;
    }
  }

  /** The sum of every worker's contributed value for the round. */
  static final class SumResponse implements Serializable {
    private static final long serialVersionUID = 1L;
    final int sum;

    SumResponse(int sum) {
      this.sum = sum;
    }
  }

  /** A worker's final main RIBs, traceroute digest, and symbolic reachable BDDs. */
  static final class Result implements Serializable {
    private static final long serialVersionUID = 1L;
    final int workerId;
    final Map<String, Map<String, List<AbstractRoute>>> ribs;
    final Map<String, String> reachability;
    final Map<org.batfish.symbolic.state.StateExpr, String> symbolicReachable;
    final long peakHeapBytes;

    Result(
        int workerId,
        Map<String, Map<String, List<AbstractRoute>>> ribs,
        Map<String, String> reachability,
        Map<org.batfish.symbolic.state.StateExpr, String> symbolicReachable,
        long peakHeapBytes) {
      this.workerId = workerId;
      this.ribs = ribs;
      this.reachability = reachability;
      this.symbolicReachable = symbolicReachable;
      this.peakHeapBytes = peakHeapBytes;
    }
  }
}
