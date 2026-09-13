// SPDX-License-Identifier: Apache-2.0
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
    final boolean descriptorShadows;

    /**
     * The controller-computed node &rarr; worker assignment (hostname to worker index). Workers use
     * this instead of independently recomputing {@code NetworkPartitioner}: the scheme is selected
     * once on the controller ({@code -Ds2.partition=<scheme>}) and any randomness lives only there.
     */
    final Map<String, Integer> assignment;

    /**
     * Java-serialized {@code SortedMap<String, Configuration>} of all snapshot configs, produced by
     * the controller so workers do not re-parse. May be null (fall back to parsing on the worker).
     */
    byte[] configs;

    /**
     * Java-serialized {@code Set<BgpAdvertisement>} of the snapshot's external BGP announcements,
     * or null. Workers cannot load these themselves (they build from shipped configs), so the
     * controller ships them; they are injected into the BGP RIBs and are shard-appointed.
     */
    byte[] externalAdverts;

    /**
     * Descriptor-shadow mode ({@code -Ds2.descriptorShadows=true}): Java-serialized {@code
     * SortedMap<String, Configuration>} of just the configs this worker owns. Null in the stock
     * path, where {@link #configs} carries the full snapshot instead.
     */
    byte[] ownedConfigs;

    /**
     * Descriptor-shadow mode: Java-serialized {@code Map<String, RemoteNodeDescriptor>} for the
     * nodes this worker does not own, replacing their full configurations with reduced shadow
     * configs. Null in the stock path.
     */
    byte[] descriptors;

    Start(
        List<S2WorkerEndpoint> endpoints,
        Map<String, Integer> assignment,
        byte[] configs,
        byte[] externalAdverts,
        byte[] ownedConfigs,
        byte[] descriptors) {
      this.endpoints = endpoints;
      this.assignment = assignment;
      this.configs = configs;
      this.externalAdverts = externalAdverts;
      this.ownedConfigs = ownedConfigs;
      this.descriptors = descriptors;
      this.descriptorShadows = descriptors != null;
    }

    /**
     * Drop the (now deserialized) config payloads so the worker does not keep the serialized copies
     * alive for the rest of the run. {@link #externalAdverts} is kept until it has been consumed.
     */
    void clearConfigPayloads() {
      configs = null;
      ownedConfigs = null;
      descriptors = null;
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
