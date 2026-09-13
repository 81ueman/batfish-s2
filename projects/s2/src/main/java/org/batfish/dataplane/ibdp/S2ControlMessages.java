// SPDX-License-Identifier: Apache-2.0

package org.batfish.dataplane.ibdp;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;
import org.batfish.datamodel.AbstractRoute;
import org.batfish.datamodel.BgpAdvertisement;
import org.batfish.datamodel.Configuration;
import org.batfish.datamodel.Ip;
import org.batfish.datamodel.IpSpace;

/** Control-plane wire messages between S2 workers and the controller (milestone 3). */
final class S2ControlMessages {

  private S2ControlMessages() {}

  // ---------------------------------------------------------------- serialization

  /** Serialize parsed configurations so a peer can skip parsing the snapshot. */
  static byte[] serializeConfigs(Map<String, Configuration> configs) throws IOException {
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    try (ObjectOutputStream oos = new ObjectOutputStream(baos)) {
      oos.writeObject(new TreeMap<>(configs));
    }
    return baos.toByteArray();
  }

  @SuppressWarnings("unchecked")
  static SortedMap<String, Configuration> deserializeConfigs(byte[] payload)
      throws IOException, ClassNotFoundException {
    try (ObjectInputStream ois = new ObjectInputStream(new ByteArrayInputStream(payload))) {
      return (SortedMap<String, Configuration>) ois.readObject();
    }
  }

  /** Serialize the snapshot's external BGP announcements so a peer can inject them. */
  static byte[] serializeExternalAdverts(Set<BgpAdvertisement> adverts) throws IOException {
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    try (ObjectOutputStream oos = new ObjectOutputStream(baos)) {
      oos.writeObject(new HashSet<>(adverts));
    }
    return baos.toByteArray();
  }

  @SuppressWarnings("unchecked")
  static Set<BgpAdvertisement> deserializeExternalAdverts(byte[] payload)
      throws IOException, ClassNotFoundException {
    try (ObjectInputStream ois = new ObjectInputStream(new ByteArrayInputStream(payload))) {
      return (Set<BgpAdvertisement>) ois.readObject();
    }
  }

  // -------------------------------------------------------------------- messages

  static final class Register implements Serializable {
    private static final long serialVersionUID = 1L;
    final int workerId;

    /**
     * The worker's route-sidecar address, advertised so the controller can ship the peer endpoint
     * list (and so peers can reach it) without being configured with the pool up front. Null for an
     * older/one-shot worker; the controller then uses its configured endpoint list.
     */
    final S2WorkerEndpoint endpoint;

    Register(int workerId) {
      this(workerId, null);
    }

    Register(int workerId, S2WorkerEndpoint endpoint) {
      this.workerId = workerId;
      this.endpoint = endpoint;
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

    /**
     * Directory this worker writes its owned hosts' data-plane slices to. Null means fall back to
     * {@code S2_SLICE_DIR} / {@code <S2_OUTPUT_DIR>/slices} (the one-shot runner behavior). The
     * controller sets it per snapshot in the persistent-pool flow so the engine can serve that
     * snapshot from a unique directory and delete it afterwards.
     */
    final String sliceDir;

    /**
     * Monotonic id of this snapshot run on a persistent pool, echoed by the worker on every
     * round/sum/done message so the controller only counts messages belonging to the active run.
     * Zero for the one-shot runner, which has exactly one run per server and ignores it.
     */
    final int runId;

    Start(
        List<S2WorkerEndpoint> endpoints,
        Map<String, Integer> assignment,
        byte[] configs,
        byte[] externalAdverts,
        byte[] ownedConfigs,
        byte[] descriptors) {
      this(endpoints, assignment, configs, externalAdverts, ownedConfigs, descriptors, null, 0);
    }

    Start(
        List<S2WorkerEndpoint> endpoints,
        Map<String, Integer> assignment,
        byte[] configs,
        byte[] externalAdverts,
        byte[] ownedConfigs,
        byte[] descriptors,
        String sliceDir,
        int runId) {
      this.endpoints = endpoints;
      this.assignment = assignment;
      this.configs = configs;
      this.externalAdverts = externalAdverts;
      this.ownedConfigs = ownedConfigs;
      this.descriptors = descriptors;
      this.descriptorShadows = descriptors != null;
      this.sliceDir = sliceDir;
      this.runId = runId;
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
    final int runId;
    final int round;
    final boolean localDirty;

    RoundRequest(int runId, int round, boolean localDirty) {
      this.runId = runId;
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
    final int runId;
    final int value;

    SumRequest(int runId, int value) {
      this.runId = runId;
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

  /**
   * Contribute this worker's unowned-ARP-IP set (computed from the FIBs of the nodes it owns) to
   * the cluster-wide union. Sent once per snapshot, after convergence, only by workers in
   * owned-only mode.
   */
  static final class UnownedArpIpsRequest implements Serializable {
    private static final long serialVersionUID = 1L;
    final int runId;
    final Set<Ip> ips;

    UnownedArpIpsRequest(int runId, Set<Ip> ips) {
      this.runId = runId;
      this.ips = ips;
    }
  }

  /** The union of every worker's contributed unowned-ARP-IP set. */
  static final class UnownedArpIpsResponse implements Serializable {
    private static final long serialVersionUID = 1L;
    final Set<Ip> ips;

    UnownedArpIpsResponse(Set<Ip> ips) {
      this.ips = ips;
    }
  }

  /**
   * Contribute this worker's owned nodes' ARP replies to the cluster-wide merge. Sent once per
   * snapshot, after convergence, right after {@link UnownedArpIpsRequest}, only by workers in
   * owned-only mode.
   */
  static final class ArpRepliesRequest implements Serializable {
    private static final long serialVersionUID = 1L;
    final int runId;
    final Map<String, Map<String, IpSpace>> arpReplies;

    ArpRepliesRequest(int runId, Map<String, Map<String, IpSpace>> arpReplies) {
      this.runId = runId;
      this.arpReplies = arpReplies;
    }
  }

  /** The merge of every worker's contributed per-node ARP replies. */
  static final class ArpRepliesResponse implements Serializable {
    private static final long serialVersionUID = 1L;
    final Map<String, Map<String, IpSpace>> arpReplies;

    ArpRepliesResponse(Map<String, Map<String, IpSpace>> arpReplies) {
      this.arpReplies = arpReplies;
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

  /**
   * Lightweight end-of-snapshot marker for the persistent-pool flow: the engine serves questions
   * from the slices the worker just wrote, so the controller never needs the worker's RIBs (unlike
   * {@link Result}, which the one-shot runner verifies).
   */
  static final class Done implements Serializable {
    private static final long serialVersionUID = 1L;
    final int runId;
    final int workerId;
    final long peakHeapBytes;

    Done(int runId, int workerId, long peakHeapBytes) {
      this.runId = runId;
      this.workerId = workerId;
      this.peakHeapBytes = peakHeapBytes;
    }
  }

  /** A worker failed to compute a snapshot; the controller fails that compute request. */
  static final class WorkerError implements Serializable {
    private static final long serialVersionUID = 1L;
    final int runId;
    final int workerId;
    final String message;

    WorkerError(int runId, int workerId, String message) {
      this.runId = runId;
      this.workerId = workerId;
      this.message = message;
    }
  }

  /**
   * Engine &rarr; controller request to compute one snapshot on the pool. The engine ships its
   * already-parsed configs (and external announcements) so the controller does not re-read the
   * snapshot, plus a unique directory the workers should write their slices to.
   */
  static final class ComputeRequest implements Serializable {
    private static final long serialVersionUID = 1L;
    final byte[] configs;
    final byte[] externalAdverts;
    final String sliceDir;
    final String snapshotName;

    ComputeRequest(byte[] configs, byte[] externalAdverts, String sliceDir, String snapshotName) {
      this.configs = configs;
      this.externalAdverts = externalAdverts;
      this.sliceDir = sliceDir;
      this.snapshotName = snapshotName;
    }
  }

  /** Controller &rarr; engine reply: where the slices landed and how many workers ran. */
  static final class ComputeResponse implements Serializable {
    private static final long serialVersionUID = 1L;
    final boolean ok;
    final String sliceDir;
    final int numWorkers;
    final String message;

    ComputeResponse(boolean ok, String sliceDir, int numWorkers, String message) {
      this.ok = ok;
      this.sliceDir = sliceDir;
      this.numWorkers = numWorkers;
      this.message = message;
    }
  }

  /** Engine &rarr; controller (and controller &rarr; worker): shut the service down. */
  static final class Shutdown implements Serializable {
    private static final long serialVersionUID = 1L;
  }
}
