// SPDX-License-Identifier: Apache-2.0

package org.batfish.dataplane.ibdp;

import java.io.FilterInputStream;
import java.io.FilterOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.Serializable;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import org.batfish.datamodel.AbstractRoute;
import org.batfish.datamodel.AnnotatedRoute;
import org.batfish.datamodel.Bgpv4Route;
import org.batfish.datamodel.OspfExternalType1Route;
import org.batfish.datamodel.OspfExternalType2Route;
import org.batfish.datamodel.OspfInterAreaRoute;
import org.batfish.datamodel.OspfIntraAreaRoute;
import org.batfish.datamodel.bgp.BgpTopology;
import org.batfish.datamodel.ospf.OspfTopology.EdgeId;
import org.batfish.dataplane.rib.RouteAdvertisement;
import org.batfish.symbolic.state.StateExpr;

/** Wire messages for the S2 sidecar. Kept deliberately small and Java-serializable. */
final class S2Messages {
  private S2Messages() {}

  /** "Give me the outgoing advertisements of process (hostname, vrf) for this edge." */
  static final class RoutesRequest implements Serializable {
    private static final long serialVersionUID = 1L;

    final String hostname;
    final String vrf;
    final BgpTopology.EdgeId edgeId;
    final boolean isNewSession;

    RoutesRequest(String hostname, String vrf, BgpTopology.EdgeId edgeId, boolean isNewSession) {
      this.hostname = hostname;
      this.vrf = vrf;
      this.edgeId = edgeId;
      this.isNewSession = isNewSession;
    }
  }

  /** The requested advertisements. */
  static final class RoutesResponse implements Serializable {
    private static final long serialVersionUID = 1L;

    final List<RouteAdvertisement<Bgpv4Route>> routes;

    RoutesResponse(List<RouteAdvertisement<Bgpv4Route>> routes) {
      this.routes = routes;
    }
  }

  /** "Give me the final main RIB of the real node (hostname, vrf)." (FIB distribution) */
  static final class MainRibRequest implements Serializable {
    private static final long serialVersionUID = 1L;

    final String hostname;
    final String vrf;

    MainRibRequest(String hostname, String vrf) {
      this.hostname = hostname;
      this.vrf = vrf;
    }
  }

  /** The owner's main RIB routes (annotated, ready to merge into a shadow's main RIB). */
  static final class MainRibResponse implements Serializable {
    private static final long serialVersionUID = 1L;

    final List<AnnotatedRoute<AbstractRoute>> routes;

    MainRibResponse(List<AnnotatedRoute<AbstractRoute>> routes) {
      this.routes = routes;
    }
  }

  /**
   * "Give me the cross-worker reachability edges whose destination (post) state lives on one of
   * these hostnames." In the backward fixpoint a worker generates only edges whose source it owns;
   * the missing edges into its own states are pulled from the peers that own their sources.
   */
  static final class BoundaryEdgesRequest implements Serializable {
    private static final long serialVersionUID = 1L;

    final Set<String> ownedHosts;

    BoundaryEdgesRequest(Set<String> ownedHosts) {
      this.ownedHosts = ownedHosts;
    }
  }

  /** One cross-worker edge: its endpoint states plus a portable form of its transition. */
  static final class SerializedEdge implements Serializable {
    private static final long serialVersionUID = 1L;

    final StateExpr preState;
    final StateExpr postState;
    final String transition;

    SerializedEdge(StateExpr preState, StateExpr postState, String transition) {
      this.preState = preState;
      this.postState = postState;
      this.transition = transition;
    }
  }

  /** The requested cross-worker edges, transition BDDs serialized by {@code TransitionTransfer}. */
  static final class BoundaryEdgesResponse implements Serializable {
    private static final long serialVersionUID = 1L;

    final List<SerializedEdge> edges;

    BoundaryEdgesResponse(List<SerializedEdge> edges) {
      this.edges = edges;
    }
  }

  /** Generic acknowledgement for requests whose reply carries no data. */
  static final class Ack implements Serializable {
    private static final long serialVersionUID = 1L;
  }

  /**
   * An OSPF message a shadow process would enqueue on its owner's real process. One request class
   * per OSPF route type; the receiver enqueues it locally.
   */
  abstract static class OspfEnqueueRequest implements Serializable {
    private static final long serialVersionUID = 1L;

    final String hostname;
    final String vrf;
    final String process;
    final EdgeId edge;

    OspfEnqueueRequest(String hostname, String vrf, String process, EdgeId edge) {
      this.hostname = hostname;
      this.vrf = vrf;
      this.process = process;
      this.edge = edge;
    }
  }

  static final class OspfIntraRequest extends OspfEnqueueRequest {
    private static final long serialVersionUID = 1L;
    final List<RouteAdvertisement<OspfIntraAreaRoute>> routes;

    OspfIntraRequest(
        String hostname,
        String vrf,
        String process,
        EdgeId edge,
        List<RouteAdvertisement<OspfIntraAreaRoute>> routes) {
      super(hostname, vrf, process, edge);
      this.routes = routes;
    }
  }

  static final class OspfInterRequest extends OspfEnqueueRequest {
    private static final long serialVersionUID = 1L;
    final List<RouteAdvertisement<OspfInterAreaRoute>> routes;

    OspfInterRequest(
        String hostname,
        String vrf,
        String process,
        EdgeId edge,
        List<RouteAdvertisement<OspfInterAreaRoute>> routes) {
      super(hostname, vrf, process, edge);
      this.routes = routes;
    }
  }

  static final class OspfType1Request extends OspfEnqueueRequest {
    private static final long serialVersionUID = 1L;
    final List<RouteAdvertisement<OspfExternalType1Route>> routes;

    OspfType1Request(
        String hostname,
        String vrf,
        String process,
        EdgeId edge,
        List<RouteAdvertisement<OspfExternalType1Route>> routes) {
      super(hostname, vrf, process, edge);
      this.routes = routes;
    }
  }

  static final class OspfType2Request extends OspfEnqueueRequest {
    private static final long serialVersionUID = 1L;
    final List<RouteAdvertisement<OspfExternalType2Route>> routes;

    OspfType2Request(
        String hostname,
        String vrf,
        String process,
        EdgeId edge,
        List<RouteAdvertisement<OspfExternalType2Route>> routes) {
      super(hostname, vrf, process, edge);
      this.routes = routes;
    }
  }

  // ------------------------------------------------------------------ RPC stats

  /**
   * Process-wide sidecar RPC counters, for the partition-evaluation boundary communication metric
   * ({@code docs/s2-port/PARTITIONING-PLAN.md} §6.1). Each worker JVM prints one summary to stderr
   * at the end of a run (see {@link S2SidecarServer#close()}).
   *
   * <p>Byte counts are the actual bytes on the wire: every connection carries exactly one request
   * and one response, so a counting wrapper around the socket stream counts exactly the bytes
   * transferred, including the Java serialization stream headers. Set {@code -Ds2.rpcStats=false}
   * to disable the summary.
   */
  static final class RpcStats {
    private RpcStats() {}

    // Route (BGP/OSPF/main-RIB/boundary) sidecar, as served by this worker.
    static final AtomicLong routeServed = new AtomicLong();
    static final AtomicLong routeServedReqBytes = new AtomicLong();
    static final AtomicLong routeServedRespBytes = new AtomicLong();
    static final AtomicLong routeServedBoundary = new AtomicLong();
    static final AtomicLong routeServedBoundaryEdges = new AtomicLong();

    // Route sidecar, as called by this worker.
    static final AtomicLong routeSent = new AtomicLong();
    static final AtomicLong routeSentReqBytes = new AtomicLong();
    static final AtomicLong routeSentRespBytes = new AtomicLong();
    static final AtomicLong routeSentBoundary = new AtomicLong();
    static final AtomicLong routeSentBoundaryEdges = new AtomicLong();

    // BDD (symbolic) sidecar.
    static final AtomicLong bddReceived = new AtomicLong();
    static final AtomicLong bddReceivedReqBytes = new AtomicLong();
    static final AtomicLong bddReceivedRespBytes = new AtomicLong();
    static final AtomicLong bddSent = new AtomicLong();
    static final AtomicLong bddSentReqBytes = new AtomicLong();
    static final AtomicLong bddSentRespBytes = new AtomicLong();

    private static final AtomicBoolean PRINTED = new AtomicBoolean();
    private static final boolean ENABLED =
        !"false".equalsIgnoreCase(System.getProperty("s2.rpcStats", "true"));

    /**
     * Prints the per-worker summary to stderr once per JVM (no-op if disabled or already printed).
     */
    static void printSummary() {
      if (!ENABLED || !PRINTED.compareAndSet(false, true)) {
        return;
      }
      System.err.printf(
          "S2 rpc-stats pid=%d route.served=%d served.reqBytes=%d served.respBytes=%d"
              + " served.boundary=%d served.boundaryEdges=%d route.sent=%d"
              + " sent.reqBytes=%d sent.respBytes=%d sent.boundary=%d sent.boundaryEdges=%d"
              + " bdd.received=%d received.reqBytes=%d received.respBytes=%d"
              + " bdd.sent=%d sent.reqBytes=%d sent.respBytes=%d%n",
          ProcessHandle.current().pid(),
          routeServed.get(),
          routeServedReqBytes.get(),
          routeServedRespBytes.get(),
          routeServedBoundary.get(),
          routeServedBoundaryEdges.get(),
          routeSent.get(),
          routeSentReqBytes.get(),
          routeSentRespBytes.get(),
          routeSentBoundary.get(),
          routeSentBoundaryEdges.get(),
          bddReceived.get(),
          bddReceivedReqBytes.get(),
          bddReceivedRespBytes.get(),
          bddSent.get(),
          bddSentReqBytes.get(),
          bddSentRespBytes.get());
    }
  }

  /** Counts every byte written to the wrapped stream into {@code counter}. */
  static final class CountingOutputStream extends FilterOutputStream {
    private final AtomicLong _counter;

    CountingOutputStream(OutputStream out, AtomicLong counter) {
      super(out);
      _counter = counter;
    }

    @Override
    public void write(int b) throws IOException {
      out.write(b);
      _counter.incrementAndGet();
    }

    @Override
    public void write(byte[] b, int off, int len) throws IOException {
      out.write(b, off, len);
      _counter.addAndGet(len);
    }
  }

  /** Counts every byte read from the wrapped stream into {@code counter}. */
  static final class CountingInputStream extends FilterInputStream {
    private final AtomicLong _counter;

    CountingInputStream(InputStream in, AtomicLong counter) {
      super(in);
      _counter = counter;
    }

    @Override
    public int read() throws IOException {
      int b = in.read();
      if (b >= 0) {
        _counter.incrementAndGet();
      }
      return b;
    }

    @Override
    public int read(byte[] b, int off, int len) throws IOException {
      int n = in.read(b, off, len);
      if (n > 0) {
        _counter.addAndGet(n);
      }
      return n;
    }
  }
}
