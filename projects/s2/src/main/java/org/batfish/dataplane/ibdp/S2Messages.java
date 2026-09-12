package org.batfish.dataplane.ibdp;

import java.io.Serializable;
import java.util.List;
import org.batfish.datamodel.AbstractRoute;
import org.batfish.datamodel.AnnotatedRoute;
import org.batfish.datamodel.Bgpv4Route;
import org.batfish.datamodel.bgp.BgpTopology;
import org.batfish.dataplane.rib.RouteAdvertisement;

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
}
