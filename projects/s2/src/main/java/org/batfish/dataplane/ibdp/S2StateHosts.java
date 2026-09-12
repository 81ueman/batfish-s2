package org.batfish.dataplane.ibdp;

import org.batfish.symbolic.state.EdgeStateExpr;
import org.batfish.symbolic.state.InterfaceStateExpr;
import org.batfish.symbolic.state.NodeStateExpr;
import org.batfish.symbolic.state.PacketPolicyAction;
import org.batfish.symbolic.state.PacketPolicyStatement;
import org.batfish.symbolic.state.StateExpr;
import org.batfish.symbolic.state.VrfStateExpr;

/** Maps a reachability {@link StateExpr} to the hostname of the switch that owns it. */
final class S2StateHosts {

  private S2StateHosts() {}

  /**
   * @return the owning hostname, or the empty string for states that are not tied to a switch (e.g.
   *     the global {@code Query} and disposition states).
   */
  static String hostname(StateExpr state) {
    if (state instanceof NodeStateExpr) {
      return ((NodeStateExpr) state).getHostname();
    }
    if (state instanceof InterfaceStateExpr) {
      return ((InterfaceStateExpr) state).getHostname();
    }
    if (state instanceof VrfStateExpr) {
      return ((VrfStateExpr) state).getHostname();
    }
    if (state instanceof EdgeStateExpr) {
      return ((EdgeStateExpr) state).getSrcNode();
    }
    if (state instanceof PacketPolicyStatement) {
      return ((PacketPolicyStatement) state).getHostname();
    }
    if (state instanceof PacketPolicyAction) {
      return ((PacketPolicyAction) state).getHostname();
    }
    return "";
  }
}
