package org.batfish.dataplane.ibdp;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.Socket;
import java.util.List;
import org.batfish.datamodel.Bgpv4Route;
import org.batfish.dataplane.rib.RouteAdvertisement;

/** Client half of the S2 sidecar: fetches a remote process's outgoing advertisements. */
final class S2SidecarClient {

  List<RouteAdvertisement<Bgpv4Route>> fetch(
      S2WorkerEndpoint endpoint, S2Messages.RoutesRequest request) {
    try (Socket socket = new Socket(endpoint.getHost(), endpoint.getPort())) {
      ObjectOutputStream out = new ObjectOutputStream(socket.getOutputStream());
      out.flush();
      ObjectInputStream in = new ObjectInputStream(socket.getInputStream());
      out.writeObject(request);
      out.flush();
      S2Messages.RoutesResponse response = (S2Messages.RoutesResponse) in.readObject();
      return response.routes;
    } catch (IOException | ClassNotFoundException e) {
      throw new RuntimeException("S2 sidecar call to " + endpoint + " failed", e);
    }
  }
}
