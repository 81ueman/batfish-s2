package org.batfish.dataplane.ibdp;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.Socket;

/** Client half of the S2 sidecar. */
final class S2SidecarClient {

  Object call(S2WorkerEndpoint endpoint, Object request) {
    try (Socket socket = new Socket(endpoint.getHost(), endpoint.getPort())) {
      ObjectOutputStream out = new ObjectOutputStream(socket.getOutputStream());
      out.flush();
      ObjectInputStream in = new ObjectInputStream(socket.getInputStream());
      out.writeObject(request);
      out.flush();
      return in.readObject();
    } catch (IOException | ClassNotFoundException e) {
      throw new RuntimeException("S2 sidecar call to " + endpoint + " failed", e);
    }
  }
}
