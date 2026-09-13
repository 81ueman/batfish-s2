// SPDX-License-Identifier: Apache-2.0

package org.batfish.dataplane.ibdp;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.Socket;

/** Client half of the S2 sidecar. */
final class S2SidecarClient {

  Object call(S2WorkerEndpoint endpoint, Object request) {
    boolean boundary = request instanceof S2Messages.BoundaryEdgesRequest;
    S2Messages.RpcStats.routeSent.incrementAndGet();
    if (boundary) {
      S2Messages.RpcStats.routeSentBoundary.incrementAndGet();
    }
    try (Socket socket = new Socket(endpoint.getHost(), endpoint.getPort())) {
      ObjectOutputStream out =
          new ObjectOutputStream(
              new S2Messages.CountingOutputStream(
                  socket.getOutputStream(), S2Messages.RpcStats.routeSentReqBytes));
      out.flush();
      ObjectInputStream in =
          new ObjectInputStream(
              new S2Messages.CountingInputStream(
                  socket.getInputStream(), S2Messages.RpcStats.routeSentRespBytes));
      out.writeObject(request);
      out.flush();
      Object response = in.readObject();
      if (boundary && response instanceof S2Messages.BoundaryEdgesResponse) {
        S2Messages.RpcStats.routeSentBoundaryEdges.addAndGet(
            ((S2Messages.BoundaryEdgesResponse) response).edges.size());
      }
      return response;
    } catch (IOException | ClassNotFoundException e) {
      throw new RuntimeException("S2 sidecar call to " + endpoint + " failed", e);
    }
  }
}
