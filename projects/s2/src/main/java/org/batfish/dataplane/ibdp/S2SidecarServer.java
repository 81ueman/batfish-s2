// SPDX-License-Identifier: Apache-2.0
package org.batfish.dataplane.ibdp;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * Worker-side sidecar: serves requests from other workers by running the owning worker's real
 * processes. Handles both route exchange (control plane) and main-RIB retrieval (FIB distribution).
 */
final class S2SidecarServer implements AutoCloseable {

  interface Handler {
    Object handle(Object request);
  }

  private final ServerSocket _server;
  private final Handler _handler;
  private final ExecutorService _pool = Executors.newCachedThreadPool();
  private volatile boolean _closed;

  S2SidecarServer(int port, Handler handler) throws IOException {
    _server = new ServerSocket(port);
    _handler = handler;
  }

  int getPort() {
    return _server.getLocalPort();
  }

  void start() {
    Thread thread =
        new Thread(
            () -> {
              while (!_closed) {
                try {
                  Socket socket = _server.accept();
                  _pool.submit(() -> serve(socket));
                } catch (IOException e) {
                  if (!_closed) {
                    throw new RuntimeException("S2 sidecar accept failed", e);
                  }
                }
              }
            },
            "s2-sidecar");
    thread.setDaemon(true);
    thread.start();
  }

  private void serve(Socket socket) {
    try (Socket s = socket) {
      // Flush both headers before either side blocks on a read.
      ObjectOutputStream out = new ObjectOutputStream(s.getOutputStream());
      out.flush();
      ObjectInputStream in = new ObjectInputStream(s.getInputStream());
      Object request = in.readObject();
      Object response;
      try {
        response = _handler.handle(request);
      } catch (RuntimeException e) {
        // Otherwise the client only sees a bare EOF because this connection closes without a reply.
        System.err.println("S2 sidecar handler failed for " + request.getClass() + ": " + e);
        e.printStackTrace();
        throw e;
      }
      out.writeObject(response);
      out.flush();
    } catch (IOException | ClassNotFoundException e) {
      throw new RuntimeException("S2 sidecar request failed", e);
    }
  }

  @Override
  public void close() {
    _closed = true;
    _pool.shutdownNow();
    try {
      _server.close();
    } catch (IOException e) {
      // ignore
    }
  }
}
