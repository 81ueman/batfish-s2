// SPDX-License-Identifier: Apache-2.0

package org.batfish.dataplane.ibdp;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.Socket;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * Engine-side client for the persistent S2 controller service (choice A).
 *
 * <p>The engine ships one {@link S2ControlMessages.ComputeRequest} per snapshot to the long-lived
 * controller and blocks until the worker pool has computed that snapshot and written the per-host
 * slices. Unlike the worker's live-RPC path (which reuses the controller connection for the
 * fixpoint), this is a short, self-contained request/reply, so each call opens its own connection.
 */
public final class S2ControllerClient implements AutoCloseable {

  private static final Logger LOGGER = LogManager.getLogger(S2ControllerClient.class);

  private static final int CONNECT_ATTEMPTS = 600;

  private final String _host;
  private final int _port;
  private int _timeoutSeconds = 3600;

  public S2ControllerClient(String host, int port) {
    _host = host;
    _port = port;
  }

  /** How long to wait for the pool to finish a snapshot (default one hour). */
  public S2ControllerClient setTimeoutSeconds(int timeoutSeconds) {
    _timeoutSeconds = timeoutSeconds;
    return this;
  }

  /**
   * Ask the pool to compute {@code request} and wait for it. Throws if the controller is
   * unreachable or the pool fails; the caller should then fall back or fail the snapshot.
   */
  public S2ControlMessages.ComputeResponse compute(S2ControlMessages.ComputeRequest request)
      throws IOException, ClassNotFoundException {
    Socket socket = connectWithRetry(_host, _port);
    try (Socket s = socket) {
      s.setSoTimeout(Math.max(1, _timeoutSeconds) * 1000);
      ObjectOutputStream out = new ObjectOutputStream(s.getOutputStream());
      out.flush();
      ObjectInputStream in = new ObjectInputStream(s.getInputStream());
      out.writeObject(request);
      out.flush();
      Object response = in.readObject();
      if (!(response instanceof S2ControlMessages.ComputeResponse)) {
        throw new IOException("unexpected S2 controller response " + response.getClass());
      }
      return (S2ControlMessages.ComputeResponse) response;
    }
  }

  /** Stop a controller service (best effort; ignored if it is already gone). */
  public void shutdown() {
    try (Socket socket = connectWithRetry(_host, _port)) {
      ObjectOutputStream out = new ObjectOutputStream(socket.getOutputStream());
      out.flush();
      out.writeObject(new S2ControlMessages.Shutdown());
      out.flush();
    } catch (IOException e) {
      LOGGER.warn("S2 controller shutdown request failed: {}", e.toString());
    }
  }

  private static Socket connectWithRetry(String host, int port) throws IOException {
    IOException last = null;
    for (int attempt = 1; attempt <= CONNECT_ATTEMPTS; attempt++) {
      try {
        return new Socket(host, port);
      } catch (IOException e) {
        last = e;
        try {
          Thread.sleep(1000);
        } catch (InterruptedException ie) {
          Thread.currentThread().interrupt();
          throw new IOException("interrupted connecting to S2 controller " + host + ":" + port, ie);
        }
      }
    }
    throw new IOException("could not connect to S2 controller " + host + ":" + port, last);
  }

  @Override
  public void close() {
    // No persistent state: each compute() owns its connection.
  }
}
