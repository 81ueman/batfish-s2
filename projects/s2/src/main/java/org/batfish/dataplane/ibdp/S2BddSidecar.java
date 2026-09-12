package org.batfish.dataplane.ibdp;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import net.sf.javabdd.BDD;
import net.sf.javabdd.BDDTransfer;
import net.sf.javabdd.JFactory;
import org.batfish.symbolic.state.StateExpr;

/**
 * Sidecar for symbolic-packet (BDD) transfer between workers. A sender serializes a BDD at a state
 * expression; the receiver reconstructs it in its own {@link JFactory} and hands it to the handler,
 * which normally continues the reachability fixpoint for that state.
 */
public final class S2BddSidecar implements AutoCloseable {

  public interface Handler {
    void transit(StateExpr state, BDD bdd);
  }

  private static final class TransitMessage implements Serializable {
    private static final long serialVersionUID = 1L;
    final StateExpr state;
    final String payload;

    TransitMessage(StateExpr state, String payload) {
      this.state = state;
      this.payload = payload;
    }
  }

  private static final class Ack implements Serializable {
    private static final long serialVersionUID = 1L;
  }

  private final ServerSocket _server;
  private final JFactory _factory;
  private final Handler _handler;
  private final ExecutorService _pool = Executors.newCachedThreadPool();
  private volatile boolean _closed;

  public S2BddSidecar(int port, JFactory factory, Handler handler) throws IOException {
    _server = new ServerSocket(port);
    _factory = factory;
    _handler = handler;
  }

  public int getPort() {
    return _server.getLocalPort();
  }

  public void start() {
    Thread thread =
        new Thread(
            () -> {
              while (!_closed) {
                try {
                  Socket socket = _server.accept();
                  _pool.submit(() -> serve(socket));
                } catch (IOException e) {
                  if (!_closed) {
                    throw new RuntimeException("S2 BDD sidecar accept failed", e);
                  }
                }
              }
            },
            "s2-bdd-sidecar");
    thread.setDaemon(true);
    thread.start();
  }

  private void serve(Socket socket) {
    try (Socket s = socket) {
      ObjectOutputStream out = new ObjectOutputStream(s.getOutputStream());
      out.flush();
      ObjectInputStream in = new ObjectInputStream(s.getInputStream());
      TransitMessage message = (TransitMessage) in.readObject();
      BDD bdd = new BDDTransfer().load(_factory, message.payload);
      _handler.transit(message.state, bdd);
      out.writeObject(new Ack());
      out.flush();
    } catch (Exception e) {
      throw new RuntimeException("S2 BDD sidecar request failed", e);
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

  /** Blocking client: ships a BDD at a state to the remote worker and waits for acknowledgement. */
  public static final class Client {
    private final S2WorkerEndpoint _endpoint;

    public Client(S2WorkerEndpoint endpoint) {
      _endpoint = endpoint;
    }

    public void transit(StateExpr state, BDD bdd) {
      try (Socket socket = new Socket(_endpoint.getHost(), _endpoint.getPort())) {
        ObjectOutputStream out = new ObjectOutputStream(socket.getOutputStream());
        out.flush();
        ObjectInputStream in = new ObjectInputStream(socket.getInputStream());
        out.writeObject(new TransitMessage(state, new BDDTransfer().save(bdd)));
        out.flush();
        in.readObject();
      } catch (Exception e) {
        throw new RuntimeException("S2 BDD transit to " + _endpoint + " failed", e);
      }
    }
  }
}
