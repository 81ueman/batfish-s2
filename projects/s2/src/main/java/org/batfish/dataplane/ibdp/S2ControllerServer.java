package org.batfish.dataplane.ibdp;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

/**
 * Controller for the multi-process S2 run (milestone 3, one Pod per worker).
 *
 * <p>It coordinates the global fixed point (each worker reports its local dirty flag every round and
 * the controller replies with the global result) and collects each worker's final RIBs.
 */
public final class S2ControllerServer implements AutoCloseable {

  private final int _numWorkers;
  private final List<S2WorkerEndpoint> _endpoints;
  private final ServerSocket _server;
  private final ExecutorService _pool = Executors.newCachedThreadPool();

  private final Map<Integer, ObjectOutputStream> _streams = new ConcurrentHashMap<>();
  private final Map<Integer, S2ControlMessages.Result> _results = new ConcurrentHashMap<>();
  private final Object _registrationLock = new Object();
  private final CountDownLatch _resultsDone = new CountDownLatch(1);
  private final RoundCoordinator _rounds;
  private int _registered;

  /** Blocks each worker's round check until all workers reported, then returns the global dirty. */
  private static final class RoundCoordinator {
    private final int _numWorkers;
    private int _currentRound = Integer.MIN_VALUE;
    private final Set<Integer> _reported = new HashSet<>();
    private boolean _anyDirty;
    private boolean _ready;

    RoundCoordinator(int numWorkers) {
      _numWorkers = numWorkers;
    }

    synchronized boolean check(int round, int workerId, boolean dirty) {
      if (round != _currentRound) {
        _currentRound = round;
        _reported.clear();
        _anyDirty = false;
        _ready = false;
      }
      _reported.add(workerId);
      _anyDirty |= dirty;
      if (_reported.size() == _numWorkers) {
        _ready = true;
        notifyAll();
      }
      while (!_ready) {
        try {
          wait();
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
          throw new RuntimeException(e);
        }
      }
      return _anyDirty;
    }
  }

  public S2ControllerServer(int port, int numWorkers, List<S2WorkerEndpoint> endpoints)
      throws IOException {
    _numWorkers = numWorkers;
    _endpoints = endpoints;
    _server = new ServerSocket(port);
    _rounds = new RoundCoordinator(numWorkers);
  }

  public int getPort() {
    return _server.getLocalPort();
  }

  public void start() {
    Thread thread =
        new Thread(
            () -> {
              while (!_server.isClosed()) {
                try {
                  Socket socket = _server.accept();
                  _pool.submit(() -> handle(socket));
                } catch (IOException e) {
                  if (!_server.isClosed()) {
                    throw new RuntimeException(e);
                  }
                }
              }
            },
            "s2-controller");
    thread.setDaemon(true);
    thread.start();
  }

  private void handle(Socket socket) {
    try (Socket s = socket) {
      ObjectOutputStream out = new ObjectOutputStream(s.getOutputStream());
      out.flush();
      ObjectInputStream in = new ObjectInputStream(s.getInputStream());
      S2ControlMessages.Register register = (S2ControlMessages.Register) in.readObject();
      int workerId = register.workerId;
      _streams.put(workerId, out);
      boolean allRegistered;
      synchronized (_registrationLock) {
        _registered++;
        allRegistered = _registered == _numWorkers;
      }
      if (allRegistered) {
        for (ObjectOutputStream workerOut : _streams.values()) {
          workerOut.writeObject(new S2ControlMessages.Start(_endpoints));
          workerOut.flush();
        }
      }
      while (true) {
        Object message = in.readObject();
        if (message instanceof S2ControlMessages.RoundRequest) {
          S2ControlMessages.RoundRequest request = (S2ControlMessages.RoundRequest) message;
          boolean globalDirty = _rounds.check(request.round, workerId, request.localDirty);
          out.writeObject(new S2ControlMessages.RoundResponse(globalDirty));
          out.flush();
        } else if (message instanceof S2ControlMessages.Result) {
          S2ControlMessages.Result result = (S2ControlMessages.Result) message;
          _results.put(result.workerId, result);
          if (_results.size() == _numWorkers) {
            _resultsDone.countDown();
          }
          return;
        }
      }
    } catch (IOException | ClassNotFoundException e) {
      throw new RuntimeException("S2 controller connection failed", e);
    }
  }

  /** Wait for all workers' results and return them keyed by worker id. */
  public Map<Integer, S2ControlMessages.Result> awaitResults(long timeoutSeconds) {
    try {
      if (!_resultsDone.await(timeoutSeconds, TimeUnit.SECONDS)) {
        throw new RuntimeException("Timed out waiting for S2 workers");
      }
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new RuntimeException(e);
    }
    return new LinkedHashMap<>(_results);
  }

  public List<S2WorkerEndpoint> getEndpoints() {
    return new ArrayList<>(_endpoints);
  }

  @Override
  public void close() {
    _pool.shutdownNow();
    try {
      _server.close();
    } catch (IOException e) {
      // ignore
    }
  }
}
