// SPDX-License-Identifier: Apache-2.0
package org.batfish.dataplane.ibdp;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

/**
 * Controller for the multi-process S2 run (milestone 3, one Pod per worker).
 *
 * <p>It coordinates the global fixed point (each worker reports its local dirty flag every round
 * and the controller replies with the global result) and collects each worker's final RIBs.
 */
public final class S2ControllerServer implements AutoCloseable {

  private final int _numWorkers;
  private final List<S2WorkerEndpoint> _endpoints;
  private final Map<String, Integer> _assignment;
  private final byte[] _configs;
  private final byte[] _externalAdverts;
  private final Map<Integer, byte[]> _ownedConfigsByWorker;
  private final byte[] _descriptors;
  private final ServerSocket _server;
  private final ExecutorService _pool = Executors.newCachedThreadPool();

  private final Map<Integer, ObjectOutputStream> _streams = new ConcurrentHashMap<>();
  private final Map<Integer, S2ControlMessages.Result> _results = new ConcurrentHashMap<>();
  private final Object _registrationLock = new Object();
  private final CountDownLatch _resultsDone = new CountDownLatch(1);
  private final RoundCoordinator _rounds;
  private final SumCoordinator _sums;
  private int _registered;

  /**
   * Round barrier shared by all workers: every worker reports a boolean each round and the barrier
   * action computes the OR for that round before any worker proceeds to the next round. Using a
   * {@link CyclicBarrier} (not wait/notify) avoids the cross-round race where a fast worker starts
   * round N+1 before a slow worker has left round N.
   */
  private static final class RoundCoordinator {
    private final java.util.concurrent.atomic.AtomicBoolean _anyDirty =
        new java.util.concurrent.atomic.AtomicBoolean();
    private final CyclicBarrier _barrier;
    private volatile boolean _result;

    RoundCoordinator(int numWorkers) {
      _barrier =
          new CyclicBarrier(
              numWorkers,
              () -> {
                _result = _anyDirty.get();
                _anyDirty.set(false);
              });
    }

    boolean check(boolean dirty) {
      if (dirty) {
        _anyDirty.set(true);
      }
      try {
        _barrier.await();
      } catch (Exception e) {
        throw new RuntimeException("S2 round synchronization failed", e);
      }
      return _result;
    }
  }

  /** Barrier that sums every worker's contribution for the round and returns the total. */
  private static final class SumCoordinator {
    private final java.util.concurrent.atomic.AtomicInteger _sum =
        new java.util.concurrent.atomic.AtomicInteger();
    private final CyclicBarrier _barrier;
    private volatile int _result;

    SumCoordinator(int numWorkers) {
      _barrier = new CyclicBarrier(numWorkers, () -> _result = _sum.getAndSet(0));
    }

    int check(int value) {
      _sum.addAndGet(value);
      try {
        _barrier.await();
      } catch (Exception e) {
        throw new RuntimeException("S2 sum synchronization failed", e);
      }
      return _result;
    }
  }

  /**
   * @param configs full snapshot configs for the stock path, or null in descriptor-shadow mode
   * @param ownedConfigsByWorker per-worker full configs for owned nodes, or null in the stock path
   * @param descriptors shared reduced shadow configs, or null in the stock path
   */
  public S2ControllerServer(
      int port,
      int numWorkers,
      List<S2WorkerEndpoint> endpoints,
      Map<String, Integer> assignment,
      byte[] configs,
      byte[] externalAdverts,
      Map<Integer, byte[]> ownedConfigsByWorker,
      byte[] descriptors)
      throws IOException {
    _numWorkers = numWorkers;
    _endpoints = endpoints;
    _assignment = assignment;
    _configs = configs;
    _externalAdverts = externalAdverts;
    _ownedConfigsByWorker = ownedConfigsByWorker;
    _descriptors = descriptors;
    _server = new ServerSocket(port);
    _rounds = new RoundCoordinator(numWorkers);
    _sums = new SumCoordinator(numWorkers);
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
        // In descriptor mode each worker gets only its owned configs; the reduced remote configs
        // are shared. In the stock path every worker gets the full snapshot (or null).
        for (Map.Entry<Integer, ObjectOutputStream> entry : _streams.entrySet()) {
          ObjectOutputStream workerOut = entry.getValue();
          byte[] ownedConfigs =
              _descriptors == null ? null : _ownedConfigsByWorker.get(entry.getKey());
          workerOut.writeObject(
              new S2ControlMessages.Start(
                  _endpoints, _assignment, _configs, _externalAdverts, ownedConfigs, _descriptors));
          workerOut.flush();
        }
      }
      while (true) {
        Object message = in.readObject();
        if (message instanceof S2ControlMessages.RoundRequest) {
          S2ControlMessages.RoundRequest request = (S2ControlMessages.RoundRequest) message;
          boolean globalDirty = _rounds.check(request.localDirty);
          out.writeObject(new S2ControlMessages.RoundResponse(globalDirty));
          out.flush();
        } else if (message instanceof S2ControlMessages.SumRequest) {
          S2ControlMessages.SumRequest request = (S2ControlMessages.SumRequest) message;
          int sum = _sums.check(request.value);
          out.writeObject(new S2ControlMessages.SumResponse(sum));
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
