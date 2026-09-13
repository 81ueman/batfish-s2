// SPDX-License-Identifier: Apache-2.0

package org.batfish.dataplane.ibdp;

import java.io.EOFException;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.SortedMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.batfish.datamodel.Configuration;

/**
 * Long-lived controller service for the persistent S2 worker pool (choice A).
 *
 * <p>Unlike {@link S2ControllerServer} (one snapshot, one server, then exit), this service accepts
 * the N worker services once and keeps their controller connections open. An engine then sends one
 * {@link S2ControlMessages.ComputeRequest} per snapshot; for each, the service computes the
 * partition, ships the payload, drives the existing round/sum barrier, and returns once every
 * worker has written its owned hosts' slices. Requests are serialized (the pool serves one snapshot
 * at a time), and each snapshot gets a monotonic {@code runId} so stragglers from a failed run
 * cannot be mistaken for the next one.
 */
public final class S2ControllerService implements AutoCloseable {

  private final int _numWorkers;
  private final List<S2WorkerEndpoint> _configuredEndpoints;
  private final ServerSocket _server;
  private final ExecutorService _pool = Executors.newCachedThreadPool();

  private final Map<Integer, WorkerSession> _workers = new ConcurrentHashMap<>();
  private final CountDownLatch _allWorkersRegistered = new CountDownLatch(1);
  private final Object _runLock = new Object();
  private final AtomicInteger _runIds = new AtomicInteger();

  private volatile Run _currentRun;
  private volatile boolean _closed;

  private long _registrationTimeoutSeconds = 600;
  private long _computeTimeoutSeconds = 3600;

  public S2ControllerService(int port, int numWorkers) throws IOException {
    this(port, numWorkers, List.of());
  }

  /**
   * @param configuredEndpoints worker sidecar endpoints indexed by worker id, as the one-shot
   *     runner knows them; may be empty, in which case each worker advertises its own endpoint on
   *     registration (the pool self-configures).
   */
  public S2ControllerService(int port, int numWorkers, List<S2WorkerEndpoint> configuredEndpoints)
      throws IOException {
    _numWorkers = numWorkers;
    _configuredEndpoints = new ArrayList<>(configuredEndpoints);
    _server = new ServerSocket(port);
  }

  public int getPort() {
    return _server.getLocalPort();
  }

  public int getNumWorkers() {
    return _numWorkers;
  }

  /** Bound how long a compute request waits for all workers to finish one snapshot. */
  public S2ControllerService setComputeTimeoutSeconds(long seconds) {
    _computeTimeoutSeconds = seconds;
    return this;
  }

  /** Bound how long a compute request waits for the pool to register. */
  public S2ControllerService setRegistrationTimeoutSeconds(long seconds) {
    _registrationTimeoutSeconds = seconds;
    return this;
  }

  public void start() {
    Thread thread =
        new Thread(
            () -> {
              while (!_closed) {
                try {
                  Socket socket = _server.accept();
                  _pool.submit(() -> handleConnection(socket));
                } catch (IOException e) {
                  if (!_closed) {
                    throw new RuntimeException("S2 controller-service accept failed", e);
                  }
                }
              }
            },
            "s2-controller-service");
    thread.setDaemon(true);
    thread.start();
  }

  /** Wait until every worker has registered. Returns false on timeout. */
  public boolean awaitWorkers(long timeoutSeconds) throws InterruptedException {
    return _allWorkersRegistered.await(timeoutSeconds, TimeUnit.SECONDS);
  }

  private void handleConnection(Socket socket) {
    try {
      ObjectOutputStream out = new ObjectOutputStream(socket.getOutputStream());
      out.flush();
      ObjectInputStream in = new ObjectInputStream(socket.getInputStream());
      Object first = in.readObject();
      if (first instanceof S2ControlMessages.Register) {
        register(socket, out, in, (S2ControlMessages.Register) first);
      } else if (first instanceof S2ControlMessages.ComputeRequest) {
        handleCompute(out, (S2ControlMessages.ComputeRequest) first);
      } else if (first instanceof S2ControlMessages.Shutdown) {
        close();
      } else {
        System.err.println("S2 controller-service: unexpected first message " + first.getClass());
        socket.close();
      }
    } catch (IOException | ClassNotFoundException e) {
      if (!_closed) {
        System.err.println("S2 controller-service connection failed: " + e);
      }
    }
  }

  private void register(
      Socket socket,
      ObjectOutputStream out,
      ObjectInputStream in,
      S2ControlMessages.Register register) {
    S2WorkerEndpoint endpoint = register.endpoint;
    if (endpoint == null) {
      if (register.workerId < 0 || register.workerId >= _configuredEndpoints.size()) {
        throw new IllegalStateException(
            "worker " + register.workerId + " did not advertise a sidecar endpoint");
      }
      endpoint = _configuredEndpoints.get(register.workerId);
    }
    WorkerSession session = new WorkerSession(register.workerId, endpoint, socket, out, in);
    _workers.put(register.workerId, session);
    System.out.printf(
        "S2 controller-service: worker %d registered at %s (%d/%d)%n",
        register.workerId, endpoint, _workers.size(), _numWorkers);
    if (_workers.size() == _numWorkers) {
      _allWorkersRegistered.countDown();
    }
    readLoop(session);
  }

  /** Continuously serve one worker's round/sum/done messages for whichever run is active. */
  private void readLoop(WorkerSession session) {
    while (!_closed) {
      Object message;
      try {
        message = session.read();
      } catch (EOFException | SocketException e) {
        markWorkerDead(session);
        return;
      } catch (IOException | ClassNotFoundException e) {
        markWorkerDead(session);
        return;
      }
      Run run = _currentRun;
      if (message instanceof S2ControlMessages.RoundRequest) {
        S2ControlMessages.RoundRequest request = (S2ControlMessages.RoundRequest) message;
        if (run == null || run.runId != request.runId || run.failure != null) {
          // A straggler from a finished/failed run, or a request after a peer died: reply with a
          // benign value so this worker's fixpoint unwinds instead of blocking the barrier.
          session.send(new S2ControlMessages.RoundResponse(false));
        } else {
          session.send(new S2ControlMessages.RoundResponse(run.rounds.check(request.localDirty)));
        }
      } else if (message instanceof S2ControlMessages.SumRequest) {
        S2ControlMessages.SumRequest request = (S2ControlMessages.SumRequest) message;
        if (run == null || run.runId != request.runId || run.failure != null) {
          session.send(new S2ControlMessages.SumResponse(0));
        } else {
          session.send(new S2ControlMessages.SumResponse(run.sums.check(request.value)));
        }
      } else if (message instanceof S2ControlMessages.Done) {
        S2ControlMessages.Done done = (S2ControlMessages.Done) message;
        if (run != null && run.runId == done.runId) {
          run.peakHeapBytes += done.peakHeapBytes;
          if (run.done.incrementAndGet() == _numWorkers) {
            run.doneLatch.countDown();
          }
        }
      } else if (message instanceof S2ControlMessages.WorkerError) {
        S2ControlMessages.WorkerError error = (S2ControlMessages.WorkerError) message;
        if (run != null && run.runId == error.runId) {
          run.failure = "worker " + error.workerId + " failed: " + error.message;
          run.doneLatch.countDown();
        }
      } else {
        System.err.println(
            "S2 controller-service: ignoring unexpected worker message " + message.getClass());
      }
    }
  }

  private void markWorkerDead(WorkerSession session) {
    if (_closed) {
      return;
    }
    System.err.printf("S2 controller-service: worker %d disconnected%n", session.workerId);
    Run run = _currentRun;
    if (run != null && run.failure == null) {
      run.failure = "worker " + session.workerId + " disconnected";
      run.doneLatch.countDown();
    }
  }

  private void handleCompute(ObjectOutputStream out, S2ControlMessages.ComputeRequest request) {
    S2ControlMessages.ComputeResponse response;
    synchronized (_runLock) {
      response = runSnapshot(request);
    }
    try {
      out.writeObject(response);
      out.flush();
    } catch (IOException e) {
      System.err.println("S2 controller-service: could not send compute response: " + e);
    }
  }

  private S2ControlMessages.ComputeResponse runSnapshot(S2ControlMessages.ComputeRequest request) {
    try {
      if (!_allWorkersRegistered.await(_registrationTimeoutSeconds, TimeUnit.SECONDS)) {
        return fail(request, "only " + _workers.size() + "/" + _numWorkers + " workers registered");
      }
      SortedMap<String, Configuration> configs =
          S2ControlMessages.deserializeConfigs(request.configs);
      S2Snapshot snap = S2Snapshot.fromConfigs(configs);
      S2Sharding.assertDistributedProtocolsSupported(snap, _numWorkers);
      S2Sharding.Plan plan = S2Sharding.plan(snap, _numWorkers);
      System.out.printf(
          "S2 controller-service: snapshot %s %s%n",
          request.snapshotName, S2Sharding.describe(plan, _numWorkers));

      boolean shipConfigs = !Boolean.getBoolean("s2.noShipConfigs");
      boolean descriptorShadowsRequested =
          Boolean.parseBoolean(System.getProperty("s2.descriptorShadows", "true"));
      S2Sharding.Payload payload =
          S2Sharding.preparePayload(
              snap, plan.assignment, _numWorkers, shipConfigs, descriptorShadowsRequested);
      if (payload.descriptorShadows()) {
        System.out.printf(
            "S2 controller-service: descriptor shadows on (%d full configs + %d descriptors)%n",
            _numWorkers, payload.numDescriptors);
      }

      int runId = _runIds.incrementAndGet();
      Run run = new Run(runId, _numWorkers);
      _currentRun = run;
      List<S2WorkerEndpoint> endpoints = endpointsById();
      for (WorkerSession session : _workers.values()) {
        byte[] ownedConfigs =
            payload.ownedConfigsByWorker == null
                ? null
                : payload.ownedConfigsByWorker.get(session.workerId);
        session.send(
            new S2ControlMessages.Start(
                endpoints,
                plan.assignment,
                payload.configs,
                payload.externalAdverts,
                ownedConfigs,
                payload.descriptors,
                request.sliceDir,
                runId));
      }
      boolean completed = run.doneLatch.await(_computeTimeoutSeconds, TimeUnit.SECONDS);
      if (!completed) {
        return fail(request, "timed out waiting for workers");
      }
      if (run.failure != null) {
        return fail(request, run.failure);
      }
      if (run.done.get() != _numWorkers) {
        return fail(request, "only " + run.done.get() + "/" + _numWorkers + " workers finished");
      }
      System.out.printf(
          "S2 controller-service: snapshot %s done (%d workers, %.1f MiB total peak heap)%n",
          request.snapshotName, _numWorkers, run.peakHeapBytes / 1048576.0);
      return new S2ControlMessages.ComputeResponse(true, request.sliceDir, _numWorkers, "");
    } catch (Exception e) {
      e.printStackTrace();
      return fail(request, "controller error: " + e);
    }
  }

  private S2ControlMessages.ComputeResponse fail(
      S2ControlMessages.ComputeRequest request, String message) {
    return new S2ControlMessages.ComputeResponse(false, request.sliceDir, _workers.size(), message);
  }

  private List<S2WorkerEndpoint> endpointsById() {
    List<S2WorkerEndpoint> endpoints = new ArrayList<>();
    for (int w = 0; w < _numWorkers; w++) {
      WorkerSession session = _workers.get(w);
      endpoints.add(session == null ? null : session.endpoint);
    }
    return endpoints;
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
    for (WorkerSession session : _workers.values()) {
      session.closeQuietly();
    }
  }

  /** A snapshot run's barriers, completion latch, and failure state. */
  private static final class Run {
    final int runId;
    final S2RoundBarrier rounds;
    final S2SumBarrier sums;
    final AtomicInteger done = new AtomicInteger();
    final CountDownLatch doneLatch = new CountDownLatch(1);
    volatile String failure;
    volatile long peakHeapBytes;

    Run(int runId, int numWorkers) {
      this.runId = runId;
      this.rounds = new S2RoundBarrier(numWorkers);
      this.sums = new S2SumBarrier(numWorkers);
    }
  }

  /** One registered worker's controller connection. */
  private static final class WorkerSession {
    final int workerId;
    final S2WorkerEndpoint endpoint;
    private final Socket socket;
    private final ObjectOutputStream out;
    private final ObjectInputStream in;

    WorkerSession(
        int workerId,
        S2WorkerEndpoint endpoint,
        Socket socket,
        ObjectOutputStream out,
        ObjectInputStream in) {
      this.workerId = workerId;
      this.endpoint = endpoint;
      this.socket = socket;
      this.out = out;
      this.in = in;
    }

    /** Read the next message from the worker. Only the session's reader thread calls this. */
    Object read() throws IOException, ClassNotFoundException {
      return in.readObject();
    }

    synchronized void send(Object message) {
      try {
        out.writeObject(message);
        out.flush();
      } catch (IOException e) {
        throw new RuntimeException(
            "S2 controller-service send to worker " + workerId + " failed", e);
      }
    }

    void closeQuietly() {
      try {
        socket.close();
      } catch (IOException e) {
        // ignore
      }
    }
  }
}
