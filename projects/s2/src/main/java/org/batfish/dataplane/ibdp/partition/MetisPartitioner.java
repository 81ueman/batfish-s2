// SPDX-License-Identifier: Apache-2.0

package org.batfish.dataplane.ibdp.partition;

import java.io.BufferedWriter;
import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * METIS partitioner: writes a {@code gpmetis} input graph and runs {@code gpmetis -seed=<seed>}
 * (port plan &sect;3.3). METIS is the quality reference for the paper; it is an external
 * dependency, so if the binary is missing (or fails/times out) this falls back to {@link
 * WeightedLptFmPartitioner} with a warning.
 *
 * <p>The graph is written with vertex weights (from {@link NodeWeights}) and edge weights
 * (estimated exchange volume), in the standard METIS adjacency format: header {@code n m fmt ncon}
 * with {@code fmt=11} (edge + vertex weights) and {@code ncon=1}; each vertex line is {@code vwgt
 * nbr ewgt ...} with 1-based neighbors, and an undirected edge is listed from both endpoints.
 *
 * <p>A fixed {@code -ptype=rb -ufactor=1} is passed so METIS balances the (weighted) vertex load
 * tightly: the default {@code gpmetis} recursive-bisection balance is already {@code 1.001}, but
 * pinning both options keeps the invocation explicit and stable across METIS builds.
 *
 * <p>Determinism: METIS is invoked with a fixed {@code -seed}; input vertices and neighbor lists
 * are sorted by hostname. The process path can be overridden with {@code
 * -Ds2.metis.path=/path/to/gpmetis}.
 */
public final class MetisPartitioner implements NodePartitioner {

  /** System property overriding the {@code gpmetis} executable path. */
  public static final String METIS_PATH_PROPERTY = "s2.metis.path";

  /** Maximum time to wait for {@code gpmetis}. */
  private static final long TIMEOUT_SECONDS = 300;

  /** Maximum time to wait for the availability probe, which only prints usage and exits. */
  private static final long PROBE_TIMEOUT_SECONDS = 10;

  private final NodePartitioner _fallback = new WeightedLptFmPartitioner();

  /**
   * Whether the {@code gpmetis} executable can be started, honoring the {@link
   * #METIS_PATH_PROPERTY} override. Used by {@link AutoSchemeSelector} to decide whether the {@code
   * AUTO} scheme can prefer METIS. A binary that starts but exits non-zero (as bare {@code gpmetis}
   * does when it prints usage) is still "available".
   */
  public static boolean isAvailable() {
    String metis = System.getProperty(METIS_PATH_PROPERTY, "gpmetis");
    Process process = null;
    try {
      process =
          new ProcessBuilder(metis)
              .redirectOutput(ProcessBuilder.Redirect.DISCARD)
              .redirectError(ProcessBuilder.Redirect.DISCARD)
              .start();
      process.waitFor(PROBE_TIMEOUT_SECONDS, TimeUnit.SECONDS);
      return !process.isAlive();
    } catch (IOException e) {
      return false;
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      return false;
    } finally {
      if (process != null && process.isAlive()) {
        process.destroyForcibly();
      }
    }
  }

  @Override
  public Map<String, Integer> partition(CommunicationGraph graph, int numWorkers, long seed) {
    if (numWorkers < 1) {
      throw new IllegalArgumentException("numWorkers must be >= 1");
    }
    if (numWorkers == 1) {
      Map<String, Integer> single = new HashMap<>();
      graph.nodes().forEach(node -> single.put(node, 0));
      return single;
    }
    if (numWorkers >= graph.nodes().size()) {
      // METIS requires fewer parts than vertices; a trivial one-node-per-worker assignment is fine.
      List<String> nodes = new ArrayList<>(graph.nodes());
      Map<String, Integer> assignment = new HashMap<>();
      for (int i = 0; i < nodes.size(); i++) {
        assignment.put(nodes.get(i), i);
      }
      return assignment;
    }
    try {
      return runMetis(graph, numWorkers, seed);
    } catch (IOException | RuntimeException e) {
      System.err.printf(
          "S2 partition METIS: %s; falling back to WEIGHTED_LPT_FM%n", e.getMessage());
      return _fallback.partition(graph, numWorkers, seed);
    }
  }

  private static Map<String, Integer> runMetis(CommunicationGraph graph, int numWorkers, long seed)
      throws IOException {
    List<String> nodes = new ArrayList<>(graph.nodes());
    Path dir = Files.createTempDirectory("s2-metis");
    try {
      Path input = dir.resolve("s2-metis.graph");
      writeGraph(graph, nodes, input);
      String metis = System.getProperty(METIS_PATH_PROPERTY, "gpmetis");
      ProcessBuilder builder =
          new ProcessBuilder(
              metis,
              "-seed=" + seed,
              "-ptype=rb",
              "-ufactor=1",
              input.toString(),
              Integer.toString(numWorkers));
      builder.redirectErrorStream(true);
      Process process = builder.start();
      boolean finished;
      try {
        finished = process.waitFor(TIMEOUT_SECONDS, TimeUnit.SECONDS);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        process.destroyForcibly();
        throw new IOException("interrupted waiting for gpmetis");
      }
      if (!finished) {
        process.destroyForcibly();
        throw new IOException("gpmetis timed out after " + TIMEOUT_SECONDS + "s");
      }
      if (process.exitValue() != 0) {
        throw new IOException("gpmetis exited with status " + process.exitValue());
      }
      Path output = input.resolveSibling(input.getFileName() + ".part." + numWorkers);
      if (!Files.exists(output)) {
        throw new IOException("gpmetis did not produce " + output.getFileName());
      }
      List<String> lines = Files.readAllLines(output, StandardCharsets.UTF_8);
      if (lines.size() < nodes.size()) {
        throw new IOException(
            "gpmetis output has " + lines.size() + " lines for " + nodes.size() + " vertices");
      }
      Map<String, Integer> assignment = new HashMap<>();
      for (int i = 0; i < nodes.size(); i++) {
        assignment.put(nodes.get(i), Integer.parseInt(lines.get(i).trim()));
      }
      return assignment;
    } finally {
      deleteRecursively(dir);
    }
  }

  @com.google.common.annotations.VisibleForTesting
  static void writeGraph(CommunicationGraph graph, List<String> nodes, Path input)
      throws IOException {
    Map<String, Integer> index = new HashMap<>();
    for (int i = 0; i < nodes.size(); i++) {
      index.put(nodes.get(i), i + 1);
    }
    // METIS counts undirected edges once; count them from each node's lower-numbered neighbors.
    long numEdges = 0;
    for (String node : nodes) {
      int u = index.get(node);
      for (String neighbor : graph.neighbors(node).keySet()) {
        if (index.get(neighbor) > u) {
          numEdges++;
        }
      }
    }
    try (BufferedWriter writer = Files.newBufferedWriter(input, StandardCharsets.UTF_8)) {
      writer.write(nodes.size() + " " + numEdges + " 11 1\n");
      for (String node : nodes) {
        StringBuilder line = new StringBuilder();
        line.append(Math.max(1, graph.weight(node)));
        List<String> neighbors = new ArrayList<>(graph.neighbors(node).keySet());
        neighbors.sort(Comparator.naturalOrder());
        for (String neighbor : neighbors) {
          // METIS adjacency entry order is <neighbor> <edge-weight> (the vertex weight came first).
          line.append(' ')
              .append(index.get(neighbor))
              .append(' ')
              .append(Math.max(1, graph.edgeWeight(node, neighbor)));
        }
        writer.write(line.toString());
        writer.write('\n');
      }
    }
  }

  private static void deleteRecursively(Path dir) {
    try {
      if (dir == null || !Files.exists(dir)) {
        return;
      }
      try (java.util.stream.Stream<Path> paths = Files.walk(dir)) {
        paths.sorted(Comparator.reverseOrder()).map(Path::toFile).forEach(File::delete);
      }
    } catch (IOException e) {
      // Best-effort cleanup; the OS temp dir will eventually reclaim it.
    }
  }
}
