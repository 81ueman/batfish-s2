// SPDX-License-Identifier: Apache-2.0

package org.batfish.dataplane.ibdp;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.UncheckedIOException;
import java.nio.file.AtomicMoveNotSupportedException;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.LinkedHashSet;
import java.util.Set;
import javax.annotation.Nullable;
import org.batfish.storage.FileBasedStorage;
import org.batfish.storage.HostDataPlaneSlice;

/**
 * Directory-backed {@link S2HostSlices}: stores one file per host under a directory and reads a
 * host's slice on demand. This is the Kubernetes shared-PVC model: a worker writes its owned hosts'
 * slices and the engine (a different JVM/Pod) resolves a host by name without loading the rest.
 *
 * <p>File names are the URL-safe Base64 of the hostname, the same convention {@link
 * FileBasedStorage} uses, so arbitrary hostnames map to valid file names. Writes go to a hidden
 * temporary file and are moved into place, so a reader never observes a partial slice.
 */
public final class S2DirectoryHostSlices implements S2HostSlices {

  private final Path _directory;
  private final Set<String> _hosts;

  private S2DirectoryHostSlices(Path directory, Set<String> hosts) {
    _directory = directory;
    _hosts = hosts;
  }

  /** Writes every slice in {@code source} as one file per host under {@code directory}. */
  public static void write(Path directory, S2HostSlices source) throws IOException {
    Files.createDirectories(directory);
    for (String host : source.hosts()) {
      HostDataPlaneSlice slice = source.get(host);
      if (slice != null) {
        writeSlice(directory, host, slice);
      }
    }
  }

  /**
   * Opens the slices already written under {@code directory}. The host names come from the file
   * names; the slice contents are read lazily by {@link #get}.
   */
  public static S2DirectoryHostSlices read(Path directory) throws IOException {
    Set<String> hosts = new LinkedHashSet<>();
    try (DirectoryStream<Path> files = Files.newDirectoryStream(directory)) {
      for (Path file : files) {
        String name = file.getFileName().toString();
        if (Files.isRegularFile(file) && !name.startsWith(".")) {
          hosts.add(FileBasedStorage.fromBase64(name));
        }
      }
    }
    return new S2DirectoryHostSlices(directory, hosts);
  }

  /**
   * Recursively delete a slice directory (slice GC). The engine registers this for the per-snapshot
   * directory it created, so the shared volume does not accumulate one directory per snapshot.
   * Missing files are ignored, so this is safe to call more than once.
   */
  public static void deleteRecursively(Path directory) throws IOException {
    if (!Files.exists(directory)) {
      return;
    }
    try (java.util.stream.Stream<Path> paths = Files.walk(directory)) {
      for (Path path : paths.sorted(java.util.Comparator.reverseOrder()).toList()) {
        Files.deleteIfExists(path);
      }
    }
  }

  @Override
  public Set<String> hosts() {
    return _hosts;
  }

  @Override
  public @Nullable HostDataPlaneSlice get(String host) {
    if (!_hosts.contains(host)) {
      return null;
    }
    try {
      return readSlice(_directory, host);
    } catch (IOException | ClassNotFoundException e) {
      throw new UncheckedIOException(
          new IOException("Failed to read S2 host slice for " + host, e));
    }
  }

  private static void writeSlice(Path directory, String host, HostDataPlaneSlice slice)
      throws IOException {
    Path output = directory.resolve(FileBasedStorage.toBase64(host));
    Path tmp = output.resolveSibling("." + output.getFileName() + ".tmp");
    try {
      try (ObjectOutputStream out = new ObjectOutputStream(Files.newOutputStream(tmp))) {
        out.writeObject(slice);
      }
      try {
        Files.move(
            tmp, output, StandardCopyOption.REPLACE_EXISTING, StandardCopyOption.ATOMIC_MOVE);
      } catch (AtomicMoveNotSupportedException e) {
        Files.move(tmp, output, StandardCopyOption.REPLACE_EXISTING);
      }
    } finally {
      Files.deleteIfExists(tmp);
    }
  }

  private static HostDataPlaneSlice readSlice(Path directory, String host)
      throws IOException, ClassNotFoundException {
    Path input = directory.resolve(FileBasedStorage.toBase64(host));
    try (ObjectInputStream in = new ObjectInputStream(Files.newInputStream(input))) {
      return (HostDataPlaneSlice) in.readObject();
    }
  }
}
