// SPDX-License-Identifier: Apache-2.0

package org.batfish.dataplane.ibdp;

import java.util.Set;
import javax.annotation.Nullable;
import org.batfish.storage.HostDataPlaneSlice;

/**
 * A source of per-host data-plane slices for the lazy global data plane.
 *
 * <p>This is the seam that lets {@link S2LazyDataPlane} resolve a host's slice from anywhere: today
 * the in-process workers' data planes ({@link S2InProcessHostSlices}), and, once the worker pool
 * lands, a directory shared by the remote workers ({@link S2DirectoryHostSlices}). A slice may be
 * fetched or deserialized on demand, so callers must not assume {@link #get} is cheap.
 */
public interface S2HostSlices {

  /** The hosts for which a slice is available. */
  Set<String> hosts();

  /** The slice for {@code host}, fetching it on demand, or {@code null} if there is none. */
  @Nullable
  HostDataPlaneSlice get(String host);
}
