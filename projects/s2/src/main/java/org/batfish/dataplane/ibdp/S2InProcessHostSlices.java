// SPDX-License-Identifier: Apache-2.0

package org.batfish.dataplane.ibdp;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import javax.annotation.Nullable;
import org.batfish.datamodel.DataPlane;
import org.batfish.storage.HostDataPlaneSlice;

/**
 * In-process {@link S2HostSlices}: extracts each host's slice eagerly from the per-worker {@link
 * DataPlane}s of a local S2 run, mirroring how {@link
 * org.batfish.storage.FileBasedStorage#storeDataPlane} reads the data plane one host at a time.
 *
 * <p>A host owned by more than one worker (only possible when owned-only mode is disabled and every
 * worker computed a full data plane) is taken from the first worker that has it; the results are
 * identical by construction.
 */
final class S2InProcessHostSlices implements S2HostSlices {

  private final Map<String, HostDataPlaneSlice> _slices;

  private S2InProcessHostSlices(Map<String, HostDataPlaneSlice> slices) {
    _slices = slices;
  }

  static S2InProcessHostSlices of(List<DataPlane> dataPlanes) {
    return of(dataPlanes, null);
  }

  /**
   * Like {@link #of(List)}, but only exposes hosts in {@code onlyHosts}. A remote worker in
   * full-dataplane mode has every host's final RIBs, but must write only the hosts it owns to the
   * shared slice directory (peers write the rest), so callers filter here.
   */
  static S2InProcessHostSlices of(List<DataPlane> dataPlanes, @Nullable Set<String> onlyHosts) {
    Map<String, HostDataPlaneSlice> slices = new LinkedHashMap<>();
    for (DataPlane dataPlane : dataPlanes) {
      // In owned-only mode a worker's FIBs cover every node (remote ones are stubs), so enumerate
      // a worker's *owned* hosts by its final RIBs: only owned nodes have one. In full mode every
      // worker has every host, and taking the first is correct because the data is identical.
      for (String host : dataPlane.getRibs().rowKeySet()) {
        if (onlyHosts != null && !onlyHosts.contains(host)) {
          continue;
        }
        if (!slices.containsKey(host)) {
          slices.put(host, HostDataPlaneSlice.extract(dataPlane, host));
        }
      }
    }
    return new S2InProcessHostSlices(slices);
  }

  @Override
  public Set<String> hosts() {
    return _slices.keySet();
  }

  @Override
  public @Nullable HostDataPlaneSlice get(String host) {
    return _slices.get(host);
  }
}
