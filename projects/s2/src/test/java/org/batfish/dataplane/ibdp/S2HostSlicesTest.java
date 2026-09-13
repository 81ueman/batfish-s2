// SPDX-License-Identifier: Apache-2.0

package org.batfish.dataplane.ibdp;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;

import com.google.common.collect.ImmutableList;
import java.io.IOException;
import java.nio.file.Path;
import java.util.List;
import java.util.Set;
import org.batfish.common.NetworkSnapshot;
import org.batfish.datamodel.DataPlane;
import org.batfish.main.Batfish;
import org.batfish.main.BatfishTestUtils;
import org.batfish.main.TestrigText;
import org.batfish.storage.HostDataPlaneSlice;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

/**
 * Tests the {@link S2HostSlices} seam: the in-process extraction of per-host slices, the
 * directory-backed (shared-storage) source's round trip, and the lazy resolution of {@link
 * S2LazyDataPlane}.
 */
public final class S2HostSlicesTest {

  private static final String TESTRIG = "org/batfish/dataplane/testrigs/s2-triangle";
  private static final List<String> CONFIGS = ImmutableList.of("r1", "r2", "r3");

  @Rule public TemporaryFolder _folder = new TemporaryFolder();

  private DataPlane loadVanillaDataPlane() throws IOException {
    TestrigText testrigText = TestrigText.builder().setConfigurationFiles(TESTRIG, CONFIGS).build();
    Batfish batfish = BatfishTestUtils.getBatfishFromTestrigText(testrigText, _folder);
    batfish.getSettings().setDataplaneEngineName(IncrementalDataPlanePlugin.PLUGIN_NAME);
    NetworkSnapshot snapshot = batfish.getSnapshot();
    batfish.computeDataPlane(snapshot);
    return batfish.loadDataPlane(snapshot);
  }

  /** The in-process source exposes exactly the data plane's hosts and per-host content. */
  @Test
  public void testInProcessSlicesMatchDataPlane() throws IOException {
    DataPlane dataPlane = loadVanillaDataPlane();
    S2HostSlices slices = S2InProcessHostSlices.of(ImmutableList.of(dataPlane));

    assertThat(slices.hosts(), equalTo(dataPlane.getFibs().keySet()));
    for (String host : dataPlane.getFibs().keySet()) {
      HostDataPlaneSlice slice = slices.get(host);
      assertThat(slice, notNullValue());
      assertThat(slice.getRibs(), equalTo(dataPlane.getRibs().row(host)));
      assertThat(slice.getFibs(), equalTo(dataPlane.getFibs().get(host)));
      assertThat(slice.getBgpRoutes(), equalTo(dataPlane.getBgpRoutes().row(host)));
      assertThat(slice.getBgpBackupRoutes(), equalTo(dataPlane.getBgpBackupRoutes().row(host)));
      assertThat(slice.getEvpnRoutes(), equalTo(dataPlane.getEvpnRoutes().row(host)));
      assertThat(slice.getEvpnBackupRoutes(), equalTo(dataPlane.getEvpnBackupRoutes().row(host)));
      assertThat(slice.getLayer2Vnis(), equalTo(dataPlane.getLayer2Vnis().row(host)));
      assertThat(slice.getLayer3Vnis(), equalTo(dataPlane.getLayer3Vnis().row(host)));
      assertThat(
          slice.getArpReplies(),
          equalTo(dataPlane.getForwardingAnalysis().getArpReplies().get(host)));
      assertThat(
          slice.getVrfForwardingBehavior(),
          equalTo(dataPlane.getForwardingAnalysis().getVrfForwardingBehavior().get(host)));
      assertThat(
          slice.getPrefixTracingInfoSummary(),
          equalTo(dataPlane.getPrefixTracingInfoSummary().get(host)));
    }
  }

  /** Writing the slices to a directory and reading them back preserves every host. */
  @Test
  public void testDirectoryRoundTrip() throws IOException {
    DataPlane dataPlane = loadVanillaDataPlane();
    S2HostSlices inProcess = S2InProcessHostSlices.of(ImmutableList.of(dataPlane));

    Path dir = _folder.newFolder().toPath();
    S2DirectoryHostSlices.write(dir, inProcess);
    S2HostSlices onDisk = S2DirectoryHostSlices.read(dir);

    assertThat(onDisk.hosts(), equalTo(inProcess.hosts()));
    for (String host : inProcess.hosts()) {
      HostDataPlaneSlice expected = inProcess.get(host);
      HostDataPlaneSlice actual = onDisk.get(host);
      assertThat(actual, notNullValue());
      assertThat(actual.getRibs(), equalTo(expected.getRibs()));
      // FIBs do not implement value equality, so compare their entries instead.
      assertThat(actual.getFibs().keySet(), equalTo(expected.getFibs().keySet()));
      expected
          .getFibs()
          .forEach(
              (vrf, fib) ->
                  assertThat(actual.getFibs().get(vrf).allEntries(), equalTo(fib.allEntries())));
      assertThat(actual.getBgpRoutes(), equalTo(expected.getBgpRoutes()));
      assertThat(actual.getBgpBackupRoutes(), equalTo(expected.getBgpBackupRoutes()));
      assertThat(actual.getEvpnRoutes(), equalTo(expected.getEvpnRoutes()));
      assertThat(actual.getEvpnBackupRoutes(), equalTo(expected.getEvpnBackupRoutes()));
      assertThat(actual.getLayer2Vnis(), equalTo(expected.getLayer2Vnis()));
      assertThat(actual.getLayer3Vnis(), equalTo(expected.getLayer3Vnis()));
      assertThat(actual.getArpReplies(), equalTo(expected.getArpReplies()));
      assertThat(actual.getVrfForwardingBehavior(), equalTo(expected.getVrfForwardingBehavior()));
      assertThat(
          actual.getPrefixTracingInfoSummary(), equalTo(expected.getPrefixTracingInfoSummary()));
    }
    assertThat(onDisk.get("no-such-host"), nullValue());
  }

  /** The lazy data plane is indistinguishable from the source it was built from. */
  @Test
  public void testLazyDataPlaneMatchesSource() throws IOException {
    DataPlane dataPlane = loadVanillaDataPlane();
    S2HostSlices slices = S2InProcessHostSlices.of(ImmutableList.of(dataPlane));
    DataPlane lazy = S2LazyDataPlane.of(slices);

    assertThat(lazy.getRibs(), equalTo(dataPlane.getRibs()));
    assertThat(lazy.getBgpRoutes(), equalTo(dataPlane.getBgpRoutes()));
    assertThat(lazy.getBgpBackupRoutes(), equalTo(dataPlane.getBgpBackupRoutes()));
    assertThat(lazy.getEvpnRoutes(), equalTo(dataPlane.getEvpnRoutes()));
    assertThat(lazy.getEvpnBackupRoutes(), equalTo(dataPlane.getEvpnBackupRoutes()));
    assertThat(lazy.getFibs(), equalTo(dataPlane.getFibs()));
    assertThat(lazy.getLayer2Vnis(), equalTo(dataPlane.getLayer2Vnis()));
    assertThat(lazy.getLayer3Vnis(), equalTo(dataPlane.getLayer3Vnis()));
    assertThat(
        lazy.getPrefixTracingInfoSummary(), equalTo(dataPlane.getPrefixTracingInfoSummary()));
    assertThat(
        lazy.getForwardingAnalysis().getArpReplies(),
        equalTo(dataPlane.getForwardingAnalysis().getArpReplies()));
    assertThat(
        lazy.getForwardingAnalysis().getVrfForwardingBehavior(),
        equalTo(dataPlane.getForwardingAnalysis().getVrfForwardingBehavior()));
  }

  /**
   * A point/row lookup must resolve only the addressed host, never the others. The source throws on
   * any other host, so a lookup that touches the whole network fails loudly.
   */
  @Test
  public void testPointLookupsResolveOnlyOwningHost() throws IOException {
    DataPlane dataPlane = loadVanillaDataPlane();
    S2HostSlices slices = S2InProcessHostSlices.of(ImmutableList.of(dataPlane));
    String target = "r1";
    S2HostSlices oneHost =
        new S2HostSlices() {
          @Override
          public Set<String> hosts() {
            return dataPlane.getFibs().keySet();
          }

          @Override
          public HostDataPlaneSlice get(String host) {
            if (!host.equals(target)) {
              throw new AssertionError("unexpected slice fetch for " + host);
            }
            return slices.get(host);
          }
        };
    DataPlane lazy = S2LazyDataPlane.of(oneHost);

    assertThat(lazy.getRibs().row(target), equalTo(dataPlane.getRibs().row(target)));
    assertThat(lazy.getFibs().get(target), equalTo(dataPlane.getFibs().get(target)));
    assertThat(lazy.getBgpRoutes().row(target), equalTo(dataPlane.getBgpRoutes().row(target)));
    assertThat(
        lazy.getForwardingAnalysis().getArpReplies().get(target),
        equalTo(dataPlane.getForwardingAnalysis().getArpReplies().get(target)));
    assertThat(
        lazy.getForwardingAnalysis().getVrfForwardingBehavior().get(target),
        equalTo(dataPlane.getForwardingAnalysis().getVrfForwardingBehavior().get(target)));
  }
}
