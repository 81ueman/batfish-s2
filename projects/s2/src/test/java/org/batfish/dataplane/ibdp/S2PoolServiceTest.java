// SPDX-License-Identifier: Apache-2.0

package org.batfish.dataplane.ibdp;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;

import com.google.common.collect.ImmutableList;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.SortedMap;
import org.batfish.common.NetworkSnapshot;
import org.batfish.datamodel.BgpAdvertisement;
import org.batfish.datamodel.Configuration;
import org.batfish.datamodel.DataPlane;
import org.batfish.main.Batfish;
import org.batfish.main.BatfishTestUtils;
import org.batfish.main.TestrigText;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

/**
 * End-to-end test of the persistent pool (choice A): a long-lived controller service plus N worker
 * services on one machine, driven by the engine-side {@link S2ControllerClient} and by the real
 * {@link S2DataPlanePlugin}. The pool serves two different snapshots without restarting, and the
 * resulting data plane matches the vanilla single-machine engine.
 */
public final class S2PoolServiceTest {

  private static final int WORKERS = 3;

  private static final String TRIANGLE = "org/batfish/dataplane/testrigs/s2-triangle";
  private static final List<String> TRIANGLE_CONFIGS = ImmutableList.of("r1", "r2", "r3");
  private static final String OSPF_BGP = "org/batfish/dataplane/testrigs/s2-ospf-bgp";
  private static final List<String> OSPF_BGP_CONFIGS = ImmutableList.of("r1", "r2", "r3");

  @Rule public TemporaryFolder _folder = new TemporaryFolder();

  /** A snapshot's vanilla data plane plus the inputs the engine ships to the controller. */
  private static final class SnapshotInput {
    final String name;
    final TestrigText testrigText;
    final SortedMap<String, Configuration> configs;
    final Set<BgpAdvertisement> adverts;
    final DataPlane vanilla;

    SnapshotInput(
        String name,
        TestrigText testrigText,
        SortedMap<String, Configuration> configs,
        Set<BgpAdvertisement> adverts,
        DataPlane vanilla) {
      this.name = name;
      this.testrigText = testrigText;
      this.configs = configs;
      this.adverts = adverts;
      this.vanilla = vanilla;
    }
  }

  private SnapshotInput loadVanilla(String testrig, List<String> configs) throws Exception {
    TestrigText testrigText = TestrigText.builder().setConfigurationFiles(testrig, configs).build();
    Batfish batfish = BatfishTestUtils.getBatfishFromTestrigText(testrigText, _folder);
    batfish.getSettings().setDataplaneEngineName(IncrementalDataPlanePlugin.PLUGIN_NAME);
    NetworkSnapshot snapshot = batfish.getSnapshot();
    batfish.computeDataPlane(snapshot);
    DataPlane vanilla = batfish.loadDataPlane(snapshot);
    SortedMap<String, Configuration> configurations = batfish.loadConfigurations(snapshot);
    Set<BgpAdvertisement> adverts = batfish.loadExternalBgpAnnouncements(snapshot, configurations);
    return new SnapshotInput(testrig, testrigText, configurations, adverts, vanilla);
  }

  /**
   * One pool serves two snapshots: the workers stay registered between them, and each snapshot's
   * slices match vanilla.
   */
  @Test
  public void testPoolServesMultipleSnapshots() throws Exception {
    SnapshotInput triangle = loadVanilla(TRIANGLE, TRIANGLE_CONFIGS);
    SnapshotInput ospfBgp = loadVanilla(OSPF_BGP, OSPF_BGP_CONFIGS);

    try (Pool pool = new Pool(WORKERS)) {
      assertSnapshotMatches(pool.controller(), triangle);
      assertSnapshotMatches(pool.controller(), ospfBgp);
    }
  }

  /**
   * The real plugin, configured with {@code s2controllerhost}/{@code s2slicedir}, matches vanilla.
   */
  @Test
  public void testPluginDrivesControllerService() throws Exception {
    SnapshotInput triangle = loadVanilla(TRIANGLE, TRIANGLE_CONFIGS);

    try (Pool pool = new Pool(WORKERS)) {
      Path sliceBase = _folder.newFolder("slices").toPath();
      Batfish s2 = BatfishTestUtils.getBatfishFromTestrigText(triangle.testrigText, _folder);
      new S2DataPlanePlugin().initialize(s2);
      s2.getSettings().setDataplaneEngineName(S2DataPlanePlugin.PLUGIN_NAME);
      s2.getSettings().setS2ControllerHost("127.0.0.1");
      s2.getSettings().setS2ControllerPort(pool.port());
      s2.getSettings().setS2SliceDir(sliceBase.toString());
      s2.getSettings().setS2StoreDataPlane(false);
      NetworkSnapshot snapshot = s2.getSnapshot();
      s2.computeDataPlane(snapshot);
      DataPlane actual = s2.loadDataPlane(snapshot);

      assertThat(actual.getRibs(), equalTo(triangle.vanilla.getRibs()));
      assertThat(actual.getBgpRoutes(), equalTo(triangle.vanilla.getBgpRoutes()));
      assertThat(actual.getFibs().keySet(), equalTo(triangle.vanilla.getFibs().keySet()));
      assertThat(
          actual.getForwardingAnalysis().getVrfForwardingBehavior(),
          equalTo(triangle.vanilla.getForwardingAnalysis().getVrfForwardingBehavior()));
    }
  }

  /** A controller service plus its N worker services, started together and closed as a unit. */
  @SuppressWarnings("PMD.CloseResource") // workers are closed together in close()
  private static final class Pool implements AutoCloseable {
    private final S2ControllerService _controller;
    private final List<S2WorkerService> _workers = new ArrayList<>();

    Pool(int numWorkers) throws Exception {
      _controller = new S2ControllerService(0, numWorkers);
      _controller.start();
      for (int w = 0; w < numWorkers; w++) {
        S2WorkerService worker =
            new S2WorkerService(w, "127.0.0.1", _controller.getPort(), 0, "127.0.0.1");
        worker.start();
        _workers.add(worker);
      }
      if (!_controller.awaitWorkers(120)) {
        close();
        throw new IllegalStateException("S2 workers did not all register");
      }
    }

    int port() {
      return _controller.getPort();
    }

    S2ControllerService controller() {
      return _controller;
    }

    @Override
    public void close() {
      for (S2WorkerService worker : _workers) {
        worker.close();
      }
      _controller.close();
    }
  }

  private void assertSnapshotMatches(S2ControllerService controller, SnapshotInput input)
      throws Exception {
    Path sliceDir = _folder.newFolder().toPath();
    S2ControlMessages.ComputeRequest request =
        new S2ControlMessages.ComputeRequest(
            S2ControlMessages.serializeConfigs(input.configs),
            S2ControlMessages.serializeExternalAdverts(input.adverts),
            sliceDir.toString(),
            input.name);
    S2ControlMessages.ComputeResponse response =
        new S2ControllerClient("127.0.0.1", controller.getPort()).compute(request);
    assertThat(response.ok, equalTo(true));
    assertThat(response.numWorkers, equalTo(WORKERS));

    S2HostSlices slices = S2DirectoryHostSlices.read(sliceDir);
    DataPlane lazy = S2LazyDataPlane.of(slices);
    assertThat(slices.hosts(), equalTo(input.vanilla.getFibs().keySet()));
    assertThat(lazy.getRibs(), equalTo(input.vanilla.getRibs()));
    assertThat(lazy.getBgpRoutes(), equalTo(input.vanilla.getBgpRoutes()));
    assertThat(lazy.getFibs().keySet(), equalTo(input.vanilla.getFibs().keySet()));
    assertThat(
        lazy.getForwardingAnalysis().getVrfForwardingBehavior(),
        equalTo(input.vanilla.getForwardingAnalysis().getVrfForwardingBehavior()));
  }
}
