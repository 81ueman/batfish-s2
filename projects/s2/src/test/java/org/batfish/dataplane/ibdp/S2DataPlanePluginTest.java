// SPDX-License-Identifier: Apache-2.0

package org.batfish.dataplane.ibdp;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;

import com.google.common.collect.ImmutableList;
import java.io.IOException;
import java.util.List;
import org.batfish.common.NetworkSnapshot;
import org.batfish.datamodel.DataPlane;
import org.batfish.main.Batfish;
import org.batfish.main.BatfishTestUtils;
import org.batfish.main.TestrigText;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

/**
 * Proves S2 is usable through the standard Batfish interface: selected with {@code
 * -dataplaneengine=s2}, the plugin computes a data plane the normal question engine can consume,
 * and the result matches the stock {@code ibdp} engine. This is the engine-selection and
 * question-answering wiring (single worker for now); distribution is a later increment.
 */
public final class S2DataPlanePluginTest {

  private static final String TESTRIG = "org/batfish/dataplane/testrigs/s2-triangle";
  private static final List<String> CONFIGS = ImmutableList.of("r1", "r2", "r3");

  @Rule public TemporaryFolder _folder = new TemporaryFolder();

  @Test
  public void testS2EngineMatchesVanilla() throws IOException {
    TestrigText testrigText = TestrigText.builder().setConfigurationFiles(TESTRIG, CONFIGS).build();

    Batfish vanilla = BatfishTestUtils.getBatfishFromTestrigText(testrigText, _folder);
    vanilla.getSettings().setDataplaneEngineName(IncrementalDataPlanePlugin.PLUGIN_NAME);
    NetworkSnapshot vanillaSnapshot = vanilla.getSnapshot();
    vanilla.computeDataPlane(vanillaSnapshot);
    DataPlane vanillaDataPlane = vanilla.loadDataPlane(vanillaSnapshot);

    Batfish s2 = BatfishTestUtils.getBatfishFromTestrigText(testrigText, _folder);
    new S2DataPlanePlugin().initialize(s2);
    s2.getSettings().setDataplaneEngineName(S2DataPlanePlugin.PLUGIN_NAME);
    NetworkSnapshot s2Snapshot = s2.getSnapshot();
    s2.computeDataPlane(s2Snapshot);
    DataPlane s2DataPlane = s2.loadDataPlane(s2Snapshot);

    // The data plane the standard question engine sees must be identical to vanilla.
    assertThat(s2DataPlane.getRibs(), equalTo(vanillaDataPlane.getRibs()));
    assertThat(s2DataPlane.getBgpRoutes(), equalTo(vanillaDataPlane.getBgpRoutes()));
    assertThat(s2DataPlane.getFibs().keySet(), equalTo(vanillaDataPlane.getFibs().keySet()));
    assertThat(
        s2DataPlane.getForwardingAnalysis().getVrfForwardingBehavior(),
        equalTo(vanillaDataPlane.getForwardingAnalysis().getVrfForwardingBehavior()));
  }
}
