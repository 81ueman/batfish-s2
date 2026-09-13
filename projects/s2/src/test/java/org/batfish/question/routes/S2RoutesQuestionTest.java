// SPDX-License-Identifier: Apache-2.0

package org.batfish.question.routes;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;

import com.google.common.collect.ImmutableList;
import java.io.IOException;
import java.util.List;
import java.util.stream.Collectors;
import org.batfish.common.NetworkSnapshot;
import org.batfish.datamodel.answers.AnswerElement;
import org.batfish.datamodel.table.TableAnswerElement;
import org.batfish.dataplane.ibdp.IncrementalDataPlanePlugin;
import org.batfish.dataplane.ibdp.S2DataPlanePlugin;
import org.batfish.main.Batfish;
import org.batfish.main.BatfishTestUtils;
import org.batfish.main.TestrigText;
import org.batfish.question.routes.RoutesQuestion.RibProtocol;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

/**
 * End-to-end proof that a stock Batfish question works against the S2 engine: with {@code
 * -dataplaneengine=s2}, the standard {@link RoutesAnswerer} answers the standard {@link
 * RoutesQuestion} exactly as the stock {@code ibdp} engine does. This is the "vanilla question
 * engine, S2 underneath" path (no S2-specific API).
 */
public final class S2RoutesQuestionTest {

  private static final String TESTRIG = "org/batfish/dataplane/testrigs/s2-triangle";
  private static final List<String> CONFIGS = ImmutableList.of("r1", "r2", "r3");

  @Rule public TemporaryFolder _folder = new TemporaryFolder();

  @Test
  public void testRoutesAnswerMatchesVanillaThreeWorkers() throws IOException {
    TestrigText testrigText = TestrigText.builder().setConfigurationFiles(TESTRIG, CONFIGS).build();

    Batfish vanilla = BatfishTestUtils.getBatfishFromTestrigText(testrigText, _folder);
    vanilla.getSettings().setDataplaneEngineName(IncrementalDataPlanePlugin.PLUGIN_NAME);
    NetworkSnapshot vanillaSnapshot = vanilla.getSnapshot();
    vanilla.computeDataPlane(vanillaSnapshot);

    Batfish s2 = BatfishTestUtils.getBatfishFromTestrigText(testrigText, _folder);
    new S2DataPlanePlugin().initialize(s2);
    s2.getSettings().setDataplaneEngineName(S2DataPlanePlugin.PLUGIN_NAME);
    s2.getSettings().setS2Workers(3);
    NetworkSnapshot s2Snapshot = s2.getSnapshot();
    s2.computeDataPlane(s2Snapshot);

    RoutesQuestion question =
        new RoutesQuestion(null, null, null, null, null, RibProtocol.MAIN, null);
    AnswerElement vanillaAnswer = new RoutesAnswerer(question, vanilla).answer(vanillaSnapshot);
    AnswerElement s2Answer = new RoutesAnswerer(question, s2).answer(s2Snapshot);

    assertThat(rows(s2Answer), equalTo(rows(vanillaAnswer)));
  }

  private static List<String> rows(AnswerElement answer) {
    return ((TableAnswerElement) answer)
        .getRowsList().stream().map(Object::toString).sorted().collect(Collectors.toList());
  }
}
