// SPDX-License-Identifier: Apache-2.0
package org.batfish.dataplane.ibdp;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

import com.google.common.collect.ImmutableList;
import java.util.List;
import java.util.Map;
import org.batfish.datamodel.Configuration;
import org.batfish.datamodel.Prefix;
import org.batfish.datamodel.PrefixSpace;
import org.batfish.main.Batfish;
import org.batfish.main.BatfishTestUtils;
import org.batfish.main.TestrigText;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

/** Unit tests for the prefix-sharding universe and dependency grouping ({@link PrefixSharder}). */
public class PrefixSharderTest {

  private static final String TRIANGLE = "org/batfish/dataplane/testrigs/s2-triangle";
  private static final List<String> TRIANGLE_CONFIGS = ImmutableList.of("r1", "r2", "r3");
  private static final String AGG = "org/batfish/dataplane/testrigs/s2-agg";
  private static final List<String> AGG_CONFIGS = ImmutableList.of("r1", "r2", "r3");

  @Rule public TemporaryFolder _folder = new TemporaryFolder();

  private Map<String, Configuration> load(String testrig, List<String> configs) throws Exception {
    Batfish batfish =
        BatfishTestUtils.getBatfishFromTestrigText(
            TestrigText.builder().setConfigurationFiles(testrig, configs).build(), _folder);
    return batfish.loadConfigurations(batfish.getSnapshot());
  }

  /** An extra prefix (e.g. an external BGP announcement) must land in some shard. */
  @Test
  public void testExtraPrefixesIncluded() throws Exception {
    Map<String, Configuration> configs = load(TRIANGLE, TRIANGLE_CONFIGS);
    Prefix external = Prefix.parse("203.0.113.0/24");
    List<PrefixSpace> shards = PrefixSharder.shards(configs, ImmutableList.of(external), 3);
    assertThat(
        "external prefix must be appointed in some shard",
        shards.stream().anyMatch(s -> s.containsPrefix(external)),
        is(true));
  }

  /** An aggregate and a prefix it covers must be assigned to the same shard. */
  @Test
  public void testAggregateAndCoveredPrefixShareShard() throws Exception {
    Map<String, Configuration> configs = load(AGG, AGG_CONFIGS);
    Prefix aggregate = Prefix.parse("2.128.0.0/16");
    Prefix moreSpecific = Prefix.parse("2.128.1.0/24");
    List<PrefixSpace> shards = PrefixSharder.shards(configs, ImmutableList.of(), 3);
    assertThat(
        "aggregate and its more-specific must be co-sharded",
        shards.stream()
            .anyMatch(s -> s.containsPrefix(aggregate) && s.containsPrefix(moreSpecific)),
        is(true));
  }
}
