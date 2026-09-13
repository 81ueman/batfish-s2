// SPDX-License-Identifier: Apache-2.0

package org.batfish.dataplane.ibdp;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

import org.junit.Test;

/** Unit tests for the deterministic auto shard-count policy ({@link PrefixShardCountSelector}). */
public class PrefixShardCountSelectorTest {

  @Test
  public void testIsAuto() {
    assertThat(PrefixShardCountSelector.isAuto("auto"), is(true));
    assertThat(PrefixShardCountSelector.isAuto("AUTO"), is(true));
    assertThat(PrefixShardCountSelector.isAuto(" auto "), is(true));
    assertThat(PrefixShardCountSelector.isAuto("8"), is(false));
    assertThat(PrefixShardCountSelector.isAuto("automatic"), is(false));
  }

  /** A network under the per-shard budget is not sharded. */
  @Test
  public void testUnderBudgetIsSingleShard() {
    assertThat(PrefixShardCountSelector.select(14, 6, 64), is(1));
    assertThat(PrefixShardCountSelector.select(64, 649, 64), is(1));
  }

  /**
   * Just over the budget needs two shards; the target is the budget (1 MiB per weight unit), so the
   * count is {@code ceil(totalWeight / budget)}.
   */
  @Test
  public void testScalesWithTotalWeight() {
    assertThat(PrefixShardCountSelector.select(65, 100, 64), is(2));
    assertThat(PrefixShardCountSelector.select(658, 1_000, 64), is(11));
  }

  /** A component is indivisible, so the count never exceeds the number of components. */
  @Test
  public void testComponentCountCapsShards() {
    assertThat(PrefixShardCountSelector.select(1_000, 3, 64), is(3));
    assertThat(PrefixShardCountSelector.select(1_000, 1, 64), is(1));
  }

  /** The measured peak regresses past {@link PrefixShardCountSelector#MAX_AUTO_SHARDS}. */
  @Test
  public void testMaxAutoShardsCapsShards() {
    assertThat(
        PrefixShardCountSelector.select(1_000_000, 100_000, 64),
        is(PrefixShardCountSelector.MAX_AUTO_SHARDS));
  }

  /** A smaller budget asks for more shards (bounded by the same caps). */
  @Test
  public void testBudgetOverride() {
    assertThat(PrefixShardCountSelector.select(658, 1_000, 16), is(16));
    assertThat(PrefixShardCountSelector.select(658, 1_000, 32), is(16));
    assertThat(PrefixShardCountSelector.select(658, 1_000, 128), is(6));
  }

  @Test
  public void testDegenerateInputs() {
    assertThat(PrefixShardCountSelector.select(0, 0, 64), is(1));
    assertThat(PrefixShardCountSelector.select(0, 5, 64), is(1));
    assertThat(PrefixShardCountSelector.select(5, 0, 64), is(1));
  }

  @Test
  public void testBudgetDefaultAndOverride() {
    System.clearProperty(PrefixShardCountSelector.BUDGET_MIB_PROPERTY);
    assertThat(
        PrefixShardCountSelector.budgetMiB(),
        is((long) PrefixShardCountSelector.DEFAULT_BUDGET_MIB));
    System.setProperty(PrefixShardCountSelector.BUDGET_MIB_PROPERTY, "16");
    try {
      assertThat(PrefixShardCountSelector.budgetMiB(), is(16L));
    } finally {
      System.clearProperty(PrefixShardCountSelector.BUDGET_MIB_PROPERTY);
    }
  }

  /** An unparseable or non-positive budget is ignored in favor of the default. */
  @Test
  public void testInvalidBudgetFallsBackToDefault() {
    for (String invalid : new String[] {"abc", "0", "-4", ""}) {
      System.setProperty(PrefixShardCountSelector.BUDGET_MIB_PROPERTY, invalid);
      try {
        assertThat(
            PrefixShardCountSelector.budgetMiB(),
            is((long) PrefixShardCountSelector.DEFAULT_BUDGET_MIB));
      } finally {
        System.clearProperty(PrefixShardCountSelector.BUDGET_MIB_PROPERTY);
      }
    }
  }
}
