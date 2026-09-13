// SPDX-License-Identifier: Apache-2.0

package org.batfish.dataplane.ibdp;

import com.google.common.annotations.VisibleForTesting;
import java.util.List;
import java.util.Set;
import org.batfish.datamodel.Prefix;

/**
 * Deterministic auto-selection of the number of control-plane prefix shards ({@code
 * S2_PREFIX_SHARDS=auto}, alias {@code -Ds2.prefixShardCount=auto}).
 *
 * <p>Prefix sharding bounds the live BGP RIB by running the EGP fixpoint once per shard, so only
 * one shard's routes are materialized at a time (with {@code -Ds2.prefixShardExternalize=true} the
 * previous shard's routes are serialized out and dropped). The right shard count trades the live
 * RIB (which shrinks like 1/N) against a per-round cost (which grows with N); on the measured
 * testbeds the peak is minimized around 7-8 shards at ~640 prefixes and at the 16-shard cap at
 * ~4096 prefixes, and regresses beyond that ({@code scripts/shard-sweep.sh}, {@code
 * docs/s2-port/M5-SCALE.md}).
 *
 * <p>This class picks N from the {@link PrefixDependencyGraph} only, which is built from the
 * snapshot on every worker, so the choice is identical on every worker for a given snapshot (it
 * does not depend on the owned/shadow partition). The policy is:
 *
 * <ol>
 *   <li>Estimate the total route/memory weight of the sharding universe as the sum of the DPDG
 *       component weights ({@link PrefixDependencyGraph#componentWeight}); this is the total live
 *       BGP RIB contribution when nothing is sharded. A prefix's weight is a base unit plus one per
 *       router that sources it (see {@link PrefixDependencyGraph#computeWeights}), so it is a
 *       route-count proxy rather than a byte count.
 *   <li>Target a per-shard weight of {@code budgetMiB / MIB_PER_WEIGHT_UNIT}, where the budget
 *       comes from {@code -Ds2.prefixShardBudgetMiB} (env {@code S2_PREFIX_SHARD_BUDGET_MIB}) and
 *       defaults to {@link #DEFAULT_BUDGET_MIB}.
 *   <li>{@code N = ceil(totalWeight / targetWeight)}, capped by the number of components (a
 *       component cannot be split) and by {@link #MAX_AUTO_SHARDS} (past which measured peak
 *       regresses), and floored at 1 (sharding disabled).
 * </ol>
 *
 * <p>The defaults were fit to the sweep: with {@link #MIB_PER_WEIGHT_UNIT} = 1 MiB per weight unit
 * and a {@link #DEFAULT_BUDGET_MIB} = 192 MiB budget, the target is 192 weight per shard, which
 * selects N=7 on {@code s2-big2} (total weight 1307) and N=16 (the cap) on {@code s2-mega} (total
 * weight 8237) — both at the measured peak optimum. The ratio is a calibration constant; a larger
 * budget asks for fewer, larger shards.
 */
final class PrefixShardCountSelector {

  /** The sentinel that requests auto-selection. */
  static final String AUTO = "auto";

  /** System property for the per-shard live-BGP-RIB budget, in MiB. */
  static final String BUDGET_MIB_PROPERTY = "s2.prefixShardBudgetMiB";

  /** Environment variable equivalent of {@link #BUDGET_MIB_PROPERTY}. */
  static final String BUDGET_MIB_ENV = "S2_PREFIX_SHARD_BUDGET_MIB";

  /** Default per-shard live-BGP-RIB budget in MiB. */
  static final int DEFAULT_BUDGET_MIB = 192;

  /**
   * MiB of live BGP RIB assumed per DPDG weight unit ({@link
   * PrefixDependencyGraph#componentWeight}). A weight unit is a route-count proxy (a prefix
   * contributes a base unit plus one per sourcing router), so this is a calibration constant rather
   * than a physical byte count. With the default it makes the {@link #DEFAULT_BUDGET_MIB} budget
   * target 192 weight per shard.
   */
  static final double MIB_PER_WEIGHT_UNIT = 1.0;

  /**
   * Hard cap on the auto-selected shard count. The measured peak is minimized at N=7-8 on {@code
   * s2-big2} (~640 prefixes) and at N=16 on {@code s2-mega} (~4096 prefixes); N=32 regressed on
   * both (665 MiB vs 449 at 8 on big2, 828 MiB vs 744 at 16 on mega), so the sweep does not justify
   * going beyond 16.
   */
  static final int MAX_AUTO_SHARDS = 16;

  private PrefixShardCountSelector() {}

  /** Whether {@code spec} (a raw shard-count setting) requests auto-selection. */
  static boolean isAuto(String spec) {
    return AUTO.equalsIgnoreCase(spec.trim());
  }

  /**
   * Selects the shard count for {@code graph} using the configured budget, and logs the decision.
   * Deterministic for a fixed snapshot.
   */
  static int select(PrefixDependencyGraph graph) {
    List<Set<Prefix>> components = graph.weaklyConnectedComponents();
    long totalWeight = 0;
    for (Set<Prefix> component : components) {
      totalWeight += graph.componentWeight(component);
    }
    long budgetMiB = budgetMiB();
    int n = select(totalWeight, components.size(), budgetMiB);
    System.err.printf(
        "S2 prefix sharding: auto selected N=%d (components=%d, totalWeight=%d, targetWeight=%d,"
            + " budget=%d MiB)%n",
        n,
        components.size(),
        totalWeight,
        Math.max(1, Math.round(budgetMiB / MIB_PER_WEIGHT_UNIT)),
        budgetMiB);
    return n;
  }

  /**
   * Pure selection policy: the smallest shard count that keeps the balanced per-shard weight under
   * the budget, bounded by the number of indivisible components and {@link #MAX_AUTO_SHARDS}.
   */
  @VisibleForTesting
  static int select(long totalWeight, int componentCount, long budgetMiB) {
    if (totalWeight <= 0 || componentCount <= 0) {
      return 1;
    }
    long targetWeight = Math.max(1, Math.round(budgetMiB / MIB_PER_WEIGHT_UNIT));
    long byBudget = (totalWeight + targetWeight - 1) / targetWeight;
    long capped = Math.min(byBudget, Math.min(componentCount, MAX_AUTO_SHARDS));
    return (int) Math.max(1, capped);
  }

  /** The configured per-shard budget in MiB (system property over env over default). */
  @VisibleForTesting
  static long budgetMiB() {
    String raw = System.getProperty(BUDGET_MIB_PROPERTY);
    if (raw == null) {
      raw = System.getenv(BUDGET_MIB_ENV);
    }
    if (raw != null) {
      try {
        long parsed = Long.parseLong(raw.trim());
        if (parsed > 0) {
          return parsed;
        }
      } catch (NumberFormatException e) {
        // Fall through to the default below.
      }
      System.err.printf(
          "S2 prefix sharding: ignoring invalid %s=%s; using default %d MiB%n",
          BUDGET_MIB_PROPERTY, raw, DEFAULT_BUDGET_MIB);
    }
    return DEFAULT_BUDGET_MIB;
  }
}
