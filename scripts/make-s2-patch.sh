#!/usr/bin/env bash
# Reconstruct the exact core patch between the S2 fork and its Batfish base.
# Usage: scripts/make-s2-patch.sh [out.patch]
set -euo pipefail

S2_REF="${S2_REF:-$HOME/ghq/github.com/81ueman/s2-reference}"
BASE="0ee91628f04be103f500c404fcbb6bf64fd63335"
OUT="${1:-/tmp/s2-core.patch}"

cd "$S2_REF"
git remote add batfish https://github.com/batfish/batfish.git 2>/dev/null || true
git fetch --depth 1 batfish "$BASE"

# Batfish moved projects/batfish-common-protocol -> projects/common; rewrite
# paths so the patch applies to current upstream. Skip the vendor-only
# AddCommunity/OverwriteAsPath feature which the minimal core does not need.
git diff "$BASE" HEAD -- \
  projects/batfish-common-protocol \
  projects/batfish/src \
  projects/bdd \
  projects/question \
  projects/symbolic \
  ':!projects/batfish-common-protocol/src/main/java/org/batfish/datamodel/routing_policy/statement/AddCommunity.java' \
  ':!projects/batfish-common-protocol/src/main/java/org/batfish/datamodel/routing_policy/statement/OverwriteAsPath.java' \
  ':!projects/batfish-common-protocol/src/main/java/org/batfish/datamodel/routing_policy/statement/StatementVisitor.java' \
  ':!projects/batfish-common-protocol/src/main/java/org/batfish/datamodel/bgp/community/CommunityStructuresVerifier.java' \
  ':!projects/batfish-common-protocol/src/main/java/org/batfish/datamodel/routing_policy/as_path/AsPathStructuresVerifier.java' \
  ':!projects/question/src/main/java/org/batfish/question/TracingHintsStripper.java' \
  ':!projects/minesweeper' \
  | sed 's#projects/batfish-common-protocol/#projects/common/#g' > "$OUT"

echo "wrote $OUT"
