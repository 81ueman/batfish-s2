#!/usr/bin/env bash
# Run Batfish (allinone) with the S2 dataplane engine, so the distributed engine is used exactly
# like the stock one: same snapshot input, same question engine, same REST/pybatfish API.
#
# Usage:
#   scripts/s2-batfish.sh [allinone args...]
#   S2_WORKERS=4 scripts/s2-batfish.sh -snapshotdir /path/to/snapshots
#
# The engine is selected with -dataplaneengine=s2 (passed to the Batfish worker as -batfishargs);
# S2_WORKERS maps to -s2workers (0/unset = auto). Anything else is forwarded to allinone.
set -euo pipefail
cd "$(git rev-parse --show-toplevel)"

BATFISH_ARGS="-dataplaneengine s2"
if [[ -n "${S2_WORKERS:-}" ]]; then
  BATFISH_ARGS="${BATFISH_ARGS} -s2workers ${S2_WORKERS}"
fi

exec bazel run //projects/allinone:allinone_main -- -batfishargs "$BATFISH_ARGS" "$@"
