#!/usr/bin/env bash
# Conservative CI entry point (ops O3).
#
# Three stages, cheapest first:
#
#   1. unit tests   bazel test //projects/s2:s2_tests            (default, fast)
#   2. upstream     the shared-code-affected Batfish suites plus the public-API
#                   allinone e2e + coordinator tests             (--upstream)
#   3. demo matrix  scripts/ci-matrix.sh                         (--matrix)
#
# The default run is stage 1 only: it is what a normal push/PR check should do.
# The heavier stages are opt-in because they are slow (stage 2 builds and runs
# large upstream suites; stage 3 launches many multi-JVM runs). The manual-only
# GitHub workflow (.github/workflows/s2-ci.yml) exposes all three stages.
#
# Usage:
#   scripts/ci.sh                          # stage 1 (default, fast)
#   scripts/ci.sh --upstream               # stages 1 + 2
#   scripts/ci.sh --upstream-only          # stage 2 only
#   scripts/ci.sh --matrix                 # stages 1 + 3
#   scripts/ci.sh --matrix-only            # stage 3 only
#   scripts/ci.sh --all                    # stages 1 + 2 + 3
#   scripts/ci.sh --tests-only             # force stage 1 only (default)
#   scripts/ci.sh --upstream --matrix --workers 3 --networks "s2-triangle s2-ospf"
#   scripts/ci.sh --list                   # print the plan, run nothing
#   scripts/ci.sh --help
#
# Options:
#   --upstream          also run the upstream regression stage
#   --upstream-only     run the upstream regression stage but skip the unit tests
#   --matrix            also run scripts/ci-matrix.sh (slow, opt-in)
#   --matrix-only       run the demo matrix but skip the unit tests
#   --all               run all three stages
#   --tests-only        force the unit-tests stage only (default)
#   --workers <n>       worker count for the matrix (default: 3)
#   --networks "<list>" matrix networks (ci-matrix default)
#   --modes "<list>"    matrix modes (ci-matrix default: "default full")
#   --test-args "<args>" extra arguments for `bazel test`
#   --list              print what would run, then exit
#   -h | --help         show this help
#
# Environment:
#   S2_CI_UPSTREAM=1    same as --upstream
#   S2_CI_MATRIX=1      same as --matrix
#   S2_CI_WORKERS       default worker count for the matrix
#   JAVA_TOOL_OPTIONS   forwarded to the unit tests, the upstream suites and every
#                       matrix run
set -uo pipefail
cd "$(git rev-parse --show-toplevel)"

RUN_TESTS=1
RUN_UPSTREAM=0
RUN_MATRIX=0
WORKERS="${S2_CI_WORKERS:-3}"
NETWORKS=""
MODES=""
TEST_ARGS=""
LIST=0

# Fixed shared-code-affected upstream suites. S2 patches shared packages (bdd,
# datamodel, dataplane/ibdp, reachability), so these are the suites that can
# regress from an S2 change even though their tests do not live in projects/s2.
UPSTREAM_SUITES=(
  //projects/batfish/src/test/java/org/batfish/dataplane:tests
  //projects/batfish/src/test/java/org/batfish/dataplane/ibdp:tests
  //projects/batfish/src/test/java/org/batfish/dataplane/traceroute:tests
  //projects/batfish/src/test/java/org/batfish/bddreachability:tests
  //projects/batfish/src/test/java/org/batfish/bddreachability/transition:tests
  //projects/common/src/test/java/org/batfish/datamodel:tests
)

# Public-API end-to-end + coordinator suites. Resolved with a bazel query so new
# e2e targets are picked up automatically; the lint targets (_pmd / :pmd) are
# dropped because they are not tests.
UPSTREAM_E2E_QUERY='tests(//projects/allinone/... + //projects/coordinator/...)'

usage() {
  sed -n '2,/^set -uo pipefail/p' "$0" | sed '$d'
}

# Print the upstream stage target list (one per line). Returns non-zero if the
# allinone/coordinator query produced nothing (so the stage fails loudly rather
# than silently skipping the e2e suites).
upstream_targets() {
  local e2e
  e2e="$(bazel query "$UPSTREAM_E2E_QUERY" 2>/dev/null | grep -v -E '(_pmd|:pmd)$' || true)"
  if [[ -z "$e2e" ]]; then
    return 1
  fi
  printf '%s\n' "${UPSTREAM_SUITES[@]}"
  printf '%s\n' "$e2e"
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --upstream) RUN_UPSTREAM=1 ;;
    --upstream-only) RUN_UPSTREAM=1; RUN_TESTS=0 ;;
    --matrix) RUN_MATRIX=1 ;;
    --matrix-only) RUN_MATRIX=1; RUN_TESTS=0 ;;
    --all) RUN_TESTS=1; RUN_UPSTREAM=1; RUN_MATRIX=1 ;;
    --tests-only) RUN_TESTS=1; RUN_UPSTREAM=0; RUN_MATRIX=0 ;;
    --workers) WORKERS="${2:?--workers needs a value}"; shift ;;
    --workers=*) WORKERS="${1#*=}" ;;
    --networks) NETWORKS="${2:?--networks needs a value}"; shift ;;
    --networks=*) NETWORKS="${1#*=}" ;;
    --modes) MODES="${2:?--modes needs a value}"; shift ;;
    --modes=*) MODES="${1#*=}" ;;
    --test-args) TEST_ARGS="${2:?--test-args needs a value}"; shift ;;
    --test-args=*) TEST_ARGS="${1#*=}" ;;
    --list) LIST=1 ;;
    -h|--help) usage; exit 0 ;;
    *) echo "unknown argument: $1" >&2; usage >&2; exit 2 ;;
  esac
  shift
done

if [[ "${S2_CI_UPSTREAM:-0}" == 1 ]]; then
  RUN_UPSTREAM=1
fi
if [[ "${S2_CI_MATRIX:-0}" == 1 ]]; then
  RUN_MATRIX=1
fi

echo "== S2 CI =="
if [[ "$RUN_TESTS" == 1 ]]; then
  echo "tests:    bazel test //projects/s2:s2_tests ${TEST_ARGS}"
else
  echo "tests:    (skipped)"
fi
if [[ "$RUN_UPSTREAM" == 1 ]]; then
  echo "upstream: bazel test <6 shared-code suites> + \"${UPSTREAM_E2E_QUERY}\" (minus *_pmd/:pmd) ${TEST_ARGS}"
else
  echo "upstream: (skipped; pass --upstream or set S2_CI_UPSTREAM=1)"
fi
if [[ "$RUN_MATRIX" == 1 ]]; then
  echo "matrix:   scripts/ci-matrix.sh --run --workers $WORKERS ${NETWORKS:+--networks \"$NETWORKS\" }${MODES:+--modes \"$MODES\" }"
else
  echo "matrix:   (skipped; pass --matrix or set S2_CI_MATRIX=1)"
fi

if [[ "$LIST" == 1 ]]; then
  exit 0
fi

status=0

if [[ "$RUN_TESTS" == 1 ]]; then
  echo
  echo "== unit tests: //projects/s2:s2_tests =="
  # shellcheck disable=SC2086 # TEST_ARGS is intentionally word-split
  if bazel test //projects/s2:s2_tests --test_output=errors $TEST_ARGS; then
    echo "== unit tests: PASS =="
  else
    echo "== unit tests: FAIL ==" >&2
    status=1
  fi
fi

if [[ "$RUN_UPSTREAM" == 1 ]]; then
  echo
  echo "== upstream regression: shared-code suites + allinone/coordinator e2e =="
  if ! upstream_list="$(upstream_targets)"; then
    echo "== upstream regression: FAIL (could not resolve allinone/coordinator tests via bazel query) ==" >&2
    status=1
  else
    printf '%s\n' "$upstream_list"
    # shellcheck disable=SC2086 # target list is intentionally word-split
    if bazel test --test_output=errors $TEST_ARGS $upstream_list; then
      echo "== upstream regression: PASS =="
    else
      echo "== upstream regression: FAIL ==" >&2
      status=1
    fi
  fi
fi

if [[ "$RUN_MATRIX" == 1 ]]; then
  echo
  echo "== demo matrix =="
  matrix_args=(--run --workers "$WORKERS")
  [[ -n "$NETWORKS" ]] && matrix_args+=(--networks "$NETWORKS")
  [[ -n "$MODES" ]] && matrix_args+=(--modes "$MODES")
  if scripts/ci-matrix.sh "${matrix_args[@]}"; then
    echo "== demo matrix: PASS =="
  else
    echo "== demo matrix: FAIL ==" >&2
    status=1
  fi
fi

if [[ "$status" == 0 ]]; then
  echo
  echo "== S2 CI: OK =="
else
  echo
  echo "== S2 CI: FAILED ==" >&2
fi
exit "$status"
