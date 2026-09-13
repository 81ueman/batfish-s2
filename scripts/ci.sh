#!/usr/bin/env bash
# Conservative CI entry point (ops O3).
#
# By default this only runs the S2 unit tests, which is what a normal push/PR
# check should do:
#
#   bazel test //projects/s2:s2_tests
#
# The full local demo matrix (scripts/ci-matrix.sh) launches many multi-JVM runs
# and is slow, so it is NOT run unless it is explicitly enabled with --matrix (or
# S2_CI_MATRIX=1). The manual-only GitHub workflow
# (.github/workflows/s2-ci.yml) is the intended place to turn it on.
#
# Usage:
#   scripts/ci.sh                       # unit tests only (default, conservative)
#   scripts/ci.sh --matrix              # unit tests, then the demo matrix
#   scripts/ci.sh --matrix --matrix-only
#   scripts/ci.sh --matrix --workers 3 --networks "s2-triangle s2-ospf"
#   scripts/ci.sh --list                # print the plan, run nothing
#   scripts/ci.sh --help
#
# Options:
#   --matrix            also run scripts/ci-matrix.sh (slow, opt-in)
#   --matrix-only       run the demo matrix but skip the unit tests
#   --tests-only        force unit-tests only (default)
#   --workers <n>       worker count for the matrix (default: 3)
#   --networks "<list>" matrix networks (ci-matrix default)
#   --modes "<list>"    matrix modes (ci-matrix default: "default owned")
#   --test-args "<args>" extra arguments for `bazel test`
#   --list              print what would run, then exit
#   -h | --help         show this help
#
# Environment:
#   S2_CI_MATRIX=1      same as --matrix
#   S2_CI_WORKERS       default worker count for the matrix
#   JAVA_TOOL_OPTIONS   forwarded to the unit tests and every matrix run
set -uo pipefail
cd "$(git rev-parse --show-toplevel)"

RUN_TESTS=1
RUN_MATRIX=0
WORKERS="${S2_CI_WORKERS:-3}"
NETWORKS=""
MODES=""
TEST_ARGS=""
LIST=0

usage() {
  sed -n '2,36p' "$0"
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --matrix) RUN_MATRIX=1 ;;
    --matrix-only) RUN_MATRIX=1; RUN_TESTS=0 ;;
    --tests-only) RUN_TESTS=1; RUN_MATRIX=0 ;;
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

if [[ "${S2_CI_MATRIX:-0}" == 1 ]]; then
  RUN_MATRIX=1
fi

echo "== S2 CI =="
if [[ "$RUN_TESTS" == 1 ]]; then
  echo "tests:  bazel test //projects/s2:s2_tests ${TEST_ARGS}"
else
  echo "tests:  (skipped)"
fi
if [[ "$RUN_MATRIX" == 1 ]]; then
  echo "matrix: scripts/ci-matrix.sh --run --workers $WORKERS ${NETWORKS:+--networks \"$NETWORKS\" }${MODES:+--modes \"$MODES\" }"
else
  echo "matrix: (skipped; pass --matrix or set S2_CI_MATRIX=1 to run the demo matrix)"
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
