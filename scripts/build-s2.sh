#!/usr/bin/env bash
# Build the S2 distributed runner and (optionally) the container image.
set -euo pipefail

export JAVA_HOME="${JAVA_HOME:-/opt/homebrew/opt/openjdk@25}"
export PATH="$JAVA_HOME/bin:$PATH"

cd "$(git rev-parse --show-toplevel)"

bazel build //projects/distributed:distributed_runner_deploy.jar

if [[ "${1:-}" == "--image" ]]; then
  docker build -f docker/Dockerfile.s2 -t s2:local .
  echo "built image s2:local"
fi
