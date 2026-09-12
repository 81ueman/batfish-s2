#!/usr/bin/env bash
# Build the S2 distributed runner and (with --image) the container image.
set -euo pipefail

export JAVA_HOME="${JAVA_HOME:-/opt/homebrew/opt/openjdk@25}"
export PATH="$JAVA_HOME/bin:$PATH"

cd "$(git rev-parse --show-toplevel)"

bazel build //projects/s2:s2_main_deploy.jar

if [[ "${1:-}" == "--image" ]]; then
  # Stage the jar inside the Docker build context (Bazel's bazel-bin symlink
  # points outside the repo and cannot be COPYed).
  cp -f bazel-bin/projects/s2/s2_main_deploy.jar docker/s2_main_deploy.jar
  docker build -f docker/Dockerfile.s2 -t s2:local .
  echo "built image s2:local"
fi
