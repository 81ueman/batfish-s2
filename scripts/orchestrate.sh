#!/usr/bin/env bash
# Orchestrates an S2 run from inside the cluster.
#
# S2Runner listens on a TCP port and accepts a single text command per
# connection. Workers are started first, then the controller drives the run.
#
# Environment:
#   WORKERS         number of worker pods (1 or 3)
#   NETWORK_NAME    snapshot name under /s2/inputs (default bgp-resolution-loop)
#   SHARDS          number of prefix shards (default 1)
#   PARTITION       random | expert | metis (default random)
#   REACHABILITY    -1 for all-pairs, N for sampled pairs (default -1)
set -euo pipefail

WORKERS="${WORKERS:-1}"
NETWORK_NAME="${NETWORK_NAME:-bgp-resolution-loop}"
SHARDS="${SHARDS:-1}"
PARTITION="${PARTITION:-random}"
REACHABILITY="${REACHABILITY:--1}"
NS="${NAMESPACE:-s2}"

CTRL_HOST="s2-controller.${NS}.svc.cluster.local"
CTRL_PORT=4090

# Send one command and print the first line of the reply (bounded wait).
send() {
  local host="$1" port="$2" msg="$3"
  local out
  out="$( { exec 3<>"/dev/tcp/${host}/${port}"; printf '%s' "$msg" >&3; timeout 10 cat <&3; } 2>/dev/null || true )"
  echo "${out}"
}

# Wait until a pod DNS name:port accepts a connection.
wait_port() {
  local host="$1" port="$2"
  for _ in $(seq 1 120); do
    if (exec 3<>"/dev/tcp/${host}/${port}") 2>/dev/null; then
      exec 3>&- 3<&- || true
      return 0
    fi
    sleep 1
  done
  echo "timeout waiting for ${host}:${port}" >&2
  return 1
}

worker_host() { echo "s2-worker-$1.s2-worker.${NS}.svc.cluster.local"; }

echo "waiting for controller ${CTRL_HOST}:${CTRL_PORT}"
wait_port "${CTRL_HOST}" "${CTRL_PORT}"

echo "starting ${WORKERS} worker(s)"
for i in $(seq 0 $((WORKERS - 1))); do
  w="$(worker_host "$i")"
  wait_port "$w" 4091
  # router <controller-ip> <worker-ip> <worker-id> shard <n>
  echo "  worker $i => $(send "$w" 4091 "router ${CTRL_HOST} ${w} 0 shard ${SHARDS}")"
done

workers_args=""
for i in $(seq 0 $((WORKERS - 1))); do
  workers_args="${workers_args} $(worker_host "$i") 0"
done

echo "starting controller"
# controller <network> partition-scheme <scheme> shard <n> reachability <n> <worker-ip> <id> ...
cmd="controller ${NETWORK_NAME} partition-scheme ${PARTITION} shard ${SHARDS} reachability ${REACHABILITY} ${workers_args}"
echo "  ${cmd}"
reply="$(send "${CTRL_HOST}" "${CTRL_PORT}" "${cmd}")"
echo "controller runtime: ${reply}"

# S2Runner accepts a second connection to shut down and report peak memory.
send "${CTRL_HOST}" "${CTRL_PORT}" "stop" || true
for i in $(seq 0 $((WORKERS - 1))); do
  send "$(worker_host "$i")" 4091 "stop" || true
done

echo "S2 run complete (workers=${WORKERS}, network=${NETWORK_NAME})"
