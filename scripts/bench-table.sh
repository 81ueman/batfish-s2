#!/usr/bin/env bash
# Run a size ladder x mode(s) through scripts/bench.sh and emit the markdown
# metrics table used in docs/s2-port/M5-SCALE.md
# (network | prefixes | mode | max peak MiB | controller MiB | engine s (w0) | wall s).
#
# It is aimed at regenerating the scale table incrementally: every measured cell
# (network, workers, mode, shards) is cached, and a re-run skips cells it has
# already measured. Cached cells whose result was not MATCH are retried unless
# --keep-failures is given (a DIFF/NO_RESULT is not a usable measurement).
#
# Usage:
#   scripts/bench-table.sh --list
#   scripts/bench-table.sh --workers 3 --modes "default full"
#   scripts/bench-table.sh --ladder "s2-big2:640 s2-mega:4096 s2-giga:32768"
#   scripts/bench-table.sh --shards "1 8" --modes "default full"
#   JAVA_TOOL_OPTIONS=-Xmx4g scripts/bench-table.sh --workers 3
#
# Options:
#   --workers "<list>"   worker counts to run (default: 3)
#   --ladder "<list>"    size ladder; each item is <network>[:<prefixes>]
#                        (default: s2-big-bgp s2-big2 s2-huge s2-mega s2-giga)
#   --networks "<list>"  alias for --ladder
#   --modes "<list>"     mode labels to run (default: "default full"). A token
#                        may be a built-in name (default, full, no-ship, owned,
#                        owned+descriptor) or "label=<JAVA_TOOL_OPTIONS>".
#                        "default" is O1's owned+descriptor mode; "full" disables
#                        both to reproduce the pre-O1 full dataplane/configs.
#                        "owned"/"owned+descriptor" are kept as explicit aliases.
#   --shards "<list>"    S2_PREFIX_SHARDS values to sweep (default: $S2_PREFIX_SHARDS or 1)
#   --cache <file>       cache file (default: results/bench-table.cache.tsv)
#   --out <file>         also write the markdown table to <file>
#   --logdir <dir>       where to keep the raw bench.sh logs (default: results/bench-table-logs)
#   --list               print the plan (cells + cache state) and exit; run nothing
#   --force              ignore the cache and re-measure every cell
#   --keep-failures      also keep cached cells whose result was not MATCH
#   -h | --help          show this help
#
# The existing JAVA_TOOL_OPTIONS is kept and the mode's options are appended;
# S2_PREFIX_SHARDS is set per cell. Both are forwarded to bench.sh / local-demo.sh,
# so the table is measured exactly like the commands in M5-SCALE.md.
#
# Sample output (a 2-cell run, `S2_BASE_PORT=19000 scripts/bench-table.sh \
#   --ladder "s2-triangle s2-line" --modes "default" --workers 3`,
# results are host- and JVM-specific):
#
#   | network | prefixes | mode | max peak MiB | controller MiB | engine s (w0) | wall s |
#   | --- | --- | --- | --- | --- | --- | --- |
#   | s2-triangle | 3 | default | 141.4 | 248.0 | 3.3 | 8 |
#   | s2-line | 6 | default | 140.0 | 266.3 | 3.3 | 8 |
set -uo pipefail
cd "$(git rev-parse --show-toplevel)"

WORKERS_LIST="3"
LADDER="s2-big-bgp s2-big2 s2-huge s2-mega s2-giga"
MODES="default full"
SHARDS="${S2_PREFIX_SHARDS:-1}"
CACHE="results/bench-table.cache.tsv"
OUT=""
LOGDIR="results/bench-table-logs"
LIST=0
FORCE=0
KEEP_FAILURES=0

usage() {
  sed -n '2,46p' "$0"
}

trim() {
  local s="$1"
  s="${s#"${s%%[![:space:]]*}"}"
  echo "${s%"${s##*[![:space:]]}"}"
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --workers) WORKERS_LIST="${2:?--workers needs a value}"; shift ;;
    --workers=*) WORKERS_LIST="${1#*=}" ;;
    --ladder|--networks) LADDER="${2:?$1 needs a value}"; shift ;;
    --ladder=*|--networks=*) LADDER="${1#*=}" ;;
    --modes) MODES="${2:?--modes needs a value}"; shift ;;
    --modes=*) MODES="${1#*=}" ;;
    --shards) SHARDS="${2:?--shards needs a value}"; shift ;;
    --shards=*) SHARDS="${1#*=}" ;;
    --cache) CACHE="${2:?--cache needs a value}"; shift ;;
    --cache=*) CACHE="${1#*=}" ;;
    --out) OUT="${2:?--out needs a value}"; shift ;;
    --out=*) OUT="${1#*=}" ;;
    --logdir) LOGDIR="${2:?--logdir needs a value}"; shift ;;
    --logdir=*) LOGDIR="${1#*=}" ;;
    --list) LIST=1 ;;
    --force) FORCE=1 ;;
    --keep-failures) KEEP_FAILURES=1 ;;
    -h|--help) usage; exit 0 ;;
    *) echo "unknown argument: $1" >&2; usage >&2; exit 2 ;;
  esac
  shift
done

BASE_JAVA_TOOL_OPTIONS="${JAVA_TOOL_OPTIONS:-}"

# Built-in modes: label -> extra JAVA_TOOL_OPTIONS.
builtin_mode_opts() {
  case "$1" in
    default)          echo "" ;;
    # Pre-O1 behavior: full per-worker dataplane + full remote configs.
    full)             echo "-Ds2.ownedDataplane=false -Ds2.descriptorShadows=false" ;;
    no-ship)          echo "-Ds2.noShipConfigs=true" ;;
    # Explicit aliases for the (now default) owned / owned+descriptor modes.
    owned)            echo "-Ds2.ownedDataplane=true" ;;
    owned+descriptor) echo "-Ds2.ownedDataplane=true -Ds2.descriptorShadows=true" ;;
    *) return 1 ;;
  esac
}

# Parse a mode token into MODE_OPTS; supports "label" (built-in) and "label=opts".
mode_opts_for() {
  local tok="$1"
  if [[ "$tok" == *"="* ]]; then
    echo "${tok#*=}"
    return 0
  fi
  builtin_mode_opts "$tok"
}

# Number of origination prefixes for a network: <net>:<N> overrides; otherwise
# count the loopback interfaces in the snapshot's configs. "?" if unknown.
prefixes_for() {
  local net="$1"
  if [[ "$net" == *:* ]]; then
    echo "${net##*:}"
    return 0
  fi
  local dir="networks/$net/configs"
  if [[ ! -d "$dir" ]]; then
    echo "?"
    return 0
  fi
  grep -rhoiE '^[[:space:]]*interface[[:space:]]+Loopback' "$dir" 2>/dev/null | wc -l | tr -d ' '
}

# net (without the optional :prefixes suffix).
net_name() {
  echo "${1%%:*}"
}

cache_lookup() {
  local key="$1"
  [[ "$FORCE" == 1 ]] && return 1
  [[ -f "$CACHE" ]] || return 1
  local line
  line="$(grep -F -- "$key"$'\t' "$CACHE" 2>/dev/null | tail -1)"
  [[ -n "$line" ]] || return 1
  IFS=$'\t' read -r _ C_PREFIXES C_WORKERS C_MODE C_SHARDS C_PEAK C_CTRL C_ENGINE C_WALL C_RESULT C_OPTS <<<"$line"
  [[ "$C_RESULT" == "MATCH" ]] && return 0
  # A non-MATCH cache entry is not a usable measurement; retry it unless asked not to.
  [[ "$KEEP_FAILURES" == 1 ]] && return 0
  return 1
}

cache_store() {
  local key="$1" pfx="$2" w="$3" mode="$4" shards="$5" peak="$6" ctrl="$7" engine="$8" wall="$9" result="${10}" opts="${11}"
  mkdir -p "$(dirname "$CACHE")"
  printf '%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\n' \
    "$key" "$pfx" "$w" "$mode" "$shards" "$peak" "$ctrl" "$engine" "$wall" "$result" "$opts" >>"$CACHE"
}

if [[ ! -f bazel-bin/projects/s2/s2_main_deploy.jar ]]; then
  echo "# building s2_main_deploy.jar (this can take a while)" >&2
  bazel build //projects/s2:s2_main_deploy.jar >/dev/null 2>&1
fi

mkdir -p "$LOGDIR"

# --- plan ------------------------------------------------------------------
# Cells are expanded in ladder x workers x mode x shards order so the table reads
# as a size ladder, with all modes for a row kept adjacent.
plan=()
plan_net=()
plan_pfx=()
plan_w=()
plan_mode=()
plan_label=()
plan_shards=()
plan_opts=()
plan_cached=()
plan_result=()
to_run=0
cached=0
for entry in $LADDER; do
  net="$(net_name "$entry")"
  pfx="$(prefixes_for "$entry")"
  for w in $WORKERS_LIST; do
    for tok in $MODES; do
      if ! opts="$(mode_opts_for "$tok")"; then
        echo "unknown mode '$tok' (use default/owned/no-ship/owned+descriptor or label=<opts>)" >&2
        exit 2
      fi
      label="${tok%%=*}"
      for n in $SHARDS; do
        modelabel="$label"
        [[ "$n" != "1" ]] && modelabel="${label}+B${n}"
        key="${net}|${w}|${modelabel}|${n}"
        plan+=("$key")
        plan_net+=("$net")
        plan_pfx+=("$pfx")
        plan_w+=("$w")
        plan_mode+=("$modelabel")
        plan_label+=("$label")
        plan_shards+=("$n")
        plan_opts+=("$opts")
        if cache_lookup "$key"; then
          plan_cached+=("1")
          plan_result+=("$C_RESULT")
          cached=$((cached + 1))
        else
          plan_cached+=("0")
          plan_result+=("-")
          to_run=$((to_run + 1))
        fi
      done
    done
  done
done

echo "== bench-table plan ==" >&2
echo "ladder:  $LADDER" >&2
echo "workers: $WORKERS_LIST" >&2
echo "modes:   $MODES" >&2
echo "shards:  $SHARDS" >&2
echo "cache:   $CACHE ($cached cached, $to_run to run)" >&2
for i in "${!plan[@]}"; do
  if [[ "${plan_cached[$i]}" == 1 ]]; then
    state="CACHED (${plan_result[$i]})"
  else
    state="RUN"
  fi
  printf '  %-18s %-20s w=%s shards=%s  prefixes=%s  %s\n' \
    "$state" "${plan_mode[$i]}" "${plan_w[$i]}" "${plan_shards[$i]}" "${plan_pfx[$i]}" "${plan_net[$i]}" >&2
done

if [[ "$LIST" == 1 ]]; then
  exit 0
fi

# --- run + collect ---------------------------------------------------------
rows=()
for i in "${!plan[@]}"; do
  net="${plan_net[$i]}"
  pfx="${plan_pfx[$i]}"
  w="${plan_w[$i]}"
  modelabel="${plan_mode[$i]}"
  label="${plan_label[$i]}"
  shards="${plan_shards[$i]}"
  opts="${plan_opts[$i]}"
  key="${plan[$i]}"

  if cache_lookup "$key"; then
    peak="$C_PEAK"; ctrl="$C_CTRL"; engine="$C_ENGINE"; wall="$C_WALL"; result="$C_RESULT"
  else
    combined="$BASE_JAVA_TOOL_OPTIONS"
    [[ -n "$opts" ]] && combined="$BASE_JAVA_TOOL_OPTIONS $opts"
    log="$LOGDIR/bench-${net}-w${w}-${modelabel}.log"
    printf '== running %s w=%s shards=%s -> %s\n' "$net" "$w" "$shards" "$log" >&2
    JAVA_TOOL_OPTIONS="$combined" S2_PREFIX_SHARDS="$shards" \
      scripts/bench.sh "$w" "$net" >"$log" 2>&1 || true
    row="$(grep -E "^\| ${net} \| ${w} \|" "$log" | tail -1)"
    if [[ -z "$row" ]]; then
      peak="-"; ctrl="-"; engine="-"; wall="-"; result="NO_RESULT"
      echo "  !! no result row for ${net} w=${w}; see $log" >&2
    else
      # | network | workers | result | max peak MiB | controller MiB | engine s (w0) | wall s |
      IFS='|' read -r _ _ _ result peak ctrl engine wall _ <<<"$row"
      result="$(trim "$result")"; peak="$(trim "$peak")"; ctrl="$(trim "$ctrl")"
      engine="$(trim "$engine")"; wall="$(trim "$wall")"
      echo "  -> result=$result peak=${peak}MiB controller=${ctrl}MiB engine=${engine}s wall=${wall}s" >&2
    fi
    cache_store "$key" "$pfx" "$w" "$modelabel" "$shards" "$peak" "$ctrl" "$engine" "$wall" "$result" "$combined"
    [[ "$result" != "MATCH" ]] && echo "  !! ${net} w=${w} ${modelabel}: result=${result} (not MATCH)" >&2
  fi
  rows+=("$(printf '| %s | %s | %s | %s | %s | %s | %s |' \
    "$net" "$pfx" "$modelabel" "$peak" "$ctrl" "$engine" "$wall")")
done

# --- emit ------------------------------------------------------------------
emit_table() {
  printf '| network | prefixes | mode | max peak MiB | controller MiB | engine s (w0) | wall s |\n'
  printf '| --- | --- | --- | --- | --- | --- | --- |\n'
  local r
  for r in "${rows[@]}"; do printf '%s\n' "$r"; done
}

if [[ -n "$OUT" ]]; then
  mkdir -p "$(dirname "$OUT")"
  emit_table | tee "$OUT"
  echo "# wrote $OUT" >&2
else
  emit_table
fi
