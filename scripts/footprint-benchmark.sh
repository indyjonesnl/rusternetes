#!/usr/bin/env bash
# Footprint benchmark harness for the all-in-one rusternetes binary (#1038).
#
# Measures the four numbers the "k3s without melting your laptop" thesis rests
# on, head-to-head-comparable against k3s/k0s/microk8s on identical hardware:
#
#   1. binary size        — release artifact, raw + stripped
#   2. time-to-cluster    — process start → API /readyz returns 200
#   3. idle RSS           — steady-state VmRSS of the idle control plane
#   4. idle CPU %         — steady-state CPU of the idle control plane
#
# It boots a real all-in-one (default SQLite backend) in a throwaway data dir,
# waits for readiness, samples, then tears it down. Output is a Markdown table
# ready to paste into docs/PERFORMANCE.md.
#
# Usage:
#   scripts/footprint-benchmark.sh                       # build-aware defaults
#   scripts/footprint-benchmark.sh --binary path/to/bin  # benchmark a specific binary
#   scripts/footprint-benchmark.sh --label "k3s" --binary $(command -v k3s) \
#       --args "server --disable=traefik" --ready-url http://127.0.0.1:6443/readyz
#   scripts/footprint-benchmark.sh --settle 20 --seconds 60   # longer windows
#
# Notes:
# * Honors CARGO_TARGET_DIR (shared-target-dir checkouts) for the default binary.
# * The all-in-one serves plain HTTP on :6443 by default (TLS is opt-in via
#   --tls), so readiness is probed over http unless --ready-url overrides it.
# * idle CPU is measured from /proc/<pid>/stat (utime+stime) over the window and
#   divided by wall-clock ticks — it is whole-process (all tokio worker threads).
set -euo pipefail

LABEL="rusternetes (all-in-one, sqlite)"
BINARY=""
ARGS=""
READY_URL=""
SETTLE=8
SECONDS_TO_SAMPLE=30
READY_TIMEOUT=60
KEEP_DATA=""

while [[ $# -gt 0 ]]; do
  case "$1" in
    --label) LABEL="$2"; shift 2 ;;
    --binary) BINARY="$2"; shift 2 ;;
    --args) ARGS="$2"; shift 2 ;;
    --ready-url) READY_URL="$2"; shift 2 ;;
    --settle) SETTLE="$2"; shift 2 ;;
    --seconds) SECONDS_TO_SAMPLE="$2"; shift 2 ;;
    --ready-timeout) READY_TIMEOUT="$2"; shift 2 ;;
    --keep-data) KEEP_DATA=1; shift ;;
    -h|--help) sed -n '2,30p' "$0"; exit 0 ;;
    *) echo "unknown arg: $1" >&2; exit 2 ;;
  esac
done

# --- resolve the binary ----------------------------------------------------
if [[ -z "$BINARY" ]]; then
  TARGET_DIR="${CARGO_TARGET_DIR:-target}"
  BINARY="$TARGET_DIR/release/rusternetes"
  if [[ ! -x "$BINARY" ]]; then
    echo "error: $BINARY not found." >&2
    echo "       build it:  cargo build --release -p rusternetes" >&2
    echo "       or pass --binary <path> (e.g. a musl build, or k3s)." >&2
    exit 1
  fi
fi
[[ -x "$BINARY" ]] || { echo "error: $BINARY is not executable" >&2; exit 1; }
[[ -z "$READY_URL" ]] && READY_URL="http://127.0.0.1:6443/readyz"

# --- 1. binary size --------------------------------------------------------
size_bytes() { stat -c %s "$1"; }
mib() { awk -v b="$1" 'BEGIN{printf "%.1f", b/1048576}'; }

RAW_BYTES=$(size_bytes "$BINARY")
STRIPPED_MIB="n/a"
if command -v strip >/dev/null 2>&1; then
  TMP_BIN="$(mktemp)"
  cp "$BINARY" "$TMP_BIN"
  strip "$TMP_BIN" 2>/dev/null && STRIPPED_MIB="$(mib "$(size_bytes "$TMP_BIN")")"
  rm -f "$TMP_BIN"
fi
RAW_MIB="$(mib "$RAW_BYTES")"

# --- boot in a throwaway data dir ------------------------------------------
DATA_DIR="$(mktemp -d -t rusternetes-footprint.XXXXXX)"
LOG="$DATA_DIR/run.log"
cleanup() {
  [[ -n "${PID:-}" ]] && kill "$PID" 2>/dev/null || true
  [[ -n "${PID:-}" ]] && wait "$PID" 2>/dev/null || true
  [[ -z "$KEEP_DATA" ]] && rm -rf "$DATA_DIR"
}
trap cleanup EXIT

echo "booting: $BINARY $ARGS" >&2
START_NS=$(date +%s%N)
# shellcheck disable=SC2086
if [[ "$LABEL" == rusternetes* ]]; then
  "$BINARY" --db-path "$DATA_DIR/cluster.db" --volume-dir "$DATA_DIR/volumes" $ARGS \
    >"$LOG" 2>&1 &
else
  "$BINARY" $ARGS >"$LOG" 2>&1 &
fi
PID=$!

# --- 2. time-to-cluster ----------------------------------------------------
TTC="timeout"
deadline=$(( $(date +%s) + READY_TIMEOUT ))
while [[ "$(date +%s)" -lt "$deadline" ]]; do
  if ! kill -0 "$PID" 2>/dev/null; then
    echo "error: process exited before becoming ready; see $LOG" >&2
    tail -n 20 "$LOG" >&2 || true
    exit 1
  fi
  code=$(curl -kso /dev/null -w '%{http_code}' --max-time 2 "$READY_URL" 2>/dev/null || echo 000)
  if [[ "$code" == "200" ]]; then
    NOW_NS=$(date +%s%N)
    TTC="$(awk -v a="$START_NS" -v b="$NOW_NS" 'BEGIN{printf "%.1f", (b-a)/1e9}')"
    break
  fi
  sleep 0.5
done
echo "time-to-cluster: ${TTC}s (ready-url $READY_URL)" >&2

# --- settle, then 3+4. idle RSS + CPU --------------------------------------
echo "settling ${SETTLE}s before sampling ..." >&2
sleep "$SETTLE"

clk=$(getconf CLK_TCK 2>/dev/null || echo 100)
read_cpu_ticks() { awk '{print $14+$15}' "/proc/$1/stat" 2>/dev/null || echo 0; }

cpu_before=$(read_cpu_ticks "$PID")
wall_before=$(date +%s%N)

min=""; max=""; sum=0; count=0
for _ in $(seq "$SECONDS_TO_SAMPLE"); do
  kill -0 "$PID" 2>/dev/null || { echo "error: process exited mid-sample; see $LOG" >&2; exit 1; }
  kb=$(awk '/^VmRSS:/ {print $2}' "/proc/$PID/status" 2>/dev/null || echo "")
  [[ -z "$kb" ]] && { echo "error: cannot read VmRSS" >&2; exit 1; }
  [[ -z "$min" || "$kb" -lt "$min" ]] && min="$kb"
  [[ -z "$max" || "$kb" -gt "$max" ]] && max="$kb"
  sum=$((sum + kb)); count=$((count + 1))
  sleep 1
done

cpu_after=$(read_cpu_ticks "$PID")
wall_after=$(date +%s%N)

avg_kb=$((sum / count))
RSS_MIN_MIB=$((min / 1024)); RSS_AVG_MIB=$((avg_kb / 1024)); RSS_MAX_MIB=$((max / 1024))
CPU_PCT=$(awk -v cb="$cpu_before" -v ca="$cpu_after" -v wb="$wall_before" -v wa="$wall_after" -v clk="$clk" \
  'BEGIN{ dt=(wa-wb)/1e9; if(dt<=0){print "0.0"; exit} printf "%.1f", 100*((ca-cb)/clk)/dt }')

# --- report ----------------------------------------------------------------
cat <<EOF

## Footprint — $LABEL

| Metric | Value |
| --- | --- |
| Binary size (raw) | ${RAW_MIB} MiB |
| Binary size (stripped) | ${STRIPPED_MIB} MiB |
| Time-to-cluster (start → /readyz 200) | ${TTC} s |
| Idle RSS (min / avg / max over ${SECONDS_TO_SAMPLE}s) | ${RSS_MIN_MIB} / ${RSS_AVG_MIB} / ${RSS_MAX_MIB} MiB |
| Idle CPU (whole process over ${SECONDS_TO_SAMPLE}s) | ${CPU_PCT} % |

_binary: \`$BINARY\` · settle ${SETTLE}s · sample ${SECONDS_TO_SAMPLE}s · host: $(uname -m), $(nproc) CPUs_
EOF
