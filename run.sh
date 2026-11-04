#!/usr/bin/env bash
set -euo pipefail
ROOT="$(cd "$(dirname "$0")" && pwd)"
cd "$ROOT"
PORT=8780
OPEN=1
DEBUG=0
for arg in "$@"; do
  case "$arg" in
    --no-open) OPEN=0 ;;
    --debug) DEBUG=1 ;;
    --port=*) PORT="${arg#*=}" ;;
  esac
done
if [ ! -x target/debug/simulmt-app ] || [ ! -x target/native/simulmt-audio-host ]; then
  ./build.sh
fi
export SIMULMT_ROOT="$ROOT"
export SIMULMT_ADDR="127.0.0.1:${PORT}"
export SIMULMT_UI_URL="http://127.0.0.1:${PORT}"
if [ "$DEBUG" = 1 ]; then
  export SIMULMT_UI_URL="${SIMULMT_UI_URL}/?debug=1"
fi
./target/debug/simulmt-app &
CORE_PID=$!
cleanup() {
  kill "$CORE_PID" 2>/dev/null || true
  wait "$CORE_PID" 2>/dev/null || true
}
trap cleanup EXIT INT TERM
for _ in $(seq 1 80); do
  if curl -fsS "http://127.0.0.1:${PORT}/api/health" >/dev/null 2>&1; then break; fi
  sleep 0.25
done
if [ "$OPEN" = 1 ]; then
  ./target/native/simulmt-desktop &
  DESKTOP_PID=$!
  wait "$DESKTOP_PID" || true
else
  echo "SimulMT console: http://127.0.0.1:${PORT}"
  wait "$CORE_PID"
fi
