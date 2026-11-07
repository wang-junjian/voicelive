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
export SIMULMT_RUNTIME_DIR="${SIMULMT_RUNTIME_DIR:-$ROOT/runtime}"
export SIMULMT_ADDR="127.0.0.1:${PORT}"
./target/debug/simulmt-app &
CORE_PID=$!
cleanup() {
  kill "$CORE_PID" 2>/dev/null || true
  wait "$CORE_PID" 2>/dev/null || true
}
trap cleanup EXIT INT TERM
READY=0
for _ in $(seq 1 80); do
  if curl -fsS "http://127.0.0.1:${PORT}/api/health" >/dev/null 2>&1; then READY=1; break; fi
  sleep 0.25
done
if [ "$READY" != 1 ]; then
  echo "EchoNote Core did not become ready" >&2
  exit 1
fi
export SIMULMT_UI_URL="$(python3 - "$SIMULMT_RUNTIME_DIR/bootstrap.json" "$DEBUG" <<'PY'
import json
import sys

with open(sys.argv[1], encoding="utf-8") as source:
    bootstrap = json.load(source)
url = bootstrap["url"] + "/"
if sys.argv[2] == "1":
    url += "?debug=1"
print(url + "#auth=" + bootstrap["token"])
PY
)"
if [ "$OPEN" = 1 ]; then
  ./target/native/simulmt-desktop &
  DESKTOP_PID=$!
  wait "$DESKTOP_PID" || true
else
  echo "EchoNote console: ${SIMULMT_UI_URL}"
  wait "$CORE_PID"
fi
