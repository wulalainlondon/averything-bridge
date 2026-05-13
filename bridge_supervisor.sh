#!/bin/bash
set -uo pipefail

BRIDGE_DIR="$(cd "$(dirname "$0")" && pwd)"
PORT="${BRIDGE_PORT:-8766}"
LOCK_DIR="$BRIDGE_DIR/.supervisor.lock"
PID_FILE="$BRIDGE_DIR/bridge.pid"
LOG_FILE="$BRIDGE_DIR/bridge.log"
ERR_FILE="$BRIDGE_DIR/bridge.err"
CHECK_SCRIPT="$BRIDGE_DIR/bridge_healthcheck.py"
PYTHON_BIN="$BRIDGE_DIR/venv/bin/python"
export BRIDGE_DISABLE_MDNS="${BRIDGE_DISABLE_MDNS:-1}"

cleanup() {
  if [[ -n "${CHILD_PID:-}" ]] && kill -0 "$CHILD_PID" 2>/dev/null; then
    kill "$CHILD_PID" 2>/dev/null || true
    sleep 1
    kill -9 "$CHILD_PID" 2>/dev/null || true
  fi
  rm -rf "$LOCK_DIR"
}
trap cleanup EXIT INT TERM

LOCK_PID_FILE="$LOCK_DIR/pid"

if ! mkdir "$LOCK_DIR" 2>/dev/null; then
  OLD_LOCK_PID="$(cat "$LOCK_PID_FILE" 2>/dev/null || true)"
  if [[ -n "$OLD_LOCK_PID" ]] && kill -0 "$OLD_LOCK_PID" 2>/dev/null; then
    echo "[supervisor] already running (pid=$OLD_LOCK_PID), exiting"
    exit 0
  else
    echo "[supervisor] stale lock (pid=${OLD_LOCK_PID:-?} dead), clearing"
    rm -rf "$LOCK_DIR"
    mkdir "$LOCK_DIR"
  fi
fi
echo $$ > "$LOCK_PID_FILE"

echo "[supervisor] start, port=$PORT"
BACKOFF=1

while true; do
  # clean stale pid owner if any
  if [[ -f "$PID_FILE" ]]; then
    OLD_PID="$(cat "$PID_FILE" 2>/dev/null || true)"
    if [[ -n "$OLD_PID" ]] && kill -0 "$OLD_PID" 2>/dev/null; then
      CMD="$(ps -p "$OLD_PID" -o command= || true)"
      if [[ "$CMD" == *"claude_bridge_v2.py"* ]]; then
        kill "$OLD_PID" 2>/dev/null || true
        sleep 1
      fi
    fi
  fi

  "$BRIDGE_DIR/run_bridge.sh" --port "$PORT" >>"$LOG_FILE" 2>>"$ERR_FILE" &
  CHILD_PID=$!
  echo "$CHILD_PID" > "$PID_FILE"
  echo "[supervisor] spawned bridge pid=$CHILD_PID"

  # wait for healthy up to 25s
  for _ in {1..50}; do
    if "$PYTHON_BIN" "$CHECK_SCRIPT" --host 127.0.0.1 --port "$PORT" --timeout 0.5; then
      BACKOFF=1
      break
    fi
    sleep 0.5
  done

  # monitor loop
  while true; do
    if ! kill -0 "$CHILD_PID" 2>/dev/null; then
      echo "[supervisor] bridge exited, restart in ${BACKOFF}s"
      sleep "$BACKOFF"
      BACKOFF=$(( BACKOFF < 30 ? BACKOFF * 2 : 30 ))
      break
    fi

    if ! "$PYTHON_BIN" "$CHECK_SCRIPT" --host 127.0.0.1 --port "$PORT" --timeout 1.5; then
      echo "[supervisor] healthcheck failed, restarting bridge"
      kill "$CHILD_PID" 2>/dev/null || true
      sleep 1
      kill -9 "$CHILD_PID" 2>/dev/null || true
      sleep "$BACKOFF"
      BACKOFF=$(( BACKOFF < 30 ? BACKOFF * 2 : 30 ))
      break
    fi

    sleep 10
  done
done
