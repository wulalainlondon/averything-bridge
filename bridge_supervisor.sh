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
    # If the orphan bridge is healthy, keep it alive and adopt it after taking the lock.
    ORPHAN_PIDS="$(pgrep -f "${BRIDGE_DIR}/bridge_v2\.py" 2>/dev/null || true)"
    HEALTHY_ORPHAN=""
    if [[ -n "$ORPHAN_PIDS" ]]; then
      for opid in $ORPHAN_PIDS; do
        if "$PYTHON_BIN" "$CHECK_SCRIPT" --host 127.0.0.1 --port "$PORT" --timeout 5 2>/dev/null; then
          echo "[supervisor] orphan bridge pid=$opid is healthy — will adopt"
          HEALTHY_ORPHAN="$opid"
          break
        fi
      done
      if [[ -z "$HEALTHY_ORPHAN" ]]; then
        echo "[supervisor] killing orphan bridge_v2.py pids: $ORPHAN_PIDS"
        kill $ORPHAN_PIDS 2>/dev/null || true
        sleep 1
        kill -9 $ORPHAN_PIDS 2>/dev/null || true
      fi
    fi
    rm -rf "$LOCK_DIR"
    mkdir "$LOCK_DIR"
  fi
fi
echo $$ > "$LOCK_PID_FILE"

echo "[supervisor] start, port=$PORT"
BACKOFF=1
MAX_RETRIES=10
RETRY_COUNT=0

# On startup: if a healthy bridge is already running, adopt it instead of killing it.
# This handles launchd restarting the supervisor while the bridge is still healthy.
EXISTING_PID="${HEALTHY_ORPHAN:-$(lsof -t -i :"$PORT" -sTCP:LISTEN 2>/dev/null | head -1 || true)}"
if [[ -n "$EXISTING_PID" ]] && kill -0 "$EXISTING_PID" 2>/dev/null; then
  if [[ -n "${HEALTHY_ORPHAN:-}" ]] || "$PYTHON_BIN" "$CHECK_SCRIPT" --host 127.0.0.1 --port "$PORT" --timeout 5 2>/dev/null; then
    echo "[supervisor] adopting healthy bridge pid=$EXISTING_PID"
    CHILD_PID="$EXISTING_PID"
    echo "$CHILD_PID" > "$PID_FILE"
    BACKOFF=1
    RETRY_COUNT=0
    # Jump directly into the monitor loop
    CONSEC_FAIL=0
    while true; do
      if ! kill -0 "$CHILD_PID" 2>/dev/null; then
        echo "[supervisor] adopted bridge exited, restarting"
        break 1
      fi
      if ! "$PYTHON_BIN" "$CHECK_SCRIPT" --host 127.0.0.1 --port "$PORT" --timeout 8; then
        CONSEC_FAIL=$(( CONSEC_FAIL + 1 ))
        echo "[supervisor] healthcheck failed (${CONSEC_FAIL}/3)"
        if (( CONSEC_FAIL >= 3 )); then
          echo "[supervisor] 3 consecutive failures, restarting bridge"
          kill "$CHILD_PID" 2>/dev/null || true
          sleep 1; kill -9 "$CHILD_PID" 2>/dev/null || true
          break 1
        fi
      else
        CONSEC_FAIL=0
      fi
      sleep 5
    done
  fi
fi

# Wait until port $PORT is free (up to $1 seconds), then force-kill if still busy.
wait_for_port_free() {
  local max_wait="${1:-30}"
  local waited=0
  while lsof -t -i :"$PORT" -sTCP:LISTEN >/dev/null 2>&1; do
    if (( waited >= max_wait )); then
      echo "[supervisor] port $PORT still busy after ${max_wait}s — force-killing"
      lsof -t -i :"$PORT" -sTCP:LISTEN 2>/dev/null | xargs kill -9 2>/dev/null || true
      sleep 2
      # one more check — warn if still occupied
      if lsof -t -i :"$PORT" -sTCP:LISTEN >/dev/null 2>&1; then
        echo "[supervisor] port $PORT still occupied after force-kill (TIME_WAIT or zombie)"
      fi
      return 0
    fi
    echo "[supervisor] port $PORT busy, waiting... (${waited}s)"
    sleep 2
    (( waited += 2 ))
  done
  return 0
}

while true; do
  # Enforce maximum consecutive restart limit.
  if (( RETRY_COUNT >= MAX_RETRIES )); then
    echo "[supervisor] reached max restarts ($MAX_RETRIES), sleeping 60s before resetting counter"
    sleep 60
    RETRY_COUNT=0
    BACKOFF=1
  fi

  # Wait for port to be free before spawning.
  if ! wait_for_port_free 30; then
    echo "[supervisor] cannot reclaim port $PORT, backing off ${BACKOFF}s"
    sleep "$BACKOFF"
    BACKOFF=$(( BACKOFF < 60 ? BACKOFF * 2 : 60 ))
    (( RETRY_COUNT++ ))
    continue
  fi

  # clean stale pid owner if any
  if [[ -f "$PID_FILE" ]]; then
    OLD_PID="$(cat "$PID_FILE" 2>/dev/null || true)"
    if [[ -n "$OLD_PID" ]] && kill -0 "$OLD_PID" 2>/dev/null; then
      CMD="$(ps -p "$OLD_PID" -o command= || true)"
      if [[ "$CMD" == *"bridge_v2.py"* ]]; then
        kill "$OLD_PID" 2>/dev/null || true
        sleep 1
      fi
    fi
  fi

  "$BRIDGE_DIR/run_bridge.sh" --port "$PORT" >>"$LOG_FILE" 2>>"$ERR_FILE" &
  CHILD_PID=$!
  echo "$CHILD_PID" > "$PID_FILE"
  echo "[supervisor] spawned bridge pid=$CHILD_PID (attempt $((RETRY_COUNT+1))/$MAX_RETRIES)"

  # wait for healthy up to 25s; also verify OUR PID owns the port
  # (guards against a stale bridge answering the healthcheck while our bridge
  # failed to bind — the root cause of the EADDRINUSE restart flood)
  HEALTHY=0
  for _ in {1..50}; do
    if ! kill -0 "$CHILD_PID" 2>/dev/null; then
      break
    fi
    if "$PYTHON_BIN" "$CHECK_SCRIPT" --host 127.0.0.1 --port "$PORT" --timeout 0.5; then
      PORT_OWNER="$(lsof -t -i :"$PORT" -sTCP:LISTEN 2>/dev/null | head -1 || true)"
      if [[ "${PORT_OWNER:-}" == "$CHILD_PID" ]]; then
        BACKOFF=1
        RETRY_COUNT=0
        HEALTHY=1
        break
      else
        echo "[supervisor] stale bridge pid=${PORT_OWNER:-?} owns port (expected $CHILD_PID) — killing stale"
        [[ -n "${PORT_OWNER:-}" ]] && { kill -9 "$PORT_OWNER" 2>/dev/null || true; sleep 1; }
        kill "$CHILD_PID" 2>/dev/null || true
        break
      fi
    fi
    sleep 0.5
  done

  if (( HEALTHY == 0 )); then
    echo "[supervisor] bridge did not become healthy within 25s"
    kill "$CHILD_PID" 2>/dev/null || true
    sleep 1
    kill -9 "$CHILD_PID" 2>/dev/null || true
    sleep "$BACKOFF"
    BACKOFF=$(( BACKOFF < 60 ? BACKOFF * 2 : 60 ))
    (( RETRY_COUNT++ ))
    continue
  fi

  # monitor loop — require 3 consecutive failures before killing
  # (prevents bulk_ingest file-read spikes from triggering false restarts)
  CONSEC_FAIL=0
  while true; do
    if ! kill -0 "$CHILD_PID" 2>/dev/null; then
      echo "[supervisor] bridge exited, restart in ${BACKOFF}s"
      sleep "$BACKOFF"
      BACKOFF=$(( BACKOFF < 60 ? BACKOFF * 2 : 60 ))
      (( RETRY_COUNT++ ))
      break
    fi

    if ! "$PYTHON_BIN" "$CHECK_SCRIPT" --host 127.0.0.1 --port "$PORT" --timeout 8; then
      CONSEC_FAIL=$(( CONSEC_FAIL + 1 ))
      echo "[supervisor] healthcheck failed (${CONSEC_FAIL}/3)"
      if (( CONSEC_FAIL >= 3 )); then
        echo "[supervisor] 3 consecutive failures, restarting bridge"
        kill "$CHILD_PID" 2>/dev/null || true
        sleep 1
        kill -9 "$CHILD_PID" 2>/dev/null || true
        sleep "$BACKOFF"
        BACKOFF=$(( BACKOFF < 60 ? BACKOFF * 2 : 60 ))
        (( RETRY_COUNT++ ))
        break
      fi
    else
      CONSEC_FAIL=0
    fi

    sleep 5
  done
done
