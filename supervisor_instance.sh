#!/bin/bash
set -uo pipefail
# Usage: supervisor_instance.sh --name NAME --port PORT --data-dir DATA_DIR [--root-dir ROOT_DIR] [--backend BACKEND] [--model MODEL]

# ---------------------------------------------------------------------------
# Parse named arguments
# ---------------------------------------------------------------------------
NAME=""
PORT=""
DATA_DIR=""
ROOT_DIR=""
BACKEND=""
MODEL=""
OLLAMA_HOST=""

while [[ $# -gt 0 ]]; do
  case "$1" in
    --name)     NAME="$2";     shift 2 ;;
    --port)     PORT="$2";     shift 2 ;;
    --data-dir) DATA_DIR="$2"; shift 2 ;;
    --root-dir) ROOT_DIR="$2"; shift 2 ;;
    --backend)  BACKEND="$2";  shift 2 ;;
    --model)    MODEL="$2";    shift 2 ;;
    --ollama-host) OLLAMA_HOST="$2"; shift 2 ;;
    *) echo "[supervisor] Unknown argument: $1" >&2; exit 1 ;;
  esac
done

if [[ -z "$NAME" || -z "$PORT" || -z "$DATA_DIR" ]]; then
  echo "[supervisor] Usage: $0 --name NAME --port PORT --data-dir DATA_DIR [--root-dir ROOT_DIR] [--backend BACKEND] [--model MODEL]" >&2
  exit 1
fi

# ---------------------------------------------------------------------------
# Paths
# ---------------------------------------------------------------------------
BRIDGE_DIR="$(cd "$(dirname "$0")" && pwd)"
LOCK_DIR="$DATA_DIR/.supervisor.lock"
PID_FILE="$DATA_DIR/bridge.pid"
LOG_FILE_INST="$DATA_DIR/bridge.log"
ERR_FILE_INST="$DATA_DIR/bridge.err"
PYTHON_BIN="$BRIDGE_DIR/venv/bin/python"
CHECK_SCRIPT="$BRIDGE_DIR/bridge_healthcheck.py"

export BRIDGE_DISABLE_MDNS="${BRIDGE_DISABLE_MDNS:-1}"

preflight_check() {
  if ! "$PYTHON_BIN" -m py_compile "$BRIDGE_DIR/bridge_v2.py" 2>>"$ERR_FILE_INST"; then
    echo "[supervisor:$NAME] preflight failed: bridge_v2.py has syntax errors"
    return 1
  fi
  return 0
}

# ---------------------------------------------------------------------------
# cleanup() — kill child and remove lock on exit
# ---------------------------------------------------------------------------
cleanup() {
  if [[ -n "${CHILD_PID:-}" ]] && kill -0 "$CHILD_PID" 2>/dev/null; then
    kill "$CHILD_PID" 2>/dev/null || true
    sleep 1
    kill -9 "$CHILD_PID" 2>/dev/null || true
  fi
  # Only remove lock if we are the current owner (prevents standing-by instances
  # from wiping the active supervisor's lock on their way out).
  if [[ "$(cat "$LOCK_PID_FILE" 2>/dev/null || true)" == "$$" ]]; then
    rm -rf "$LOCK_DIR"
  fi
}
trap cleanup EXIT INT TERM

# read_lock_owner — read lock PID, retry once on empty file (handles transient
# window during stale-lock clearing: rm -rf then mkdir leaves file momentarily
# absent, which would otherwise trigger a false "lock taken" self-exit).
read_lock_owner() {
  local _o
  _o="$(cat "$LOCK_PID_FILE" 2>/dev/null || true)"
  if [[ -z "$_o" ]]; then
    sleep 1
    _o="$(cat "$LOCK_PID_FILE" 2>/dev/null || true)"
  fi
  echo "$_o"
}

# ---------------------------------------------------------------------------
# Lock acquisition — detect stale lock and handle healthy orphans
# ---------------------------------------------------------------------------
LOCK_PID_FILE="$LOCK_DIR/pid"

_lock_wait_logged=0
while ! mkdir "$LOCK_DIR" 2>/dev/null; do
  OLD_LOCK_PID="$(cat "$LOCK_PID_FILE" 2>/dev/null || true)"
  if [[ -n "$OLD_LOCK_PID" ]] && kill -0 "$OLD_LOCK_PID" 2>/dev/null; then
    # Active supervisor holds the lock — stand by instead of exiting immediately.
    # This prevents the outer bridge_supervisor.sh from seeing a quick exit and
    # re-spawning indefinitely, which caused a lock-race churn that restarted the
    # bridge every 15 seconds.
    if (( _lock_wait_logged == 0 )); then
      echo "[supervisor:$NAME] supervisor $OLD_LOCK_PID active, standing by"
      _lock_wait_logged=1
    fi
    sleep 15
    continue
  fi
  # Stale lock: old holder is dead. Clear it and retry mkdir.
  echo "[supervisor:$NAME] stale lock (pid=${OLD_LOCK_PID:-?} dead), clearing"
  # If an orphan bridge is healthy, keep it alive and adopt it.
  # Only consider processes that own OUR port — never kill bridges on other ports.
  _port_pids="$(lsof -t -i :"$PORT" -sTCP:LISTEN 2>/dev/null || true)"
  _pidfile_orphan=""
  if [[ -f "$PID_FILE" ]]; then
    _op="$(cat "$PID_FILE" 2>/dev/null || true)"
    if [[ -n "$_op" ]] && kill -0 "$_op" 2>/dev/null; then
      _cmd="$(ps -p "$_op" -o command= 2>/dev/null || true)"
      [[ "$_cmd" == *"bridge_v2.py"* ]] && _pidfile_orphan="$_op"
    fi
  fi
  ORPHAN_PIDS="$(printf '%s\n%s\n' "$_port_pids" "$_pidfile_orphan" | sort -u | grep -v '^$' || true)"
  HEALTHY_ORPHAN=""
  if [[ -n "$ORPHAN_PIDS" ]]; then
    for opid in $ORPHAN_PIDS; do
      if "$PYTHON_BIN" "$CHECK_SCRIPT" --host 127.0.0.1 --port "$PORT" --timeout 5 2>/dev/null; then
        echo "[supervisor:$NAME] orphan bridge pid=$opid is healthy — will adopt"
        HEALTHY_ORPHAN="$opid"
        break
      fi
    done
    if [[ -z "$HEALTHY_ORPHAN" ]]; then
      echo "[supervisor:$NAME] killing orphan pids on port $PORT: $ORPHAN_PIDS"
      kill $ORPHAN_PIDS 2>/dev/null || true
      sleep 1
      kill -9 $ORPHAN_PIDS 2>/dev/null || true
    fi
  fi
  rm -rf "$LOCK_DIR"
  # Loop back to retry mkdir
done
echo $$ > "$LOCK_PID_FILE"

echo "[supervisor:$NAME] start, port=$PORT, data-dir=$DATA_DIR"

# ---------------------------------------------------------------------------
# Log rotation helper — keep both bridge.log and bridge.err under 5 MB
# ---------------------------------------------------------------------------
_LOG_MAX_BYTES=$((5 * 1024 * 1024))

_rotate_log_if_large() {
  local _file="$1"
  local _label="$2"
  if [[ -f "$_file" ]]; then
    local _sz
    _sz=$(wc -c < "$_file" 2>/dev/null || echo 0)
    if [[ "$_sz" -gt "$_LOG_MAX_BYTES" ]]; then
      # copytruncate semantics: the bridge child holds an O_APPEND fd on this
      # file for its whole lifetime (see redirect at spawn). Renaming with mv
      # would leave the child writing to the renamed inode while the fresh file
      # stays empty — rotation silently fails. Instead copy the contents aside,
      # then truncate the SAME inode the child still holds; O_APPEND writes
      # continue at the new (small) tail. Slightly more I/O than mv, but correct
      # for a file held open by another process.
      cp -f "$_file" "${_file}.1" 2>/dev/null || true
      : > "$_file"
      echo "[supervisor:$NAME] rotated ${_label} (was ${_sz} bytes)"
    fi
  fi
}

# Rotate at startup
_rotate_log_if_large "$ERR_FILE_INST" "bridge.err"
_rotate_log_if_large "$LOG_FILE_INST" "bridge.log"

# Counter for periodic in-loop log rotation (every ~60 monitor iterations ≈ 5 min)
_LOG_CHECK_INTERVAL=60
_LOG_CHECK_COUNTER=0

BACKOFF=1
MAX_RETRIES=10
RETRY_COUNT=0

# ---------------------------------------------------------------------------
# Startup adopt — if a healthy bridge is already running, adopt it.
# Handles launchd/parent supervisor restarting us while the bridge is live.
# ---------------------------------------------------------------------------
EXISTING_PID="${HEALTHY_ORPHAN:-$(lsof -t -i :"$PORT" -sTCP:LISTEN 2>/dev/null | head -1 || true)}"
if [[ -n "$EXISTING_PID" ]] && kill -0 "$EXISTING_PID" 2>/dev/null; then
  if [[ -n "${HEALTHY_ORPHAN:-}" ]] || "$PYTHON_BIN" "$CHECK_SCRIPT" --host 127.0.0.1 --port "$PORT" --timeout 5 2>/dev/null; then
    echo "[supervisor:$NAME] adopting healthy bridge pid=$EXISTING_PID"
    CHILD_PID="$EXISTING_PID"
    echo "$CHILD_PID" > "$PID_FILE"
    BACKOFF=1
    RETRY_COUNT=0
    CONSEC_FAIL=0
    while true; do
      if ! kill -0 "$CHILD_PID" 2>/dev/null; then
        echo "[supervisor:$NAME] adopted bridge exited, restarting"
        break 1
      fi
      if ! "$PYTHON_BIN" "$CHECK_SCRIPT" --host 127.0.0.1 --port "$PORT" --timeout 8; then
        CONSEC_FAIL=$(( CONSEC_FAIL + 1 ))
        echo "[supervisor:$NAME] healthcheck failed (${CONSEC_FAIL}/3)"
        if (( CONSEC_FAIL >= 3 )); then
          echo "[supervisor:$NAME] 3 consecutive failures, restarting bridge"
          kill "$CHILD_PID" 2>/dev/null || true
          sleep 1; kill -9 "$CHILD_PID" 2>/dev/null || true
          break 1
        fi
      else
        CONSEC_FAIL=0
      fi
      sleep 5
      _lock_owner="$(read_lock_owner)"
      if [[ "${_lock_owner}" != "$$" ]]; then
        echo "[supervisor:$NAME] lock taken by pid=${_lock_owner:-?}, self-exiting"
        exit 0
      fi
    done
  fi
fi

# ---------------------------------------------------------------------------
# wait_for_port_free — waits up to $1 seconds, then force-kills if still busy
# ---------------------------------------------------------------------------
wait_for_port_free() {
  local max_wait="${1:-30}"
  local waited=0
  while lsof -t -i :"$PORT" -sTCP:LISTEN >/dev/null 2>&1; do
    # Never force-kill if we lost the lock — another supervisor owns this port.
    local _wfp_owner
    _wfp_owner="$(read_lock_owner)"
    if [[ "${_wfp_owner}" != "$$" ]]; then
      echo "[supervisor:$NAME] lock taken by pid=${_wfp_owner:-?} while waiting for port, self-exiting"
      exit 0
    fi
    if (( waited >= max_wait )); then
      echo "[supervisor:$NAME] port $PORT still busy after ${max_wait}s — force-killing"
      lsof -t -i :"$PORT" -sTCP:LISTEN 2>/dev/null | xargs kill -9 2>/dev/null || true
      sleep 2
      if lsof -t -i :"$PORT" -sTCP:LISTEN >/dev/null 2>&1; then
        echo "[supervisor:$NAME] port $PORT still occupied after force-kill (TIME_WAIT or zombie)"
      fi
      return 0
    fi
    echo "[supervisor:$NAME] port $PORT busy, waiting... (${waited}s)"
    sleep 2
    (( waited += 2 ))
  done
  return 0
}

# ---------------------------------------------------------------------------
# Main spawn + monitor loop
# ---------------------------------------------------------------------------
while true; do
  # Self-eviction at top of every outer loop iteration.
  _lock_owner="$(read_lock_owner)"
  if [[ "${_lock_owner}" != "$$" ]]; then
    echo "[supervisor:$NAME] lock taken by pid=${_lock_owner:-?}, self-exiting"
    exit 0
  fi

  if ! preflight_check; then
    sleep "$BACKOFF"
    BACKOFF=$(( BACKOFF < 60 ? BACKOFF * 2 : 60 ))
    (( RETRY_COUNT++ ))
    continue
  fi

  # Enforce maximum consecutive restart limit.
  if (( RETRY_COUNT >= MAX_RETRIES )); then
    echo "[supervisor:$NAME] reached max restarts ($MAX_RETRIES), sleeping 60s before resetting counter"
    sleep 60
    RETRY_COUNT=0
    BACKOFF=1
  fi

  # Wait for port to be free before spawning.
  if ! wait_for_port_free 30; then
    echo "[supervisor:$NAME] cannot reclaim port $PORT, backing off ${BACKOFF}s"
    sleep "$BACKOFF"
    BACKOFF=$(( BACKOFF < 60 ? BACKOFF * 2 : 60 ))
    (( RETRY_COUNT++ ))
    continue
  fi

  # Clean stale pid owner if any.
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

  # Build spawn command — only include optional arguments when non-empty.
  args=(--port "$PORT" --data-dir "$DATA_DIR")
  if [[ -n "$ROOT_DIR" ]]; then
    args+=(--root-dir "$ROOT_DIR")
  fi
  if [[ -n "$BACKEND" ]]; then
    args+=(--backend "$BACKEND")
  fi
  if [[ -n "$MODEL" ]]; then
    args+=(--model "$MODEL")
  fi
  if [[ -n "$OLLAMA_HOST" ]]; then
    args+=(--ollama-host "$OLLAMA_HOST")
  fi

  "$BRIDGE_DIR/run_bridge.sh" "${args[@]}" >>"$LOG_FILE_INST" 2>>"$ERR_FILE_INST" &
  CHILD_PID=$!
  echo "$CHILD_PID" > "$PID_FILE"
  echo "[supervisor:$NAME] spawned bridge pid=$CHILD_PID (attempt $((RETRY_COUNT+1))/$MAX_RETRIES)"

  # Wait for healthy up to 120s; also verify OUR PID owns the port.
  # Uses 120s (240 × 0.5s) instead of 25s to accommodate slow init paths
  # (1900+ session restore + codex backend scan can take 30-60s on cold start).
  # With SO_REUSEPORT multiple PIDs may listen simultaneously; we check
  # whether CHILD_PID is IN the listener set rather than requiring it to be
  # the sole owner, and clean up any stale co-listeners.
  HEALTHY=0
  for _ in {1..240}; do
    if ! kill -0 "$CHILD_PID" 2>/dev/null; then
      break
    fi
    if "$PYTHON_BIN" "$CHECK_SCRIPT" --host 127.0.0.1 --port "$PORT" --timeout 0.5; then
      PORT_OWNERS="$(lsof -t -i :"$PORT" -sTCP:LISTEN 2>/dev/null || true)"
      if echo "${PORT_OWNERS}" | grep -qx "${CHILD_PID}"; then
        # Kill any stale co-listeners from previous bridge runs
        for _spid in $(echo "${PORT_OWNERS}" | grep -v "^${CHILD_PID}$"); do
          echo "[supervisor:$NAME] killing stale co-listener pid=$_spid from port $PORT"
          kill -9 "$_spid" 2>/dev/null || true
        done
        BACKOFF=1
        RETRY_COUNT=0
        HEALTHY=1
        break
      else
        STALE_PORT_OWNER="$(echo "${PORT_OWNERS}" | head -1)"
        echo "[supervisor:$NAME] stale bridge pid=${STALE_PORT_OWNER:-?} owns port (expected $CHILD_PID) — killing stale"
        [[ -n "${PORT_OWNERS:-}" ]] && { kill -9 $PORT_OWNERS 2>/dev/null || true; sleep 1; }
        kill "$CHILD_PID" 2>/dev/null || true
        break
      fi
    fi
    sleep 0.5
  done

  if (( HEALTHY == 0 )); then
    echo "[supervisor:$NAME] bridge did not become healthy within 120s"
    kill "$CHILD_PID" 2>/dev/null || true
    sleep 1
    kill -9 "$CHILD_PID" 2>/dev/null || true
    sleep "$BACKOFF"
    BACKOFF=$(( BACKOFF < 60 ? BACKOFF * 2 : 60 ))
    (( RETRY_COUNT++ ))
    continue
  fi

  # Monitor loop — require 3 consecutive failures before killing.
  # (Prevents bulk_ingest file-read spikes from triggering false restarts.)
  CONSEC_FAIL=0
  while true; do
    if ! kill -0 "$CHILD_PID" 2>/dev/null; then
      echo "[supervisor:$NAME] bridge exited, restart in ${BACKOFF}s"
      sleep "$BACKOFF"
      BACKOFF=$(( BACKOFF < 60 ? BACKOFF * 2 : 60 ))
      (( RETRY_COUNT++ ))
      break
    fi

    if ! "$PYTHON_BIN" "$CHECK_SCRIPT" --host 127.0.0.1 --port "$PORT" --timeout 8; then
      CONSEC_FAIL=$(( CONSEC_FAIL + 1 ))
      echo "[supervisor:$NAME] healthcheck failed (${CONSEC_FAIL}/3)"
      if (( CONSEC_FAIL >= 3 )); then
        echo "[supervisor:$NAME] 3 consecutive failures, restarting bridge"
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

    # Periodic log rotation inside monitor loop (bash 3.2 safe)
    _LOG_CHECK_COUNTER=$(( _LOG_CHECK_COUNTER + 1 ))
    if [[ "$_LOG_CHECK_COUNTER" -ge "$_LOG_CHECK_INTERVAL" ]]; then
      _LOG_CHECK_COUNTER=0
      _rotate_log_if_large "$ERR_FILE_INST" "bridge.err"
      _rotate_log_if_large "$LOG_FILE_INST" "bridge.log"
    fi

    _lock_owner="$(read_lock_owner)"
    if [[ "${_lock_owner}" != "$$" ]]; then
      echo "[supervisor:$NAME] lock taken by pid=${_lock_owner:-?}, self-exiting"
      exit 0
    fi
  done
done
