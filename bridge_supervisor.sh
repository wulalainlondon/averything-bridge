#!/bin/bash
set -uo pipefail

BRIDGE_DIR="$(cd "$(dirname "$0")" && pwd)"
INSTANCES_CONFIG="${BRIDGE_INSTANCES_CONFIG:-$BRIDGE_DIR/instances.json}"

# ---------------------------------------------------------------------------
# Legacy single-instance mode — no instances.json, fall back to BRIDGE_PORT
# ---------------------------------------------------------------------------
if [[ ! -f "$INSTANCES_CONFIG" ]]; then
  echo "[supervisor] WARNING: no instances.json found, running in legacy single-instance mode (deprecated)"
  exec "$BRIDGE_DIR/supervisor_instance.sh" --name "default" --port "${BRIDGE_PORT:-8766}" --data-dir "$BRIDGE_DIR"
fi

# ---------------------------------------------------------------------------
# Multi-instance mode — parse instances.json
# ---------------------------------------------------------------------------
INSTANCES_JSON="$(python3 -c "
import json, sys, os
data = json.load(open('$INSTANCES_CONFIG'))
for inst in data['instances']:
    name = inst['name']
    port = inst['port']
    data_dir = os.path.expanduser(inst['data_dir'])
    root_dir = os.path.expanduser(inst.get('root_dir', ''))
    print(f'{name}|{port}|{data_dir}|{root_dir}')
" 2>&1)" || { echo "[supervisor] Failed to parse $INSTANCES_CONFIG: $INSTANCES_JSON"; exit 1; }

if [[ -z "$INSTANCES_JSON" ]]; then
  echo "[supervisor] instances.json contains no instances"
  exit 1
fi

# ---------------------------------------------------------------------------
# Validate: no duplicate ports
# ---------------------------------------------------------------------------
DUPLICATE_PORTS="$(echo "$INSTANCES_JSON" | awk -F'|' '{print $2}' | sort | uniq -d)"
if [[ -n "$DUPLICATE_PORTS" ]]; then
  echo "[supervisor] ERROR: duplicate ports found in $INSTANCES_CONFIG: $DUPLICATE_PORTS"
  exit 1
fi

# ---------------------------------------------------------------------------
# Parallel indexed arrays — bash 3.2 has no associative arrays (declare -A)
# Index i corresponds to the same instance across all arrays.
# ---------------------------------------------------------------------------
CHILD_NAMES=()
CHILD_PIDS=()
CHILD_PORTS=()
CHILD_DATA_DIRS=()
CHILD_ROOT_DIRS=()

# ---------------------------------------------------------------------------
# cleanup() — kill all child supervisors on exit
# ---------------------------------------------------------------------------
cleanup() {
  local i
  for (( i=0; i<${#CHILD_PIDS[@]}; i++ )); do
    local pid="${CHILD_PIDS[$i]}"
    if [[ -n "$pid" ]] && kill -0 "$pid" 2>/dev/null; then
      echo "[supervisor] stopping instance '${CHILD_NAMES[$i]}' (pid=$pid)"
      kill "$pid" 2>/dev/null || true
    fi
  done
}
trap cleanup EXIT INT TERM

# ---------------------------------------------------------------------------
# spawn_instance INDEX — (re-)spawn supervisor_instance.sh for one entry
# ---------------------------------------------------------------------------
spawn_instance() {
  local idx="$1"
  local name="${CHILD_NAMES[$idx]}"
  local port="${CHILD_PORTS[$idx]}"
  local data_dir="${CHILD_DATA_DIRS[$idx]}"
  local root_dir="${CHILD_ROOT_DIRS[$idx]}"

  mkdir -p "$data_dir"

  if [[ -n "$root_dir" ]]; then
    "$BRIDGE_DIR/supervisor_instance.sh" \
      --name "$name" --port "$port" \
      --data-dir "$data_dir" --root-dir "$root_dir" &
  else
    "$BRIDGE_DIR/supervisor_instance.sh" \
      --name "$name" --port "$port" \
      --data-dir "$data_dir" &
  fi
  CHILD_PIDS[$idx]=$!
  echo "[supervisor] started instance '$name' on port $port (supervisor pid=${CHILD_PIDS[$idx]})"
}

# ---------------------------------------------------------------------------
# Populate arrays and do the initial spawn
# ---------------------------------------------------------------------------
while IFS='|' read -r name port data_dir root_dir; do
  idx=${#CHILD_NAMES[@]}
  CHILD_NAMES+=("$name")
  CHILD_PORTS+=("$port")
  CHILD_DATA_DIRS+=("$data_dir")
  CHILD_ROOT_DIRS+=("$root_dir")
  CHILD_PIDS+=("")   # placeholder; filled by spawn_instance
  spawn_instance "$idx"
done <<< "$INSTANCES_JSON"

echo "[supervisor] ${#CHILD_NAMES[@]} instance(s) started"

# ---------------------------------------------------------------------------
# Monitor loop — poll every 5s; restart any exited instance supervisor.
# macOS bash 3.2 does NOT support wait -n, so we use polling.
# ---------------------------------------------------------------------------
while true; do
  sleep 5
  local_i=0
  for (( local_i=0; local_i<${#CHILD_PIDS[@]}; local_i++ )); do
    pid="${CHILD_PIDS[$local_i]}"
    name="${CHILD_NAMES[$local_i]}"
    if [[ -z "$pid" ]] || ! kill -0 "$pid" 2>/dev/null; then
      echo "[supervisor] instance '$name' supervisor exited (pid=${pid:-?}), restarting in 5s"
      sleep 5
      spawn_instance "$local_i"
    fi
  done
done
