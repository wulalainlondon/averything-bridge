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
INSTANCES_JSON="$(BRIDGE_INSTANCES_CONFIG="$INSTANCES_CONFIG" python3 -c '
import json, sys, os
data = json.load(open(os.environ["BRIDGE_INSTANCES_CONFIG"]))
for inst in data["instances"]:
    name = inst["name"]
    port = inst["port"]
    data_dir = os.path.expanduser(inst["data_dir"])
    root_dir = os.path.expanduser(inst.get("root_dir", ""))
    backend = inst.get("backend", "")
    model = inst.get("model", "")
    ollama_host = inst.get("ollama_host", "")
    print(f"{name}|{port}|{data_dir}|{root_dir}|{backend}|{model}|{ollama_host}")
' 2>&1)" || { echo "[supervisor] Failed to parse $INSTANCES_CONFIG: $INSTANCES_JSON"; exit 1; }

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
# CHILD_NAMES[i]="" is used as a sentinel for deleted/removed entries.
# ---------------------------------------------------------------------------
CHILD_NAMES=()
CHILD_PIDS=()
CHILD_PORTS=()
CHILD_DATA_DIRS=()
CHILD_ROOT_DIRS=()
CHILD_BACKENDS=()
CHILD_MODELS=()
CHILD_OLLAMA_HOSTS=()

# ---------------------------------------------------------------------------
# parse_field LINE FIELD_NUMBER — extract pipe-delimited field (1-based)
# ---------------------------------------------------------------------------
parse_field() {
  echo "$1" | cut -d'|' -f"$2"
}

# ---------------------------------------------------------------------------
# kill_supervisor PID PORT — SIGTERM → 2s → SIGKILL; also clear port squatters
# ---------------------------------------------------------------------------
kill_supervisor() {
  local pid="$1"
  local port="$2"
  if [[ -n "$pid" ]] && kill -0 "$pid" 2>/dev/null; then
    kill "$pid" 2>/dev/null || true
    sleep 2
    kill -9 "$pid" 2>/dev/null || true
  fi
  local bridge_pids
  bridge_pids="$(lsof -t -i :"$port" -sTCP:LISTEN 2>/dev/null || true)"
  if [[ -n "$bridge_pids" ]]; then
    echo "$bridge_pids" | xargs kill -9 2>/dev/null || true
  fi
}

# ---------------------------------------------------------------------------
# cleanup() — kill all child supervisors on exit
# ---------------------------------------------------------------------------
cleanup() {
  local i
  for (( i=0; i<${#CHILD_PIDS[@]}; i++ )); do
    [[ -z "${CHILD_NAMES[$i]}" ]] && continue
    local pid="${CHILD_PIDS[$i]}"
    if [[ -n "$pid" ]] && kill -0 "$pid" 2>/dev/null; then
      echo "[supervisor] stopping instance '${CHILD_NAMES[$i]}' (pid=$pid)"
      kill "$pid" 2>/dev/null || true
      sleep 1
      kill -9 "$pid" 2>/dev/null || true
    fi
  done
}
trap cleanup EXIT INT TERM HUP

# ---------------------------------------------------------------------------
# spawn_instance INDEX — (re-)spawn supervisor_instance.sh for one entry
# ---------------------------------------------------------------------------
spawn_instance() {
  local idx="$1"
  local name="${CHILD_NAMES[$idx]}"
  local port="${CHILD_PORTS[$idx]}"
  local data_dir="${CHILD_DATA_DIRS[$idx]}"
  local root_dir="${CHILD_ROOT_DIRS[$idx]}"
  local backend="${CHILD_BACKENDS[$idx]}"
  local model="${CHILD_MODELS[$idx]}"
  local ollama_host="${CHILD_OLLAMA_HOSTS[$idx]}"

  mkdir -p "$data_dir"

  args=(--name "$name" --port "$port" --data-dir "$data_dir")
  if [[ -n "$root_dir" ]]; then
    args+=(--root-dir "$root_dir")
  fi
  if [[ -n "$backend" ]]; then
    args+=(--backend "$backend")
  fi
  if [[ -n "$model" ]]; then
    args+=(--model "$model")
  fi
  if [[ -n "$ollama_host" ]]; then
    args+=(--ollama-host "$ollama_host")
  fi

  "$BRIDGE_DIR/supervisor_instance.sh" "${args[@]}" &
  CHILD_PIDS[$idx]=$!
  echo "[supervisor] started instance '$name' on port $port (supervisor pid=${CHILD_PIDS[$idx]})"
}

# ---------------------------------------------------------------------------
# sync_instances — hot-reload instances.json; diff vs current arrays
# Called from monitor loop every poll cycle.
# ---------------------------------------------------------------------------
sync_instances() {
  local new_json
  new_json="$(BRIDGE_INSTANCES_CONFIG="$INSTANCES_CONFIG" python3 -c '
import json, sys, os
try:
    data = json.load(open(os.environ["BRIDGE_INSTANCES_CONFIG"]))
    for inst in data["instances"]:
        name = inst["name"]
        port = inst["port"]
        data_dir = os.path.expanduser(inst["data_dir"])
        root_dir = os.path.expanduser(inst.get("root_dir", ""))
        backend = inst.get("backend", "")
        model = inst.get("model", "")
        ollama_host = inst.get("ollama_host", "")
        print(f"{name}|{port}|{data_dir}|{root_dir}|{backend}|{model}|{ollama_host}")
except Exception as e:
    print("ERROR:" + str(e), file=sys.stderr)
    sys.exit(1)
' 2>/dev/null)" || {
    echo "[supervisor] sync_instances: failed to parse $INSTANCES_CONFIG, skipping hot-reload"
    return
  }

  # ---- Build lists of (name, line) from the fresh JSON ----
  local new_names=()
  local new_lines=()
  local line
  while IFS= read -r line; do
    [[ -z "$line" ]] && continue
    local n
    n="$(parse_field "$line" 1)"
    new_names+=("$n")
    new_lines+=("$line")
  done <<< "$new_json"

  # ---- Detect REMOVED or MODIFIED instances ----
  local i
  for (( i=0; i<${#CHILD_NAMES[@]}; i++ )); do
    local cur_name="${CHILD_NAMES[$i]}"
    [[ -z "$cur_name" ]] && continue  # already sentinel

    # Check if this name still exists in the new JSON
    local found_idx=-1
    local j
    for (( j=0; j<${#new_names[@]}; j++ )); do
      if [[ "${new_names[$j]}" == "$cur_name" ]]; then
        found_idx=$j
        break
      fi
    done

    if [[ $found_idx -eq -1 ]]; then
      # REMOVED: name no longer in instances.json
      echo "[supervisor] sync: instance '$cur_name' removed from config, stopping"
      kill_supervisor "${CHILD_PIDS[$i]}" "${CHILD_PORTS[$i]}"
      CHILD_NAMES[$i]=""
      CHILD_PIDS[$i]=""
    else
      # Check if port or root_dir changed (MODIFIED)
      local new_port new_root_dir
      new_port="$(parse_field "${new_lines[$found_idx]}" 2)"
      new_root_dir="$(parse_field "${new_lines[$found_idx]}" 4)"
      if [[ "$new_port" != "${CHILD_PORTS[$i]}" ]] || [[ "$new_root_dir" != "${CHILD_ROOT_DIRS[$i]}" ]]; then
        echo "[supervisor] sync: instance '$cur_name' config changed (port/root_dir), restarting"
        kill_supervisor "${CHILD_PIDS[$i]}" "${CHILD_PORTS[$i]}"
        # Sentinel the old slot
        CHILD_NAMES[$i]=""
        CHILD_PIDS[$i]=""
        # Append as new entry at the end
        local new_idx=${#CHILD_NAMES[@]}
        local new_line="${new_lines[$found_idx]}"
        CHILD_NAMES+=("$(parse_field "$new_line" 1)")
        CHILD_PORTS+=("$(parse_field "$new_line" 2)")
        CHILD_DATA_DIRS+=("$(parse_field "$new_line" 3)")
        CHILD_ROOT_DIRS+=("$(parse_field "$new_line" 4)")
        CHILD_BACKENDS+=("$(parse_field "$new_line" 5)")
        CHILD_MODELS+=("$(parse_field "$new_line" 6)")
        CHILD_OLLAMA_HOSTS+=("$(parse_field "$new_line" 7)")
        CHILD_PIDS+=("")
        # Check .bridge_state before spawning
        local new_data_dir
        new_data_dir="$(parse_field "$new_line" 3)"
        local state_file="$new_data_dir/.bridge_state"
        local state=""
        [[ -f "$state_file" ]] && state="$(cat "$state_file" 2>/dev/null | tr -d '[:space:]')"
        if [[ "$state" == "disabled" ]]; then
          echo "[supervisor] sync: instance '$(parse_field "$new_line" 1)' is disabled, not spawning"
        else
          spawn_instance "$new_idx"
        fi
      fi
    fi
  done

  # ---- Detect ADDED instances ----
  for (( j=0; j<${#new_names[@]}; j++ )); do
    local new_name="${new_names[$j]}"
    local already_present=0
    for (( i=0; i<${#CHILD_NAMES[@]}; i++ )); do
      if [[ "${CHILD_NAMES[$i]}" == "$new_name" ]]; then
        already_present=1
        break
      fi
    done
    if [[ $already_present -eq 0 ]]; then
      # ADDED: new instance not yet tracked
      local new_line="${new_lines[$j]}"
      local new_idx=${#CHILD_NAMES[@]}
      echo "[supervisor] sync: new instance '$new_name' detected, adding"
      CHILD_NAMES+=("$(parse_field "$new_line" 1)")
      CHILD_PORTS+=("$(parse_field "$new_line" 2)")
      CHILD_DATA_DIRS+=("$(parse_field "$new_line" 3)")
      CHILD_ROOT_DIRS+=("$(parse_field "$new_line" 4)")
      CHILD_BACKENDS+=("$(parse_field "$new_line" 5)")
      CHILD_MODELS+=("$(parse_field "$new_line" 6)")
      CHILD_OLLAMA_HOSTS+=("$(parse_field "$new_line" 7)")
      CHILD_PIDS+=("")
      local new_data_dir
      new_data_dir="$(parse_field "$new_line" 3)"
      local state_file="$new_data_dir/.bridge_state"
      local state=""
      [[ -f "$state_file" ]] && state="$(cat "$state_file" 2>/dev/null | tr -d '[:space:]')"
      if [[ "$state" == "disabled" ]]; then
        echo "[supervisor] sync: instance '$new_name' is disabled, not spawning"
      else
        spawn_instance "$new_idx"
      fi
    fi
  done
}

# ---------------------------------------------------------------------------
# Populate arrays and do the initial spawn
# ---------------------------------------------------------------------------
while IFS='|' read -r name port data_dir root_dir backend model ollama_host; do
  idx=${#CHILD_NAMES[@]}
  CHILD_NAMES+=("$name")
  CHILD_PORTS+=("$port")
  CHILD_DATA_DIRS+=("$data_dir")
  CHILD_ROOT_DIRS+=("$root_dir")
  CHILD_BACKENDS+=("$backend")
  CHILD_MODELS+=("$model")
  CHILD_OLLAMA_HOSTS+=("$ollama_host")
  CHILD_PIDS+=("")   # placeholder; filled by spawn_instance
  spawn_instance "$idx"
done <<< "$INSTANCES_JSON"

echo "[supervisor] ${#CHILD_NAMES[@]} instance(s) started"

# ---------------------------------------------------------------------------
# Monitor loop — poll every 5s; hot-reload instances.json; enforce .bridge_state;
# restart any exited instance supervisor.
# macOS bash 3.2 does NOT support wait -n, so we use polling.
# ---------------------------------------------------------------------------
while true; do
  sleep 5

  # ---- Hot-reload instances.json ----
  sync_instances

  # ---- Per-instance: enforce .bridge_state and crash-restart ----
  local_i=0
  for (( local_i=0; local_i<${#CHILD_PIDS[@]}; local_i++ )); do
    # Skip sentinel (deleted) entries
    [[ -z "${CHILD_NAMES[$local_i]}" ]] && continue

    local_name="${CHILD_NAMES[$local_i]}"
    local_pid="${CHILD_PIDS[$local_i]}"
    local_port="${CHILD_PORTS[$local_i]}"
    local_data_dir="${CHILD_DATA_DIRS[$local_i]}"

    # Read .bridge_state
    local_state_file="$local_data_dir/.bridge_state"
    local_state=""
    [[ -f "$local_state_file" ]] && local_state="$(cat "$local_state_file" 2>/dev/null | tr -d '[:space:]')"

    if [[ "$local_state" == "disabled" ]]; then
      # Kill if still running
      if [[ -n "$local_pid" ]] && kill -0 "$local_pid" 2>/dev/null; then
        echo "[supervisor] instance '$local_name' marked disabled, stopping (pid=$local_pid)"
        kill_supervisor "$local_pid" "$local_port"
        CHILD_PIDS[$local_i]=""
      fi
      # Do not restart — continue to next instance
      continue
    fi

    # enabled (or no state file) — apply crash-restart logic
    if [[ -z "$local_pid" ]] || ! kill -0 "$local_pid" 2>/dev/null; then
      echo "[supervisor] instance '$local_name' supervisor exited (pid=${local_pid:-?}), restarting in 5s"
      sleep 5
      spawn_instance "$local_i"
    fi
  done
done
