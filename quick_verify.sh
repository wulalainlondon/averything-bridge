#!/usr/bin/env bash
# quick_verify.sh — smoke-test 5-bug patch
set -euo pipefail
PASS=0; FAIL=0

ok()  { echo "[PASS] $*"; PASS=$((PASS+1)); }
fail(){ echo "[FAIL] $*"; FAIL=$((FAIL+1)); }

# 1. Bridge health
lsof -ti :8766 >/dev/null 2>&1 && ok "BUG-00b/bridge: port 8766 open" || fail "port 8766 not open"

# 2. BUG-00b: DEFAULT_CWD respects BRIDGE_DEFAULT_CWD env
cd "$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
got=$(BRIDGE_DEFAULT_CWD=/tmp venv/bin/python -c "
import importlib, sys, os
os.environ['BRIDGE_DEFAULT_CWD'] = '/tmp'
import bridge_v2
print(bridge_v2.DEFAULT_CWD)
" 2>/dev/null || true)
[[ "$got" == "/tmp" ]] && ok "BUG-00b: BRIDGE_DEFAULT_CWD=/tmp respected" || fail "BUG-00b: got '$got'"

# 3. BUG-00c: _persist_session writes both keys
got=$(venv/bin/python - <<'EOF'
import json, tempfile, os, sys
sys.path.insert(0, ".")
os.environ.setdefault("BRIDGE_DEFAULT_CWD", "")
import bridge_v2
from dataclasses import dataclass

# Build a minimal fake session and persist it
import time, uuid, bridge_v2
sid = str(uuid.uuid4())
bridge_v2.SAVED_SESSIONS_FILE = f"/tmp/test_sessions_{sid[:8]}.json"
session = bridge_v2.Session(
    session_id=sid,
    name="test",
    created_at=0.0,
    cwd="/tmp",
    backend_name="claude",
    model="",
    sandbox="danger-full-access",
    image_dir="",
)
session.resume_id = "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee"
bridge_v2._persist_session(session)
data = json.load(open(bridge_v2.SAVED_SESSIONS_FILE))
entry = data[sid]
assert entry.get("resume_id") == session.resume_id, f"missing resume_id: {entry}"
assert entry.get("claude_uuid") == session.resume_id, f"missing claude_uuid: {entry}"
print("ok")
EOF
)
[[ "$got" == "ok" ]] && ok "BUG-00c: both resume_id + claude_uuid written" || fail "BUG-00c: $got"

# 4. BUG-00d: _find_newest_jsonl_uuid returns None for nonexistent cwd (no crash)
got=$(venv/bin/python - <<'EOF'
import sys; sys.path.insert(0, ".")
import os; os.environ.setdefault("BRIDGE_DEFAULT_CWD", "")
from backends.claude_cli import ClaudeCliBackend
b = ClaudeCliBackend(claude_projects_dir=os.path.expanduser("~/.claude/projects"))
r = b._find_newest_jsonl_uuid("/nonexistent/path/xyz")
print("none" if r is None else r)
EOF
)
[[ "$got" == "none" ]] && ok "BUG-00d: _find_newest_jsonl_uuid returns None for missing cwd" || fail "BUG-00d: got '$got'"

# 5. BUG-07: upsert_session_metadata exists and is callable
got=$(venv/bin/python - <<'EOF'
import sys; sys.path.insert(0, ".")
from search.ingest.worker import IngestWorker
w = IngestWorker.__new__(IngestWorker)
w._conn = None
w.upsert_session_metadata(
    session_id="aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee",
    source="claude",
    cwd="/tmp",
    display_name="test session",
)
print("ok")
EOF
)
[[ "$got" == "ok" ]] && ok "BUG-07: upsert_session_metadata callable (no-op when conn=None)" || fail "BUG-07: $got"

# 6. All existing tests still pass
cd "$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
venv/bin/python -m pytest tests/ -q --tb=short 2>&1 | tail -3
pass_line=$(venv/bin/python -m pytest tests/ -q --tb=short 2>&1 | grep -E "passed")
[[ "$pass_line" == *"passed"* ]] && ok "pytest: $pass_line" || fail "pytest: $pass_line"

echo ""
echo "Result: $PASS passed, $FAIL failed"
[[ $FAIL -eq 0 ]] && exit 0 || exit 1
