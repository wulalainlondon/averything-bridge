#!/bin/bash
# Test the Claude Bridge WebSocket server.
# Tries wscat first; falls back to the venv Python websockets client.

BRIDGE_DIR="/Users/wulala/Downloads/Helper/claude-bridge/bridge"
WS_URL="ws://127.0.0.1:8765"

# Pick python: prefer venv, fall back to system python3
if [ -x "$BRIDGE_DIR/venv/bin/python" ]; then
    PYTHON="$BRIDGE_DIR/venv/bin/python"
elif command -v python3 &>/dev/null; then
    PYTHON="python3"
else
    echo "ERROR: No python3 found."
    exit 1
fi

echo "==> Claude Bridge smoke-test"
echo "    Target: $WS_URL"
echo "    Python: $PYTHON"
echo ""

# ---- helper: Python websockets test ----------------------------------------
run_python_test() {
    "$PYTHON" - <<'PYEOF'
import asyncio, json, sys
import websockets

async def test():
    url = "ws://127.0.0.1:8765"
    try:
        async with websockets.connect(url, open_timeout=5) as ws:
            # --- ping/pong ---
            await ws.send(json.dumps({"type": "ping"}))
            resp = json.loads(await asyncio.wait_for(ws.recv(), timeout=5))
            assert resp.get("type") == "pong", f"Expected pong, got {resp}"
            print("  [PASS] ping -> pong")
            print("\nAll tests passed.")
    except ConnectionRefusedError:
        print("ERROR: Bridge is not running on port 8765.", file=sys.stderr)
        print("  Start with: cd /Users/wulala/Downloads/Helper/claude-bridge/bridge", file=sys.stderr)
        print("              source venv/bin/activate && python claude_bridge.py --port 8765", file=sys.stderr)
        sys.exit(1)
    except Exception as e:
        print(f"ERROR: {e}", file=sys.stderr)
        sys.exit(1)

asyncio.run(test())
PYEOF
}

# ---- check if bridge is already running ------------------------------------
if ! lsof -iTCP:8765 -sTCP:LISTEN &>/dev/null; then
    echo "Bridge is not running on port 8765."
    echo "Starting bridge in background for test..."
    nohup "$BRIDGE_DIR/venv/bin/python" "$BRIDGE_DIR/claude_bridge.py" --port 8765 \
        >> "$BRIDGE_DIR/bridge.log" 2>> "$BRIDGE_DIR/bridge.err" &
    STARTED_PID=$!
    sleep 2
fi

# ---- try wscat first -------------------------------------------------------
if command -v wscat &>/dev/null; then
    echo "wscat found — running ping test..."
    echo '{"type":"ping"}' | timeout 5 wscat --connect "$WS_URL" --no-color 2>&1 | head -20
    echo ""
    echo "wscat output above should contain: {\"type\":\"pong\"}"
    echo ""
    echo "Running Python assertion check..."
    run_python_test
else
    echo "wscat not found — using Python websockets client."
    echo ""
    run_python_test
fi

# ---- stop bridge if we started it -----------------------------------------
if [ -n "$STARTED_PID" ]; then
    kill "$STARTED_PID" 2>/dev/null
    echo ""
    echo "(Stopped test bridge instance PID $STARTED_PID)"
fi
