#!/bin/bash
BRIDGE_DIR="/Users/wulala/Downloads/Helper/claude-bridge/bridge"
exec "$BRIDGE_DIR/venv/bin/python" "$BRIDGE_DIR/claude_bridge.py" "$@"
