#!/bin/bash
BRIDGE_DIR="$(cd "$(dirname "$0")" && pwd)"
exec "$BRIDGE_DIR/venv/bin/python" "$BRIDGE_DIR/claude_bridge_v2.py" "$@"
