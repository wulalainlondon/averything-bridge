#!/usr/bin/env bash
set -euo pipefail

# One-click installer for macOS/Linux.
# - Checks Python/Node
# - Creates venv + installs Python deps
# - Optionally installs Claude CLI
# - Optionally registers macOS launchd service

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$ROOT_DIR"

echo "==> claude-bridge one-click install (macOS/Linux)"

if ! command -v python3 >/dev/null 2>&1; then
  echo "ERROR: python3 not found. Install Python 3.10+ first."
  exit 1
fi
PY_VER="$(python3 -c 'import sys; print(f"{sys.version_info.major}.{sys.version_info.minor}")')"
echo "Python: $PY_VER"

if ! command -v npm >/dev/null 2>&1; then
  echo "WARNING: npm not found. Claude/Codex CLI auto-install will be skipped."
fi

if [[ ! -d venv ]]; then
  echo "==> Creating virtualenv"
  python3 -m venv venv
fi

echo "==> Installing Python dependencies"
venv/bin/python -m pip install --upgrade pip
venv/bin/python -m pip install -r requirements.txt

if [[ "${INSTALL_CLAUDE_CLI:-1}" == "1" ]]; then
  if command -v npm >/dev/null 2>&1; then
    if ! command -v claude >/dev/null 2>&1; then
      echo "==> Installing Claude CLI"
      npm install -g @anthropic-ai/claude-code
    else
      echo "Claude CLI already installed"
    fi
  fi
fi

if [[ "$(uname -s)" == "Darwin" ]]; then
  if [[ "${INSTALL_LAUNCHD:-1}" == "1" ]]; then
    echo "==> Installing macOS launchd service"
    bash install.sh
  else
    echo "Skipping launchd install (INSTALL_LAUNCHD=0)"
  fi
fi

echo ""
echo "Done."
echo "Start command:"
echo "  venv/bin/python bridge_v2.py --port 8766 --backend claude"
