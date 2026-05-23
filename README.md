# averything-bridge

Control Claude, Codex, or Ollama on your Mac from your phone.

**[Download Android app (v1.1.0)](https://github.com/wulalainlondon/averything-bridge/releases/latest/download/averything-v1.1.0.apk)**

For AI agents/Codex auto-setup instructions, see `AGENTS.md`.

This is the server-side bridge. It runs on your Mac and connects your mobile app to local AI runtimes via WebSocket. I built this because I wanted to use Claude from my phone while the actual computation runs on my Mac — and I use it every day.

## How it works

```
Phone App  ──WebSocket──  bridge (your Mac)  ──subprocess──  Claude / Codex / Ollama
```

The bridge manages sessions, streams responses back to your phone in real time, and handles reconnects, offline buffering, and push notifications when a long task finishes.

## Requirements

- macOS (Linux/Windows also supported; auto-start script is macOS-only)
- Python 3.10+
- At least one of:
  - [Claude CLI](https://claude.ai/download) (`npm install -g @anthropic-ai/claude-code`)
  - [Ollama](https://ollama.com) with a model pulled
  - Codex CLI

## Quick start

```bash
git clone https://github.com/wulalainlondon/averything-bridge
cd averything-bridge
python3 -m venv venv
venv/bin/pip install -r requirements.txt
venv/bin/python bridge_v2.py --port 8766
```

Windows (PowerShell):

```powershell
py -3 -m venv venv
.\venv\Scripts\python -m pip install -r requirements.txt
.\venv\Scripts\python bridge_v2.py --port 8766
```

Then open the companion app on your phone and point it at your Mac's IP.

## One-click install

macOS / Linux:

```bash
chmod +x install_oneclick.sh
./install_oneclick.sh
```

Windows (PowerShell):

```powershell
powershell -ExecutionPolicy Bypass -File .\install_windows.ps1
```

Optional Windows flags:

```powershell
# Codex backend
powershell -ExecutionPolicy Bypass -File .\install_windows.ps1 -Backend codex

# Ollama backend
powershell -ExecutionPolicy Bypass -File .\install_windows.ps1 -Backend ollama -OllamaModel llama3.2
```

## Connection options

| Method | URL format | Notes |
|--------|-----------|-------|
| Local network | `ws://192.168.x.x:8766` | Fastest |
| Tailscale | `ws://100.x.x.x:8766` | Works across networks |
| Cloudflare tunnel | `wss://xxx.trycloudflare.com` | Public, no setup needed |

For Cloudflare tunnel, start with `--tunnel` flag. The URL will appear in the logs.

## Basic security (recommended)

Set a shared auth token before starting bridge:

macOS/Linux:

```bash
export BRIDGE_AUTH_TOKEN="replace-with-a-long-random-string"
```

Windows (PowerShell):

```powershell
$env:BRIDGE_AUTH_TOKEN="replace-with-a-long-random-string"
```

When set, the first client message (`hello`) must include `auth_token`, otherwise the connection is rejected.

## Backends

```bash
# Claude CLI (default)
venv/bin/python bridge_v2.py --port 8766

# Ollama
venv/bin/python bridge_v2.py --port 8766 --backend ollama --model llama3.2

# Codex
venv/bin/python bridge_v2.py --port 8766 --backend codex
```

## Auto-start

macOS (launchd):

```bash
bash install.sh
```

This installs a launchd agent so the bridge starts automatically on login and restarts if it crashes.

Before each release/update, run the mandatory gate checklist:
- [RELEASE_CHECKLIST.md](RELEASE_CHECKLIST.md)

Windows (Task Scheduler):

```powershell
powershell -ExecutionPolicy Bypass -File .\install_windows_startup.ps1
```

Optional backend selection:

```powershell
powershell -ExecutionPolicy Bypass -File .\install_windows_startup.ps1 -Backend codex
powershell -ExecutionPolicy Bypass -File .\install_windows_startup.ps1 -Backend ollama -OllamaModel llama3.2
```

## Push notifications (optional)

If you want push notifications when Claude finishes a long task, set up Firebase:

```bash
# Place your Firebase service account key at:
~/.config/claude-bridge/serviceAccountKey.json

# Or set the path via env:
export SERVICE_ACCOUNT_FILE=/path/to/serviceAccountKey.json
```

See [docs/FIREBASE_SETUP.md](docs/FIREBASE_SETUP.md) for the full setup guide.

## Companion app

→ [averything-app](https://github.com/BridgeAverthing/averything-app) — Android/iOS app source
→ [Download latest APK](https://github.com/wulalainlondon/averything-bridge/releases/latest)

## License

MIT
