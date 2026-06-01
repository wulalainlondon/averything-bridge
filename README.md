# averything-bridge

Control desktop AI runtimes from the Averything mobile app.

If you are an AI coding agent: follow [AGENTS.md](AGENTS.md) exactly. Do not stop after cloning. Install, start the bridge, and report the final connection URL.

## AI Agent Install

Expected result:

- Bridge dependencies installed.
- Bridge running on port `8766`.
- Bridge auto-start configured where supported.
- Final response includes `ws://<local-ip>:8766`.
- Mobile app on the same Wi-Fi can discover the bridge automatically.

Run the repo-specific agent instructions:

```text
Clone https://github.com/wulalainlondon/averything-bridge and follow AGENTS.md.
```

## Mobile App

Latest Android APK:

```text
https://github.com/BridgeAverthing/averything-app/releases/latest/download/averything-v1.2.4.apk
```

After installing the app, keep the phone and computer on the same Wi-Fi. The app should find the bridge automatically. Manual connection URL format is:

```text
ws://<local-ip>:8766
```

## What This Runs

```text
Phone app --WebSocket--> bridge on your computer --subprocess/API--> Claude, Codex, or Ollama
```

The bridge manages sessions, streams responses to the phone in real time, restores reconnects, buffers offline events, exposes local files/media, and can send optional push notifications when long tasks finish.

Supported desktop runtimes:

- Claude CLI: `npm install -g @anthropic-ai/claude-code`
- Codex CLI
- Ollama with a local model

## Manual Fallback

Use this only if an AI agent is not doing the install.

macOS / Linux:

```bash
git clone https://github.com/wulalainlondon/averything-bridge
cd averything-bridge
chmod +x install_oneclick.sh
./install_oneclick.sh
```

Windows PowerShell:

```powershell
git clone https://github.com/wulalainlondon/averything-bridge
cd averything-bridge
powershell -ExecutionPolicy Bypass -File .\install_windows.ps1
powershell -ExecutionPolicy Bypass -File .\install_windows_startup.ps1
```

Manual foreground run:

```bash
python3 -m venv venv
venv/bin/pip install -r requirements.txt
venv/bin/python bridge_v2.py --port 8766
```

Windows foreground run:

```powershell
py -3 -m venv venv
.\venv\Scripts\python -m pip install -r requirements.txt
.\venv\Scripts\python bridge_v2.py --port 8766
```

## Backend Options

Claude is the default backend:

```bash
venv/bin/python bridge_v2.py --port 8766 --backend claude
```

Codex:

```bash
venv/bin/python bridge_v2.py --port 8766 --backend codex
```

Ollama:

```bash
venv/bin/python bridge_v2.py --port 8766 --backend ollama --model llama3.2
```

Windows installer backend flags:

```powershell
powershell -ExecutionPolicy Bypass -File .\install_windows.ps1 -Backend codex
powershell -ExecutionPolicy Bypass -File .\install_windows.ps1 -Backend ollama -OllamaModel llama3.2
```

## Connection Options

| Method | URL format | Notes |
|--------|------------|-------|
| Same Wi-Fi | `ws://192.168.x.x:8766` | Fastest; app discovery should find it automatically |
| Tailscale | `ws://100.x.x.x:8766` | Works across networks |
| Cloudflare tunnel | `wss://xxx.trycloudflare.com` | Start bridge with `--tunnel` |

## Optional Security

Set a shared token before starting the bridge:

```bash
export BRIDGE_AUTH_TOKEN="replace-with-a-long-random-string"
```

Windows:

```powershell
$env:BRIDGE_AUTH_TOKEN="replace-with-a-long-random-string"
```

When set, the mobile app must include the token in its first `hello` message.

## Optional Push Notifications

Push notifications need a Firebase service account key at:

```text
~/.config/claude-bridge/serviceAccountKey.json
```

or:

```bash
export SERVICE_ACCOUNT_FILE=/path/to/serviceAccountKey.json
```

The bridge works without Firebase; push notifications will simply be disabled.

## Development

Before releases or behavior-sensitive changes, run:

```bash
python3 -m pytest
```

Release checklist:

- [RELEASE_CHECKLIST.md](RELEASE_CHECKLIST.md)

## Contact

Questions or feedback: open an issue or email `wulalainlondon@gmail.com`.

## License

MIT
