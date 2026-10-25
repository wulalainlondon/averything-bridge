#!/usr/bin/env python3
"""
everything-go bridge monitor dashboard
Usage: python3 monitor.py [port]   (default port: 7788)
"""

import os, sys, time, json, subprocess, threading, webbrowser
from http.server import ThreadingHTTPServer, BaseHTTPRequestHandler
from pathlib import Path

BRIDGE_PORT  = 8766
LABEL        = "com.everything-go.app"
STDERR_LOG   = "/tmp/com.everything-go.app.stderr.log"
MONITOR_PORT = int(sys.argv[1]) if len(sys.argv) > 1 else 7788

# ── HTML / CSS / JS ──────────────────────────────────────────────────────────

HTML = """<!DOCTYPE html>
<html lang="zh-TW">
<head>
<meta charset="UTF-8">
<title>everything-go monitor</title>
<style>
*{margin:0;padding:0;box-sizing:border-box}
body{background:#0a0f1e;color:#e2e8f0;font-family:'SF Mono','Fira Code',monospace;padding:28px;min-height:100vh}

h1{font-size:.75rem;font-weight:600;color:#475569;letter-spacing:.12em;text-transform:uppercase;margin-bottom:20px}

.top{display:flex;align-items:center;justify-content:space-between;margin-bottom:24px}
.status-badge{display:flex;align-items:center;gap:10px}
.dot{width:11px;height:11px;border-radius:50%;flex-shrink:0;transition:background .4s,box-shadow .4s}
.dot.running{background:#22c55e;box-shadow:0 0 10px #22c55e99;animation:pulse 2s infinite}
.dot.stopped{background:#ef4444;box-shadow:0 0 10px #ef444488}
@keyframes pulse{0%,100%{box-shadow:0 0 8px #22c55e88}50%{box-shadow:0 0 16px #22c55ecc}}
.status-label{font-size:1.4rem;font-weight:800;letter-spacing:.04em;transition:color .4s}
.status-label.running{color:#22c55e}
.status-label.stopped{color:#ef4444}
.launchd-badge{font-size:.7rem;padding:3px 8px;border-radius:4px;background:#1e293b;color:#64748b;border:1px solid #334155}

.cards{display:grid;grid-template-columns:repeat(4,1fr);gap:12px;margin-bottom:20px}
.card{background:#111827;border:1px solid #1e293b;border-radius:10px;padding:16px 18px}
.card-label{font-size:.65rem;color:#4b5563;text-transform:uppercase;letter-spacing:.1em;margin-bottom:8px}
.card-value{font-size:1.35rem;font-weight:700;color:#f1f5f9;font-variant-numeric:tabular-nums}
.card-value.ok{color:#22c55e}
.card-value.err{color:#ef4444}
.card-value.dim{color:#64748b}

.actions{display:flex;gap:10px;margin-bottom:14px;align-items:center}
.btn{padding:7px 16px;border:none;border-radius:7px;cursor:pointer;font-size:.82rem;font-weight:700;font-family:inherit;transition:all .15s;letter-spacing:.02em}
.btn:active{transform:scale(.96)}
.btn-restart{background:#f59e0b;color:#000}
.btn-restart:hover{background:#fbbf24}
.btn-restart:disabled{background:#78350f;color:#92400e;cursor:not-allowed}
.btn-clear{background:#1e293b;color:#94a3b8;border:1px solid #334155}
.btn-clear:hover{background:#273548}
.scroll-lock{display:flex;align-items:center;gap:6px;font-size:.72rem;color:#475569;margin-left:auto;cursor:pointer;user-select:none}
.scroll-lock input{cursor:pointer;accent-color:#6366f1}

.log-wrap{background:#060c18;border:1px solid #1a2236;border-radius:10px;overflow:hidden}
.log-header{display:flex;align-items:center;justify-content:space-between;padding:8px 14px;background:#0d1526;border-bottom:1px solid #1a2236}
.log-title{font-size:.65rem;color:#374151;text-transform:uppercase;letter-spacing:.1em}
.log-count{font-size:.65rem;color:#374151}
#log-box{height:430px;overflow-y:auto;padding:10px 14px;scroll-behavior:smooth}
.log-line{font-size:.76rem;line-height:1.75;white-space:pre-wrap;word-break:break-all}
.log-line.conn-in{color:#34d399}
.log-line.conn-out{color:#f59e0b}
.log-line.conn{color:#60a5fa}
.log-line.fcm-err{color:#f87171}
.log-line.fcm{color:#fb923c}
.log-line.search{color:#a78bfa}
.log-line.media{color:#22d3ee}
.log-line.ws-err{color:#f87171}
.log-line.monitor{color:#6366f1;font-style:italic}
.log-line.other{color:#4b5563}

.footer{margin-top:10px;font-size:.65rem;color:#1e293b;display:flex;gap:16px}

::-webkit-scrollbar{width:5px}
::-webkit-scrollbar-track{background:#060c18}
::-webkit-scrollbar-thumb{background:#1e293b;border-radius:3px}
::-webkit-scrollbar-thumb:hover{background:#334155}
</style>
</head>
<body>

<h1>EVERYTHING-GO · BRIDGE MONITOR</h1>

<div class="top">
  <div class="status-badge">
    <div id="dot" class="dot stopped"></div>
    <span id="status-label" class="status-label stopped">STOPPED</span>
  </div>
  <span id="launchd-badge" class="launchd-badge">exit —</span>
</div>

<div class="cards">
  <div class="card">
    <div class="card-label">PID</div>
    <div class="card-value dim" id="c-pid">—</div>
  </div>
  <div class="card">
    <div class="card-label">Uptime</div>
    <div class="card-value dim" id="c-uptime">—</div>
  </div>
  <div class="card">
    <div class="card-label">Clients</div>
    <div class="card-value dim" id="c-clients">—</div>
  </div>
  <div class="card">
    <div class="card-label">Port 8766</div>
    <div class="card-value dim" id="c-port">—</div>
  </div>
</div>

<div class="actions">
  <button class="btn btn-restart" id="btn-restart" onclick="restart()">⟳ Restart</button>
  <button class="btn btn-clear" onclick="clearLogs()">Clear</button>
  <label class="scroll-lock">
    <input type="checkbox" id="chk-scroll" checked onchange="autoScroll=this.checked">
    Auto-scroll
  </label>
</div>

<div class="log-wrap">
  <div class="log-header">
    <span class="log-title">stderr log · live stream</span>
    <span class="log-count" id="log-count">0 lines</span>
  </div>
  <div id="log-box"></div>
</div>

<div class="footer">
  <span>Refreshing every 3s</span>
  <span>/tmp/com.everything-go.app.stderr.log</span>
</div>

<script>
const logBox = document.getElementById('log-box');
let autoScroll = true;
let lineCount = 0;
const MAX_LINES = 800;

function classify(line) {
  if (line.includes('[monitor]'))                                   return 'monitor';
  if (line.includes('[conn]') && line.includes('connected') && !line.includes('disconnected')) return 'conn-in';
  if (line.includes('[conn]') && line.includes('disconnected'))     return 'conn-out';
  if (line.includes('[conn]'))                                      return 'conn';
  if (line.includes('[fcm]') && (line.includes('error') || line.includes('Error'))) return 'fcm-err';
  if (line.includes('[fcm]'))                                       return 'fcm';
  if (line.includes('[search]'))                                    return 'search';
  if (line.includes('[media]'))                                     return 'media';
  if (line.includes('ws accept error') || line.includes('error:')) return 'ws-err';
  return 'other';
}

function appendLine(text) {
  if (!text.trim()) return;
  if (lineCount >= MAX_LINES) {
    logBox.firstChild && logBox.removeChild(logBox.firstChild);
    lineCount--;
  }
  const el = document.createElement('div');
  el.className = 'log-line ' + classify(text);
  el.textContent = text;
  logBox.appendChild(el);
  lineCount++;
  document.getElementById('log-count').textContent = lineCount + ' lines';
  if (autoScroll) logBox.scrollTop = logBox.scrollHeight;
}

function clearLogs() {
  logBox.innerHTML = '';
  lineCount = 0;
  document.getElementById('log-count').textContent = '0 lines';
}

logBox.addEventListener('scroll', () => {
  const atBottom = logBox.scrollTop + logBox.clientHeight >= logBox.scrollHeight - 40;
  autoScroll = atBottom;
  document.getElementById('chk-scroll').checked = atBottom;
});

async function fetchStatus() {
  try {
    const d = await (await fetch('/api/status')).json();
    const running = d.running;

    document.getElementById('dot').className = 'dot ' + (running ? 'running' : 'stopped');
    const sl = document.getElementById('status-label');
    sl.className = 'status-label ' + (running ? 'running' : 'stopped');
    sl.textContent = running ? 'RUNNING' : 'STOPPED';

    const badge = document.getElementById('launchd-badge');
    badge.textContent = 'exit ' + (d.launchd_exit ?? '—');
    badge.style.color = d.launchd_exit === '0' ? '#22c55e' : (d.launchd_exit ? '#ef4444' : '#64748b');

    const setCard = (id, val, cls) => {
      const el = document.getElementById(id);
      el.textContent = val;
      el.className = 'card-value ' + (cls || (val === '—' ? 'dim' : ''));
    };
    setCard('c-pid',     d.pid     || '—');
    setCard('c-uptime',  d.uptime  || '—');
    setCard('c-clients', d.clients !== null ? String(d.clients) : '—');
    setCard('c-port', d.port_ok ? 'LISTEN' : 'DOWN', d.port_ok ? 'ok' : 'err');
  } catch(e) {}
}

async function restart() {
  const btn = document.getElementById('btn-restart');
  btn.textContent = '⟳ Restarting…';
  btn.disabled = true;
  try {
    await fetch('/api/restart', { method: 'POST' });
    appendLine('[monitor] restart triggered — waiting for process…');
  } catch(e) {}
  setTimeout(() => {
    btn.textContent = '⟳ Restart';
    btn.disabled = false;
    fetchStatus();
  }, 4000);
}

// SSE log stream
function connectSSE() {
  const sse = new EventSource('/api/logs');
  sse.onmessage = e => appendLine(e.data);
  sse.onerror = () => {
    sse.close();
    appendLine('[monitor] log stream disconnected — reconnecting in 3s…');
    setTimeout(connectSSE, 3000);
  };
}

fetchStatus();
setInterval(fetchStatus, 3000);
connectSSE();
</script>
</body>
</html>"""

# ── Backend ───────────────────────────────────────────────────────────────────

def _run(*cmd):
    try:
        return subprocess.run(cmd, capture_output=True, text=True, timeout=5)
    except Exception:
        return None


def get_status():
    # Find the PID that's binding BRIDGE_PORT (not dev test runs)
    pid = None
    r = _run("pgrep", "-x", "everything-go")
    if r and r.stdout.strip():
        for p in r.stdout.strip().split():
            r2 = _run("ps", "-p", p, "-o", "args=")
            if r2 and f"--port {BRIDGE_PORT}" in r2.stdout:
                pid = int(p)
                break
        # fallback: pick first if none had explicit --port
        if pid is None and r.stdout.strip():
            try:
                pid = int(r.stdout.strip().split()[0])
            except Exception:
                pass

    uptime, clients, port_ok = "—", 0, False
    if pid:
        r = _run("ps", "-p", str(pid), "-o", "etime=")
        if r:
            uptime = r.stdout.strip() or "—"
        r = _run("lsof", "-Pan", "-p", str(pid), "-i")
        if r:
            lines = r.stdout.splitlines()
            clients = sum(1 for l in lines if "ESTABLISHED" in l)
            port_ok = any(f":{BRIDGE_PORT}" in l and "LISTEN" in l for l in lines)

    launchd_exit = None
    r = _run("launchctl", "list")
    if r:
        for line in r.stdout.splitlines():
            if LABEL in line:
                parts = line.split()
                launchd_exit = parts[1] if len(parts) >= 2 else None
                break

    return {
        "running":      pid is not None and port_ok,
        "pid":          pid,
        "uptime":       uptime,
        "clients":      clients,
        "port_ok":      port_ok,
        "launchd_exit": launchd_exit,
    }


def do_restart():
    r = _run("launchctl", "kickstart", "-k", f"gui/{os.getuid()}/{LABEL}")
    return r is not None and r.returncode == 0


# ── HTTP Handler ──────────────────────────────────────────────────────────────

class Handler(BaseHTTPRequestHandler):

    def do_GET(self):
        if self.path == "/":
            self._send(200, "text/html; charset=utf-8", HTML.encode())
        elif self.path == "/api/status":
            self._send(200, "application/json", json.dumps(get_status()).encode())
        elif self.path == "/api/logs":
            self._stream_logs()
        else:
            self.send_error(404)

    def do_POST(self):
        if self.path == "/api/restart":
            ok = do_restart()
            self._send(200, "application/json", json.dumps({"ok": ok}).encode())
        else:
            self.send_error(404)

    def _send(self, code, ctype, data):
        self.send_response(code)
        self.send_header("Content-Type", ctype)
        self.send_header("Content-Length", str(len(data)))
        self.end_headers()
        self.wfile.write(data)

    def _stream_logs(self):
        self.send_response(200)
        self.send_header("Content-Type", "text/event-stream")
        self.send_header("Cache-Control", "no-cache")
        self.send_header("Connection", "keep-alive")
        self.end_headers()

        log = Path(STDERR_LOG)
        try:
            # Wait for log file if missing
            for _ in range(20):
                if log.exists():
                    break
                self._sse("[monitor] waiting for log file…")
                time.sleep(1)

            with log.open("r", errors="replace") as f:
                # Send last 120 lines as history
                all_lines = f.readlines()
                for line in all_lines[-120:]:
                    self._sse(line.rstrip())

                # Tail new lines
                while True:
                    line = f.readline()
                    if line:
                        self._sse(line.rstrip())
                    else:
                        time.sleep(0.25)

        except (BrokenPipeError, ConnectionResetError, OSError):
            pass

    def _sse(self, text):
        if not text:
            return
        self.wfile.write(f"data: {text}\n\n".encode())
        self.wfile.flush()

    def log_message(self, *_):
        pass  # suppress access logs


# ── Entry point ───────────────────────────────────────────────────────────────

if __name__ == "__main__":
    url = f"http://127.0.0.1:{MONITOR_PORT}"
    print(f"[monitor] bridge monitor → {url}")
    print(f"[monitor] Press Ctrl-C to stop")
    server = ThreadingHTTPServer(("127.0.0.1", MONITOR_PORT), Handler)
    threading.Timer(0.7, lambda: webbrowser.open(url)).start()
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        print("\n[monitor] stopped")
