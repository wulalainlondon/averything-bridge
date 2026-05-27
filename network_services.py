"""HTTP media, mDNS, and Cloudflare tunnel helpers."""
from __future__ import annotations

import asyncio
import http
import json
import mimetypes
import os
import re
import shutil
from collections.abc import Awaitable, Callable
from typing import Any
from urllib.parse import unquote

from websockets.datastructures import Headers as WsHeaders
from websockets.http11 import Response as WsResponse

try:
    import socket
    from zeroconf import NonUniqueNameException, ServiceInfo, Zeroconf
    _ZEROCONF_AVAILABLE = True
except ImportError:
    socket = None  # type: ignore[assignment]
    ServiceInfo = None  # type: ignore[assignment]
    Zeroconf = None  # type: ignore[assignment]
    _ZEROCONF_AVAILABLE = False


_ALLOWED_MEDIA_EXTS = {".jpg", ".jpeg", ".png", ".gif", ".webp", ".mp4", ".mov", ".m4v", ".avi"}

_root_dir = ""
_instance_id = ""
_log: Any = None
_broadcast_json: Callable[[dict], Awaitable[int]] | None = None
_spawn_task: Callable[[str, Awaitable[Any]], Any] | None = None
_notify_tunnel_with_retry: Callable[[str], Awaitable[None]] | None = None
_set_media_base_url: Callable[[str], None] | None = None
_cloudflared_proc: asyncio.subprocess.Process | None = None
_current_tunnel_url: str | None = None
_tunnel_retry_task: Any = None
_tunnel_url_delivered = False


def configure(
    *,
    root_dir: str,
    instance_id: str,
    log: Any,
    broadcast_json: Callable[[dict], Awaitable[int]],
    spawn_task: Callable[[str, Awaitable[Any]], Any],
    notify_tunnel_with_retry: Callable[[str], Awaitable[None]],
    set_media_base_url: Callable[[str], None],
) -> None:
    global _root_dir, _instance_id, _log, _broadcast_json, _spawn_task
    global _notify_tunnel_with_retry, _set_media_base_url
    _root_dir = root_dir
    _instance_id = instance_id
    _log = log
    _broadcast_json = broadcast_json
    _spawn_task = spawn_task
    _notify_tunnel_with_retry = notify_tunnel_with_retry
    _set_media_base_url = set_media_base_url


def _info(message: str, *args: Any) -> None:
    if _log is not None:
        _log.info(message, *args)


def _warning(message: str, *args: Any) -> None:
    if _log is not None:
        _log.warning(message, *args)


def get_current_tunnel_url() -> str | None:
    return _current_tunnel_url


def set_current_tunnel_url(url: str | None) -> None:
    global _current_tunnel_url
    _current_tunnel_url = url


def is_tunnel_url_delivered() -> bool:
    return _tunnel_url_delivered


def mark_tunnel_url_delivered() -> None:
    global _tunnel_url_delivered, _tunnel_retry_task
    _tunnel_url_delivered = True
    if _tunnel_retry_task and not _tunnel_retry_task.done():
        _tunnel_retry_task.cancel()
    _tunnel_retry_task = None


async def media_request_handler(connection, request):
    if not request.path.startswith("/media/"):
        return None
    file_path = unquote(request.path[6:])
    ext = os.path.splitext(file_path)[1].lower()
    if ext not in _ALLOWED_MEDIA_EXTS:
        return connection.respond(http.HTTPStatus.FORBIDDEN, "Forbidden\n")
    real_path = os.path.realpath(file_path)
    if _root_dir:
        from utils.path_jail import JailEscape, resolve_jailed
        try:
            resolve_jailed(file_path, _root_dir)
        except JailEscape:
            return connection.respond(
                http.HTTPStatus.FORBIDDEN,
                json.dumps({"error": "path outside instance root"}) + "\n",
            )
    if not os.path.isfile(real_path):
        return connection.respond(http.HTTPStatus.NOT_FOUND, "Not found\n")
    try:
        loop = asyncio.get_running_loop()
        data = await loop.run_in_executor(None, lambda: open(real_path, "rb").read())
    except OSError:
        return connection.respond(http.HTTPStatus.INTERNAL_SERVER_ERROR, "Read error\n")
    mime_type, _ = mimetypes.guess_type(real_path)
    return WsResponse(
        status_code=200,
        reason_phrase="OK",
        headers=WsHeaders([
            ("Content-Type", mime_type or "application/octet-stream"),
            ("Content-Length", str(len(data))),
            ("Cache-Control", "no-cache"),
        ]),
        body=data,
    )


def _start_mdns_blocking(port: int):
    """Blocking mDNS registration — must run in a thread, not on the event loop."""
    if os.environ.get("BRIDGE_DISABLE_MDNS", "0") == "1":
        _info("mDNS disabled by BRIDGE_DISABLE_MDNS=1")
        return None
    if not _ZEROCONF_AVAILABLE or socket is None or ServiceInfo is None or Zeroconf is None:
        _warning("zeroconf not installed — mDNS disabled. Run: pip install zeroconf")
        return None
    try:
        try:
            _s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
            _s.connect(("8.8.8.8", 80))
            local_ip = _s.getsockname()[0]
            _s.close()
        except Exception:
            local_ip = socket.gethostbyname(socket.gethostname())
        info = ServiceInfo(
            "_bridge._tcp.local.",
            "bridge._bridge._tcp.local.",
            addresses=[socket.inet_aton(local_ip)],
            port=port,
            properties={"version": "2"},
        )
        zc = Zeroconf()
        try:
            zc.register_service(info)
        except NonUniqueNameException:
            # Stale cache or another instance holds the name; allow Zeroconf to pick a variant.
            zc.register_service(info, allow_name_change=True)
        _info("mDNS: bridge.local advertised at %s:%d", local_ip, port)
        return zc
    except Exception as exc:
        _warning("mDNS registration failed: %s", exc)
        return None


async def start_mdns(port: int):
    """Async wrapper: Zeroconf registration is blocking, so run it off the event loop."""
    return await asyncio.to_thread(_start_mdns_blocking, port)


async def _drain_proc_stderr(proc) -> None:
    try:
        async for _ in proc.stderr:
            pass
    except Exception:
        pass


def is_cloudflared_running() -> bool:
    return _cloudflared_proc is not None and _cloudflared_proc.returncode is None


def _start_tunnel_retry(ws_url: str) -> None:
    global _tunnel_retry_task
    if _tunnel_retry_task and not _tunnel_retry_task.done():
        _tunnel_retry_task.cancel()
    if _spawn_task is not None and _notify_tunnel_with_retry is not None:
        _tunnel_retry_task = _spawn_task("notify-fcm:tunnel", _notify_tunnel_with_retry(ws_url))


async def start_cloudflared_tunnel(port: int) -> None:
    global _cloudflared_proc, _current_tunnel_url, _tunnel_url_delivered
    if is_cloudflared_running():
        _info("cloudflared already running, skipping")
        return
    cfd = shutil.which("cloudflared")
    if not cfd:
        print("WARNING: cloudflared not installed, skipping tunnel")
        print("   Install: https://developers.cloudflare.com/cloudflare-one/connections/connect-networks/downloads/")
        return
    proc = await asyncio.create_subprocess_exec(
        cfd, "tunnel", "--url", f"http://localhost:{port}",
        stdout=asyncio.subprocess.DEVNULL,
        stderr=asyncio.subprocess.PIPE,
    )
    _cloudflared_proc = proc
    print("Waiting for cloudflared tunnel...")
    async for line_bytes in proc.stderr:
        line = line_bytes.decode(errors="replace")
        m = re.search(r'https://[\w.-]+\.trycloudflare\.com', line)
        if m:
            url = m.group(0)
            ws_url = url.replace("https://", "wss://")
            print(f"\n{'='*56}")
            print("Tunnel URL (fill in app Settings):")
            print(f"   {ws_url}")
            print(f"{'='*56}\n")
            _info("Cloudflared tunnel: %s", ws_url)
            _current_tunnel_url = ws_url
            _tunnel_url_delivered = False
            _start_tunnel_retry(ws_url)
            if _set_media_base_url is not None:
                _set_media_base_url(url)
            if _spawn_task is not None:
                _spawn_task("cloudflared-drain-stderr", _drain_proc_stderr(proc))
                if _broadcast_json is not None:
                    _spawn_task("broadcast-tunnel-url", _broadcast_json({
                        "type": "tunnel_url",
                        "url": ws_url,
                        "instance_id": _instance_id,
                    }))
            return
    _warning("cloudflared tunnel URL not detected")
    _cloudflared_proc = None
    _current_tunnel_url = None


async def tunnel_url_file_watcher(tunnel_url_file: str) -> None:
    global _current_tunnel_url
    _info("[tunnel-watcher] started, watching %s", tunnel_url_file)
    while True:
        await asyncio.sleep(30)
        try:
            if not tunnel_url_file:
                break
            if os.path.isfile(tunnel_url_file):
                candidate = open(tunnel_url_file).read().strip()
            else:
                candidate = ""
            if candidate == (_current_tunnel_url or ""):
                continue
            _current_tunnel_url = candidate or None
            if _current_tunnel_url:
                _info("[tunnel-watcher] URL changed → %s", _current_tunnel_url)
                https_url = _current_tunnel_url.replace("wss://", "https://")
                if _set_media_base_url is not None:
                    _set_media_base_url(https_url)
                _start_tunnel_retry(_current_tunnel_url)
            else:
                _info("[tunnel-watcher] URL cleared (cloudflared restarting?)")
            if _spawn_task is not None and _broadcast_json is not None:
                _spawn_task("broadcast-tunnel-url", _broadcast_json({
                    "type": "tunnel_url",
                    "url": _current_tunnel_url or "",
                    "instance_id": _instance_id,
                }))
        except Exception as exc:
            _warning("[tunnel-watcher] error: %s", exc)
