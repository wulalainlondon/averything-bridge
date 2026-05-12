#!/usr/bin/env python3
"""
Claude Bridge — WebSocket server that proxies Claude Code CLI to a mobile app.
Uses the modern websockets.asyncio.server API (websockets >= 14).
"""

import argparse
import asyncio
import json
import logging
import os
import re
import signal
import sys
from urllib.parse import quote as urlquote

from websockets.asyncio.server import serve, ServerConnection

# ---------------------------------------------------------------------------
# Paths
# ---------------------------------------------------------------------------
BRIDGE_DIR = os.path.dirname(os.path.abspath(__file__))
LOG_FILE   = os.path.join(BRIDGE_DIR, "bridge.log")
CLAUDE_BIN = "/Users/wulala/.npm-global/bin/claude"
LINE_SEND  = "/Users/wulala/Downloads/Helper/line_send.py"

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------
logging.basicConfig(
    level=logging.DEBUG,
    format="%(asctime)s %(levelname)s %(message)s",
    handlers=[
        logging.FileHandler(LOG_FILE, encoding="utf-8"),
        logging.StreamHandler(sys.stdout),
    ],
)
log = logging.getLogger("bridge")

# ---------------------------------------------------------------------------
# Media detection
# ---------------------------------------------------------------------------
MEDIA_RE = re.compile(
    r'(/(?:[^\s\'"<>]+\.(?:jpg|jpeg|png|gif|webp|mp4|mov|m4v|avi)))',
    re.IGNORECASE,
)
IMAGE_EXTS = {".jpg", ".jpeg", ".png", ".gif", ".webp"}
VIDEO_EXTS = {".mp4", ".mov", ".m4v", ".avi"}

# ---------------------------------------------------------------------------
# Global HTTP server state (one shared server for the whole process)
# ---------------------------------------------------------------------------
_http_server_proc: "asyncio.subprocess.Process | None" = None
HTTP_PORT = 9090


async def ensure_http_server() -> None:
    global _http_server_proc
    if _http_server_proc is not None and _http_server_proc.returncode is None:
        return
    log.info("Starting SimpleHTTPServer on port %d", HTTP_PORT)
    _http_server_proc = await asyncio.create_subprocess_exec(
        sys.executable, "-m", "http.server", str(HTTP_PORT), "--directory", "/",
        stdout=asyncio.subprocess.DEVNULL,
        stderr=asyncio.subprocess.DEVNULL,
    )


async def scan_for_media(text: str, ws: ServerConnection) -> None:
    matches = MEDIA_RE.findall(text)
    for path in matches:
        if not os.path.exists(path):
            continue
        ext = os.path.splitext(path)[1].lower()
        if ext in IMAGE_EXTS:
            media_type = "image"
        elif ext in VIDEO_EXTS:
            media_type = "video"
        else:
            continue
        await ensure_http_server()
        encoded = urlquote(path)
        url = f"http://127.0.0.1:{HTTP_PORT}{encoded}"
        payload = {"type": "media", "media_type": media_type, "path": path, "url": url}
        log.info("Media detected: %s", payload)
        await ws.send(json.dumps(payload))


# ---------------------------------------------------------------------------
# LINE notification
# ---------------------------------------------------------------------------
async def notify_line(last_text: str) -> None:
    summary = last_text[-80:] if len(last_text) > 80 else last_text
    cmd = ["python3", LINE_SEND, f"Claude 完成：{summary}"]
    try:
        proc = await asyncio.create_subprocess_exec(*cmd)
        await proc.wait()
        log.info("LINE notification sent")
    except Exception as exc:
        log.warning("LINE notification failed: %s", exc)


# ---------------------------------------------------------------------------
# Claude subprocess runner
# ---------------------------------------------------------------------------
async def stream_text(text: str, ws: ServerConnection, chunk_size: int = 4) -> None:
    """Send text in small chunks to simulate streaming typewriter effect."""
    for i in range(0, len(text), chunk_size):
        chunk = text[i:i + chunk_size]
        await ws.send(json.dumps({"type": "text_chunk", "content": chunk}))
        await asyncio.sleep(0.02)


async def run_claude(content: str, ws: ServerConnection, session_id: str | None = None) -> str | None:
    """Run Claude and return the new session_id (or None on failure)."""
    cmd = [CLAUDE_BIN, "--output-format", "stream-json", "--verbose"]
    if session_id:
        cmd += ["--resume", session_id]
    cmd += ["--print", content]
    log.info("Spawning Claude (session=%s): %s", session_id, cmd)

    try:
        proc = await asyncio.create_subprocess_exec(
            *cmd,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
            stdin=asyncio.subprocess.PIPE,
        )
    except Exception as exc:
        log.error("Failed to spawn Claude: %s", exc)
        await ws.send(json.dumps({"type": "error", "message": str(exc)}))
        return

    # Track active tool_use blocks by index → {tool_use_id, name}
    tool_blocks: dict[int, dict] = {}
    accumulated_text = ""
    returned_session_id: str | None = None

    # Shared flag: set by ws_listener when client sends "stop"
    stop_event = asyncio.Event()

    # Queue for prompt_reply payloads from the client
    prompt_reply_queue: asyncio.Queue[str] = asyncio.Queue()

    async def ws_listener() -> None:
        """Concurrently watch for stop / prompt_reply from client."""
        try:
            async for raw in ws:
                try:
                    msg = json.loads(raw)
                except json.JSONDecodeError:
                    continue
                mtype = msg.get("type")
                if mtype == "stop":
                    log.info("Received stop — terminating Claude")
                    stop_event.set()
                    try:
                        proc.send_signal(signal.SIGTERM)
                    except ProcessLookupError:
                        pass
                    await asyncio.sleep(1)
                    try:
                        proc.kill()
                    except ProcessLookupError:
                        pass
                    await ws.send(json.dumps({"type": "stopped"}))
                elif mtype == "prompt_reply":
                    await prompt_reply_queue.put(msg.get("content", ""))
                elif mtype == "ping":
                    await ws.send(json.dumps({"type": "pong"}))
        except Exception:
            pass

    async def stdout_reader() -> None:
        nonlocal accumulated_text
        async for line_bytes in proc.stdout:
            line = line_bytes.decode("utf-8", errors="replace").strip()
            if not line:
                continue

            # Detect interactive prompts (not NDJSON)
            if re.search(r'\? |\[y/N\]|\[Y/n\]', line):
                log.debug("Interactive prompt detected: %s", line)
                await ws.send(json.dumps({"type": "prompt", "text": line}))
                try:
                    reply = await asyncio.wait_for(prompt_reply_queue.get(), timeout=60)
                    proc.stdin.write((reply + "\n").encode())
                    await proc.stdin.drain()
                except asyncio.TimeoutError:
                    log.warning("Timed out waiting for prompt_reply")
                continue

            try:
                evt = json.loads(line)
            except json.JSONDecodeError:
                log.debug("Non-JSON stdout line: %s", line)
                continue

            etype = evt.get("type", "")

            if etype == "assistant":
                message = evt.get("message", {})
                content_blocks = message.get("content", [])
                for block in content_blocks:
                    btype = block.get("type", "")
                    if btype == "text":
                        text = block.get("text", "")
                        if text:
                            accumulated_text += text
                            await stream_text(text, ws)
                            await scan_for_media(text, ws)
                    elif btype == "tool_use":
                        tool_id = block.get("id", "")
                        name = block.get("name", "")
                        input_data = block.get("input", {})
                        command = input_data.get("command", json.dumps(input_data))
                        tool_blocks[tool_id] = {"name": name}
                        await ws.send(json.dumps({
                            "type": "tool_start",
                            "name": name,
                            "tool_use_id": tool_id,
                            "command": command,
                        }))

            elif etype == "tool_result":
                tool_id = evt.get("tool_use_id", "")
                output = evt.get("content", "")
                if isinstance(output, list):
                    output = "\n".join(
                        b.get("text", "") for b in output if b.get("type") == "text"
                    )
                await ws.send(json.dumps({
                    "type": "tool_result",
                    "tool_use_id": tool_id,
                    "output": str(output),
                }))
                await ws.send(json.dumps({"type": "tool_end", "tool_use_id": tool_id}))

            elif etype == "result":
                subtype = evt.get("subtype", "")
                new_session_id = evt.get("session_id")
                if subtype == "success":
                    log.info("result success, session_id=%s", new_session_id)
                    nonlocal returned_session_id
                    returned_session_id = new_session_id
                    await ws.send(json.dumps({"type": "done"}))
                    asyncio.create_task(notify_line(accumulated_text))
                else:
                    err = evt.get("result", "Unknown error")
                    log.error("result error: %s", err)
                    await ws.send(json.dumps({"type": "error", "message": str(err)}))

            elif etype == "system":
                log.debug("system event subtype=%s", evt.get("subtype", ""))

            elif etype == "rate_limit_event":
                log.debug("rate_limit_event received")

            else:
                log.debug("Unhandled event type: %s", etype)

    async def stderr_reader() -> None:
        async for line_bytes in proc.stderr:
            line = line_bytes.decode("utf-8", errors="replace").strip()
            if line:
                log.warning("Claude stderr: %s", line)

    listener_task = asyncio.create_task(ws_listener())
    stderr_task   = asyncio.create_task(stderr_reader())

    try:
        await stdout_reader()
    finally:
        await proc.wait()
        stderr_task.cancel()
        listener_task.cancel()
        try:
            await stderr_task
        except asyncio.CancelledError:
            pass
        try:
            await listener_task
        except asyncio.CancelledError:
            pass
        rc = proc.returncode
        log.info("Claude process exited with code %s", rc)
        if rc != 0 and not stop_event.is_set():
            try:
                await ws.send(json.dumps({"type": "error", "message": f"Claude exited with code {rc}"}))
            except Exception:
                pass
    return returned_session_id


# ---------------------------------------------------------------------------
# WebSocket handler
# ---------------------------------------------------------------------------
async def handler(ws: ServerConnection) -> None:
    remote = ws.remote_address
    log.info("Client connected: %s", remote)
    session_id: str | None = None  # persists across messages in same WS connection

    try:
        async for raw in ws:
            log.debug("Received: %s", str(raw)[:200])
            try:
                msg = json.loads(raw)
            except json.JSONDecodeError:
                log.warning("Non-JSON message from client: %s", str(raw)[:200])
                continue

            mtype = msg.get("type")

            if mtype == "ping":
                await ws.send(json.dumps({"type": "pong"}))

            elif mtype == "clear_session":
                session_id = None
                log.info("Session cleared")

            elif mtype == "message":
                content = msg.get("content", "")
                if not content:
                    await ws.send(json.dumps({"type": "error", "message": "Empty content"}))
                    continue
                new_sid = await run_claude(content, ws, session_id=session_id)
                if new_sid:
                    session_id = new_sid
                    log.info("Session updated: %s", session_id)

            else:
                log.debug("Unhandled message type: %s", mtype)

    except Exception as exc:
        # ConnectionClosed variants are expected; log others
        name = type(exc).__name__
        if "ConnectionClosed" in name:
            log.info("Client disconnected: %s (%s)", remote, exc)
        else:
            log.exception("Unhandled error in handler: %s", exc)
    finally:
        log.info("Client gone: %s", remote)


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------
async def main(port: int) -> None:
    log.info("Claude Bridge starting on port %d", port)
    async with serve(
        handler,
        "0.0.0.0",
        port,
        ping_interval=30,
        ping_timeout=30,
    ):
        log.info("Bridge listening on ws://0.0.0.0:%d", port)
        await asyncio.Future()  # run forever


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Claude WebSocket Bridge")
    parser.add_argument("--port", type=int, default=8765)
    args = parser.parse_args()
    asyncio.run(main(args.port))
