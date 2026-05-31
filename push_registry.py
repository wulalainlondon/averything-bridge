"""Firebase, FCM, and file-push inbox support."""
from __future__ import annotations

import asyncio
import base64
import datetime
import json
import mimetypes
import os
import re
import time
import uuid
from collections.abc import Awaitable, Callable
from pathlib import Path
from typing import Any

import client_manager

try:
    import firebase_admin
    from firebase_admin import credentials, messaging as fb_messaging, storage as fb_storage
    _FIREBASE_AVAILABLE = True
except ImportError:
    _FIREBASE_AVAILABLE = False


BroadcastJson = Callable[[dict], Awaitable[int]]
SpawnTask = Callable[[str, Awaitable[Any]], Any]

_firebase_app = None
_firebase_storage_app = None
_PUSH_FILE_REGISTRY: dict[str, dict] = {}
_PUSH_INLINE_MAX_BYTES = 50 * 1024 * 1024
_INBOX_TTL_SECONDS = 7 * 24 * 3600
_STORAGE_KEY_FILE = os.environ.get(
    "BRIDGE_STORAGE_KEY",
    os.path.expanduser("~/.config/claude-bridge/storage-serviceAccountKey.json"),
)

_DATA_DIR = ""
_ROOT_DIR = ""
_FCM_TOKEN_FILE = ""
_SERVICE_ACCOUNT_FILE = ""
_INSTANCE_ID = ""
_log: Any = None
_broadcast_json: BroadcastJson | None = None
_spawn_task: SpawnTask | None = None
_is_tunnel_delivered: Callable[[], bool] = lambda: False


def configure(
    *,
    data_dir: str,
    root_dir: str,
    fcm_token_file: str,
    service_account_file: str,
    instance_id: str,
    log: Any,
    broadcast_json: BroadcastJson,
    spawn_task: SpawnTask,
    is_tunnel_delivered: Callable[[], bool],
) -> None:
    global _DATA_DIR, _ROOT_DIR, _FCM_TOKEN_FILE, _SERVICE_ACCOUNT_FILE, _INSTANCE_ID
    global _log, _broadcast_json, _spawn_task, _is_tunnel_delivered
    _DATA_DIR = data_dir
    _ROOT_DIR = root_dir
    _FCM_TOKEN_FILE = fcm_token_file
    _SERVICE_ACCOUNT_FILE = service_account_file
    _INSTANCE_ID = instance_id
    _log = log
    _broadcast_json = broadcast_json
    _spawn_task = spawn_task
    _is_tunnel_delivered = is_tunnel_delivered


def _warning(message: str, *args: Any) -> None:
    if _log is not None:
        _log.warning(message, *args)


def _info(message: str, *args: Any) -> None:
    if _log is not None:
        _log.info(message, *args)


def _debug(message: str, *args: Any) -> None:
    if _log is not None:
        _log.debug(message, *args)


def init_firebase() -> None:
    global _firebase_app, _firebase_storage_app
    if not _FIREBASE_AVAILABLE:
        _warning("firebase-admin not installed — FCM disabled. Run: pip install firebase-admin")
        return

    if os.path.exists(_SERVICE_ACCOUNT_FILE):
        try:
            cred = credentials.Certificate(_SERVICE_ACCOUNT_FILE)
            _firebase_app = firebase_admin.initialize_app(cred)
            _info("Firebase FCM initialized")
        except Exception as exc:
            _warning("Firebase FCM init failed: %s", exc)
    else:
        _warning("serviceAccountKey.json not found at %s — FCM disabled", _SERVICE_ACCOUNT_FILE)

    storage_key = _STORAGE_KEY_FILE if os.path.exists(_STORAGE_KEY_FILE) else _SERVICE_ACCOUNT_FILE
    if os.path.exists(storage_key):
        try:
            with open(storage_key) as f:
                sk = json.load(f)
            bucket_name = f"{sk['project_id']}.firebasestorage.app"
            storage_cred = credentials.Certificate(storage_key)
            _firebase_storage_app = firebase_admin.initialize_app(
                storage_cred,
                {"storageBucket": bucket_name},
                name="storage",
            )
            _info("Firebase Storage initialized (bucket: %s)", bucket_name)
        except Exception as exc:
            _warning("Firebase Storage init failed: %s", exc)


def _inbox_file_path() -> str:
    return os.path.join(_DATA_DIR, "inbox.json")


def save_inbox() -> None:
    try:
        with open(_inbox_file_path(), "w", encoding="utf-8") as f:
            json.dump(_PUSH_FILE_REGISTRY, f)
    except Exception as exc:
        _warning("inbox save failed: %s", exc)


def load_inbox() -> None:
    global _PUSH_FILE_REGISTRY
    path = _inbox_file_path()
    if not os.path.isfile(path):
        return
    try:
        with open(path, encoding="utf-8") as f:
            data = json.load(f)
        now = time.time()
        _PUSH_FILE_REGISTRY = {
            fid: entry for fid, entry in data.items()
            if now - entry.get("pushed_at", 0) < _INBOX_TTL_SECONDS
        }
        _info("Loaded inbox: %d entries", len(_PUSH_FILE_REGISTRY))
    except Exception as exc:
        _warning("inbox load failed: %s", exc)


def pending_file_push_items(device_id: str = "", *, include_pushed_at: bool = False) -> list[dict]:
    now = time.time()
    items: list[dict] = []
    for fid, entry in list(_PUSH_FILE_REGISTRY.items()):
        if now - entry.get("pushed_at", 0) > _INBOX_TTL_SECONDS:
            continue
        acked = set(entry.get("acked_device_ids") or [])
        target = set(entry.get("target_device_ids") or [])
        if target and device_id and device_id not in target:
            continue
        if device_id and device_id in acked:
            continue
        item: dict = {
            "file_id": fid,
            "filename": entry["filename"],
            "size": entry["size"],
            "mime_type": entry["mime_type"],
        }
        if include_pushed_at:
            item["pushed_at"] = entry.get("pushed_at", 0)
        if "data" in entry:
            item["data"] = entry["data"]
        elif "url" in entry:
            item["url"] = entry["url"]
        else:
            continue
        items.append(item)
    return items


async def handle_push_file(ws: Any, path: str, sender_device_id: str = "") -> None:
    if _ROOT_DIR:
        from utils.path_jail import JailEscape, resolve_jailed
        try:
            path = resolve_jailed(path, _ROOT_DIR)
        except JailEscape as e:
            _warning("[jail] push_file escape: req=%r resolved=%r root=%r", e.req_path, e.resolved, e.root_dir)
            try:
                await ws.send(json.dumps({"type": "error", "message": f"Path outside instance root: {e.req_path}"}))
            except Exception:
                pass
            return
    expanded = os.path.expanduser(path)
    if not os.path.isfile(expanded):
        try:
            await ws.send(json.dumps({"type": "error", "message": f"File not found: {path}"}))
        except Exception:
            pass
        return

    filename = os.path.basename(expanded)
    size = os.path.getsize(expanded)
    file_id = f"push_{uuid.uuid4().hex[:12]}"
    mime_type, _ = mimetypes.guess_type(filename)
    if not mime_type:
        mime_type = "application/octet-stream"
    target_device_ids = [
        device_id
        for device_id in client_manager.connected_device_ids()
        if device_id != sender_device_id
    ]

    if size <= _PUSH_INLINE_MAX_BYTES:
        try:
            with open(expanded, "rb") as fh:
                data_b64 = base64.b64encode(fh.read()).decode("ascii")
            _PUSH_FILE_REGISTRY[file_id] = {
                "blob_path": None,
                "filename": filename,
                "size": size,
                "mime_type": mime_type,
                "data": data_b64,
                "target_device_ids": target_device_ids,
                "acked_device_ids": [],
                "pushed_at": time.time(),
            }
            save_inbox()
            _info("push_file inline: %s (%d bytes)", filename, size)
            broadcast_payload = {
                "type": "file_push",
                "file_id": file_id,
                "filename": filename,
                "size": size,
                "mime_type": mime_type,
                "data": data_b64,
            }
            try:
                await ws.send(json.dumps({
                    "type": "push_ack",
                    "file_id": file_id,
                    "filename": filename,
                    "size": size,
                }))
            except Exception:
                pass
            if _spawn_task is not None and _broadcast_json is not None:
                _spawn_task(f"broadcast-push:{file_id}", _broadcast_json(broadcast_payload))
                _spawn_task(f"notify-fcm:push-file:{file_id}", notify_fcm_file_push(file_id, filename))
        except Exception as exc:
            _warning("push_file inline failed: %s", exc)
            try:
                await ws.send(json.dumps({"type": "error", "message": f"Push failed: {exc}"}))
            except Exception:
                pass
        return

    if _firebase_storage_app is None:
        try:
            await ws.send(json.dumps({"type": "error", "message": "File too large for inline transfer and Firebase Storage not available"}))
        except Exception:
            pass
        return

    blob_path = f"bridge_pushes/{file_id}/{filename}"
    try:
        loop = asyncio.get_event_loop()
        bucket = fb_storage.bucket(app=_firebase_storage_app)

        def _upload() -> str:
            blob = bucket.blob(blob_path)
            blob.upload_from_filename(expanded, content_type=mime_type)
            return blob.generate_signed_url(
                expiration=datetime.timedelta(days=7),
                method="GET",
                version="v4",
            )

        url = await loop.run_in_executor(None, _upload)
        _PUSH_FILE_REGISTRY[file_id] = {
            "blob_path": blob_path,
            "filename": filename,
            "url": url,
            "size": size,
            "mime_type": mime_type,
            "target_device_ids": target_device_ids,
            "acked_device_ids": [],
            "pushed_at": time.time(),
        }
        save_inbox()
        _info("push_file uploaded: %s → %s", filename, blob_path)

        if _broadcast_json is not None:
            await _broadcast_json({
                "type": "file_push",
                "file_id": file_id,
                "filename": filename,
                "url": url,
                "size": size,
                "mime_type": mime_type,
            })
        if _spawn_task is not None:
            _spawn_task(f"notify-fcm:push-file:{file_id}", notify_fcm_file_push(file_id, filename))
    except Exception as exc:
        _warning("push_file upload failed: %s", exc)
        try:
            await ws.send(json.dumps({"type": "error", "message": f"Upload failed: {exc}"}))
        except Exception:
            pass


async def handle_file_push_ack(file_id: str, device_id: str = "") -> None:
    entry = _PUSH_FILE_REGISTRY.get(file_id)
    if not entry:
        _debug("file_push_ack: unknown file_id %s", file_id)
        return
    target = set(entry.get("target_device_ids") or [])
    acked = set(entry.get("acked_device_ids") or [])
    if device_id:
        acked.add(device_id)
        entry["acked_device_ids"] = sorted(acked)
        save_inbox()
    should_delete = target.issubset(acked) if target else bool(acked)
    if not should_delete:
        return
    _PUSH_FILE_REGISTRY.pop(file_id, None)
    save_inbox()
    blob_path = entry.get("blob_path")
    if not blob_path or _firebase_storage_app is None:
        return
    try:
        loop = asyncio.get_event_loop()
        bucket = fb_storage.bucket(app=_firebase_storage_app)

        def _delete() -> None:
            blob = bucket.blob(blob_path)
            blob.delete()

        await loop.run_in_executor(None, _delete)
        _info("push_file deleted from storage: %s", blob_path)
    except Exception as exc:
        _warning("push_file delete failed: %s", exc)


async def send_tunnel_fcm_once(ws_url: str) -> bool:
    if _firebase_app is None:
        return False
    try:
        with open(_FCM_TOKEN_FILE) as f:
            token = f.read().strip()
    except FileNotFoundError:
        _warning("FCM tunnel notify: no token on file")
        return False
    try:
        message = fb_messaging.Message(
            data={"type": "tunnel_url", "url": ws_url, "instance_id": _INSTANCE_ID},
            token=token,
        )
        loop = asyncio.get_running_loop()
        await loop.run_in_executor(None, fb_messaging.send, message)
        _info("FCM tunnel URL pushed to device: %s", ws_url)
        return True
    except Exception as exc:
        _warning("FCM tunnel notify failed: %s", exc)
        return False


async def notify_fcm_tunnel_with_retry(ws_url: str) -> None:
    for attempt in range(5):
        if _is_tunnel_delivered():
            _info("FCM tunnel URL delivery confirmed — stopping retry after %d attempt(s)", attempt)
            return
        await send_tunnel_fcm_once(ws_url)
        if attempt < 4:
            await asyncio.sleep(60)
    _info("FCM tunnel URL retry exhausted (5 attempts)")


async def notify_fcm(session_name: str, last_text: str, session_id: str = "") -> None:
    if _firebase_app is None:
        _warning("FCM not ready, skipping notification")
        return
    try:
        with open(_FCM_TOKEN_FILE) as f:
            token = f.read().strip()
    except FileNotFoundError:
        _warning("No FCM token on file — skipping notification")
        return

    def _clean_md(s: str) -> str:
        s = re.sub(r'[*`#_~>]+', '', s)
        s = re.sub(r'\[([^\]]+)\]\([^)]+\)', r'\1', s)
        return re.sub(r'\s+', ' ', s).strip()

    paragraphs = [p for p in last_text.split('\n\n') if p.strip()]
    source = paragraphs[-1] if paragraphs else last_text
    clean = _clean_md(source)
    sentence_parts = re.split(r'(?<=[。！？!?\.])\s+', clean, maxsplit=1)
    summary = sentence_parts[0].strip() if sentence_parts else clean
    if not summary:
        summary = _clean_md(last_text)
    if len(summary) > 120:
        summary = summary[:120].rstrip() + "…"

    message = fb_messaging.Message(
        notification=fb_messaging.Notification(
            title=f"✓ {session_name}",
            body=summary,
        ),
        data={"type": "task_done", "session_id": session_id},
        token=token,
    )
    loop = asyncio.get_running_loop()
    for attempt in range(3):
        try:
            await loop.run_in_executor(None, fb_messaging.send, message)
            _info("FCM notification sent")
            return
        except Exception as exc:
            if attempt < 2:
                wait = 2 ** attempt
                _warning("FCM send failed (attempt %d/3): %s — retrying in %ds", attempt + 1, exc, wait)
                await asyncio.sleep(wait)
            else:
                if _log is not None:
                    _log.error("FCM send failed after 3 attempts: %s", exc)


async def notify_fcm_file_push(file_id: str, filename: str) -> None:
    if _firebase_app is None:
        return
    try:
        with open(_FCM_TOKEN_FILE) as f:
            token = f.read().strip()
    except FileNotFoundError:
        return
    message = fb_messaging.Message(
        notification=fb_messaging.Notification(
            title="📎 新檔案",
            body=filename,
        ),
        data={
            "type": "file_push",
            "file_id": file_id,
            "filename": filename,
            "deep_link": "bridge://inbox",
        },
        token=token,
    )
    loop = asyncio.get_running_loop()
    for attempt in range(3):
        try:
            await loop.run_in_executor(None, fb_messaging.send, message)
            _info("FCM file_push notification sent: %s", filename)
            return
        except Exception as exc:
            if attempt < 2:
                await asyncio.sleep(2 ** attempt)
            else:
                _warning("FCM file_push notify failed after 3 attempts: %s", exc)


async def notify_fcm_user_input(
    session_name: str,
    header: str,
    question_text: str,
    session_id: str = "",
    request_id: str = "",
) -> None:
    if _firebase_app is None:
        return
    try:
        with open(_FCM_TOKEN_FILE) as f:
            token = f.read().strip()
    except FileNotFoundError:
        return
    body = question_text or header or "需要你的回覆"
    if len(body) > 120:
        body = body[:120].rstrip() + "…"
    message = fb_messaging.Message(
        notification=fb_messaging.Notification(
            title=f"❓ {session_name}",
            body=body,
        ),
        data={
            "type": "user_input_request",
            "session_id": session_id,
            "request_id": request_id,
            "deep_link": "bridge://chat",
        },
        token=token,
    )
    loop = asyncio.get_running_loop()
    for attempt in range(3):
        try:
            await loop.run_in_executor(None, fb_messaging.send, message)
            _info("FCM user_input notification sent for session %s", session_id)
            return
        except Exception as exc:
            if attempt < 2:
                await asyncio.sleep(2 ** attempt)
            else:
                _warning("FCM user_input notify failed after 3 attempts: %s", exc)


async def notify_fcm_session_died(session_name: str, session_id: str = "") -> None:
    """Send an FCM push when a session permanently dies (no more restarts)."""
    if _firebase_app is None:
        return
    try:
        with open(_FCM_TOKEN_FILE) as f:
            token = f.read().strip()
    except FileNotFoundError:
        return
    if not token:
        return
    message = fb_messaging.Message(
        notification=fb_messaging.Notification(
            title=f"⚠️ {session_name}",
            body="Session terminated and could not be restarted.",
        ),
        data={
            "type": "session_died",
            "session_id": session_id,
            "deep_link": "bridge://chat",
        },
        token=token,
    )
    loop = asyncio.get_running_loop()
    for attempt in range(3):
        try:
            await loop.run_in_executor(None, fb_messaging.send, message)
            _info("FCM session_died notification sent for session %s", session_id)
            return
        except Exception as exc:
            if attempt < 2:
                await asyncio.sleep(2 ** attempt)
            else:
                _warning("FCM session_died notify failed after 3 attempts: %s", exc)
