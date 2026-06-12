from __future__ import annotations

import asyncio
import json
import mimetypes
import os
import re
import shutil
import time
import urllib.parse
import uuid

from utils.path_jail import JailEscape, resolve_jailed

VIDEO_EXTS = {".mp4", ".mov", ".m4v", ".webm", ".mkv"}
IMAGE_EXTS = {".jpg", ".jpeg", ".png", ".gif", ".webp", ".heic", ".avif"}
DOC_EXTS = {".md", ".txt", ".pdf", ".html", ".htm", ".json", ".csv"}
SUBTITLE_EXTS = {".srt", ".vtt", ".ass"}
MEDIA_EXTS = VIDEO_EXTS | IMAGE_EXTS | DOC_EXTS | SUBTITLE_EXTS


def _artifact_kind(path: str) -> str:
    ext = os.path.splitext(path)[1].lower()
    name = os.path.basename(path).lower()
    if os.path.isdir(path):
        return "folder"
    if ext in VIDEO_EXTS:
        return "video"
    if ext in IMAGE_EXTS:
        return "image"
    if ext in SUBTITLE_EXTS:
        return "subtitle"
    if "transcript" in name:
        return "transcript"
    if "summary" in name:
        return "summary"
    return "document"


def _media_url(path: str, base_url: str) -> str:
    encoded = urllib.parse.quote(os.path.realpath(path), safe="")
    if base_url:
        return f"{base_url.rstrip('/')}/media/{encoded}"
    return f"/media/{encoded}"


def _artifact_for_path(path: str, *, source: str, base_url: str, source_task_id: str = "", source_session_id: str = "") -> dict:
    stat = os.stat(path)
    mime_type, _ = mimetypes.guess_type(path)
    real_path = os.path.realpath(path)
    title = os.path.basename(path)
    kind = _artifact_kind(path)
    payload = {
        "id": "file:" + real_path,
        "kind": kind,
        "status": "ready",
        "title": title,
        "path": real_path,
        "source": source,
        "created_at": int(stat.st_ctime),
        "updated_at": int(stat.st_mtime),
        "metadata": {
            "mime_type": mime_type or "application/octet-stream",
            "file_size": int(stat.st_size),
        },
    }
    if kind in {"video", "image", "document", "subtitle", "transcript", "summary"}:
        payload["url"] = _media_url(real_path, base_url)
    if source_task_id:
        payload["source_task_id"] = source_task_id
    if source_session_id:
        payload["source_session_id"] = source_session_id
    return payload


def _scan_dir(path: str, *, limit: int, base_url: str) -> list[dict]:
    artifacts: list[dict] = []
    if not os.path.isdir(path):
        return artifacts
    for root, dirs, files in os.walk(path):
        dirs[:] = [d for d in dirs if not d.startswith(".") and d not in {"node_modules", ".git", "venv", ".venv"}]
        for name in files:
            if name.startswith("."):
                continue
            full_path = os.path.join(root, name)
            ext = os.path.splitext(name)[1].lower()
            if ext not in MEDIA_EXTS:
                continue
            try:
                artifacts.append(_artifact_for_path(full_path, source="file", base_url=base_url))
            except OSError:
                continue
            if len(artifacts) >= limit:
                artifacts.sort(key=lambda item: item.get("updated_at", 0), reverse=True)
                return artifacts
    artifacts.sort(key=lambda item: item.get("updated_at", 0), reverse=True)
    return artifacts[:limit]


def _default_scan_paths(root_dir: str) -> list[str]:
    home = os.path.expanduser("~")
    candidates = [
        os.path.join(home, "Downloads", "youtube"),
        os.path.join(home, "Downloads", "bridge-inbox"),
        os.path.join(home, "Downloads"),
    ]
    if root_dir:
        candidates.insert(0, root_dir)
    seen: set[str] = set()
    paths: list[str] = []
    for candidate in candidates:
        real = os.path.realpath(os.path.expanduser(candidate))
        if real in seen or not os.path.isdir(real):
            continue
        seen.add(real)
        paths.append(real)
    return paths


async def handle_artifact_msg(mtype: str, msg: dict, ws, ctx: dict) -> bool:
    if mtype == "scan_artifacts":
        limit = int(msg.get("limit") or 100)
        limit = max(1, min(limit, 300))
        root_dir = ctx.get("root_dir", "")
        base_url = ctx.get("media_base_url", lambda: "")()
        paths: list[str]
        if msg.get("path"):
            try:
                paths = [resolve_jailed(str(msg["path"]), root_dir)]
            except JailEscape:
                await ws.send(json.dumps(ctx["msg_artifacts_list"]([])))
                return True
        else:
            paths = _default_scan_paths(root_dir)
        artifacts: list[dict] = []
        per_path_limit = max(1, limit // max(1, len(paths)))
        for path in paths:
            artifacts.extend(_scan_dir(path, limit=per_path_limit, base_url=base_url))
            if len(artifacts) >= limit:
                break
        artifacts.sort(key=lambda item: item.get("updated_at", 0), reverse=True)
        await ws.send(json.dumps(ctx["msg_artifacts_list"](artifacts[:limit])))
        return True

    if mtype == "youtube_task":
        task_id = "yt_" + uuid.uuid4().hex[:10]
        url = str(msg.get("url") or "").strip()
        session_id = str(msg.get("session_id") or "")
        artifact = {
            "id": "task:" + task_id,
            "kind": "task",
            "status": "running",
            "title": "YouTube Processing",
            "source": "youtube",
            "source_task_id": task_id,
            "created_at": int(time.time()),
            "updated_at": int(time.time()),
            "metadata": {"original_url": url},
        }
        if session_id:
            artifact["source_session_id"] = session_id
        await ws.send(json.dumps(ctx["msg_youtube_task_started"](task_id, artifact)))
        ctx["spawn_task"](
            "youtube-task:" + task_id,
            _run_youtube_task(task_id=task_id, url=url, session_id=session_id, ws=ws, ctx=ctx, task_artifact=artifact),
        )
        return True

    return False


async def _run_youtube_task(*, task_id: str, url: str, session_id: str, ws, ctx: dict, task_artifact: dict) -> None:
    base_url = ctx.get("media_base_url", lambda: "")()
    out_dir = os.path.join(os.path.expanduser("~"), "Downloads", "youtube")
    os.makedirs(out_dir, exist_ok=True)
    ytdlp = shutil.which("yt-dlp")
    if not ytdlp:
        failed = {**task_artifact, "status": "failed", "updated_at": int(time.time())}
        await ws.send(json.dumps(ctx["msg_youtube_task_failed"](task_id, failed, "yt-dlp not found")))
        return
    before = set(os.listdir(out_dir))
    template = os.path.join(out_dir, "%(title).180B [%(id)s].%(ext)s")
    cmd = [
        ytdlp,
        "--no-playlist",
        "--write-auto-subs",
        "--write-subs",
        "--sub-langs",
        "zh-TW,zh-Hant,zh,en,ja",
        "--convert-subs",
        "srt",
        "-f",
        "bv*+ba/b",
        "-o",
        template,
        url,
    ]
    proc = await asyncio.create_subprocess_exec(
        *cmd,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.STDOUT,
        cwd=out_dir,
    )
    stdout, _ = await proc.communicate()
    if proc.returncode != 0:
        message = stdout.decode("utf-8", errors="replace").strip()
        message = re.sub(r"\s+", " ", message)[-240:] or f"yt-dlp exited {proc.returncode}"
        failed = {**task_artifact, "status": "failed", "updated_at": int(time.time())}
        await ws.send(json.dumps(ctx["msg_youtube_task_failed"](task_id, failed, message)))
        return
    after = set(os.listdir(out_dir))
    created_names = sorted(after - before)
    artifacts: list[dict] = []
    for name in created_names:
        path = os.path.join(out_dir, name)
        if os.path.isfile(path) and os.path.splitext(name)[1].lower() in MEDIA_EXTS:
            artifacts.append(_artifact_for_path(
                path,
                source="youtube",
                base_url=base_url,
                source_task_id=task_id,
                source_session_id=session_id,
            ))
    if not artifacts:
        artifacts = _scan_dir(out_dir, limit=12, base_url=base_url)
    await ws.send(json.dumps(ctx["msg_youtube_task_done"](task_id, artifacts)))
