"""
Codex backend — image input mixin.

Decodes base64 image blobs from client messages into temp files under the
session image dir and builds the localImage input items Codex expects; cleans
the temp files up when the turn ends.
"""

import base64
import logging
import os
import uuid
from typing import TYPE_CHECKING

from .codex_common import _AppServerState, _ext_for_media_type

if TYPE_CHECKING:
    from bridge_v2 import Session

log = logging.getLogger("bridge_v2")


class _CodexImageMixin:
    def _prepare_image_input(self, session: "Session", img: dict,
                             state: _AppServerState) -> dict | None:
        media_type = str(img.get("media_type", "image/jpeg"))
        raw_b64 = str(img.get("data", "")).strip()
        if not raw_b64:
            return None
        if "," in raw_b64 and raw_b64.lower().startswith("data:"):
            raw_b64 = raw_b64.split(",", 1)[1]
        try:
            blob = base64.b64decode(raw_b64, validate=False)
        except Exception:
            return None

        ext = _ext_for_media_type(media_type)
        tmp_path = self._write_session_image(session, blob, ext)
        if not tmp_path:
            return None
        state.temp_image_paths.append(tmp_path)
        return {"type": "localImage", "path": tmp_path}

    @staticmethod
    def _resolve_image_dir(session: "Session") -> str:
        configured = (session.image_dir or "").strip()
        if configured:
            return os.path.abspath(os.path.expanduser(configured))
        base = session.cwd if os.path.isdir(session.cwd) else os.path.expanduser("~")
        return os.path.join(base, ".bridge_images")

    def _write_session_image(self, session: "Session", blob: bytes, ext: str) -> str | None:
        try:
            root = self._resolve_image_dir(session)
            os.makedirs(root, exist_ok=True)
            request_id = (session.current_request_id or f"r_{uuid.uuid4().hex[:8]}").replace("/", "_")
            filename = f"{session.session_id}_{request_id}_{uuid.uuid4().hex[:8]}{ext}"
            path = os.path.join(root, filename)
            with open(path, "wb") as f:
                f.write(blob)
            return path
        except Exception:
            return None

    @staticmethod
    def _cleanup_temp_images(state: _AppServerState) -> None:
        for p in state.temp_image_paths:
            try:
                os.remove(p)
            except Exception:
                pass
        state.temp_image_paths.clear()
