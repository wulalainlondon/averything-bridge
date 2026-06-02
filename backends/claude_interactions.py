"""
Claude backend — AskUserQuestion / user-input interaction mixin.

Handles the blocking dance around AskUserQuestion tool calls: writing
tool_result blocks back into the live stream-json stdin, resolving the paused
stdout reader, and cancelling dangling tool_use blocks so --resume history
stays clean.
"""

import asyncio
import json
import time
import logging
from typing import Any, TYPE_CHECKING

from interactions import REGISTRY as INTERACTIONS

if TYPE_CHECKING:
    from bridge_v2 import Session

log = logging.getLogger("bridge_v2")


class _ClaudeUserInputMixin:
    async def _write_stream_json(self, session: "Session", payload: dict) -> None:
        state = self._get_state(session)
        if state.proc is None or state.proc.returncode is not None or state.proc.stdin is None:
            await self._spawn_proc(session, allow_resume_fallback=True)
        if state.proc is None or state.proc.returncode is not None or state.proc.stdin is None:
            raise RuntimeError("Claude process is not running")
        state.proc.stdin.write((json.dumps(payload) + "\n").encode("utf-8"))
        await state.proc.stdin.drain()
        session.last_activity = time.time()

    async def handle_user_input_response(self, session: "Session", interaction: Any, response: dict) -> None:
        answers = response.get("answers") if isinstance(response.get("answers"), dict) else {}
        if not answers:
            answers = {
                k: v for k, v in response.items()
                if k not in {"type", "response_type", "request_id", "session_id"}
            }
        output = json.dumps({
            "request_id": interaction.request_id,
            "answers": answers,
            "cancelled": bool(response.get("cancelled") or response.get("canceled")),
        }, ensure_ascii=False)
        content_block: dict
        if interaction.tool_use_id:
            content_block = {
                "type": "tool_result",
                "tool_use_id": interaction.tool_use_id,
                "content": output,
            }
        else:
            content_block = {
                "type": "text",
                "text": f"Structured user input response:\n{output}",
            }
        await self._write_stream_json(session, {
            "type": "user",
            "message": {"role": "user", "content": [content_block]},
        })
        session.is_streaming = True
        # Unblock the stdout reader that's waiting for this AskUserQuestion response.
        state = self._get_state(session)
        state.tool_waiting_interactions.pop(interaction.tool_use_id, None)
        ev = state.tool_waiting_events.pop(interaction.tool_use_id, None)
        if ev is not None:
            ev.set()

    # ------------------------------------------------------------------
    # AskUserQuestion blocking helpers
    # ------------------------------------------------------------------

    def has_pending_user_input(self, session: "Session") -> bool:
        """True if the stdout reader is currently paused on an AskUserQuestion."""
        state = self._states.get(session.session_id)
        return bool(state and state.tool_waiting_events)

    @staticmethod
    def _build_tool_result_content(content: str, images: list | None = None,
                                   files: list | None = None) -> Any:
        """Build the `content` field of a tool_result block from a user message.

        Returns a plain string for text-only replies (the simplest valid form),
        or a list of content blocks when images/files are attached.
        """
        blocks: list = []
        if content:
            blocks.append({"type": "text", "text": content})
        for img in (images or []):
            blocks.append({
                "type": "image",
                "source": {
                    "type": "base64",
                    "media_type": img.get("media_type", "image/jpeg"),
                    "data": img.get("data", ""),
                },
            })
        for f in (files or []):
            name = f.get("name", "file")
            file_content = f.get("content", "")
            media_type = f.get("media_type", "text/plain")
            if media_type == "application/pdf":
                blocks.append({"type": "text", "text": f"[File attached (pdf, omitted from answer): {name}]"})
            else:
                ext = name.rsplit(".", 1)[-1] if "." in name else ""
                fence = f"```{ext}\n{file_content}\n```" if ext else file_content
                blocks.append({"type": "text", "text": f"[File: {name}]\n{fence}"})
        if not blocks:
            return ""
        if len(blocks) == 1 and blocks[0]["type"] == "text":
            return blocks[0]["text"]
        return blocks

    async def handle_message_during_user_input(
        self, session: "Session", content: str,
        images: list | None = None, files: list | None = None,
    ) -> bool:
        """Consume a plain message that arrived while a turn is paused on an
        AskUserQuestion: feed it back as the dangling tool_use's tool_result and
        unblock the stdout reader.  Returns False (message untouched) if nothing
        is actually pending."""
        state = self._get_state(session)
        pending_ids = list(state.tool_waiting_events.keys())
        if not pending_ids:
            return False

        first_content = self._build_tool_result_content(content, images, files)
        blocks: list = []
        for idx, tid in enumerate(pending_ids):
            blocks.append({
                "type": "tool_result",
                "tool_use_id": tid,
                # The first dangling ask receives the user's free-text reply;
                # any others (multiple AskUserQuestion in one turn) are cancelled
                # since a single message can only answer one.
                "content": first_content if idx == 0
                else json.dumps({"cancelled": True}, ensure_ascii=False),
            })

        await self._write_stream_json(session, {
            "type": "user",
            "message": {"role": "user", "content": blocks},
        })
        session.is_streaming = True
        session.last_activity = time.time()

        # Drop the resolved interactions from the registry so they no longer
        # appear as pending (and a stale user_input_response can't double-answer
        # the same tool_use).
        for tid in pending_ids:
            rid = state.tool_waiting_interactions.pop(tid, None)
            if rid:
                try:
                    await INTERACTIONS.resolve(rid)
                except Exception:
                    pass

        # Unblock the stdout reader.
        for tid in pending_ids:
            ev = state.tool_waiting_events.pop(tid, None)
            if ev is not None:
                ev.set()

        log.info("[%s] Plain message consumed as AskUserQuestion answer (%d tool_use resolved)",
                 session.session_id, len(pending_ids))
        return True

    async def _cancel_pending_user_input(self, session: "Session") -> None:
        """Write a {"cancelled": true} tool_result for every dangling
        AskUserQuestion tool_use so the in-flight process can persist a clean
        history (no orphan tool_use to poison --resume), then unblock the reader.

        Writes straight to the live stdin — must NOT respawn — and gives Claude a
        brief beat to flush the tool_result into its JSONL before the caller
        terminates the process."""
        state = self._get_state(session)
        pending_ids = list(state.tool_waiting_events.keys())
        if not pending_ids:
            return

        blocks = [{
            "type": "tool_result",
            "tool_use_id": tid,
            "content": json.dumps({"cancelled": True}, ensure_ascii=False),
        } for tid in pending_ids]

        try:
            if (state.proc is not None and state.proc.returncode is None
                    and state.proc.stdin is not None):
                payload = json.dumps({
                    "type": "user",
                    "message": {"role": "user", "content": blocks},
                }) + "\n"
                state.proc.stdin.write(payload.encode("utf-8"))
                await state.proc.stdin.drain()
                # Let Claude persist the tool_result to its session JSONL.
                await asyncio.sleep(0.3)
        except Exception as exc:
            log.warning("[%s] failed to write cancelled tool_result(s): %s",
                        session.session_id, exc)

        for tid in pending_ids:
            rid = state.tool_waiting_interactions.pop(tid, None)
            if rid:
                try:
                    await INTERACTIONS.resolve(rid)
                except Exception:
                    pass

        for tid in pending_ids:
            ev = state.tool_waiting_events.pop(tid, None)
            if ev is not None:
                ev.set()
        log.info("[%s] Cancelled %d dangling AskUserQuestion tool_use(s)",
                 session.session_id, len(pending_ids))
