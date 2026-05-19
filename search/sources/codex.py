import hashlib
import json
import logging
from pathlib import Path
from typing import Iterator, Optional

from .base import SearchableMessage
from ..ingest.display_name import is_framework_noise
from .._strip_wrappers import strip_framework_wrappers

log = logging.getLogger(__name__)

_CODEX_ROOT = Path.home() / '.codex' / 'sessions'


def _extract_text_codex(content) -> Optional[str]:
    """Extract text from Codex payload content field."""
    if isinstance(content, str):
        text = content.strip()
    elif isinstance(content, list):
        parts = []
        for block in content:
            if not isinstance(block, dict):
                continue
            # Codex uses 'input_text' and 'text' block types
            btype = block.get('type', '')
            if btype in ('text', 'input_text', 'output_text'):
                t = block.get('text', '').strip()
                if t:
                    parts.append(t)
        text = '\n'.join(parts) if parts else None
    else:
        return None
    if not text:
        return None
    text = strip_framework_wrappers(text)
    return text or None


def _is_codex_bootstrap_text(text: str) -> bool:
    stripped = text.lstrip()
    return (
        stripped.startswith('# AGENTS.md instructions')
        and '<environment_context>' in stripped
        and '<INSTRUCTIONS>' in stripped
    )


class CodexSessionSource:
    name = 'codex'

    @property
    def watch_root(self) -> Path:
        """Return the root directory this source watches (respects module-level override)."""
        return _CODEX_ROOT

    def is_enabled(self) -> bool:
        return _CODEX_ROOT.is_dir()

    def discover(self) -> Iterator[Path]:
        if not _CODEX_ROOT.is_dir():
            return
        # Pattern: ~/.codex/sessions/YYYY/MM/DD/rollout-*.jsonl
        for p in _CODEX_ROOT.glob('*/*/*/rollout-*.jsonl'):
            yield p.resolve()

    def iter_messages(
        self, path: Path, start_offset: int = 0
    ) -> Iterator[tuple[SearchableMessage, int]]:
        session_id = self.session_id_for(path)
        cwd_cache: Optional[str] = None
        cwd_resolved = False

        try:
            fh = open(path, 'rb')
        except OSError as exc:
            log.warning('CodexSessionSource: cannot open %s: %s', path, exc)
            return

        with fh:
            fh.seek(start_offset)
            offset = start_offset
            line_num = 0

            while True:
                raw = fh.readline()
                if not raw:
                    break
                if not raw.endswith(b'\n'):
                    break
                next_offset = offset + len(raw)
                line_num += 1

                try:
                    record = json.loads(raw)
                except json.JSONDecodeError:
                    log.warning('CodexSessionSource: bad JSON at %s offset %d', path, offset)
                    offset = next_offset
                    continue

                try:
                    rec_type = record.get('type', '')

                    # Extract cwd from session_meta
                    if not cwd_resolved and rec_type == 'session_meta':
                        payload = record.get('payload', {})
                        if isinstance(payload, dict):
                            cwd_cache = payload.get('cwd')
                            cwd_resolved = True
                        offset = next_offset
                        continue

                    # Only process response_item records
                    if rec_type != 'response_item':
                        offset = next_offset
                        continue

                    payload = record.get('payload', {})
                    if not isinstance(payload, dict):
                        offset = next_offset
                        continue

                    if payload.get('type') != 'message':
                        offset = next_offset
                        continue

                    role = payload.get('role', '')
                    if role not in ('user', 'assistant'):
                        offset = next_offset
                        continue
                    if role == 'assistant' and payload.get('phase') == 'commentary':
                        offset = next_offset
                        continue

                    content = payload.get('content')
                    text = _extract_text_codex(content)
                    if not text:
                        offset = next_offset
                        continue
                    if role == 'user' and _is_codex_bootstrap_text(text):
                        offset = next_offset
                        continue

                    timestamp = record.get('timestamp', '')
                    msg_uuid = record.get('uuid') or f'{path.stem}:line:{line_num}'
                    parent_uuid = record.get('parent_uuid') or record.get('parentUuid')

                    yield SearchableMessage(
                        source='codex',
                        session_id=session_id,
                        msg_uuid=msg_uuid,
                        parent_uuid=parent_uuid,
                        role=role,
                        timestamp=timestamp,
                        text=text,
                        is_subagent=False,
                        cwd=cwd_cache,
                    ), next_offset

                except Exception as exc:
                    log.warning(
                        'CodexSessionSource: unexpected record shape at %s offset %d: %s',
                        path, offset, exc,
                    )

                offset = next_offset

    def head_signature(self, path: Path) -> str:
        try:
            with open(path, 'rb') as fh:
                data = fh.read(4096)
        except OSError:
            return ''
        return hashlib.sha256(data).hexdigest()

    def session_id_for(self, path: Path) -> str:
        # Stem is like: rollout-2026-04-24T06-57-40-019dbc90-34bb-7410-8c0a-a1c360db06eb
        # Use the full stem as the native id for uniqueness
        return f'codex:{path.stem}'

    def get_session_meta(self, path: Path) -> dict:
        project_dir = str(path.parent)
        first_ts = ''
        display_name = ''
        cwd = None

        try:
            with open(path, 'rb') as fh:
                for raw in fh:
                    if not raw.strip():
                        continue
                    try:
                        record = json.loads(raw)
                    except json.JSONDecodeError:
                        continue

                    try:
                        rec_type = record.get('type', '')

                        if not first_ts:
                            first_ts = record.get('timestamp', '')

                        if cwd is None and rec_type == 'session_meta':
                            payload = record.get('payload', {})
                            if isinstance(payload, dict):
                                cwd = payload.get('cwd')

                        if not display_name and rec_type == 'response_item':
                            payload = record.get('payload', {})
                            if isinstance(payload, dict) and payload.get('type') == 'message':
                                if payload.get('role') == 'user':
                                    content = payload.get('content')
                                    text = _extract_text_codex(content)
                                    if text and not _is_codex_bootstrap_text(text) and not is_framework_noise(text):
                                        display_name = ' '.join(text.split())[:80]

                    except Exception:
                        continue

                    if cwd is not None and first_ts and display_name:
                        break
        except OSError:
            pass

        return {
            'cwd': cwd,
            'project_dir': project_dir,
            'first_ts': first_ts,
            'display_name': display_name,
        }
