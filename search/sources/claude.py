import hashlib
import json
import logging
from pathlib import Path
from typing import Iterator, Optional

from .base import SearchableMessage
from ..ingest.display_name import is_framework_noise
from .._strip_wrappers import strip_framework_wrappers

log = logging.getLogger(__name__)

_CLAUDE_ROOT = Path.home() / '.claude' / 'projects'

# Record types that carry no search-worthy content; skip immediately.
_NOISE_TYPES = frozenset({
    'attachment',
    'permission-mode',
    'file-history-snapshot',
    'deferred_tools_delta',
    'progress',
    'last-prompt',
    'queue-operation',
    'system',
})


def _extract_text(content) -> Optional[str]:
    """Return stripped text from a message content field, or None if no text."""
    if isinstance(content, str):
        text = content.strip()
    elif isinstance(content, list):
        parts = []
        for block in content:
            if not isinstance(block, dict):
                continue
            if block.get('type') == 'text':
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


class ClaudeJsonlSource:
    name = 'claude'

    @property
    def watch_root(self) -> Path:
        """Return the root directory this source watches (respects module-level override)."""
        return _CLAUDE_ROOT

    def is_enabled(self) -> bool:
        return _CLAUDE_ROOT.is_dir()

    def discover(self) -> Iterator[Path]:
        if not _CLAUDE_ROOT.is_dir():
            return
        # Main session files
        for p in _CLAUDE_ROOT.glob('*/*.jsonl'):
            yield p.resolve()
        # Subagent files
        for p in _CLAUDE_ROOT.glob('*/*/subagents/agent-*.jsonl'):
            yield p.resolve()

    def iter_messages(
        self, path: Path, start_offset: int = 0
    ) -> Iterator[tuple[SearchableMessage, int]]:
        is_sub = '/subagents/' in str(path)
        session_id = self.session_id_for(path)
        cwd_cache: Optional[str] = None
        cwd_resolved = False

        try:
            fh = open(path, 'rb')
        except OSError as exc:
            log.warning('ClaudeJsonlSource: cannot open %s: %s', path, exc)
            return

        with fh:
            fh.seek(start_offset)
            offset = start_offset
            line_num = 0

            while True:
                raw = fh.readline()
                if not raw:
                    break
                # Incomplete line — no trailing newline, stop and let caller resume
                if not raw.endswith(b'\n'):
                    break
                next_offset = offset + len(raw)
                line_num += 1

                try:
                    record = json.loads(raw)
                except json.JSONDecodeError:
                    log.warning('ClaudeJsonlSource: bad JSON at %s offset %d', path, offset)
                    offset = next_offset
                    continue

                rec_type = record.get('type', '')

                # Cache cwd from first record that has it
                if not cwd_resolved and 'cwd' in record:
                    cwd_cache = record.get('cwd')
                    cwd_resolved = True

                # Skip noise types
                if rec_type in _NOISE_TYPES:
                    offset = next_offset
                    continue

                # Only emit user/assistant
                if rec_type not in ('user', 'assistant'):
                    offset = next_offset
                    continue

                # Skip internal sidechains for user records
                if rec_type == 'user' and record.get('isSidechain', False):
                    offset = next_offset
                    continue

                msg = record.get('message', {})
                if not isinstance(msg, dict):
                    offset = next_offset
                    continue

                content = msg.get('content')
                text = _extract_text(content)
                if not text:
                    offset = next_offset
                    continue

                msg_uuid = record.get('uuid') or f'{path.stem}:line:{line_num}'
                parent_uuid = record.get('parentUuid')
                timestamp = record.get('timestamp', '')

                yield SearchableMessage(
                    source='claude',
                    session_id=session_id,
                    msg_uuid=msg_uuid,
                    parent_uuid=parent_uuid,
                    role=rec_type,
                    timestamp=timestamp,
                    text=text,
                    is_subagent=is_sub,
                    cwd=cwd_cache,
                ), next_offset

                offset = next_offset

    def head_signature(self, path: Path) -> str:
        try:
            with open(path, 'rb') as fh:
                data = fh.read(4096)
        except OSError:
            return ''
        return hashlib.sha256(data).hexdigest()

    def session_id_for(self, path: Path) -> str:
        if '/subagents/' in str(path):
            # path is: .../projects/<project-dir>/<session-uuid>/subagents/agent-<id>.jsonl
            session_uuid = path.parents[1].name
            agent_id = path.stem.replace('agent-', '')
            return f'claude:{session_uuid}:subagent:{agent_id}'
        return f'claude:{path.stem}'

    def get_session_meta(self, path: Path) -> dict:
        project_dir = ''
        first_ts = ''
        display_name = ''
        cwd = None

        if '/subagents/' in str(path):
            project_dir = str(path.parents[2])
        else:
            project_dir = str(path.parent)

        try:
            with open(path, 'rb') as fh:
                for raw in fh:
                    if not raw.strip():
                        continue
                    try:
                        record = json.loads(raw)
                    except json.JSONDecodeError:
                        continue

                    if cwd is None and 'cwd' in record:
                        cwd = record.get('cwd')

                    if not first_ts and 'timestamp' in record:
                        first_ts = record.get('timestamp', '')

                    if not display_name and record.get('type') == 'user':
                        msg = record.get('message', {})
                        content = msg.get('content') if isinstance(msg, dict) else None
                        text = _extract_text(content) if content is not None else None
                        if text and not is_framework_noise(text):
                            display_name = ' '.join(text.split())[:80]

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
