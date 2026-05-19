from dataclasses import dataclass
from typing import Literal, Iterator, Protocol, Optional
from pathlib import Path


@dataclass(frozen=True)
class SearchableMessage:
    source: Literal['claude', 'codex', 'ollama']
    session_id: str           # format: "{source}:{native_id}" — unique across backends
    msg_uuid: str             # unique id within original storage (uuid field, fallback line number)
    parent_uuid: Optional[str]
    role: Literal['user', 'assistant', 'system']
    timestamp: str            # ISO8601, empty string if missing
    text: str                 # stripped plain text (multiple text blocks joined with \n)
    is_subagent: bool
    cwd: Optional[str]        # session working directory


class SearchSource(Protocol):
    name: Literal['claude', 'codex', 'ollama']

    def is_enabled(self) -> bool:
        """Check if backend is available on this machine (paths exist, etc.)"""

    def discover(self) -> Iterator[Path]:
        """Yield absolute paths to all indexable jsonl files (including subagents)"""

    def iter_messages(
        self, path: Path, start_offset: int = 0
    ) -> Iterator[tuple[SearchableMessage, int]]:
        """Yield (message, next_offset) starting from start_offset byte position.
        next_offset is the file byte offset after consuming the line (including newline).
        Incomplete last lines (no trailing newline) are not emitted."""

    def head_signature(self, path: Path) -> str:
        """Read first 4KB and return sha256 hex; used to detect file rotation/rewrite"""

    def session_id_for(self, path: Path) -> str:
        """Derive session_id (with source prefix) from path"""

    def get_session_meta(self, path: Path) -> dict:
        """Return {'cwd': str|None, 'project_dir': str, 'first_ts': str, 'display_name': str}"""
