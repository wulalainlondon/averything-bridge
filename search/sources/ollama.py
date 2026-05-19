from pathlib import Path
from typing import Iterator, Literal

from .base import SearchableMessage


class OllamaSource:
    name: Literal['ollama'] = 'ollama'

    def is_enabled(self) -> bool:
        return False

    def discover(self) -> Iterator[Path]:
        if False:
            yield

    def iter_messages(
        self, path: Path, start_offset: int = 0
    ) -> Iterator[tuple[SearchableMessage, int]]:
        raise NotImplementedError('OllamaSource pending v2 — needs jsonl persistence first')

    def head_signature(self, path: Path) -> str:
        raise NotImplementedError('OllamaSource pending v2 — needs jsonl persistence first')

    def session_id_for(self, path: Path) -> str:
        raise NotImplementedError('OllamaSource pending v2 — needs jsonl persistence first')

    def get_session_meta(self, path: Path) -> dict:
        raise NotImplementedError('OllamaSource pending v2 — needs jsonl persistence first')
