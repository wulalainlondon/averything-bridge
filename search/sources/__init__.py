from typing import TYPE_CHECKING, List

from .base import SearchableMessage, SearchSource
from .claude import ClaudeJsonlSource
from .codex import CodexSessionSource
from .ollama import OllamaSource

if TYPE_CHECKING:
    from ...config import BridgeConfig  # noqa: F401 — type-only import


def registered_sources(config: 'BridgeConfig | None' = None) -> List[SearchSource]:
    """Return all registered source adapters.

    When config is None or a source has enabled='auto', the source is included
    if is_enabled() returns True (path-based auto-detection).
    Pass config with explicit disable flags to suppress specific sources.
    """
    root_dir = getattr(config, 'root_dir', '') if config is not None else ''
    candidates = [
        ClaudeJsonlSource(root_dir=root_dir),
        CodexSessionSource(root_dir=root_dir),
        OllamaSource(),
    ]

    if config is None:
        return [src for src in candidates if src.is_enabled()]

    # Map source name → enabled flag from config.sources
    sources_cfg = config.sources
    enabled_flags = {
        'claude': getattr(sources_cfg, 'claude_enabled', 'auto'),
        'codex': getattr(sources_cfg, 'codex_enabled', 'auto'),
        'ollama': getattr(sources_cfg, 'ollama_enabled', 'auto'),
    }

    result = []
    for src in candidates:
        flag = enabled_flags.get(src.name, 'auto')
        if flag == 'yes':
            result.append(src)
        elif flag == 'no':
            pass  # explicitly disabled — skip
        else:  # 'auto'
            if src.is_enabled():
                result.append(src)

    return result


__all__ = [
    'SearchableMessage',
    'SearchSource',
    'ClaudeJsonlSource',
    'CodexSessionSource',
    'OllamaSource',
    'registered_sources',
]
