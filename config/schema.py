"""
Config dataclass schema for claude-bridge.
Plain dataclasses — no pydantic dependency required.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
from typing import Literal, Optional


@dataclass
class SearchConfig:
    enabled: bool = True
    tokenizer: Literal["auto", "trigram", "unicode61"] = "auto"
    index_path: Path = field(
        default_factory=lambda: Path("~/.claude-bridge-runtime/search.db").expanduser()
    )
    max_index_size_mb: int = 1024
    ingest_on_startup: bool = True
    ingest_startup_delay_sec: float = 2.0
    ingest_bulk_pause_every_files: int = 50
    ingest_bulk_pause_sec: float = 0.01
    ingest_idle_recent_window_sec: float = 3.0
    ingest_idle_pause_sec: float = 0.05
    watch_enabled: bool = True
    watch_interval_sec: int = 2
    # When False (BRIDGE_SEARCH_FULL_INDEX=0), only session metadata is indexed;
    # message body rows are skipped.  Expected to reduce search.db from ~169MB to <5MB.
    # App-side SQLite FTS handles local message search; bridge index is for session listing only.
    full_index: bool = True


@dataclass
class SourcesConfig:
    claude_enabled: Literal["auto", "yes", "no"] = "auto"
    claude_projects_dir: Path = field(
        default_factory=lambda: Path("~/.claude/projects").expanduser()
    )
    codex_enabled: Literal["auto", "yes", "no"] = "auto"
    codex_sessions_dir: Path = field(
        default_factory=lambda: Path("~/.codex/sessions").expanduser()
    )
    ollama_enabled: Literal["auto", "yes", "no"] = "no"


@dataclass
class ServerConfig:
    bind: str = "0.0.0.0"
    port: int = 8766
    push_firebase_credentials_path: Optional[Path] = None


@dataclass
class BridgeConfig:
    search: SearchConfig = field(default_factory=SearchConfig)
    sources: SourcesConfig = field(default_factory=SourcesConfig)
    server: ServerConfig = field(default_factory=ServerConfig)
    config_file_path: Optional[Path] = None
