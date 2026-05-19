"""
Default values for BridgeConfig.
These cover all hardcoded paths identified in AUDIT_HARDCODED.md.
"""
from pathlib import Path

DEFAULTS: dict = {
    "search": {
        "enabled": True,
        "tokenizer": "auto",
        "index_path": str(Path("~/.claude-bridge-runtime/search.db").expanduser()),
        "max_index_size_mb": 1024,
        "ingest_on_startup": True,
        "ingest_startup_delay_sec": 2.0,
        "ingest_bulk_pause_every_files": 50,
        "ingest_bulk_pause_sec": 0.01,
        "ingest_idle_recent_window_sec": 3.0,
        "ingest_idle_pause_sec": 0.05,
        "watch_enabled": True,
        "watch_interval_sec": 2,
    },
    "sources": {
        "claude_enabled": "auto",
        "claude_projects_dir": str(Path("~/.claude/projects").expanduser()),
        "codex_enabled": "auto",
        "codex_sessions_dir": str(Path("~/.codex/sessions").expanduser()),
        "ollama_enabled": "no",
    },
    "server": {
        "bind": "0.0.0.0",
        "port": 8766,
        "push_firebase_credentials_path": None,
    },
}
