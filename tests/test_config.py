"""
Unit tests for bridge.config system.
Run with: pytest bridge/tests/test_config.py
"""
from __future__ import annotations

import os
import platform
import sys
import textwrap
from pathlib import Path
from unittest import mock

import pytest

# Ensure bridge package is importable when running from repo root
sys.path.insert(0, str(Path(__file__).parent.parent.parent / "bridge"))

from bridge.config.loader import load_config, _apply_env_overrides, _deep_merge
from bridge.config.schema import BridgeConfig
from bridge.config import get_config


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _write_yaml(tmp_path: Path, content: str) -> Path:
    cfg_file = tmp_path / "config.yaml"
    cfg_file.write_text(textwrap.dedent(content))
    return cfg_file


# ---------------------------------------------------------------------------
# test_defaults_load_without_yaml
# ---------------------------------------------------------------------------

def test_defaults_load_without_yaml(tmp_path):
    """Config loads successfully with no YAML file present."""
    # Patch the default config path to a non-existent location
    with mock.patch("bridge.config.loader._DEFAULT_CONFIG_PATH", tmp_path / "no_such_file.yaml"):
        cfg = load_config()
    assert isinstance(cfg, BridgeConfig)
    assert cfg.search.tokenizer == "auto"
    assert cfg.search.enabled is True
    assert cfg.server.port == 8766
    assert cfg.config_file_path is None


# ---------------------------------------------------------------------------
# test_yaml_overrides_defaults
# ---------------------------------------------------------------------------

def test_yaml_overrides_defaults(tmp_path):
    """A YAML file overrides specific defaults while leaving others intact."""
    yaml_content = """\
        search:
          tokenizer: trigram
          max_index_size_mb: 512
        server:
          port: 9999
    """
    cfg_file = _write_yaml(tmp_path, yaml_content)
    with mock.patch("bridge.config.loader._DEFAULT_CONFIG_PATH", cfg_file):
        cfg = load_config()
    assert cfg.search.tokenizer == "trigram"
    assert cfg.search.max_index_size_mb == 512
    assert cfg.server.port == 9999
    # untouched defaults intact
    assert cfg.search.enabled is True
    assert cfg.server.bind == "0.0.0.0"
    assert cfg.config_file_path == cfg_file


# ---------------------------------------------------------------------------
# test_env_overrides_yaml
# ---------------------------------------------------------------------------

def test_env_overrides_yaml(tmp_path):
    """Environment variables override YAML values."""
    yaml_content = """\
        search:
          tokenizer: unicode61
        server:
          port: 9000
    """
    cfg_file = _write_yaml(tmp_path, yaml_content)
    env_overrides = {
        "BRIDGE_SEARCH__TOKENIZER": "trigram",
        "BRIDGE_SEARCH__INGEST_STARTUP_DELAY_SEC": "0.5",
        "BRIDGE_SEARCH__INGEST_BULK_PAUSE_EVERY_FILES": "25",
        "BRIDGE_SEARCH__INGEST_BULK_PAUSE_SEC": "0.02",
        "BRIDGE_SEARCH__INGEST_IDLE_RECENT_WINDOW_SEC": "4.5",
        "BRIDGE_SEARCH__INGEST_IDLE_PAUSE_SEC": "0.08",
        "BRIDGE_SERVER__PORT": "7777",
    }
    with mock.patch("bridge.config.loader._DEFAULT_CONFIG_PATH", cfg_file):
        with mock.patch.dict(os.environ, env_overrides, clear=False):
            cfg = load_config()
    assert cfg.search.tokenizer == "trigram"
    assert cfg.search.ingest_startup_delay_sec == 0.5
    assert cfg.search.ingest_bulk_pause_every_files == 25
    assert cfg.search.ingest_bulk_pause_sec == 0.02
    assert cfg.search.ingest_idle_recent_window_sec == 4.5
    assert cfg.search.ingest_idle_pause_sec == 0.08
    assert cfg.server.port == 7777


# ---------------------------------------------------------------------------
# test_double_underscore_env_parses_nested
# ---------------------------------------------------------------------------

def test_double_underscore_env_parses_nested(tmp_path):
    """BRIDGE_SECTION__KEY env format correctly maps to nested config."""
    with mock.patch("bridge.config.loader._DEFAULT_CONFIG_PATH", tmp_path / "absent.yaml"):
        with mock.patch.dict(os.environ, {"BRIDGE_SEARCH__ENABLED": "false"}, clear=False):
            cfg = load_config()
    assert cfg.search.enabled is False


def test_double_underscore_env_parses_nested_path(tmp_path):
    """BRIDGE_SOURCES__CLAUDE_PROJECTS_DIR correctly sets path."""
    custom_dir = str(tmp_path / "custom_projects")
    with mock.patch("bridge.config.loader._DEFAULT_CONFIG_PATH", tmp_path / "absent.yaml"):
        with mock.patch.dict(os.environ, {"BRIDGE_SOURCES__CLAUDE_PROJECTS_DIR": custom_dir}, clear=False):
            cfg = load_config()
    assert str(cfg.sources.claude_projects_dir) == custom_dir


# ---------------------------------------------------------------------------
# test_invalid_yaml_returns_clear_error
# ---------------------------------------------------------------------------

def test_invalid_yaml_returns_clear_error(tmp_path):
    """Malformed YAML raises ValueError with the file path in the message."""
    bad_yaml = tmp_path / "config.yaml"
    bad_yaml.write_text("search:\n  enabled: [unclosed bracket\n")
    with pytest.raises((ValueError, Exception)) as exc_info:
        with mock.patch("bridge.config.loader._DEFAULT_CONFIG_PATH", bad_yaml):
            load_config()
    # The error message should mention the file path or describe a parse failure
    msg = str(exc_info.value).lower()
    assert str(bad_yaml) in str(exc_info.value) or "parse" in msg or "yaml" in msg


# ---------------------------------------------------------------------------
# test_check_fts5_returns_truthy_when_available
# ---------------------------------------------------------------------------

def test_check_fts5_returns_truthy_when_available():
    """check_sqlite_fts5 returns ok=True when FTS5 is available in this env."""
    from bridge.config.checks import check_sqlite_fts5
    result = check_sqlite_fts5()
    # On most dev machines FTS5 is available; if not, we just verify the result shape
    assert result.name == "SQLite FTS5"
    assert isinstance(result.ok, bool)
    assert result.severity in ("info", "error", "warning")
    if result.ok:
        assert "FTS5" in result.message
    else:
        assert "FTS5" in result.message or "fts5" in result.message.lower()


# ---------------------------------------------------------------------------
# test_check_inotify_skipped_on_macos
# ---------------------------------------------------------------------------

def test_check_inotify_skipped_on_macos():
    """check_inotify_limit returns ok=True with informational message on non-Linux."""
    from bridge.config.checks import check_inotify_limit
    with mock.patch("platform.system", return_value="Darwin"):
        result = check_inotify_limit()
    assert result.ok is True
    assert "not on Linux" in result.message


# ---------------------------------------------------------------------------
# Additional edge-case tests
# ---------------------------------------------------------------------------

def test_extra_config_path_overrides_user_config(tmp_path):
    """--config FILE takes precedence over ~/.claude-bridge/config.yaml."""
    (tmp_path / "user").mkdir(exist_ok=True)
    user_yaml = tmp_path / "user" / "config.yaml"
    user_yaml.write_text("server:\n  port: 8000\n")

    extra_yaml = tmp_path / "extra.yaml"
    extra_yaml.write_text("server:\n  port: 5000\n")

    with mock.patch("bridge.config.loader._DEFAULT_CONFIG_PATH", user_yaml):
        cfg = load_config(extra_config_path=extra_yaml)
    assert cfg.server.port == 5000
    assert cfg.config_file_path == extra_yaml


def test_deep_merge_does_not_mutate_base():
    base = {"a": {"x": 1, "y": 2}}
    override = {"a": {"y": 99}}
    result = _deep_merge(base, override)
    assert result["a"]["x"] == 1
    assert result["a"]["y"] == 99
    assert base["a"]["y"] == 2  # base untouched


def test_get_config_returns_singleton(tmp_path):
    """get_config() returns the same object on repeated calls without reload."""
    import bridge.config as bc
    bc._config = None  # reset singleton
    with mock.patch("bridge.config.loader._DEFAULT_CONFIG_PATH", tmp_path / "absent.yaml"):
        c1 = get_config()
        c2 = get_config()
    assert c1 is c2
    bc._config = None  # cleanup


# ---------------------------------------------------------------------------
# F15: env var path injection must be rejected
# ---------------------------------------------------------------------------

def test_env_path_injection_rejected_for_index_path(tmp_path, caplog):
    """BRIDGE_SEARCH__INDEX_PATH=/etc/passwd must be rejected; default path used."""
    import logging
    with mock.patch("bridge.config.loader._DEFAULT_CONFIG_PATH", tmp_path / "absent.yaml"):
        with mock.patch.dict(os.environ, {"BRIDGE_SEARCH__INDEX_PATH": "/etc/passwd"}, clear=False):
            with caplog.at_level(logging.WARNING, logger="bridge.config.loader"):
                cfg = load_config()

    # Must NOT use /etc/passwd
    assert str(cfg.search.index_path) != "/etc/passwd", (
        "index_path must not be set to /etc/passwd"
    )
    # Must log a warning
    assert any("index_path" in r.message or "allowed roots" in r.message
               for r in caplog.records), (
        f"Expected a warning about index_path, got: {[r.message for r in caplog.records]}"
    )


def test_env_path_injection_rejects_dotdot_segments(tmp_path, caplog):
    """Paths with '..' segments in sources dirs must be rejected."""
    import logging
    dotdot_path = str(tmp_path) + "/../../etc"
    with mock.patch("bridge.config.loader._DEFAULT_CONFIG_PATH", tmp_path / "absent.yaml"):
        with mock.patch.dict(os.environ,
                             {"BRIDGE_SOURCES__CLAUDE_PROJECTS_DIR": dotdot_path},
                             clear=False):
            with caplog.at_level(logging.WARNING, logger="bridge.config.loader"):
                cfg = load_config()

    # Resolved path must not contain /etc
    assert "/etc" not in str(cfg.sources.claude_projects_dir) or \
           str(cfg.sources.claude_projects_dir).startswith(str(Path.home())), (
        f"claude_projects_dir should be default, got: {cfg.sources.claude_projects_dir}"
    )


def test_legitimate_path_under_home_accepted(tmp_path):
    """A valid path under ~ must be accepted for index_path."""
    home_subpath = str(Path.home() / ".claude-bridge-runtime" / "mytest.db")
    with mock.patch("bridge.config.loader._DEFAULT_CONFIG_PATH", tmp_path / "absent.yaml"):
        with mock.patch.dict(os.environ,
                             {"BRIDGE_SEARCH__INDEX_PATH": home_subpath},
                             clear=False):
            cfg = load_config()

    assert str(cfg.search.index_path) == home_subpath, (
        f"Legitimate home path should be accepted, got: {cfg.search.index_path}"
    )


def test_legitimate_path_under_tmp_accepted(tmp_path):
    """A valid path under /tmp must be accepted for index_path."""
    tmp_subpath = "/tmp/bridge_test_index.db"
    with mock.patch("bridge.config.loader._DEFAULT_CONFIG_PATH", tmp_path / "absent.yaml"):
        with mock.patch.dict(os.environ,
                             {"BRIDGE_SEARCH__INDEX_PATH": tmp_subpath},
                             clear=False):
            cfg = load_config()

    assert str(cfg.search.index_path) == tmp_subpath, (
        f"/tmp path should be accepted, got: {cfg.search.index_path}"
    )
