"""
Backend contract tests — verifies all backend implementations conform to the
Backend ABC contract and that get_resumable_sessions returns the correct schema.

Run: python3 -m pytest tests/test_backend_contract.py -v --tb=short
"""
from __future__ import annotations

import asyncio
import inspect
from unittest.mock import AsyncMock, MagicMock, patch

import pytest


# ── helpers ───────────────────────────────────────────────────────────────────

def make_session(**kwargs):
    """Build a minimal mock Session for tests that need one."""
    session = MagicMock()
    session.session_id = "test-session-0001"
    session.resume_id = None
    session.is_streaming = False
    session.is_stopping = False
    session.accumulated_text = ""
    session.last_activity = 0.0
    session.ws_ref = None
    session.name = "test"
    session.context_used = 0
    session.context_max = 0
    for k, v in kwargs.items():
        setattr(session, k, v)
    return session


def _all_backend_classes():
    """Return all concrete Backend subclasses."""
    from backends.claude_cli import ClaudeCliBackend
    from backends.codex_appserver import CodexAppServerBackend
    from backends.gemini_cli import GeminiCliBackend
    from backends.ollama import OllamaBackend
    return [ClaudeCliBackend, CodexAppServerBackend, GeminiCliBackend, OllamaBackend]


def _make_instance(cls):
    """Instantiate a backend class with safe defaults (no real subprocesses or disk scans)."""
    from backends.claude_cli import ClaudeCliBackend
    from backends.codex_appserver import CodexAppServerBackend

    if cls is ClaudeCliBackend:
        b = cls()
        # Point away from any real Claude projects directory so _scan_local_sessions_sync
        # hits FileNotFoundError and returns [] without touching the real filesystem.
        b._claude_projects_dir = "/nonexistent/__test_contract_dir__"
        return b

    if cls is CodexAppServerBackend:
        b = cls.__new__(cls)
        b.__init__("/usr/bin/codex")
        # Point away from real ~/.codex/session_index.jsonl
        b._native_session_index_path = "/nonexistent/__test_session_index__.jsonl"
        return b

    # GeminiCliBackend and OllamaBackend take no required args
    return cls()


# ── Backend ABC interface ──────────────────────────────────────────────────────

def test_backend_abc_required_methods():
    """Backend ABC must declare spawn/send/stop/clear as abstractmethods."""
    from backends.base import Backend
    abstract = {
        name for name, m in inspect.getmembers(Backend)
        if getattr(m, "__isabstractmethod__", False)
    }
    assert {"spawn", "send", "stop", "clear"} <= abstract


def test_backend_abc_optional_defaults():
    """Backend ABC must provide default implementations for optional methods."""
    from backends.base import Backend
    for name in (
        "close", "supports_resume", "fetch_usage",
        "get_resumable_sessions", "load_session_history",
        "_begin_send", "find_session_file", "detect_turn_end",
        "get_pid", "kill_session_proc",
    ):
        assert hasattr(Backend, name), f"Backend is missing default method: {name}"


def test_states_mixin_interface():
    """_StatesMixin must expose _get_state and _state_factory."""
    from backends.base import _StatesMixin
    assert hasattr(_StatesMixin, "_get_state")
    assert hasattr(_StatesMixin, "_state_factory")


# ── All backends: inheritance and method presence ──────────────────────────────

@pytest.mark.parametrize("cls", _all_backend_classes())
def test_backend_inherits_base(cls):
    from backends.base import Backend
    assert issubclass(cls, Backend), f"{cls.__name__} must inherit Backend"


@pytest.mark.parametrize("cls", _all_backend_classes())
def test_backend_has_required_abstract_methods(cls):
    for method in ("spawn", "send", "stop", "clear"):
        assert hasattr(cls, method), f"{cls.__name__} missing required method: {method}"


@pytest.mark.parametrize("cls", _all_backend_classes())
def test_backend_has_contract_methods(cls):
    """All backends must expose (inherited or overridden) contract methods."""
    for method in (
        "find_session_file", "detect_turn_end",
        "get_pid", "kill_session_proc",
        "supports_resume", "get_resumable_sessions",
    ):
        assert hasattr(cls, method), f"{cls.__name__} missing contract method: {method}"


# ── _StatesMixin backends must override _state_factory ───────────────────────

def _states_mixin_backends():
    from backends.base import _StatesMixin
    return [cls for cls in _all_backend_classes() if issubclass(cls, _StatesMixin)]


@pytest.mark.parametrize("cls", _states_mixin_backends())
def test_states_mixin_factory_overridden(cls):
    """Every _StatesMixin subclass must override _state_factory in its own class body."""
    assert "_state_factory" in vars(cls), (
        f"{cls.__name__} uses _StatesMixin but did not override _state_factory"
    )


# ── JsonRpcPlumber interface ───────────────────────────────────────────────────

def test_jsonrpc_plumber_interface():
    """JsonRpcPlumber must expose all required methods."""
    from backends.jsonrpc import JsonRpcPlumber
    p = JsonRpcPlumber("test")
    for name in ("write", "request", "notify", "dispatch_response", "fail_all"):
        assert hasattr(p, name), f"JsonRpcPlumber missing: {name}"


def test_codex_backend_uses_jsonrpc_plumber():
    """CodexAppServerBackend must hold a JsonRpcPlumber on _rpc_plumber."""
    from backends.codex_appserver import CodexAppServerBackend
    from backends.jsonrpc import JsonRpcPlumber
    b = CodexAppServerBackend.__new__(CodexAppServerBackend)
    b.__init__("/usr/bin/codex")
    assert isinstance(b._rpc_plumber, JsonRpcPlumber)


def test_gemini_state_uses_jsonrpc_plumber():
    """_GeminiState must hold a JsonRpcPlumber on .plumber."""
    from backends.gemini_cli import _GeminiState
    from backends.jsonrpc import JsonRpcPlumber
    state = _GeminiState()
    assert isinstance(state.plumber, JsonRpcPlumber)


# ── _begin_send contract ───────────────────────────────────────────────────────

def test_begin_send_returns_false_when_streaming():
    """`_begin_send` must return False and not modify state when session is already streaming."""
    from backends.ollama import OllamaBackend

    async def _run():
        b = OllamaBackend()
        session = make_session(is_streaming=True)
        with patch("backends.events.send_event", new=AsyncMock()):
            result = await b._begin_send(session)
        return result, session.is_streaming

    result, still_streaming = asyncio.run(_run())
    assert result is False
    assert still_streaming is True  # must not have changed


def test_begin_send_returns_true_and_sets_streaming():
    """`_begin_send` must return True and set is_streaming=True when idle."""
    from backends.ollama import OllamaBackend

    async def _run():
        b = OllamaBackend()
        session = make_session(is_streaming=False)
        with patch("backends.events.send_event", new=AsyncMock()):
            result = await b._begin_send(session)
        return result, session.is_streaming, session.accumulated_text

    result, is_streaming, accumulated = asyncio.run(_run())
    assert result is True
    assert is_streaming is True
    assert accumulated == ""


# ── get_resumable_sessions schema contract ────────────────────────────────────

REQUIRED_SESSION_KEYS = {"id", "name", "resume_id", "last_used", "cwd"}


@pytest.mark.parametrize("cls", _all_backend_classes())
def test_resumable_sessions_returns_list(cls):
    """get_resumable_sessions() must return a list (even when empty)."""
    async def _run():
        b = _make_instance(cls)
        return await b.get_resumable_sessions(limit=5)

    result = asyncio.run(_run())
    assert isinstance(result, list), (
        f"{cls.__name__}.get_resumable_sessions() must return list, got {type(result)}"
    )


@pytest.mark.parametrize("cls", _all_backend_classes())
def test_resumable_sessions_schema(cls):
    """Every item from get_resumable_sessions() must contain the required keys with correct types."""
    async def _run():
        b = _make_instance(cls)
        return await b.get_resumable_sessions(limit=5)

    result = asyncio.run(_run())
    for item in result:
        missing = REQUIRED_SESSION_KEYS - set(item.keys())
        assert not missing, (
            f"{cls.__name__}.get_resumable_sessions() item missing keys: {missing}\nItem: {item}"
        )
        assert isinstance(item["id"], str), (
            f"{cls.__name__}: 'id' must be str, got {type(item['id'])}"
        )
        assert isinstance(item["name"], str), (
            f"{cls.__name__}: 'name' must be str, got {type(item['name'])}"
        )
        assert isinstance(item["resume_id"], str), (
            f"{cls.__name__}: 'resume_id' must be str, got {type(item['resume_id'])}"
        )
        assert isinstance(item["last_used"], int), (
            f"{cls.__name__}: 'last_used' must be int, got {type(item['last_used'])}"
        )
        assert isinstance(item["cwd"], str), (
            f"{cls.__name__}: 'cwd' must be str, got {type(item['cwd'])}"
        )


# ── Ollama-specific contracts ──────────────────────────────────────────────────

def test_ollama_supports_resume_false():
    """OllamaBackend must return False for supports_resume() (in-memory only)."""
    from backends.ollama import OllamaBackend
    assert OllamaBackend().supports_resume() is False


# ── MSG_SESSION_BUSY constant ──────────────────────────────────────────────────

def test_msg_session_busy_constant():
    """MSG_SESSION_BUSY must be a non-empty string."""
    from backends.base import MSG_SESSION_BUSY
    assert isinstance(MSG_SESSION_BUSY, str)
    assert len(MSG_SESSION_BUSY) > 0


# ── find_session_file / detect_turn_end return types ─────────────────────────

@pytest.mark.parametrize("cls", _all_backend_classes())
def test_find_session_file_returns_str_or_none(cls):
    """find_session_file() must return str or None, never raise."""
    b = _make_instance(cls)
    result = b.find_session_file("nonexistent-id-00000000")
    assert result is None or isinstance(result, str), (
        f"{cls.__name__}.find_session_file() must return str|None, got {type(result)}"
    )


@pytest.mark.parametrize("cls", _all_backend_classes())
def test_detect_turn_end_returns_false_for_empty(cls):
    """detect_turn_end([]) must return False."""
    b = _make_instance(cls)
    assert b.detect_turn_end([]) is False


@pytest.mark.parametrize("cls", _all_backend_classes())
def test_detect_turn_end_returns_false_for_unknown_lines(cls):
    """detect_turn_end() must return False for unrecognised line types."""
    b = _make_instance(cls)
    assert b.detect_turn_end([{"type": "unknown_event"}]) is False


# ── get_pid / kill_session_proc return types ──────────────────────────────────

@pytest.mark.parametrize("cls", _all_backend_classes())
def test_get_pid_returns_int_or_none(cls):
    """get_pid() must return int or None for a fresh session with no process."""
    b = _make_instance(cls)
    session = make_session()
    result = b.get_pid(session)
    assert result is None or isinstance(result, int), (
        f"{cls.__name__}.get_pid() must return int|None, got {type(result)}"
    )


@pytest.mark.parametrize("cls", _all_backend_classes())
def test_kill_session_proc_returns_bool(cls):
    """kill_session_proc() must return bool (False when no process is running)."""
    b = _make_instance(cls)
    session = make_session()
    result = b.kill_session_proc(session)
    assert isinstance(result, bool), (
        f"{cls.__name__}.kill_session_proc() must return bool, got {type(result)}"
    )
