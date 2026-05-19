"""Unit tests for bridge.search.sources adapters."""
import hashlib
import json
import types
from pathlib import Path

import pytest

from bridge.search.sources.base import SearchableMessage
from bridge.search.sources.claude import ClaudeJsonlSource
from bridge.search.sources.codex import CodexSessionSource
from bridge.search.sources.ollama import OllamaSource
from bridge.search.sources import registered_sources
from bridge.config.schema import BridgeConfig, SourcesConfig, SearchConfig, ServerConfig


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _write_jsonl(path: Path, records: list) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, 'w', encoding='utf-8') as fh:
        for rec in records:
            fh.write(json.dumps(rec, ensure_ascii=False) + '\n')


def _user_record(text='hello', uuid='u1', parent=None, sidechain=False, cwd='/tmp'):
    return {
        'type': 'user',
        'uuid': uuid,
        'parentUuid': parent,
        'isSidechain': sidechain,
        'timestamp': '2026-01-01T00:00:00.000Z',
        'cwd': cwd,
        'message': {'role': 'user', 'content': text},
    }


def _assistant_record(blocks=None, uuid='a1'):
    content = blocks if blocks is not None else [{'type': 'text', 'text': 'reply'}]
    return {
        'type': 'assistant',
        'uuid': uuid,
        'parentUuid': None,
        'isSidechain': False,
        'timestamp': '2026-01-01T00:01:00.000Z',
        'message': {'role': 'assistant', 'content': content},
    }


def _noise_record(noise_type='permission-mode'):
    return {'type': noise_type, 'permissionMode': 'default'}


def _codex_session_meta(cwd='/tmp/codex'):
    return {
        'timestamp': '2026-01-01T00:00:00.000Z',
        'type': 'session_meta',
        'payload': {'id': 'sess-1', 'cwd': cwd},
    }


def _codex_msg(role='user', text='codex hello', uuid='cu1'):
    return {
        'timestamp': '2026-01-01T00:01:00.000Z',
        'type': 'response_item',
        'uuid': uuid,
        'payload': {
            'type': 'message',
            'role': role,
            'content': [{'type': 'input_text', 'text': text}],
        },
    }


def _codex_noise():
    return {
        'timestamp': '2026-01-01T00:02:00.000Z',
        'type': 'event_msg',
        'payload': {'type': 'task_started'},
    }


# ---------------------------------------------------------------------------
# ClaudeJsonlSource — discover
# ---------------------------------------------------------------------------

def test_claude_discover_includes_main_and_subagent_files(tmp_path, monkeypatch):
    projects = tmp_path / '.claude' / 'projects'
    proj_a = projects / '-my-project'
    sess_uuid = 'aaaa-1111-2222-3333-4444'
    main_file = proj_a / f'{sess_uuid}.jsonl'
    _write_jsonl(main_file, [_user_record()])

    subagent_dir = proj_a / sess_uuid / 'subagents'
    sub_file = subagent_dir / 'agent-deadbeef.jsonl'
    _write_jsonl(sub_file, [_user_record()])

    import bridge.search.sources.claude as claude_mod
    monkeypatch.setattr(claude_mod, '_CLAUDE_ROOT', projects)

    src = ClaudeJsonlSource()
    assert src.is_enabled()
    paths = list(src.discover())
    stems = {p.name for p in paths}
    assert f'{sess_uuid}.jsonl' in stems
    assert 'agent-deadbeef.jsonl' in stems


# ---------------------------------------------------------------------------
# ClaudeJsonlSource — iter_messages noise skipping
# ---------------------------------------------------------------------------

def test_claude_iter_skips_noise_records(tmp_path, monkeypatch):
    projects = tmp_path / '.claude' / 'projects' / '-proj'
    f = projects / 'sess-abc.jsonl'
    _write_jsonl(f, [
        _noise_record('permission-mode'),
        _noise_record('system'),
        _user_record('real message', uuid='r1'),
    ])

    import bridge.search.sources.claude as claude_mod
    monkeypatch.setattr(claude_mod, '_CLAUDE_ROOT', projects.parent)

    src = ClaudeJsonlSource()
    msgs = [m for m, _ in src.iter_messages(f)]
    assert len(msgs) == 1
    assert msgs[0].text == 'real message'
    assert msgs[0].role == 'user'


# ---------------------------------------------------------------------------
# ClaudeJsonlSource — str and list content
# ---------------------------------------------------------------------------

def test_claude_iter_handles_str_and_list_content(tmp_path):
    f = tmp_path / 'sess.jsonl'
    _write_jsonl(f, [
        _user_record('string content', uuid='u-str'),
        _assistant_record(blocks=[
            {'type': 'text', 'text': 'block one'},
            {'type': 'tool_use', 'id': 'tid', 'name': 'bash', 'input': {}},
            {'type': 'text', 'text': 'block two'},
        ], uuid='a-list'),
    ])

    src = ClaudeJsonlSource()
    msgs = [m for m, _ in src.iter_messages(f)]
    assert len(msgs) == 2
    assert msgs[0].text == 'string content'
    # tool_use blocks are ignored; only text blocks concatenated
    assert msgs[1].text == 'block one\nblock two'


# ---------------------------------------------------------------------------
# ClaudeJsonlSource — incremental resume from offset
# ---------------------------------------------------------------------------

def test_claude_iter_incremental_resume_from_offset(tmp_path):
    f = tmp_path / 'sess.jsonl'
    r1 = _user_record('first', uuid='u1')
    r2 = _user_record('second', uuid='u2')
    _write_jsonl(f, [r1, r2])

    src = ClaudeJsonlSource()
    first_pass = list(src.iter_messages(f, start_offset=0))
    assert len(first_pass) == 2
    _, offset_after_first = first_pass[0]

    # Resume from offset — should only see second message
    second_pass = list(src.iter_messages(f, start_offset=offset_after_first))
    assert len(second_pass) == 1
    assert second_pass[0][0].text == 'second'


# ---------------------------------------------------------------------------
# ClaudeJsonlSource — partial last line not emitted
# ---------------------------------------------------------------------------

def test_claude_iter_does_not_emit_partial_last_line(tmp_path):
    f = tmp_path / 'sess.jsonl'
    complete = json.dumps(_user_record('complete', uuid='c1')) + '\n'
    partial = json.dumps(_user_record('partial', uuid='p1'))  # no trailing newline
    f.write_bytes(complete.encode() + partial.encode())

    src = ClaudeJsonlSource()
    msgs = list(src.iter_messages(f))
    assert len(msgs) == 1
    assert msgs[0][0].text == 'complete'


# ---------------------------------------------------------------------------
# ClaudeJsonlSource — head_signature changes on rewrite
# ---------------------------------------------------------------------------

def test_claude_head_signature_changes_when_file_rewritten(tmp_path):
    f = tmp_path / 'sess.jsonl'
    _write_jsonl(f, [_user_record('original')])
    src = ClaudeJsonlSource()
    sig1 = src.head_signature(f)

    _write_jsonl(f, [_user_record('rewritten')])
    sig2 = src.head_signature(f)

    assert sig1 != sig2
    assert len(sig1) == 64  # sha256 hex


# ---------------------------------------------------------------------------
# ClaudeJsonlSource — session_id_for subagent
# ---------------------------------------------------------------------------

def test_claude_session_id_for_subagent_has_correct_format(tmp_path):
    # Simulate: projects/-proj/SESSION-UUID/subagents/agent-AGENTID.jsonl
    sess_uuid = 'ee6b69d5-c520-4f93-a055-1cec11da348d'
    agent_id = 'a09dedb6cafebabe'
    path = tmp_path / '-proj' / sess_uuid / 'subagents' / f'agent-{agent_id}.jsonl'

    src = ClaudeJsonlSource()
    sid = src.session_id_for(path)
    assert sid == f'claude:{sess_uuid}:subagent:{agent_id}'


def test_claude_session_id_for_main_file(tmp_path):
    sess_uuid = 'ee6b69d5-c520-4f93-a055-1cec11da348d'
    path = tmp_path / '-proj' / f'{sess_uuid}.jsonl'

    src = ClaudeJsonlSource()
    sid = src.session_id_for(path)
    assert sid == f'claude:{sess_uuid}'


# ---------------------------------------------------------------------------
# ClaudeJsonlSource — sidechain user records are skipped
# ---------------------------------------------------------------------------

def test_claude_iter_skips_sidechain_user_records(tmp_path):
    f = tmp_path / 'sess.jsonl'
    _write_jsonl(f, [
        _user_record('visible', uuid='u1', sidechain=False),
        _user_record('hidden', uuid='u2', sidechain=True),
    ])

    src = ClaudeJsonlSource()
    msgs = [m for m, _ in src.iter_messages(f)]
    assert len(msgs) == 1
    assert msgs[0].text == 'visible'


# ---------------------------------------------------------------------------
# CodexSessionSource — iter_messages only yields response_item messages
# ---------------------------------------------------------------------------

def test_codex_iter_only_yields_response_item_messages(tmp_path):
    f = tmp_path / 'rollout-test.jsonl'
    _write_jsonl(f, [
        _codex_session_meta(),
        _codex_noise(),
        _codex_msg('user', 'codex user msg', uuid='cu1'),
        _codex_msg('assistant', 'codex assistant reply', uuid='ca1'),
    ])

    src = CodexSessionSource()
    msgs = [m for m, _ in src.iter_messages(f)]
    assert len(msgs) == 2
    roles = {m.role for m in msgs}
    assert roles == {'user', 'assistant'}
    assert msgs[0].source == 'codex'


# ---------------------------------------------------------------------------
# CodexSessionSource — survives unknown schema shape
# ---------------------------------------------------------------------------

def test_codex_iter_survives_unknown_schema_shape(tmp_path):
    f = tmp_path / 'rollout-weird.jsonl'
    good_msg = _codex_msg('user', 'good message', uuid='g1')
    weird = {'type': 'response_item', 'payload': None}  # payload=None is unexpected
    _write_jsonl(f, [good_msg, weird])

    src = CodexSessionSource()
    # Should not raise; weird record is skipped
    msgs = [m for m, _ in src.iter_messages(f)]
    assert len(msgs) == 1
    assert msgs[0].text == 'good message'


# ---------------------------------------------------------------------------
# OllamaSource — is_disabled
# ---------------------------------------------------------------------------

def test_ollama_is_disabled():
    src = OllamaSource()
    assert src.is_enabled() is False
    assert src.name == 'ollama'
    assert list(src.discover()) == []


def test_ollama_iter_raises_not_implemented():
    src = OllamaSource()
    with pytest.raises(NotImplementedError):
        list(src.iter_messages(Path('/dev/null')))


# ---------------------------------------------------------------------------
# registered_sources — respects config disable flag
# ---------------------------------------------------------------------------

def test_registered_sources_returns_auto_filtered_by_default(tmp_path, monkeypatch):
    """When config=None, sources are filtered by is_enabled() (auto-detect)."""
    import bridge.search.sources.claude as claude_mod
    import bridge.search.sources.codex as codex_mod

    # Point claude root to a directory that exists → claude enabled
    (tmp_path / 'projects').mkdir()
    monkeypatch.setattr(claude_mod, '_CLAUDE_ROOT', tmp_path / 'projects')
    # Point codex root to nonexistent → codex disabled
    monkeypatch.setattr(codex_mod, '_CODEX_ROOT', tmp_path / 'no_codex')

    srcs = registered_sources()
    names = {s.name for s in srcs}
    assert 'claude' in names
    assert 'codex' not in names  # not enabled (dir doesn't exist)
    assert 'ollama' not in names  # always disabled


def test_registered_sources_respects_real_config_disable(tmp_path, monkeypatch):
    """claude_enabled='no' in real BridgeConfig must exclude claude from results."""
    import bridge.search.sources.claude as claude_mod

    # Even if claude root exists, 'no' must override
    (tmp_path / 'projects').mkdir()
    monkeypatch.setattr(claude_mod, '_CLAUDE_ROOT', tmp_path / 'projects')

    cfg = BridgeConfig(
        search=SearchConfig(),
        sources=SourcesConfig(claude_enabled='no', codex_enabled='no', ollama_enabled='no'),
        server=ServerConfig(),
    )
    srcs = registered_sources(config=cfg)
    names = {s.name for s in srcs}
    assert 'claude' not in names, "claude_enabled='no' must exclude claude"
    assert 'codex' not in names
    assert 'ollama' not in names


def test_registered_sources_yes_forces_inclusion(tmp_path, monkeypatch):
    """claude_enabled='yes' includes claude regardless of is_enabled()."""
    import bridge.search.sources.claude as claude_mod

    # claude root does NOT exist — is_enabled() would return False
    monkeypatch.setattr(claude_mod, '_CLAUDE_ROOT', tmp_path / 'nonexistent')

    cfg = BridgeConfig(
        search=SearchConfig(),
        sources=SourcesConfig(claude_enabled='yes', codex_enabled='no', ollama_enabled='no'),
        server=ServerConfig(),
    )
    srcs = registered_sources(config=cfg)
    names = {s.name for s in srcs}
    assert 'claude' in names, "claude_enabled='yes' must include claude even if root absent"


def test_registered_sources_auto_falls_back_to_is_enabled(tmp_path, monkeypatch):
    """claude_enabled='auto' uses is_enabled() to decide."""
    import bridge.search.sources.claude as claude_mod

    # Root doesn't exist → is_enabled() is False
    monkeypatch.setattr(claude_mod, '_CLAUDE_ROOT', tmp_path / 'no_projects')

    cfg = BridgeConfig(
        search=SearchConfig(),
        sources=SourcesConfig(claude_enabled='auto', codex_enabled='no', ollama_enabled='no'),
        server=ServerConfig(),
    )
    srcs = registered_sources(config=cfg)
    names = {s.name for s in srcs}
    assert 'claude' not in names, "claude_enabled='auto' with nonexistent root must exclude claude"

    # Now make the root exist
    (tmp_path / 'projects').mkdir()
    monkeypatch.setattr(claude_mod, '_CLAUDE_ROOT', tmp_path / 'projects')
    srcs2 = registered_sources(config=cfg)
    names2 = {s.name for s in srcs2}
    assert 'claude' in names2, "claude_enabled='auto' with existing root must include claude"


# ---------------------------------------------------------------------------
# SearchableMessage is frozen
# ---------------------------------------------------------------------------

def test_searchable_message_is_frozen():
    msg = SearchableMessage(
        source='claude',
        session_id='claude:abc',
        msg_uuid='u1',
        parent_uuid=None,
        role='user',
        timestamp='2026-01-01T00:00:00Z',
        text='hello',
        is_subagent=False,
        cwd='/tmp',
    )
    with pytest.raises((AttributeError, TypeError)):
        msg.text = 'changed'  # type: ignore[misc]


# ---------------------------------------------------------------------------
# Framework wrapper stripping — integration via iter_messages
# ---------------------------------------------------------------------------

def test_claude_iter_strips_system_reminder_wrapper(tmp_path):
    """Messages with <system-reminder>…</system-reminder> are cleaned; real text kept."""
    f = tmp_path / 'sess.jsonl'
    content = '<system-reminder>Injected context</system-reminder>\nActual user message'
    _write_jsonl(f, [_user_record(text=content, uuid='u-sr')])

    src = ClaudeJsonlSource()
    msgs = [m for m, _ in src.iter_messages(f)]
    assert len(msgs) == 1
    assert msgs[0].text == 'Actual user message'
    assert '<system-reminder>' not in msgs[0].text


def test_claude_iter_strips_local_command_stdout_wrapper(tmp_path):
    """Messages with <local-command-stdout> blocks are cleaned."""
    f = tmp_path / 'sess.jsonl'
    content = '<local-command-stdout>cmd output here</local-command-stdout>\nUser follow-up'
    _write_jsonl(f, [_user_record(text=content, uuid='u-lcs')])

    src = ClaudeJsonlSource()
    msgs = [m for m, _ in src.iter_messages(f)]
    assert len(msgs) == 1
    assert 'User follow-up' in msgs[0].text
    assert 'local-command-stdout' not in msgs[0].text


def test_claude_iter_drops_msg_reduced_to_empty_by_stripping(tmp_path):
    """A message that becomes empty after wrapper stripping is not emitted."""
    f = tmp_path / 'sess.jsonl'
    content = '<system-reminder>all noise nothing else</system-reminder>'
    _write_jsonl(f, [_user_record(text=content, uuid='u-empty')])

    src = ClaudeJsonlSource()
    msgs = [m for m, _ in src.iter_messages(f)]
    assert len(msgs) == 0


def test_codex_iter_strips_system_reminder_wrapper(tmp_path):
    """Codex messages with <system-reminder> wrapper are cleaned."""
    f = tmp_path / 'rollout-strip-test.jsonl'
    noisy = '<system-reminder>reminder noise</system-reminder>\nCodex user message'
    _write_jsonl(f, [_codex_msg('user', noisy, uuid='cu-sr')])

    src = CodexSessionSource()
    msgs = [m for m, _ in src.iter_messages(f)]
    assert len(msgs) == 1
    assert msgs[0].text == 'Codex user message'
    assert '<system-reminder>' not in msgs[0].text


def test_codex_iter_strips_turn_aborted_wrapper(tmp_path):
    """Codex-injected turn abort notices are not indexed as real user text."""
    f = tmp_path / 'rollout-abort-test.jsonl'
    noisy = (
        '打包taildrop給我\n'
        '<turn_aborted>\n'
        'The user interrupted the previous turn on purpose.\n'
        '</turn_aborted>\n'
        '我什麼都沒做 自動發出的'
    )
    _write_jsonl(f, [_codex_msg('user', noisy, uuid='cu-abort')])

    src = CodexSessionSource()
    msgs = [m for m, _ in src.iter_messages(f)]
    assert len(msgs) == 1
    assert '打包taildrop給我' in msgs[0].text
    assert '我什麼都沒做' in msgs[0].text
    assert 'turn_aborted' not in msgs[0].text
