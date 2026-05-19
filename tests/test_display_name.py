"""
Unit tests for bridge.search.ingest.display_name.

Run: pytest bridge/tests/test_display_name.py
"""
from __future__ import annotations

import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).parent.parent.parent))

from bridge.search.ingest.display_name import is_framework_noise, pick_display_name


# ---------------------------------------------------------------------------
# is_framework_noise
# ---------------------------------------------------------------------------

def test_is_framework_noise_recognizes_caveat():
    assert is_framework_noise("Caveat: The following tools are restricted") is True


def test_is_framework_noise_recognizes_local_command_caveat():
    assert is_framework_noise("<local-command-caveat>This command may be unsafe</local-command-caveat>") is True


def test_is_framework_noise_recognizes_continuation():
    assert is_framework_noise("This session is being continued from a previous conversation that ran out of context.") is True


def test_is_framework_noise_recognizes_agents_md():
    assert is_framework_noise("# AGENTS.md instructions for /Users/alice/project") is True


def test_is_framework_noise_recognizes_agents_md_variant():
    # Whitespace between # and AGENTS.md
    assert is_framework_noise("#  AGENTS.md\nsome content") is True


def test_is_framework_noise_recognizes_environment_context():
    assert is_framework_noise("<environment_context>{\n  \"platform\": \"darwin\"\n}</environment_context>") is True


def test_is_framework_noise_recognizes_system_reminder():
    assert is_framework_noise("<system-reminder>\nRemember to use tools carefully.\n</system-reminder>") is True


def test_is_framework_noise_recognizes_command_name():
    assert is_framework_noise("<command-name>bash</command-name>") is True


def test_is_framework_noise_recognizes_command_output():
    assert is_framework_noise("<command-output>ls -la</command-output>") is True


def test_is_framework_noise_recognizes_local_command_stdout():
    assert is_framework_noise("<local-command-stdout>No conversations found to resume</local-command-stdout>") is True


def test_is_framework_noise_recognizes_local_command_stderr():
    assert is_framework_noise("<local-command-stderr>some error output</local-command-stderr>") is True


def test_is_framework_noise_recognizes_slash_command_exit():
    assert is_framework_noise("/exit") is True


def test_is_framework_noise_recognizes_slash_command_clear():
    assert is_framework_noise("/clear") is True


def test_is_framework_noise_recognizes_slash_command_compact():
    assert is_framework_noise("/compact") is True


def test_is_framework_noise_empty_string():
    assert is_framework_noise("") is True


def test_is_framework_noise_whitespace_only():
    assert is_framework_noise("   \n  ") is True


def test_is_framework_noise_passes_real_user_text():
    assert is_framework_noise("Can you help me refactor this Python module?") is False


def test_is_framework_noise_passes_real_user_text_with_newlines():
    assert is_framework_noise("Fix the bug in auth.py\n\nThe error is on line 42") is False


def test_is_framework_noise_passes_text_starting_with_hash_non_agents():
    # A markdown heading that is NOT AGENTS.md is real user content
    assert is_framework_noise("# Summary of my project") is False


def test_is_framework_noise_case_insensitive_caveat():
    assert is_framework_noise("caveat: something") is True


def test_is_framework_noise_case_insensitive_continuation():
    assert is_framework_noise("this session is being continued from a previous conversation") is True


# ---------------------------------------------------------------------------
# pick_display_name
# ---------------------------------------------------------------------------

def _msgs(*items):
    """Build a list of {'role': ..., 'text': ...} dicts."""
    return [{'role': role, 'text': text} for role, text in items]


def test_pick_display_name_skips_noise_returns_first_real_user_msg():
    msgs = _msgs(
        ('user', '<local-command-caveat>Caveat content</local-command-caveat>'),
        ('user', 'This session is being continued from a previous conversation.'),
        ('user', 'Please help me write unit tests for the auth module.'),
        ('assistant', 'Sure, I can help with that.'),
    )
    result = pick_display_name(iter(msgs))
    assert result == 'Please help me write unit tests for the auth module.'


def test_pick_display_name_returns_none_if_all_noise():
    msgs = _msgs(
        ('user', 'Caveat: The following tools are restricted'),
        ('user', '<environment_context>{"platform": "darwin"}</environment_context>'),
        ('user', '/clear'),
        ('assistant', 'Cleared.'),
    )
    result = pick_display_name(iter(msgs))
    assert result is None


def test_pick_display_name_truncates_to_max_len():
    long_text = 'A' * 200
    msgs = _msgs(('user', long_text))
    result = pick_display_name(iter(msgs), max_len=80)
    assert result == 'A' * 80


def test_pick_display_name_skips_assistant_messages():
    msgs = _msgs(
        ('assistant', 'This is a real assistant response'),
        ('user', 'Caveat: blah'),
        ('user', 'What is the capital of France?'),
    )
    result = pick_display_name(iter(msgs))
    assert result == 'What is the capital of France?'


def test_pick_display_name_collapses_whitespace():
    msgs = _msgs(('user', 'Fix the  bug\n\nin  auth.py'))
    result = pick_display_name(iter(msgs))
    assert result == 'Fix the bug in auth.py'


def test_pick_display_name_empty_iterator():
    result = pick_display_name(iter([]))
    assert result is None


def test_pick_display_name_custom_max_len():
    msgs = _msgs(('user', 'Hello world'))
    result = pick_display_name(iter(msgs), max_len=5)
    assert result == 'Hello'


def test_pick_display_name_uses_first_real_msg_not_second():
    msgs = _msgs(
        ('user', 'Caveat: restricted'),
        ('user', 'First real message'),
        ('user', 'Second real message'),
    )
    result = pick_display_name(iter(msgs))
    assert result == 'First real message'
