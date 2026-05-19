"""Unit tests for framework-wrapper stripping (bridge.search._strip_wrappers).

Tests for the integration of strip_framework_wrappers into ClaudeJsonlSource
and CodexSessionSource iter_messages are added to test_search_sources.py to
avoid the pre-existing circular import between bridge.search.sources and
bridge.search.ingest (which only resolves when the full suite initialises the
package in a specific order).
"""
import pytest

from bridge.search._strip_wrappers import strip_framework_wrappers


# ---------------------------------------------------------------------------
# strip_framework_wrappers — closed-form <tag>…</tag>
# ---------------------------------------------------------------------------

def test_strip_removes_closed_system_reminder():
    text = '<system-reminder>Injected noise here</system-reminder>\nReal user text'
    result = strip_framework_wrappers(text)
    assert result == 'Real user text'
    assert '<system-reminder>' not in result


def test_strip_removes_closed_local_command_stdout():
    text = 'Before\n<local-command-stdout>ls -la output</local-command-stdout>\nAfter'
    result = strip_framework_wrappers(text)
    assert 'local-command-stdout' not in result
    assert 'Before' in result
    assert 'After' in result


def test_strip_removes_closed_command_output():
    text = '<command-output>some output</command-output>'
    result = strip_framework_wrappers(text)
    assert result == ''


def test_strip_removes_multiline_closed_wrapper():
    text = (
        '<system-reminder>\n'
        'line one of noise\n'
        'line two of noise\n'
        '</system-reminder>\n'
        'Real content here'
    )
    result = strip_framework_wrappers(text)
    assert 'Real content here' in result
    assert 'noise' not in result


def test_strip_removes_multiple_wrappers():
    text = (
        '<system-reminder>noise A</system-reminder>\n'
        'Good text\n'
        '<local-command-stdout>noise B</local-command-stdout>\n'
        'More good text'
    )
    result = strip_framework_wrappers(text)
    assert 'Good text' in result
    assert 'More good text' in result
    assert 'noise A' not in result
    assert 'noise B' not in result


def test_strip_preserves_real_text_unchanged():
    text = 'How do I use GitHub Actions?'
    assert strip_framework_wrappers(text) == 'How do I use GitHub Actions?'


def test_strip_handles_empty_string():
    assert strip_framework_wrappers('') == ''


def test_strip_handles_whitespace_only():
    assert strip_framework_wrappers('   ') == ''


def test_strip_case_insensitive():
    text = '<SYSTEM-REMINDER>noise</SYSTEM-REMINDER>\nReal'
    result = strip_framework_wrappers(text)
    assert 'Real' in result
    assert 'noise' not in result


def test_strip_removes_command_stderr():
    text = '<command-stderr>error output</command-stderr>\nActual content'
    result = strip_framework_wrappers(text)
    assert 'error output' not in result
    assert 'Actual content' in result


def test_strip_leaves_content_before_and_after_wrapper():
    text = 'question\n<system-reminder>reminder</system-reminder>\nanswer'
    result = strip_framework_wrappers(text)
    assert 'question' in result
    assert 'answer' in result
    assert 'reminder' not in result


def test_strip_handles_environment_details_tag():
    text = '<environment_details>env stuff</environment_details>\nReal message'
    result = strip_framework_wrappers(text)
    assert 'env stuff' not in result
    assert 'Real message' in result
