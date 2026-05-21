"""Tests for bridge.utils.path_jail."""
from __future__ import annotations

import os
import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).parent.parent.parent))

from bridge.utils.path_jail import JailEscape, resolve_jailed


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _jail(tmp_path, req, **kw):
    return resolve_jailed(req, str(tmp_path), **kw)


# ---------------------------------------------------------------------------
# 1. Absolute path inside jail → returns it
# ---------------------------------------------------------------------------

def test_absolute_path_inside_jail(tmp_path):
    target = tmp_path / "subdir" / "file.txt"
    target.parent.mkdir(parents=True)
    target.touch()

    result = _jail(tmp_path, str(target))
    assert result == str(target)
    assert isinstance(result, str)


# ---------------------------------------------------------------------------
# 2. Relative path that resolves inside jail → OK
# ---------------------------------------------------------------------------

def test_relative_path_inside_jail(tmp_path, monkeypatch):
    sub = tmp_path / "work"
    sub.mkdir()
    # Make tmp_path the cwd so the relative path resolves there.
    monkeypatch.chdir(tmp_path)

    result = _jail(tmp_path, "work")
    assert result == str(sub)


# ---------------------------------------------------------------------------
# 3. ~ expansion that lands inside jail → OK
# ---------------------------------------------------------------------------

def test_tilde_expansion_inside_jail(tmp_path, monkeypatch):
    # Point HOME at tmp_path so ~ expands to it.
    monkeypatch.setenv("HOME", str(tmp_path))

    result = _jail(tmp_path, "~")
    assert result == os.path.realpath(str(tmp_path))


# ---------------------------------------------------------------------------
# 4. .. traversal escape → raises JailEscape
# ---------------------------------------------------------------------------

def test_dotdot_traversal_raises(tmp_path):
    sub = tmp_path / "inner"
    sub.mkdir()
    # Attempt to escape upward from inner/
    escape_attempt = str(sub / ".." / "..")
    with pytest.raises(JailEscape) as exc_info:
        resolve_jailed(escape_attempt, str(sub))
    assert exc_info.value.root_dir == str(sub)


# ---------------------------------------------------------------------------
# 5. /etc/passwd directly → raises JailEscape
# ---------------------------------------------------------------------------

def test_absolute_outside_path_raises(tmp_path):
    with pytest.raises(JailEscape):
        _jail(tmp_path, "/etc/passwd")


# ---------------------------------------------------------------------------
# 6. Symlink pointing outside jail → raises JailEscape
# ---------------------------------------------------------------------------

def test_symlink_outside_jail_raises(tmp_path, tmp_path_factory):
    # Create a second tmp directory to act as the outside target.
    outside = tmp_path_factory.mktemp("outside")
    secret = outside / "secret.txt"
    secret.write_text("sensitive")

    # Place a symlink inside the jail pointing to the outside file.
    link = tmp_path / "evil_link"
    os.symlink(str(secret), str(link))

    with pytest.raises(JailEscape):
        _jail(tmp_path, str(link))


# ---------------------------------------------------------------------------
# 7. Path equal to root itself (allow_root_itself=True) → OK
# ---------------------------------------------------------------------------

def test_path_equal_to_root_allowed(tmp_path):
    result = resolve_jailed(str(tmp_path), str(tmp_path), allow_root_itself=True)
    assert result == os.path.realpath(str(tmp_path))


# ---------------------------------------------------------------------------
# 8. Path equal to root itself (allow_root_itself=False) → raises JailEscape
# ---------------------------------------------------------------------------

def test_path_equal_to_root_disallowed(tmp_path):
    with pytest.raises(JailEscape):
        resolve_jailed(str(tmp_path), str(tmp_path), allow_root_itself=False)


# ---------------------------------------------------------------------------
# 9. Empty root_dir → returns realpath of input (no jail)
# ---------------------------------------------------------------------------

def test_empty_root_dir_disables_jail(tmp_path):
    target = str(tmp_path / "anything.txt")
    result = resolve_jailed(target, "")
    assert result == os.path.realpath(target)


def test_empty_root_dir_with_absolute_outside():
    # No jail → /etc/passwd is fine; compare against realpath to handle macOS symlinks (/etc → /private/etc).
    result = resolve_jailed("/etc/passwd", "")
    assert result == os.path.realpath("/etc/passwd")


# ---------------------------------------------------------------------------
# 10. Empty / None req_path with root set → returns root_dir
# ---------------------------------------------------------------------------

def test_empty_string_req_path_returns_root(tmp_path):
    result = resolve_jailed("", str(tmp_path))
    assert result == os.path.realpath(str(tmp_path))


def test_none_req_path_returns_root(tmp_path):
    result = resolve_jailed(None, str(tmp_path))
    assert result == os.path.realpath(str(tmp_path))


# ---------------------------------------------------------------------------
# 11. Trailing slash on path inside jail → OK (normalised)
# ---------------------------------------------------------------------------

def test_trailing_slash_normalised(tmp_path):
    sub = tmp_path / "subdir"
    sub.mkdir()
    result = _jail(tmp_path, str(sub) + "/")
    # os.path.realpath strips the trailing slash.
    assert result == str(sub)
    assert not result.endswith("/")


# ---------------------------------------------------------------------------
# 12. Deeply nested path inside jail → OK
# ---------------------------------------------------------------------------

def test_deeply_nested_path(tmp_path):
    deep = tmp_path / "a" / "b" / "c" / "d" / "e"
    deep.mkdir(parents=True)
    result = _jail(tmp_path, str(deep))
    assert result == str(deep)
    assert result.startswith(str(tmp_path))
