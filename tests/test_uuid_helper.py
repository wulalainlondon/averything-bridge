"""
Unit tests for bridge.utils.uuid_helper.

Run: pytest bridge/tests/test_uuid_helper.py
"""
from __future__ import annotations

import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).parent.parent.parent))

from bridge.utils.uuid_helper import is_valid_uuid


def test_is_valid_uuid_accepts_canonical_form():
    assert is_valid_uuid("550e8400-e29b-41d4-a716-446655440000") is True


def test_is_valid_uuid_rejects_agent_prefix():
    # The bad value seen in production logs
    assert is_valid_uuid("agent-ac532ada8b890b02c") is False


def test_is_valid_uuid_rejects_empty():
    assert is_valid_uuid("") is False
    assert is_valid_uuid(None) is False


def test_is_valid_uuid_case_insensitive():
    assert is_valid_uuid("550E8400-E29B-41D4-A716-446655440000") is True
    assert is_valid_uuid("550e8400-E29B-41d4-A716-446655440000") is True


def test_is_valid_uuid_rejects_missing_hyphens():
    assert is_valid_uuid("550e8400e29b41d4a716446655440000") is False


def test_is_valid_uuid_rejects_wrong_length():
    assert is_valid_uuid("550e8400-e29b-41d4-a716") is False
    assert is_valid_uuid("550e8400-e29b-41d4-a716-4466554400001234") is False
