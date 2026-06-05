from __future__ import annotations

import json

from protocol import BridgeCommand, parse_client_command


def test_parse_client_command_decodes_valid_json_frame():
    command, err = parse_client_command(
        json.dumps({"type": "message", "session_id": "s1", "content": "hello"}),
        raw_len=64,
    )

    assert err is None
    assert command == BridgeCommand(
        type="message",
        payload={"type": "message", "session_id": "s1", "content": "hello"},
        raw_len=64,
    )


def test_parse_client_command_accepts_already_decoded_dict():
    command, err = parse_client_command({"type": "ping"})

    assert err is None
    assert command is not None
    assert command.type == "ping"
    assert command.payload == {"type": "ping"}


def test_parse_client_command_returns_validation_error():
    command, err = parse_client_command({"type": "message"})

    assert command is None
    assert err == "'message' missing required field 'session_id'"


def test_parse_client_command_returns_json_error():
    command, err = parse_client_command("{")

    assert command is None
    assert err == "invalid JSON"
