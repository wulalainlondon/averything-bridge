from __future__ import annotations

from transport import transport_remote_address, transport_user_agent


class _HeadersTransport:
    remote_address = ("127.0.0.1", 1234)

    class request:
        headers = {"User-Agent": "bridge-test/1"}


class _BareTransport:
    remote_address = ("webrtc", 0)
    request = None


def test_transport_user_agent_reads_websocket_headers():
    assert transport_user_agent(_HeadersTransport()) == "bridge-test/1"


def test_transport_user_agent_is_empty_without_request():
    assert transport_user_agent(_BareTransport()) == ""


def test_transport_remote_address_is_best_effort():
    assert transport_remote_address(_BareTransport()) == ("webrtc", 0)
    assert transport_remote_address(object()) is None
