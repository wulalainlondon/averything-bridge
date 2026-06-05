from __future__ import annotations

import asyncio

import pytest

from handlers import webrtc_signaling


class FakePc:
    def __init__(self) -> None:
        self.closed = False

    async def close(self) -> None:
        self.closed = True


@pytest.fixture(autouse=True)
def clean_webrtc_registry():
    webrtc_signaling._PC_BY_SIGNALING.clear()
    yield
    webrtc_signaling._PC_BY_SIGNALING.clear()


def test_detach_promoted_pc_prevents_signaling_cleanup_from_closing_datachannel_pc():
    ws = object()
    pc = FakePc()
    webrtc_signaling._PC_BY_SIGNALING[ws] = pc

    webrtc_signaling._detach_promoted_pc(ws, pc)
    webrtc_signaling.cleanup_for_ws(ws)

    assert ws not in webrtc_signaling._PC_BY_SIGNALING
    assert pc.closed is False


@pytest.mark.asyncio
async def test_cleanup_for_ws_still_closes_unpromoted_pc():
    ws = object()
    pc = FakePc()
    webrtc_signaling._PC_BY_SIGNALING[ws] = pc

    webrtc_signaling.cleanup_for_ws(ws)
    await asyncio.sleep(0)

    assert ws not in webrtc_signaling._PC_BY_SIGNALING
    assert pc.closed is True
