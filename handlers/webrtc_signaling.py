"""WebRTC P2P signaling for claude-bridge.

Lets clients upgrade from the Cloudflare-tunneled WebSocket to a direct
peer-to-peer DataChannel via WebRTC NAT traversal. The existing WebSocket
connection acts as the signaling channel (SDP / ICE exchange).

Protocol:

    Client -> Server:
        webrtc_offer { type, sdp: str }
        webrtc_ice   { type, candidate: str, sdpMid: str, sdpMLineIndex: int }
    Server -> Client:
        webrtc_answer { type, sdp: str }
        webrtc_ready  { type } (DataChannel is open on the server side)

aiortc does NOT support trickle ICE outbound — all server-side candidates are
baked into the answer SDP, so the server never sends webrtc_ice itself. The
client (browser) DOES trickle, and those candidates are accepted via the
`webrtc_ice` inbound message.

Takeover flow:

    1. Client opens WS via Cloudflare Tunnel (existing path).
    2. Client creates RTCPeerConnection + DataChannel, sends webrtc_offer.
    3. Server creates answering PC, returns webrtc_answer.
    4. Client trickles its ICE candidates via webrtc_ice; server applies.
    5. DataChannel opens on both ends. Server wraps it in WebRTCChannel
       (a ServerConnection-shaped adapter) and re-enters bridge_v2.handler()
       on the adapter, plus emits webrtc_ready on the legacy WS.
    6. Client sends `hello` over the DataChannel. The bridge's hello handler
       enforces a single live socket per device_id and closes the old WS.
       From there all traffic flows P2P.
"""
from __future__ import annotations

import asyncio
import json
import logging
from typing import Any, Awaitable, Callable, Optional

from transport import BridgeTransport

try:
    from aiortc import (
        RTCConfiguration,
        RTCIceServer,
        RTCPeerConnection,
        RTCSessionDescription,
    )
    from aiortc.sdp import candidate_from_sdp
    _AIORTC_AVAILABLE = True
    _AIORTC_IMPORT_ERROR: Optional[str] = None
except ImportError as _e:
    _AIORTC_AVAILABLE = False
    _AIORTC_IMPORT_ERROR = str(_e)

log = logging.getLogger("bridge.webrtc")

DEFAULT_ICE_SERVERS = (
    [
        RTCIceServer(urls="stun:stun.l.google.com:19302"),
        RTCIceServer(urls="stun:stun1.l.google.com:19302"),
    ]
    if _AIORTC_AVAILABLE
    else []
)

WEBRTC_MESSAGE_TYPES = frozenset({"webrtc_offer", "webrtc_answer", "webrtc_ice"})

# Per-signaling-websocket registry of active answering peer connections.
# Keyed by the signaling websocket so an inbound webrtc_ice can find the PC
# without the client having to carry an extra correlation id.
_PC_BY_SIGNALING: "dict[Any, RTCPeerConnection]" = {}


class WebRTCChannel(BridgeTransport):
    """WebSocket-like adapter over an aiortc DataChannel.

    Duck-types ``websockets.asyncio.server.ServerConnection`` so the existing
    ``bridge_v2.handler()`` loop can run unchanged on top of a P2P
    DataChannel:
        - ``await adapter.send(str | bytes)`` -> DataChannel.send
        - ``async for raw in adapter`` -> yields each inbound frame
        - ``adapter.remote_address`` / ``adapter.request`` for handshake peek
        - ``await adapter.close()`` tears down the PC
    """

    def __init__(self, pc: "RTCPeerConnection", dc: Any, remote_address: tuple) -> None:
        self._pc = pc
        self._dc = dc
        self.remote_address = remote_address
        self.request = None  # ServerConnection.request — only used for UA peek
        self._inbox: asyncio.Queue = asyncio.Queue()
        self._closed = False

        @dc.on("message")
        def _on_message(msg):
            if self._closed:
                return
            if isinstance(msg, (bytes, bytearray)):
                try:
                    text = bytes(msg).decode("utf-8")
                except Exception:
                    log.warning("[webrtc] dropped non-utf8 binary frame")
                    return
            else:
                text = msg
            try:
                self._inbox.put_nowait(text)
            except Exception:
                log.exception("[webrtc] inbox put failed")

        @dc.on("close")
        def _on_close():
            self._closed = True
            try:
                self._inbox.put_nowait(None)
            except Exception:
                pass

    async def send(self, data: Any) -> None:
        if self._closed:
            raise ConnectionError("WebRTC DataChannel closed")
        # aiortc DataChannel.send is synchronous and accepts str or bytes.
        self._dc.send(data)

    async def close(self, *_args, **_kwargs) -> None:
        if self._closed:
            return
        self._closed = True
        try:
            self._dc.close()
        except Exception:
            pass
        try:
            await self._pc.close()
        except Exception:
            pass
        try:
            self._inbox.put_nowait(None)
        except Exception:
            pass

    def __aiter__(self):
        return self

    async def __anext__(self):
        if self._closed and self._inbox.empty():
            raise StopAsyncIteration
        item = await self._inbox.get()
        if item is None:
            raise StopAsyncIteration
        return item


async def handle_webrtc_message(
    mtype: str,
    msg: dict,
    ws,
    on_channel_ready: Callable[["WebRTCChannel"], Awaitable[None]],
) -> bool:
    """Dispatch one WebRTC signaling frame from a signaling WebSocket.

    Returns True if the message type was recognized (and consumed).

    ``on_channel_ready`` is awaited once the server-side DataChannel is open
    and wrapped in a WebRTCChannel. Callers typically schedule the existing
    bridge ``handler()`` to run on the adapter so all message dispatch logic
    is reused.
    """
    if mtype not in WEBRTC_MESSAGE_TYPES:
        return False

    if not _AIORTC_AVAILABLE:
        log.warning("[webrtc] aiortc unavailable (%s); rejecting %s", _AIORTC_IMPORT_ERROR, mtype)
        await _safe_send(ws, {
            "type": "error",
            "code": "webrtc_unsupported",
            "message": "WebRTC unavailable on bridge — aiortc not installed",
        })
        return True

    if mtype == "webrtc_offer":
        await _handle_offer(ws, msg, on_channel_ready)
    elif mtype == "webrtc_ice":
        await _handle_ice(ws, msg)
    else:
        # webrtc_answer — bridge is always the answerer in this design,
        # clients should not send answers back. Log and ignore.
        log.debug("[webrtc] ignored inbound webrtc_answer (server is answerer)")
    return True


async def _handle_offer(ws, msg, on_channel_ready):
    sdp = msg.get("sdp")
    if not isinstance(sdp, str) or not sdp.strip():
        await _safe_send(ws, {
            "type": "error",
            "code": "webrtc_offer_invalid",
            "message": "missing sdp",
        })
        return

    # Tear down any previous PC on this signaling channel (re-offer)
    old = _PC_BY_SIGNALING.pop(ws, None)
    if old is not None:
        try:
            await old.close()
        except Exception:
            pass

    pc = RTCPeerConnection(configuration=RTCConfiguration(iceServers=list(DEFAULT_ICE_SERVERS)))
    _PC_BY_SIGNALING[ws] = pc

    @pc.on("connectionstatechange")
    async def _on_state_change():
        log.info("[webrtc] pc state: %s", pc.connectionState)
        if pc.connectionState in ("failed", "closed", "disconnected"):
            _PC_BY_SIGNALING.pop(ws, None)

    @pc.on("datachannel")
    def _on_datachannel(channel):
        log.info(
            "[webrtc] datachannel offered by peer: label=%s id=%s state=%s",
            channel.label,
            getattr(channel, "id", None),
            getattr(channel, "readyState", None),
        )
        _detach_promoted_pc(ws, pc)

        # Best-effort remote address; the selected pair isn't always available
        # synchronously, so just tag the adapter with a sentinel.
        remote = ("webrtc", 0)
        promoted = False

        def _promote_channel():
            nonlocal promoted
            if promoted:
                return
            promoted = True
            log.info("[webrtc] datachannel open — promoting to bridge handler")
            adapter = WebRTCChannel(pc=pc, dc=channel, remote_address=remote)
            asyncio.create_task(on_channel_ready(adapter))
            asyncio.create_task(_safe_send(ws, {"type": "webrtc_ready"}))

        @channel.on("open")
        def _on_open():
            _promote_channel()

        if getattr(channel, "readyState", None) == "open":
            _promote_channel()

    try:
        await pc.setRemoteDescription(RTCSessionDescription(sdp=sdp, type="offer"))
        answer = await pc.createAnswer()
        await pc.setLocalDescription(answer)
    except Exception:
        log.exception("[webrtc] offer/answer negotiation failed")
        _PC_BY_SIGNALING.pop(ws, None)
        try:
            await pc.close()
        except Exception:
            pass
        await _safe_send(ws, {
            "type": "error",
            "code": "webrtc_negotiation_failed",
            "message": "could not produce SDP answer",
        })
        return

    await _safe_send(ws, {"type": "webrtc_answer", "sdp": pc.localDescription.sdp})


async def _handle_ice(ws, msg):
    pc = _PC_BY_SIGNALING.get(ws)
    if pc is None:
        log.debug("[webrtc] ignoring inbound ICE — no PC for this WS")
        return
    cand_str = msg.get("candidate")
    if not isinstance(cand_str, str) or not cand_str.strip():
        # null candidate signals end-of-trickle; nothing to apply.
        return
    sdp_line = cand_str
    if sdp_line.startswith("candidate:"):
        sdp_line = sdp_line[len("candidate:"):]
    try:
        candidate = candidate_from_sdp(sdp_line)
        sdp_mid = msg.get("sdpMid")
        if sdp_mid is not None:
            candidate.sdpMid = sdp_mid
        sdp_mline = msg.get("sdpMLineIndex")
        if sdp_mline is not None:
            try:
                candidate.sdpMLineIndex = int(sdp_mline)
            except (TypeError, ValueError):
                pass
        await pc.addIceCandidate(candidate)
    except Exception:
        log.exception("[webrtc] addIceCandidate failed; cand=%r", cand_str[:200])


async def _safe_send(ws, payload: dict) -> None:
    try:
        await ws.send(json.dumps(payload))
    except Exception:
        log.debug("[webrtc] send dropped (signaling WS closed?)")


def _detach_promoted_pc(ws, pc) -> None:
    """Keep a promoted DataChannel alive after its signaling WebSocket closes."""
    if _PC_BY_SIGNALING.get(ws) is pc:
        _PC_BY_SIGNALING.pop(ws, None)


def cleanup_for_ws(ws) -> None:
    """Drop the answering PC tied to ``ws`` when its signaling channel dies."""
    pc = _PC_BY_SIGNALING.pop(ws, None)
    if pc is None:
        return
    try:
        loop = asyncio.get_event_loop()
        if loop.is_running():
            loop.create_task(pc.close())
        else:
            loop.run_until_complete(pc.close())
    except Exception:
        pass


__all__ = [
    "WEBRTC_MESSAGE_TYPES",
    "WebRTCChannel",
    "handle_webrtc_message",
    "cleanup_for_ws",
]
