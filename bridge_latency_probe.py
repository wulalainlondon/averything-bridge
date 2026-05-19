#!/usr/bin/env python3
"""Measure bridge WebSocket latency without depending on the mobile app."""
from __future__ import annotations

import argparse
import asyncio
import json
import time
from collections import Counter
from typing import Any, Callable


def _now() -> float:
    return time.perf_counter()


def _ms_since(start: float) -> float:
    return round((_now() - start) * 1000.0, 1)


async def _recv_until(
    ws: Any,
    predicate: Callable[[dict], bool],
    *,
    timeout: float,
    counters: Counter,
) -> tuple[dict, dict]:
    deadline = _now() + timeout
    frame_count = 0
    byte_count = 0
    last_types: list[str] = []
    while _now() < deadline:
        raw = await asyncio.wait_for(ws.recv(), timeout=max(0.05, deadline - _now()))
        frame_count += 1
        byte_count += len(raw.encode("utf-8") if isinstance(raw, str) else raw)
        msg = json.loads(raw)
        mtype = str(msg.get("type") or "")
        counters[mtype] += 1
        last_types.append(mtype)
        if len(last_types) > 20:
            del last_types[:-20]
        if predicate(msg):
            return msg, {
                "frames": frame_count,
                "bytes": byte_count,
                "last_types": last_types,
            }
    raise TimeoutError(f"timed out waiting for frame; last_types={last_types}")


async def measure(args: argparse.Namespace) -> dict:
    try:
        import websockets
    except Exception as exc:  # pragma: no cover - environment guard
        raise RuntimeError("websockets is required for bridge_latency_probe.py") from exc

    uri = f"ws://{args.host}:{args.port}"
    counters: Counter = Counter()
    result: dict[str, Any] = {
        "uri": uri,
        "session_id": args.session_id,
        "request_id": args.request_id,
        "timeout_s": args.timeout,
        "latency_ms": {},
        "frame_counts": {},
        "payload_bytes": {},
    }

    connect_start = _now()
    async with websockets.connect(uri, open_timeout=args.timeout, ping_interval=None) as ws:
        result["latency_ms"]["connect"] = _ms_since(connect_start)

        wait_start = _now()
        hello, info = await _recv_until(
            ws,
            lambda m: m.get("type") == "hello_ack",
            timeout=args.timeout,
            counters=counters,
        )
        result["latency_ms"]["initial_hello_ack"] = _ms_since(wait_start)
        result["frame_counts"]["before_initial_hello_ack"] = info["frames"]
        result["payload_bytes"]["before_initial_hello_ack"] = info["bytes"]
        result["initial_client_id"] = hello.get("client_id")

        wait_start = _now()
        sessions, info = await _recv_until(
            ws,
            lambda m: m.get("type") == "sessions_list",
            timeout=args.timeout,
            counters=counters,
        )
        result["latency_ms"]["initial_sessions_list"] = _ms_since(wait_start)
        result["frame_counts"]["before_initial_sessions_list"] = info["frames"]
        result["payload_bytes"]["before_initial_sessions_list"] = info["bytes"]
        result["initial_session_count"] = len(sessions.get("sessions") or [])

        await ws.send(json.dumps({
            "type": "new_session",
            "session_id": args.session_id,
            "name": args.session_name,
            "backend": args.backend,
        }))
        wait_start = _now()
        created, info = await _recv_until(
            ws,
            lambda m: m.get("type") == "session_created" and m.get("session_id") == args.session_id,
            timeout=args.timeout,
            counters=counters,
        )
        result["latency_ms"]["new_session_to_created"] = _ms_since(wait_start)
        result["frame_counts"]["before_session_created"] = info["frames"]
        result["payload_bytes"]["before_session_created"] = info["bytes"]
        result["created_backend"] = created.get("backend")

        await ws.send(json.dumps({
            "type": "message",
            "session_id": args.session_id,
            "request_id": args.request_id,
            "content": args.content,
        }))
        wait_start = _now()
        ack, info = await _recv_until(
            ws,
            lambda m: (
                m.get("type") == "message_ack"
                and m.get("session_id") == args.session_id
                and m.get("request_id") == args.request_id
            ),
            timeout=args.timeout,
            counters=counters,
        )
        result["latency_ms"]["message_to_ack"] = _ms_since(wait_start)
        result["frame_counts"]["before_message_ack"] = info["frames"]
        result["payload_bytes"]["before_message_ack"] = info["bytes"]
        result["message_ack_status"] = ack.get("status")

        wait_start = _now()
        _queued, info = await _recv_until(
            ws,
            lambda m: (
                m.get("type") == "session_command_queued"
                and m.get("session_id") == args.session_id
                and m.get("request_id") == args.request_id
            ),
            timeout=args.timeout,
            counters=counters,
        )
        result["latency_ms"]["message_ack_to_command_queued"] = _ms_since(wait_start)
        result["frame_counts"]["before_command_queued"] = info["frames"]
        result["payload_bytes"]["before_command_queued"] = info["bytes"]

        if args.cleanup:
            await ws.send(json.dumps({"type": "stop", "session_id": args.session_id}))
            await ws.send(json.dumps({"type": "close_session", "session_id": args.session_id}))

    result["received_type_counts"] = dict(counters.most_common())
    return result


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Measure bridge WebSocket connection and message latency")
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=8766)
    parser.add_argument("--backend", default="ollama")
    parser.add_argument("--timeout", type=float, default=30.0)
    parser.add_argument("--session-id", default=f"s_probe_{int(time.time())}")
    parser.add_argument("--session-name", default="Latency Probe")
    parser.add_argument("--request-id", default="r_probe_1")
    parser.add_argument("--content", default="latency probe")
    parser.add_argument("--no-cleanup", action="store_false", dest="cleanup")
    parser.set_defaults(cleanup=True)
    return parser


def main() -> int:
    args = build_parser().parse_args()
    result = asyncio.run(measure(args))
    print(json.dumps(result, ensure_ascii=False, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
