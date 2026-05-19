#!/usr/bin/env python3
import argparse
import asyncio
import socket
import sys


async def _websocket_healthcheck(host: str, port: int, timeout: float) -> int:
    try:
        import websockets
    except Exception:
        return _tcp_healthcheck(host, port, timeout)

    uri = f"ws://{host}:{port}"
    try:
        # User-Agent identifies this as a liveness probe so bridge_v2.py can
        # skip the heavyweight onboarding (hello_ack, 29KB sessions_list,
        # offline buffer replay, session.ws_ref reassignment).  Without this
        # marker, every 3s healthcheck steals session.ws_ref away from the
        # real app and drops events bound for the real client.
        async with websockets.connect(
            uri,
            open_timeout=timeout,
            close_timeout=timeout,
            ping_interval=None,
            additional_headers={"User-Agent": "bridge-healthcheck/1"},
        ) as ws:
            pong_waiter = await ws.ping()
            await asyncio.wait_for(pong_waiter, timeout=timeout)
            return 0
    except Exception:
        return 1


def _tcp_healthcheck(host: str, port: int, timeout: float) -> int:
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.settimeout(timeout)
    try:
      s.connect((host, port))
      return 0
    except Exception:
      return 1
    finally:
      try:
        s.close()
      except Exception:
        pass


def main() -> int:
    p = argparse.ArgumentParser(description='Bridge WebSocket healthcheck')
    p.add_argument('--host', default='127.0.0.1')
    p.add_argument('--port', type=int, default=8766)
    p.add_argument('--timeout', type=float, default=1.5)
    args = p.parse_args()

    return asyncio.run(_websocket_healthcheck(args.host, args.port, args.timeout))


if __name__ == '__main__':
    raise SystemExit(main())
