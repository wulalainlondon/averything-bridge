"""
UDP LAN discovery broadcaster for Claude Bridge.

Sends a JSON announce packet every `interval_s` seconds so that
mobile apps on the same subnet can find the bridge without manual
IP configuration.

Payload (≤512 B):
  {
    "magic": "CLAUDE_BRIDGE_DISCOVERY_V1",
    "type": "announce",
    "ws_port": 8766,
    "ips": ["192.168.1.42"],
    "hostname": "macbook.local",
    "instance_id": "b_1a2b3c4d",
    "version": "1.0",
    "ts": 1740000000
  }
"""

import asyncio
import json
import logging
import socket
import time
from typing import List, Optional, Tuple

log = logging.getLogger(__name__)

# How often (in seconds) we allow a send-failure warning to be logged.
_WARN_INTERVAL_S = 60.0


def _get_local_v4_addrs() -> List[Tuple[str, str]]:
    """Return list of (ip, broadcast) for all non-loopback IPv4 interfaces.

    Uses only stdlib socket; adds no new pip dependencies.
    The broadcast address is derived as a.b.c.255 (class-C approximation).
    """
    results: List[Tuple[str, str]] = []
    try:
        # Connect a UDP socket to an external address to discover the
        # preferred outbound interface; then also enumerate via getaddrinfo.
        # Primary method: gethostbyname_ex to get all addresses.
        hostname = socket.gethostname()
        try:
            _, _, addrs = socket.gethostbyname_ex(hostname)
        except OSError:
            addrs = []

        # Fallback: connect to a public IP and record the local address.
        try:
            with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as s:
                s.connect(("8.8.8.8", 80))
                fallback_ip = s.getsockname()[0]
                if fallback_ip not in addrs:
                    addrs.append(fallback_ip)
        except OSError:
            pass

        seen = set()
        for ip in addrs:
            if ip.startswith("127.") or ip in seen:
                continue
            seen.add(ip)
            parts = ip.split(".")
            if len(parts) == 4:
                bcast = f"{parts[0]}.{parts[1]}.{parts[2]}.255"
                results.append((ip, bcast))
    except Exception as exc:
        log.warning("discovery_broadcaster: failed to enumerate interfaces: %s", exc)
    return results


def _get_local_v6_addrs() -> List[str]:
    """Return non-loopback link-local IPv6 addresses (best-effort)."""
    results: List[str] = []
    try:
        infos = socket.getaddrinfo(socket.gethostname(), None, socket.AF_INET6)
        seen: set = set()
        for info in infos:
            ip = info[4][0]
            # Skip loopback (::1) and already-seen
            if ip in ("::1", "") or ip in seen:
                continue
            seen.add(ip)
            results.append(ip)
    except Exception:
        pass
    return results


class _BroadcastProtocol(asyncio.DatagramProtocol):
    """Minimal asyncio DatagramProtocol — we only use it for its transport."""

    def __init__(self) -> None:
        self.transport: Optional[asyncio.DatagramTransport] = None

    def connection_made(self, transport: asyncio.BaseTransport) -> None:
        self.transport = transport  # type: ignore[assignment]

    def error_received(self, exc: Exception) -> None:
        # Non-fatal; send errors are handled at call site.
        pass

    def connection_lost(self, exc: Optional[Exception]) -> None:
        pass


class DiscoveryBroadcaster:
    """Periodically broadcasts a UDP discover-me packet on the LAN."""

    def __init__(
        self,
        *,
        ws_port: int,
        discovery_port: int = 8767,
        interval_s: float = 2.0,
        instance_id: str,
        version: str,
    ) -> None:
        self._ws_port = ws_port
        self._discovery_port = discovery_port
        self._interval_s = interval_s
        self._instance_id = instance_id
        self._version = version

        self._transport: Optional[asyncio.DatagramTransport] = None
        self._protocol: Optional[_BroadcastProtocol] = None
        self._task: Optional[asyncio.Task] = None  # type: ignore[type-arg]
        self._last_warn_ts: float = 0.0

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    async def start(self) -> None:
        """Create the UDP socket and launch the broadcast loop."""
        loop = asyncio.get_event_loop()

        # Build a raw IPv4 UDP socket with SO_BROADCAST + SO_REUSEADDR.
        sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_BROADCAST, 1)
        sock.setblocking(False)
        sock.bind(("0.0.0.0", 0))

        self._transport, self._protocol = await loop.create_datagram_endpoint(
            _BroadcastProtocol,
            sock=sock,
        )

        ifaces = _get_local_v4_addrs()
        ip_list = [ip for ip, _ in ifaces]
        log.info(
            "DiscoveryBroadcaster started — port %d, interfaces: %s, instance_id: %s",
            self._discovery_port,
            ip_list or ["(none detected)"],
            self._instance_id,
        )

        self._task = asyncio.ensure_future(self._loop())

    async def stop(self) -> None:
        """Cancel the broadcast loop and close the socket cleanly."""
        if self._task and not self._task.done():
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass
        if self._transport:
            self._transport.close()
            self._transport = None
        log.info("DiscoveryBroadcaster stopped")

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _build_payload(self, ips: List[str]) -> bytes:
        hostname = socket.gethostname()
        data = {
            "magic": "CLAUDE_BRIDGE_DISCOVERY_V1",
            "type": "announce",
            "ws_port": self._ws_port,
            "ips": ips,
            "hostname": hostname,
            "instance_id": self._instance_id,
            "version": self._version,
            "ts": int(time.time()),
        }
        raw = json.dumps(data, separators=(",", ":")).encode()
        # Trim if somehow over 512 B (shouldn't happen with reasonable hostnames)
        if len(raw) > 512:
            data["ips"] = ips[:1]
            raw = json.dumps(data, separators=(",", ":")).encode()
        return raw

    def _send_v4(self, payload: bytes, ifaces: List[Tuple[str, str]]) -> None:
        if not self._transport:
            return
        targets = set()
        targets.add("255.255.255.255")
        for _, bcast in ifaces:
            targets.add(bcast)
        for addr in targets:
            try:
                self._transport.sendto(payload, (addr, self._discovery_port))
            except Exception as exc:
                self._rate_limited_warn("v4 send to %s failed: %s", addr, exc)

    def _send_v6_best_effort(self, payload: bytes) -> None:
        """Send to ff02::1 (all-nodes multicast) on all link-local interfaces."""
        v6_addrs = _get_local_v6_addrs()
        if not v6_addrs:
            return
        try:
            with socket.socket(socket.AF_INET6, socket.SOCK_DGRAM) as s:
                s.setsockopt(socket.IPPROTO_IPV6, socket.IPV6_MULTICAST_HOPS, 1)
                s.sendto(payload, ("ff02::1", self._discovery_port))
        except Exception as exc:
            self._rate_limited_warn("v6 multicast send failed: %s", exc)

    def _rate_limited_warn(self, msg: str, *args: object) -> None:
        now = time.monotonic()
        if now - self._last_warn_ts >= _WARN_INTERVAL_S:
            self._last_warn_ts = now
            log.warning("DiscoveryBroadcaster: " + msg, *args)

    async def _loop(self) -> None:
        try:
            while True:
                ifaces = _get_local_v4_addrs()
                ips = [ip for ip, _ in ifaces]
                payload = self._build_payload(ips)
                self._send_v4(payload, ifaces)
                self._send_v6_best_effort(payload)
                await asyncio.sleep(self._interval_s)
        except asyncio.CancelledError:
            pass
