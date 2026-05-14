from __future__ import annotations

import time
from dataclasses import dataclass
from threading import Lock
from typing import Dict


@dataclass
class PerfStat:
    count: int = 0
    total_ms: float = 0.0
    max_ms: float = 0.0


class PerfTracker:
    def __init__(self, slow_threshold_ms: float = 250.0, report_interval_s: float = 60.0):
        self._slow_ms = slow_threshold_ms
        self._report_s = report_interval_s
        self._lock = Lock()
        self._stats: Dict[str, PerfStat] = {}
        self._last_report = time.time()

    def record(self, mtype: str, elapsed_ms: float, log) -> None:
        with self._lock:
            stat = self._stats.setdefault(mtype, PerfStat())
            stat.count += 1
            stat.total_ms += elapsed_ms
            if elapsed_ms > stat.max_ms:
                stat.max_ms = elapsed_ms

            if elapsed_ms >= self._slow_ms:
                log.warning("[perf] slow message: type=%s elapsed_ms=%.1f", mtype, elapsed_ms)

            now = time.time()
            if now - self._last_report >= self._report_s:
                self._last_report = now
                self._emit_summary(log)

    def _emit_summary(self, log) -> None:
        rows = []
        for mtype, s in self._stats.items():
            avg = (s.total_ms / s.count) if s.count else 0.0
            rows.append((avg, mtype, s.count, s.max_ms))
        rows.sort(reverse=True)
        top = rows[:8]
        if not top:
            return
        summary = ", ".join(
            f"{name}:n={cnt},avg={avg:.1f}ms,max={mx:.1f}ms" for avg, name, cnt, mx in top
        )
        log.info("[perf] message summary %s", summary)
