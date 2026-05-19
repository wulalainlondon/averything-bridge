"""
claude-bridge doctor — startup capability checker.

Usage:
    python -m bridge.cli.doctor
    python -m bridge.cli.doctor --verbose
    python -m bridge.cli.doctor --config /path/to/config.yaml

Exit code: 1 if any check has severity='error' and ok=False, else 0.
"""
from __future__ import annotations

import argparse
import dataclasses
import sys
from datetime import datetime, timezone
from pathlib import Path

# Allow running as __main__ from the bridge directory
if __name__ == "__main__" and __package__ is None:
    import os
    sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
    __package__ = "bridge.cli"

from config import load_config
from config.checks import ALL_CHECKS, CheckResult
from config.schema import BridgeConfig
from config.defaults import DEFAULTS


_ICON = {True: "✓", False: "✗"}  # ✓ ✗
_WARN_ICON = "⚠"                       # ⚠


def _format_result(result: CheckResult) -> str:
    if result.ok:
        icon = _ICON[True]
    elif result.severity == "warning":
        icon = _WARN_ICON
    else:
        icon = _ICON[False]
    return f"{icon}  {result.message}"


def _config_summary(cfg: BridgeConfig) -> tuple[str, list[str]]:
    """Return (source description, list of non-default key=value strings)."""
    if cfg.config_file_path is None:
        source = "<defaults only — no ~/.claude-bridge/config.yaml found>"
    else:
        source = str(cfg.config_file_path)

    non_defaults: list[str] = []

    def _check(section_name: str, obj: object, default_dict: dict) -> None:
        for f in dataclasses.fields(obj):  # type: ignore[arg-type]
            val = getattr(obj, f.name)
            default = default_dict.get(f.name)
            # Normalise Path → str for comparison
            val_cmp = str(val) if isinstance(val, Path) else val
            default_cmp = str(default) if isinstance(default, Path) else default
            if val_cmp != default_cmp and default is not None:
                non_defaults.append(f"{section_name}.{f.name}={val!r}")

    _check("search", cfg.search, DEFAULTS["search"])
    _check("sources", cfg.sources, DEFAULTS["sources"])
    _check("server", cfg.server, DEFAULTS["server"])
    return source, non_defaults


def run_doctor(cfg: BridgeConfig, verbose: bool = False) -> int:
    now = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S")
    print(f"\nclaude-bridge doctor — {now}\n")

    results: list[CheckResult] = []
    for check_fn in ALL_CHECKS:
        result = check_fn()
        results.append(result)
        print(_format_result(result))

    ok_count = sum(1 for r in results if r.ok)
    warn_count = sum(1 for r in results if not r.ok and r.severity == "warning")
    err_count = sum(1 for r in results if not r.ok and r.severity == "error")

    print(f"\nSummary: {ok_count} {_ICON[True]}  {warn_count} {_WARN_ICON}  {err_count} {_ICON[False]}\n")

    source, non_defaults = _config_summary(cfg)
    print(f"Config loaded from: {source}")
    if non_defaults:
        print("Active config keys with non-default values:")
        for entry in non_defaults:
            print(f"  {entry}")
    else:
        print("Active config keys with non-default values: (none)")

    if verbose:
        print("\n--- Full config ---")
        for section_name in ("search", "sources", "server"):
            obj = getattr(cfg, section_name)
            print(f"[{section_name}]")
            for f in dataclasses.fields(obj):  # type: ignore[arg-type]
                print(f"  {f.name} = {getattr(obj, f.name)!r}")

    print()
    return 1 if err_count > 0 else 0


def main() -> None:
    parser = argparse.ArgumentParser(
        prog="python -m bridge.cli.doctor",
        description="Check claude-bridge runtime capabilities",
    )
    parser.add_argument(
        "--config", metavar="FILE", type=Path, default=None,
        help="Path to a YAML config file (overrides ~/.claude-bridge/config.yaml)",
    )
    parser.add_argument(
        "--verbose", action="store_true",
        help="Print full resolved config after checks",
    )
    args = parser.parse_args()

    cfg = load_config(extra_config_path=args.config)
    exit_code = run_doctor(cfg, verbose=args.verbose)
    sys.exit(exit_code)


if __name__ == "__main__":
    main()
