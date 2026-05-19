"""
bridge.config — public API for the configuration system.

Usage:
    from config import get_config, load_config
    cfg = get_config()
    print(cfg.search.tokenizer)
"""
from __future__ import annotations

from pathlib import Path
from typing import Optional

from .loader import load_config as _load_config
from .schema import BridgeConfig, SearchConfig, SourcesConfig, ServerConfig

__all__ = [
    "get_config",
    "load_config",
    "BridgeConfig",
    "SearchConfig",
    "SourcesConfig",
    "ServerConfig",
]

_config: Optional[BridgeConfig] = None


def load_config(extra_config_path: Optional[Path] = None) -> BridgeConfig:
    """
    Load (or reload) config from all layers and cache the result.
    Call this once at startup, optionally passing --config FILE path.
    """
    global _config
    _config = _load_config(extra_config_path=extra_config_path)
    return _config


def get_config() -> BridgeConfig:
    """
    Return the cached config. Loads with defaults if not yet initialised.
    Safe to call anywhere after process start without explicit initialisation.
    """
    global _config
    if _config is None:
        _config = _load_config()
    return _config
