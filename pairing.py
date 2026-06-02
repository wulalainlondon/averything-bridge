"""
Pairing-token persistence (pairing.json read / write / clear).

The PAIRING_FILE path is read lazily from bridge_v2 on each call because
_init_paths() reassigns it at runtime. The in-memory _PAIRING dict stays in
bridge_v2 (mutated across the handler/main lifecycle).
"""

import json
import os
from pathlib import Path


def _load_pairing() -> dict:
    import bridge_v2
    try:
        with open(bridge_v2.PAIRING_FILE) as f:
            return json.load(f)
    except Exception:
        return {}


def _save_pairing(data: dict) -> None:
    import bridge_v2
    import tempfile
    path = Path(bridge_v2.PAIRING_FILE)
    path.parent.mkdir(parents=True, exist_ok=True)
    fd, tmp_str = tempfile.mkstemp(dir=path.parent, prefix=".tmp_", suffix=".json")
    tmp = Path(tmp_str)
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as f:
            json.dump(data, f)
        tmp.replace(path)
    except Exception:
        try:
            tmp.unlink(missing_ok=True)
        except Exception:
            pass
        raise


def _clear_pairing() -> None:
    import bridge_v2
    try:
        os.remove(bridge_v2.PAIRING_FILE)
    except FileNotFoundError:
        pass
