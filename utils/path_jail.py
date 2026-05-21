import os


class JailEscape(ValueError):
    def __init__(self, req_path: str, resolved: str, root_dir: str):
        self.req_path = req_path
        self.resolved = resolved
        self.root_dir = root_dir
        super().__init__(
            f"Path escape: {req_path!r} resolves to {resolved!r} outside jail {root_dir!r}"
        )


def is_inside_jail(path: str, root_dir: str) -> bool:
    """Return True if *path* (already realpath'd) is inside *root_dir* (already realpath'd)."""
    try:
        return os.path.commonpath([path, root_dir]) == root_dir
    except ValueError:
        # commonpath raises ValueError on mixed absolute/relative on Windows; treat as escape.
        return False


def resolve_jailed(
    req_path: str,
    root_dir: str,
    *,
    allow_root_itself: bool = True,
) -> str:
    """Return a safe absolute path guaranteed to be inside *root_dir*.

    Parameters
    ----------
    req_path:
        The raw path requested by the caller.  May be absolute, relative,
        or contain ``~`` / ``..`` components.
    root_dir:
        Jail boundary.  Also ``realpath``'d internally, so symlinks in the
        root itself are resolved before comparison.  If empty string, the
        jail is disabled (backward-compat mode).
    allow_root_itself:
        When *False*, a resolved path that equals the jail root exactly is
        treated as an escape and raises :class:`JailEscape`.

    Returns
    -------
    str
        The resolved absolute path, always a plain ``str``.

    Raises
    ------
    JailEscape
        When the resolved path is outside the jail (or equals the root and
        *allow_root_itself* is *False*).
    """
    # --- Backward-compat mode: jail disabled ---
    if not root_dir:
        target = req_path if req_path else "~"
        return os.path.realpath(os.path.expanduser(target))

    # Defensively resolve the root itself (handles symlinks in the root path).
    real_root = os.path.realpath(root_dir)

    # --- Empty / None req_path → return jail root ---
    if not req_path:
        return real_root

    # --- Resolve the requested path ---
    expanded = os.path.expanduser(req_path)
    resolved = os.path.realpath(expanded)

    # --- Jail check ---
    inside = is_inside_jail(resolved, real_root)

    if inside and not allow_root_itself and resolved == real_root:
        raise JailEscape(req_path, resolved, root_dir)

    if not inside:
        raise JailEscape(req_path, resolved, root_dir)

    return resolved
