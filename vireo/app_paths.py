"""Default storage locations shared with the desktop launcher.

The manifest is relative to the user's home directory. Keep the legacy layout
until relocation and migration are implemented together. These are defaults,
not overrides: callers still own CLI/config choices and catalog-relative paths.
Resolving a path never creates, moves, or deletes anything.
"""

import json
import os
from pathlib import Path

_LAYOUT: dict[str, str] = json.loads(
    (Path(__file__).parent / "data" / "app_paths.json").read_text(encoding="utf-8")
)


def default_path(name: str, *, expand_user: bool = True) -> str:
    """Resolve a named default; optionally retain ``~`` for deferred expansion.

    Expand on each call so dynamic consumers keep honoring the current home.
    Existing module-level constants may continue to resolve once at import.
    Unknown names raise KeyError rather than silently using the data root.
    """
    relative = _LAYOUT[name]
    if not expand_user:
        return "~/" + relative
    return os.path.expanduser(os.path.join("~", *relative.split("/")))
