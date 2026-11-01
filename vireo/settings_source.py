"""The global settings, as the data layer reads them, without importing ``config``.

``config`` owns the settings file (``~/.vireo/config.json``): loading it,
merging defaults, migrations and saving. The data layer may not import it
(``vireo/tests/test_import_graph.py``), yet a few ``Database`` reads depend on
a setting: the workspace-effective ``detector_confidence`` floor, the
``keyword_case`` override, ``sync_flags_to_xmp``, the edit-history bounds.
This module is the port between them. ``config`` installs itself here as the
provider when it is imported, and ``Database`` reads ``load()`` / ``get(key)``
from here and applies the workspace overrides itself
(``Database.effective_config``), so the result is exactly
``db.get_effective_config(cfg.load())``.

The provider is looked up on every call, never cached, so a test that
monkeypatches ``config.load`` or ``config.get``, or points
``config.CONFIG_PATH`` at a temporary file, reaches these reads unchanged.

When nothing has installed a provider yet (a script or tool that opens a
``Database`` without importing ``config``), the first read imports ``config``
by name, which installs it. That keeps every entry point reading the user's
settings as it did when the data layer imported ``config`` itself, while the
data layer's modules no longer depend on it statically.
"""

from __future__ import annotations

import importlib
from typing import Any, Protocol


class SettingsProvider(Protocol):
    def load(self) -> dict[str, Any]: ...

    def get(self, key: str) -> Any: ...

    def config_value(self, values: Any, key: str, section: str | None = None) -> Any: ...


_provider: SettingsProvider | None = None


def install(provider: SettingsProvider) -> None:
    """Make ``provider`` the source of global settings.

    The first provider wins: ``config`` installs itself on import, and a
    second copy of that module (imported as ``vireo.config``) must not
    replace the one the rest of the process configures.
    """
    global _provider
    if _provider is None:
        _provider = provider


def _installed() -> SettingsProvider:
    if _provider is None:
        # The default provider is the application's ``config`` module,
        # resolved by name on first use (as Django resolves its settings
        # module), so a script or tool that opens a Database without
        # importing config still reads the user's settings exactly as before.
        # Importing it installs it (see the end of config.py).
        importlib.import_module("config")
    assert _provider is not None
    return _provider


def load() -> dict[str, Any]:
    """The global settings, defaults merged (``config.load()``)."""
    return _installed().load()


def get(key: str) -> Any:
    """One global setting, falling back to its default (``config.get(key)``)."""
    return _installed().get(key)


def config_value(values: Any, key: str, section: str | None = None) -> Any:
    """``values[key]``, falling back to the ``DEFAULTS`` default (``config.config_value``).

    For the data layer, which may not import ``config``: pass the effective
    config (``Database.effective_config()``) and the setting's name, and the
    default comes from ``config.DEFAULTS``, never a restated literal.
    """
    return _installed().config_value(values, key, section)
