"""A ``TrashService`` test double for the routes and ``PhotoDeletion``.

Route tests get one through the ``fake_trash`` fixture: asking for it makes
``app_and_db`` build the app with ``create_app(trash=fake_trash)``, so the
audit, duplicates and move-cleanup routes and ``PhotoDeletion`` all hold
this object. Tests that build ``PhotoDeletion`` themselves pass it as
``trash=``.

Until a test programs it, every method runs the real ``TrashService`` (under
the root ``isolated_trash`` fixture, so nothing reaches the host Trash).
``handle_trash(handler)`` replaces ``trash_paths`` and
``report_network_roots(roots)`` replaces the mount-table snapshot.

The handler receives exactly the arguments the caller passed, positional and
keyword, so it can assert on how the route called the Trash. Return the
real shape: ``(moved, successful paths, failures)``.

Every ``trash_paths`` call is recorded in ``trash_calls`` and every
mount-table query counted in ``network_roots_queries``, handled or not.
Assert on them when the test's other assertions would also hold without
the fake (a route that never reached it, or reached the real Trash
instead), so a fake that stopped being wired in fails the test.
"""

from __future__ import annotations

from collections.abc import Callable, Collection
from dataclasses import dataclass, field
from typing import Any

from services.trash import TrashService

_REAL = object()


@dataclass
class TrashCall:
    """One ``trash_paths`` call: its paths and the other arguments."""

    paths: list[str]
    args: tuple[Any, ...] = ()
    kwargs: dict[str, Any] = field(default_factory=dict)


class FakeTrash(TrashService):
    """Records Trash calls; runs the real Trash unless told otherwise."""

    def __init__(self) -> None:
        self.trash_calls: list[TrashCall] = []
        self.network_roots_queries = 0
        self._trash_handler: Callable[..., Any] | None = None
        self._network_roots: Any = _REAL

    def handle_trash(self, handler: Callable[..., Any]) -> None:
        """Answer every later ``trash_paths`` call with ``handler``."""
        self._trash_handler = handler

    def report_network_roots(self, roots: Collection[str] | None) -> None:
        """Make ``network_volume_roots()`` return ``roots`` (``None``: the query failed)."""
        self._network_roots = roots

    @property
    def trashed_paths(self) -> list[list[str]]:
        """The paths of each ``trash_paths`` call, in call order."""
        return [call.paths for call in self.trash_calls]

    def trash_paths(self, filepaths, *args, **kwargs):
        paths = list(filepaths)
        self.trash_calls.append(TrashCall(paths, args, dict(kwargs)))
        if self._trash_handler is None:
            return super().trash_paths(paths, *args, **kwargs)
        return self._trash_handler(paths, *args, **kwargs)

    def network_volume_roots(self):
        self.network_roots_queries += 1
        if self._network_roots is _REAL:
            return super().network_volume_roots()
        return self._network_roots
