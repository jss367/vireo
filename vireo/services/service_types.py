"""Type names shared by the service modules' annotations.

Nothing here runs: the aliases are plain ``dict``/``tuple`` types and the
protocols only describe callables the web layer injects, so a service can
name what it receives without importing ``vireo/web/`` (which services never
do) or Flask's response classes at runtime.
"""

from __future__ import annotations

from collections.abc import Callable
from typing import TYPE_CHECKING, Any, Protocol

if TYPE_CHECKING:
    from flask import Response

# A JobRunner job record. The runner keeps jobs as plain dicts whose keys
# depend on the job type and phase (``id``, ``status``, ``progress``,
# ``config``, ``result``, step trees, private ``_``-prefixed bookkeeping),
# so the values are genuinely dynamic.
JobDict = dict[str, Any]

# A JSON-ready payload (request body, job config, job result, API response
# body). Values are arbitrary JSON, so they are typed ``Any``.
JsonDict = dict[str, Any]

# What a job's work function returns to ``JobRunner.start``: a result
# payload, or None when the job records nothing.
JobResult = JsonDict | None

# A job's work function, as ``JobRunner.start`` calls it.
JobWork = Callable[[JobDict], JobResult]

# A Flask view's error answer: the ``jsonify`` response and its status.
ErrorResponse = tuple["Response", int]


class JsonError(Protocol):
    """The app's ``web.responses.json_error``."""

    def __call__(
        self, msg: str, status: int = 400, *, code: str | None = None,
        message: str | None = None,
    ) -> ErrorResponse: ...

