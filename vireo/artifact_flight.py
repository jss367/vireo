"""Process-wide coordination and atomic publication for rendered artifacts.

Interactive image routes and background warming jobs can discover the same
cache miss at the same time.  ``SingleFlightGroup`` lets exactly one caller
produce a durable artifact for a resolved destination path while equal-key
callers wait and then consume the published file.
"""

from __future__ import annotations

import contextlib
import os
import tempfile
import threading
from collections.abc import Callable
from dataclasses import dataclass, field
from typing import TypeVar

from file_replace import replace_file

T = TypeVar("T")


class ArtifactProducerFailed(RuntimeError):
    """The producer for an equal-key artifact failed before publication."""


@dataclass
class _Flight:
    event: threading.Event = field(default_factory=threading.Event)
    error: BaseException | None = None


@dataclass(frozen=True)
class FlightResult[T]:
    value: T | None = None
    produced: bool = False
    skipped: bool = False


class SingleFlightGroup:
    """Coordinate one in-process producer per artifact key.

    The producer and waiter callbacks deliberately remain separate.  HTTP
    responses are request-context objects and must not be handed from a
    producer thread to waiter threads; waiters instead reopen the atomically
    published artifact through their own request context.
    """

    def __init__(self):
        self._lock = threading.Lock()
        self._flights: dict[str, _Flight] = {}

    def is_active(self, key: str) -> bool:
        with self._lock:
            return key in self._flights

    def run(
        self,
        key: str,
        producer: Callable[[], T],
        consumer: Callable[[], T],
        *,
        join: bool = True,
    ) -> FlightResult[T]:
        """Produce ``key`` once, or consume it after its producer finishes.

        With ``join=False``, an equal-key caller returns ``skipped=True``
        immediately.  This is used for speculative browser prefetches so they
        never occupy a server thread waiting for work another request owns.
        """
        with self._lock:
            flight = self._flights.get(key)
            if flight is None:
                flight = _Flight()
                self._flights[key] = flight
                is_producer = True
            elif not join:
                return FlightResult(skipped=True)
            else:
                is_producer = False

        if not is_producer:
            flight.event.wait()
            if flight.error is not None:
                raise ArtifactProducerFailed(
                    f"artifact producer failed for {key!r}"
                ) from flight.error
            return FlightResult(value=consumer(), produced=False)

        try:
            value = producer()
            return FlightResult(value=value, produced=True)
        except BaseException as exc:
            flight.error = exc
            raise
        finally:
            # Remove the registry entry before waking existing waiters.  A new
            # request can then claim a fresh flight after a failed producer;
            # existing waiters retain this flight object and see its outcome.
            with self._lock:
                if self._flights.get(key) is flight:
                    del self._flights[key]
            flight.event.set()


def atomic_write_bytes(data: bytes, destination: str) -> None:
    """Atomically publish already-encoded bytes at ``destination``."""
    destination = os.path.abspath(os.fspath(destination))
    directory = os.path.dirname(destination)
    os.makedirs(directory, exist_ok=True)
    fd, temporary = tempfile.mkstemp(
        prefix=f".{os.path.basename(destination)}.",
        suffix=".jpg.tmp",
        dir=directory,
    )
    try:
        with os.fdopen(fd, "wb") as handle:
            handle.write(data)
            handle.flush()
            os.fsync(handle.fileno())
        if not data:
            raise OSError("refusing to publish an empty artifact")
        # Flush the completed bytes before the name becomes visible.  The
        # directory fsync below is available on POSIX; os.replace remains the
        # atomic visibility boundary on every supported platform.
        replace_file(temporary, destination)
        if os.name != "nt":
            # Some mounted filesystems reject directory fsync even though the
            # atomic rename itself succeeded. Treat the extra durability flush
            # as best-effort so a valid published JPEG is never reported as a
            # failed render.
            with contextlib.suppress(OSError):
                directory_fd = os.open(directory, os.O_RDONLY)
                try:
                    os.fsync(directory_fd)
                finally:
                    os.close(directory_fd)
    finally:
        with contextlib.suppress(FileNotFoundError):
            os.unlink(temporary)


preview_artifact_flights = SingleFlightGroup()
original_artifact_flights = SingleFlightGroup()

# Only one speculative cache-miss producer may read/decode source photos at a
# time.  Visible requests can still join that artifact's flight or generate a
# different artifact without waiting behind this admission gate.
preview_prefetch_slots = threading.BoundedSemaphore(1)
