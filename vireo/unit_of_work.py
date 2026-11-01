"""The unit of work: one owned transaction per route, job step or service call.

``Database.transaction()`` is the only commit point the data layer is moving
to (``docs/plans/2026-10-25-unit-of-work-design.md``). This module holds its
machinery so ``db.py`` keeps only the façade method:

- ``UnitOfWork`` tracks one connection's open units. The outermost unit's
  transaction begins at its first write, as sqlite3's own transactions do, or
  at its start with ``immediate=True`` (``BEGIN IMMEDIATE``, for units whose
  reads decide their writes). It commits, retrying while SQLite reports
  locked/busy, when its block exits, rolls back if the block raises, and then
  runs the ``after_commit`` callbacks registered inside it. A nested unit is a
  savepoint: released when its block exits, rolled back to when it raises
  (and the exception propagates), so a caller that catches an inner failure
  and carries on keeps none of the failed inner unit.
- ``HoldingConnection`` is the connection class every ``Database`` opens. It
  exists only for the migration: while a unit is open, a data method that
  still commits on its own (``self.conn.commit()``) is held to a no-op and
  sqlite3's committing ``with conn:`` block becomes a savepoint, so wrapping a
  caller in a unit never splits it even before its callees stop committing.
  A data method that still rolls back on its own ends the unit's transaction
  part-way; the connection records that, and the unit raises
  ``TransactionOwnershipError`` at its exit instead of committing whatever
  was written after the rollback. The class is deleted when no data method
  commits or rolls back (``test_transaction_ownership.py`` reaches zero).
"""

from __future__ import annotations

import contextlib
import logging
import sqlite3
import time
from typing import TYPE_CHECKING, Literal

if TYPE_CHECKING:
    from collections.abc import Callable, Iterator
    from types import TracebackType

log = logging.getLogger(__name__)

# ``BEGIN IMMEDIATE`` retries for ``retry_busy`` units, on top of the
# connection's 30 s ``busy_timeout``, and commit retries for every unit: the
# budget ``execute_with_retry`` and ``commit_with_retry`` give a scan's writes.
BUSY_RETRIES = 5
BUSY_BASE_DELAY = 0.1

# Called with the database path whenever a ``Database`` closes over an open
# transaction. The test suite registers one to fail a test whose writes were
# never committed (``vireo/tests/conftest.py``).
discarded_transaction_listeners: list[Callable[[str], object]] = []


def report_discarded_transaction(db_path: str) -> None:
    """A ``Database`` is closing with a transaction open; its writes are lost.

    That is a write a caller asked a callee not to commit (``_commit=False``)
    and then never committed, or a transaction an error path left open.
    """
    log.warning(
        "Closing %s discarded an open transaction: its uncommitted writes are lost",
        db_path,
    )
    for listener in list(discarded_transaction_listeners):
        listener(db_path)


class TransactionOwnershipError(RuntimeError):
    """A unit of work's transaction was ended by something other than its owner."""


def _while_locked(operation: Callable[[], object], *, retries: int) -> None:
    """Run ``operation``, retrying with backoff while SQLite reports locked/busy."""
    for attempt in range(retries + 1):
        try:
            operation()
            return
        except sqlite3.OperationalError as exc:
            message = str(exc).lower()
            if ("locked" not in message and "busy" not in message) or attempt == retries:
                raise
            time.sleep(BUSY_BASE_DELAY * (2 ** attempt))


def _begin_immediate(conn: sqlite3.Connection, *, retry_busy: bool) -> None:
    _while_locked(
        lambda: conn.execute("BEGIN IMMEDIATE"),
        retries=BUSY_RETRIES if retry_busy else 0,
    )


def _commit(conn: sqlite3.Connection) -> None:
    _while_locked(conn.commit, retries=BUSY_RETRIES)


def _release(conn: sqlite3.Connection, name: str) -> None:
    try:
        conn.execute(f"RELEASE {name}")
    except sqlite3.OperationalError:
        if getattr(conn, "doomed_at", None) is not None:
            # A legacy rollback already discarded the savepoint with the
            # whole transaction; the outermost unit reports it.
            return
        raise TransactionOwnershipError(
            f"Savepoint {name} was ended before its block finished: "
            "something committed the unit of work's transaction early."
        ) from None


def _roll_back_to(conn: sqlite3.Connection, name: str) -> None:
    # Gone if a legacy rollback ended the transaction; the exception that is
    # propagating already reports the failure.
    with contextlib.suppress(sqlite3.OperationalError):
        conn.execute(f"ROLLBACK TO {name}")
        conn.execute(f"RELEASE {name}")


class HoldingConnection(sqlite3.Connection):
    """``sqlite3.Connection`` that defers to an open unit of work.

    ``commit_holds`` counts the units open on this connection. While it is
    nonzero ``commit()`` is a no-op, ``with conn:`` is a savepoint, and
    ``rollback()`` records ``doomed_at``, the connection's ``total_changes``
    at that moment, so the outermost unit refuses to commit anything written
    after it.
    """

    commit_holds = 0
    doomed_at: int | None = None
    # The outermost open unit, set while it holds this connection, so a
    # legacy ``with conn:`` savepoint owns ``after_commit`` callbacks the
    # way a nested unit does.
    unit: UnitOfWork | None = None

    def commit(self) -> None:
        if self.commit_holds:
            return
        super().commit()

    def rollback(self) -> None:
        if self.commit_holds and self.doomed_at is None:
            self.doomed_at = self.total_changes
        super().rollback()

    def __enter__(self) -> HoldingConnection:
        savepoints: list[str | None] = self.__dict__.setdefault("_with_savepoints", [])
        if self.commit_holds:
            if not self.in_transaction:
                # A SAVEPOINT outside a transaction would begin one that its
                # RELEASE commits; the unit's owner commits instead.
                _begin_immediate(self, retry_busy=False)
            name = f"uow_with_{len(savepoints)}"
            self.execute(f"SAVEPOINT {name}")
            savepoints.append(name)
            if self.unit is not None:
                self.unit.push_frame()
        else:
            savepoints.append(None)
        return super().__enter__()

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        tb: TracebackType | None,
    ) -> Literal[False]:
        name = self.__dict__["_with_savepoints"].pop()
        if name is None:
            return super().__exit__(exc_type, exc, tb)
        if self.unit is not None:
            self.unit.pop_frame(keep=exc_type is None)
        if exc_type is None:
            _release(self, name)
        else:
            _roll_back_to(self, name)
        return False


def _roll_back_after_failure(conn: sqlite3.Connection) -> None:
    """Roll back a failed unit without hiding the failure that ended it."""
    if not conn.in_transaction:
        return
    try:
        conn.rollback()
    except sqlite3.Error:
        # The error already propagating is the one the caller must see;
        # SQLite discards the transaction when the connection closes.
        log.exception("Rollback failed after a unit of work failed")


def _run_callbacks(callbacks: list[Callable[[], object]]) -> None:
    for callback in callbacks:
        try:
            callback()
        except Exception:  # broad: the unit is committed; a failed follow-up is logged and must neither report the commit as failed nor skip the callbacks after it
            log.exception("after_commit callback %r failed", callback)


def _hold(conn: sqlite3.Connection, delta: int) -> None:
    if isinstance(conn, HoldingConnection):
        conn.commit_holds += delta


class UnitOfWork:
    """The units of work open on one connection."""

    def __init__(self) -> None:
        self.depth = 0
        self._retry_busy = False
        # One list per open unit, innermost last.
        self._callbacks: list[list[Callable[[], object]]] = []

    def enter(
        self, conn: sqlite3.Connection, *, immediate: bool, retry_busy: bool, join: bool = False,
    ) -> contextlib.AbstractContextManager[None]:
        """The context for one ``transaction()`` block on ``conn``."""
        if self.depth:
            return self._nested(conn, retry_busy=retry_busy)
        if join and conn.in_transaction:
            return self._joined(conn)
        return self._outermost(conn, immediate=immediate, retry_busy=retry_busy)

    def push_frame(self) -> None:
        """Open a callback frame for a savepoint inside the open unit."""
        self._callbacks.append([])

    def pop_frame(self, *, keep: bool) -> None:
        """Close a savepoint's frame: hand its callbacks up, or drop them."""
        inner = self._callbacks.pop()
        if keep:
            self._callbacks[-1].extend(inner)

    def after_commit(self, callback: Callable[[], object]) -> None:
        """Run ``callback`` once the outermost unit commits (now, if none is open)."""
        if not self.depth:
            callback()
            return
        self._callbacks[-1].append(callback)

    @contextlib.contextmanager
    def _outermost(
        self, conn: sqlite3.Connection, *, immediate: bool, retry_busy: bool,
    ) -> Iterator[None]:
        if immediate:
            # Raises, as ``BEGIN`` does, if a transaction is already open.
            _begin_immediate(conn, retry_busy=retry_busy)
        # Otherwise an open transaction here is one an unconverted caller
        # began (a legacy ``begin_immediate()`` or a ``_commit=False`` write):
        # the unit adopts it, as ``with conn:`` always did. A migration rule.
        if isinstance(conn, HoldingConnection):
            conn.doomed_at = None
            conn.unit = self
        self.depth = 1
        self._retry_busy = retry_busy
        self._callbacks = [[]]
        _hold(conn, 1)
        finished = False
        try:
            yield
            finished = True
        finally:
            callbacks = self._callbacks[0]
            doomed_at = getattr(conn, "doomed_at", None)
            self._leave(conn)
            if not finished:
                _roll_back_after_failure(conn)
        if doomed_at is not None:
            # An unconverted data method rolled the unit back part-way. If
            # nothing was written since, the unit simply ends rolled back, as
            # it always did; committing writes made after it would keep half
            # a unit, so those are refused.
            wrote_after = conn.total_changes > doomed_at
            if conn.in_transaction:
                conn.rollback()
            if wrote_after:
                raise TransactionOwnershipError(
                    "A data method rolled back this unit of work's transaction "
                    "part-way; nothing the unit wrote was kept."
                )
            return
        committed = False
        try:
            _commit(conn)
            committed = True
        finally:
            if not committed:
                _roll_back_after_failure(conn)
        _run_callbacks(callbacks)

    @contextlib.contextmanager
    def _joined(self, conn: sqlite3.Connection) -> Iterator[None]:
        """A unit inside a transaction an unconverted caller began and will end.

        A savepoint, like a nested unit, and it never commits: the caller
        that began the transaction owns it. Its ``after_commit`` callbacks run
        when the block ends, since the owner's commit is out of sight. A
        migration rule only, like adoption.
        """
        if isinstance(conn, HoldingConnection):
            conn.doomed_at = None
            conn.unit = self
        self.depth = 1
        self._retry_busy = False
        self._callbacks = [[]]
        _hold(conn, 1)
        conn.execute("SAVEPOINT uow_join")
        finished = False
        try:
            yield
            finished = True
        finally:
            callbacks = self._callbacks[0]
            self._leave(conn)
            if not finished:
                _roll_back_to(conn, "uow_join")
        _release(conn, "uow_join")
        _run_callbacks(callbacks)

    def _leave(self, conn: sqlite3.Connection) -> None:
        self.depth = 0
        self._callbacks = []
        if isinstance(conn, HoldingConnection):
            conn.unit = None
        _hold(conn, -1)

    @contextlib.contextmanager
    def _nested(self, conn: sqlite3.Connection, *, retry_busy: bool) -> Iterator[None]:
        if not conn.in_transaction:
            # The owner has not written yet. A SAVEPOINT outside a
            # transaction would begin one that its RELEASE commits, so begin
            # it here, with the writer lock, for the owner to commit.
            _begin_immediate(conn, retry_busy=retry_busy or self._retry_busy)
        self.depth += 1
        name = f"uow_{self.depth}"
        conn.execute(f"SAVEPOINT {name}")
        self.push_frame()
        finished = False
        try:
            yield
            finished = True
        finally:
            self.depth -= 1
            self.pop_frame(keep=finished)
            if not finished:
                _roll_back_to(conn, name)
        _release(conn, name)
