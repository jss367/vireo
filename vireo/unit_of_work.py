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
- ``GuardedConnection`` is the connection class every ``Database`` opens.
  While a unit is open on it, ``commit()``, ``rollback()`` and sqlite3's
  committing ``with conn:`` block raise ``TransactionOwnershipError``: only
  the unit that owns the transaction ends it, so a callee that tried to
  commit or roll back its caller's work fails loudly instead of splitting the
  unit (``test_transaction_ownership.py`` keeps data methods from trying).
"""

from __future__ import annotations

import contextlib
import dataclasses
import logging
import sqlite3
import time
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from collections.abc import Callable, Iterator

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
        raise TransactionOwnershipError(
            f"Savepoint {name} was ended before its block finished: "
            "something committed the unit of work's transaction early."
        ) from None


def _roll_back_to(conn: sqlite3.Connection, name: str) -> None:
    # Gone if the transaction already ended (SQLite rolls a transaction back
    # by itself after some errors); the propagating exception reports it.
    with contextlib.suppress(sqlite3.OperationalError):
        conn.execute(f"ROLLBACK TO {name}")
        conn.execute(f"RELEASE {name}")


class GuardedConnection(sqlite3.Connection):
    """``sqlite3.Connection`` that refuses to end an open unit's transaction.

    ``units`` is the ``UnitOfWork`` of the ``Database`` that opened the
    connection. While it has a unit open, ``commit()``, ``rollback()`` and
    ``with conn:`` raise ``TransactionOwnershipError``; the unit commits or
    rolls back itself after closing, when none is open.
    """

    units: UnitOfWork | None = None

    def _refuse_inside_a_unit(self, action: str) -> None:
        if self.units is not None and self.units.depth:
            raise TransactionOwnershipError(
                f"{action} inside a unit of work: the unit that owns the "
                "transaction commits or rolls it back when its block exits."
            )

    def commit(self) -> None:
        self._refuse_inside_a_unit("commit()")
        super().commit()

    def rollback(self) -> None:
        self._refuse_inside_a_unit("rollback()")
        super().rollback()

    def __enter__(self) -> GuardedConnection:
        self._refuse_inside_a_unit("with conn:")
        return super().__enter__()


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


@dataclasses.dataclass(frozen=True)
class InOwnUnit:
    """An ``after_commit`` callback that writes, run as its own unit of work.

    Callbacks run once the owning unit has committed, when no transaction is
    open, so one that writes needs an owner of its own: ``unit`` is the
    ``Database.transaction`` that owns it. Equal callbacks compare equal, so
    ``after_commit(..., once=True)`` keeps one of them per unit.
    """

    callback: Callable[[], object]
    unit: Callable[..., contextlib.AbstractContextManager[None]]
    immediate: bool = False

    def __call__(self) -> None:
        with self.unit(immediate=self.immediate):
            self.callback()


class UnitOfWork:
    """The units of work open on one connection."""

    def __init__(self) -> None:
        self.depth = 0
        self._retry_busy = False
        # True while the outermost open unit is a joined one: the owner, not
        # this unit, owns the commit, so ``after_commit`` has no durable
        # commit to hook into and is refused.
        self._in_joined = False
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

    def hold_writer_lock(self, conn: sqlite3.Connection) -> None:
        """Take SQLite's writer lock now for the open unit, if it has not yet.

        A unit that has written already holds it, and so does a transaction
        its caller began outside any unit (``begin_immediate()``), which this
        leaves alone. Raises ``TransactionOwnershipError`` when neither is
        open: the lock would begin a transaction nobody owns.
        """
        if self._owner_needs_begin(conn, "hold_writer_lock()"):
            _begin_immediate(conn, retry_busy=self._retry_busy)

    def hold_read_snapshot(self, conn: sqlite3.Connection) -> None:
        """Begin the open unit's transaction now (deferred), if it has not yet.

        The reads that follow share one snapshot, and no lock is taken until
        the first of them. A transaction already open is left alone; raises
        ``TransactionOwnershipError`` when neither a unit nor a transaction
        is open.
        """
        if self._owner_needs_begin(conn, "hold_read_snapshot()"):
            conn.execute("BEGIN")

    def _owner_needs_begin(self, conn: sqlite3.Connection, action: str) -> bool:
        if conn.in_transaction:
            return False
        if not self.depth:
            raise TransactionOwnershipError(
                f"{action} outside a unit of work: the transaction it begins "
                "would have no owner; open a unit with transaction() first."
            )
        return True

    @property
    def joined(self) -> bool:
        """True while the open units sit inside a transaction they do not own."""
        return self._in_joined

    def after_commit(self, callback: Callable[[], object], *, once: bool = False) -> None:
        """Run ``callback`` once the outermost unit commits (now, if none is open).

        With ``once``, a callback equal to one already waiting in this unit
        is not added again.
        """
        if self._in_joined:
            # A joined unit is a savepoint inside a transaction an
            # unconverted caller began and will end. Releasing the savepoint
            # does not commit the owner's transaction, so a callback that ran
            # here could perform an irreversible side effect for writes the
            # owner later rolls back. Refuse the callback instead: callers
            # that need durable follow-up work check ``owns_transaction`` and
            # run it after the join block, when they also own the commit.
            raise TransactionOwnershipError(
                "after_commit cannot be used inside a joined unit of work: "
                "the owning transaction's commit is out of sight."
            )
        if not self.depth:
            callback()
            return
        if once and any(callback == waiting for frame in self._callbacks for waiting in frame):
            return
        self._callbacks[-1].append(callback)

    @contextlib.contextmanager
    def _outermost(
        self, conn: sqlite3.Connection, *, immediate: bool, retry_busy: bool,
    ) -> Iterator[None]:
        if immediate:
            # Raises, as ``BEGIN`` does, if a transaction is already open.
            _begin_immediate(conn, retry_busy=retry_busy)
        # Otherwise an open transaction here is one a caller began outside
        # any unit (``begin_immediate()``, or a write with no unit around
        # it): the unit adopts it, as ``with conn:`` does.
        self.depth = 1
        self._retry_busy = retry_busy
        self._callbacks = [[]]
        finished = False
        try:
            yield
            finished = True
        finally:
            callbacks = self._callbacks[0]
            self._leave()
            if not finished:
                _roll_back_after_failure(conn)
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
        that began the transaction owns it. ``after_commit`` is refused here
        because savepoint release is not a durable commit, and the owner's
        commit is out of sight: callers that need durable follow-up work
        check ``owns_transaction`` and run it after the join block, when they
        also own the commit. A migration rule only, like adoption.
        """
        self.depth = 1
        self._retry_busy = False
        self._in_joined = True
        self._callbacks = [[]]
        conn.execute("SAVEPOINT uow_join")
        finished = False
        try:
            yield
            finished = True
        finally:
            self._leave()
            if not finished:
                _roll_back_to(conn, "uow_join")
        _release(conn, "uow_join")

    def _leave(self) -> None:
        self.depth = 0
        self._in_joined = False
        self._callbacks = []

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
        self._callbacks.append([])
        finished = False
        try:
            yield
            finished = True
        finally:
            self.depth -= 1
            # Hand the savepoint's callbacks to its owner, or drop them.
            inner = self._callbacks.pop()
            if finished:
                self._callbacks[-1].extend(inner)
            if not finished:
                _roll_back_to(conn, name)
        _release(conn, name)
