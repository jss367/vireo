"""SQLite database for Vireo photo browser metadata cache."""

from __future__ import annotations

import contextlib
import json
import logging
import os
import sqlite3
import time
from collections.abc import Callable, Collection, Iterable, Iterator, Mapping, Sequence
from typing import TYPE_CHECKING, Any, Literal, Self

import settings_source
from keyword_normalization import (
    normalize_keyword_display,
)
from new_images_cache import get_shared_cache
from repositories.edit_history import edit_prediction_ids
from repositories.effective_config import subject_types, workspace_effective_config
from repositories.folder_subtrees import escape_like as _escape_like
from repositories.folder_subtrees import path_for_subtree_match as _path_for_subtree_match
from repositories.keyword_provenance import location_component_rank

# The provenance lattice is re-exported: callers import it from ``db``.
from repositories.keyword_source import _KEYWORD_SOURCE_RANK as _KEYWORD_SOURCE_RANK
from repositories.keyword_source import (
    _KEYWORD_SOURCE_UNRECOGNIZED_RANK as _KEYWORD_SOURCE_UNRECOGNIZED_RANK,
)
from repositories.keyword_source import KEYWORD_SOURCE_ACCEPT as KEYWORD_SOURCE_ACCEPT
from repositories.keyword_source import KEYWORD_SOURCE_CONFLICT_SQL as KEYWORD_SOURCE_CONFLICT_SQL
from repositories.keyword_source import KEYWORD_SOURCE_MANUAL as KEYWORD_SOURCE_MANUAL
from repositories.keyword_source import KEYWORD_SOURCE_PRECEDENCE as KEYWORD_SOURCE_PRECEDENCE
from repositories.keyword_source import KEYWORD_SOURCE_UNKNOWN as KEYWORD_SOURCE_UNKNOWN
from repositories.keyword_source import keyword_source_max as keyword_source_max
from repositories.keyword_source import keyword_source_max_sql as keyword_source_max_sql
from repositories.keyword_source import keyword_source_rank as keyword_source_rank
from repositories.keyword_source import keyword_source_rank_sql as keyword_source_rank_sql
from repositories.keywords import KEYWORD_TYPES
from repositories.photo_sql import (
    PHOTO_COLS,
    PHOTO_DATE_ASC_ORDER,
    PHOTO_DETAIL_COLS,
    inclusive_date_to,
)
from repositories.prediction_review import AUTO_MATCH_REVIEW_MARKER
from repositories.prediction_rules import DECIDED_PREDICTION_STATUSES as _DECIDED_PREDICTION_STATUSES
from unit_of_work import (
    GuardedConnection,
    InOwnUnit,
    TransactionOwnershipError,
    UnitOfWork,
    report_discarded_transaction,
)

if TYPE_CHECKING:
    from types import TracebackType

    from canonical_schema import CanonicalSchema
    from repositories.audit import AuditRepository
    from repositories.caches import CachesRepository
    from repositories.card_cleanup import CardCleanupRepository
    from repositories.collections import CollectionRepository
    from repositories.companion_pairing import CompanionPairingRepository
    from repositories.computation_cache import ComputationCacheRepository
    from repositories.culling import CullingRepository
    from repositories.detections import DetectionsRepository
    from repositories.duplicates import DuplicatesRepository
    from repositories.edit_history import EditHistoryRepository
    from repositories.edits import EditsRepository
    from repositories.exif_search import ExifSearchRepository
    from repositories.folders import FolderRepository
    from repositories.identity_alternatives import IdentityAlternativesRepository
    from repositories.imports import ImportRepository
    from repositories.inat import InatRepository
    from repositories.job_history import JobHistoryRepository
    from repositories.keyword_merges import KeywordMergeRepository
    from repositories.keyword_provenance import KeywordProvenanceRepository
    from repositories.keyword_taxa import KeywordTaxonRepository
    from repositories.keyword_tree import KeywordTreeRepository
    from repositories.keywords import KeywordRepository
    from repositories.local_folders import LocalFolderRepository
    from repositories.local_workspaces import LocalWorkspaceRepository
    from repositories.locations import LocationRepository
    from repositories.masks_features import MasksFeaturesRepository
    from repositories.meta import MetaRepository
    from repositories.model_runs import ModelRunsRepository
    from repositories.moves_merge import MovesMergeRepository
    from repositories.pending_archives import PendingArchiveRepository
    from repositories.photo_keywords import PhotoKeywordRepository
    from repositories.photo_labels import PhotoLabelRepository
    from repositories.photo_metadata import PhotoMetadataRepository
    from repositories.photo_review import PhotoReviewRepository
    from repositories.photo_species import PhotoSpeciesRepository
    from repositories.photo_versions import PhotoVersionsRepository
    from repositories.photo_visibility import PhotoVisibilityRepository
    from repositories.photos import PhotoRepository
    from repositories.pipeline_features import PipelineFeaturesRepository
    from repositories.predictions import PredictionRepository
    from repositories.processes import ProcessesRepository
    from repositories.scanning import ScanningRepository
    from repositories.source_cleanup import SourceCleanupRepository
    from repositories.species_curation import SpeciesCurationRepository
    from repositories.species_identity_repairs import SpeciesIdentityRepairRepository
    from repositories.species_provenance import SpeciesProvenanceRepository
    from repositories.stats import StatsRepository
    from repositories.storage import StorageRepository
    from repositories.subjects import SubjectsRepository
    from repositories.sync import SyncRepository
    from repositories.taxa import TaxaRepository
    from repositories.working_copies import WorkingCopyRepository
    from repositories.workspace_folders import WorkspaceFolderRepository
    from repositories.workspaces import WorkspaceRepository

# Config, recipes, place details, prediction results and history payloads are
# heterogeneous JSON dictionaries. Their values intentionally use Any; IDs,
# control flags, repository accessors and transaction APIs have concrete types.

log = logging.getLogger(__name__)

_SQLITE_PARAM_CHUNK_SIZE = 800


class IncompatibleDatabaseError(RuntimeError):
    """The on-disk database is from a Vireo version this build can't open.

    Raised when schema setup fails against a pre-existing database — almost
    always because the file predates a schema change and there is no
    migration path (e.g. an old ``predictions`` table lacking the
    ``classifier_model`` column). ``CREATE TABLE IF NOT EXISTS`` silently
    skips the stale table, so the mismatch only surfaces later as an
    ``OperationalError``: SQLite spells it ``no such column: …`` or
    ``no such table: …`` when a SELECT/index references the missing
    schema, and ``table <name> has no column named <col>`` when an
    INSERT/UPDATE targets an existing-but-stale table missing a newly
    added column. We convert any of these into an actionable signal so
    callers can tell the user to back up and remove the file rather than
    crashing with a raw traceback.

    ``db_path`` is the offending file; ``cause`` is the original SQLite error
    text, preserved so genuine schema bugs (vs. legitimately old DBs) stay
    diagnosable. ``newer=True`` means the reverse mismatch — the file's
    schema version is ahead of this build (a downgraded install opening a
    catalog a newer Vireo already migrated) — where the remedy is updating
    the app, not discarding the database.
    """

    def __init__(
        self, db_path: str | os.PathLike[str], cause: str | None = None, newer: bool = False
    ) -> None:
        self.db_path = db_path
        self.cause = cause
        self.newer = newer
        if newer:
            msg = (
                f"The database at {db_path} was created by a newer version "
                f"of Vireo than this build supports"
            )
        else:
            msg = (
                f"The database at {db_path} is from an incompatible older "
                f"version of Vireo and cannot be opened by this build"
            )
        if cause:
            msg += f" ({cause})"
        super().__init__(msg)


class MissingPhotosCancelled(RuntimeError):
    """Raised when a Missing Originals filesystem scan is cancelled."""



def _is_keyword_word_char(ch):
    return ch.isalnum()


def _contains_whole_keyword_token(value, token):
    start = 0
    while True:
        idx = value.find(token, start)
        if idx < 0:
            return False
        before = idx == 0 or not _is_keyword_word_char(value[idx - 1])
        end = idx + len(token)
        after = end == len(value) or not _is_keyword_word_char(value[end])
        if before and after:
            return True
        start = idx + 1


def _sqlite_keyword_text_match(value, token, match_case=0, whole_word=0):
    if value is None or token is None:
        return 0
    value = str(value)
    token = str(token)
    if not token:
        return 1
    if not match_case:
        value = value.casefold()
        token = token.casefold()
    if whole_word:
        return 1 if _contains_whole_keyword_token(value, token) else 0
    return 1 if token in value else 0


def text_search_match(
    value: str | None, token: str | None, match_case: bool = False, whole_word: bool = False
) -> bool:
    return bool(_sqlite_keyword_text_match(value, token, match_case, whole_word))


def _keyword_token_clause(keyword, match_case=False, whole_word=False):
    """Build a WHERE clause for a multi-token keyword search.

    The query is split on whitespace into tokens; a photo matches only if
    EVERY token appears in the photo's filename or in at least one of its
    keyword names. By default matching is case-insensitive literal substring
    search. ``match_case`` switches to case-sensitive search. ``whole_word``
    requires alphanumeric boundaries, so "tern" matches "Common Tern" and
    "tern_001.jpg" but not "Western Gull". Tokens may be satisfied by
    different keywords, so "red bill" matches a photo tagged "Red-billed
    leiothrix" (both tokens in one keyword) as well as a photo tagged both
    "Reddish" and "Billboard" (one token each). In the default LIKE path,
    tokens are escaped so LIKE metacharacters (``%``/``_``) in the query match
    literally.

    Returns ``(clause_sql, params)``. The clause references the outer photos
    alias ``p``, so callers must expose the photos table as ``p``. Returns
    ``(None, [])`` when the query has no tokens (caller applies no keyword
    filter).
    """
    tokens = keyword.split()
    if not tokens:
        return None, []
    clauses = []
    params = []
    match_case_param = 1 if match_case else 0
    whole_word_param = 1 if whole_word else 0
    for tok in tokens:
        if match_case or whole_word:
            clauses.append(
                "(vireo_keyword_text_match(p.filename, ?, ?, ?) OR EXISTS ("
                "SELECT 1 FROM photo_keywords pk_s "
                "JOIN keywords k_s ON k_s.id = pk_s.keyword_id "
                "WHERE pk_s.photo_id = p.id "
                "AND vireo_keyword_text_match(k_s.name, ?, ?, ?)))"
            )
            params.extend([tok, match_case_param, whole_word_param])
            params.extend([tok, match_case_param, whole_word_param])
            continue
        like = f"%{_escape_like(tok)}%"
        clauses.append(
            "(p.filename LIKE ? ESCAPE '\\' OR EXISTS ("
            "SELECT 1 FROM photo_keywords pk_s "
            "JOIN keywords k_s ON k_s.id = pk_s.keyword_id "
            "WHERE pk_s.photo_id = p.id AND k_s.name LIKE ? ESCAPE '\\'))"
        )
        params.append(like)
        params.append(like)
    return " AND ".join(clauses), params


def _subtree_prefix(path: str) -> str:
    return _path_for_subtree_match(path) + "/"


def _subtree_relative(child_path: str, root_path: str) -> str:
    root = _path_for_subtree_match(root_path)
    child = _path_for_subtree_match(child_path)
    return child[len(root):].lstrip("/")


def _join_subtree_path(root_path: str, relative_path: str) -> str:
    parts = [p for p in relative_path.split("/") if p]
    if not parts:
        return root_path
    # Preserve ``root_path``'s separator convention. ``os.path.join`` on
    # Windows always inserts ``\``, which mixes separators when the root is
    # forward-slash — breaking equality lookups (``WHERE path = ?``) against
    # existing folder rows stored with matching separators. Subtree LIKE
    # queries normalize with REPLACE, but equality queries do not.
    sep = "\\" if ("\\" in root_path and "/" not in root_path) else "/"
    stripped = root_path.rstrip("/\\")
    if not stripped:
        return root_path + sep.join(parts)
    return stripped + sep + sep.join(parts)


def _stored_parent_path(path: str) -> str | None:
    stripped = path.rstrip("/\\")
    if not stripped or stripped in ("/", "\\"):
        return None
    sep_idx = max(stripped.rfind("/"), stripped.rfind("\\"))
    if sep_idx < 0:
        return None
    if sep_idx == 0:
        return stripped[0]
    if sep_idx == 2 and len(stripped) >= 2 and stripped[1] == ":":
        return stripped[:3]
    return stripped[:sep_idx]


def _chunks(values, size=_SQLITE_PARAM_CHUNK_SIZE):
    values = list(values)
    for idx in range(0, len(values), size):
        yield values[idx:idx + size]



NO_LOCATION_INFORMATION_RULES = {
    "mode": "all",
    "rules": [
        {"field": "has_gps", "op": "equals", "value": 0},
        {"field": "has_location_keyword", "op": "equals", "value": 0},
    ],
}

ALL_NAV_IDS = frozenset({
    "import",
    "pipeline", "jobs", "pipeline_review", "pipeline_rapid_review", "review", "cull",
    "misses", "highlights", "life_list", "browse", "edit", "map", "location_review",
    "dashboard", "storage", "audit", "card_cleanup", "move", "id_conflicts",
    "settings", "workspace", "lightroom", "shortcuts",
    "keywords", "duplicates", "logs",
})

DEFAULT_TABS = [
    "import", "browse", "pipeline", "pipeline_review",
    "review", "cull", "jobs", "highlights", "misses", "storage", "settings",
]

# Retired nav id -> current nav id. Applied when reading a workspace's saved
# tabs so a pinned tab written under an old id survives a rename instead of
# being dropped as unknown. "compare" was renamed to "id_conflicts".
NAV_ID_ALIASES = {
    "compare": "id_conflicts",
}


def commit_with_retry(
    conn: sqlite3.Connection, max_retries: int = 5, base_delay: float = 0.1
) -> None:
    """Commit ``conn`` with retry on transient "locked"/"busy" errors.

    Parallel scan workers can still race past the 30s ``busy_timeout`` PRAGMA
    under sustained write pressure. This helper catches the resulting
    ``sqlite3.OperationalError`` (``"database is locked"``/``"is busy"``) and
    retries with exponential backoff. Non-transient OperationalErrors (disk
    I/O, constraint violations) propagate immediately so the caller can mark
    folders partial and surface the failure.
    """
    for attempt in range(max_retries + 1):
        try:
            conn.commit()
            return
        except sqlite3.OperationalError as e:
            msg = str(e).lower()
            if ("locked" not in msg and "busy" not in msg) or attempt == max_retries:
                raise
            time.sleep(base_delay * (2 ** attempt))


def execute_with_retry(
    conn: sqlite3.Connection,
    sql: str,
    params: Sequence[object] | Mapping[str, object] = (),
    max_retries: int = 5,
    base_delay: float = 0.1,
) -> sqlite3.Cursor:
    """Run ``conn.execute(sql, params)`` with retry on transient
    "locked"/"busy" errors. Returns the cursor.

    The 30s ``busy_timeout`` PRAGMA covers both INSERT/UPDATE statements
    and commits, but a single 30s wait isn't enough when another writer
    holds the lock for longer (observed: a cull job's pHash backfill held
    the writer lock for the entire backfill loop and an active scan's next
    ``add_photo`` INSERT timed out, killing the scan stage). This helper
    extends ``busy_timeout`` with bounded retry/backoff so brief contention
    bursts don't abort callers mid-write.

    A negative retry budget raises ``ValueError`` instead of silently
    skipping the statement and returning no cursor.
    """
    for attempt in range(max_retries + 1):
        try:
            return conn.execute(sql, params)
        except sqlite3.OperationalError as e:
            msg = str(e).lower()
            if ("locked" not in msg and "busy" not in msg) or attempt == max_retries:
                raise
            time.sleep(base_delay * (2 ** attempt))
    raise ValueError("max_retries must be nonnegative")




def _check_keyword_type_kwarg(kwargs):
    """Reject an ``update_keyword`` ``type`` that is not a keyword type."""
    if 'type' in kwargs:
        kt = kwargs['type']
        # Guard the membership test against non-hashable JSON values —
        # api_update_keyword passes the request body through, and
        # `x in frozenset` raises TypeError on unhashable input. Treat
        # any non-string as invalid (raise ValueError so the route's
        # existing catch yields the documented 400).
        if not isinstance(kt, str) or kt not in KEYWORD_TYPES:
            raise ValueError(f"Invalid keyword type: {kt!r}")


def _keyword_update_effective_type(updates, cur_type, taxon_id):
    """``(effective_type, type_changed)`` for an ``update_keyword`` call.

    Reconciles ``updates['is_species']`` on an explicit type change.
    """
    # Peer lookup must use the EFFECTIVE type, not the pre-update
    # row type: an explicit retype kwarg or the general→taxonomy
    # auto-promotion below can move this row into a slot where a
    # peer already lives.
    effective_type = updates.get('type', cur_type)
    if 'type' not in updates and cur_type == 'general' and taxon_id:
        effective_type = 'taxonomy'
    type_changed = effective_type != cur_type
    # The type dropdown sends only {type: ...}. Keep the legacy
    # is_species flag coherent on an actual type transition:
    # otherwise demoting a taxonomy homonym to a deliberate type
    # such as 'location' leaves is_species=1, and downstream
    # queries that accept ``type='taxonomy' OR is_species=1`` still
    # treat it as a species. Do not touch no-op type submissions:
    # legacy general rows can legitimately retain is_species=1
    # until taxonomy marking normalizes them.
    if 'type' in updates and type_changed:
        updates['is_species'] = int(effective_type == 'taxonomy')
    return effective_type, type_changed


def _apply_rename_retype(updates, cur_type, taxon_id):
    """Auto-retype a renamed keyword, the same way add_keyword types an insert."""
    if cur_type == 'general':
        # Only promote to taxonomy if a match exists;
        # otherwise leave type/taxon_id alone.
        if taxon_id:
            updates.setdefault('type', 'taxonomy')
            # Gate taxon_id on the EFFECTIVE type so an
            # explicit non-taxonomy type kwarg (e.g.
            # type='location') doesn't end up with a
            # taxonomy link. Mirror add_keyword's invariant
            # for the auto-promoted case: type='taxonomy'
            # backed by a matched taxon implies is_species=1.
            if updates.get('type') == 'taxonomy':
                updates.setdefault('taxon_id', taxon_id)
                updates['is_species'] = 1
    elif (cur_type == 'taxonomy' and taxon_id
          and updates.get('type', 'taxonomy') == 'taxonomy'):
        # Already taxonomy: refresh taxon_id only if the new
        # name matches a (possibly different) taxon AND the
        # effective type stays 'taxonomy' (caller may demote
        # to 'location' etc.). If no match, leave the
        # existing link in place.
        updates.setdefault('taxon_id', taxon_id)
    # Other manual types ('location', 'people', etc.) are
    # preserved — user intent wins.


class Database:
    """Local SQLite database that caches photo metadata from XMP sidecars.

    Args:
        db_path: path to the SQLite database file (created if missing)
    """

    def __init__(self, db_path: str | os.PathLike[str], *, initialize_schema: bool = True) -> None:
        db_dir = os.path.dirname(db_path)
        if db_path != ":memory:" and db_dir:
            os.makedirs(db_dir, exist_ok=True)
        # Preserved for the new-images cache key, which compounds
        # (db_path, workspace_id) so instances against different SQLite files
        # don't cross-read each other's cached results (workspace_id=1 is
        # reused across every database as the default workspace).
        # Path-like inputs share the same cache key as their string form.
        self._db_path = os.fspath(db_path)
        self._species_provenance_before: dict[tuple[int, int], dict[str, Any] | None] = {}
        # ``conn`` exists only while the connection is open: ``close`` deletes
        # it, so its type is the connection, never ``None``. ``close`` and
        # ``__del__`` tolerate its absence if ``sqlite3.connect`` raises.
        self.conn: GuardedConnection = sqlite3.connect(
            db_path, check_same_thread=False, factory=GuardedConnection,
        )
        self._unit = UnitOfWork()
        self.conn.units = self._unit
        self.conn.row_factory = sqlite3.Row
        self.conn.create_function(
            "vireo_keyword_text_match",
            4,
            _sqlite_keyword_text_match,
            deterministic=True,
        )
        self.conn.execute("PRAGMA journal_mode=WAL")
        self.conn.execute("PRAGMA foreign_keys=ON")
        self.conn.execute("PRAGMA synchronous=NORMAL")
        self.conn.execute("PRAGMA cache_size=-10000")  # 10 MB
        self.conn.execute("PRAGMA temp_store=MEMORY")
        self.conn.execute("PRAGMA mmap_size=30000000")  # 30 MB
        self.conn.execute("PRAGMA busy_timeout=30000")  # 30 s — tolerate parallel scan writers
        self._active_workspace_id: int | None = None
        self._new_images_cache = get_shared_cache()
        if not initialize_schema:
            self._restore_active_workspace()
            return
        # Schema setup asserts the canonical schema against whatever is on
        # disk. `CREATE TABLE IF NOT EXISTS` silently skips a stale table, so
        # a database from an older Vireo (e.g. a pre-`classifier_model`
        # `predictions` table) only fails later when a dependent index/query
        # references the missing column. Convert *that* specific failure
        # into a typed, actionable error so callers can guide the user to
        # reset the file. SQLite spells the stale-schema mismatch three
        # ways depending on the failing statement: `no such column: …`
        # (SELECT / index expression referencing a missing column),
        # `no such table: …` (referencing a missing table), and
        # `table <name> has no column named <col>` (INSERT/UPDATE targeting
        # an existing-but-stale table that lacks a newly added column —
        # `_create_tables` has INSERT paths into long-lived tables like
        # `db_meta` that can hit this when the on-disk shape is older
        # than the current build expects). Other OperationalErrors — file
        # locked, read-only, full disk, I/O error — are environmental and
        # recoverable; they must propagate as themselves so the user gets
        # accurate diagnosis instead of misleading "back up and remove
        # your DB" remediation. The per-column migrations inside
        # `_create_tables` catch their own expected OperationalErrors, so
        # only genuine schema mismatches reach this handler. On a fresh
        # or current database this never raises.
        try:
            self._create_tables()
        except sqlite3.OperationalError as e:
            msg = str(e).lower()
            if (
                msg.startswith("no such column")
                or msg.startswith("no such table")
                or "has no column named" in msg
            ):
                raise IncompatibleDatabaseError(self._db_path, str(e)) from e
            raise
        with self.transaction():
            self.folders.repair_missing_parents()
        with self.transaction(immediate=True):
            self.repair_stale_folder_parents()
        with self.transaction():
            self.ensure_default_workspace()
        # Idempotent default-keyword seed. Cheap warm-path (single
        # SELECT 1 LIMIT 1 short-circuit) — matches ensure_default_workspace
        # above.
        with self.transaction():
            self.keywords.ensure_default_genres()
        from species_identity_repair import repair_on_upgrade
        repaired = repair_on_upgrade(self)
        if repaired:
            log.info("Corrected species identity for %d predictions; review decisions preserved", repaired)
        self._restore_active_workspace()

    def _restore_active_workspace(self):
        """Restore the last-used workspace on an already initialized schema."""
        last_id = self._workspace_repository().most_recently_opened_id()
        if last_id is None:
            raise RuntimeError("Vireo database has no workspace after schema initialization")
        self.set_active_workspace(last_id)

    def close(self) -> None:
        """Close the underlying sqlite3 connection.

        Safe to call multiple times. Without an explicit close, the
        connection's file descriptors only release when CPython gc collects
        the object — under Python 3.14's stricter ResourceWarning handling,
        accumulating unclosed connections in long-running test suites
        exhausts the per-process fd limit and breaks coverage's own
        sqlite database.

        Closing removes ``conn`` rather than setting it to ``None``, so the
        attribute is always a live connection while it exists; ``closed``
        reports the state.
        """
        conn = getattr(self, "conn", None)
        if conn is not None:
            # sqlite3 raises ProgrammingError when closed from a foreign
            # thread; nothing useful can be done with a handle we are dropping.
            with contextlib.suppress(sqlite3.Error):
                if conn.in_transaction:
                    report_discarded_transaction(self._db_path)
                conn.close()
            del self.conn

    @property
    def closed(self) -> bool:
        """True once ``close`` has run (or ``__init__`` never connected)."""
        return not hasattr(self, "conn")

    def __enter__(self) -> Self:
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        tb: TracebackType | None,
    ) -> Literal[False]:
        self.close()
        return False

    def __del__(self) -> None:
        # Safety net for callers that don't use the context manager or call
        # close() explicitly. __del__ may run during interpreter shutdown
        # when sqlite3 is already torn down — swallow everything.
        with contextlib.suppress(Exception):
            self.close()

    # -- Transaction control --
    #
    # How code outside the data layer opens, ends or abandons a transaction
    # without touching ``conn``. ``transaction()`` is the unit of work, the
    # commit point the data layer is moving to (see
    # ``docs/plans/2026-10-25-unit-of-work-design.md``); the rest are the
    # ``sqlite3.Connection`` operations they name, for work outside any unit
    # (read snapshots, jobs that still commit by hand). Inside a unit each of
    # them raises ``TransactionOwnershipError``: the unit alone ends its
    # transaction (``unit_of_work.GuardedConnection``).

    @property
    def in_transaction(self) -> bool:
        """True while the connection has an open transaction."""
        return self.conn.in_transaction

    @property
    def in_unit_of_work(self) -> bool:
        """True while a ``transaction()`` block is open on this Database."""
        return bool(self._unit.depth)

    def commit(self) -> None:
        """Commit the open transaction; raises inside a unit of work."""
        self.conn.commit()

    def rollback(self) -> None:
        """Roll back the open transaction; raises inside a unit of work.

        Abandon a unit by raising out of its block instead.
        """
        self.conn.rollback()
        self._species_provenance_before = {}

    def commit_with_retry(self) -> None:
        """Commit like ``commit``, retrying while SQLite reports locked or busy.

        ``db.commit_with_retry`` (the module function) on this connection:
        transient "database is locked"/"busy" errors back off and retry, and
        anything else, or the last retry's error, propagates.
        """
        commit_with_retry(self.conn)

    def begin_immediate(self) -> None:
        """Open a transaction that holds SQLite's writer lock from the start.

        ``BEGIN IMMEDIATE`` waits up to the connection's ``busy_timeout`` for
        the lock and raises ``sqlite3.OperationalError`` if it is not freed in
        time, or if a transaction is already open (BEGIN does not nest), so
        commit or roll back first. Inside a unit of work it raises
        ``TransactionOwnershipError``: use ``transaction(immediate=True)``.
        """
        self._refuse_inside_a_unit("begin_immediate()")
        self.conn.execute("BEGIN IMMEDIATE")

    def begin(self) -> None:
        """Open a deferred transaction, so the reads that follow share one snapshot.

        Plain ``BEGIN``: no lock is taken until the first statement, and every
        read after that sees the same snapshot until ``commit()`` or
        ``rollback()`` (read-only callers release it with ``rollback()``).
        Raises ``sqlite3.OperationalError`` if a transaction is already open
        (BEGIN does not nest), and ``TransactionOwnershipError`` inside a unit
        of work.
        """
        self._refuse_inside_a_unit("begin()")
        self.conn.execute("BEGIN")

    def _refuse_inside_a_unit(self, action: str) -> None:
        if self._unit.depth:
            raise TransactionOwnershipError(
                f"{action} inside a unit of work: the unit owns its transaction."
            )

    @contextlib.contextmanager
    def transaction(
        self, *, immediate: bool = False, retry_busy: bool = False, join: bool = False,
    ) -> Iterator[None]:
        """One unit of work: commit when the block exits, roll back if it raises.

        The outermost block's transaction begins at its first write, as
        sqlite3's own do, so reads and slow work (file I/O, network) before
        that write hold no lock. ``immediate=True`` begins it at once with
        ``BEGIN IMMEDIATE`` instead, holding SQLite's writer lock across the
        block so the reads that decide its writes cannot go stale; use it for
        check-then-write, and keep slow work out of such a block.
        ``retry_busy`` (for jobs) retries a ``BEGIN IMMEDIATE`` with backoff
        after ``busy_timeout`` expires instead of raising. The commit retries
        while SQLite reports locked/busy, then ``after_commit`` callbacks run.

        Inside another block it is a savepoint (beginning the owner's
        transaction, with the writer lock, if nothing has been written yet):
        released when the block exits, rolled back to when it raises (and the
        exception propagates), so a caller that catches the failure keeps
        none of the inner block.

        An outermost ``immediate`` block raises ``sqlite3.OperationalError``
        if a transaction is already open, as ``begin_immediate()`` does. An
        outermost plain block entered while one is open (an unconverted
        caller's ``begin_immediate()`` or uncommitted write) adopts it, as
        ``with conn:`` did; with ``join=True`` it instead runs as a savepoint
        inside that transaction and leaves the commit to whoever began it.
        Both are migration rules.
        """
        provenance = dict(self._species_provenance_before)
        entered = finished = False
        try:
            with self._unit.enter(
                self.conn, immediate=immediate, retry_busy=retry_busy, join=join,
            ):
                entered = True
                yield
            finished = True
        finally:
            # Python-side state follows the savepoint: a nested rollback
            # restores the snapshot taken at its start, an outermost one
            # (like ``rollback()``) drops everything. A block refused at entry
            # (``immediate`` over a caller's open transaction) took nothing,
            # so the caller's snapshot stays with the caller's transaction.
            # A joined rollback is also a savepoint rollback: the caller's SQL
            # transaction and its earlier writes survive, so the snapshot the
            # caller recorded for those writes must survive too.
            if entered and not finished:
                survives = self._unit.depth or self.in_transaction
                self._species_provenance_before = provenance if survives else {}

    def hold_writer_lock(self) -> None:
        """Inside a unit of work, take SQLite's writer lock now.

        For a callee that reads what decides its writes but cannot open the
        unit itself (``transaction(immediate=True)`` is the owner's choice):
        the reads that follow cannot go stale. A no-op once the unit has
        written; raises ``TransactionOwnershipError`` outside a unit.
        """
        self._unit.hold_writer_lock(self.conn)

    def hold_read_snapshot(self) -> None:
        """Inside a unit of work, begin its transaction now with a deferred ``BEGIN``.

        Every read that follows shares one snapshot (no lock is taken until
        the first read), for read-only units that compose several queries. A
        no-op once the unit's transaction has begun; raises
        ``TransactionOwnershipError`` outside a unit.
        """
        self._unit.hold_read_snapshot(self.conn)

    def after_commit(
        self, callback: Callable[[], object], *,
        own_unit: bool = False, immediate: bool = False, once: bool = False,
    ) -> None:
        """Run ``callback`` once the open unit of work commits.

        For work that must only happen once the data is durable (pruning
        history, deleting cached files). Dropped if the unit (or the
        savepoint it was registered in) rolls back; run at once when no unit
        is open. A callback that raises is logged and does not undo the
        commit.

        ``own_unit`` runs a callback that writes as its own unit of work
        (``immediate`` as in ``transaction``), committed on its own after the
        unit that registered it. ``once`` skips a callback equal to one this
        unit already holds, so a write recorded several times in one unit
        schedules its follow-up once.
        """
        if own_unit:
            callback = InOwnUnit(callback, self.transaction, immediate)
        self._unit.after_commit(callback, once=once)

    def set_progress_handler(self, handler: Callable[[], int] | None, n: int) -> None:
        """``sqlite3.Connection.set_progress_handler`` on this connection.

        ``handler`` runs every ``n`` SQLite VM instructions, and a truthy
        return interrupts the running statement (``OperationalError:
        interrupted``); ``None`` removes it.
        """
        self.conn.set_progress_handler(handler, n)

    def _canonical_schema(self) -> CanonicalSchema:
        """Build the canonical-schema setup on this connection."""
        from canonical_schema import CanonicalSchema

        return CanonicalSchema(self.conn)

    def _create_tables(self):
        self._canonical_schema().create_tables()

    # ------------------------------------------------------------------
    # Saved processes (user-editable process presets; global, not scoped)
    # ------------------------------------------------------------------
    def _processes_repository(self) -> ProcessesRepository:
        """Build the saved-processes repository on this connection.

        Saved processes are global (not workspace-scoped), so no active
        workspace is needed.
        """
        from repositories.processes import ProcessesRepository

        return ProcessesRepository(self.conn)

    @property
    def processes(self) -> ProcessesRepository:
        """The saved-process domain: ``db.processes.list_all()``, ``.get(id)``, ``.create(name, ...)``.

        A domain accessor, not a cached attribute: every access builds a fresh
        repository through ``_processes_repository``, exactly as a forwarding
        wrapper called at that moment would. Saved processes are global, so it
        needs no active workspace. Update and delete live in
        ``services/saved_processes.py`` (``update_saved_process`` /
        ``delete_saved_process``), which check the row exists first.
        """
        return self._processes_repository()

    def repair_stale_folder_parents(self) -> int:
        """Repair parent links left behind by older folder moves.

        Only non-NULL links that contradict the saved paths are changed.
        Managed local copies deliberately retain their archive parentage, so
        defer links involving those folders until their local session ends.
        This runs on startup and is idempotent; no filesystem access or photo
        and workspace membership changes are needed, even for offline paths.
        """
        updates = self._folder_repository().repair_stale_parents()
        if updates:
            log.info("Repaired %d stale folder parent links", len(updates))
        return len(updates)

    # -- Workspaces --

    def set_active_workspace(self, workspace_id: int | None) -> None:
        """Set the active workspace for scoped queries."""
        self._active_workspace_id = workspace_id

    def _ws_id(self) -> int:
        """Return active workspace id, raising if none set."""
        if self._active_workspace_id is None:
            raise RuntimeError("No active workspace set")
        return self._active_workspace_id

    @property
    def active_workspace_id(self) -> int | None:
        """The active workspace id, or ``None`` when no workspace is active.

        The public read of the active-workspace state for code outside this
        class; change it through ``set_active_workspace``.
        """
        return self._active_workspace_id

    def require_workspace_id(self) -> int:
        """Return the active workspace id, raising ``RuntimeError`` if none is set."""
        return self._ws_id()

    def invalidate_new_images_cache_for_folders(self, folder_ids: Collection[int]) -> None:
        """Clear cache for every workspace linked to any of the given folder_ids."""
        if not folder_ids:
            return
        ws_ids = self._workspace_repository().ids_for_folders(folder_ids)
        self._new_images_cache.invalidate_workspaces(self._db_path, ws_ids)

    def invalidate_new_images_cache_for_workspace(self, workspace_id: int) -> None:
        """Clear path-dependent new-image results for one workspace."""
        self._new_images_cache.invalidate_workspaces(
            self._db_path, [workspace_id]
        )

    def invalidate_new_images_cache_for_workspaces(self, workspace_ids: Iterable[int]) -> None:
        """Clear new-image results for several workspaces under one cache lock."""
        self._new_images_cache.invalidate_workspaces(self._db_path, workspace_ids)

    def _verify_photo_in_workspace(self, photo_id: int) -> None:
        """Raise ValueError if the photo is not in the active workspace."""
        if not self.workspace_folders.photo_in_workspace(photo_id, self._ws_id()):
            raise ValueError(
                f"Photo {photo_id} does not belong to the active workspace"
            )

    def _photo_syncable_in_workspace(self, photo_id: int) -> bool:
        """Return True if the workspace may write this photo's sidecar.

        Library membership, or a ``workspace_sync_only_photos`` grant --
        the narrow record that tracked-merge collision handling writes so a
        remapped edit stays syncable without the workspace gaining
        visibility of every other photo in the folder.
        """
        if self.workspace_folders.photo_in_workspace(photo_id, self._ws_id()):
            return True
        return self._moves_merge_repository().photo_has_sync_only_grant(
            photo_id, self._ws_id()
        )

    def _verify_photo_syncable_in_workspace(self, photo_id: int) -> None:
        """Raise ValueError if the workspace may not write this sidecar."""
        if not self._photo_syncable_in_workspace(photo_id):
            raise ValueError(
                f"Photo {photo_id} does not belong to the active workspace"
            )

    def create_workspace(
        self,
        name: str,
        config_overrides: dict[str, Any] | None = None,
        ui_state: dict[str, Any] | None = None,
    ) -> int:
        """Create a new workspace (no commit: the caller's unit owns it). Returns the id."""
        from catalog_result_files import highest_workspace_id
        id_floor = (
            highest_workspace_id(os.path.dirname(self._db_path))
            if self._db_path != ":memory:" else 0
        )
        workspace_id = self._workspace_repository().create(
            name, config_overrides, ui_state, id_floor=id_floor,
        )
        # Invalidate any cached reader state even though IDs now advance
        # monotonically past both catalog rows and legacy result files --
        # once the caller's unit has committed the row.
        self.after_commit(
            lambda: self._new_images_cache.invalidate_workspaces(self._db_path, [workspace_id])
        )
        return workspace_id

    def get_effective_config(self, global_config: dict[str, Any]) -> dict[str, Any]:
        """Return config with workspace overrides applied over global config.

        Args:
            global_config: dict from config.load()
        Returns:
            dict with workspace overrides deep-merged on top of global config
            so a nested override (e.g. ``{"pipeline": {"w_focus": 0.5}}``)
            replaces only the named leaf, not the whole parent dict
            (``repositories.effective_config.workspace_effective_config``).
        """
        return workspace_effective_config(self.conn, self._active_workspace_id, global_config)

    def effective_config(self) -> dict[str, Any]:
        """The global settings with the active workspace's overrides applied.

        Exactly ``get_effective_config(cfg.load())``: the global settings
        come through ``settings_source`` (which ``config`` installs), so the
        data layer never imports ``config``. Repositories read the same
        value on their own connection through
        ``repositories.effective_config.active_effective_config``.
        """
        return self.get_effective_config(settings_source.load())

    def get_subject_types(self) -> set[str]:
        """Return the keyword types that count as 'identified' for the active
        workspace (``repositories.effective_config.subject_types``).
        """
        return subject_types(self.effective_config())

    # Conservative chunk size for filter_out_subject_tagged. Most modern
    # SQLite builds default to SQLITE_MAX_VARIABLE_NUMBER=32766, but older
    # / Linux-distro builds still ship with the historical 999 cap. 800
    # leaves comfortable headroom for the type binds (up to 5) plus a
    # safety margin. Module-level constant so tests can monkeypatch.
    _FILTER_SUBJECT_CHUNK = 800

    def _photo_visibility_repository(self) -> PhotoVisibilityRepository:
        """Build the photo-visibility repository on this connection.

        It receives ``self._ws_id`` uncalled: the grant and revoke writes take
        their workspace id as an argument and never resolve it, while
        ``visible_photo_ids``, ``affected_workspaces`` and
        ``preserve_for_move`` resolve the active workspace (raising
        ``RuntimeError`` when none is set) before running any SQL.
        """
        from repositories.photo_visibility import PhotoVisibilityRepository

        return PhotoVisibilityRepository(
            self.conn, self._ws_id, chunk_size=_SQLITE_PARAM_CHUNK_SIZE,
        )

    @property
    def photo_visibility(self) -> PhotoVisibilityRepository:
        """Photo-level workspace access: ``db.photo_visibility.visible_photo_ids(ids)`` and friends.

        A domain accessor, not a cached attribute: every access builds a fresh
        repository through ``_photo_visibility_repository``, exactly as a
        forwarding wrapper called at that moment would. Accessing it never
        needs a workspace; the methods that act on the active workspace
        resolve it when they run (and raise there when none is set). Do not
        hold the returned repository across ``set_active_workspace``.
        """
        return self._photo_visibility_repository()

    def _workspace_repository(self) -> WorkspaceRepository:
        """Build the workspace repository on this connection.

        It receives ``self._ws_id`` uncalled: the active-workspace methods
        (tabs, new-images snapshots) resolve it first thing, raising
        ``RuntimeError`` when none is set, while the catalog-wide methods take
        the workspace id as an argument and never resolve it.
        """
        from repositories.workspaces import WorkspaceRepository

        return WorkspaceRepository(
            self.conn,
            self._ws_id,
            allowed_nav_ids=ALL_NAV_IDS,
            default_tabs=DEFAULT_TABS,
            nav_id_aliases=NAV_ID_ALIASES,
            chunk_size=_SQLITE_PARAM_CHUNK_SIZE,
        )

    @property
    def workspaces(self) -> WorkspaceRepository:
        """The workspace domain: ``db.workspaces.get(id)``, ``.update(id, ...)``, ``.get_tabs()``, ...

        A domain accessor, not a cached attribute: every access builds a fresh
        repository through ``_workspace_repository``. Accessing it never needs
        a workspace. The tab and new-images-snapshot methods act on the
        workspace active when they run and raise ``RuntimeError`` when none is
        set; the row methods (``get``, ``id_for_name``, ``list_all``,
        ``update``, ``set_group_state``, ``forget_label_file``) take an id or
        span every workspace and work with none active. Creating and deleting
        a workspace also clear the new-images cache, so callers go through
        ``Database.create_workspace`` (``__init__`` runs it) and
        ``services.workspace_lifecycle.delete_workspace``.
        """
        return self._workspace_repository()

    def _workspace_folder_repository(self) -> WorkspaceFolderRepository:
        """Build the workspace-folder membership repository on this connection.

        Every membership method takes its workspace id explicitly, so the
        repository is not bound to the active workspace; callers of
        ``photo_in_workspace`` and ``visible_ids`` pass it, and the
        ``services.workspace_folders`` functions that default to it
        (``get_workspace_root_folder_ids`` and the like) resolve it themselves.
        """
        from repositories.workspace_folders import WorkspaceFolderRepository

        return WorkspaceFolderRepository(
            self.conn,
            path_for_subtree_match=_path_for_subtree_match,
            chunk_size=_SQLITE_PARAM_CHUNK_SIZE,
        )

    @property
    def workspace_folders(self) -> WorkspaceFolderRepository:
        """Workspace-folder membership: ``db.workspace_folders.list_workspaces_for_folder(id)``.

        A domain accessor, not a cached attribute: every access builds a fresh
        repository through ``_workspace_folder_repository``, exactly as a
        forwarding wrapper called at that moment would. Every method takes its
        workspace id explicitly, so accessing it never needs an active
        workspace. Membership changes that also walk subtrees, record
        removals or invalidate the new-images cache (``add_workspace_folder``,
        ``remove_workspace_folder_tree``, ``get_workspace_folders``, ...)
        live in ``services/workspace_folders.py``.
        """
        return self._workspace_folder_repository()

    # -- Move source cleanup (``move_cleanup`` owns the transaction) --

    def _source_cleanup_repository(self) -> SourceCleanupRepository:
        """Build the (catalog-wide) move-source-cleanup repository on this connection."""
        from repositories.source_cleanup import SourceCleanupRepository

        return SourceCleanupRepository(self.conn)

    @property
    def source_cleanup(self) -> SourceCleanupRepository:
        """Move-source cleanup SQL: ``db.source_cleanup.folder_has_photos(id)`` and friends.

        A domain accessor, not a cached attribute: every access builds a fresh
        repository through ``_source_cleanup_repository``, exactly as a
        forwarding wrapper called at that moment would. Nothing in it is
        scoped to the active workspace, so accessing it never needs one, and
        nothing in it commits: ``move_cleanup`` opens and ends the transaction.
        """
        return self._source_cleanup_repository()

    def ensure_default_workspace(self) -> int:
        """Create the Default workspace if it doesn't exist (no commit). Returns its id."""
        default_id = self._workspace_repository().default_id()
        if default_id is not None:
            return default_id
        return self.create_workspace("Default")

    def _species_curation_repository(self) -> SpeciesCurationRepository:
        """Build the species-curation repository on this connection.

        The active workspace is resolved lazily (``Database._ws_id`` is
        passed as a resolver), so methods that return early or canonicalize
        a species name first still raise at the point they always did.
        """
        from repositories.species_curation import SpeciesCurationRepository

        return SpeciesCurationRepository(self.conn, self._ws_id)

    @property
    def species_curation(self) -> SpeciesCurationRepository:
        """Species curation: ``db.species_curation.get_highlights(...)`` and friends.

        Highlights ordering, the global representative photos, the
        per-workspace photo preferences and the Life List / taxonomy-explorer
        reads. A domain accessor, not a cached attribute: every access builds
        a fresh repository through ``_species_curation_repository``, exactly
        as a forwarding wrapper called at that moment would. Accessing it
        never needs a workspace; the workspace-scoped methods resolve the
        active one when they run (raising ``RuntimeError`` when none is set).
        """
        return self._species_curation_repository()

    # -- Reference taxonomy (``taxonomy.py`` loaders own the commits) --

    def _taxa_repository(self) -> TaxaRepository:
        """Build the (catalog-wide) reference-taxonomy repository on this connection."""
        from repositories.taxa import TaxaRepository

        return TaxaRepository(self.conn)

    @property
    def taxa(self) -> TaxaRepository:
        """The reference-taxonomy domain: ``db.taxa.upsert_taxon(...)`` and friends.

        A domain accessor, not a cached attribute: every access builds a fresh
        repository through ``_taxa_repository``, exactly as a forwarding
        wrapper called at that moment would, so the connection is resolved per
        use. Nothing in it is workspace-scoped and nothing in it commits; the
        ``taxonomy.py`` loaders keep their own commit points.
        """
        return self._taxa_repository()

    # -- Species identity repairs (callers own the transactions) --

    def _species_identity_repair_repository(self) -> SpeciesIdentityRepairRepository:
        """Build the (catalog-wide) species-identity repair repository on this connection."""
        from repositories.species_identity_repairs import SpeciesIdentityRepairRepository

        return SpeciesIdentityRepairRepository(self.conn)

    @property
    def species_identity_repairs(self) -> SpeciesIdentityRepairRepository:
        """The species-identity repair domain: ``db.species_identity_repairs.record_repair(...)`` and friends.

        A domain accessor, not a cached attribute: every access builds a fresh
        repository through ``_species_identity_repair_repository``, exactly as
        a forwarding wrapper called at that moment would, so the connection is
        resolved per use. Nothing in it is workspace-scoped and nothing in it
        commits or opens a transaction; ``species_identity_repair`` and
        ``label_source_identities`` keep their own transaction boundaries.
        """
        return self._species_identity_repair_repository()

    # -- Folders --

    def _folder_repository(self) -> FolderRepository:
        """Build the folder repository on this connection.

        It receives ``self._ws_id`` uncalled: the workspace-scoped reads
        (``tree``, ``subtree_ids``, ``missing``, ``count``,
        ``with_quality_data``) resolve it before any SQL, raising
        ``RuntimeError`` when none is set, and the catalog-wide methods never
        resolve it. The path helpers are looked up here, so monkeypatches of
        the ``db`` module functions still reach the moved SQL. A folder merge
        runs in ``services.folder_merges``, which composes the cross-domain
        carries.
        """
        from repositories.folders import FolderRepository

        return FolderRepository(
            self.conn,
            self._ws_id,
            path_for_subtree_match=_path_for_subtree_match,
            stored_parent_path=_stored_parent_path,
            subtree_prefix=_subtree_prefix,
            subtree_relative=_subtree_relative,
            join_subtree_path=_join_subtree_path,
            chunk_size=_SQLITE_PARAM_CHUNK_SIZE,
            keyword_source_conflict_sql=KEYWORD_SOURCE_CONFLICT_SQL,
        )

    @property
    def folders(self) -> FolderRepository:
        """The folder rows: ``db.folders.get(folder_id)``, ``db.folders.tree()`` and friends.

        A domain accessor, not a cached attribute: every access builds a
        fresh repository through ``_folder_repository``, exactly as a
        forwarding wrapper called at that moment would. Accessing it never
        needs a workspace; the workspace-scoped reads resolve the active one
        when they run.
        """
        return self._folder_repository()

    # -- Library integrity verification --

    def _audit_repository(self) -> AuditRepository:
        """Build the audit repository on this connection.

        It receives ``self._ws_id`` uncalled: the audit-run and integrity
        reads resolve it first thing (raising ``RuntimeError`` when no
        workspace is active), while the catalog-wide hash-check write takes
        the photo id as an argument and never resolves it.
        """
        from repositories.audit import AuditRepository

        return AuditRepository(
            self.conn,
            self._ws_id,
            chunk_size=_SQLITE_PARAM_CHUNK_SIZE,
        )

    @property
    def audit(self) -> AuditRepository:
        """The library-audit domain: ``db.audit.record_run(...)``, ``.update_photo_hash_check(...)``, ...

        A domain accessor, not a cached attribute: every access builds a fresh
        repository through ``_audit_repository``. Accessing it never needs a
        workspace; ``record_run``, ``get_runs`` and the ``get_integrity_*``
        reads act on the workspace active when they run and raise
        ``RuntimeError`` when none is set, and ``update_photo_hash_check``
        works by photo id with none active.
        """
        return self._audit_repository()

    # -- Card cleanup (``card_cleanup`` owns the gates and the commits) --

    def _card_cleanup_repository(self) -> CardCleanupRepository:
        """Build the (catalog-wide) card-cleanup repository on this connection."""
        from repositories.card_cleanup import CardCleanupRepository

        return CardCleanupRepository(self.conn)

    @property
    def card_cleanup(self) -> CardCleanupRepository:
        """Card cleanup's catalog SQL: ``db.card_cleanup.rows_by_hash(hash)`` and friends.

        A domain accessor, not a cached attribute: every access builds a fresh
        repository through ``_card_cleanup_repository``, exactly as a
        forwarding wrapper called at that moment would. Nothing in it is
        scoped to a workspace, and its verdict write runs in the caller's
        transaction (``card_cleanup`` commits through ``db.commit()``).
        """
        return self._card_cleanup_repository()

    # -- Move operations --

    def _moves_merge_repository(self) -> MovesMergeRepository:
        """Build the moves/merge repository on this connection.

        Folders, photos and keywords are catalog-wide, so the repository takes
        no workspace id: its workspace-scoped callers resolve the active one
        themselves (``services.moves_merge.merge_staged_tree_into_archive``
        passes ``require_workspace_id`` as a callable, after its staged-root
        lookup). The subtree path
        helpers are ``db`` module functions and are passed in, and so is the
        folder parent relink every move or merge runs mid-transaction. The
        keyword-association carry is the repository's own; it re-tags through
        the provenance writer.
        """
        from repositories.moves_merge import MovesMergeRepository

        return MovesMergeRepository(
            self.conn,
            subtree_prefix=_subtree_prefix,
            subtree_relative=_subtree_relative,
            join_subtree_path=_join_subtree_path,
            # Late-bound: ``db.folders`` binds this repository's GPS-review
            # carry, so building it here would recurse.
            relink_parents_by_path=lambda folder_ids: (
                self.folders.relink_parents_by_path(folder_ids)),
        )

    @property
    def moves_merge(self) -> MovesMergeRepository:
        """The moves domain: ``db.moves_merge.folder_path_row(id)`` and friends.

        A domain accessor, not a cached attribute: every access builds a fresh
        repository through ``_moves_merge_repository``, exactly as a forwarding
        wrapper called at that moment would, so the connection is resolved per
        use. ``move.py``'s file-move jobs reach their SQL through it.
        """
        return self._moves_merge_repository()

    def _move_location_state_for_merge(self, losing_id, surviving_id):
        """Move ``losing_id``'s location tags onto ``surviving_id`` if newer.

        A queued ``location`` change stores no coordinates: ``sync_to_xmp``
        derives them at write time from the photo's link to a
        ``type='location'`` keyword. When the merge deletes one of two rows
        that both describe the same on-disk file, the survivor's tags are
        what any remapped queue row will write, so the losing row's
        location intent has to travel with the remap or the sync silently
        writes the wrong GPS — or clears it.

        Whose assignment wins is decided by queue chronology, not by which
        row the merge happens to delete. If the survivor carries a location
        change queued *later* than the loser's, the survivor's tags already
        encode the newer intent and are left alone; otherwise the loser's
        tags replace them. An empty tag list on a winning loser encodes a
        "clear location" edit and drops the survivor's tag too.

        Chronology is compared across every workspace, not just the active
        one: ``photo_keywords`` is global, so a location change queued in a
        sibling workspace describes the same tags this merge is about to
        rewrite and its timestamp counts the same.

        Returns ``True`` when the survivor's tags were replaced.
        """
        loser_key = self.moves_merge.newest_location_change_key(losing_id)
        if loser_key is None:
            # Nothing queued on the row being deleted: its tags carry no
            # pending intent, and the survivor keeps whatever it has.
            return False
        survivor_key = self.moves_merge.newest_location_change_key(surviving_id)
        if survivor_key is not None and survivor_key > loser_key:
            # The survivor's own queued assignment is the newer one. This is
            # the replacement-import shape: a stale archive row still holds
            # an old queued location while the row that will represent the
            # file has a newer one that the pre-transfer sync may already
            # have written. Taking the loser's tags here would revert it.
            return False
        losing_kw_ids = self._moves_merge_repository().prepare_location_transfer(
            losing_id, surviving_id)
        for kw_id in losing_kw_ids:
            # Route through tag_photo rather than a raw INSERT so the write
            # carries the manual provenance stamp and folds against any
            # existing survivor row through the shared upsert. A raw insert
            # would land with source = NULL, which retirement passes read as
            # generated and delete. A queued ``location`` change originates
            # in set_photo_location, so the intent riding along is
            # user-authored.
            self.keyword_provenance.tag(surviving_id, kw_id, source=KEYWORD_SOURCE_MANUAL)
        return True

    # -- Photos --

    @property
    def photos(self) -> PhotoRepository:
        """Photo rows: ``db.photos.get(photo_id)``, ``db.photos.list_page(...)`` and friends.

        A domain accessor, not a cached attribute: every access builds a
        fresh repository through ``_photos_repository``. Catalog-wide
        methods (``get``, ``get_by_ids``, ``update_quality`` ...) never read
        the workspace; the workspace-scoped reads (``list_page``,
        ``get_ids``, ``count``, the Best Batch rows ...) resolve the active
        one before any SQL and raise when none is active, so never hold one
        across ``set_active_workspace``. ``add_photo`` lives in
        ``services/photo_ingest.py`` and ``delete_photos`` in
        ``services/catalog_deletion.py``.
        """
        return self._photos_repository()

    def _photos_repository(self) -> PhotoRepository:
        """Build the core photo-row repository on this connection.

        Photo rows are global, so the workspace is passed as
        ``Database._ws_id`` itself, uncalled: the reads that go through
        ``workspace_folders`` resolve it (raising if none is active) before
        any SQL, and the catalog-wide methods never do. The column lists and
        the ``db`` module helpers are passed in, the helpers read at call
        time so tests that patch ``db.execute_with_retry`` still apply.
        """
        from repositories.photos import PhotoRepository

        return PhotoRepository(
            self.conn,
            self._ws_id,
            chunk_size=_SQLITE_PARAM_CHUNK_SIZE,
            filter_subject_chunk=self._FILTER_SUBJECT_CHUNK,
            photo_cols=PHOTO_COLS,
            photo_detail_cols=PHOTO_DETAIL_COLS,
            execute_with_retry=execute_with_retry,
            inclusive_date_to=inclusive_date_to,
            keyword_token_clause=_keyword_token_clause,
        )

    def _duplicates_repository(self) -> DuplicatesRepository:
        """Build the duplicates repository on this connection.

        Every duplicate method is catalog-wide (photos are global), so the
        repository takes no workspace id and never calls ``_ws_id()``.
        """
        from repositories.duplicates import DuplicatesRepository

        return DuplicatesRepository(
            self.conn, chunk_size=_SQLITE_PARAM_CHUNK_SIZE,
        )

    @property
    def duplicates(self) -> DuplicatesRepository:
        """The exact-duplicate domain: ``db.duplicates.find_groups()``, ``.reopen(hash)``, ...

        A domain accessor, not a cached attribute: every access builds a fresh
        repository through ``_duplicates_repository``, exactly as a forwarding
        wrapper called at that moment would. Every method is catalog-wide, so
        it needs no active workspace. Resolution lives in
        ``services/duplicate_resolution.py`` (``apply_duplicate_resolution``,
        ``bulk_resolve_by_folder``, ``keep_duplicate_copy``,
        ``check_and_resolve_duplicates_for_hash``); the merge they share,
        ``_apply_winner_loser_merge``, stays here; it re-tags the winner
        through the provenance writer (``keyword_provenance.tag``).
        """
        return self._duplicates_repository()

    def _apply_winner_loser_merge(self, winner_id, loser_ids, winner_flag=None, *, manual_review=False):
        """Merge rating/keywords from losers onto winner, then flag losers
        as rejected, in the caller's unit of work (never commits). Shared
        between the resolver-based
        ``duplicate_resolution.apply_duplicate_resolution`` and the
        user-driven ``bulk_resolve_by_folder`` and ``keep_duplicate_copy``,
        each of which reads its plan and applies it in one immediate unit.

        ``winner_flag``, when given, is written to the winner in the same
        unit (``keep_duplicate_copy`` un-rejects the copy it keeps).
        Only explicit user choices publish manual flag attribution; automatic
        resolution never publishes new manual confirmation.
        """
        from duplicates import merge_metadata
        from repositories.species_provenance import evidence_for_photo_copy

        repo = self._duplicates_repository()
        winner_meta = repo.photo_metadata(winner_id)
        loser_metas = [repo.photo_metadata(lid) for lid in loser_ids]
        merge = merge_metadata(winner_meta, loser_metas)

        if winner_flag is not None:
            repo.set_flag(winner_id, winner_flag)
        if merge.new_rating != winner_meta.rating:
            repo.set_rating(winner_id, merge.new_rating)
        # Carry every loser's durable provenance -- and its species
        # evidence -- onto the winner, including keyword IDs the winner
        # already has. merge_metadata() omits overlaps from
        # keyword_ids_to_add, but a manual loser must still upgrade a
        # weaker winner association -- so this loop is driven by what the
        # losers carry, not by what needs adding. tag_photo's upsert folds
        # against the winner's own stamp, so a weak loser can never pull a
        # stronger winner down; the same upsert keeps the winner's own
        # species_provenance when the winner already recorded one.
        loser_keyword_sources: dict[int, str | None] = {}
        loser_keyword_evidence: dict[int, str | None] = {}
        for row in repo.loser_keyword_rows(loser_ids, survivor_id=winner_id):
            kw_id = row["keyword_id"]
            loser_keyword_sources[kw_id] = keyword_source_max(
                row["source"], loser_keyword_sources.get(kw_id),
            )
            if row["species_provenance"] is not None and loser_keyword_evidence.get(kw_id) is None:
                loser_keyword_evidence[kw_id] = evidence_for_photo_copy(row["species_provenance"], row["photo_id"])
        for kw_id, merged_source in loser_keyword_sources.items():
            self.keyword_provenance.tag(
                winner_id, kw_id, source=merged_source,
                species_provenance=loser_keyword_evidence.get(kw_id),
            )
        # Rejected losers remain in the catalog and can be reopened.
        # Copy their suppression state after choosing carried tags, so
        # both identities retain removals and one loser's detached key
        # cannot suppress another loser's attached tag during this merge.
        repo.copy_loser_embedded_offered_keys(winner_id, loser_ids)
        # TODO: pending-edit copy for duplicate merge — see plan Task 7.
        # Skipped because pending_changes is workspace-scoped and its
        # value/change_token columns are non-trivial to copy safely in
        # this transaction. Rare edge case; revisit if product needs it.
        # Collections are rule-based (no junction table) so
        # merge.collection_ids_to_add has nothing to write either.
        repo.reject(loser_ids)
        if manual_review:
            self.photo_review.confirm_duplicate_choices(winner_id, loser_ids)

        logging.getLogger(__name__).info(
            "Duplicate resolved: kept id=%s, rejected id(s)=%s",
            winner_id,
            loser_ids,
        )


    # Coverage signals shown on the dashboard. Each entry is a (key, SQL
    # predicate) pair; the predicate references the ``photos`` alias ``p`` and
    # returns 1 when that pipeline stage has run for the row. Detection and
    # classification are joined in separately since they live in other tables.
    _COVERAGE_PHOTO_COLUMNS = [
        ("timestamp", "p.timestamp IS NOT NULL"),
        ("exif", "p.exif_data IS NOT NULL"),
        ("gps", "p.latitude IS NOT NULL AND p.longitude IS NOT NULL"),
        ("file_hash", "p.file_hash IS NOT NULL"),
        ("phash", "p.phash IS NOT NULL"),
        ("thumbnail", "p.thumb_path IS NOT NULL"),
        ("working_copy", "p.working_copy_path IS NOT NULL"),
        ("mask", "p.mask_path IS NOT NULL"),
        ("subject_sharpness", "p.subject_tenengrad IS NOT NULL"),
        ("bg_sharpness", "p.bg_tenengrad IS NOT NULL"),
        ("eye", "p.eye_x IS NOT NULL"),
        ("quality", "p.quality_score IS NOT NULL"),
        ("dino_embedding", "p.dino_subject_embedding IS NOT NULL"),
        ("label_embedding", "EXISTS (SELECT 1 FROM photo_embeddings pe WHERE pe.photo_id = p.id)"),
        ("burst", "p.burst_id IS NOT NULL"),
        ("rating", "p.rating IS NOT NULL AND p.rating > 0"),
    ]

    def _stats_repository(self) -> StatsRepository:
        """Build the stats repository on this connection.

        It receives ``self._ws_id`` uncalled, and each workspace-scoped read
        resolves it before running any SQL, so building it never raises.
        """
        from repositories.stats import StatsRepository

        return StatsRepository(
            self.conn,
            self._ws_id,
            coverage_photo_columns=self._COVERAGE_PHOTO_COLUMNS,
            chunk_size=_SQLITE_PARAM_CHUNK_SIZE,
        )

    @property
    def stats(self) -> StatsRepository:
        """The dashboard and pipeline-plan counters: ``db.stats.stage_scope_ids(...)``, ...

        A domain accessor, not a cached attribute: every access builds a fresh
        repository through ``_stats_repository``. Accessing it never needs a
        workspace; the workspace-scoped reads resolve the workspace active
        when they run and raise ``RuntimeError`` when none is set, while
        ``stage_scope_ids`` and the readers taking an explicit workspace id
        (``classification_inventory_counts``, ``sampled_top1_medians``) work
        with none active. The counters that resolve config or a photo scope
        first live in ``services/stage_counts.py`` (``count_*``) and
        ``services/dashboard_stats.py`` (``get_coverage_stats``, ...).
        """
        return self._stats_repository()

    def _location_repository(self) -> LocationRepository:
        """Build the locations repository on this connection.

        Photos and keywords are catalog-wide; the few workspace-scoped
        methods resolve the active workspace through ``self._ws_id`` at the
        point the original code did, so building the repository never raises.
        """
        from repositories.locations import LocationRepository

        return LocationRepository(
            self.conn,
            self._ws_id,
            chunk_size=_SQLITE_PARAM_CHUNK_SIZE,
            photo_date_asc_order=PHOTO_DATE_ASC_ORDER,
        )

    @property
    def locations(self) -> LocationRepository:
        """The locations domain: ``db.locations.gps_discrepancy_rows(ids)`` and friends.

        A domain accessor, not a cached attribute: every access builds a fresh
        repository through ``_location_repository``, exactly as a forwarding
        wrapper called at that moment would. Accessing it never needs a
        workspace; the few scoped methods resolve the active one when called.
        Do not hold the returned repository across ``set_active_workspace``.
        """
        return self._location_repository()

    def update_photo_wildlife_excluded(
        self, photo_id: int, excluded: bool, verify_workspace: bool = True,
    ) -> None:
        """Set whether a photo is excluded from wildlife detection/classification (no commit)."""
        if verify_workspace:
            self._verify_photo_in_workspace(photo_id)
        self._photo_review_repository().set_wildlife_excluded(photo_id, excluded)

    def _photo_review_repository(self) -> PhotoReviewRepository:
        from repositories.photo_review import PhotoReviewRepository

        return PhotoReviewRepository(
            self.conn,
            self._active_workspace_id,
            chunk_size=_SQLITE_PARAM_CHUNK_SIZE,
        )

    @property
    def photo_review(self) -> PhotoReviewRepository:
        """Ratings and flags: ``db.photo_review.set_flag(photo_id, "flagged")`` and friends.

        A domain accessor, not a cached attribute: every access builds a fresh
        repository through ``_photo_review_repository``, exactly as a
        forwarding wrapper called at that moment would, so it carries the
        workspace active at the access (``None`` when unset; the
        ``verify_workspace=True`` writes and ``wildlife_excluded_states``
        raise ``RuntimeError`` then). Do not hold the returned repository
        across ``set_active_workspace``.

        The wildlife-exclusion toggle stays on ``Database`` as
        ``update_photo_wildlife_excluded`` because its workspace check is
        ``_verify_photo_in_workspace``, not the repository's.
        """
        return self._photo_review_repository()

    from repositories.photo_labels import VALID_COLOR_LABELS

    def _photo_label_repository(self) -> PhotoLabelRepository:
        """Build the color-label repository on this connection.

        It receives ``self._ws_id`` uncalled, and every repository method
        resolves it before doing anything else, so a call with no active
        workspace raises ``RuntimeError`` where the eager resolution here
        used to.
        """
        from repositories.photo_labels import PhotoLabelRepository

        return PhotoLabelRepository(
            self.conn,
            self._ws_id,
            chunk_size=_SQLITE_PARAM_CHUNK_SIZE,
        )

    @property
    def photo_labels(self) -> PhotoLabelRepository:
        """Workspace color labels: ``db.photo_labels.set(photo_id, "red")`` and friends.

        A domain accessor, not a cached attribute: every access builds a fresh
        repository through ``_photo_label_repository``. Accessing it never
        needs a workspace; each method reads the one active when it runs.
        Do not hold the returned repository across ``set_active_workspace``.
        """
        return self._photo_label_repository()

    @property
    def photo_versions(self) -> PhotoVersionsRepository:
        """Independent versions sharing the source photo and its metadata."""
        from repositories.photo_versions import PhotoVersionsRepository
        return PhotoVersionsRepository(self.conn)

    def _edits_repository(self) -> EditsRepository:
        """Build the edits repository on this connection.

        Recipes and presets are not workspace-scoped, so this never calls
        ``self._ws_id()``; the optional workspace check runs in the wrapper.
        """
        from repositories.edits import EditsRepository

        return EditsRepository(
            self.conn,
            chunk_size=_SQLITE_PARAM_CHUNK_SIZE,
            preset_name_max=self.EDIT_PRESET_NAME_MAX,
        )

    @property
    def edits(self) -> EditsRepository:
        """Edit recipes and presets: ``db.edits.get_photo_recipes(ids)``, ``db.edits.list_presets()``.

        A domain accessor, not a cached attribute: every access builds a fresh
        repository through ``_edits_repository``. The domain is catalog-wide,
        so it needs no active workspace.

        The single-photo recipe reads and writes stay on ``Database``
        (``get_photo_edit_recipe``, ``set_photo_edit_recipe``,
        ``clear_photo_edit_recipe``) because they run the optional
        active-workspace check first; call those, not the repository methods
        behind them.
        """
        return self._edits_repository()

    def get_photo_edit_recipe(
        self, photo_id: int, verify_workspace: bool = False
    ) -> dict[str, Any] | None:
        """Return the normalized edit recipe dict for a photo, or None."""
        if verify_workspace:
            self._verify_photo_in_workspace(photo_id)
        return self._edits_repository().get_photo_recipe(photo_id)

    def set_photo_edit_recipe(
        self,
        photo_id: int,
        recipe: Mapping[str, Any] | str | None,
        verify_workspace: bool = True,
    ) -> dict[str, Any] | None:
        """Set or clear a non-destructive edit recipe for a photo (no commit).

        Returns the normalized recipe dict, or None when the provided recipe is
        a no-op and the stored row was cleared.
        """
        if verify_workspace:
            self._verify_photo_in_workspace(photo_id)
        return self._edits_repository().set_photo_recipe(photo_id, recipe)

    def clear_photo_edit_recipe(self, photo_id: int, verify_workspace: bool = True) -> bool:
        """Remove a photo's edit recipe (no commit). Returns True if a row was removed."""
        if verify_workspace:
            self._verify_photo_in_workspace(photo_id)
        return self._edits_repository().clear_photo_recipe(photo_id)

    # --- edit presets (global reusable development settings) ----------------------

    EDIT_PRESET_NAME_MAX = 80

    # ------------------------------------------------------------------
    # caches: preview_cache LRU and offline originals (db.caches)
    # ------------------------------------------------------------------
    def _caches_repository(self) -> CachesRepository:
        """Build the preview/offline-original cache repository on this connection.

        Both caches are catalog-wide, so the repository takes no workspace id.
        ``execute_with_retry`` is read from this module at call time so tests
        that patch ``db.execute_with_retry`` still apply.
        """
        from repositories.caches import CachesRepository

        return CachesRepository(
            self.conn,
            execute_with_retry=execute_with_retry,
        )

    @property
    def caches(self) -> CachesRepository:
        """The preview-cache and offline-original domain: ``db.caches.preview_get(...)``.

        A domain accessor, not a cached attribute: every access builds a fresh
        repository through ``_caches_repository``, exactly as a forwarding
        wrapper called at that moment would, so the connection and the
        module's ``execute_with_retry`` (which tests patch) are resolved per
        use. Do not hold the returned repository
        across a patch of those helpers or a connection swap.
        """
        return self._caches_repository()

    # -- Scanner catalog pass, companion pairing, working copies --

    def _scanning_repository(self) -> ScanningRepository:
        """Build the scanner's catalog-pass repository on this connection.

        A scan catalogs photos globally, so it takes no workspace id.
        """
        from repositories.scanning import ScanningRepository

        return ScanningRepository(self.conn)

    @property
    def scanning(self) -> ScanningRepository:
        """The scanner's per-file and per-folder catalog SQL: ``db.scanning.update_photo(...)`` and friends.

        A domain accessor, not a cached attribute: every access builds a fresh
        repository through ``_scanning_repository``. Do not hold the returned
        repository across a connection swap.
        """
        return self._scanning_repository()

    def _companion_pairing_repository(self) -> CompanionPairingRepository:
        """Build the RAW+JPEG companion-pairing repository on this connection.

        Pairing is catalog-wide, so it takes no workspace id.
        """
        from repositories.companion_pairing import CompanionPairingRepository

        return CompanionPairingRepository(self.conn)

    @property
    def companion_pairing(self) -> CompanionPairingRepository:
        """RAW+JPEG pairing: ``db.companion_pairing.claim_companion(...)`` and friends.

        A domain accessor, not a cached attribute: every access builds a fresh
        repository through ``_companion_pairing_repository``. Do not hold the
        returned repository across a connection swap.
        """
        return self._companion_pairing_repository()

    def _working_copy_repository(self) -> WorkingCopyRepository:
        """Build the working-copy extraction repository on this connection.

        The ``working_copy_*`` columns are catalog-wide, so it takes no
        workspace id.
        """
        from repositories.working_copies import WorkingCopyRepository

        return WorkingCopyRepository(self.conn)

    @property
    def working_copies(self) -> WorkingCopyRepository:
        """The working-copy extraction pass: ``db.working_copies.set_working_copy(...)`` and friends.

        A domain accessor, not a cached attribute: every access builds a fresh
        repository through ``_working_copy_repository``. Do not hold the
        returned repository across a connection swap.
        """
        return self._working_copy_repository()

    def _masks_features_repository(self) -> MasksFeaturesRepository:
        """Build the masks/features repository on this connection.

        Mask, feature and embedding rows are catalog-wide. The repository
        receives ``self._ws_id`` uncalled: the workspace-scoped pipeline
        selectors resolve it (raising ``RuntimeError`` when no workspace is
        active) before running any SQL, and every other method never
        resolves it. The storage cleanup deletes remove files only inside
        the masks directory (``_masks_dir_real``), which is passed as a value.
        """
        from repositories.masks_features import MasksFeaturesRepository

        return MasksFeaturesRepository(
            self.conn,
            self._ws_id,
            masks_dir=self._masks_dir_real(),
        )

    @property
    def masks_features(self) -> MasksFeaturesRepository:
        """SAM masks, pipeline features and embeddings: ``db.masks_features.get_mask(...)``.

        A domain accessor, not a cached attribute: every access builds a fresh
        repository through ``_masks_features_repository``, exactly as a
        forwarding wrapper called at that moment would. Accessing it never
        needs a workspace; the workspace-scoped selectors
        (``variant_coverage``, ``workspace_photo_ids_with_variant``,
        ``workspace_mask_candidate_detections``, ``photos_with_embedding``)
        read the active workspace when they run and raise ``RuntimeError``
        before any SQL when none is set, while the photo- and variant-keyed
        reads and writes work in any workspace. Do not hold the returned
        repository across ``set_active_workspace``.

        Work that reads the workspace config or the photo scope first lives
        in ``services/subject_masks.py`` (``set_active_mask_variant``,
        ``delete_stale_masks``, ``sam_variant_rerun_warning``,
        ``get_photos_missing_masks``, ``list_photos_for_eye_keypoint_stage``)
        and ``services/detection_store.py`` (``upsert_photo_embedding``, behind
        its workspace guard).
        """
        return self._masks_features_repository()

    def _subjects_repository(self) -> SubjectsRepository:
        """Build the subject-analysis repository on this connection.

        ``detection_subjects``, ``photo_subject_state`` and
        ``photo_subject_choices`` are catalog-wide, so the repository takes
        no workspace id; the one workspace-dependent read receives it from
        its caller.
        """
        from repositories.subjects import SubjectsRepository

        return SubjectsRepository(self.conn)

    @property
    def subjects(self) -> SubjectsRepository:
        """Per-subject analysis and the primary subject: ``db.subjects.choice(pid)`` and friends.

        A domain accessor, not a cached attribute: every access builds a fresh
        repository through ``_subjects_repository``, exactly as a forwarding
        wrapper called at that moment would. The tables are catalog-wide, so
        the accessor needs no active workspace. The decisions (which detection
        is primary, whether the active mask still matches it) and the commit
        points stay in ``subjects``.
        """
        return self._subjects_repository()

    def _culling_repository(self) -> CullingRepository:
        """Build the (catalog-wide) culling-analysis repository on this connection."""
        from repositories.culling import CullingRepository

        return CullingRepository(self.conn, self._ws_id)

    @property
    def culling(self) -> CullingRepository:
        """The culling analysis's catalog reads: ``db.culling.top_prediction(pid)`` and friends.

        A domain accessor, not a cached attribute: every access builds a fresh
        repository through ``_culling_repository``, exactly as a forwarding
        wrapper called at that moment would. Its reads and the pHash write are
        keyed by photo id, so accessing it never needs a workspace. Do not
        hold the returned repository across a connection swap.
        """
        return self._culling_repository()

    def _masks_dir_real(self):
        """Realpath of the masks directory, used as the containment root
        for cleanup deletes. Returns None for ``:memory:`` databases or
        when the parent directory can't be resolved (in which case the
        caller refuses to delete files by stored path).
        """
        if self._db_path == ":memory:":
            return None
        parent = os.path.dirname(self._db_path)
        if not parent:
            return None
        return os.path.realpath(os.path.join(parent, "masks"))

    # -- Keywords --

    @property
    def keywords(self) -> KeywordRepository:
        """Keyword rows: ``db.keywords.name_of(keyword_id)``, ``db.keywords.ensure_default_genres()`` and friends.

        A domain accessor, not a cached attribute: every access builds a
        fresh repository, whose scoped methods resolve the workspace at use.
        Do not retain one across a workspace switch. Insert a keyword with
        ``db.keywords.add``.
        """
        return self._keyword_repository()

    def _keyword_repository(self) -> KeywordRepository:
        """Build the keyword repository on this connection.

        The active workspace is resolved lazily (``Database._ws_id`` is
        passed as a resolver), so scoped methods raise only when they run
        without one.
        """
        from repositories.keywords import KeywordRepository

        return KeywordRepository(self.conn, self._ws_id)

    def _keyword_taxon_repository(self) -> KeywordTaxonRepository:
        """Build the keyword species-name and taxon-link repository on this connection.

        Catalog-wide: it needs only the connection.
        """
        from repositories.keyword_taxa import KeywordTaxonRepository

        return KeywordTaxonRepository(self.conn)

    @property
    def keyword_taxa(self) -> KeywordTaxonRepository:
        """Species names and taxon links of keywords: ``db.keyword_taxa.mark_species(taxonomy)`` and friends.

        A domain accessor, not a cached attribute: every access builds a
        fresh repository through ``_keyword_taxon_repository``. Catalog-wide,
        so it needs no active workspace.
        """
        return self._keyword_taxon_repository()

    def _keyword_tree_repository(self) -> KeywordTreeRepository:
        """Build the keyword-tree repository on this connection.

        The active workspace is resolved lazily (``Database._ws_id`` is
        passed as a resolver).
        """
        from repositories.keyword_tree import KeywordTreeRepository

        return KeywordTreeRepository(self.conn, self._ws_id)

    @property
    def keyword_tree(self) -> KeywordTreeRepository:
        """The keyword hierarchy and its edits: ``db.keyword_tree.get_tree()``, ``db.keyword_tree.delete(id)`` and friends.

        A domain accessor, not a cached attribute: every access builds a
        fresh repository, whose scoped methods resolve the workspace at use.
        Do not retain one across a workspace switch.
        """
        return self._keyword_tree_repository()

    def _photo_keyword_repository(self) -> PhotoKeywordRepository:
        """Build the photo-keyword association reads on this connection.

        Catalog-wide: nothing in it needs the active workspace. ``_chunks``,
        ``KEYWORD_TYPES`` and the subject-filter chunk size are read here, at
        call time.
        """
        from repositories.photo_keywords import PhotoKeywordRepository

        return PhotoKeywordRepository(
            self.conn,
            chunks=_chunks,
            keyword_types=KEYWORD_TYPES,
            filter_subject_chunk=self._FILTER_SUBJECT_CHUNK,
        )

    @property
    def photo_keywords(self) -> PhotoKeywordRepository:
        """Which keywords photos carry: ``db.photo_keywords.get_for_photo(photo_id)`` and friends.

        A domain accessor, not a cached attribute: every access builds a
        fresh repository through ``_photo_keyword_repository``. The domain is
        catalog-wide, so it needs no active workspace. Callers outside the
        data layer tag and untag through ``services.keyword_tagging``.
        """
        return self._photo_keyword_repository()

    @property
    def identity_alternatives(self) -> IdentityAlternativesRepository:
        """Unresolved candidate identities; never confirmed species keywords."""
        from repositories.identity_alternatives import IdentityAlternativesRepository
        return IdentityAlternativesRepository(
            self.conn, self._ws_id,
            keyword_source_conflict_sql=KEYWORD_SOURCE_CONFLICT_SQL,
        )

    def _photo_species_repository(self) -> PhotoSpeciesRepository:
        """Build the photo-species reads on this connection.

        The active workspace is resolved lazily (``Database._ws_id`` is passed
        as a resolver), so ``get_accepted_species`` raises at the point it
        always did.
        """
        from repositories.photo_species import PhotoSpeciesRepository

        return PhotoSpeciesRepository(self.conn, self._ws_id)

    @property
    def photo_species(self) -> PhotoSpeciesRepository:
        """The species photos carry: ``db.photo_species.get_species_for_photos(ids)`` and friends.

        A domain accessor, not a cached attribute: every access builds a
        fresh repository, whose scoped method resolves the workspace at use.
        Do not retain one across a workspace switch.
        """
        return self._photo_species_repository()

    def _keyword_provenance_repository(self) -> KeywordProvenanceRepository:
        """Build the keyword-provenance repository on this connection.

        It holds the ``photo_keywords`` writers that create or converge an
        association (``tag``, ``merge_keyword_into``,
        ``link_keyword_to_place``) and the flows that call them
        mid-transaction (``upsert_one_keyword``, ``accept_prediction``). The
        active workspace is resolved lazily (``Database._ws_id`` is passed as
        a resolver).
        """
        from repositories.keyword_provenance import KeywordProvenanceRepository

        return KeywordProvenanceRepository(self.conn, self._ws_id)

    @property
    def keyword_provenance(self) -> KeywordProvenanceRepository:
        """The ``photo_keywords`` writers: ``db.keyword_provenance.merge_keyword_into(...)`` and friends.

        Code outside the data layer tags through ``services.keyword_tagging``
        and records an accept through ``services.review_decisions``; the
        provenance contract reads every call of the writer behind them.

        A domain accessor, not a cached attribute: every access builds a fresh
        repository through ``_keyword_provenance_repository``, which resolves
        the active workspace only when a method needs it. Do not hold the
        returned repository across ``set_active_workspace``.
        """
        return self._keyword_provenance_repository()

    @property
    def keyword_merges(self) -> KeywordMergeRepository:
        """The statements behind ``keyword_identity``: ``db.keyword_merges.alias_owner(key)`` and friends.

        Import-alias lookups and writes, the identity-group counts, and the
        reads and survivor writes of the explicit keyword and location
        merges. A domain accessor, not a cached attribute: every access
        builds a fresh repository on this connection. Catalog-wide, so it
        needs no active workspace; nothing in it commits.
        """
        from repositories.keyword_merges import KeywordMergeRepository

        return KeywordMergeRepository(self.conn)

    def merge_taxonomy_root_twins(self) -> int:
        """Fold each top-level species twin into the row filed in the tree.

        The pairs are ``KeywordTreeRepository.taxonomy_root_twins``. Both rows
        carry the same name, so the sidecars already hold the survivor's
        flat keyword and the merge queues no XMP change. All-or-nothing within
        the caller's unit of work (the startup repair takes it immediate);
        returns the number of rows merged away. Does not commit.
        """
        merged = 0
        for root_id, placed_id in self.keyword_tree.taxonomy_root_twins():
            merged += self.keyword_provenance.merge_keyword_into(
                root_id, placed_id, pending_source_only=True,
            )
        if merged:
            log.info("merged %d top-level species keyword twin(s)", merged)
        return merged

    def repair_misclassified_location_ancestors(self) -> int:
        """Restore every location-tree node damaged by legacy taxonomy marking.

        This is intentionally idempotent and narrowly structural: only a
        taxonomy chain between location nodes is changed. Each pass restores
        the next node from the location parent downward. Genuine taxonomy
        keywords and cross-type name collisions keep the existing conflict
        protection. A final pass collapses same-name location roots that a
        prior retype could have left alongside a pre-existing coordless
        root — SQLite's ``UNIQUE(name, parent_id)`` index does not enforce
        ``parent_id IS NULL``, so without this later child-place upserts
        would settle on one root and orphan the other's descendants.
        """
        repaired = self._location_repository().repair_misclassified_ancestors()
        merged_roots = self.keyword_provenance.merge_duplicate_location_roots()
        if repaired:
            log.info(
                "repaired %d misclassified location ancestor keyword(s)",
                repaired,
            )
        if merged_roots:
            log.info(
                "merged %d duplicate location root keyword(s)",
                merged_roots,
            )
        return repaired

    def upsert_place_chain(self, details: Mapping[str, Any]) -> int:
        """Upsert a Google Place + its parent chain. Returns the leaf id.

        ``details`` is the normalized dict produced by
        :func:`vireo.places.place_details`: ``place_id``, ``name``, ``lat``,
        ``lng``, ``address_components``.

        Useful administrative/geographic ``address_component`` entries become
        parent ``type='location'`` keywords chained via ``parent_id``. Street
        numbers, routes, postal codes, and other address fragments are not
        keyword parents. Per Task 4's finding, Google's standard responses do
        NOT carry a per-component ``place_id``, so parents dedupe on
        ``(name, parent_id)`` and only the leaf carries ``place_id``/coords.

        Idempotent: calling twice with the same ``details`` returns the same
        leaf id and does not create duplicate rows. Does not commit; a caller
        that survives a failure here wraps the call in its own (nested)
        ``with db.transaction():`` so the partial chain rolls back.
        """
        if not details.get("place_id"):
            raise ValueError("upsert_place_chain requires details['place_id']")

        name = details.get("name", "")
        lat = details.get("lat")
        lng = details.get("lng")
        components = details.get("address_components") or []

        existing_leaf = self._location_repository().find_place_keyword(details["place_id"])
        chain = self.keyword_provenance.upsert_location_parent_chain(
            components,
            leaf_name=name,
            leaf_types=details.get("types"),
            exclude_keyword_id=(
                existing_leaf["id"] if existing_leaf is not None else None
            ),
        )
        parent_id = chain[-1] if chain else None
        return self.keyword_provenance.upsert_one_keyword(
            name=name,
            parent_id=parent_id,
            place_id=details["place_id"],
            latitude=lat,
            longitude=lng,
            reuse_location_component=(
                location_component_rank({
                    "types": details.get("types"),
                }) is not None
            ),
        )

    def set_photo_location(self, photo_id: int, leaf_keyword_id: int) -> None:
        """Set ``photo_id``'s location to ``leaf_keyword_id``.

        Removes any existing ``type='location'`` keyword links for the photo,
        then inserts the new link. Does not commit: the caller's unit of
        work keeps the removal and the new link together.

        Raises ``ValueError`` if ``leaf_keyword_id`` does not exist or its
        keyword type is not ``'location'`` — otherwise the DELETE would strip
        real location links and replace them with a non-location link that
        :meth:`locations.clear_photo` could not clean up.
        """
        repo = self._location_repository()
        repo.require_location_keyword(leaf_keyword_id)
        repo.delete_photo_links(photo_id)
        # Every caller of this method is a person assigning a place
        # (map click, text entry, batch apply, EXIF-derived confirm), so
        # the association is user-authored.
        self.keyword_provenance.tag(
            photo_id, leaf_keyword_id,
            source=KEYWORD_SOURCE_MANUAL,
        )

    def get_or_create_text_location(self, name: str | None) -> int:
        """Find or create a free-text ``type='location'`` keyword.

        No ``place_id``, no coords, no parent. Whitespace is stripped from
        ``name``; raises ``ValueError`` if the stripped result is empty or
        contains ``|`` (Lightroom's hierarchy delimiter -- a pipe in a
        location name has no reversible XMP encoding, so it is rejected here
        rather than downstream in ``SidecarEditor.set_location_keywords``,
        where a silent skip would let ``sync_to_xmp`` clear the pending
        change without ever writing the keyword).
        Returns the keyword id.
        """
        if name is None:
            raise ValueError("location name must not be empty")
        stripped = name.strip()
        if not stripped:
            raise ValueError("location name must not be empty")
        if "|" in stripped:
            raise ValueError(
                "location name may not contain '|' -- XMP keyword "
                "hierarchies reserve it as the level delimiter"
            )
        return self.keyword_provenance.upsert_one_keyword(
            name=stripped,
            parent_id=None,
            place_id=None,
            latitude=None,
            longitude=None,
        )

    def _meta_repository(self) -> MetaRepository:
        """Build the (catalog-wide) db_meta repository on this connection."""
        from repositories.meta import MetaRepository

        return MetaRepository(self.conn)

    @property
    def meta(self) -> MetaRepository:
        """The ``db_meta`` markers: ``db.meta.get(key)`` and ``db.meta.set(key, value)``.

        A domain accessor, not a cached attribute: every access builds a fresh
        repository through ``_meta_repository``. ``db_meta`` is catalog-wide,
        so nothing in it needs a workspace.
        """
        return self._meta_repository()

    def _local_workspace_repository(self) -> LocalWorkspaceRepository:
        """Build local-copy persistence on the live catalog connection."""
        from repositories.local_workspaces import LocalWorkspaceRepository

        return LocalWorkspaceRepository(self.conn)

    @property
    def local_workspaces(self) -> LocalWorkspaceRepository:
        """Fresh API for workspace-local copies; ids are explicit per call."""
        return self._local_workspace_repository()

    def _local_folder_repository(self) -> LocalFolderRepository:
        """Build the (catalog-wide) folder-level local-copy repository on this connection."""
        from repositories.local_folders import LocalFolderRepository

        return LocalFolderRepository(self.conn)

    @property
    def local_folders(self) -> LocalFolderRepository:
        """The folder-level local-copy domain: ``db.local_folders.state_rows(ids)``.

        A domain accessor, not a cached attribute: every access builds a fresh
        repository through ``_local_folder_repository``, exactly as a
        forwarding wrapper called at that moment would. The domain is
        catalog-wide, so it needs no active workspace.
        """
        return self._local_folder_repository()

    def _exif_search_repository(self) -> ExifSearchRepository:
        """Build the (catalog-wide) EXIF search text backfill on this connection."""
        from repositories.exif_search import ExifSearchRepository

        return ExifSearchRepository(self.conn)

    @property
    def exif_search(self) -> ExifSearchRepository:
        """The EXIF search-text backfill: ``db.exif_search.index_batch(after_id, limit)``.

        A domain accessor, not a cached attribute: every access builds a fresh
        repository through ``_exif_search_repository``, so the connection is
        read per use. The domain is catalog-wide, so it needs no active
        workspace.
        """
        return self._exif_search_repository()

    def _photo_metadata_repository(self) -> PhotoMetadataRepository:
        """Build the (catalog-wide) photo camera-metadata reads on this connection."""
        from repositories.photo_metadata import PhotoMetadataRepository

        return PhotoMetadataRepository(self.conn)

    @property
    def photo_metadata(self) -> PhotoMetadataRepository:
        """Export's camera-metadata reads: ``db.photo_metadata.exif_data_by_ids(ids)``.

        A domain accessor, not a cached attribute: every access builds a fresh
        repository through ``_photo_metadata_repository``, so the connection
        is resolved per use. The reads are catalog-wide, so unlike
        ``db.photos`` it never needs an active workspace.
        """
        return self._photo_metadata_repository()

    @property
    def species_provenance(self) -> SpeciesProvenanceRepository:
        """Per-species assignment evidence, independent of photographer flags."""
        from repositories.species_provenance import SpeciesProvenanceRepository
        return SpeciesProvenanceRepository(self.conn, self._species_provenance_before)

    def get_life_list_unmatched_species(self) -> list[str]:
        """Backward-compatible list of uncounted identification names."""
        return [
            row["name"]
            for row in self.species_curation.get_life_list_uncounted_identifications()
        ]

    def get_species_representatives(self, eligible_only: bool = False) -> dict[str, int]:
        """Return {species: main_photo_id} for compatibility callers."""
        return {
            species: photo_ids[0]
            for species, photo_ids in self.species_curation.get_representative_lists(
                eligible_only=eligible_only
            ).items()
            if photo_ids
        }

    def set_species_representative(self, species: str, photo_id: int) -> None:
        """Mark a photo as a representative for a species globally.

        Multiple photos can represent a species. Re-selecting an existing
        representative promotes it by assigning the newest selection order.
        A compatibility ``photo_preferences`` row is kept for older callers
        that still expect one main representative in the active workspace.
        """
        self.species_curation.set_photo_preference(
            "species_representative", species, photo_id
        )

    # Reads the Highlights relabel snapshots before it moves any curation.
    # None of them commits.

    def _refuse_cross_type_rename(self, new_name, parent_id, keyword_id):
        """Raise ValueError when a different-type keyword holds the target slot."""
        # No same-type peer, but a DIFFERENT-type peer at the
        # same (name, parent_id) would hit the table-level
        # UNIQUE(name, parent_id) constraint at UPDATE time for
        # a non-NULL parent and surface as an uncaught
        # IntegrityError/500. Detect it here and raise
        # ValueError so api_update_keyword returns a documented
        # 400. For NULL parents a cross-type peer is allowed to
        # coexist (mirrors add_keyword).
        if parent_id is not None:
            cross = self._keyword_tree_repository().cross_type_peer(
                new_name, parent_id, keyword_id,
            )
            if cross is not None:
                raise ValueError(
                    f"cannot rename to {new_name!r}: a "
                    f"{cross['type']!r} keyword with that name "
                    f"already exists under this parent"
                )

    def update_keyword(self, keyword_id: int, **kwargs: Any) -> int:
        """Update keyword fields. Supports: type, taxon_id, latitude, longitude, name.

        On a name change, re-runs the same taxonomy auto-detection that
        add_keyword does on insert: if the keyword's current type is
        'general' and the new name matches a taxon, it's promoted to
        type='taxonomy' with the matching taxon_id. If the current type is
        already 'taxonomy' and the new name matches a different taxon,
        taxon_id is updated. Manually-set non-'general' types (e.g.
        'location', 'individual') are preserved. Explicit type/taxon_id
        kwargs always win over auto-detection, and an explicit type change
        reconciles the legacy ``is_species`` flag with the requested type.
        """
        _check_keyword_type_kwarg(kwargs)
        allowed = {'type', 'taxon_id', 'latitude', 'longitude', 'name'}
        updates = {k: v for k, v in kwargs.items() if k in allowed}
        if not updates:
            return keyword_id

        # Normalize the rename target with the same rules add_keyword
        # applies on insert, so PUT /api/keywords/<id> can't sneak stray
        # edge quotes or an empty-after-normalization string into a row
        # that add_keyword would have rejected/deduped.
        if 'name' in updates:
            updates['name'] = normalize_keyword_display(updates['name'])
            if not updates['name']:
                raise ValueError("keyword name is empty after normalization")

        # On a rename or retype, resolve the effective (name, type) and, if
        # they diverge from the stored row, look for a same-slot peer to
        # merge into instead of writing a duplicate. The type-only case
        # matters too: the Keywords type dropdown sends `{type: newType}`
        # with no `name`, and retyping `general apapane` to `taxonomy`
        # while a taxonomy `apapane` peer exists at the same (NULL) parent
        # would otherwise leave two same-name taxonomy rows — NULL parents
        # bypass UNIQUE(name, parent_id).
        if 'name' in updates or 'type' in updates:
            current = self._keyword_tree_repository().get_update_target(keyword_id)
            if current is not None:
                parent_id = current["parent_id"]
                cur_type = current["type"]
                new_name = updates.get('name', current['name'])
                name_changed = new_name != current["name"]
                # Resolve taxon match lazily: only a rename needs it (both
                # for auto-promotion below and for the effective type when
                # no explicit type is passed). Prefer species rank: the
                # auto-promotion below stamps is_species=1 on a taxonomy
                # match, so binding to a genus/family homonym would then
                # get filtered out by the rank='species' Life List /
                # Compare queries.
                taxon_id = (
                    self.keyword_taxa.lookup_taxon_id(
                        new_name, prefer_species=True,
                    )
                    if name_changed else None
                )
                # Peer lookup must use the EFFECTIVE type (see
                # ``_keyword_update_effective_type``).
                effective_type, type_changed = _keyword_update_effective_type(
                    updates, cur_type, taxon_id,
                )
                if name_changed or type_changed:
                    # Merge into a same-slot same-type peer instead of
                    # writing a duplicate. Without this, top-level renames
                    # slip past UNIQUE(name, parent_id) — SQLite treats NULL
                    # parents as distinct — silently producing two peer rows;
                    # child renames raise IntegrityError from the UPDATE
                    # below and surface as a 500. Restrict to same-type
                    # peers: the dedup boundary elsewhere in this file is
                    # (name, parent_id, type), so a rename onto a
                    # different-type peer must NOT silently retag photos
                    # across types.
                    peer = self._keyword_tree_repository().same_type_peer(
                        new_name, parent_id, effective_type, keyword_id,
                    )
                    if peer:
                        # Return the surviving id so callers
                        # (api_update_keyword) can retarget sidecar and
                        # preferences bookkeeping onto the surviving row.
                        # Source-only: a photo that already removed the
                        # old spelling must keep that queued removal even
                        # when it also carries the peer.
                        self.keyword_provenance.merge_keyword_into(
                            keyword_id, peer["id"], pending_source_only=True,
                        )
                        return peer["id"]
                    self._refuse_cross_type_rename(new_name, parent_id, keyword_id)

                # Auto-retype on rename: same logic as add_keyword. Only
                # fires on an actual name change so idempotent PUT-style
                # updates (client re-sending the existing name) don't
                # unexpectedly reclassify a 'general' keyword once the taxa
                # table is populated.
                if name_changed:
                    _apply_rename_retype(updates, cur_type, taxon_id)

        self._keyword_tree_repository().apply_update(keyword_id, updates)
        return keyword_id

    # -- Predictions --

    def _prediction_repository(self) -> PredictionRepository:
        """Build the prediction repository on this connection.

        The active workspace is resolved lazily (``Database._ws_id`` is
        passed as a resolver): several methods only consult it on some
        paths (``add_prediction`` only when it writes review state), so
        they keep raising at exactly the point they always did.
        """
        from repositories.predictions import PredictionRepository

        return PredictionRepository(self.conn, self._ws_id)

    @property
    def predictions(self) -> PredictionRepository:
        """Classifier predictions: ``db.predictions.id_for_key(...)`` and friends.

        A domain accessor, not a cached attribute: every access builds a fresh
        repository through ``_prediction_repository``, so the connection and
        the lazily resolved workspace are read per use. Routes record review
        decisions through ``services.review_decisions``, never through this
        repository's writers (``test_route_contract`` holds them to it).
        """
        return self._prediction_repository()


    # The one definition of a settled review lives in
    # ``repositories.prediction_rules``; callers read it here too.
    DECIDED_PREDICTION_STATUSES = _DECIDED_PREDICTION_STATUSES

    # Reads the prediction-decision routes run under ``BEGIN IMMEDIATE``
    # (see ``services.prediction_decisions``). None of them commits.

    # -- Detections --

    def _model_runs_repository(self) -> ModelRunsRepository:
        """Build the model-runs repository on this connection.

        Every model-run table is catalog-wide, so the repository takes no
        workspace id; the classify preflight wrappers resolve the active
        workspace's detector floor and pass it in.
        """
        from repositories.model_runs import ModelRunsRepository

        return ModelRunsRepository(
            self.conn,
            auto_match_review_marker=AUTO_MATCH_REVIEW_MARKER,
        )

    @property
    def model_runs(self) -> ModelRunsRepository:
        """Detector and classifier run records: ``db.model_runs.record_detector_run(...)``.

        A domain accessor, not a cached attribute: every access builds a fresh
        repository through ``_model_runs_repository``, exactly as a forwarding
        wrapper called at that moment would, so the connection is resolved
        per use. Every model-run table is catalog-wide, so no method needs an
        active workspace.

        The detection writes receive ``detector_run_is_pinned`` from here as
        their ``is_pinned`` callback, so a patch of
        ``ModelRunsRepository.detector_run_is_pinned`` reaches them. The
        classify preflight reads that resolve the workspace's detector floor
        live in ``services/classify_preflight.py``.
        """
        return self._model_runs_repository()

    def _computation_cache_repository(self) -> ComputationCacheRepository:
        """Build the (catalog-wide) computation-cache repository on this connection."""
        from repositories.computation_cache import ComputationCacheRepository

        return ComputationCacheRepository(self.conn)

    @property
    def computation_cache(self) -> ComputationCacheRepository:
        """The portable computation cache's catalog SQL: ``db.computation_cache.run_predictions(...)``.

        A domain accessor, not a cached attribute: every access builds a fresh
        repository through ``_computation_cache_repository``, exactly as a
        forwarding wrapper called at that moment would. Every table it reads
        or writes is catalog-wide, so no method needs an active workspace, and
        none commits: ``computation_cache`` commits or rolls back through
        ``db.commit()`` / ``db.rollback()``.
        """
        return self._computation_cache_repository()

    def _detections_repository(self) -> DetectionsRepository:
        """Build the detections repository on this connection.

        ``detections`` is catalog-wide, so the repository takes no workspace
        id; ``services/miss_flags.py`` resolves the workspace and the scope
        clause and passes them in, and ``services/detection_store.py``
        resolves the readers' confidence floors.
        """
        from repositories.detections import DetectionsRepository

        return DetectionsRepository(
            self.conn,
            chunk_size=_SQLITE_PARAM_CHUNK_SIZE,
        )

    @property
    def detections(self) -> DetectionsRepository:
        """Detector output: ``db.detections.get_ids_for_photos(ids)``, ``db.detections.clear(pid)`` and friends.

        A domain accessor, not a cached attribute: every access builds a fresh
        repository through ``_detections_repository``, so the connection is
        read per use. The table is
        catalog-wide, so the accessor needs no active workspace.

        Work that adds a config lookup or a second step lives in the
        services: ``detection_store.write_detection_batch`` (the pinned-run
        lookup), ``get_detections``, ``get_detections_for_photos`` and
        ``get_predictions_for_detection`` (the effective confidence floors)
        and ``delete_detections_by_ids`` (the primary-subject re-sync); the
        misses functions in ``miss_flags`` (workspace and filter scope). Call
        those, not the repository methods behind them.
        """
        return self._detections_repository()

    def _pipeline_features_repository(self) -> PipelineFeaturesRepository:
        """Build the pipeline feature-load repository on this connection.

        Its workspace-scoped reads take the workspace id from the caller,
        which resolves it before reading config, so the repository takes no
        workspace callable.
        """
        from repositories.pipeline_features import PipelineFeaturesRepository

        return PipelineFeaturesRepository(self.conn)

    @property
    def pipeline_features(self) -> PipelineFeaturesRepository:
        """Grouping-pipeline reads: ``db.pipeline_features.feature_rows(ws, scoped=False)`` and friends.

        A domain accessor, not a cached attribute: every access builds a fresh
        repository through ``_pipeline_features_repository``, exactly as a
        forwarding wrapper called at that moment would. Accessing it needs no
        active workspace; the workspace-scoped reads take the workspace id as
        an argument. The confidence floors, scopes and decisions stay in
        ``pipeline``.
        """
        return self._pipeline_features_repository()

    # -- Pending Changes --

    def _sync_repository(self) -> SyncRepository:
        """Build the pending-changes repository on this connection.

        The repository gets ``self._ws_id`` as a resolver rather than an id,
        so the active workspace is read exactly where these methods always
        read it: a staged-scope read with no matching rows, or a claim whose
        workspace lookup fails inside ``with conn:``, behaves as before.
        Methods that accept ``workspace_id`` fall back to it the same way.
        """
        from repositories.sync import SyncRepository

        return SyncRepository(
            self.conn, self._ws_id, chunk_size=_SQLITE_PARAM_CHUNK_SIZE,
        )

    @property
    def pending_changes(self) -> SyncRepository:
        """The pending XMP sync queue: ``db.pending_changes.list_all()`` and friends.

        A domain accessor, not a cached attribute: every access builds a fresh
        repository through ``_sync_repository``, exactly as a forwarding
        wrapper called at that moment would. The repository holds
        ``self._ws_id`` as a resolver, so the active workspace is still read
        lazily at the point each method reads it (and raises there when none
        is set). Do not hold the returned repository across
        ``set_active_workspace``.

        Queue a change with ``queue`` and cancel one with ``cancel`` (which
        queues the inverse of a keyword intent that may already have reached
        a sidecar). ``queue_flag_change_if_enabled`` (the undo replay's flag
        setter) stays on ``Database``. The claim, the token removal and the
        clears live in ``services/pending_changes.py``.
        """
        return self._sync_repository()

    def queue_flag_change_if_enabled(
        self, photo_id: int, flag: str | None, workspace_id: int | None = None
    ) -> str | None:
        """Queue a flag write when the active config opts into XMP flag sync."""
        ws_id = workspace_id if workspace_id is not None else self._ws_id()
        flag = flag or "none"
        self.pending_changes.cancel(photo_id, "flag", workspace_id=ws_id)
        if flag not in {"none", "flagged", "rejected"}:
            log.warning("Not queueing invalid XMP flag value for photo %s: %r", photo_id, flag)
            return None
        try:
            enabled = bool(self.effective_config().get("sync_flags_to_xmp", False))
        except Exception:
            log.warning("Failed to read sync_flags_to_xmp config", exc_info=True)
            enabled = False
        if not enabled:
            return None

        token = self.pending_changes.queue(photo_id, "flag", flag, workspace_id=ws_id)
        return token

    # -- Edit History --

    def _edit_history_repository(self) -> EditHistoryRepository:
        """Build the edit-history repository on this connection.

        It receives ``self._ws_id`` uncalled: the methods that act on the
        active workspace's history resolve it (raising ``RuntimeError`` when
        none is set) before running any SQL, while the id-keyed helpers never
        resolve it. ``_NON_UNDOABLE`` is read here, at build time, so the
        undo/redo cursor reads skip the same action types the façade does,
        and the relabel-curation restores receive
        ``species_curation.restore_representative`` as their callback.
        """
        from repositories.edit_history import EditHistoryRepository

        return EditHistoryRepository(
            self.conn,
            self._ws_id,
            non_undoable=self._NON_UNDOABLE,
            restore_species_representative=self.species_curation.restore_representative,
        )

    @property
    def edit_history(self) -> EditHistoryRepository:
        """The undo/redo edit history: ``db.edit_history.list_recent()`` and friends.

        A domain accessor, not a cached attribute: every access builds a fresh
        repository through ``_edit_history_repository``, exactly as a
        forwarding wrapper called at that moment would. Accessing it never
        needs a workspace; the history listings and the undo/redo cursor
        reads (``list_recent``, ``recipe_history_for_photo``,
        ``latest_undoable``, ``count_undoable``, ``oldest_redoable``) read
        the active workspace when they run and raise ``RuntimeError`` before
        any SQL when none is set, while the id-keyed reads
        (``action_and_new_value``, ``item_photo_ids``, ``has_changed_items``,
        ``keyword_name``) work in any workspace. Do not hold the returned
        repository across ``set_active_workspace``.

        Recording an edit (``record_edit``, which prunes after a committed
        record), undo and redo, and their replay handlers stay on
        ``Database``; the prediction-review replay receives this repository's
        ``prediction_scope`` as a callback.
        """
        return self._edit_history_repository()

    def record_edit(
        self,
        action_type: str,
        description: str,
        new_value: str | None,
        items: Iterable[Mapping[str, Any]],
        is_batch: bool = False,
        *,
        source: str | None = None,
        reason: str | None = None,
    ) -> int:
        """Record an edit action with per-photo before/after values.

        Clears the redo stack (any undone entries) since a new action
        invalidates them. Never commits: the caller's unit of work owns the
        edit, and once that unit commits the history is pruned to its
        configured bound in a unit of its own (``prune_edit_history``), once
        per unit however many edits it recorded. A write outside any unit,
        or inside a transaction the unit joined but does not own, schedules
        no prune; its owner prunes after its own commit.

        Args:
            source, reason: the edit's provenance when the caller stated it
                     (who decided, and why); None for the user's own clicks.
        """
        items = self.species_provenance.history_items(action_type, items)
        edit_id = self._edit_history_repository().record(
            action_type, description, new_value, items,
            is_batch=is_batch, source=source, reason=reason,
        )
        if self._unit.depth and not self._unit.joined:
            self.after_commit(self.prune_edit_history, own_unit=True, immediate=True, once=True)
        return edit_id

    # Action types that appear in history but cannot be reversed
    _NON_UNDOABLE = (
        'audit_remove_orphans', 'audit_resolve', 'keyword_clean',
        'identity_alternatives_superseded',
        'prediction_reject', 'discard',
        # Legacy rows lack the before-state required for a lossless replay.
        # New assignments use photo_location; new reject/review decisions
        # use prediction_status. Linking/merging vocabulary is still audited.
        'location_set', 'location_clear', 'location_link', 'location_gps_review',
        # Legacy reviewed rows omit prediction IDs; replace-species rows
        # still omit the removed keyword IDs and migrated curation state.
        'prediction_reviewed', 'prediction_replace_species',
    )



    # ------------------------------------------------------------------
    # Undo / redo
    #
    # ``edit_history`` rows carry an ``action_type``; each ``edit_history_items``
    # row holds the per-photo ``old_value`` / ``new_value``. Undo restores the
    # old value, redo re-applies the new one. Plain per-photo fields share a
    # setter and only differ in which value is used; keyword and prediction
    # actions have asymmetric handlers because they also reverse the
    # pending-sidecar queue and prediction review statuses.
    # ------------------------------------------------------------------

    def _apply_undo(self, entry, items):
        """Reverse the effects of an edit entry."""
        self._apply_edit_items(entry, items, undo=True)

    def _apply_redo(self, entry, items):
        """Re-apply the effects of an undone edit entry."""
        self._apply_edit_items(entry, items, undo=False)

    def _apply_edit_items(self, entry, items, *, undo):
        action = entry['action_type']
        setter = self._EDIT_FIELD_SETTERS.get(action)
        handlers = self._UNDO_HANDLERS if undo else self._REDO_HANDLERS
        handler = handlers.get(action)
        value_key = 'old_value' if undo else 'new_value'
        if setter is None and handler is None:
            raise ValueError(f'Unsupported history action: {action}')
        if action == 'flag':
            for item in items:
                self._edit_set_flag(item['photo_id'], item[value_key])
                self.photo_review.restore_flag_attribution_history(
                    entry['id'], item['photo_id'], undo=undo,
                )
            return
        for item in items:
            if setter is not None:
                setter(self, item['photo_id'], item[value_key])
            elif handler is not None:
                handler(self, entry, item)
        self.species_provenance.replay_history(items, undo=undo)

    # -- plain per-photo fields (same setter for undo and redo) ----------

    def _edit_set_species_provenance(self, pid, value):
        snapshot = json.loads(value)
        self.species_provenance.restore(pid, snapshot['keyword_id'], snapshot['state'])

    def _edit_set_flag(self, pid, value):
        # Edit history is already workspace-scoped; skip re-verification
        self.photo_review.set_flag(pid, value, verify_workspace=False)
        self.queue_flag_change_if_enabled(pid, value)

    def _edit_set_wildlife_excluded(self, pid, value):
        self.update_photo_wildlife_excluded(
            pid, value == "1", verify_workspace=False
        )

    def _edit_set_color_label(self, pid, value):
        if value:
            self.photo_labels.set(pid, value)
        else:
            self.photo_labels.remove(pid)

    def _edit_set_edit_recipe(self, pid, value):
        self.set_photo_edit_recipe(
            pid, value if value else None, verify_workspace=False,
        )

    def _edit_set_photo_version(self, pid, value):
        state = json.loads(value)
        if state["value"] is None:
            self.photo_versions.delete(pid, state["id"])
        else:
            self.photo_versions.put(pid, state["value"])
        self.photo_versions.queue_sync(pid, self.require_workspace_id())

    def restore_photo_location_state(self, pid: int, value: str) -> None:
        """Put back the place links and GPS review ``photo_location_state`` captured.

        The ``photo_location`` replay; ``services.edit_history`` queues the
        sidecar sync after it (the data layer does not import services).
        """
        state = json.loads(value)
        self.locations.clear_photo(pid)
        for link in state['links']:
            # Deleted vocabulary stays deleted, like ordinary keyword undo.
            row = self.locations.keyword_place_row(link['keyword_id'])
            if row is not None and row['type'] == 'location':
                self.keyword_provenance.tag(
                    pid, link['keyword_id'],
                    source=keyword_source_max(link['source']),
                )
        self.locations.delete_gps_review(pid)
        if state.get('gps_review') is not None:
            self.locations.save_gps_reviews([(pid, state['gps_review'])])

    def _edit_set_prediction_status(self, pid, value):
        recorded = json.loads(value)
        current = json.loads(self.edit_history.prediction_status_state(
            [int(pred_id) for pred_id in recorded],
        ))
        for pred_id, state in recorded.items():
            # Classification can reuse deleted IDs; only the instance token
            # proves this is the original row. Metadata may be enriched in place.
            # Earlier snapshots without an instance token stay inert.
            if not isinstance(state, dict) or not state.get('identity'):
                continue
            if not state['identity'].get('prediction_instance_id'):
                continue
            current_identity = current.get(pred_id, {}).get('identity', {})
            if current_identity.get('photo_id') != pid:
                continue
            if current_identity.get('prediction_instance_id') == state['identity']['prediction_instance_id']:
                self.predictions.update_status(int(pred_id), state['status'])

    # -- rating -----------------------------------------------------------

    def _undo_rating(self, entry, item):
        pid, old_val = item['photo_id'], item['old_value']
        self.photo_review.set_rating(pid, int(old_val), verify_workspace=False)
        if old_val != entry['new_value']:
            self.pending_changes.cancel(pid, 'rating', entry['new_value'])
            self.pending_changes.queue(pid, 'rating', old_val)

    def _redo_rating(self, entry, item):
        pid, old_val, new_val = item['photo_id'], item['old_value'], item['new_value']
        self.photo_review.set_rating(
            pid, int(new_val) if new_val else 0, verify_workspace=False,
        )
        if old_val != new_val:
            self.pending_changes.cancel(pid, 'rating', old_val)
            self.pending_changes.queue(pid, 'rating', new_val)

    # -- keyword helpers shared by the keyword / species handlers ----------

    def _flip_pending_keyword_change(self, pid, name, cancel_type, queue_type):
        """Reverse one side of the pending-sidecar queue for a keyword.

        Symmetric with ``queue_keyword_remove``: the original edit either
        queued a change of ``cancel_type`` or, when a not-yet-synced change
        of ``queue_type`` was pending, cancelled that one instead. Reversing
        must restore whichever side the edit touched -- otherwise an
        add -> remove -> undo flow leaves the tag on the photo with no
        pending sidecar write, and the restored keyword never syncs.
        """
        if self.pending_changes.cancel(pid, cancel_type, name) == 0:
            self.pending_changes.queue(pid, queue_type, name)

    def _retag_for_edit(self, pid, keyword_id):
        """Re-add a keyword removed by an edit; returns the keyword name."""
        name = self._edit_history_repository().keyword_name(keyword_id)
        if name is None:
            # Deleting a keyword deliberately retires it. Old history must
            # not recreate it or strand the history cursor on a foreign key.
            return None
        # Stamp the recreated association so durable authorship does not
        # disappear merely because untagging deleted the row.
        self.keyword_provenance.tag(pid, keyword_id, source=KEYWORD_SOURCE_MANUAL)
        if name:
            self._flip_pending_keyword_change(
                pid, name, 'keyword_remove', 'keyword_add',
            )
        return name

    def _untag_for_edit(self, pid, keyword_id):
        """Remove a keyword added by an edit; returns the keyword name."""
        self.photo_keywords.untag(pid, keyword_id)
        name = self._edit_history_repository().keyword_name(keyword_id)
        if name:
            self._flip_pending_keyword_change(
                pid, name, 'keyword_add', 'keyword_remove',
            )
        return name

    # -- keyword_remove ---------------------------------------------------

    def _undo_keyword_remove(self, entry, item):
        # Undo is an explicit request to restore the removed tag.
        self._retag_for_edit(item['photo_id'], int(entry['new_value']))

    def _redo_keyword_remove(self, entry, item):
        # Mirror the undo path: if undo re-queued a `keyword_add`, redo
        # cancels it rather than stacking a conflicting `keyword_remove`.
        self._untag_for_edit(item['photo_id'], int(entry['new_value']))

    # -- keyword_add / prediction_accept ----------------------------------
    #
    # ``no_tag`` marks a prediction_accept where the photo already carried
    # an equivalent species, so no tag was actually added. Undo must not
    # untag a keyword the user deliberately kept, and redo must not re-tag
    # or re-queue a keyword that was never touched -- only the prediction
    # status flip is reversed / re-applied.
    # A prediction batch can accept different keyword aliases of one species.
    # Its items carry the actual keyword IDs; the parent ID is only a default
    # for old entries without an item value.
    #
    # Predicted-only relabels (no prior species tag) record their action as
    # `keyword_add` but still carry a `curation` payload when the photo held
    # highlight/representative rows under other species. Those rows are
    # restored / re-applied here too, mirroring the `species_replace` path.

    def _undo_keyword_add(self, entry, item):
        pid, old_val = item['photo_id'], item['old_value']
        action = entry['action_type']
        old_meta = self._edit_old_value_meta(old_val)
        skip_tag = old_meta.get('no_tag', False)
        raw_kid = (item['new_value'] if action == 'prediction_accept' else None) or entry['new_value']
        # A no-tag accept (a species-less burst pick) records no keyword.
        kid = int(raw_kid) if raw_kid or not skip_tag else None
        kw_name = self._edit_history_repository().keyword_name(kid) if kid is not None else None
        if not skip_tag:
            if action == 'prediction_accept' and (
                not old_val or old_meta.get('keyword_only') or old_meta.get('symmetric_keyword_queue')
            ):
                # Accept on all may have cancelled a pending
                # removal (or already synced), so undo must restore a removal
                # when there is no pending add left to cancel.
                self._untag_for_edit(pid, kid)
                for removal in old_meta.get('flat_removals', []):
                    # A shared workspace can be deleted between the edit and
                    # undo. Restore only records whose workspace still exists.
                    if self._edit_history_repository().workspace_exists(
                        removal['workspace_id'],
                    ):
                        self.pending_changes.queue(
                            pid, 'keyword_remove_flat', removal['value'],
                            workspace_id=removal['workspace_id'],
                        )
            else:
                # A captured or completed add needs a corrective removal;
                # simply deleting a pending row cannot undo its sidecar write.
                self._untag_for_edit(pid, kid)
        if action == 'keyword_add':
            self._restore_edit_prediction_status(old_meta)
            if kw_name:
                self._edit_history_repository().restore_relabel_curation(
                    entry['workspace_id'], pid, kw_name,
                    old_meta.get('curation'),
                )
        if action == 'prediction_accept' and old_val:
            self._undo_prediction_accept_statuses(old_meta, old_val)

    def _redo_keyword_add(self, entry, item):
        pid, old_val = item['photo_id'], item['old_value']
        action = entry['action_type']
        old_meta = self._edit_old_value_meta(old_val)
        skip_tag = old_meta.get('no_tag', False)
        raw_kid = (item['new_value'] if action == 'prediction_accept' else None) or entry['new_value']
        # A no-tag accept (a species-less burst pick) records no keyword.
        kid = int(raw_kid) if raw_kid or not skip_tag else None
        kw_name = self._edit_history_repository().keyword_name(kid) if kid is not None else None
        if not skip_tag:
            if action == 'prediction_accept' and (
                not old_val or old_meta.get('keyword_only') or old_meta.get('symmetric_keyword_queue')
            ):
                # Cancel the removal restored by undo before queueing an add.
                self._retag_for_edit(pid, kid)
                for removal in old_meta.get('flat_removals', []):
                    self.pending_changes.cancel(
                        pid, 'keyword_remove_flat', removal['value'],
                        workspace_id=removal['workspace_id'],
                    )
            else:
                self._retag_for_edit(pid, kid)
        if action == 'keyword_add':
            self._reject_edit_prediction(old_meta)
            if kw_name:
                self._edit_history_repository().reapply_relabel_curation(
                    entry['workspace_id'], pid, kw_name,
                    old_meta.get('curation'),
                )
        if action == 'prediction_accept' and old_val:
            self._redo_prediction_accept_statuses(old_meta, old_val)

    def _undo_prediction_accept_statuses(self, old_meta, old_val):
        """Restore predictions to their pre-accept review state.

        Scope by labels_fingerprint too -- without it, undoing an accept in
        one label set would flip statuses of predictions produced under a
        different fingerprint and could promote the wrong fingerprint's
        top-confidence row back to 'pending'.

        Accept-subject no-tag accepts can span multiple classifier models on
        one detection, so iterate and dedupe by (detection, model,
        fingerprint) so each unique sibling scope is reset exactly once.
        """
        pred_ids = edit_prediction_ids(old_meta, old_val)
        if not pred_ids:
            return
        self._edit_history_repository().undo_prediction_accept_statuses(
            pred_ids, self.edit_history.prediction_scope,
        )
        # A Highlights confirm of a ``reviewed`` row records that status, so
        # undo restores the user's earlier decision instead of ``pending``.
        if old_meta.get("prior_status") == "reviewed" and old_meta.get("prediction_id"):
            self.predictions.update_status(int(old_meta["prediction_id"]), "reviewed")
        # Group apply snapshots each pick row's pre-apply review status so
        # undo can restore a member the user had previously rejected. The
        # scope reset above lands every row at ``alternative`` / ``pending``;
        # applying the recorded statuses afterwards overwrites those with
        # the exact prior state.
        prior_statuses = old_meta.get("prior_statuses")
        if prior_statuses:
            for pred_id_str, status in prior_statuses.items():
                if not status:
                    continue
                try:
                    pred_id = int(pred_id_str)
                except (TypeError, ValueError):
                    continue
                self.predictions.update_status(pred_id, status)

    def _redo_prediction_accept_statuses(self, old_meta, old_val):
        """Re-accept every recorded prediction and re-reject its siblings.

        Siblings are scoped to the same labels_fingerprint so the redo
        matches the original accept's scope and doesn't touch predictions
        from other label sets. Every id accepted in this batch is excluded
        from rejection, since a no-tag accept can hold several ids in one
        scope.
        """
        pred_ids = edit_prediction_ids(old_meta, old_val)
        if not pred_ids:
            return
        # Resolve the active workspace up front, before any status write.
        self._ws_id()
        history = self._edit_history_repository()
        accepted_by_scope = {}
        for pred_id in pred_ids:
            scope = self.edit_history.prediction_scope(pred_id)
            if scope is None:
                continue
            self.predictions.update_status(pred_id, 'accepted')
            accepted_by_scope.setdefault(scope, set()).add(pred_id)
        history.reject_accept_siblings(accepted_by_scope)
        # Group apply also rejects a sibling that was already ``accepted``
        # (see ``_accept_group_pick_rows``), which ``reject_accept_siblings``
        # leaves alone. Its snapshot records that prior ``accepted``, so
        # re-reject exactly those rows to match the original apply.
        accepted_ids = {int(p) for p in pred_ids}
        for pred_id_str, status in (old_meta.get("prior_statuses") or {}).items():
            if status != "accepted":
                continue
            try:
                pred_id = int(pred_id_str)
            except (TypeError, ValueError):
                continue
            if pred_id not in accepted_ids:
                self.predictions.update_status(pred_id, 'rejected')

    # -- species_replace --------------------------------------------------
    #
    # Atomic swap: the edit replaced old_value's species with new_value's.
    # Undo untags the new species and retags the old ones; redo does the
    # reverse. Both mirror the pending-change queue, and restore / re-apply
    # any species_highlights / photo_preferences rows the original relabel
    # migrated to the new species -- otherwise the photo lands back in its
    # old species bucket while the curated Highlight/Representative rows
    # stay stranded under the new one (see PR #1161).

    def _undo_species_replace(self, entry, item):
        pid = item['photo_id']
        old_meta = self._edit_old_value_meta(item['old_value'])
        new_kid = int(item['new_value']) if item['new_value'] else None
        new_kw_name = self._untag_for_edit(pid, new_kid) if new_kid else None
        for old_kid in old_meta.get("keyword_ids") or []:
            self._retag_for_edit(pid, old_kid)
        if new_kw_name:
            self._edit_history_repository().restore_relabel_curation(
                entry['workspace_id'], pid, new_kw_name,
                old_meta.get('curation'),
            )
        self._restore_edit_prediction_status(old_meta)

    def _redo_species_replace(self, entry, item):
        pid = item['photo_id']
        old_meta = self._edit_old_value_meta(item['old_value'])
        new_kid = int(item['new_value']) if item['new_value'] else None
        for old_kid in old_meta.get("keyword_ids") or []:
            self._untag_for_edit(pid, old_kid)
        new_kw_name = self._retag_for_edit(pid, new_kid) if new_kid else None
        if new_kw_name:
            self._edit_history_repository().reapply_relabel_curation(
                entry['workspace_id'], pid, new_kw_name,
                old_meta.get('curation'),
            )
        self._reject_edit_prediction(old_meta)

    _EDIT_FIELD_SETTERS = {
        'species_provenance': _edit_set_species_provenance,
        'flag': _edit_set_flag,
        'wildlife_excluded': _edit_set_wildlife_excluded,
        'color_label': _edit_set_color_label,
        'edit_recipe': _edit_set_edit_recipe,
        'photo_version': _edit_set_photo_version,
        'prediction_status': _edit_set_prediction_status,
    }
    _UNDO_HANDLERS = {
        'rating': _undo_rating,
        'keyword_add': _undo_keyword_add,
        'prediction_accept': _undo_keyword_add,
        'keyword_remove': _undo_keyword_remove,
        'species_replace': _undo_species_replace,
    }
    _REDO_HANDLERS = {
        'rating': _redo_rating,
        'keyword_add': _redo_keyword_add,
        'prediction_accept': _redo_keyword_add,
        'keyword_remove': _redo_keyword_remove,
        'species_replace': _redo_species_replace,
    }

    def _edit_old_value_meta(self, old_value: str | int | None) -> dict[str, Any]:
        """Parse edit item old_value, including newer JSON metadata payloads."""
        if not old_value:
            return {"keyword_id": None, "keyword_ids": []}
        if isinstance(old_value, str) and old_value.lstrip().startswith("{"):
            try:
                data = json.loads(old_value)
            except (TypeError, ValueError):
                return {"keyword_id": None}
            keyword_id = data.get("keyword_id")
            keyword_ids = data.get("keyword_ids")
            try:
                data["keyword_id"] = int(keyword_id) if keyword_id else None
            except (TypeError, ValueError):
                data["keyword_id"] = None
            if not isinstance(keyword_ids, list):
                keyword_ids = [data["keyword_id"]] if data["keyword_id"] else []
            parsed_ids = []
            for kid in keyword_ids:
                with contextlib.suppress(TypeError, ValueError):
                    parsed_ids.append(int(kid))
            data["keyword_ids"] = parsed_ids
            return data
        try:
            keyword_id = int(old_value)
            return {"keyword_id": keyword_id, "keyword_ids": [keyword_id]}
        except (TypeError, ValueError):
            return {"keyword_id": None, "keyword_ids": []}

    def _edit_prediction_id(self, meta, fallback):
        ids = edit_prediction_ids(meta, fallback)
        return ids[0] if ids else None

    def _restore_edit_prediction_status(self, meta):
        pred_id = meta.get("prediction_id")
        if not pred_id:
            return
        status = meta.get("prediction_status") or "pending"
        self.predictions.update_status(int(pred_id), status)

    def _reject_edit_prediction(self, meta):
        pred_id = meta.get("prediction_id")
        if not pred_id:
            return
        self.predictions.update_status(int(pred_id), "rejected")

    def prune_edit_history(self) -> dict[str, int]:
        """Delete the active workspace's oldest entries beyond the configured bounds.

        Undone entries awaiting redo are never pruned. Never commits: run it
        in a unit of work (``record_edit`` schedules it after its unit's
        commit). Returns the grouping snapshots retired and their bytes.
        """
        max_entries = settings_source.get('max_edit_history') or 1000
        return self._edit_history_repository().prune(
            max_entries,
            max_payload_bytes=max(0, int(settings_source.get('grouping_history_max_mb'))) * 1024**2,
        )

    # -- Collections --

    @property
    def collections(self) -> CollectionRepository:
        """Collections and the rule-driven photo queries: ``db.collections.get_photos(...)``.

        A domain accessor, not a cached attribute: every access builds a
        fresh repository, whose scoped methods resolve the workspace at use.
        Do not retain one across a workspace switch. Coordinated façade
        methods (``rules_resolvable``, ``query_photo_position``, the
        ``*_stacked`` id lists, ...) stay on ``Database`` and call through it.
        """
        return self._collection_repository()

    def _collection_repository(self) -> CollectionRepository:
        """Build the collections repository on this connection.

        The repository gets ``self._ws_id`` as a resolver rather than an id,
        so the active workspace is read exactly where these methods always
        read it: rule validation still fails before a missing workspace does,
        and ``create_default_collections(workspace_id=...)`` still needs no
        active workspace. The repository reads the effective config, the
        subject types and folder subtrees on its own connection and imports
        the SQL fragments it builds with.
        """
        from repositories.collections import CollectionRepository

        return CollectionRepository(self.conn, self._ws_id)

    def remap_collection_photo_ids(self, mapping: Mapping[int, int | None]) -> int:
        """Point every workspace's ``photo_ids`` rules at surviving photos; no commit."""
        from repositories.collections import remap_collection_photo_ids
        return remap_collection_photo_ids(self.conn, mapping)

    def photo_ids_named_by_collections(self) -> set[int]:
        """Every photo id named by a ``photo_ids`` rule in any workspace."""
        from repositories.collections import photo_ids_named_by_collections
        return photo_ids_named_by_collections(self.conn)

    def prune_collection_ids_of_missing_photos(self) -> list[dict[str, Any]]:
        """Drop ``photo_ids`` entries naming deleted photos, in every workspace; no commit."""
        from repositories.collections import prune_collection_ids_of_missing_photos
        return prune_collection_ids_of_missing_photos(self.conn)

    # ------ iNaturalist submissions ------

    def _inat_repository(self) -> InatRepository:
        """Build the (catalog-wide) iNaturalist repository on this connection."""
        from repositories.inat import InatRepository

        return InatRepository(self.conn, chunk_size=_SQLITE_PARAM_CHUNK_SIZE)

    @property
    def inat(self) -> InatRepository:
        """The iNaturalist submission records: ``db.inat.record_submission(...)``, ``.get_submissions(ids)``.

        A domain accessor, not a cached attribute: every access builds a fresh
        repository through ``_inat_repository``. Submissions are keyed by
        photo, so it needs no active workspace.
        """
        return self._inat_repository()

    # -- Job history (``JobRunner`` owns the table and its own writes) --

    def _job_history_repository(self) -> JobHistoryRepository:
        """Build the (catalog-wide) job-history repository on this connection."""
        from repositories.job_history import JobHistoryRepository

        return JobHistoryRepository(self.conn)

    @property
    def job_history(self) -> JobHistoryRepository:
        """The job-history domain: ``db.job_history.get(job_id)`` and friends.

        A domain accessor, not a cached attribute: every access builds a fresh
        repository through ``_job_history_repository``, exactly as a forwarding
        wrapper called at that moment would, so the connection and any
        workspace scoping are resolved per use. Do not hold the returned
        repository across ``set_active_workspace``.
        """
        return self._job_history_repository()

    # -- Pending NAS transfers --

    def _pending_archive_repository(self) -> PendingArchiveRepository:
        """Build the pending-NAS-transfer repository on this connection.

        It receives ``self._ws_id`` uncalled: the workspace-scoped listing and
        discard resolve it (raising ``RuntimeError`` when no workspace is
        active) before running any SQL, while ``set_state`` addresses a
        transfer by its unique id and never resolves it.
        """
        from repositories.pending_archives import PendingArchiveRepository

        return PendingArchiveRepository(self.conn, self._ws_id)

    @property
    def pending_archives(self) -> PendingArchiveRepository:
        """The pending-NAS-transfer domain: ``db.pending_archives.delete(id)`` and friends.

        A domain accessor, not a cached attribute: every access builds a fresh
        repository through ``_pending_archive_repository``. Accessing it never
        needs a workspace; ``open_with_review_collection`` and ``delete`` are
        scoped to the active workspace and raise ``RuntimeError`` when none is
        set, and ``set_state`` works by id in any workspace.
        """
        return self._pending_archive_repository()

    # -- Import job catalog reads --

    def _imports_repository(self) -> ImportRepository:
        """Build the (catalog-wide) import-job repository on this connection."""
        from repositories.imports import ImportRepository

        return ImportRepository(self.conn)

    @property
    def imports(self) -> ImportRepository:
        """The import job's catalog SQL: ``db.imports.photo_hash_row(folder, name)`` and friends.

        A domain accessor, not a cached attribute: every access builds a fresh
        repository through ``_imports_repository``. Nothing in it is scoped to
        a workspace, and its writes run in the caller's transaction (the
        import job commits or rolls back through ``db.commit()`` /
        ``db.rollback()``).
        """
        return self._imports_repository()

    @property
    def storage(self) -> StorageRepository:
        """Global managed-storage inspection; never retains an active scope."""
        from repositories.storage import StorageRepository
        return StorageRepository(self.conn)
