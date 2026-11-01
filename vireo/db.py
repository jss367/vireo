"""SQLite database for Vireo photo browser metadata cache."""

from __future__ import annotations

import contextlib
import json
import logging
import os
import sqlite3
import time
from collections.abc import Callable, Collection, Iterable, Iterator, Mapping, Sequence
from typing import TYPE_CHECKING, Any, Literal, Self, TypedDict

import settings_source
from keyword_normalization import (
    keyword_match_key,
    normalize_keyword_display,
)
from new_images_cache import get_shared_cache
from repositories.keyword_identity_sql import resolve_import_alias
from repositories.prediction_review import prediction_review_join, prediction_review_status
from repositories.species_taxon_lookup import taxon_lookup_variants
from unit_of_work import HoldingConnection, UnitOfWork, report_discarded_transaction

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


class BrowseStackConfig(TypedDict):
    """Normalized Browse stack settings, independent of the full app config."""

    time_gap: float
    split_mode: str


log = logging.getLogger(__name__)

AUTO_MATCH_REVIEW_MARKER = "__vireo_auto_match__"

# Durable provenance values for ``photo_keywords.source``.
#
# ``KEYWORD_SOURCE_MANUAL`` means "a person explicitly asked for this
# association"; nothing that prunes generated metadata may delete it.
# ``KEYWORD_SOURCE_UNKNOWN`` (NULL) means "we genuinely cannot tell" — the
# only writers allowed to use it are the sidecar readers (scanner, XMP
# reconcile), where a term may equally have come from the user's Lightroom
# catalog or from a keyword Vireo itself wrote out.
#
# ``tag_photo`` defaults to MANUAL on purpose. The failure mode of a
# mis-stamped association is asymmetric: guessing "manual" for a generated
# tag leaves a stale keyword the user can delete, while guessing "unknown"
# for a hand-added tag lets retirement passes erase user metadata silently
# and unrecoverably. So the default is the fail-safe value and every
# provenance-neutral writer has to say so explicitly. See
# ``test_keyword_provenance_contract.py``, which pins that inventory.
KEYWORD_SOURCE_MANUAL = "manual"
# Written by the prediction-accept path (PR #1479 Phase 4). Nothing in this
# module writes it yet; it is declared here so the lattice below has its
# middle element and both PRs share one ordering instead of two.
KEYWORD_SOURCE_ACCEPT = "accept"
KEYWORD_SOURCE_UNKNOWN = None

# Provenance is a lattice, weakest first: NULL < 'accept' < 'manual'.
#
# The rule the lattice exists to state once: **when two associations for the
# same (photo, keyword) converge, the survivor takes the stronger claim,
# never the weaker.** Associations converge in more places than they are
# created — a duplicate merge folds losers onto a winner, a keyword
# rename/curation-merge repoints rows onto a canonical keyword, RAW/JPEG
# companion pairing copies a companion's keywords onto the primary, and every
# ``tag_photo`` re-tag lands on a row that may already exist. Each of those is
# a place where a hand-added keyword can quietly decay into an unattributed
# row that a retirement pass reads as generated and deletes (the one-shot
# Wildlife genre retirement, since removed, worked that way).
# ``COALESCE(new, old)`` is not the rule — it is only accidentally
# equal to it while the column is set-or-NULL, and it downgrades 'manual' to
# 'accept' the moment a third value exists.
#
# So the fold is expressed once, here, and every convergence point uses it:
# ``keyword_source_max`` in Python, ``keyword_source_max_sql`` /
# ``KEYWORD_SOURCE_CONFLICT_SQL`` in SQL.
# ``test_keyword_provenance_contract.py`` enumerates the call sites and fails
# on a new one that does not.
KEYWORD_SOURCE_PRECEDENCE = (
    KEYWORD_SOURCE_UNKNOWN,
    KEYWORD_SOURCE_ACCEPT,
    KEYWORD_SOURCE_MANUAL,
)
_KEYWORD_SOURCE_RANK = {
    value: rank for rank, value in enumerate(KEYWORD_SOURCE_PRECEDENCE)
}
# A stamp this build does not recognise still means "somebody deliberately
# claimed this row", so it must outrank "no stamp at all" — but it never
# outranks an explicit 'manual', which is the one value with a delete-safety
# guarantee attached.
_KEYWORD_SOURCE_UNRECOGNIZED_RANK = 1


def keyword_source_rank(source: str | None) -> int:
    """Position of ``source`` in the provenance lattice (higher = stronger)."""
    if source is None:
        return _KEYWORD_SOURCE_RANK[KEYWORD_SOURCE_UNKNOWN]
    return _KEYWORD_SOURCE_RANK.get(source, _KEYWORD_SOURCE_UNRECOGNIZED_RANK)


def keyword_source_max(*sources: str | None) -> str | None:
    """Return the strongest of ``sources``: the fold for converging rows.

    Ties keep the first argument, so callers pass the incoming/stronger
    candidate first when they want it to win a same-rank tie.
    """
    best = KEYWORD_SOURCE_UNKNOWN
    best_rank = keyword_source_rank(KEYWORD_SOURCE_UNKNOWN)
    for source in sources:
        rank = keyword_source_rank(source)
        if rank > best_rank:
            best, best_rank = source, rank
    return best


def keyword_source_rank_sql(expr: str) -> str:
    """SQL for ``keyword_source_rank(expr)``, from the same ordering."""
    whens = " ".join(
        f"WHEN {expr} = '{value}' THEN {rank}"
        for rank, value in enumerate(KEYWORD_SOURCE_PRECEDENCE)
        if value is not None
    )
    return (
        f"(CASE WHEN {expr} IS NULL THEN "
        f"{_KEYWORD_SOURCE_RANK[KEYWORD_SOURCE_UNKNOWN]} {whens} "
        f"ELSE {_KEYWORD_SOURCE_UNRECOGNIZED_RANK} END)"
    )


def keyword_source_max_sql(left: str, right: str) -> str:
    """SQL for ``keyword_source_max(left, right)``.

    ``left`` and ``right`` are SQL expressions and are each substituted twice
    (once to rank, once to yield the value), so pass column references,
    literals, or scalar subqueries bound with *named* parameters — positional
    ``?`` would have to be supplied twice in an order the caller cannot see.
    """
    return (
        f"(CASE WHEN {keyword_source_rank_sql(left)} >= "
        f"{keyword_source_rank_sql(right)} THEN {left} ELSE {right} END)"
    )


# The upsert clause every ``INSERT INTO photo_keywords`` must end with: an
# existing row keeps its provenance unless the incoming row's is stronger,
# and keeps its species_provenance unless it has none. Rows that INSERT only
# three columns leave ``excluded.species_provenance`` NULL, so COALESCE
# leaves the surviving row's evidence intact; merge paths that INSERT four
# columns carry the loser's evidence onto the survivor when the survivor had
# none, matching the ``_fold_history_state`` rule that keeps the survivor's
# own stamp over the incoming one.
KEYWORD_SOURCE_CONFLICT_SQL = (
    "ON CONFLICT(photo_id, keyword_id) DO UPDATE SET "
    "source = "
    + keyword_source_max_sql("excluded.source", "photo_keywords.source")
    + ", species_provenance = COALESCE("
    "photo_keywords.species_provenance, excluded.species_provenance)"
)

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


def _taxon_lookup_variants(name: str) -> list[str]:
    """Return the normal punctuation-tolerant species lookup variants."""
    return taxon_lookup_variants(name)


def _path_for_subtree_match(value: str) -> str:
    """Normalize a stored path for platform-neutral subtree prefix matching."""
    return value.replace("\\", "/").rstrip("/")


def _escape_like(s: str) -> str:
    """Escape SQL LIKE metacharacters so a path is matched literally.

    LIKE treats ``%`` and ``_`` as wildcards unconditionally — an unescaped
    folder path like ``/pics/my_dir`` also matches ``/pics/myXdir``, which
    silently corrupts sibling folders in path-cascade UPDATEs. Pair with
    ``LIKE ? ESCAPE '\\'`` at the call site (same convention as ingest.py
    and scanner.py).
    """
    return s.replace("\\", "\\\\").replace("%", "\\%").replace("_", "\\_")


# Sentinel for ``resolve_species_display_name(case_convention=...)``: absent
# means "work it out", which is different from an explicit ``None`` ("there is
# no convention, leave the caller's spelling alone").
_DETECT_CASE_CONVENTION = object()


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


# Life-list ancestor suppression. Broadening the identification-keyword
# rank guard admits linked genus/family/class rows, but Lightroom catalog
# imports copy every ``includeParents`` ancestor onto the photo as a flat
# keyword (see ``vireo/catalog.py``) and the classifier already treats
# ancestor tags as "broader labels for the same taxon" rather than
# separate identifications (see ``_can_auto_accept_detection_prediction``
# in ``vireo/classify_job/store_predictions.py``). Without a hierarchy filter, a normal
# species-tagged robin would also inflate ``Turdus`` / ``Turdidae`` /
# ``Aves`` Life List buckets on those catalogs.
#
# Suppress a linked higher-rank taxonomy keyword on a photo whenever the
# same photo carries another linked taxonomy keyword whose taxon is a
# strict descendant of it. Species-rank keywords are never suppressed;
# unlinked keywords (``taxon_id IS NULL``) can't be checked for ancestry
# and pass through unchanged. The clause references outer aliases
# ``pk`` (photo_keywords) and ``k`` (keywords); callers don't need to
# join ``taxa`` themselves because the rank/ancestry checks resolve
# ``k.taxon_id`` via inner subqueries.
_LIFE_LIST_ANCESTOR_SUPPRESSION_CLAUSE = """
    AND NOT (
        k.taxon_id IS NOT NULL
        AND EXISTS (
            SELECT 1 FROM taxa t_sup_rank
            WHERE t_sup_rank.id = k.taxon_id
              AND t_sup_rank.rank IS NOT NULL
              AND t_sup_rank.rank != 'species'
        )
        AND EXISTS (
            SELECT 1 FROM photo_keywords pk_sup
            JOIN keywords k_sup ON k_sup.id = pk_sup.keyword_id
             AND (k_sup.is_species = 1 OR k_sup.type = 'taxonomy')
             AND k_sup.taxon_id IS NOT NULL
            WHERE pk_sup.photo_id = pk.photo_id
              AND k_sup.id != k.id
              AND k.taxon_id IN (
                  WITH RECURSIVE anc(id) AS (
                      SELECT parent_id FROM taxa
                       WHERE id = k_sup.taxon_id
                         AND parent_id IS NOT NULL
                      UNION ALL
                      SELECT t_anc.parent_id
                      FROM taxa t_anc JOIN anc a ON t_anc.id = a.id
                       WHERE t_anc.parent_id IS NOT NULL
                  )
                  SELECT id FROM anc
              )
        )
    )
"""


# Canonical set of keyword type values stored in keywords.type.
# - taxonomy: a species/genus/etc. (linked to taxa via taxon_id)
# - individual: a named person, pet, or otherwise tracked individual
# - location: a named location ("Yosemite", "backyard")
# - genre: a non-subject visual category ("Landscape", "Sunset")
# - general: catch-all/free-form tag (the legacy default)
KEYWORD_TYPES = frozenset({"taxonomy", "individual", "location", "genre", "general"})

# --- Browse stacks -----------------------------------------------------
# Defaults for the Stacks toggle's burst grouping, mirroring
# ``config.DEFAULTS``. Duplicated rather than imported so a Database built
# without any config (tests, background jobs) still groups the same way the
# app does instead of silently picking a different gap.
BROWSE_STACK_TIME_GAP_DEFAULT = 3.0
BROWSE_STACK_SPLIT_MODE_DEFAULT = "break"
BROWSE_STACK_SPLIT_MODES = ("break", "partition")

# ``julianday`` returns days as a double, so differencing two modern dates
# leaves roughly 0.05 ms of resolution once the result is scaled back to
# seconds. Without a tolerance, frames spaced exactly at the configured gap
# fall on either side of the comparison depending on that noise — one pair of
# 3.000s-apart frames stacks while the next does not. A millisecond is far
# below any capture-time resolution EXIF records and puts the boundary where
# the setting says it is: a gap *equal* to the setting is inside the burst.
BURST_GAP_TOLERANCE_SECONDS = 0.001


def normalize_browse_stack_config(config: object = None) -> BrowseStackConfig:
    """Return ``{"time_gap", "split_mode"}`` for Browse's stack projection.

    Accepts either an already-normalized stack dict or a full (effective)
    config dict, so callers can pass ``db.get_effective_config(cfg.load())``
    straight through. Anything unreadable falls back to the default rather
    than raising: a malformed setting must not take Browse down, and the
    default is the behaviour the settings UI describes.
    """
    gap = BROWSE_STACK_TIME_GAP_DEFAULT
    mode = BROWSE_STACK_SPLIT_MODE_DEFAULT
    if isinstance(config, dict):
        raw_gap = config.get("time_gap", config.get("browse_stack_time_gap", gap))
        try:
            gap = float(raw_gap)
        except (TypeError, ValueError):
            gap = BROWSE_STACK_TIME_GAP_DEFAULT
        if not (gap >= 0.0):  # also catches NaN
            gap = BROWSE_STACK_TIME_GAP_DEFAULT
        raw_mode = config.get(
            "split_mode", config.get("browse_stack_split_mode", mode)
        )
        mode = raw_mode if raw_mode in BROWSE_STACK_SPLIT_MODES else (
            BROWSE_STACK_SPLIT_MODE_DEFAULT
        )
    return {"time_gap": gap, "split_mode": mode}


_LOCATION_COMPONENT_RANKS = {
    "country": 10,
    "administrative_area_level_1": 20,
    "administrative_area_level_2": 30,
    "administrative_area_level_3": 40,
    "administrative_area_level_4": 50,
    "administrative_area_level_5": 60,
    "administrative_area_level_6": 70,
    "administrative_area_level_7": 80,
    "locality": 90,
    "postal_town": 90,
    "sublocality": 100,
    "sublocality_level_1": 100,
    "sublocality_level_2": 110,
    "sublocality_level_3": 120,
    "sublocality_level_4": 130,
    "sublocality_level_5": 140,
    "neighborhood": 150,
}

# Default set of types that count as "identifying" a photo for queue
# membership / classifier skip purposes. Workspaces can override.
SUBJECT_TYPES_DEFAULT = frozenset({"taxonomy", "individual", "genre"})

NEEDS_IDENTIFICATION_RULES = [
    {"field": "has_subject", "op": "equals", "value": 0},
    {"field": "wildlife_excluded", "op": "equals", "value": 0},
]

GPS_WITHOUT_LOCATION_KEYWORD_RULES = [
    {"field": "location_keyword_missing", "op": "equals", "value": 1},
]

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


def _inclusive_date_to(date_to):
    """Pad a date_to bound so it includes sub-second timestamps.

    The frontend sends either 'YYYY-MM-DD' (date picker) or
    'YYYY-MM-DDTHH:MM:SS' (timeline click).  With sub-second precision
    timestamps like '23:59:59.500000', a naive '<= 23:59:59' comparison
    excludes them.  Append '.999999' so the bound covers the full second.
    Fractional seconds shorter than 6 digits are padded with '9's so that
    lexical comparison remains inclusive (e.g. '.5' → '.599999').
    """
    if date_to is None:
        return None
    if not isinstance(date_to, str):
        return None
    if len(date_to) == 10:  # bare date
        return date_to + "T23:59:59.999999"
    if len(date_to) == 19:  # date + time, no fractional seconds
        return date_to + ".999999"
    # Has fractional seconds — pad to 6 digits with '9' for inclusive upper bound
    dot_idx = date_to.rfind(".")
    if dot_idx >= 0:
        frac = date_to[dot_idx + 1:]
        if len(frac) < 6:
            return date_to + "9" * (6 - len(frac))
    return date_to


def _rule_upper_bound(value):
    """Pad only bare ``YYYY-MM-DD`` values for universal-filter upper bounds.

    A precise instant like ``2024-01-01T12:00:00`` is what the caller meant;
    padding it to ``.999999`` would spuriously include sub-second photos in
    that same clock second (e.g. ``12:00:00.5``) on a strict ``>`` or match
    them on a ``<=``. Only bare dates need to be advanced to end-of-day so
    ``<= 2024-01-01`` covers the whole named day.
    """
    if isinstance(value, str) and len(value) == 10:
        return _inclusive_date_to(value)
    return value


_PHOTO_DATE_ASC_ORDER = "p.timestamp IS NULL, p.timestamp ASC, p.filename ASC, p.id ASC"
_PHOTO_DATE_DESC_ORDER = "p.timestamp IS NULL, p.timestamp DESC, p.filename ASC, p.id ASC"

# A photo's "prediction confidence" is the score of the one prediction the UI
# presents as its strongest guess — not "some prediction row exists with this
# score". The eligibility gates are exactly the ones every other surface that
# speaks for the *displayed* prediction applies, so the sort key and the badge
# cannot rank a photo on a guess Browse hides:
#
# * ``species IS NOT NULL`` — a detection-only row ("animal", no species) is
#   not a species guess and has no confidence to rank by.
# * ``detector_confidence >= <workspace floor>`` — detections under the
#   floor are hidden everywhere (``detection_store.get_detections_for_photos``, the
#   dashboard counters, ``_prediction_exists``), so their predictions must
#   not position a card either.
# * status not ``rejected`` — a guess the user threw away must not keep the
#   photo at the top of a confidence sort.
# * status not ``alternative`` — runner-up rows are dropped from top-level
#   results by ``/api/predictions``, matching ``_prediction_exists``.
# * newest ``labels_fingerprint`` per (detection, classifier) — the same pin
#   ``get_top_prediction_for_photo`` and ``_prediction_exists`` use, so a
#   reclassified photo ranks on its current label set rather than on a
#   stale row left behind by the previous one.
#
# Correlates on the outer ``p.id``, so every caller must expose ``photos``
# (or a projection carrying ``id``) as ``p``. Takes the two parameters
# ``Database._top_prediction_confidence_params()`` returns, in that order.
# It is deliberately a correlated subquery rather than a grouped CTE: at 88k
# photos / 192k predictions the CTE form materializes every photo in the
# catalog (~0.35s even for a 60-photo folder), while the correlated form is
# index-driven per scoped row — 0.03s on a small workspace and no slower
# than the CTE on the largest one.
_TOP_PREDICTION_CONFIDENCE_EXPR = f"""(
            SELECT MAX(conf_pr.confidence)
            FROM detections conf_d
            JOIN predictions conf_pr ON conf_pr.detection_id = conf_d.id
            {prediction_review_join('conf_pr', review_alias='conf_prv')}
            WHERE conf_d.photo_id = p.id
              AND conf_d.detector_confidence >= ?
              AND conf_pr.species IS NOT NULL
              AND {prediction_review_status('conf_prv')}
                  NOT IN ('rejected', 'alternative')
              AND conf_pr.labels_fingerprint = (
                  SELECT conf_pr2.labels_fingerprint FROM predictions conf_pr2
                  WHERE conf_pr2.detection_id = conf_pr.detection_id
                    AND conf_pr2.classifier_model = conf_pr.classifier_model
                  ORDER BY conf_pr2.created_at DESC, conf_pr2.id DESC
                  LIMIT 1
              )
        )"""

# Sorts whose ORDER BY reads only ``photos`` columns, so they need no params.
_PHOTO_SORT_ORDERS = {
    "date": _PHOTO_DATE_ASC_ORDER,
    "date_desc": _PHOTO_DATE_DESC_ORDER,
    "name": "p.filename ASC, p.id ASC",
    "name_desc": "p.filename DESC, p.id ASC",
    "rating": "p.rating DESC, p.filename ASC, p.id ASC",
    "sharpness": "p.sharpness DESC, p.filename ASC, p.id ASC",
    "sharpness_asc": "p.sharpness ASC, p.filename ASC, p.id ASC",
    "quality": "p.quality_score DESC, p.filename ASC, p.id ASC",
}

# Photos the classifier has never scored are unscored, not unconfident, so
# they sort last in *both* directions — "Prediction confidence (lowest)" is
# read as "the guesses Vireo is least sure of", and answering it with
# thousands of frames that carry no guess at all would bury exactly the
# photos the sort exists to surface. ``NULLS LAST`` rather than the
# ``expr IS NULL, expr`` idiom used for plain columns above: repeating the
# key here would mean evaluating the correlated subquery twice per row.
_PREDICTION_CONFIDENCE_SORTS = {
    "prediction_confidence": "DESC",
    "prediction_confidence_asc": "ASC",
}


def _contains_prediction_field(node, prediction_fields):
    """Whether a rule node is, or (transitively) holds, a prediction leaf."""
    if not isinstance(node, dict):
        return False
    if "rules" in node and "field" not in node:
        return any(
            _contains_prediction_field(c, prediction_fields)
            for c in node.get("rules") or []
        )
    return node.get("field") in prediction_fields


def _relax_negated_walk(node, negated, prediction_fields, broad):
    """``_relax_negated_prediction_leaves``'s recursive pass over one node."""
    if isinstance(node, dict) and "rules" in node and "field" not in node:
        mode = node.get("mode", "all")
        child_negated = negated or (mode == "none")
        new_children = []
        had_broad = False
        for child in node.get("rules") or []:
            is_group = (
                isinstance(child, dict)
                and "rules" in child
                and "field" not in child
            )
            # Under negation, a subgroup that mixes metadata with
            # prediction leaves would be compiled by the outer
            # ``NOT EXISTS(...)`` in a way that lets its metadata
            # siblings prune photos before the row filter sees them
            # (see ``Database._relax_negated_prediction_leaves``). Drop the whole subgroup so
            # SQL broadens; the row filter re-evaluates the
            # ORIGINAL tree per row.
            if (
                child_negated
                and is_group
                and _contains_prediction_field(child, prediction_fields)
            ):
                had_broad = True
                continue
            stripped = _relax_negated_walk(
                child, child_negated, prediction_fields, broad,
            )
            if stripped is None:
                # Bare row-level leaf dropped under negation:
                # SQL for that leaf broadens to TRUE.
                had_broad = True
                continue
            if stripped is broad:
                had_broad = True
                continue
            new_children.append(stripped)
        # ``any`` with any broad child is TRUE at the photo
        # level — the whole OR broadens. ``all``/``none`` drop
        # broad children silently (they don't restrict) unless
        # every child broadened, in which case the group itself
        # is broad and must propagate.
        if mode == "any" and had_broad:
            return broad
        if had_broad and not new_children:
            return broad
        new_node = dict(node)
        new_node["rules"] = new_children
        return new_node
    if negated and isinstance(node, dict) and \
            node.get("field") in prediction_fields:
        return None
    return node


def _rule_truthy(value):
    """Whether a rule value reads as boolean true."""
    return value is True or value == 1 or value == "1" or value == "true"


def _rule_num_match(row_val, op, want):
    """A numeric rule leaf evaluated against one row value."""
    if row_val is None:
        return False
    try:
        a = float(row_val)
    except (TypeError, ValueError):
        return False
    if op == "between":
        if not isinstance(want, list) or len(want) != 2:
            return False
        try:
            lo, hi = float(want[0]), float(want[1])
        except (TypeError, ValueError):
            return False
        return lo <= a <= hi
    try:
        b = float(want)
    except (TypeError, ValueError):
        return False
    if op == ">=":
        return a >= b
    if op == "<=":
        return a <= b
    if op == ">":
        return a > b
    if op == "<":
        return a < b
    if op in ("equals", "is"):
        return a == b
    if op == "is not":
        return a != b
    return False


def _rule_text_match(row_val, op, want):
    """A case-insensitive text rule leaf evaluated against one row value."""
    a = "" if row_val is None else str(row_val)
    b = "" if want is None else str(want)
    if op == "contains":
        return b.lower() in a.lower()
    if op == "not_contains":
        return b.lower() not in a.lower()
    if op == "starts_with":
        return a.lower().startswith(b.lower())
    if op == "ends_with":
        return a.lower().endswith(b.lower())
    if op in ("equals", "is"):
        return a.lower() == b.lower()
    if op == "is not":
        return a.lower() != b.lower()
    return False


def _rule_enum_match(row_val, op, want):
    """An enum rule leaf evaluated against one row value."""
    a = "" if row_val is None else str(row_val)
    if op in ("in", "not_in"):
        values = [str(v) for v in (want or [])]
        inside = a in values
        return inside if op == "in" else not inside
    if op in ("equals", "is"):
        return a == str(want)
    if op == "is not":
        return a != str(want)
    return False


def _prediction_leaf_row_match(rule, row, leaf_photo_sets):
    """One rule leaf evaluated against one prediction row.

    ``leaf_photo_sets`` maps ``id(leaf)`` to the photo ids a metadata leaf
    inside an ``any``/``none`` group matches (``None``: match every row).
    """
    field = rule.get("field")
    op = rule.get("op", "")
    value = rule.get("value")
    if field == "prediction_confidence":
        return _rule_num_match(row["confidence"], op, value)
    if field == "prediction_status":
        return _rule_enum_match(row["status"], op, value)
    if field == "classifier_model":
        return _rule_text_match(row["classifier_model"], op, value)
    if field in (
        "taxonomy_kingdom",
        "taxonomy_phylum",
        "taxonomy_class",
        "taxonomy_order",
        "taxonomy_family",
        "taxonomy_genus",
    ):
        col = row[field] if field in row.keys() else None  # noqa: SIM118 -- sqlite3.Row supports `in` on keys() but not on the row itself
        if op == "contains":
            return _rule_text_match(col, "contains", value)
        return _rule_text_match(col, op, value)
    if field == "needs_review":
        is_pending = str(row["status"] or "pending") == "pending"
        return is_pending if _rule_truthy(value) else not is_pending
    # Non-prediction leaf. In a pure ``all`` position the SQL
    # subquery already proved this photo satisfies it, so shortcut
    # to True. Inside a mixed ``any``/``none`` group we can't
    # shortcut (SQL only proved *some* sibling matched at the
    # photo level, not this leaf), so answer from the pre-resolved
    # per-leaf photo-id set.
    pset = leaf_photo_sets.get(id(rule))
    if pset is None:
        return True
    return row["photo_id"] in pset


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
        self.conn: HoldingConnection = sqlite3.connect(
            db_path, check_same_thread=False, factory=HoldingConnection,
        )
        self._unit = UnitOfWork()
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
        self.folders.repair_missing_parents()
        self.repair_stale_folder_parents()
        self.ensure_default_workspace()
        # Idempotent default-keyword seed. Cheap warm-path (single
        # SELECT 1 LIMIT 1 short-circuit) — matches ensure_default_workspace
        # above.
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
    # ``sqlite3.Connection`` operations they name, kept for callers not yet
    # converted. While a unit is open, ``commit()`` is a no-op and
    # ``rollback()`` dooms the unit (``unit_of_work.HoldingConnection``).

    @property
    def in_transaction(self) -> bool:
        """True while the connection has an open transaction."""
        return self.conn.in_transaction

    @property
    def in_unit_of_work(self) -> bool:
        """True while a ``transaction()`` block is open on this Database."""
        return bool(self._unit.depth)

    def commit(self) -> None:
        """Commit the open transaction (a no-op inside a unit of work)."""
        self.conn.commit()

    def rollback(self) -> None:
        """Roll back the open transaction.

        Inside a unit of work this ends the unit's transaction part-way, so
        the unit raises ``TransactionOwnershipError`` at its exit instead of
        committing; abandon a unit by raising out of its block instead.
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
        commit or roll back first.
        """
        self.conn.execute("BEGIN IMMEDIATE")

    def begin(self) -> None:
        """Open a deferred transaction, so the reads that follow share one snapshot.

        Plain ``BEGIN``: no lock is taken until the first statement, and every
        read after that sees the same snapshot until ``commit()`` or
        ``rollback()`` (read-only callers release it with ``rollback()``).
        Raises ``sqlite3.OperationalError`` if a transaction is already open
        (BEGIN does not nest).
        """
        self.conn.execute("BEGIN")

    @contextlib.contextmanager
    def transaction(
        self, *, immediate: bool = False, retry_busy: bool = False,
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
        ``with conn:`` did; that is a migration rule.
        """
        provenance = dict(self._species_provenance_before)
        entered = finished = False
        try:
            with self._unit.enter(self.conn, immediate=immediate, retry_busy=retry_busy):
                entered = True
                yield
            finished = True
        finally:
            # Python-side state follows the savepoint: a nested rollback
            # restores the snapshot taken at its start, an outermost one
            # (like ``rollback()``) drops everything. A block refused at entry
            # (``immediate`` over a caller's open transaction) took nothing,
            # so the caller's snapshot stays with the caller's transaction.
            if entered and not finished:
                self._species_provenance_before = provenance if self._unit.depth else {}

    def after_commit(self, callback: Callable[[], object]) -> None:
        """Run ``callback`` once the open unit of work commits.

        For work that must only happen once the data is durable (pruning
        history, deleting cached files). Dropped if the unit (or the
        savepoint it was registered in) rolls back; run at once when no unit
        is open. A callback that raises is logged and does not undo the
        commit.
        """
        self._unit.after_commit(callback)

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
        """Create a new workspace. Returns the workspace id."""
        from catalog_result_files import highest_workspace_id
        id_floor = (
            highest_workspace_id(os.path.dirname(self._db_path))
            if self._db_path != ":memory:" else 0
        )
        workspace_id = self._workspace_repository().create(
            name, config_overrides, ui_state, id_floor=id_floor,
        )
        # Invalidate any cached reader state even though IDs now advance
        # monotonically past both catalog rows and legacy result files.
        self._new_images_cache.invalidate_workspaces(self._db_path, [workspace_id])
        return workspace_id

    def get_effective_config(self, global_config: dict[str, Any]) -> dict[str, Any]:
        """Return config with workspace overrides applied over global config.

        Args:
            global_config: dict from config.load()
        Returns:
            dict with workspace overrides deep-merged on top of global config
            so a nested override (e.g. ``{"pipeline": {"w_focus": 0.5}}``)
            replaces only the named leaf, not the whole parent dict.
        """
        import config_schema

        ws = self.workspaces.get(self._active_workspace_id)
        if not ws or not ws["config_overrides"]:
            return global_config
        try:
            overrides = json.loads(ws["config_overrides"]) if isinstance(ws["config_overrides"], str) else ws["config_overrides"]
            if not isinstance(overrides, dict):
                return global_config
            effective = config_schema.deep_merge(global_config, overrides)
        except (json.JSONDecodeError, TypeError):
            return global_config
        # An override stored before the workspace routes validated values
        # (``null``, ``"abc"``) falls back to the global value it shadows.
        return config_schema.repair_types(effective, global_config)

    def effective_config(self) -> dict[str, Any]:
        """The global settings with the active workspace's overrides applied.

        Exactly ``get_effective_config(cfg.load())``, for the data layer: the
        global settings come through ``settings_source`` (which ``config``
        installs), so neither ``Database`` nor a repository imports ``config``.
        Repositories receive this method from their factory.
        """
        return self.get_effective_config(settings_source.load())

    def get_subject_types(self) -> set[str]:
        """Return the keyword types that count as 'identified' for the active
        workspace.

        config_overrides is arbitrary JSON (api_update_workspace persists
        whatever the client sends), so subject_types entries may not be
        strings — guard the membership test against unhashable values
        (nested lists/objects) to avoid TypeError downstream in callers
        like collection filtering and the classify skip-gate.
        """
        effective = self.effective_config()
        raw = effective.get("subject_types", list(SUBJECT_TYPES_DEFAULT))
        if not isinstance(raw, list):
            return set(SUBJECT_TYPES_DEFAULT)
        return {t for t in raw if isinstance(t, str) and t in KEYWORD_TYPES}

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
        """Create the Default workspace if it doesn't exist. Returns its id."""
        default_id = self._workspace_repository().default_id()
        if default_id is not None:
            return default_id
        return self.create_workspace("Default")

    def _species_curation_repository(self) -> SpeciesCurationRepository:
        """Build the species-curation repository on this connection.

        The active workspace is resolved lazily (``Database._ws_id`` is
        passed as a resolver), so methods that return early or canonicalize
        a species name first still raise at the point they always did. The
        two façade methods from other domains that the moved bodies call
        (species-name resolution and folder subtrees) are handed over bound,
        so monkeypatches of ``Database`` keep reaching the moved code.
        """
        from repositories.species_curation import SpeciesCurationRepository

        return SpeciesCurationRepository(
            self.conn,
            self._ws_id,
            chunks=_chunks,
            life_list_ancestor_suppression_clause=(
                _LIFE_LIST_ANCESTOR_SUPPRESSION_CLAUSE
            ),
            resolve_species_display_name=self.resolve_species_display_name,
            get_folder_subtree_ids=self.get_folder_subtree_ids,
            effective_config=self.effective_config,
        )

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
        resolve it. ``commit_with_retry`` and the path helpers are looked up
        here, so monkeypatches of the ``db`` module functions still reach the
        moved SQL. The merge's carries into other domains (the GPS-review
        transfer and the embedded-keyword offers) are bound here too.
        """
        from repositories.folders import FolderMergeHooks, FolderRepository
        from services.identity_alternatives import prepare_photo_merge

        return FolderRepository(
            self.conn,
            self._ws_id,
            commit_with_retry=commit_with_retry,
            path_for_subtree_match=_path_for_subtree_match,
            stored_parent_path=_stored_parent_path,
            subtree_prefix=_subtree_prefix,
            subtree_relative=_subtree_relative,
            join_subtree_path=_join_subtree_path,
            chunk_size=_SQLITE_PARAM_CHUNK_SIZE,
            merge_hooks=FolderMergeHooks(
                transfer_gps_review=self.moves_merge.transfer_gps_review,
                transfer_embedded_offered=self.photo_keywords.transfer_embedded_offered_keys,
                prepare_identity_merge=lambda losing, survivor: prepare_photo_merge(self, losing, survivor),
            ),
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

    def get_folder_subtree_ids(self, folder_id: int) -> list[int]:
        """Return [folder_id, ...descendant_ids] restricted to the active workspace.

        The root is always included as-is so callers' own workspace filter on
        photos still applies. Descendants are walked through
        ``folders.parent_id`` only when both the parent (the current node)
        AND the child are linked to the active workspace, so branches that
        pass through detached folders never propagate. In particular, a stale
        or crafted ``folder_id`` for a folder that is no longer in the active
        workspace will not expand into its active descendants.
        """
        return self._folder_repository().subtree_ids(folder_id)

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
        helpers are ``db`` module functions and are passed in, and so are the
        two methods of other domains every move or merge runs mid-transaction:
        the folder parent relink and the keyword-association carry, which
        re-tags through ``tag_photo`` on the façade.
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
            carry_keyword_associations=self._carry_keyword_associations_for_merge,
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
            self.tag_photo(surviving_id, kw_id, _commit=False)
        return True

    def _carry_keyword_associations_for_merge(
            self, losing_id, surviving_id, by_key):
        """Apply the losing row's winning keyword ops to the survivor's tags.

        ``by_key`` is the grouping built by
        ``moves_merge.reconcile_conflicting_keyword_edits``; rows it has already
        deleted are skipped by id. Only keys whose newest surviving change
        sits on the losing photo are applied -- when the survivor holds the
        newer change its own association already reflects it.
        """
        live = self._moves_merge_repository().live_pending_change_ids(losing_id, surviving_id)

        def _flatten(photo_slot):
            out = []
            for ws_sides in photo_slot.values():
                out.extend(ws_sides["add"] + ws_sides["remove"])
            return out

        for match_key, slot in by_key.items():
            loser = [r for r in _flatten(slot[losing_id]) if r["id"] in live]
            if not loser:
                continue
            survivor = [
                r for r in _flatten(slot[surviving_id]) if r["id"] in live
            ]

            def key(r):
                return (r["created_at"] or "", r["id"])

            newest_loser = max(loser, key=key)
            if survivor and key(max(survivor, key=key)) > key(newest_loser):
                continue
            if newest_loser["change_type"] == "keyword_add":
                # The association the add already wrote is about to be
                # deleted with the losing photo. Re-point it at the
                # survivor. If it is somehow absent the catalog is already
                # inconsistent and there is no id to carry -- leave it to
                # the sync rather than guessing a keyword by name.
                for kw_id in self.moves_merge.photo_keyword_ids_matching(
                        losing_id, match_key):
                    source = keyword_source_max(
                        self.moves_merge.photo_keyword_source(losing_id, kw_id))
                    self.tag_photo(surviving_id, kw_id, source=source, _commit=False)
                    self.species_provenance.carry(losing_id, surviving_id, keyword_id=kw_id)
            elif newest_loser["change_type"] == "keyword_remove":
                for kw_id in self.moves_merge.photo_keyword_ids_matching(
                        surviving_id, match_key):
                    self.untag_photo(surviving_id, kw_id, _commit=False)

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
        time so tests that patch ``db.commit_with_retry`` /
        ``db.execute_with_retry`` still apply. The façade methods the
        listing reads consult (``repositories.photos.FACADE_METHODS``: folder
        subtrees, the rules engine, the location-status filter, the
        collection repository factory) are bound from this instance, so
        patches of those ``Database`` methods still reach them.
        """
        from repositories.photos import PhotoRepository

        return PhotoRepository(
            self.conn,
            self._ws_id,
            chunk_size=_SQLITE_PARAM_CHUNK_SIZE,
            filter_subject_chunk=self._FILTER_SUBJECT_CHUNK,
            photo_cols=self.PHOTO_COLS,
            photo_detail_cols=self.PHOTO_DETAIL_COLS,
            execute_with_retry=execute_with_retry,
            commit_with_retry=commit_with_retry,
            inclusive_date_to=_inclusive_date_to,
            keyword_token_clause=_keyword_token_clause,
            facade=self,
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
        ``_apply_winner_loser_merge``, stays here because it re-tags the
        winner through ``tag_photo``.
        """
        return self._duplicates_repository()

    def _apply_winner_loser_merge(self, winner_id, loser_ids, winner_flag=None, *, manual_review=False):
        """Merge rating/keywords from losers onto winner, then flag losers
        as rejected. Single transaction. Shared between the resolver-based
        ``duplicate_resolution.apply_duplicate_resolution`` and the
        user-driven ``bulk_resolve_by_folder`` and ``keep_duplicate_copy``.

        ``winner_flag``, when given, is written to the winner in the same
        transaction (``keep_duplicate_copy`` un-rejects the copy it keeps).
        Only explicit user choices publish manual flag attribution; automatic
        resolution never publishes new manual confirmation.
        """
        from duplicates import merge_metadata
        from repositories.species_provenance import evidence_for_photo_copy

        repo = self._duplicates_repository()
        winner_meta = repo.photo_metadata(winner_id)
        loser_metas = [repo.photo_metadata(lid) for lid in loser_ids]
        merge = merge_metadata(winner_meta, loser_metas)

        with repo.transaction():
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
                self.tag_photo(
                    winner_id, kw_id, source=merged_source, _commit=False,
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

    # Columns to return in photo list queries (excludes large fields)
    PHOTO_COLS = """id, folder_id, filename, extension, file_size, file_mtime, xmp_mtime,
                    timestamp, width, height, rating, flag, thumb_path, sharpness,
                    subject_sharpness, subject_size, quality_score,
                    latitude, longitude, companion_path, working_copy_path,
                    wildlife_excluded, miss_no_subject, miss_clipped, miss_oof,
                    camera_make, camera_model, iso, lens, focal_length, aperture"""

    # Columns for single-photo detail queries (includes exif_data JSON +
    # eye-focus fields consumed by the review lightbox's crosshair overlay)
    PHOTO_DETAIL_COLS = (
        PHOTO_COLS
        + ", exif_data, eye_x, eye_y, eye_conf, eye_tenengrad,"
        + " working_copy_failed_at, working_copy_failed_mtime,"
        + " working_copy_failed_source"
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

    @staticmethod
    def _append_location_status_filter(conditions, location_status):
        """Add a Browse coordinate-source predicate using the photo alias ``p``.

        ``exif`` means the original photo has a complete EXIF coordinate pair.
        ``assigned`` means EXIF GPS is absent or incomplete but a linked,
        structured location supplies a complete pair. ``none`` means neither
        source can place the photo on the map.
        """
        if location_status is None:
            return
        assigned_exists = """EXISTS (
            SELECT 1 FROM photo_keywords pk_location_status
            JOIN keywords k_location_status
              ON k_location_status.id = pk_location_status.keyword_id
            WHERE pk_location_status.photo_id = p.id
              AND k_location_status.type = 'location'
              AND k_location_status.latitude IS NOT NULL
              AND k_location_status.longitude IS NOT NULL
        )"""
        no_exif = "(p.latitude IS NULL OR p.longitude IS NULL)"
        if location_status == "exif":
            conditions.append(
                "p.latitude IS NOT NULL AND p.longitude IS NOT NULL"
            )
        elif location_status == "assigned":
            conditions.append(f"{no_exif} AND {assigned_exists}")
        elif location_status == "none":
            conditions.append(f"{no_exif} AND NOT {assigned_exists}")
        else:
            raise ValueError(
                "location_status must be 'exif', 'assigned', or 'none'"
            )

    def _location_repository(self) -> LocationRepository:
        """Build the locations repository on this connection.

        Photos and keywords are catalog-wide; the few workspace-scoped
        methods resolve the active workspace through ``self._ws_id`` at the
        point the original code did, so building the repository never raises.
        The façade methods the map reads and the root merge call (folder
        subtrees, the rules query builder, ``_merge_keyword_into``) are
        handed over bound, so patches of them keep reaching the moved SQL.
        """
        from repositories.locations import LocationRepository

        return LocationRepository(
            self.conn,
            self._ws_id,
            chunk_size=_SQLITE_PARAM_CHUNK_SIZE,
            photo_date_asc_order=_PHOTO_DATE_ASC_ORDER,
            folder_subtree_ids=self.get_folder_subtree_ids,
            build_query_from_rules=self._build_query_from_rules,
            merge_keyword_into=self._merge_keyword_into,
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
        self, photo_id: int, excluded: bool, verify_workspace: bool = True, _commit: bool = True
    ) -> None:
        """Set whether a photo is excluded from wildlife detection/classification.

        ``_commit=False`` leaves the write in the caller's open transaction.
        """
        if verify_workspace:
            self._verify_photo_in_workspace(photo_id)
        self._photo_review_repository().set_wildlife_excluded(
            photo_id, excluded, _commit=_commit,
        )

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
        _commit: bool = True,
    ) -> dict[str, Any] | None:
        """Set or clear a non-destructive edit recipe for a photo.

        Returns the normalized recipe dict, or None when the provided recipe is
        a no-op and the stored row was cleared. Pass ``_commit=False`` to include
        the write in a caller-owned batch transaction.
        """
        if verify_workspace:
            self._verify_photo_in_workspace(photo_id)
        return self._edits_repository().set_photo_recipe(
            photo_id, recipe, _commit=_commit
        )

    def clear_photo_edit_recipe(self, photo_id: int, verify_workspace: bool = True) -> bool:
        """Remove a photo's edit recipe. Returns True if a row was removed."""
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
        The retry helpers are read from this module at call time so tests that
        patch ``db.commit_with_retry`` / ``db.execute_with_retry`` still apply.
        """
        from repositories.caches import CachesRepository

        return CachesRepository(
            self.conn,
            execute_with_retry=execute_with_retry,
            commit_with_retry=commit_with_retry,
        )

    @property
    def caches(self) -> CachesRepository:
        """The preview-cache and offline-original domain: ``db.caches.preview_get(...)``.

        A domain accessor, not a cached attribute: every access builds a fresh
        repository through ``_caches_repository``, exactly as a forwarding
        wrapper called at that moment would, so the connection and the
        module's ``execute_with_retry`` / ``commit_with_retry`` (which tests
        patch) are resolved per use. Do not hold the returned repository
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
        workspace id. ``commit_with_retry`` is read from this module at call
        time so a test that patches ``db.commit_with_retry`` still applies.
        """
        from repositories.working_copies import WorkingCopyRepository

        return WorkingCopyRepository(self.conn, commit_with_retry=commit_with_retry)

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
        resolves it. ``commit_with_retry`` is read from this module at call
        time so tests that patch ``db.commit_with_retry`` still apply, and
        the storage cleanup deletes receive ``_safe_remove_mask_file`` (the
        masks-directory containment check) as ``remove_file``.
        """
        from repositories.masks_features import MasksFeaturesRepository

        return MasksFeaturesRepository(
            self.conn,
            self._ws_id,
            commit_with_retry=commit_with_retry,
            remove_file=self._safe_remove_mask_file,
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

        return CullingRepository(self.conn, self.effective_config)

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

    def _safe_remove_mask_file(self, path):
        """``os.remove`` ``path`` only if it resolves inside the masks
        directory. The user-triggerable storage cleanup endpoints feed
        ``photo_masks.path`` straight into this; without the realpath
        containment check, a corrupted or migrated row pointing at
        ``/etc/...`` could cause arbitrary file deletion. Mirrors the
        defense already in place on ``/api/masks/<pid>/<variant>.png``.
        """
        if not path:
            return
        masks_dir = self._masks_dir_real()
        if masks_dir is None:
            log.warning(
                "Refusing to remove mask file %s (no masks dir resolved)",
                path,
            )
            return
        try:
            abs_path = os.path.realpath(path)
        except OSError:
            log.warning("Failed to resolve mask path %s", path)
            return
        if not (abs_path == masks_dir
                or abs_path.startswith(masks_dir + os.sep)):
            log.warning(
                "Refusing to remove mask file %s outside masks dir %s",
                path, masks_dir,
            )
            return
        try:
            if os.path.isfile(abs_path):
                os.remove(abs_path)
        except OSError:
            log.warning("Failed to remove mask file %s", abs_path)

    # -- Keywords --

    @property
    def keywords(self) -> KeywordRepository:
        """Keyword rows: ``db.keywords.name_of(keyword_id)``, ``db.keywords.ensure_default_genres()`` and friends.

        A domain accessor, not a cached attribute: every access builds a
        fresh repository, whose scoped methods resolve the workspace at use.
        Do not retain one across a workspace switch. The methods the keyword
        repositories bind from the façade by name (``FACADE_METHODS``:
        ``add_keyword``, ``resolve_species_display_name`` ...) keep their
        ``Database`` wrappers, as does ``untag_photo`` beside ``tag_photo``.
        """
        return self._keyword_repository()

    def _keyword_repository(self) -> KeywordRepository:
        """Build the keyword repository on this connection.

        The active workspace is resolved lazily (``Database._ws_id`` is
        passed as a resolver), so methods keep raising at the point they
        always did. Every façade method a moved body calls is handed over
        bound, under its ``Database`` name, so monkeypatches keep reaching
        the moved code; the ``db`` module names the bodies read
        (``resolve_import_alias``, ``KEYWORD_TYPES``) are read here, at call
        time.
        """
        from repositories.keywords import KeywordRepository

        return KeywordRepository(
            self.conn,
            self._ws_id,
            keyword_types=KEYWORD_TYPES,
            resolve_import_alias=resolve_import_alias,
            facade=self,
        )

    def _keyword_taxon_repository(self) -> KeywordTaxonRepository:
        """Build the keyword species-name and taxon-link repository on this connection.

        Catalog-wide: nothing in it needs the active workspace. The façade
        methods its bodies call are handed over bound, under their
        ``Database`` names; the detection sentinel and the taxon-name variant
        helper are read here, at call time.
        """
        from repositories.keyword_taxa import KeywordTaxonRepository

        return KeywordTaxonRepository(
            self.conn,
            detect_case_convention_sentinel=_DETECT_CASE_CONVENTION,
            taxon_lookup_variants=_taxon_lookup_variants,
            facade=self,
        )

    @property
    def keyword_taxa(self) -> KeywordTaxonRepository:
        """Species names and taxon links of keywords: ``db.keyword_taxa.mark_species(taxonomy)`` and friends.

        A domain accessor, not a cached attribute: every access builds a
        fresh repository through ``_keyword_taxon_repository``. Catalog-wide,
        so it needs no active workspace. The lookups other keyword
        repositories bind by name (``detect_keyword_case_convention``,
        ``resolve_species_display_name``, ``_species_root_name_for_taxon``,
        ``_lookup_taxon_id_for_keyword``, ``_resolve_species_by_lineage``)
        keep their ``Database`` wrappers.
        """
        return self._keyword_taxon_repository()

    def _keyword_tree_repository(self) -> KeywordTreeRepository:
        """Build the keyword-tree repository on this connection.

        The active workspace is resolved lazily (``Database._ws_id`` is
        passed as a resolver). The façade methods its bodies call (the
        duplicate-merge pass, the dependent-rename carry and the
        species-curation renames) are handed over bound, under their
        ``Database`` names, so monkeypatches keep reaching the moved code.
        """
        from repositories.keyword_tree import KeywordTreeRepository

        return KeywordTreeRepository(
            self.conn, self._ws_id, chunks=_chunks, facade=self,
        )

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
        catalog-wide, so it needs no active workspace. ``untag_photo`` keeps
        its ``Database`` wrapper beside ``tag_photo``.
        """
        return self._photo_keyword_repository()

    @property
    def identity_alternatives(self) -> IdentityAlternativesRepository:
        """Unresolved candidate identities; never confirmed species keywords."""
        from repositories.identity_alternatives import IdentityAlternativesRepository
        return IdentityAlternativesRepository(
            self.conn, self._ws_id, lookup_taxon_id=self._lookup_taxon_id_for_keyword,
            keyword_source_conflict_sql=KEYWORD_SOURCE_CONFLICT_SQL,
        )

    def _photo_species_repository(self) -> PhotoSpeciesRepository:
        """Build the photo-species reads on this connection.

        The active workspace is resolved lazily (``Database._ws_id`` is passed
        as a resolver), so ``get_accepted_species`` raises at the point it
        always did.
        """
        from repositories.photo_species import PhotoSpeciesRepository

        return PhotoSpeciesRepository(self.conn, self._ws_id, chunks=_chunks)

    @property
    def photo_species(self) -> PhotoSpeciesRepository:
        """The species photos carry: ``db.photo_species.get_species_for_photos(ids)`` and friends.

        A domain accessor, not a cached attribute: every access builds a
        fresh repository, whose scoped method resolves the workspace at use.
        Do not retain one across a workspace switch.
        ``get_photos_with_equivalent_species`` keeps its ``Database`` wrapper:
        ``repositories/keyword_provenance.py`` binds it by name.
        """
        return self._photo_species_repository()

    def _keyword_provenance_repository(self) -> KeywordProvenanceRepository:
        """Build the keyword-provenance repository on this connection.

        It holds the ``photo_keywords`` writers that create or converge an
        association (``tag_photo``, ``_merge_keyword_into``,
        ``link_keyword_to_place``) and the
        flows that call them mid-transaction (``_upsert_one_keyword``,
        ``accept_prediction``). The active
        workspace is resolved lazily (``Database._ws_id`` is passed as a
        resolver). Every façade method a moved body calls -- including these
        methods' calls to each other -- is handed over bound, under its
        ``Database`` name, so monkeypatches keep reaching the moved code; the
        class attributes and ``db`` module names the bodies read (``_chunks``,
        the provenance fold and ``KEYWORD_SOURCE_MANUAL``) are read here, at
        call time.
        """
        from repositories.keyword_provenance import KeywordProvenanceRepository

        return KeywordProvenanceRepository(
            self.conn,
            self._ws_id,
            chunks=_chunks,
            keyword_source_manual=KEYWORD_SOURCE_MANUAL,
            keyword_source_conflict_sql=KEYWORD_SOURCE_CONFLICT_SQL,
            keyword_source_max_sql=keyword_source_max_sql,
            keyword_source_max=keyword_source_max,
            decided_prediction_statuses=self.DECIDED_PREDICTION_STATUSES,
            facade=self,
        )

    @property
    def keyword_provenance(self) -> KeywordProvenanceRepository:
        """The ``photo_keywords`` writers without a ``Database`` wrapper: ``db.keyword_provenance.link_keyword_to_place(...)``.

        ``tag_photo``, ``_merge_keyword_into`` and ``accept_prediction`` keep
        theirs: the provenance contract, the repositories' ``FACADE_METHODS``
        and ``test_route_contract`` find them by those names.

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

    def detect_keyword_case_convention(self) -> str | None:
        """Detect the casing convention used by existing species keywords.

        Returns:
            'title' if most are Title Case (e.g. "Black Phoebe")
            'lower' if most are lowercase after first word (e.g. "Black phoebe")
            'upper' if most are ALL CAPS
            None if not enough data to determine
        """
        return self._keyword_taxon_repository().detect_case_convention()

    def _apply_case_convention(self, name, convention):
        """Apply a casing convention to a species name."""
        if convention == "lower":
            # First word capitalized, rest lowercase: "Black phoebe"
            words = name.split()
            if len(words) > 1:
                first = self._sentence_case_first_word(words[0])
                return first + " " + " ".join(w.lower() for w in words[1:])
            return self._sentence_case_first_word(name)
        elif convention == "title":
            # Title Case: "Black Phoebe"
            return name.title()
        return name

    @staticmethod
    def _sentence_case_first_word(word):
        """Capitalize a first word without mangling mixed-case eponyms.

        ``str.capitalize()`` lowercases everything after the first letter,
        so a classifier label like ``McKay's bunting`` would come out
        ``Mckay's bunting``. Preserve existing internal casing unless the
        word is ALL CAPS (shouty label files), which sentence-cases.
        """
        if not word:
            return word
        has_case = any(ch.lower() != ch.upper() for ch in word)
        if has_case and word == word.upper():
            chars = list(word.lower())
            for idx, ch in enumerate(chars):
                if ch.lower() != ch.upper():
                    chars[idx] = ch.upper()
                    break
            return "".join(chars)
        return word[0].upper() + word[1:]

    def resolve_species_display_name(
        self,
        name: str,
        apply_case_convention: bool = True,
        case_convention: object = _DETECT_CASE_CONVENTION,
    ) -> str:
        """Predict the stored species name that add_keyword(is_species=True) would use.

        Species relabel endpoints snapshot curation dst_existed before
        add_keyword actually runs, so they need to know the final stored
        spelling in advance. Cases:

        1. A single species-bearing root keyword row matches (SQLite
           ASCII NOCASE) → preserve that stored spelling; existing
           curation rows key on it. A non-species general homonym with
           the same NOCASE key (e.g. a hand-tagged ``Common Waxbill``
           general alongside a taxonomy ``Common waxbill``) is ignored
           here — add_keyword's typed lookup prefers the taxonomy row,
           so returning the general's spelling would key curation onto a
           string add_keyword never stores.
        2. Multiple species-bearing stored spellings match the same
           NOCASE key (intentional homonyms — e.g. legacy general
           ``Robin`` (is_species=1) alongside taxonomy ``robin``) →
           preserve the caller's spelling. Silently picking one would
           route bucket/curation writes across genuinely different
           species rows, so the eligibility check (which compares
           ``bucket["species"]`` to this result exactly) would then
           reject requests coming from the other homonym's bucket.
           Bucket collection, API parse, and DB setters all funnel
           through this call, so preserving keeps them agreeing on the
           same string. (Callers that need the exact spelling
           add_keyword will land on after promotion — e.g. relabel
           snapshots — apply add_keyword's ORDER BY themselves.)
        3. No species-bearing row but a non-species general row shares
           the NOCASE key → return that general's spelling.
           add_keyword(is_species=True) would find and promote that row
           in place, keeping its name, so curation must key on the same
           string.
        4. No matching root keyword row → apply the same species-casing
           convention that add_keyword applies for new species keywords,
           so pre-existing curation from predictions (which inserted
           `Black Phoebe`) is matched even when the request submits
           `black phoebe`.

        A hierarchy leaf whose spelling differs from its root alias is also
        canonicalized through a unique linked taxon. This keeps accepted
        hierarchy buckets and species curation on the same root key while the
        photo itself retains the hierarchy-bearing keyword association.

        Pass ``apply_case_convention=False`` to skip case 4 and return the
        caller's own spelling when nothing is stored. Callers that *display*
        the result want "the spelling some row actually holds, or the name I
        gave you" — case 4 mints a spelling (``Bubulcus ibis`` ->
        ``Bubulcus Ibis``) that no row anywhere carries, which is fine for
        predicting where add_keyword will land but wrong to show a user as
        though a model or the catalog had said it.
        """
        return self._keyword_taxon_repository().resolve_species_display(
            name, apply_case_convention=apply_case_convention, case_convention=case_convention,
        )

    def species_case_convention(self) -> str | None:
        """The casing ``add_keyword`` would apply to a brand-new species name.

        The configured override wins; otherwise it is detected from the
        existing species keywords. Both readings cost a config load and a
        scan of every species keyword, which is why bulk callers resolve it
        once and hand it back to ``resolve_species_display_name`` instead of
        paying for it per name.
        """
        override = settings_source.get("keyword_case")
        if override and override != "auto":
            return override
        return self.detect_keyword_case_convention()

    def _species_root_name_for_taxon(self, taxon_id):
        """Canonical root keyword spelling for a species taxon, if any.

        ``resolve_species_display_name`` uses the same lookup to route
        curation keys through the canonical root when only a hierarchy
        alias is stored (e.g. a leaf ``Desert Verdin`` after
        the retired duplicate-species repair detached the top-level
        ``Verdin``). Callers with the taxon id in hand can skip the
        name-based lookup and go straight to the root row.
        """
        return self._keyword_taxon_repository().species_root_name_for_taxon(taxon_id)

    def _lookup_taxon_id_for_keyword(
        self, name, prefer_species=False, species_only=False,
    ):
        """Return the local taxa.id matching a keyword name, if any.

        When ``prefer_species`` is true, break ties in favor of a
        ``rank='species'`` taxon. A catalog can hold homonyms across ranks
        (for example a common species ``Puma`` alongside the genus
        ``Puma``); without a preference, an unordered ``LIMIT 1`` can bind
        an ``is_species=True`` keyword to the non-species taxon, and every
        downstream ``rank='species'`` filter (Life List, Compare, Explorer)
        then silently drops the photo even though the accept appeared to
        succeed.

        When ``prefer_species`` is true, the direct ``taxa`` match is only
        returned immediately if it is species-rank. Otherwise the
        ``taxa_common_names`` fallback is still consulted for a species-rank
        alternate name before the higher-rank direct hit wins.
        ``populate_taxa_db_from_json`` explicitly indexes alternate English
        names in ``taxa_common_names``, so accepting an alternate species
        label that collides with a genus/family can and does happen.

        ``species_only`` tightens ``prefer_species`` from "prefer" to
        "require": if no lookup variant surfaces a species-rank taxon,
        return ``None`` instead of falling back to the higher-rank hit.
        Explicit-species callers (``is_species=True`` inserts and rebinds
        along ``add_keyword``'s taxonomy path) must set this — the row
        gets stamped ``is_species=1`` unconditionally once ``taxon_id``
        is returned, and downstream rank readers restrict to
        ``t.rank = 'species' OR t.rank IS NULL``. Silently binding an
        accepted species to a genus/family would make the just-created
        tag invisible to Life List, Compare, Explorer, and highlight /
        preference eligibility; leaving ``taxon_id`` NULL keeps the
        ``rank IS NULL`` branch honoring the tag until a genuine
        species-rank match becomes available.

        General-keyword auto-detect callers (``is_species=False`` INSERT
        path, rename auto-promotion) must NOT set ``species_only``: they
        legitimately link general/typed keywords to family/genus taxa
        (e.g. a hand-tagged ``Penduline tits`` linked to the family
        taxon), and ``is_keyword_species`` already filters those out via
        ``taxon_rank`` for species-specific readers.
        """
        return self._keyword_taxon_repository().lookup_taxon_id(
            name, prefer_species=prefer_species, species_only=species_only,
        )

    def _add_source_species_keyword(self, name, source_taxon_id, parent_id=None, _commit=True):
        """Bind accepted source evidence without reassigning same-name tags."""
        return self._keyword_repository().add_source_species(
            name, source_taxon_id, parent_id=parent_id, _commit=_commit,
        )

    def add_keyword(
        self,
        name: str,
        parent_id: int | None = None,
        is_species: bool = False,
        kw_type: str | None = None,
        _commit: bool = True,
        source_taxon_id: int | None = None,
        _resolve_alias: bool = False,
    ) -> int:
        """Insert a keyword. Returns existing id if duplicate (case-insensitive).

        If a keyword with the same name but different casing exists, reuses
        the existing one rather than creating a duplicate.

        For new species keywords, auto-detects the user's casing convention
        from existing keywords and applies it (unless overridden by config).

        Args:
            kw_type: Optional explicit keyword type. Must be one of
                     ``KEYWORD_TYPES`` if provided. When ``None``, the type is
                     auto-detected (``taxonomy`` for species or names matching
                     a known taxon, otherwise ``general``).
            _commit: If False, skip the internal commit (caller is responsible
                     for committing the transaction).
            source_taxon_id: Explicit iNaturalist ID for a species keyword;
                     bypass common-name inference and reuse only that identity.
            _resolve_alias: Import callers opt in for leaf keywords only.
                     Manual additions must not inherit imported keyword aliases,
                     and a leaf alias must not relocate a new parent chain.
        """
        return self._keyword_repository().add(
            name, parent_id=parent_id, is_species=is_species, kw_type=kw_type, _commit=_commit, source_taxon_id=source_taxon_id, _resolve_alias=_resolve_alias,
        )

    def _restore_misclassified_location_ancestor(
        self, keyword_id, allow_leaf=False,
    ):
        """Restore one legacy taxonomy row that is structurally a location.

        Before explicit keyword types were protected during taxonomy marking,
        geographic homonyms such as ``California`` could be changed from a
        location into taxonomy. A taxonomy row below a location parent that
        sits above a location descendant connected only through other
        taxonomy rows is an administrative location-tree node. A place-
        bearing row is also unambiguously a location, regardless of where
        it sits. ``allow_leaf`` is reserved for an explicit Google
        administrative-component match, where the place response supplies
        the evidence a coordless leaf lacks during startup.

        Root rows (``parent_id IS NULL``) lack a location parent proving
        they belong to a legacy location chain, so a location descendant
        alone is not enough evidence — a genuine taxonomy root with a
        mixed-hierarchy location descendant (e.g. ``Ardea`` → ``Backyard``)
        would otherwise be retyped and lose its taxonomy metadata. A bare
        name collision from ``allow_leaf`` is also not enough: a genuine
        root taxonomy homonym such as ``Turkey`` (the species) would be
        clobbered when the user later saves the Google country ``Turkey``.
        SQLite permits multiple ``parent_id IS NULL`` rows with the same
        name, so the caller can safely insert a separate location root
        instead. Roots therefore need stronger structural evidence: an
        existing ``place_id`` or a location descendant reachable through
        taxonomy rows. Clear stale taxonomy metadata and restore the
        row's type when evidence is sufficient.
        """
        return self._location_repository().restore_misclassified_ancestor(
            keyword_id, allow_leaf=allow_leaf,
        )

    def merge_taxonomy_root_twins(self) -> int:
        """Fold each top-level species twin into the row filed in the tree.

        The pairs are ``KeywordTreeRepository.taxonomy_root_twins``. Both rows
        carry the same name, so the sidecars already hold the survivor's
        flat keyword and the merge queues no XMP change. All-or-nothing;
        returns the number of rows merged away.
        """
        merged = 0
        with self.transaction():
            if not self.in_transaction:
                self.begin_immediate()
            for root_id, placed_id in self.keyword_tree.taxonomy_root_twins():
                merged += self._merge_keyword_into(
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
        repaired, merged_roots = (
            self._location_repository().repair_misclassified_ancestors(
                restore_ancestor=self._restore_misclassified_location_ancestor,
            )
        )
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

    @staticmethod
    def _location_component_rank(component):
        """Return a broad-to-narrow rank for useful location components."""
        types = component.get("types") if isinstance(component, dict) else []
        if not isinstance(types, list):
            return None
        ranks = [
            _LOCATION_COMPONENT_RANKS[t]
            for t in types
            if isinstance(t, str) and t in _LOCATION_COMPONENT_RANKS
        ]
        if not ranks:
            return None
        return min(ranks)

    def _location_parent_components(self, components, leaf_name="", leaf_types=None):
        """Return address components suitable for keyword parents.

        Google address components can include street numbers, routes, postal
        codes, rooms, and other address fragments. Those are useful for a
        formatted address, but they make noisy keyword parents such as "1200"
        or "94107". Keep administrative/geographic levels only and sort by
        component type so postal-code placement in Google's response cannot
        become the root of the hierarchy.
        """
        leaf_type_set = (
            {t for t in (leaf_types or []) if isinstance(t, str)}
            if isinstance(leaf_types, list)
            else set()
        )
        candidates = []
        seen = set()
        leaf_norm = leaf_name.strip().casefold() if isinstance(leaf_name, str) else ""
        for index, comp in enumerate(components or []):
            if not isinstance(comp, dict):
                continue
            name = (comp.get("name") or comp.get("long_name") or "").strip()
            if not name:
                continue
            rank = self._location_component_rank(comp)
            if rank is None:
                continue
            types = tuple(t for t in comp.get("types", []) if isinstance(t, str))
            candidates.append((rank, index, name, types))

        leaf_component = None
        if leaf_norm and leaf_type_set:
            leaf_matches = [
                item for item in candidates
                if item[2].casefold() == leaf_norm
                and any(
                    t in leaf_type_set and t in _LOCATION_COMPONENT_RANKS
                    for t in item[3]
                )
            ]
            if leaf_matches:
                # If a leaf has the same text as multiple admin levels
                # ("New York" city and state), drop only the narrowest
                # matching component and keep the broader parent.
                leaf_component = max(leaf_matches, key=lambda item: item[0])

        normalized = []
        for candidate in candidates:
            rank, index, name, _types = candidate
            if leaf_component is not None and candidate == leaf_component:
                continue
            key = (rank, name.casefold())
            if key in seen:
                continue
            seen.add(key)
            normalized.append((rank, index, {"name": name}))
        normalized.sort(key=lambda item: (item[0], item[1]))
        return [item[2] for item in normalized]

    def _upsert_location_parent_chain(
        self,
        components,
        leaf_name="",
        leaf_types=None,
        exclude_keyword_id=None,
    ):
        """Upsert a chain of parent location keywords from ``address_components``.

        Walks broadest → narrowest, returning the list of visited keyword ids
        in broadest → narrowest order. Returns an empty list if ``components``
        is empty / all entries lack a name. The deepest (narrowest) parent is
        ``chain[-1]`` if non-empty. ``exclude_keyword_id`` prevents an
        existing selected place from being reused as its own parent when its
        display name differs from the matching address component. Caller is
        responsible for the surrounding transaction.
        """
        chain: list[int] = []
        parent_id = None
        for comp in self._location_parent_components(components, leaf_name, leaf_types):
            if not comp.get("name"):
                continue
            component_id = self.keyword_provenance.upsert_one_keyword(
                name=comp["name"],
                parent_id=parent_id,
                place_id=None,
                latitude=None,
                longitude=None,
                reuse_location_component=True,
            )
            if component_id == exclude_keyword_id:
                continue
            parent_id = component_id
            chain.append(component_id)
        return chain

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
        leaf id and does not create duplicate rows.
        """
        if not details.get("place_id"):
            raise ValueError("upsert_place_chain requires details['place_id']")

        name = details.get("name", "")
        lat = details.get("lat")
        lng = details.get("lng")
        components = details.get("address_components") or []

        repo = self._location_repository()
        with repo.transaction():
            existing_leaf = repo.find_place_keyword(details["place_id"])
            chain = self._upsert_location_parent_chain(
                components,
                leaf_name=name,
                leaf_types=details.get("types"),
                exclude_keyword_id=(
                    existing_leaf["id"] if existing_leaf is not None else None
                ),
            )
            parent_id = chain[-1] if chain else None
            leaf_id = self.keyword_provenance.upsert_one_keyword(
                name=name,
                parent_id=parent_id,
                place_id=details["place_id"],
                latitude=lat,
                longitude=lng,
                reuse_location_component=(
                    self._location_component_rank({
                        "types": details.get("types"),
                    }) is not None
                ),
            )
        return leaf_id

    def set_photo_location(self, photo_id: int, leaf_keyword_id: int) -> None:
        """Set ``photo_id``'s location to ``leaf_keyword_id``.

        Removes any existing ``type='location'`` keyword links for the photo,
        then inserts the new link. Atomic.

        Raises ``ValueError`` if ``leaf_keyword_id`` does not exist or its
        keyword type is not ``'location'`` — otherwise the DELETE would strip
        real location links and replace them with a non-location link that
        :meth:`locations.clear_photo` could not clean up.
        """
        repo = self._location_repository()
        repo.require_location_keyword(leaf_keyword_id)
        with repo.transaction():
            repo.delete_photo_links(photo_id)
            # Every caller of this method is a person assigning a place
            # (map click, text entry, batch apply, EXIF-derived confirm), so
            # the association is user-authored.
            self.tag_photo(
                photo_id, leaf_keyword_id,
                source=KEYWORD_SOURCE_MANUAL, _commit=False,
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
        with self._location_repository().transaction():
            return self.keyword_provenance.upsert_one_keyword(
                name=stripped,
                parent_id=None,
                place_id=None,
                latitude=None,
                longitude=None,
            )

    def _merge_duplicate_keywords_pass(self, ws):
        """Convergence loop for merge_duplicate_keywords. Caller commits."""
        total_merged = 0
        while True:
            rows = self._keyword_tree_repository().duplicate_scope_rows(ws)
            grouped = {}
            for row in rows:
                key = keyword_match_key(row["name"])
                if not key:
                    continue
                species_bearing = (
                    row["type"] == "taxonomy" or row["is_species"] == 1
                )
                grouped.setdefault(
                    (key, row["parent_id"], row["type"], species_bearing),
                    [],
                ).append(row)
            dupes = [
                group for group in grouped.values()
                if len({row["id"] for row in group}) > 1
            ]
            if not dupes:
                break

            for group in dupes:
                # Prefer an already-clean spelling when one exists; otherwise
                # keep the earliest row to preserve the old case-only behavior.
                ordered = sorted(
                    group,
                    key=lambda row: (
                        normalize_keyword_display(row["name"]) != row["name"],
                        row["id"],
                    ),
                )
                keep_id = ordered[0]["id"]
                all_ids = [row["id"] for row in ordered]

                # A prior group in this pass can recursively delete ids
                # from later groups: merging duplicate parents cascades
                # into their duplicate children, so a child group whose
                # keep_id was one of those children is now stale. Skip
                # groups whose keep_id is gone (the next while iteration
                # re-queries and picks a fresh survivor) and drop dead
                # remove_ids so we don't UPDATE photo_keywords toward a
                # non-existent FK target.
                alive = self._keyword_tree_repository().live_ids(all_ids)
                if keep_id not in alive:
                    continue
                remove_ids = [x for x in all_ids if x != keep_id and x in alive]

                self.keyword_tree.normalize_row_name(keep_id)
                for rid in remove_ids:
                    total_merged += self._merge_keyword_into(
                        rid, keep_id, pending_source_only=True,
                    )

        return total_merged

    def _rename_keyword_dependents(self, keyword_id, old_name, new_name):
        """Carry a keyword row's rename into every string that mirrors its name.

        Pending sidecar edits and the species curation tables store the
        keyword's spelling rather than its id, so a row renamed without this
        leaves an unsynced ``keyword_add`` writing the old word into XMP and
        drops starred photos out of the highlight/life-list queries, which
        compare those strings exact against ``keywords.name``.

        Scoped to photos that actually carry ``keyword_id`` (and, for pending
        changes, the workspaces those (photo, keyword) tags belong to), so a
        separate same-spelling keyword row elsewhere in the DB is never
        rewritten by side effect. Safe to call either side of the
        ``keywords`` UPDATE: only ``photo_keywords`` is read, and a name
        change does not touch it. Caller commits.
        """
        return self._keyword_tree_repository().rename_dependents(keyword_id, old_name, new_name)



    def _reparent_disambiguated(self, child, dst_id, new_name):
        """Move a colliding child under ``dst_id`` under a free name.

        The three collision branches in ``_merge_keyword_into`` all preserve
        the migrating row rather than folding it away, which means they all
        rename it -- and a rename is never just the ``keywords`` row. Pending
        sidecar edits and the species curation tables key on the spelling, so
        skipping the dependent migration leaves an unsynced ``keyword_add``
        writing the retired name into XMP and drops starred photos out of the
        highlight and life-list queries. Caller commits.
        """
        return self._keyword_tree_repository().reparent_disambiguated(child, dst_id, new_name)

    def _merge_keyword_into(self, src_id, dst_id, *, pending_source_only=False):
        """Merge keyword ``src_id`` into ``dst_id`` and delete the source.

        Moves photo associations, then reparents the source's children onto
        the destination. A child whose name matches an existing sibling
        under the destination merges into that sibling recursively only when
        both share the same ``type`` — "Birds > Heron" and "birds > Heron"
        must converge on one Heron. Match uses ``keyword_match_key`` (ASCII
        case fold on the display-normalized name), the same key every lookup
        and dedup path uses, so a case-only variant is a collision even
        though SQLite's UNIQUE(name, parent_id) index is BINARY — an
        unchecked reparent would otherwise leave two semantic peers no
        import could tell apart. When the existing sibling has a different
        ``type`` (e.g. a 'general' Macro vs. a 'genre' Macro), the dedup
        boundary is (LOWER(name), parent_id, type), so they are NOT
        duplicates; preserve both by disambiguating the migrating child's
        name with an id suffix. Cycles are impossible: parent_id chains are
        acyclic by construction.
        Non-link metadata (is_species, coordinates, taxon_id,
        source_taxon_id) folds into the destination when it lacks its own,
        so deleting the source can't silently drop species/location info
        that only the duplicate carried.

        Rewrites pending_changes so an unsynced keyword_add/keyword_remove
        queued under the source spelling points at the surviving name after
        the merge. Without this, the merge deletes the source row but leaves
        the pending change referring to the old spelling, so the next
        ``sync_to_xmp`` writes a keyword the DB no longer has.

        Explicit merges set ``pending_source_only`` so only photos currently
        carrying the source have their pending edits rewritten. A photo that
        already removed the source must still remove that old spelling from
        its sidecar, even when it also carries the destination keyword.

        Returns the number of keyword rows merged away (>= 1). Caller
        commits.
        """
        return self._keyword_provenance_repository().merge_keyword_into(
            src_id, dst_id, pending_source_only=pending_source_only,
        )

    def tag_photo(
        self,
        photo_id: int,
        keyword_id: int,
        source: str | None = KEYWORD_SOURCE_MANUAL,
        _commit: bool = True,
        *,
        provenance: dict[str, Any] | None = None,
        species_provenance: str | None = None,
        _record_provenance: bool = False,
    ) -> None:
        """Associate a keyword with a photo.

        Args:
            source: Durable provenance for the association. The stamp lives on
                    the row, so authorship survives ``_prune_edit_history``
                    discarding the ``keyword_add`` entry.

                    Defaults to ``KEYWORD_SOURCE_MANUAL`` because that is the
                    fail-safe answer: a call site that forgets to declare
                    provenance leaves an association that retirement passes
                    refuse to delete, rather than one they silently erase.
                    Only the sidecar readers (scanner, XMP reconcile) may pass
                    ``KEYWORD_SOURCE_UNKNOWN``, and they must do it
                    explicitly — the contract test enumerates them.

                    An existing stamp is never downgraded: the upsert stores
                    the lattice max of the existing and incoming values, so
                    re-tagging with ``KEYWORD_SOURCE_UNKNOWN`` keeps a
                    recorded ``'manual'`` (and a future ``'accept'`` re-tag
                    would too).
            _record_provenance: Capture attribution only for an undoable edit.
                    Import and merge writes leave this false.
            provenance: dict of species evidence the caller is recording
                    deliberately (a reviewer's identification). Snapshotted
                    for undo and set after the row is tagged, so it survives
                    the ON CONFLICT fold that would otherwise keep an
                    existing value.
            species_provenance: pre-serialized evidence JSON forwarded during
                    a merge (loser → winner). Unlike ``provenance`` it does
                    not snapshot or overwrite -- the shared conflict clause
                    keeps the surviving row's evidence over the incoming
                    one, so the loser's row is only applied when the winner
                    had none.
            _commit: If False, skip the internal commit (caller is responsible
                     for committing the transaction).
        """
        if provenance is not None and _record_provenance:
            self.species_provenance.remember_before(photo_id, keyword_id, force=True)
        self._keyword_provenance_repository().tag(
            photo_id, keyword_id, source=source,
            species_provenance=species_provenance, _commit=False,
        )
        if provenance is not None:
            self.species_provenance.set(photo_id, keyword_id, provenance)
        if _commit:
            self.commit()

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

        return ExifSearchRepository(self.conn, commit_with_retry)

    @property
    def exif_search(self) -> ExifSearchRepository:
        """The EXIF search-text backfill: ``db.exif_search.index_batch(after_id, limit)``.

        A domain accessor, not a cached attribute: every access builds a fresh
        repository through ``_exif_search_repository``, so the connection and
        the module's ``commit_with_retry`` are read per use. The domain is
        catalog-wide, so it needs no active workspace.
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

    def untag_photo(
        self, photo_id: int, keyword_id: int, _commit: bool = True, *, _record_history: bool = False
    ) -> None:
        """Remove a keyword association from a photo.

        Stays a ``Database`` method beside ``tag_photo``: the structural
        guards that keep keyword re-tagging on the façade (moves/merge,
        species curation, predictions) find both writers by these names.

        Args:
            _commit: If False, skip the internal commit (caller is responsible
                     for committing the transaction).
            _record_history: Opt in only when the caller will record an
                     undoable keyword/species edit. Reconciliation, merge and
                     replay writers leave this false so no stale snapshots
                     can accumulate or leak into a later edit.
        """
        if _record_history:
            self.species_provenance.remember_before(photo_id, keyword_id)
        return self._photo_keyword_repository().untag(photo_id, keyword_id, _commit=_commit)

    def get_photos_with_equivalent_species(
        self,
        photo_ids: Collection[int],
        keyword_id: int,
        exclude_keyword_ids: Iterable[int] | None = None,
    ) -> set[int]:
        """Return submitted photo ids already carrying the target species.

        Species identity is the linked taxon when available, not a particular
        keyword row. This lets a hierarchical ``Birds|Verdin`` tag satisfy a
        later confirmation that resolved to the top-level Verdin keyword.
        Unlinked legacy species fall back to the normalized display name.

        When ``exclude_keyword_ids`` is provided, keyword rows with those ids
        are ignored during the match. Callers use this to look past rows that
        are about to be removed — for example, a same-taxon replacement where
        the "already carries this species" answer must reflect only the rows
        that will survive the mutation.
        """
        return self._photo_species_repository().get_photos_with_equivalent_species(
            photo_ids, keyword_id, exclude_keyword_ids=exclude_keyword_ids,
        )

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

    def set_species_representative(self, species: str, photo_id: int, _commit: bool = True) -> None:
        """Mark a photo as a representative for a species globally.

        Multiple photos can represent a species. Re-selecting an existing
        representative promotes it by assigning the newest selection order.
        A compatibility ``photo_preferences`` row is kept for older callers
        that still expect one main representative in the active workspace.
        """
        self.species_curation.set_photo_preference(
            "species_representative", species, photo_id, _commit=_commit
        )

    def rename_photo_preferences_species(
        self,
        old_species: str,
        new_species: str,
        photo_workspace_pairs: Collection[tuple[int, int]] | None = None,
        _commit: bool = True,
    ) -> int:
        """Rename stored representative-photo preferences across workspaces."""
        return self._species_curation_repository().rename_photo_preferences(
            old_species, new_species, photo_workspace_pairs=photo_workspace_pairs, _commit=_commit,
        )

    def rename_species_highlights_species(
        self,
        old_species: str,
        new_species: str,
        photo_workspace_pairs: Collection[tuple[int, int]] | None = None,
        _commit: bool = True,
    ) -> int:
        """Rename ordered species-highlight rows to a new species bucket.

        Companion to :meth:`rename_photo_preferences_species` for the
        ``species_highlights`` table. The rows carry a per-species ``rank``,
        so we can't just rewrite the ``species`` column — a bucket may
        already exist for ``new_species`` with its own ranks. Instead, each
        moved row is deleted from the old bucket and inserted at the end
        of the new bucket (``MAX(rank) + 1``) while preserving its
        old-bucket order. Rows whose photo already appears in the new
        bucket are dropped rather than duplicated.

        When ``photo_workspace_pairs`` is provided, only rows matching
        those ``(photo_id, workspace_id)`` pairs are moved (used by
        ``api_update_keyword`` so we only rebucket highlights for photos
        actually tagged with the renamed keyword). When omitted, all
        workspaces are rebucketed.

        Returns the count of highlight rows that landed in the new bucket
        (excludes rows dropped as duplicates).
        """
        return self._species_curation_repository().rename_highlights(
            old_species, new_species, photo_workspace_pairs=photo_workspace_pairs, _commit=_commit,
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
                    self._lookup_taxon_id_for_keyword(
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
                        self._merge_keyword_into(
                            keyword_id, peer["id"], pending_source_only=True,
                        )
                        self._keyword_tree_repository().commit()
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
        they keep raising at exactly the point they always did. The façade
        methods the moved bodies call are handed over bound, so
        monkeypatches of ``Database`` keep reaching the moved code, and the
        ``db`` module names the bodies read (``_chunks``,
        ``commit_with_retry``, ``log``, the auto-match marker, the top
        confidence expression) are read here, at call time.
        """
        from repositories.predictions import PredictionRepository
        from species_identity import SpeciesResolver

        return PredictionRepository(
            self.conn,
            self._ws_id,
            chunks=_chunks,
            commit_with_retry=commit_with_retry,
            auto_match_review_marker=AUTO_MATCH_REVIEW_MARKER,
            top_prediction_confidence_expr=_TOP_PREDICTION_CONFIDENCE_EXPR,
            decided_prediction_statuses=self.DECIDED_PREDICTION_STATUSES,
            facade=self,
            species_resolver=lambda: SpeciesResolver(db=self),
        )

    @property
    def predictions(self) -> PredictionRepository:
        """Classifier predictions: ``db.predictions.id_for_key(...)`` and friends.

        A domain accessor, not a cached attribute: every access builds a fresh
        repository through ``_prediction_repository``, so the connection, the
        lazily resolved workspace and the module's ``commit_with_retry`` are
        read per use. Only the prediction-decision writers keep ``Database``
        wrappers (``update_prediction_status``,
        ``update_predictions_status_by_photo``, ``ungroup_prediction``,
        ``set_review_status``, ``accept_subject_species``): ``test_route_contract``
        finds decision routes by those names.
        """
        return self._prediction_repository()

    # Row-level filterable fields: those the row's own values can prove.
    # ``species`` is intentionally excluded — the SQL ``species`` leaf
    # (``_build_query_from_rules``) matches confirmed photo keywords via
    # ``photo_keywords → keywords → taxa``, whereas ``pr.species`` on the
    # returned row holds the model's *proposed* species. Re-checking the
    # proposed species against a keyword filter would silently hide the
    # exact disagreement/refinement rows users would filter for
    # (e.g. a photo tagged ``Robin`` with a pending ``Sparrow`` prediction
    # would be selected by SQL and then dropped here).
    _PREDICTION_ROW_FIELDS = frozenset({
        "prediction_confidence",
        "prediction_status",
        "classifier_model",
        "taxonomy_kingdom",
        "taxonomy_phylum",
        "taxonomy_class",
        "taxonomy_order",
        "taxonomy_family",
        "taxonomy_genus",
        "needs_review",
    })

    def _relax_negated_prediction_leaves(self, rules):
        """Strip prediction-field leaves — and mixed subgroups containing
        them — that sit under a ``none`` group so
        ``get_predictions``'s photo-scoping subquery doesn't
        over-restrict on row-level predicates.

        ``_build_query_from_rules`` compiles a ``none`` group as
        photo-level ``NOT EXISTS(...)``. For a bare leaf like
        ``prediction_confidence >= 0.8`` that removes any photo with a
        sibling row above the threshold, even though the 0.10 sibling
        satisfies ``none(prediction_confidence >= 0.8)`` per row —
        dropping the leaf broadens the SQL to a no-op there and lets
        ``_filter_prediction_rows_by_rules`` evaluate the ORIGINAL
        tree per row.

        A mixed subgroup like ``none(all(rating >= 5, conf >= 0.8))``
        is trickier: stripping only the prediction leaf leaves
        ``none(all(rating >= 5))`` = ``NOT EXISTS(rating >= 5)``,
        which excludes every rating-5 photo — including one whose only
        high-conf row is 0.10 and should PASS the outer expression at
        the row level (see r3618935666). Whenever a negated subgroup
        (transitively) contains a prediction row field we therefore
        drop the whole subgroup so SQL becomes a no-op there; the
        row-level pass then evaluates the original expression per row
        against the actual metadata + prediction values.

        Leaves and subgroups in ``all``/``any`` positions still pull
        their weight in the SQL — an ``all`` narrows and an ``any``
        widens correctly at the photo level.

        A negated branch that ends up fully emptied by this pass
        represents "broaden to TRUE at photo level" (the row filter
        will decide per row). Under an ``any`` parent that TRUE
        satisfies the OR for every photo, so we propagate a broad
        marker up: an ``any`` group with any broad child becomes broad
        itself, and a top-level broad marker resolves to empty rules
        (no photo-level restriction). Without this, ``any(none(
        prediction_confidence >= 0.8), rating >= 5)`` compiled as just
        ``rating >= 5``, hiding rating-3 photos whose low-confidence
        rows satisfy the outer OR at the row level (see r3619014565).
        """
        broad = self._RELAX_BROAD

        fields = self._PREDICTION_ROW_FIELDS

        if isinstance(rules, list):
            wrapped = _relax_negated_walk(
                {"mode": "all", "rules": rules}, False, fields, broad,
            )
            if wrapped is broad or wrapped is None:
                return []
            return wrapped.get("rules", [])
        walked = _relax_negated_walk(rules, False, fields, broad)
        if walked is broad or walked is None:
            return {"mode": "all", "rules": []}
        return walked

    # Sentinel returned by ``_relax_negated_prediction_leaves`` when a
    # branch broadens to TRUE at the photo level. Identity-compared with
    # ``is``; never stored in a rules tree that reaches SQL.
    _RELAX_BROAD = object()

    def _filter_prediction_rows_by_rules(self, rows, rules):
        """Drop returned prediction rows that don't satisfy prediction-field
        rules at the row level.

        The SQL rules subquery limits the set of *photos* whose predictions
        surface, but leaves like ``prediction_confidence >= 0.8`` return
        every current prediction for a photo that has any matching one.
        This walks the rule tree per row: prediction-field leaves are
        evaluated against the row's own values; every other leaf is treated
        as satisfied (the photo already matched via the SQL subquery).

        The satisfied-by-default shortcut is only sound inside pure ``all``
        intersections — SQL ensured every non-prediction leaf was True at
        the photo level. Inside an ``any`` or ``none`` group the shortcut
        would incorrectly contribute True for a leaf that may have been
        False for this photo (e.g. ``(rating >= 5 OR prediction_confidence
        >= 0.8)`` — a rating-3 photo with one 0.95 and one 0.10 prediction:
        SQL keeps both rows because of the 0.95, but the 0.10 row must not
        be shown and shortcutting ``rating >= 5`` to True would let it
        through). For non-prediction leaves nested inside a mixed ``any``/
        ``none`` group we therefore resolve the leaf to its actual matching
        photo ids and answer per-row from that set, so prediction leaves in
        the same group still filter and the metadata leaf's truth is real.
        """
        if not rows:
            return rows
        # Normalize to the same shape ``_build_query_from_rules`` accepts so
        # both list and grouped-tree inputs go through the same walker.
        if isinstance(rules, list):
            root = {"mode": "all", "rules": rules}
        elif isinstance(rules, dict) and "rules" in rules:
            root = rules
        else:
            return rows

        def _touches_prediction(node):
            if not isinstance(node, dict):
                return False
            if "rules" in node and "field" not in node:
                return any(_touches_prediction(c) for c in node.get("rules") or [])
            return node.get("field") in self._PREDICTION_ROW_FIELDS

        if not _touches_prediction(root):
            return rows

        # Resolve non-prediction leaves nested inside ``any``/``none`` groups
        # to the photo-id set they actually match. Anywhere else the outer
        # SQL subquery already proved the leaf holds for the photo, so we
        # keep the True-shortcut. ``id(node)`` is stable across this call —
        # the dict identity is what we look up in ``_prediction_leaf_row_match``.
        leaf_photo_sets = {}

        def _collect_alt_metadata_leaves(node, inside_alt=False):
            if not isinstance(node, dict):
                return
            if "rules" in node and "field" not in node:
                mode = node.get("mode", "all")
                child_alt = inside_alt or mode in ("any", "none")
                for c in node.get("rules") or []:
                    _collect_alt_metadata_leaves(c, child_alt)
                return
            if inside_alt and node.get("field") not in self._PREDICTION_ROW_FIELDS:
                key = id(node)
                if key in leaf_photo_sets:
                    return
                try:
                    leaf_photo_sets[key] = set(self.collections.query_photo_ids([node]))
                except (ValueError, sqlite3.Error):
                    # Malformed / unresolvable leaf: fall back to matching
                    # every photo the SQL subquery kept. Never hide rows
                    # the outer filter should have allowed through.
                    leaf_photo_sets[key] = None

        _collect_alt_metadata_leaves(root)

        def _eval(node, row):
            if "rules" in node and "field" not in node:
                mode = node.get("mode", "all")
                children = node.get("rules") or []
                if not children:
                    # Empty group: match SQL (all=True, any=False, none=True).
                    return mode != "any"
                if mode == "any":
                    return any(_eval(c, row) for c in children)
                if mode == "none":
                    return not any(_eval(c, row) for c in children)
                return all(_eval(c, row) for c in children)
            return _prediction_leaf_row_match(node, row, leaf_photo_sets)

        return [r for r in rows if _eval(root, r)]

    # The one definition of "this prediction's review is already settled".
    # ``app.py`` binds ``_DECIDED_PREDICTION_STATUSES`` to it, the grouped
    # accept expansion below uses it, and Browse's panel mirrors it in JS, so
    # no surface can drift on which rows are still actionable. See
    # ``app.py``'s ``_decided_prediction_ids`` for why ``alternative`` is
    # absent and what each endpoint then does with those rows.
    DECIDED_PREDICTION_STATUSES = ("accepted", "rejected", "reviewed")

    def update_prediction_status(
        self, prediction_id: int, status: str, _commit: bool = True
    ) -> None:
        """Update per-workspace review status for a prediction.

        Review state lives in ``prediction_review`` keyed by
        (prediction_id, workspace_id); we upsert here rather than UPDATE
        so the "first review in a fresh workspace" path still writes a row.

        Args:
            _commit: If False, skip the internal commit (caller is responsible
                     for committing the transaction).
        """
        return self._prediction_repository().update_status(prediction_id, status, _commit=_commit)

    def update_predictions_status_by_photo(
        self, photo_id: int, status: str, _commit: bool = True
    ) -> None:
        """Upsert review status for every prediction of a photo in the active workspace.

        Review state is workspace-scoped (``prediction_review``); detections
        and predictions are global.  We enumerate the prediction ids via the
        detections join and upsert each review row.

        ``_commit=False`` lets a caller that already holds the prediction
        decision lock (``services.prediction_decisions``'
        ``under_prediction_decision_lock``) fold these writes into that
        transaction.

        Deliberately unconditional. The stale-overwrite this method used to
        enable — a group apply flipping an ``accepted`` row to ``rejected``
        while the keyword the accept added stays on the photo — is guarded
        one level up, in ``api_prediction_group_apply``, against the statuses
        the burst modal displayed (``_stale_group_apply_photos``). A
        ``WHERE status NOT IN DECIDED_PREDICTION_STATUSES`` clause on the
        ``ON CONFLICT`` below was tried first and removed: it cannot tell a
        stale payload from the user re-opening an applied burst and changing
        the split, which is a reachable flow (Review's default tab is "All",
        and every grouped card there renders "Review Burst Group"), so it
        silently froze the statuses while the flag and keyword writes above
        it — which run before this call and outside the lock — went ahead
        anyway. Guarding here is both too strict and too late.
        """
        return self._prediction_repository().update_status_by_photo(
            photo_id, status, _commit=_commit,
        )

    def ungroup_prediction(self, prediction_id: int, _commit: bool = True) -> None:
        """Remove a prediction from its group in the active workspace.

        ``group_id`` lives in ``prediction_review``; this only clears the
        review row for the current workspace.
        """
        return self._prediction_repository().ungroup(prediction_id, _commit=_commit)

    def accept_prediction(
        self,
        prediction_id: int,
        replace_species: bool = False,
        photo_ids: Collection[int] | None = None,
        prediction_ids: Collection[int] | None = None,
        _commit: bool = True,
        species_taxonomy: Any = ...,
    ) -> dict[str, Any] | None:
        """Accept a prediction: mark as accepted and add species keyword.

        If the prediction belongs to a group, derives the consensus species
        from the individual votes and applies that to all photos.

        When ``replace_species`` is True, every photo that receives the new
        keyword first has its existing species/taxonomy keywords removed, so
        grouped photos are replaced consistently rather than accumulating both
        the old and new species tags. Each entry in the returned ``affected``
        list carries the ``old_species`` names that were stripped from that
        photo (empty when ``replace_species`` is False).

        When ``photo_ids`` is provided for a grouped prediction, only matching
        group members are tagged and marked accepted. This lets callers apply a
        grouped accept to a filtered subset without changing hidden photos.

        ``prediction_ids`` is the stricter form of the same limit, for callers
        that already know the exact prediction rows they are acting on: the
        grouped accept touches only those rows. Prefer it over ``photo_ids``
        whenever the caller has a submitted id list. A photo is not a unique
        key for a prediction — one photo can carry several rows in the same
        burst group (one per classifier model, or per detection) — so a
        photo-id limit lets a grouped accept reach a row on an allowed photo
        that the caller never submitted. ``photo_ids`` remains for callers
        whose intent really is "these photos" (highlight confirm, accept
        subject), where the row set is chosen by this method.

        Independently of either limit, group expansion only ever *discovers*
        undecided rows: a group member already ``accepted`` or ``rejected`` is
        left alone unless the caller named it (as the entry row, or in
        ``prediction_ids``). Accepting one burst member must not resurrect a
        sibling the user rejected in Review, nor re-flip a long-accepted one
        into a history item whose "previous" status never happened.

        Both limits are settled before the first write, so a call whose scope
        excludes every candidate row is a true no-op: no keyword row is
        created, no sibling alternative is rejected, no status is flipped. It
        returns ``accepted_prediction_ids: []`` with ``keyword_id`` set to the
        existing keyword for the resolved species (``None`` when no such
        keyword exists yet).

        The returned ``accepted_prediction_ids`` lists every prediction row
        this call marked accepted, so a caller looping over a submitted batch
        can skip rows a previous grouped accept already covered instead of
        re-accepting them into duplicate history items.

        ``species_key`` is the resolved consensus identity, independent of
        the particular keyword alias used to tag the photos. Batch callers
        compare this key and record each result's actual ``keyword_id`` for
        undo/redo rather than requiring equivalent aliases to share an ID.

        ``species_taxonomy`` is the loaded taxonomy (``load_local_taxonomy()``)
        a ``replace_species`` accept uses to keep keywords a neighbouring
        subject's prediction supports as broader/same/narrower. Replacement
        requires an explicit choice: pass the loaded taxonomy, or pass None
        to choose exact-name protection when taxonomy is unavailable. Omitting
        it raises ValueError before any writes. The caller loads it, so the
        data layer never reads the taxonomy file.

        All database changes are performed atomically in a single transaction
        unless ``_commit`` is False and the caller owns the transaction.
        """
        return self._keyword_provenance_repository().accept_prediction(
            prediction_id, replace_species=replace_species, photo_ids=photo_ids,
            prediction_ids=prediction_ids, _commit=_commit,
            species_taxonomy=species_taxonomy,
        )

    def accept_subject_species(
        self, prediction_id: int, _commit: bool = True
    ) -> dict[str, Any] | None:
        """Accept agreeing model predictions for one detected subject.

        Compare uses this for an additional-species suggestion: the species
        keyword is added once to the photo, the existing species keywords are
        preserved, and every current-model prediction that names the same
        species on the same detection is resolved together. Grouped
        predictions are explicitly limited to this photo so accepting a
        subject in Compare cannot silently tag the rest of a burst.

        ``_commit=False`` leaves the surrounding transaction open so the
        caller can bundle the accept with its own writes (edit history,
        precondition checks) inside one ``BEGIN IMMEDIATE`` — used by
        ``api_accept_subject_species`` to serialize with the rest of the
        prediction-decision routes.
        """
        return self._prediction_repository().accept_subject_species(
            prediction_id, _commit=_commit,
        )

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
            commit_with_retry=commit_with_retry,
        )

    @property
    def model_runs(self) -> ModelRunsRepository:
        """Detector and classifier run records: ``db.model_runs.record_detector_run(...)``.

        A domain accessor, not a cached attribute: every access builds a fresh
        repository through ``_model_runs_repository``, exactly as a forwarding
        wrapper called at that moment would, so the connection and the
        module's ``commit_with_retry`` (which tests patch) are resolved per
        use. Every model-run table is catalog-wide, so no method needs an
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

    def set_review_status(
        self,
        prediction_id: int,
        workspace_id: int,
        status: str,
        individual: str | None = None,
        group_id: str | None = None,
    ) -> None:
        return self._prediction_repository().set_review_status(
            prediction_id, workspace_id, status, individual=individual, group_id=group_id,
        )

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
            commit_with_retry=commit_with_retry,
        )

    @property
    def detections(self) -> DetectionsRepository:
        """Detector output: ``db.detections.get_ids_for_photos(ids)``, ``db.detections.clear(pid)`` and friends.

        A domain accessor, not a cached attribute: every access builds a fresh
        repository through ``_detections_repository``, so the connection and
        the module's ``commit_with_retry`` are read per use. The table is
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

        Coordinated queue work stays on ``Database`` where something binds it
        by name: ``queue_change`` and ``remove_pending_changes`` (bound into
        the keyword-provenance and location repositories, so a patch of the
        ``Database`` method reaches them) and ``queue_flag_change_if_enabled``
        (the undo replay's flag setter). The claim, the token removal and the
        clears live in ``services/pending_changes.py``. Queue a change
        through ``db.queue_change``, not ``db.pending_changes.queue``.
        """
        return self._sync_repository()

    def queue_change(
        self,
        photo_id: int,
        change_type: str,
        value: str,
        workspace_id: int | None = None,
        _commit: bool = True,
    ) -> str | None:
        """Add a change to the sync queue, skipping redundant intents.

        Returns the inserted pending change token, or None if already queued.
        A rating only deduplicates against its latest queued value: 1 -> 2 -> 1
        must retain the last 1 so chronological sync writes the current rating.
        If workspace_id is not provided, uses the active workspace.

        Args:
            _commit: If False, skip the internal commit (caller is responsible
                     for committing the transaction).
        """
        return self._sync_repository().queue(
            photo_id, change_type, value, workspace_id=workspace_id, _commit=_commit,
        )

    def remove_pending_changes(
        self,
        photo_id: int,
        change_type: str | None = None,
        value: str | None = None,
        workspace_id: int | None = None,
        _commit: bool = True,
    ) -> int:
        """Delete matching pending changes, preserving captured keyword intents.

        Return the number of rows removed.

        If a keyword may already have reached a writer, cancelling it queues
        its inverse in the same transaction. The inverse is also marked as
        possibly written: further toggles must keep explicit repair work
        until a sync acknowledges it, even if the first write fails.

        Args:
            _commit: If False, skip the internal commit (caller is responsible
                     for committing the transaction).
        """
        ws_id = workspace_id if workspace_id is not None else self._ws_id()
        repo = self._sync_repository()
        removed = repo.delete_matching(
            photo_id, ws_id, change_type=change_type, value=value,
        )
        inverse = {"keyword_add": "keyword_remove", "keyword_remove": "keyword_add"}
        for row in removed:
            if row["change_type"] in inverse and (
                row["sync_started"] or repo.keyword_sidecar_alias(photo_id, ws_id, row["value"])
            ):
                kind = inverse[row["change_type"]]
                self.queue_change(photo_id, kind, row["value"], workspace_id=ws_id, _commit=False)
                repo.mark_sync_started(photo_id, ws_id, kind, row["value"])
        if _commit:
            repo.commit()
        return len(removed)

    def queue_flag_change_if_enabled(
        self, photo_id: int, flag: str | None, workspace_id: int | None = None, _commit: bool = True
    ) -> str | None:
        """Queue a flag write when the active config opts into XMP flag sync."""
        ws_id = workspace_id if workspace_id is not None else self._ws_id()
        flag = flag or "none"
        self.remove_pending_changes(photo_id, "flag", workspace_id=ws_id, _commit=False)
        if flag not in {"none", "flagged", "rejected"}:
            log.warning("Not queueing invalid XMP flag value for photo %s: %r", photo_id, flag)
            if _commit:
                self._sync_repository().commit()
            return None
        try:
            enabled = bool(self.effective_config().get("sync_flags_to_xmp", False))
        except Exception:
            log.warning("Failed to read sync_flags_to_xmp config", exc_info=True)
            enabled = False
        if not enabled:
            if _commit:
                self._sync_repository().commit()
            return None

        token = self.queue_change(
            photo_id, "flag", flag, workspace_id=ws_id, _commit=False
        )
        if _commit:
            self._sync_repository().commit()
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
        _commit: bool = True,
        *,
        source: str | None = None,
        reason: str | None = None,
    ) -> int:
        """Record an edit action with per-photo before/after values.

        Clears the redo stack (any undone entries) since a new action invalidates them.

        Args:
            _commit: If False, skip the internal commit and the history prune
                     (caller is responsible for committing the transaction;
                     prune can be run later).
            source, reason: the edit's provenance when the caller stated it
                     (who decided, and why); None for the user's own clicks.
        """
        items = self.species_provenance.history_items(action_type, items)
        edit_id = self._edit_history_repository().record(
            action_type, description, new_value, items,
            is_batch=is_batch, _commit=_commit, source=source, reason=reason,
        )
        if _commit:
            self._prune_edit_history()
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
            with self.photo_review.flag_write():
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
        self.photo_review.set_flag(pid, value, verify_workspace=False, _commit=False)
        self.queue_flag_change_if_enabled(pid, value, _commit=False)

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

    def _edit_set_photo_location(self, pid, value):
        from services.pending_changes import queue_location_sync_if_enabled
        state = json.loads(value)
        self.locations.clear_photo(pid)
        for link in state['links']:
            # Deleted vocabulary stays deleted, like ordinary keyword undo.
            row = self.locations.keyword_place_row(link['keyword_id'])
            if row is not None and row['type'] == 'location':
                self.tag_photo(
                    pid, link['keyword_id'],
                    source=keyword_source_max(link['source']), _commit=False,
                )
        self.locations.delete_gps_review(pid)
        if state.get('gps_review') is not None:
            self.locations.save_gps_reviews([(pid, state['gps_review'])])
        queue_location_sync_if_enabled(self, pid, _commit=False)

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
                self.update_prediction_status(int(pred_id), state['status'], _commit=False)

    # -- rating -----------------------------------------------------------

    def _undo_rating(self, entry, item):
        pid, old_val = item['photo_id'], item['old_value']
        self.photo_review.set_rating(pid, int(old_val), verify_workspace=False)
        if old_val != entry['new_value']:
            self.remove_pending_changes(pid, 'rating', entry['new_value'])
            self.queue_change(pid, 'rating', old_val)

    def _redo_rating(self, entry, item):
        pid, old_val, new_val = item['photo_id'], item['old_value'], item['new_value']
        self.photo_review.set_rating(
            pid, int(new_val) if new_val else 0, verify_workspace=False,
        )
        if old_val != new_val:
            self.remove_pending_changes(pid, 'rating', old_val)
            self.queue_change(pid, 'rating', new_val)

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
        if self.remove_pending_changes(pid, cancel_type, name) == 0:
            self.queue_change(pid, queue_type, name)

    def _retag_for_edit(self, pid, keyword_id):
        """Re-add a keyword removed by an edit; returns the keyword name."""
        name = self._edit_history_repository().keyword_name(keyword_id)
        if name is None:
            # Deleting a keyword deliberately retires it. Old history must
            # not recreate it or strand the history cursor on a foreign key.
            return None
        # Stamp the recreated association so durable authorship does not
        # disappear merely because untagging deleted the row.
        self.tag_photo(pid, keyword_id, source='manual')
        if name:
            self._flip_pending_keyword_change(
                pid, name, 'keyword_remove', 'keyword_add',
            )
        return name

    def _untag_for_edit(self, pid, keyword_id):
        """Remove a keyword added by an edit; returns the keyword name."""
        self.untag_photo(pid, keyword_id)
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
                        self.queue_change(
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
                    self.remove_pending_changes(
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
        pred_ids = self._edit_prediction_ids(old_meta, old_val)
        if not pred_ids:
            return
        self._edit_history_repository().undo_prediction_accept_statuses(
            pred_ids, self.edit_history.prediction_scope,
        )
        # A Highlights confirm of a ``reviewed`` row records that status, so
        # undo restores the user's earlier decision instead of ``pending``.
        if old_meta.get("prior_status") == "reviewed" and old_meta.get("prediction_id"):
            self.update_prediction_status(int(old_meta["prediction_id"]), "reviewed")
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
                self.update_prediction_status(pred_id, status)

    def _redo_prediction_accept_statuses(self, old_meta, old_val):
        """Re-accept every recorded prediction and re-reject its siblings.

        Siblings are scoped to the same labels_fingerprint so the redo
        matches the original accept's scope and doesn't touch predictions
        from other label sets. Every id accepted in this batch is excluded
        from rejection, since a no-tag accept can hold several ids in one
        scope.
        """
        pred_ids = self._edit_prediction_ids(old_meta, old_val)
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
            self.update_prediction_status(pred_id, 'accepted')
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
                self.update_prediction_status(pred_id, 'rejected')

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
        'photo_location': _edit_set_photo_location,
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

    def _edit_prediction_ids(self, meta, fallback):
        """Return every accepted prediction id captured on this edit item.

        Accept-subject can accept agreeing predictions from multiple
        classifier models on one detection, so the ``no_tag`` variant
        stashes the full list under ``prediction_ids``. Those siblings
        do not share a single ``(detection_id, classifier_model,
        labels_fingerprint)`` scope, so undo/redo must reset every
        recorded id — using only the first drops the other classifiers'
        accepted rows on undo. Regular accepts still carry a singular
        ``prediction_id`` or a bare-int ``old_value`` fallback.
        """
        ids = []
        seen = set()

        def _push(raw):
            if raw is None:
                return
            try:
                pid = int(raw)
            except (TypeError, ValueError):
                return
            if pid in seen:
                return
            seen.add(pid)
            ids.append(pid)

        if meta:
            _push(meta.get("prediction_id"))
            raw_ids = meta.get("prediction_ids")
            if isinstance(raw_ids, list):
                for raw in raw_ids:
                    _push(raw)
        if not ids:
            _push(fallback)
        return ids

    def _edit_prediction_id(self, meta, fallback):
        ids = self._edit_prediction_ids(meta, fallback)
        return ids[0] if ids else None

    def _restore_edit_prediction_status(self, meta):
        pred_id = meta.get("prediction_id")
        if not pred_id:
            return
        status = meta.get("prediction_status") or "pending"
        self.update_prediction_status(int(pred_id), status, _commit=False)

    def _reject_edit_prediction(self, meta):
        pred_id = meta.get("prediction_id")
        if not pred_id:
            return
        self.update_prediction_status(int(pred_id), "rejected", _commit=False)

    def prune_edit_history(self) -> dict[str, int]:
        """Apply the configured history bound after a caller-owned edit transaction commits."""
        return self._prune_edit_history()

    def _prune_edit_history(self):
        """Delete oldest entries beyond the configured max (excludes undone entries awaiting redo)."""
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
        active workspace. The façade methods the rules engine consults
        (``effective_config``, ``get_subject_types``,
        ``get_folder_subtree_ids``) are passed bound, so monkeypatches of
        them keep reaching the moved SQL, and the module helpers and
        constants it reads are passed in at call time for the same reason.
        """
        from repositories.collections import CollectionRepository

        return CollectionRepository(
            self.conn,
            self._ws_id,
            effective_config=self.effective_config,
            get_subject_types=self.get_subject_types,
            get_folder_subtree_ids=self.get_folder_subtree_ids,
            chunks=_chunks,
            escape_like=_escape_like,
            path_for_subtree_match=_path_for_subtree_match,
            normalize_browse_stack_config=normalize_browse_stack_config,
            rule_upper_bound=_rule_upper_bound,
            life_list_ancestor_suppression_clause=_LIFE_LIST_ANCESTOR_SUPPRESSION_CLAUSE,
            burst_gap_tolerance_seconds=BURST_GAP_TOLERANCE_SECONDS,
            needs_identification_rules=NEEDS_IDENTIFICATION_RULES,
            gps_without_location_keyword_rules=GPS_WITHOUT_LOCATION_KEYWORD_RULES,
            photo_date_asc_order=_PHOTO_DATE_ASC_ORDER,
            photo_sort_orders=_PHOTO_SORT_ORDERS,
            prediction_confidence_sorts=_PREDICTION_CONFIDENCE_SORTS,
            top_prediction_confidence_expr=_TOP_PREDICTION_CONFIDENCE_EXPR,
            photo_cols=self.PHOTO_COLS,
            stack_keyword_set_ctes=self._STACK_KEYWORD_SET_CTES,
            stack_keyword_joins=self._STACK_KEYWORD_JOINS,
            stack_run_window=self._STACK_RUN_WINDOW,
            stack_cover_order=self._STACK_COVER_ORDER,
            stack_sort_specs=self._STACK_SORT_SPECS,
            suggest_value_exprs=self._SUGGEST_VALUE_EXPRS,
        )

    def remap_collection_photo_ids(self, mapping: Mapping[int, int | None]) -> int:
        """Point every workspace's ``photo_ids`` rules at surviving photos; no commit."""
        from repositories.collections import remap_collection_photo_ids
        return remap_collection_photo_ids(self.conn, mapping)

    def photo_ids_named_by_collections(self) -> set[int]:
        """Every photo id named by a ``photo_ids`` rule in any workspace."""
        from repositories.collections import photo_ids_named_by_collections
        return photo_ids_named_by_collections(self.conn)

    def prune_collection_ids_of_missing_photos(self) -> list[dict[str, Any]]:
        """Drop ``photo_ids`` entries naming deleted photos, in every workspace; commits."""
        from repositories.collections import prune_collection_ids_of_missing_photos
        return prune_collection_ids_of_missing_photos(self.conn, commit=commit_with_retry)

    def _build_query_from_rules(self, rules, include_offline_folders=False,
                                row_scoped=False):
        """Build SQL clauses from a smart-collection rule tree.

        Returns (folder_join, join_clause, where, params). Raises ValueError on
        malformed input — callers that accept rules from untrusted sources
        (e.g. the live-preview API) should catch and surface a 400.

        By default the folder join filters to accessible folders
        (``status IN ('ok', 'partial')``), matching what Browse and pipeline
        callers need. Pass ``include_offline_folders=True`` for metadata-only
        callers (e.g. Dashboard totals) that count photos even when their
        storage is currently missing.

        ``row_scoped=True`` selects broader candidate SQL for the negative
        prediction-field operators (``prediction_status is not/not_in``,
        ``classifier_model is not``): ``EXISTS(status != X)`` rather than
        ``NOT EXISTS(status = X)``. That widening is only safe when the
        caller re-evaluates the rule per row afterwards, which
        ``get_predictions`` does via ``_filter_prediction_rows_by_rules``.
        Photo-scoped callers (``/api/photos/query``, saved-collection
        evaluation, calendar/geo totals) MUST leave this False — the
        broad ``NOT EXISTS(status = X)`` form is what includes photos
        with no current predictions in "is not Rejected" results (see
        review r3619275290).

        Backward compatibility: the original collection format was a flat list
        of rule objects, implicitly combined with AND. Newer collections may use
        a grouped tree:

            {"mode": "all"|"any"|"none", "rules": [rule_or_group, ...]}
        """
        return self._collection_repository()._build_query_from_rules(
            rules,
            include_offline_folders=include_offline_folders,
            row_scoped=row_scoped,
        )

    def _top_prediction_confidence_params(self):
        """Bind values for ``_TOP_PREDICTION_CONFIDENCE_EXPR``, in SQL order.

        The detector floor is the workspace-effective ``detector_confidence``
        — the same read ``_build_query_from_rules`` makes for its prediction
        predicates. Reading it here rather than baking a constant in keeps
        the sort, the badge, and the universal filter agreeing about which
        detections exist after the user moves the slider.
        """
        return self._collection_repository()._top_prediction_confidence_params()

    # Species / location keyword sets, folded to one stable string per photo
    # so two frames compare equal exactly when they carry the same keywords
    # of that kind. A photo with no keyword of that kind has no row here at
    # all and reads as NULL (compared as '' below), so two untagged frames
    # match each other.
    #
    # One grouped pass over ``photo_keywords`` rather than a correlated
    # subquery per photo: at catalog scale the correlated form cost ~20s on
    # an unfiltered workspace, because it re-ran for all 63k scoped rows.
    #
    # ``GROUP_CONCAT`` has no guaranteed order, but it does not need one
    # here: every photo's fold is produced by the same scan of the same
    # subquery within a single execution, so two photos carrying the same
    # keywords always concatenate them the same way — which is the only
    # property the equality comparison below relies on. The inner ORDER BY
    # additionally makes that order ascending-by-id in practice, which keeps
    # the folds readable when debugging a stack boundary.
    #
    # ``taxonomy OR is_species`` mirrors the predicate used everywhere else a
    # species keyword is identified (see ``remove_auto_keywords``): the
    # background ``mark_species_keywords`` pass sets ``is_species`` on
    # taxonomy keywords, and a keyword added between scans can be typed
    # before it is flagged.
    _STACK_KEYWORD_SET_CTES = """
        photo_stack_species AS (
            SELECT photo_id, GROUP_CONCAT(keyword_id) AS keyword_set
            FROM (
                SELECT pk.photo_id, pk.keyword_id
                FROM photo_keywords pk
                JOIN keywords k ON k.id = pk.keyword_id
                WHERE k.type = 'taxonomy' OR k.is_species = 1
                ORDER BY pk.photo_id, pk.keyword_id
            )
            GROUP BY photo_id
        ), photo_stack_location AS (
            SELECT photo_id, GROUP_CONCAT(keyword_id) AS keyword_set
            FROM (
                SELECT pk.photo_id, pk.keyword_id
                FROM photo_keywords pk
                JOIN keywords k ON k.id = pk.keyword_id
                WHERE k.type = 'location'
                ORDER BY pk.photo_id, pk.keyword_id
            )
            GROUP BY photo_id
        )
    """

    # Attaches the two folds to a row set exposing ``id``.
    _STACK_KEYWORD_JOINS = """
        LEFT JOIN photo_stack_species
               ON photo_stack_species.photo_id = {alias}.id
        LEFT JOIN photo_stack_location
               ON photo_stack_location.photo_id = {alias}.id
    """

    # Ordering that defines "consecutive frames" for burst detection. Bursts
    # never span folders, so the sequence restarts per folder.
    _STACK_RUN_WINDOW = "PARTITION BY folder_id ORDER BY timestamp, id"

    _STACK_COVER_ORDER = """
        CASE COALESCE(flag, 'none')
          WHEN 'flagged' THEN 2 WHEN 'none' THEN 1 ELSE 0 END DESC,
        quality_score IS NULL, quality_score DESC,
        subject_sharpness IS NULL, subject_sharpness DESC,
        sharpness IS NULL, sharpness DESC,
        rating IS NULL, rating DESC,
        (COALESCE(width, 0) * COALESCE(height, 0)) DESC,
        COALESCE(file_size, 0) DESC,
        id ASC
    """

    # Every stacked sort places a stack exactly where its *leading member* —
    # the member that would come first in the unstacked list — sits, so
    # toggling Stacks never reorders anything.
    #
    # ``member`` is the unstacked ORDER BY (see the identical ``sort_map`` in
    # query_photos / query_photos_for_rules / query_photo_ids) applied inside
    # the stack, so FIRST_VALUE over it yields that leading member; ``key`` is
    # the column the primary term reads; ``order`` re-applies the same
    # comparator to the leading member's values.
    #
    # Aggregating each term independently is what breaks: MAX(rating) with
    # MIN(filename) reads two different members, so a burst of a.jpg rated 1
    # and z.jpg rated 5 sorts as (5, "a.jpg") and jumps ahead of a single
    # m.jpg rated 5 that outranks z.jpg with Stacks off. An order that
    # silently depends on the Stacks toggle is exactly the hidden behaviour
    # CORE_PHILOSOPHY's "no black boxes" rule forbids in a culling workflow
    # (Codex P2 on PR #1561).
    _STACK_SORT_SPECS = {
        "date": {
            "member": "timestamp IS NULL, timestamp ASC, filename ASC, id ASC",
            "key": "timestamp",
            "order": (
                "_stack_lead_key IS NULL, _stack_lead_key ASC, "
                "_stack_lead_filename ASC, _stack_lead_id ASC"
            ),
        },
        "date_desc": {
            "member": "timestamp IS NULL, timestamp DESC, filename ASC, id ASC",
            "key": "timestamp",
            "order": (
                "_stack_lead_key IS NULL, _stack_lead_key DESC, "
                "_stack_lead_filename ASC, _stack_lead_id ASC"
            ),
        },
        "name": {
            "member": "filename ASC, id ASC",
            "key": "filename",
            "order": "_stack_lead_filename ASC, _stack_lead_id ASC",
        },
        "name_desc": {
            "member": "filename DESC, id ASC",
            "key": "filename",
            "order": "_stack_lead_filename DESC, _stack_lead_id ASC",
        },
        "rating": {
            "member": "rating DESC, filename ASC, id ASC",
            "key": "rating",
            "order": (
                "_stack_lead_key DESC, _stack_lead_filename ASC, "
                "_stack_lead_id ASC"
            ),
        },
        "sharpness": {
            "member": "sharpness DESC, filename ASC, id ASC",
            "key": "sharpness",
            "order": (
                "_stack_lead_key DESC, _stack_lead_filename ASC, "
                "_stack_lead_id ASC"
            ),
        },
        # Softest-first treats a stack as only as sharp as its softest
        # member, and a stack holding any unscored member is itself unscored
        # — both fall out of the leading-member rule for free, because
        # SQLite's ASC sorts NULL first and the smallest score next.
        "sharpness_asc": {
            "member": "sharpness ASC, filename ASC, id ASC",
            "key": "sharpness",
            "order": (
                "_stack_lead_key ASC, _stack_lead_filename ASC, "
                "_stack_lead_id ASC"
            ),
        },
        "quality": {
            "member": "quality_score DESC, filename ASC, id ASC",
            "key": "quality_score",
            "order": (
                "_stack_lead_key DESC, _stack_lead_filename ASC, "
                "_stack_lead_id ASC"
            ),
        },
        # ``_prediction_confidence`` is not a photos column — it is projected
        # into ``scoped`` by ``_browse_stack_query_parts`` only for these two
        # sorts, so the correlated subquery behind it never runs for a grid
        # ordered by anything else. ``NULLS LAST`` mirrors the unstacked
        # clause in ``_photo_sort_clause``: a stack whose leading member
        # carries no prediction sinks to the end in both directions rather
        # than heading up the "lowest" list.
        "prediction_confidence": {
            "member": (
                "_prediction_confidence DESC NULLS LAST, filename ASC, id ASC"
            ),
            "key": "_prediction_confidence",
            "order": (
                "_stack_lead_key DESC NULLS LAST, _stack_lead_filename ASC, "
                "_stack_lead_id ASC"
            ),
        },
        "prediction_confidence_asc": {
            "member": (
                "_prediction_confidence ASC NULLS LAST, filename ASC, id ASC"
            ),
            "key": "_prediction_confidence",
            "order": (
                "_stack_lead_key ASC NULLS LAST, _stack_lead_filename ASC, "
                "_stack_lead_id ASC"
            ),
        },
    }

    # (display_expr, group_expr) for suggest-capable columns.
    # ``group_expr`` folds the value the same way the corresponding filter
    # matches it — camera/lens rules use ``LOWER(col) = LOWER(?)`` so their
    # facets must group case-insensitively, or ``Sony A1`` and ``sony a1``
    # would split into two 1-count suggestions while selecting either would
    # match both photos. ``display_expr`` is what the picker shows; using
    # ``MIN(col)`` picks a stable representative spelling within each
    # case-insensitive bucket instead of always lower-casing the label.

    _SUGGEST_VALUE_EXPRS = {
        "camera_make": ("MIN(p.camera_make)", "LOWER(p.camera_make)"),
        "camera_model": ("MIN(p.camera_model)", "LOWER(p.camera_model)"),
        "lens": ("MIN(p.lens)", "LOWER(p.lens)"),
    }

    def _resolve_species_by_lineage(self, species_name, expected_ancestors):
        """Pick the species-rank taxon matching ``species_name`` and lineage.

        Every candidate, including a sole local row, must contain every name
        in ``expected_ancestors``. A taxonomy name index can retain only the
        wrong side of a homonym, so candidate count alone is not evidence that
        the local row belongs to the lookup's lineage. Missing lineage context,
        zero matches, or multiple matches all return ``None`` rather than
        permanently crediting the wrong Life List species/class.
        """
        return self._keyword_taxon_repository().resolve_species_by_lineage(
            species_name, expected_ancestors,
        )

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
