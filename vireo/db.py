"""SQLite database for Vireo photo browser metadata cache."""

import contextlib
import json
import logging
import os
import sqlite3
import time
import unicodedata

from keyword_identity import (
    free_sibling_name,
    keywords_claim_different_taxa,
    resolve_import_alias,
)
from keyword_normalization import (
    keyword_match_key,
    normalize_keyword_display,
)
from new_images import get_shared_cache
from repositories import UNSET as _UNSET  # sentinel for "not provided" vs explicit None

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
# row that ``retire_builtin_wildlife_genre()`` then reads as generated and
# deletes. ``COALESCE(new, old)`` is not the rule — it is only accidentally
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


def keyword_source_rank(source):
    """Position of ``source`` in the provenance lattice (higher = stronger)."""
    if source is None:
        return _KEYWORD_SOURCE_RANK[KEYWORD_SOURCE_UNKNOWN]
    return _KEYWORD_SOURCE_RANK.get(source, _KEYWORD_SOURCE_UNRECOGNIZED_RANK)


def keyword_source_max(*sources):
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


def keyword_source_rank_sql(expr):
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


def keyword_source_max_sql(left, right):
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
# existing row keeps its provenance unless the incoming row's is stronger.
KEYWORD_SOURCE_CONFLICT_SQL = (
    "ON CONFLICT(photo_id, keyword_id) DO UPDATE SET source = "
    + keyword_source_max_sql("excluded.source", "photo_keywords.source")
)

_SQLITE_PARAM_CHUNK_SIZE = 800
_MISSING_PHOTOS_PROGRESS_INTERVAL = 200
_WILDLIFE_RETIREMENT_WRITE_CHUNK_SIZE = 100


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

    def __init__(self, db_path, cause=None, newer=False):
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


def _nfc(name: str) -> str:
    """NFC-normalize a filename for byte-exact comparison against scandir output.

    macOS APFS stores names as written but compares with normalization, so the
    DB row may be NFC while a filename on disk is NFD (or vice versa). NFC on
    both sides makes set-membership reliable across that mismatch.

    Case is intentionally NOT folded here: case-sensitivity depends on the
    underlying filesystem (APFS default and NTFS are case-insensitive; ext4
    and most network mounts are not). Unconditional lowercasing would collapse
    distinct files on case-sensitive volumes; ``get_missing_photos`` instead
    falls back to ``os.path.exists`` on miss, deferring case rules to the kernel.
    """
    return unicodedata.normalize("NFC", name)


_TAXON_LOOKUP_TRANSLATION = str.maketrans({
    "’": "'",
    "‘": "'",
    "`": "'",
    "´": "'",
    "ʼ": "'",
    "ʹ": "'",
    "‛": "'",
    "“": '"',
    "”": '"',
    "„": '"',
    "‟": '"',
    "‐": "-",
    "‑": "-",
    "‒": "-",
    "–": "-",
    "—": "-",
    "―": "-",
})


def _taxon_lookup_variants(name: str) -> list[str]:
    """Return ordered taxon-name variants for punctuation-tolerant lookup."""
    stripped = str(name).strip()
    variants = []
    for variant in (
        stripped,
        unicodedata.normalize("NFKC", stripped).translate(_TAXON_LOOKUP_TRANSLATION),
    ):
        collapsed = " ".join(variant.split())
        if collapsed and collapsed not in variants:
            variants.append(collapsed)
    return variants


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


def text_search_match(value, token, match_case=False, whole_word=False):
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
# in ``vireo/classify_job.py``). Without a hierarchy filter, a normal
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


def normalize_browse_stack_config(config=None):
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


class _Connection(sqlite3.Connection):
    """``sqlite3.Connection`` whose ``commit()`` can be held off.

    Undo/redo replays an edit through the ordinary per-photo setters, and
    each of those commits on its own. Under the prediction-decision lock
    that first commit would end ``BEGIN IMMEDIATE`` and let another decision
    interleave with the rest of the replay. ``Database._commits_held()``
    raises ``_commit_holds`` so those inner commits become no-ops and the
    replay lands as one transaction.
    """

    _commit_holds = 0

    def commit(self):
        if self._commit_holds:
            return
        super().commit()


def commit_with_retry(conn, max_retries=5, base_delay=0.1):
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


def execute_with_retry(conn, sql, params=(), max_retries=5, base_delay=0.1):
    """Run ``conn.execute(sql, params)`` with retry on transient
    "locked"/"busy" errors. Returns the cursor.

    The 30s ``busy_timeout`` PRAGMA covers both INSERT/UPDATE statements
    and commits, but a single 30s wait isn't enough when another writer
    holds the lock for longer (observed: a cull job's pHash backfill held
    the writer lock for the entire backfill loop and an active scan's next
    ``add_photo`` INSERT timed out, killing the scan stage). This helper
    extends ``busy_timeout`` with bounded retry/backoff so brief contention
    bursts don't abort callers mid-write.
    """
    for attempt in range(max_retries + 1):
        try:
            return conn.execute(sql, params)
        except sqlite3.OperationalError as e:
            msg = str(e).lower()
            if ("locked" not in msg and "busy" not in msg) or attempt == max_retries:
                raise
            time.sleep(base_delay * (2 ** attempt))


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
#   floor are hidden everywhere (``get_detections_for_photos``, the
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
_TOP_PREDICTION_CONFIDENCE_EXPR = """(
            SELECT MAX(conf_pr.confidence)
            FROM detections conf_d
            JOIN predictions conf_pr ON conf_pr.detection_id = conf_d.id
            LEFT JOIN prediction_review conf_prv
                   ON conf_prv.prediction_id = conf_pr.id
                  AND conf_prv.workspace_id = ?
            WHERE conf_d.photo_id = p.id
              AND conf_d.detector_confidence >= ?
              AND conf_pr.species IS NOT NULL
              AND COALESCE(conf_prv.status, 'pending')
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


class Database:
    """Local SQLite database that caches photo metadata from XMP sidecars.

    Args:
        db_path: path to the SQLite database file (created if missing)
    """

    def __init__(self, db_path, *, initialize_schema=True):
        db_dir = os.path.dirname(db_path)
        if db_path != ":memory:" and db_dir:
            os.makedirs(db_dir, exist_ok=True)
        # Preserved for the new-images cache key, which compounds
        # (db_path, workspace_id) so instances against different SQLite files
        # don't cross-read each other's cached results (workspace_id=1 is
        # reused across every database as the default workspace).
        self._db_path = db_path
        # Pre-set so __del__ can run safely if sqlite3.connect raises.
        self.conn = None
        self.conn = sqlite3.connect(
            db_path, check_same_thread=False, factory=_Connection,
        )
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
        self._active_workspace_id = None
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
        self.repair_missing_folder_parents()
        self.repair_stale_folder_parents()
        self.ensure_default_workspace()
        # Normalize retired keyword types before seeding the built-in genres.
        # Cheap warm-path (single SELECT 1 LIMIT 1) once all legacy rows are
        # gone.
        self.migrate_legacy_keyword_types()
        # Idempotent default-keyword seed. Cheap warm-path (single
        # SELECT 1 LIMIT 1 short-circuit) — matches ensure_default_workspace
        # above.
        self.ensure_default_genre_keywords()
        # Idempotent, one-shot: seed species_highlights from legacy
        # photo_preferences rows with purpose='highlights' so upgraded
        # DBs don't lose their prior Highlights picks the first time the
        # ordered-highlights UI reads only species_highlights. Gated by
        # db_meta so it runs at most once per DB.
        self.backfill_species_highlights_from_legacy_preferences()
        # Idempotent, one-shot: seed globally shared species representatives
        # from the older per-workspace single-preference rows.
        self.backfill_species_representatives_from_legacy_preferences()
        # One-shot keyword-name normalization backfill. keywords.name,
        # pending sidecar change values, and species curation rows
        # historically stored names verbatim, so imports could seed
        # edge-quote variants like `‘apapane` alongside `apapane`.
        # add_keyword / update_keyword / queue_change now normalize on
        # write; this brings pre-existing rows onto the same invariant so
        # runtime code never guards against stored variants. Gated by
        # db_meta rather than PRAGMA user_version: unmerged branch builds
        # have already advanced some live DBs past the next free version
        # number, which would silently skip a version-gated migration.
        self.normalize_keyword_data()
        from species_identity_repair import repair_on_upgrade
        repaired = repair_on_upgrade(self)
        if repaired:
            log.info("Corrected species identity for %d predictions; review decisions preserved", repaired)
        self._restore_active_workspace()

    def _restore_active_workspace(self):
        """Restore the last-used workspace on an already initialized schema."""
        last_id = self._workspace_repository(scoped=False).most_recently_opened_id()
        if last_id is None:
            raise RuntimeError("Vireo database has no workspace after schema initialization")
        self.set_active_workspace(last_id)

    def close(self):
        """Close the underlying sqlite3 connection.

        Safe to call multiple times. Without an explicit close, the
        connection's file descriptors only release when CPython gc collects
        the object — under Python 3.14's stricter ResourceWarning handling,
        accumulating unclosed connections in long-running test suites
        exhausts the per-process fd limit and breaks coverage's own
        sqlite database.
        """
        conn = getattr(self, "conn", None)
        if conn is not None:
            with contextlib.suppress(Exception):
                conn.close()
            self.conn = None

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        self.close()
        return False

    def __del__(self):
        # Safety net for callers that don't use the context manager or call
        # close() explicitly. __del__ may run during interpreter shutdown
        # when sqlite3 is already torn down — swallow everything.
        with contextlib.suppress(Exception):
            self.close()

    def _create_tables(self):
        self.conn.executescript(
            """
            CREATE TABLE IF NOT EXISTS folders (
                id          INTEGER PRIMARY KEY,
                path        TEXT UNIQUE,
                parent_id   INTEGER REFERENCES folders(id),
                name        TEXT,
                photo_count INTEGER DEFAULT 0,
                status      TEXT NOT NULL DEFAULT 'ok'
            );

            CREATE TABLE IF NOT EXISTS photos (
                id                       INTEGER PRIMARY KEY,
                folder_id                INTEGER REFERENCES folders(id),
                filename                 TEXT,
                extension                TEXT,
                file_size                INTEGER,
                file_mtime               REAL,
                xmp_mtime                REAL,
                timestamp                TEXT,
                width                    INTEGER,
                height                   INTEGER,
                rating                   INTEGER DEFAULT 0,
                flag                     TEXT DEFAULT 'none',
                thumb_path               TEXT,
                sharpness                REAL,
                detection_box            TEXT,
                detection_conf           REAL,
                subject_sharpness        REAL,
                subject_size             REAL,
                quality_score            REAL,
                latitude                 REAL,
                longitude                REAL,
                phash                    TEXT,
                mask_path                TEXT,
                dino_subject_embedding   BLOB,
                dino_global_embedding    BLOB,
                subject_tenengrad        REAL,
                bg_tenengrad             REAL,
                crop_complete            REAL,
                bg_separation            REAL,
                subject_clip_high        REAL,
                subject_clip_low         REAL,
                subject_y_median         REAL,
                phash_crop               TEXT,
                noise_estimate           REAL,
                dino_embedding_variant   TEXT,
                active_mask_variant      TEXT,
                focal_length             REAL,
                burst_id                 TEXT,
                file_hash                TEXT,
                companion_path           TEXT,
                exif_data                TEXT,
                working_copy_path        TEXT,
                working_copy_evicted_mtime REAL,
                working_copy_failed_at   TEXT,
                working_copy_failed_mtime REAL,
                working_copy_failed_source TEXT,
                last_move_source_folder_path TEXT,
                eye_x                    REAL,
                eye_y                    REAL,
                eye_conf                 REAL,
                eye_tenengrad            REAL,
                eye_kp_fingerprint       TEXT,
                miss_no_subject          INTEGER,
                miss_clipped             INTEGER,
                miss_oof                 INTEGER,
                miss_computed_at         TEXT,
                wildlife_excluded        INTEGER NOT NULL DEFAULT 0,
                hash_checked_at          TEXT,
                hash_status              TEXT,
                UNIQUE(folder_id, filename)
            );

            CREATE TABLE IF NOT EXISTS taxa (
                id          INTEGER PRIMARY KEY,
                inat_id     INTEGER UNIQUE,
                name        TEXT NOT NULL,
                common_name TEXT,
                rank        TEXT NOT NULL,
                parent_id   INTEGER REFERENCES taxa(id),
                kingdom     TEXT
            );

            CREATE TABLE IF NOT EXISTS companion_identities (
                photo_id INTEGER PRIMARY KEY REFERENCES photos(id) ON DELETE CASCADE,
                filename TEXT NOT NULL,
                file_size INTEGER,
                timestamp TEXT,
                file_hash TEXT
            );

            CREATE TABLE IF NOT EXISTS keywords (
                id          INTEGER PRIMARY KEY,
                name        TEXT,
                parent_id   INTEGER REFERENCES keywords(id),
                is_species  INTEGER DEFAULT 0,
                type        TEXT NOT NULL DEFAULT 'general',
                latitude    REAL,
                longitude   REAL,
                taxon_id    INTEGER REFERENCES taxa(id),
                UNIQUE(name, parent_id)
            );

            -- ``source`` is durable provenance for the association itself.
            -- 'manual' means "a person explicitly added this; never treat it
            -- as generated". NULL means unknown (legacy rows, scanner/XMP
            -- imports, model output). Authorship used to be recoverable only
            -- from ``edit_history``, which ``_prune_edit_history`` trims to
            -- ``max_edit_history`` rows — so a hand-added tag could outlive
            -- every trace that a human added it. Provenance belongs on the
            -- row it describes, where nothing prunes it.
            CREATE TABLE IF NOT EXISTS photo_keywords (
                photo_id    INTEGER REFERENCES photos(id),
                keyword_id  INTEGER REFERENCES keywords(id),
                source      TEXT,
                PRIMARY KEY (photo_id, keyword_id)
            );

            -- Singleton key/value table for one-shot migration markers.
            CREATE TABLE IF NOT EXISTS db_meta (
                key   TEXT PRIMARY KEY,
                value TEXT
            );

            CREATE TABLE IF NOT EXISTS workspaces (
                id              INTEGER PRIMARY KEY,
                name            TEXT NOT NULL UNIQUE,
                config_overrides TEXT,
                ui_state        TEXT,
                tabs            TEXT,
                created_at      TEXT DEFAULT (datetime('now')),
                last_opened_at  TEXT,
                pinned_at       TEXT,
                last_grouped_at         INTEGER,
                last_group_fingerprint  TEXT
            );

            CREATE TABLE IF NOT EXISTS workspace_folders (
                workspace_id    INTEGER REFERENCES workspaces(id) ON DELETE CASCADE,
                folder_id       INTEGER REFERENCES folders(id),
                is_root         INTEGER NOT NULL DEFAULT 1,
                PRIMARY KEY (workspace_id, folder_id)
            );

            -- Remember removed catalog entries that survive in other
            -- workspaces, so recursive discovery cannot link them back.
            -- These are catalog removals, not filesystem scan exclusions:
            -- explicitly importing a folder again restores its membership.
            CREATE TABLE IF NOT EXISTS workspace_folder_removals (
                workspace_id INTEGER REFERENCES workspaces(id) ON DELETE CASCADE,
                folder_id INTEGER REFERENCES folders(id) ON DELETE CASCADE,
                recursive INTEGER NOT NULL DEFAULT 0,
                PRIMARY KEY (workspace_id, folder_id)
            );
            CREATE INDEX IF NOT EXISTS idx_workspace_folder_removals_folder
                ON workspace_folder_removals(folder_id);
            CREATE TRIGGER IF NOT EXISTS workspace_folder_restore_on_link
            AFTER INSERT ON workspace_folders
            BEGIN
                DELETE FROM workspace_folder_removals
                WHERE workspace_id = NEW.workspace_id AND folder_id = NEW.folder_id;
            END;

            -- Sync-only photo grants. Rows here give ``_resolve_xmp_paths``
            -- a way to find a photo's sidecar for a workspace that owns a
            -- queued edit on the photo but has no ``workspace_folders`` link
            -- to its folder. Tracked-merge collision handling adds a row per
            -- sibling workspace whose ``pending_changes`` were remapped onto
            -- a survivor, so the row can sync without the workspace gaining
            -- library membership on every other photo in that folder.
            --
            -- Keyed by photo, not by folder: the grant authorizes one
            -- photo's sidecar, and ``move_photos`` rewrites
            -- ``photos.folder_id`` without touching anything here -- a
            -- folder-keyed grant would silently stop applying the moment the
            -- active workspace moved the survivor. Resolving the folder at
            -- read time instead means the grant follows the photo.
            --
            -- Read only by ``get_sync_only_photo_paths`` and
            -- ``_photo_syncable_in_workspace``; every browse/library query
            -- stays folder-scoped through ``workspace_folders``.
            CREATE TABLE IF NOT EXISTS workspace_sync_only_photos (
                workspace_id    INTEGER NOT NULL REFERENCES workspaces(id) ON DELETE CASCADE,
                photo_id        INTEGER NOT NULL REFERENCES photos(id) ON DELETE CASCADE,
                PRIMARY KEY (workspace_id, photo_id)
            );

            CREATE TABLE IF NOT EXISTS local_workspaces (
                workspace_id INTEGER PRIMARY KEY REFERENCES workspaces(id) ON DELETE CASCADE,
                state        TEXT NOT NULL,
                created_at   REAL,
                activated_at REAL
            );

            CREATE TABLE IF NOT EXISTS local_workspace_folders (
                workspace_id    INTEGER NOT NULL REFERENCES workspaces(id) ON DELETE CASCADE,
                folder_id       INTEGER NOT NULL REFERENCES folders(id) ON DELETE CASCADE,
                source_path     TEXT NOT NULL,
                local_path      TEXT NOT NULL,
                original_status TEXT NOT NULL DEFAULT 'ok',
                is_root         INTEGER NOT NULL DEFAULT 0,
                root_index      INTEGER,
                PRIMARY KEY (workspace_id, folder_id)
            );

            -- Folder-scoped managed local copies.  A root folder is a
            -- library resource shared by every workspace that references it;
            -- workspace-local status is derived from these rows rather than
            -- owning a second copy of the lifecycle state.
            CREATE TABLE IF NOT EXISTS local_folders (
                root_folder_id INTEGER PRIMARY KEY REFERENCES folders(id) ON DELETE CASCADE,
                state          TEXT NOT NULL,
                created_at     REAL,
                activated_at   REAL
            );

            CREATE TABLE IF NOT EXISTS local_folder_mappings (
                root_folder_id INTEGER NOT NULL REFERENCES local_folders(root_folder_id) ON DELETE CASCADE,
                folder_id      INTEGER NOT NULL UNIQUE REFERENCES folders(id) ON DELETE CASCADE,
                source_path    TEXT NOT NULL,
                local_path     TEXT NOT NULL,
                original_status TEXT NOT NULL DEFAULT 'ok',
                is_root        INTEGER NOT NULL DEFAULT 0,
                PRIMARY KEY (root_folder_id, folder_id)
            );

            CREATE TABLE IF NOT EXISTS collections (
                id           INTEGER PRIMARY KEY,
                name         TEXT,
                rules        TEXT,
                workspace_id INTEGER REFERENCES workspaces(id) ON DELETE CASCADE,
                visual_json  TEXT
            );

            CREATE TABLE IF NOT EXISTS pending_archives (
                id TEXT PRIMARY KEY,
                workspace_id INTEGER NOT NULL,
                collection_id INTEGER,
                destination TEXT NOT NULL,
                staging_destination TEXT NOT NULL,
                target_json TEXT NOT NULL,
                state TEXT NOT NULL DEFAULT 'pending',
                error TEXT NOT NULL DEFAULT '',
                created_at TEXT NOT NULL DEFAULT (datetime('now'))
            );

            CREATE TRIGGER IF NOT EXISTS pending_archives_require_workspace
            BEFORE INSERT ON pending_archives
            WHEN NOT EXISTS (SELECT 1 FROM workspaces WHERE id = NEW.workspace_id)
            BEGIN
                SELECT RAISE(ABORT, 'Pending NAS transfer workspace no longer exists');
            END;

            CREATE TRIGGER IF NOT EXISTS pending_archives_protect_workspace
            BEFORE DELETE ON workspaces
            WHEN EXISTS (
                SELECT 1 FROM pending_archives
                WHERE workspace_id = OLD.id AND state != 'complete'
            )
            BEGIN
                SELECT RAISE(ABORT, 'Send pending photos to NAS before deleting this workspace');
            END;

            CREATE TRIGGER IF NOT EXISTS pending_archives_cleanup_workspace
            AFTER DELETE ON workspaces
            BEGIN
                DELETE FROM pending_archives WHERE workspace_id = OLD.id;
            END;

            CREATE TRIGGER IF NOT EXISTS pending_archives_clear_collection
            AFTER DELETE ON collections
            BEGIN
                UPDATE pending_archives SET collection_id = NULL WHERE collection_id = OLD.id;
            END;

            CREATE TABLE IF NOT EXISTS pending_changes (
                id          INTEGER PRIMARY KEY,
                photo_id    INTEGER REFERENCES photos(id) ON DELETE CASCADE,
                change_type TEXT,
                value       TEXT,
                change_token TEXT,
                sync_started INTEGER NOT NULL DEFAULT 0,
                created_at  TEXT DEFAULT (datetime('now')),
                workspace_id INTEGER REFERENCES workspaces(id) ON DELETE CASCADE
            );

            CREATE TABLE IF NOT EXISTS location_gps_reviews (
                photo_id INTEGER PRIMARY KEY REFERENCES photos(id) ON DELETE CASCADE,
                fingerprint TEXT NOT NULL,
                reviewed_at TEXT NOT NULL DEFAULT (datetime('now'))
            );

            CREATE TABLE IF NOT EXISTS detections (
                id                  INTEGER PRIMARY KEY,
                photo_id            INTEGER NOT NULL REFERENCES photos(id) ON DELETE CASCADE,
                detector_model      TEXT NOT NULL DEFAULT 'megadetector-v6',
                runtime_fingerprint TEXT NOT NULL DEFAULT 'legacy',
                box_x               REAL,
                box_y               REAL,
                box_w               REAL,
                box_h               REAL,
                detector_confidence REAL,
                category            TEXT,
                created_at          TEXT DEFAULT (datetime('now'))
            );

            -- subject_size is declared REAL because compute_all_quality_features
            -- stores it as a fraction in [0, 1]. SQLite's flexible type affinity
            -- means existing databases that pre-date this fix (where the column
            -- was declared INTEGER) still tolerate REAL values without an
            -- ALTER, so we don't bother emitting a migration for the column
            -- type — only fresh DBs see the corrected declaration.
            -- prompt_* are declared REAL because detections.box_* are
            -- normalized values in [0, 1] and any int truncation would
            -- collapse every prompt to (0, 0, 0, 0). SQLite's column
            -- type affinity already accepts REAL into INTEGER-declared
            -- columns, so older DBs created with INTEGER continue to
            -- store the new REAL prompts verbatim — no migration is
            -- needed; legacy rows with prompt_x = 0 will simply be
            -- detected as stale on the next pipeline run.
            CREATE TABLE IF NOT EXISTS photo_masks (
                photo_id          INTEGER NOT NULL REFERENCES photos(id) ON DELETE CASCADE,
                variant           TEXT    NOT NULL,
                path              TEXT    NOT NULL,
                created_at        INTEGER NOT NULL,
                detector_model    TEXT    NOT NULL,
                prompt_x          REAL    NOT NULL,
                prompt_y          REAL    NOT NULL,
                prompt_w          REAL    NOT NULL,
                prompt_h          REAL    NOT NULL,
                subject_size      REAL,
                subject_tenengrad REAL,
                bg_tenengrad      REAL,
                crop_complete     REAL,
                quality_input_recipe TEXT,
                subject_clip_high REAL,
                subject_clip_low REAL,
                subject_y_median REAL,
                bg_separation REAL,
                phash_crop TEXT,
                noise_estimate REAL,
                PRIMARY KEY (photo_id, variant)
            );

            CREATE TABLE IF NOT EXISTS subject_raw_analysis (
                detection_id INTEGER PRIMARY KEY REFERENCES detections(id) ON DELETE CASCADE,
                recipe TEXT NOT NULL,
                report_json TEXT NOT NULL,
                created_at INTEGER NOT NULL
            );

            CREATE TABLE IF NOT EXISTS detection_subjects (
                detection_id INTEGER PRIMARY KEY REFERENCES detections(id) ON DELETE CASCADE,
                source_key TEXT NOT NULL,
                crop TEXT NOT NULL,
                quality_score REAL NOT NULL,
                exposure_ev REAL NOT NULL,
                features TEXT NOT NULL
            );
            CREATE TABLE IF NOT EXISTS photo_subject_choices (
                photo_id INTEGER PRIMARY KEY REFERENCES photos(id) ON DELETE CASCADE,
                detection_id INTEGER NOT NULL
            );
            CREATE TABLE IF NOT EXISTS photo_subject_state (
                photo_id INTEGER PRIMARY KEY REFERENCES photos(id) ON DELETE CASCADE,
                detection_id INTEGER NOT NULL
            );

            CREATE TABLE IF NOT EXISTS predictions (
                id                   INTEGER PRIMARY KEY,
                detection_id         INTEGER NOT NULL REFERENCES detections(id) ON DELETE CASCADE,
                classifier_model     TEXT NOT NULL,
                labels_fingerprint   TEXT NOT NULL DEFAULT 'legacy',
                labels_fingerprint_full TEXT,
                species              TEXT,
                confidence           REAL,
                category             TEXT,
                scientific_name      TEXT,
                taxonomy_kingdom     TEXT,
                taxonomy_phylum     TEXT,
                taxonomy_class       TEXT,
                taxonomy_order       TEXT,
                taxonomy_family      TEXT,
                taxonomy_genus       TEXT,
                created_at           TEXT DEFAULT (datetime('now')),
                UNIQUE(detection_id, classifier_model, labels_fingerprint, species)
            );

            CREATE TABLE IF NOT EXISTS detector_runs (
                photo_id        INTEGER NOT NULL REFERENCES photos(id) ON DELETE CASCADE,
                detector_model  TEXT NOT NULL,
                runtime_fingerprint TEXT NOT NULL DEFAULT 'legacy',
                input_fingerprint TEXT,
                run_at          TEXT DEFAULT (datetime('now')),
                box_count       INTEGER NOT NULL DEFAULT 0,
                PRIMARY KEY (photo_id, detector_model)
            );

            CREATE TABLE IF NOT EXISTS classifier_runs (
                detection_id         INTEGER NOT NULL REFERENCES detections(id) ON DELETE CASCADE,
                classifier_model     TEXT NOT NULL,
                labels_fingerprint   TEXT NOT NULL,
                labels_fingerprint_full TEXT,
                runtime_fingerprint TEXT NOT NULL DEFAULT 'legacy',
                input_recipe TEXT,
                input_fingerprint TEXT,
                run_at               TEXT DEFAULT (datetime('now')),
                prediction_count     INTEGER NOT NULL DEFAULT 0,
                PRIMARY KEY (detection_id, classifier_model, labels_fingerprint)
            );

            -- Absolute (pre-softmax) match strength for one classifier run.
            --
            -- Deliberately NOT folded into classifier_runs: that table is the
            -- skip-gate for re-classification and is written only when a run
            -- produced at least one prediction, because a zero-count row there
            -- would strand the detection as permanently "done". The run that
            -- matched nothing is exactly the run this table exists to record,
            -- so it keeps its own key and is written unconditionally. It has
            -- no effect on caching.
            --
            -- max_match_score is the best raw score over the WHOLE label list,
            -- including labels that never cleared the prediction threshold.
            -- score_kind names its scale ('cosine' for BioCLIP, 'logit' for a
            -- supervised model) because the two are not comparable and a
            -- threshold calibrated on one is meaningless on the other.
            CREATE TABLE IF NOT EXISTS classifier_match_scores (
                detection_id         INTEGER NOT NULL REFERENCES detections(id) ON DELETE CASCADE,
                classifier_model     TEXT NOT NULL,
                labels_fingerprint   TEXT NOT NULL,
                max_match_score      REAL,
                match_margin         REAL,
                top_species          TEXT,
                label_count          INTEGER,
                score_kind           TEXT,
                run_at               TEXT DEFAULT (datetime('now')),
                PRIMARY KEY (detection_id, classifier_model, labels_fingerprint)
            );

            CREATE TABLE IF NOT EXISTS photo_embeddings (
                photo_id    INTEGER NOT NULL REFERENCES photos(id) ON DELETE CASCADE,
                model       TEXT NOT NULL,
                variant     TEXT NOT NULL DEFAULT '',
                embedding   BLOB NOT NULL,
                created_at  TEXT NOT NULL DEFAULT (datetime('now')),
                PRIMARY KEY (photo_id, model, variant)
            );

            CREATE TABLE IF NOT EXISTS labels_fingerprints (
                fingerprint    TEXT PRIMARY KEY,
                full_fingerprint TEXT,
                display_name   TEXT,
                sources_json   TEXT,
                label_count    INTEGER,
                created_at     TEXT DEFAULT (datetime('now'))
            );

            CREATE TABLE IF NOT EXISTS prediction_review (
                prediction_id  INTEGER NOT NULL REFERENCES predictions(id) ON DELETE CASCADE,
                workspace_id   INTEGER NOT NULL REFERENCES workspaces(id) ON DELETE CASCADE,
                status         TEXT NOT NULL DEFAULT 'pending',
                reviewed_at    TEXT,
                individual     TEXT,
                group_id       TEXT,
                vote_count     INTEGER,
                total_votes    INTEGER,
                PRIMARY KEY (prediction_id, workspace_id)
            );

            CREATE TABLE IF NOT EXISTS inat_submissions (
                id              INTEGER PRIMARY KEY,
                photo_id        INTEGER NOT NULL REFERENCES photos(id) ON DELETE CASCADE,
                observation_id  INTEGER NOT NULL,
                observation_url TEXT NOT NULL,
                submitted_at    TEXT NOT NULL DEFAULT (datetime('now')),
                UNIQUE(photo_id, observation_id)
            );

            CREATE TABLE IF NOT EXISTS edit_history (
                id           INTEGER PRIMARY KEY,
                workspace_id INTEGER REFERENCES workspaces(id) ON DELETE CASCADE,
                action_type  TEXT NOT NULL,
                description  TEXT NOT NULL,
                new_value    TEXT,
                is_batch     INTEGER DEFAULT 0,
                undone       INTEGER DEFAULT 0,
                created_at   TEXT DEFAULT (datetime('now'))
            );

            CREATE TABLE IF NOT EXISTS edit_history_items (
                id        INTEGER PRIMARY KEY,
                edit_id   INTEGER NOT NULL REFERENCES edit_history(id) ON DELETE CASCADE,
                photo_id  INTEGER NOT NULL REFERENCES photos(id) ON DELETE CASCADE,
                old_value TEXT,
                new_value TEXT
            );

            -- Large per-edit blobs (the before/after encounter snapshots a
            -- ``pipeline_grouping`` edit restores from) live here, not in
            -- ``edit_history.new_value``. Those snapshots run to tens of MB
            -- each; kept inline they made every scan of ``edit_history``
            -- (undo status after each flag, the prune inside every
            -- record_edit) walk gigabytes of overflow pages. Loaded only by
            -- an actual undo/redo; deleted with the parent row.
            CREATE TABLE IF NOT EXISTS edit_history_payloads (
                edit_id  INTEGER PRIMARY KEY REFERENCES edit_history(id) ON DELETE CASCADE,
                payload  TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS taxa_common_names (
                taxon_id    INTEGER REFERENCES taxa(id) ON DELETE CASCADE,
                name        TEXT NOT NULL,
                locale      TEXT DEFAULT 'en',
                PRIMARY KEY (taxon_id, name)
            );

            CREATE TABLE IF NOT EXISTS informal_groups (
                id          INTEGER PRIMARY KEY,
                name        TEXT NOT NULL UNIQUE
            );

            CREATE TABLE IF NOT EXISTS informal_group_taxa (
                group_id    INTEGER REFERENCES informal_groups(id) ON DELETE CASCADE,
                taxon_id    INTEGER REFERENCES taxa(id) ON DELETE CASCADE,
                PRIMARY KEY (group_id, taxon_id)
            );

            CREATE TABLE IF NOT EXISTS move_rules (
                id          INTEGER PRIMARY KEY,
                name        TEXT NOT NULL,
                destination TEXT NOT NULL,
                criteria    TEXT DEFAULT '{}',
                created_at  TEXT DEFAULT (datetime('now')),
                last_run_at TEXT
            );

            CREATE TABLE IF NOT EXISTS photo_color_labels (
                photo_id      INTEGER REFERENCES photos(id) ON DELETE CASCADE,
                workspace_id  INTEGER REFERENCES workspaces(id) ON DELETE CASCADE,
                color         TEXT NOT NULL,
                PRIMARY KEY (photo_id, workspace_id)
            );

            CREATE TABLE IF NOT EXISTS photo_edit_recipes (
                photo_id    INTEGER PRIMARY KEY REFERENCES photos(id) ON DELETE CASCADE,
                recipe_json TEXT NOT NULL,
                updated_at  TEXT NOT NULL DEFAULT (datetime('now'))
            );

            CREATE TABLE IF NOT EXISTS edit_presets (
                id          INTEGER PRIMARY KEY AUTOINCREMENT,
                name        TEXT NOT NULL UNIQUE,
                recipe_json TEXT NOT NULL,
                created_at  TEXT NOT NULL DEFAULT (datetime('now')),
                updated_at  TEXT NOT NULL DEFAULT (datetime('now'))
            );

            CREATE TABLE IF NOT EXISTS photo_preferences (
                workspace_id  INTEGER NOT NULL REFERENCES workspaces(id) ON DELETE CASCADE,
                purpose       TEXT NOT NULL,
                species       TEXT NOT NULL,
                photo_id      INTEGER NOT NULL REFERENCES photos(id) ON DELETE CASCADE,
                created_at    TEXT DEFAULT (datetime('now')),
                updated_at    TEXT DEFAULT (datetime('now')),
                PRIMARY KEY (workspace_id, purpose, species)
            );

            CREATE TABLE IF NOT EXISTS species_representatives (
                id             INTEGER PRIMARY KEY AUTOINCREMENT,
                species        TEXT NOT NULL,
                photo_id       INTEGER NOT NULL REFERENCES photos(id) ON DELETE CASCADE,
                selected_order INTEGER NOT NULL,
                created_at     TEXT DEFAULT (datetime('now')),
                updated_at     TEXT DEFAULT (datetime('now')),
                UNIQUE(species, photo_id)
            );

            CREATE TABLE IF NOT EXISTS species_highlights (
                workspace_id  INTEGER NOT NULL REFERENCES workspaces(id) ON DELETE CASCADE,
                species       TEXT NOT NULL,
                photo_id      INTEGER NOT NULL REFERENCES photos(id) ON DELETE CASCADE,
                rank          INTEGER NOT NULL,
                created_at    TEXT DEFAULT (datetime('now')),
                updated_at    TEXT DEFAULT (datetime('now')),
                PRIMARY KEY (workspace_id, species, photo_id)
            );

            CREATE TABLE IF NOT EXISTS preview_cache (
                photo_id INTEGER NOT NULL,
                size INTEGER NOT NULL,
                bytes INTEGER NOT NULL,
                last_access_at REAL NOT NULL,
                PRIMARY KEY (photo_id, size),
                FOREIGN KEY (photo_id) REFERENCES photos(id) ON DELETE CASCADE
            );

            CREATE TABLE IF NOT EXISTS offline_originals (
                photo_id INTEGER NOT NULL PRIMARY KEY,
                original_path TEXT,
                xmp_path TEXT,
                companion_path TEXT,
                bytes INTEGER NOT NULL DEFAULT 0,
                source_size INTEGER,
                source_mtime REAL,
                cached_at REAL NOT NULL,
                status TEXT NOT NULL,
                error TEXT,
                FOREIGN KEY (photo_id) REFERENCES photos(id) ON DELETE CASCADE
            );

            CREATE TABLE IF NOT EXISTS new_image_snapshots (
              id INTEGER PRIMARY KEY AUTOINCREMENT,
              workspace_id INTEGER NOT NULL REFERENCES workspaces(id) ON DELETE CASCADE,
              created_at TEXT NOT NULL,
              file_count INTEGER NOT NULL
            );

            CREATE TABLE IF NOT EXISTS new_image_snapshot_files (
              snapshot_id INTEGER NOT NULL REFERENCES new_image_snapshots(id) ON DELETE CASCADE,
              file_path TEXT NOT NULL,
              PRIMARY KEY (snapshot_id, file_path)
            );

            -- Last-run record per audit check (drift, orphans, untracked,
            -- sidecars, integrity). One row per (workspace, check); the
            -- audit page's summary banner reads these so its "archive
            -- intact" light reflects checks that actually ran, with
            -- timestamps, rather than assuming absence of evidence.
            CREATE TABLE IF NOT EXISTS audit_runs (
                workspace_id  INTEGER NOT NULL REFERENCES workspaces(id) ON DELETE CASCADE,
                check_name    TEXT NOT NULL,
                ran_at        TEXT NOT NULL,
                problem_count INTEGER NOT NULL DEFAULT 0,
                PRIMARY KEY (workspace_id, check_name)
            );

            CREATE TABLE IF NOT EXISTS place_reverse_geocode_cache (
                lat_grid    INTEGER NOT NULL,
                lng_grid    INTEGER NOT NULL,
                place_id    TEXT,
                response    TEXT NOT NULL,
                fetched_at  INTEGER NOT NULL,
                PRIMARY KEY (lat_grid, lng_grid)
            );

            -- User-editable "saved processes": named snapshots of the process
            -- page's stage toggles. Global (shared across workspaces); the
            -- per-workspace and app-wide *default* pointers live in config as
            -- ``pipeline.default_process_id`` (an id from this table). Seeded
            -- once from process_strategies.SEED_PROCESSES; see the db_meta
            -- guards in the migration section.
            CREATE TABLE IF NOT EXISTS saved_processes (
                id                INTEGER PRIMARY KEY AUTOINCREMENT,
                name              TEXT NOT NULL UNIQUE,
                skip_classify     INTEGER NOT NULL DEFAULT 0,
                skip_extract_masks INTEGER NOT NULL DEFAULT 0,
                skip_eye_keypoints INTEGER NOT NULL DEFAULT 0,
                skip_regroup      INTEGER NOT NULL DEFAULT 0,
                miss_enabled      INTEGER NOT NULL DEFAULT 1,
                review_mode       TEXT,
                is_seed           INTEGER NOT NULL DEFAULT 0,
                sort_order        INTEGER NOT NULL DEFAULT 0
            );

            CREATE INDEX IF NOT EXISTS idx_taxa_parent ON taxa(parent_id);
            CREATE INDEX IF NOT EXISTS idx_taxa_rank ON taxa(rank);
            CREATE INDEX IF NOT EXISTS idx_taxa_name ON taxa(name);
            CREATE INDEX IF NOT EXISTS idx_taxa_common ON taxa(common_name);
            -- SpeciesResolver._preferred_common matches a model label or a
            -- keyword name against the preferred name case-insensitively;
            -- idx_taxa_common is BINARY, so without this one every unresolved
            -- name scans the whole 1.3M-row taxa table.
            CREATE INDEX IF NOT EXISTS idx_taxa_common_lower ON taxa(lower(common_name));

            CREATE INDEX IF NOT EXISTS idx_photos_timestamp ON photos(timestamp);
            CREATE INDEX IF NOT EXISTS idx_photos_folder ON photos(folder_id);

            -- Undo/redo status, undo_last_edit, and _prune_edit_history all
            -- filter on (workspace_id, undone) and order by (created_at, id);
            -- the prune's NOT EXISTS and every cascade/retarget look items
            -- up by edit_id.
            CREATE INDEX IF NOT EXISTS idx_edit_history_ws_undone_created
                ON edit_history(workspace_id, undone, created_at, id);
            CREATE INDEX IF NOT EXISTS idx_edit_history_items_edit
                ON edit_history_items(edit_id);
            CREATE INDEX IF NOT EXISTS idx_photos_rating ON photos(rating);
            CREATE INDEX IF NOT EXISTS idx_photos_file_hash ON photos(file_hash);

            CREATE INDEX IF NOT EXISTS idx_keywords_name ON keywords(name);
            CREATE INDEX IF NOT EXISTS idx_keywords_parent_id ON keywords(parent_id);
            CREATE INDEX IF NOT EXISTS idx_keywords_taxon_id ON keywords(taxon_id);
            -- type is low-cardinality (5-value enum) but heavily filtered by
            -- subject rules, classifier skip gates, and migration probes.
            -- Without an index those scan the full keywords table on every
            -- _get_db()-per-request Database instantiation.
            CREATE INDEX IF NOT EXISTS idx_keywords_type ON keywords(type);
            CREATE INDEX IF NOT EXISTS idx_photo_keywords_photo ON photo_keywords(photo_id);
            CREATE INDEX IF NOT EXISTS idx_photo_keywords_keyword ON photo_keywords(keyword_id);
            CREATE INDEX IF NOT EXISTS idx_photo_color_labels_ws
                ON photo_color_labels(workspace_id);
            CREATE INDEX IF NOT EXISTS idx_photo_preferences_photo
                ON photo_preferences(photo_id);
            CREATE INDEX IF NOT EXISTS idx_species_representatives_photo
                ON species_representatives(photo_id);
            CREATE INDEX IF NOT EXISTS idx_species_representatives_order
                ON species_representatives(species, selected_order DESC);
            CREATE INDEX IF NOT EXISTS idx_species_highlights_photo
                ON species_highlights(photo_id);
            CREATE INDEX IF NOT EXISTS idx_species_highlights_rank
                ON species_highlights(workspace_id, species, rank);
            CREATE INDEX IF NOT EXISTS preview_cache_last_access
                ON preview_cache(last_access_at);
            CREATE INDEX IF NOT EXISTS idx_offline_originals_status
                ON offline_originals(status);
            CREATE INDEX IF NOT EXISTS idx_new_image_snapshots_ws
                ON new_image_snapshots(workspace_id);

            CREATE INDEX IF NOT EXISTS idx_detections_photo
                ON detections(photo_id);
            CREATE INDEX IF NOT EXISTS idx_detections_photo_model
                ON detections(photo_id, detector_model);
            CREATE INDEX IF NOT EXISTS idx_detections_conf
                ON detections(photo_id, detector_confidence);
            CREATE INDEX IF NOT EXISTS idx_predictions_detection
                ON predictions(detection_id);
            -- Explicit unique index on the predictions identity tuple. The
            -- CREATE TABLE declares the same UNIQUE, but SQLite's auto-
            -- generated unique index (sqlite_autoindex_*) has NULL `sql` in
            -- sqlite_master, which makes it impossible to assert against in
            -- tests that inspect index SQL. This explicit index gives us a
            -- stable name and a visible CREATE statement.
            CREATE UNIQUE INDEX IF NOT EXISTS idx_predictions_identity
                ON predictions(detection_id, classifier_model,
                               labels_fingerprint, species);
            CREATE INDEX IF NOT EXISTS idx_classifier_runs_detection
                ON classifier_runs(detection_id);
            CREATE INDEX IF NOT EXISTS idx_classifier_match_scores_detection
                ON classifier_match_scores(detection_id);
            CREATE INDEX IF NOT EXISTS idx_photo_embeddings_model
                ON photo_embeddings(model, variant);
            CREATE INDEX IF NOT EXISTS idx_prediction_review_workspace
                ON prediction_review(workspace_id);
            CREATE INDEX IF NOT EXISTS idx_collections_workspace
                ON collections(workspace_id);
            CREATE INDEX IF NOT EXISTS idx_pending_workspace
                ON pending_changes(workspace_id);
            -- The tracked-merge collision loop probes and remaps
            -- ``pending_changes`` by ``photo_id`` alone, once per colliding
            -- staged photo. Without this index each probe scans the whole
            -- queue, so the cost grows with the pending backlog times the
            -- collision count.
            CREATE INDEX IF NOT EXISTS idx_pending_photo
                ON pending_changes(photo_id);

            -- Monotonic observation marker shared by folder-health endpoints.
            -- Clients use it to order responses by the SQLite snapshot they
            -- observed rather than by request start or network delivery.
            INSERT OR IGNORE INTO db_meta(key, value)
                VALUES ('folder_health_version', '0');
            CREATE TRIGGER IF NOT EXISTS trg_folder_health_version_insert
            AFTER INSERT ON folders
            BEGIN
                UPDATE db_meta
                SET value = CAST(value AS INTEGER) + 1
                WHERE key = 'folder_health_version';
            END;
            CREATE TRIGGER IF NOT EXISTS trg_folder_health_version_delete
            AFTER DELETE ON folders
            BEGIN
                UPDATE db_meta
                SET value = CAST(value AS INTEGER) + 1
                WHERE key = 'folder_health_version';
            END;
            CREATE TRIGGER IF NOT EXISTS trg_folder_health_version_status
            AFTER UPDATE OF status ON folders
            WHEN OLD.status IS NOT NEW.status
            BEGIN
                UPDATE db_meta
                SET value = CAST(value AS INTEGER) + 1
                WHERE key = 'folder_health_version';
            END;
            CREATE TRIGGER IF NOT EXISTS trg_folder_health_version_ws_insert
            AFTER INSERT ON workspace_folders
            BEGIN
                UPDATE db_meta
                SET value = CAST(value AS INTEGER) + 1
                WHERE key = 'folder_health_version';
            END;
            CREATE TRIGGER IF NOT EXISTS trg_folder_health_version_ws_delete
            AFTER DELETE ON workspace_folders
            BEGIN
                UPDATE db_meta
                SET value = CAST(value AS INTEGER) + 1
                WHERE key = 'folder_health_version';
            END;

            -- Per-workspace monotonic write counter for `pending_changes`.
            -- The sync-preview cache in app.py keys its snapshot on this
            -- version to detect row replacements — `pending_changes.id` is
            -- a plain INTEGER PRIMARY KEY (no AUTOINCREMENT), so SQLite
            -- will reuse the highest deleted id on the next INSERT. A
            -- cheap COUNT/MAX/SUM aggregate can stay identical across
            -- such a delete+insert even though `change_token`, `value`,
            -- `change_type`, or `photo_id` differ; this counter changes
            -- for every row write and closes that stale-hit window.
            CREATE TRIGGER IF NOT EXISTS trg_pending_changes_version_insert
            AFTER INSERT ON pending_changes
            BEGIN
                INSERT OR IGNORE INTO db_meta(key, value)
                    VALUES ('pending_changes_version:' || NEW.workspace_id, '0');
                UPDATE db_meta
                SET value = CAST(value AS INTEGER) + 1
                WHERE key = 'pending_changes_version:' || NEW.workspace_id;
            END;
            CREATE TRIGGER IF NOT EXISTS trg_pending_changes_version_delete
            AFTER DELETE ON pending_changes
            BEGIN
                INSERT OR IGNORE INTO db_meta(key, value)
                    VALUES ('pending_changes_version:' || OLD.workspace_id, '0');
                UPDATE db_meta
                SET value = CAST(value AS INTEGER) + 1
                WHERE key = 'pending_changes_version:' || OLD.workspace_id;
            END;
            CREATE TRIGGER IF NOT EXISTS trg_pending_changes_version_update
            AFTER UPDATE ON pending_changes
            BEGIN
                INSERT OR IGNORE INTO db_meta(key, value)
                    VALUES ('pending_changes_version:' || NEW.workspace_id, '0');
                UPDATE db_meta
                SET value = CAST(value AS INTEGER) + 1
                WHERE key = 'pending_changes_version:' || NEW.workspace_id;
            END;
            -- When move_folders_to_workspace() reassigns a pending_changes
            -- row from one workspace to another, the update trigger above
            -- only bumps the destination workspace's counter. Without this
            -- second trigger, a cached progressive preview for the source
            -- workspace keeps its fingerprint and continues serving the
            -- rows that have already moved away instead of returning 409.
            CREATE TRIGGER IF NOT EXISTS trg_pending_changes_version_update_source_ws
            AFTER UPDATE OF workspace_id ON pending_changes
            WHEN OLD.workspace_id IS NOT NEW.workspace_id
            BEGIN
                INSERT OR IGNORE INTO db_meta(key, value)
                    VALUES ('pending_changes_version:' || OLD.workspace_id, '0');
                UPDATE db_meta
                SET value = CAST(value AS INTEGER) + 1
                WHERE key = 'pending_changes_version:' || OLD.workspace_id;
            END;
        """
        )
        cur = self.conn.cursor()
        removal_cols = {r[1] for r in cur.execute("PRAGMA table_info(workspace_folder_removals)")}
        if "recursive" not in removal_cols:
            # The old table only recorded exact folder IDs. A single-folder
            # unlink and a subtree removal followed by an explicit child
            # restore can leave identical rows, so recursion cannot safely
            # be inferred from current membership. Preserve the stored
            # exact scope; future tree removals record recursion explicitly.
            cur.execute(
                "ALTER TABLE workspace_folder_removals "
                "ADD COLUMN recursive INTEGER NOT NULL DEFAULT 0"
            )
        scope_version = cur.execute(
            "SELECT value FROM db_meta WHERE key = 'workspace_folder_removal_scope_version'"
        ).fetchone()
        if scope_version is None or scope_version[0] != "1":
            # Upgrade catalogs created by earlier branch builds too: their
            # view scanned the full catalog for every exact removal, and
            # every descendant could carry a redundant recursive record.
            recursive_by_workspace = {}
            for row in cur.execute(
                "SELECT workspace_id, folder_id FROM workspace_folder_removals WHERE recursive = 1"
            ).fetchall():
                recursive_by_workspace.setdefault(row["workspace_id"], set()).add(row["folder_id"])
            for workspace_id, folder_ids in recursive_by_workspace.items():
                redundant = folder_ids - self._folder_removal_root_ids(folder_ids)
                cur.executemany(
                    "UPDATE workspace_folder_removals SET recursive = 0 WHERE workspace_id = ? AND folder_id = ?",
                    [(workspace_id, fid) for fid in redundant],
                )
            cur.execute("DROP VIEW IF EXISTS workspace_removed_folders")
            cur.execute(
                "INSERT OR REPLACE INTO db_meta(key, value) VALUES ('workspace_folder_removal_scope_version', '1')"
            )
        # Share the effective removal scope across passive discovery,
        # membership reads and local-copy preparation. Source paths keep
        # the scope stable while folders are rebased into local storage.
        cur.execute("""CREATE VIEW IF NOT EXISTS workspace_removed_folders AS
            WITH paths AS NOT MATERIALIZED (
                SELECT f.id,
                       RTRIM(REPLACE(f.path, '\\', '/'), '/') AS path,
                       RTRIM(REPLACE(COALESCE(m.source_path, f.path), '\\', '/'), '/') AS source_path
                FROM folders f
                LEFT JOIN local_folder_mappings m ON m.folder_id = f.id
            ), scopes AS NOT MATERIALIZED (
                -- Exact records use primary-key lookups; only recursive
                -- roots need to search the catalog for descendants.
                SELECT workspace_id, folder_id AS root_id, folder_id
                FROM workspace_folder_removals
                UNION ALL
                SELECT removed.workspace_id, root.id, candidate.id
                FROM workspace_folder_removals removed
                JOIN paths root ON root.id = removed.folder_id
                JOIN paths candidate
                  ON substr(candidate.path, 1, length(root.path) + 1) = root.path || '/'
                  OR substr(candidate.source_path, 1, length(root.source_path) + 1) = root.source_path || '/'
                WHERE removed.recursive = 1 AND candidate.id != root.id
            )
            SELECT removed.workspace_id, candidate.id AS folder_id
            FROM scopes removed
            JOIN paths root ON root.id = removed.root_id
            JOIN paths candidate ON candidate.id = removed.folder_id
            WHERE NOT EXISTS (
                SELECT 1 FROM workspace_folders direct
                WHERE direct.workspace_id = removed.workspace_id
                  AND direct.folder_id = candidate.id
            ) AND NOT EXISTS (
                -- An explicitly restored subfolder root may cover new
                -- descendants without restoring its removed ancestors.
                SELECT 1 FROM workspace_folders restored
                JOIN paths restored_path ON restored_path.id = restored.folder_id
                WHERE restored.workspace_id = removed.workspace_id
                  AND restored.is_root = 1
                  AND (substr(restored_path.path, 1, length(root.path) + 1) = root.path || '/'
                       OR substr(restored_path.source_path, 1, length(root.source_path) + 1) = root.source_path || '/')
                  AND (candidate.id = restored.folder_id
                       OR substr(candidate.path, 1, length(restored_path.path) + 1) = restored_path.path || '/'
                       OR substr(candidate.source_path, 1, length(restored_path.source_path) + 1) = restored_path.source_path || '/')
            )
        """)
        pending_cols = {r[1] for r in cur.execute("PRAGMA table_info(pending_changes)")}
        if "sync_started" not in pending_cols:
            cur.execute("ALTER TABLE pending_changes ADD COLUMN sync_started INTEGER NOT NULL DEFAULT 0")
        pred_cols = {r[1] for r in cur.execute("PRAGMA table_info(predictions)")}
        if "source_taxon_id" not in pred_cols:
            cur.execute("ALTER TABLE predictions ADD COLUMN source_taxon_id INTEGER")
        # This row's own raw (pre-softmax) score. NULL on every row written
        # before the column existed, and NULL is the honest value there — it
        # means "not recorded", never "matched badly". Probed by column rather
        # than PRAGMA user_version for the reason normalize_keyword_data()
        # documents: branch builds have already advanced live DBs past the
        # next free version number, so a version-gated migration silently
        # skips on exactly the databases that need it.
        if "match_score" not in pred_cols:
            cur.execute("ALTER TABLE predictions ADD COLUMN match_score REAL")
        cur.execute("PRAGMA table_info(keywords)")
        kw_cols = {row[1] for row in cur.fetchall()}
        if "source_taxon_id" not in kw_cols:
            cur.execute("ALTER TABLE keywords ADD COLUMN source_taxon_id INTEGER")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_keywords_source_taxon_id ON keywords(source_taxon_id)")
        if "place_id" not in kw_cols:
            cur.execute("ALTER TABLE keywords ADD COLUMN place_id TEXT")
        cur.execute("""CREATE TABLE IF NOT EXISTS keyword_import_aliases (
            path_key TEXT PRIMARY KEY,
            path_json TEXT NOT NULL,
            keyword_id INTEGER NOT NULL REFERENCES keywords(id) ON DELETE CASCADE
        )""")
        cur.execute(
            "CREATE UNIQUE INDEX IF NOT EXISTS idx_keywords_place_id "
            "ON keywords(place_id) WHERE place_id IS NOT NULL"
        )
        # Migration: folders.parent_id. Truly legacy databases predate the
        # column, and CREATE TABLE IF NOT EXISTS above is a no-op for them —
        # so add the column here so repair_missing_folder_parents() (and
        # every other query that reads parent_id) can run.
        try:
            self.conn.execute("SELECT parent_id FROM folders LIMIT 0")
        except sqlite3.OperationalError:
            self.conn.execute(
                "ALTER TABLE folders "
                "ADD COLUMN parent_id INTEGER REFERENCES folders(id)"
            )
        # Phase 1 storage-philosophy migration: classifier embeddings move
        # from single-slot photos.(embedding, embedding_model) columns into
        # the per-(photo, model, variant) photo_embeddings table. Rows whose
        # embedding_model was never recorded have no key in the new schema
        # and are dropped — they are recomputable from pixels. Truly legacy
        # databases that pre-date embedding_model fall into the same bucket.
        try:
            self.conn.execute("SELECT embedding FROM photos LIMIT 0")
        except sqlite3.OperationalError:
            pass
        else:
            try:
                self.conn.execute("SELECT embedding_model FROM photos LIMIT 0")
                has_embedding_model = True
            except sqlite3.OperationalError:
                has_embedding_model = False
            if has_embedding_model:
                self.conn.execute(
                    """INSERT OR IGNORE INTO photo_embeddings
                           (photo_id, model, variant, embedding)
                       SELECT id, embedding_model, '', embedding
                       FROM photos
                       WHERE embedding IS NOT NULL
                         AND embedding_model IS NOT NULL"""
                )
                self.conn.execute("ALTER TABLE photos DROP COLUMN embedding_model")
            self.conn.execute("ALTER TABLE photos DROP COLUMN embedding")
        # Migration: add `tabs` column. Per the unified-tabs design (2026-04-30),
        # we reset every workspace's tabs to DEFAULT_TABS — solo-user app, no
        # preservation of prior nav_order / open_tabs customizations.
        try:
            self.conn.execute("SELECT tabs FROM workspaces LIMIT 0")
        except sqlite3.OperationalError:
            self.conn.execute("ALTER TABLE workspaces ADD COLUMN tabs TEXT")
            self.conn.execute(
                "UPDATE workspaces SET tabs = ? WHERE tabs IS NULL",
                (json.dumps(DEFAULT_TABS),),
            )
        # Migration (import/process split PR 3): insert the Import tab
        # before Process ("pipeline") in every saved tabs row that predates
        # the split. One-shot, guarded by PRAGMA user_version so a later
        # unpin isn't silently undone on the next Database.__init__ call
        # (and `_get_db()` opens a fresh Database per request).
        current_user_version = self.conn.execute("PRAGMA user_version").fetchone()[0]
        if current_user_version < 1:
            rows = self.conn.execute(
                "SELECT id, tabs FROM workspaces WHERE tabs IS NOT NULL"
            ).fetchall()
            for row in rows:
                try:
                    tabs = json.loads(row["tabs"])
                except (TypeError, ValueError):
                    continue
                if not isinstance(tabs, list) or "import" in tabs:
                    continue
                if "pipeline" in tabs:
                    tabs.insert(tabs.index("pipeline"), "import")
                else:
                    tabs.insert(0, "import")
                self.conn.execute(
                    "UPDATE workspaces SET tabs = ? WHERE id = ?",
                    (json.dumps(tabs), row["id"]),
                )
            self.conn.execute("PRAGMA user_version = 1")
            current_user_version = 1

        # Migration (storage page): cache/storage controls moved out of
        # Settings and Dashboard, so existing workspaces need a visible
        # Storage tab once. Guard with user_version so a later user unpin
        # stays respected across fresh Database handles.
        if current_user_version < 2:
            rows = self.conn.execute(
                "SELECT id, tabs FROM workspaces WHERE tabs IS NOT NULL"
            ).fetchall()
            for row in rows:
                try:
                    tabs = json.loads(row["tabs"])
                except (TypeError, ValueError):
                    continue
                if not isinstance(tabs, list) or "storage" in tabs:
                    continue
                # A legacy table that lacked the tabs column was initialized
                # above with today's compact primary workflow. Do not let this
                # historical migration append a secondary page to that new
                # default; Storage remains available under Tools.
                if tabs == DEFAULT_TABS:
                    continue
                if "settings" in tabs:
                    tabs.insert(tabs.index("settings"), "storage")
                elif "misses" in tabs:
                    tabs.insert(tabs.index("misses") + 1, "storage")
                else:
                    tabs.append("storage")
                self.conn.execute(
                    "UPDATE workspaces SET tabs = ? WHERE id = ?",
                    (json.dumps(tabs), row["id"]),
                )
            self.conn.execute("PRAGMA user_version = 2")
            current_user_version = 2

        # (Version 3 was briefly used on the fix-import-page-routing branch
        # for an "import catch-up" that tried to backfill Import for
        # databases suspected of having skipped the v1 migration. It was
        # dropped before shipping: chronologically v1 (dae1653, 2026-07-05)
        # landed before v2 (e988f21, 2026-07-08) and both live in this same
        # method, so no real database can be at user_version 2 without
        # having run v1. The catch-up therefore only fired on rows whose
        # shape matched a user who unpinned Import from the current
        # default — clobbering a legitimate preference to fix a scenario
        # that cannot occur. The number is skipped rather than reused so
        # any dev DB that briefly reached user_version 3 keeps monotonic
        # ordering into v4.)

        # Migration (import page prominence): Import is now the first pinned
        # page, because adding photos is the natural starting workflow. Move
        # an existing Import tab to the front once. Rows that lack Import
        # are left alone — a one-shot migration must not silently re-add a
        # tab a user removed.
        if current_user_version < 4:
            rows = self.conn.execute(
                "SELECT id, tabs FROM workspaces WHERE tabs IS NOT NULL"
            ).fetchall()
            for row in rows:
                try:
                    tabs = json.loads(row["tabs"])
                except (TypeError, ValueError):
                    continue
                if not isinstance(tabs, list) or "import" not in tabs:
                    continue
                if tabs[0] == "import":
                    continue
                tabs = [t for t in tabs if t != "import"]
                tabs.insert(0, "import")
                self.conn.execute(
                    "UPDATE workspaces SET tabs = ? WHERE id = ?",
                    (json.dumps(tabs), row["id"]),
                )
            self.conn.execute("PRAGMA user_version = 4")
            current_user_version = 4

        # Migration: drop legacy open_tabs column (replaced by `tabs`).
        try:
            self.conn.execute("SELECT open_tabs FROM workspaces LIMIT 0")
            self.conn.execute("ALTER TABLE workspaces DROP COLUMN open_tabs")
        except sqlite3.OperationalError:
            pass  # column already absent (already dropped or fresh schema)
        # Migration: per-workspace grouping provenance. last_grouped_at is
        # the unix epoch when run_full_pipeline last completed for this
        # workspace; last_group_fingerprint is a stable hash of the encounter
        # + burst params used. Both NULL for fresh workspaces.
        try:
            self.conn.execute("SELECT last_grouped_at FROM workspaces LIMIT 0")
        except sqlite3.OperationalError:
            self.conn.execute(
                "ALTER TABLE workspaces ADD COLUMN last_grouped_at INTEGER"
            )
        try:
            self.conn.execute("SELECT last_group_fingerprint FROM workspaces LIMIT 0")
        except sqlite3.OperationalError:
            self.conn.execute(
                "ALTER TABLE workspaces ADD COLUMN last_group_fingerprint TEXT"
            )
        # Migration: add `pinned_at` for the alphabetical-with-pinned-on-top
        # workspace dropdown. NULL means unpinned; an ISO timestamp marks the
        # workspace as pinned.
        try:
            self.conn.execute("SELECT pinned_at FROM workspaces LIMIT 0")
        except sqlite3.OperationalError:
            self.conn.execute("ALTER TABLE workspaces ADD COLUMN pinned_at TEXT")
        # Migration: distinguish user-facing workspace roots from internal
        # descendant links materialized for recursive roots.
        try:
            self.conn.execute("SELECT is_root FROM workspace_folders LIMIT 0")
        except sqlite3.OperationalError:
            self.conn.execute(
                "ALTER TABLE workspace_folders "
                "ADD COLUMN is_root INTEGER NOT NULL DEFAULT 1"
            )
            self.conn.execute(
                """UPDATE workspace_folders AS child_wf
                   SET is_root = 0
                   WHERE EXISTS (
                     SELECT 1
                     FROM workspace_folders AS root_wf
                     JOIN folders root ON root.id = root_wf.folder_id
                     JOIN folders child ON child.id = child_wf.folder_id
                     WHERE root_wf.workspace_id = child_wf.workspace_id
                       AND root_wf.folder_id != child_wf.folder_id
                       AND substr(
                         REPLACE(child.path, '\\', '/'),
                         1,
                         length(RTRIM(REPLACE(root.path, '\\', '/'), '/') || '/')
                       ) = RTRIM(REPLACE(root.path, '\\', '/'), '/') || '/'
                   )"""
            )
        # Migration: workspace_sync_only_folders -> workspace_sync_only_photos.
        # #1661 briefly recorded these grants keyed by folder; a database
        # opened by that parent commit still carries them, and every reader
        # in this commit prefers the new photo-keyed table. Without this
        # migration the sibling-workspace pending edits that #1661 preserved
        # lose their path grant on upgrade and stay queued as inaccessible
        # with nothing saying why. Rewrite what we can identify: every
        # ``workspace_sync_only_folders`` row was written by
        # ``_link_survivor_for_sibling_edits`` for a specific survivor
        # sitting in that folder at grant time. Match each legacy row to the
        # pending photos that were actually authorized by it -- photos still
        # in the granted folder, and photos that ``move_photos`` later
        # relocated out of it (matched by ``last_move_source_folder_path``,
        # the exact provenance the mover records for this purpose).
        # Restricting the migration this way keeps unrelated pending edits
        # in the same workspace -- for example, an edit for a folder
        # subsequently unlinked from the workspace by
        # ``remove_workspace_folder`` -- from silently gaining sync-only
        # access on upgrade, which was never something the legacy grant
        # authorized. Grants for library-visible photos are inert:
        # ``_photo_syncable_in_workspace`` short-circuits on library
        # membership before consulting the grant.
        #
        # The legacy table stays after this best-effort copy: ``move_photos``
        # clears ``last_move_source_folder_path`` after draining the last
        # same-stem move from a source folder, so a survivor moved before
        # upgrade can match neither its current folder nor its stale
        # provenance and slip past the migration. Retaining the row lets
        # ``_photo_syncable_in_workspace`` and ``get_sync_only_photo_paths``
        # keep resolving the grant at read time -- via the same criteria,
        # so a photo that returns to the granted folder or gets its
        # provenance restamped is still recoverable -- rather than losing
        # the record and the sibling's preserved edit with it. The
        # migration is idempotent (``INSERT OR IGNORE``) so re-running it
        # on subsequent opens fills in whatever the previous run missed.
        legacy_sof = self.conn.execute(
            "SELECT 1 FROM sqlite_master "
            "WHERE type='table' AND name='workspace_sync_only_folders'"
        ).fetchone()
        if legacy_sof is not None:
            self.conn.execute(
                """INSERT OR IGNORE INTO workspace_sync_only_photos
                       (workspace_id, photo_id)
                   SELECT DISTINCT pc.workspace_id, pc.photo_id
                   FROM pending_changes pc
                   JOIN photos p ON p.id = pc.photo_id
                   JOIN workspace_sync_only_folders sof
                     ON sof.workspace_id = pc.workspace_id
                   LEFT JOIN folders granted
                     ON granted.id = sof.folder_id
                   WHERE sof.folder_id = p.folder_id
                      OR (granted.path IS NOT NULL
                          AND granted.path
                              = p.last_move_source_folder_path)"""
            )
        # Migration: working-copy failure markers. Backfill (and the inline
        # scan extraction) record a failure here when extract_working_copy
        # returns False, gated by file_mtime so a user-replaced file retries
        # on the next pass instead of being permanently skipped.
        try:
            self.conn.execute("SELECT working_copy_failed_at FROM photos LIMIT 0")
        except sqlite3.OperationalError:
            self.conn.execute(
                "ALTER TABLE photos ADD COLUMN working_copy_failed_at TEXT"
            )
        try:
            self.conn.execute("SELECT working_copy_failed_mtime FROM photos LIMIT 0")
        except sqlite3.OperationalError:
            self.conn.execute(
                "ALTER TABLE photos ADD COLUMN working_copy_failed_mtime REAL"
            )
        try:
            self.conn.execute("SELECT working_copy_failed_source FROM photos LIMIT 0")
        except sqlite3.OperationalError:
            self.conn.execute(
                "ALTER TABLE photos ADD COLUMN working_copy_failed_source TEXT"
            )
        # Quota eviction is not an extraction failure: keep its source-mtime
        # marker separate so startup backfill does not immediately recreate
        # deliberately removed working copies.
        try:
            self.conn.execute("SELECT working_copy_evicted_mtime FROM photos LIMIT 0")
        except sqlite3.OperationalError:
            self.conn.execute(
                "ALTER TABLE photos ADD COLUMN working_copy_evicted_mtime REAL"
            )
        # Record the folder a photo most recently moved from. This lets
        # per-photo moves prove that a same-stem file already at the
        # destination is a RAW/JPEG sibling from the same source instead of
        # an unrelated photo whose developed render would be overwritten.
        # The value is the source folder's path (not its folders.id): SQLite
        # INTEGER PRIMARY KEY without AUTOINCREMENT reuses freed rowids after
        # ``delete_folder``, so a stale id could compare equal to an unrelated
        # new folder and bypass the collision guard.
        try:
            self.conn.execute(
                "SELECT last_move_source_folder_path FROM photos LIMIT 0"
            )
        except sqlite3.OperationalError:
            self.conn.execute(
                "ALTER TABLE photos "
                "ADD COLUMN last_move_source_folder_path TEXT"
            )
        # Migration: add eye_kp_fingerprint column. Set to NULL for new
        # photos; populated when the eye-keypoint stage runs. Phase 1 also
        # backfills existing eye-keypoint rows to the current fingerprint
        # in a separate migration step (see Task 2.1).
        try:
            self.conn.execute("SELECT eye_kp_fingerprint FROM photos LIMIT 0")
        except sqlite3.OperationalError:
            self.conn.execute(
                "ALTER TABLE photos ADD COLUMN eye_kp_fingerprint TEXT"
            )
        # One-shot backfill: stamp the current EYE_KP_FINGERPRINT_VERSION
        # onto photos that already have eye-keypoint data, so existing
        # users don't see "Outdated" for unchanged data on first upgrade.
        # Gated by db_meta so it runs exactly once per DB. Probe for
        # eye_tenengrad first — synthetic old-shape DBs in tests can
        # predate that column, in which case there's no eye-keypoint data
        # to backfill anyway and we just record the marker so we don't
        # keep probing.
        marker = self.conn.execute(
            "SELECT value FROM db_meta WHERE key='eye_kp_fingerprint_backfill'"
        ).fetchone()
        if marker is None:
            try:
                self.conn.execute("SELECT eye_tenengrad FROM photos LIMIT 0")
            except sqlite3.OperationalError:
                pass
            else:
                from pipeline import EYE_KP_FINGERPRINT_VERSION
                self.conn.execute(
                    "UPDATE photos SET eye_kp_fingerprint = ? "
                    "WHERE eye_tenengrad IS NOT NULL AND eye_kp_fingerprint IS NULL",
                    (EYE_KP_FINGERPRINT_VERSION,),
                )
            self.conn.execute(
                "INSERT INTO db_meta(key, value) VALUES ('eye_kp_fingerprint_backfill', '1')"
            )
        try:
            self.conn.execute("SELECT active_mask_variant FROM photos LIMIT 0")
        except sqlite3.OperationalError:
            self.conn.execute(
                "ALTER TABLE photos ADD COLUMN active_mask_variant TEXT"
            )
        try:
            self.conn.execute("SELECT wildlife_excluded FROM photos LIMIT 0")
        except sqlite3.OperationalError:
            self.conn.execute(
                "ALTER TABLE photos "
                "ADD COLUMN wildlife_excluded INTEGER NOT NULL DEFAULT 0"
            )
        # Migration: quality recipe and miss-classifier columns. PHOTO_COLS/get_collection_photos
        # and misses.py both reference these; without the fallback ALTER, any
        # DB created before the miss-classifier feature fails every photo-list
        # query with "no such column".
        for column, column_type in (
            ("quality_input_recipe", "TEXT"),
            ("miss_no_subject", "INTEGER"),
            ("miss_clipped", "INTEGER"),
            ("miss_oof", "INTEGER"),
            ("miss_computed_at", "TEXT"),
        ):
            try:
                self.conn.execute(f"SELECT {column} FROM photos LIMIT 0")
            except sqlite3.OperationalError:
                self.conn.execute(
                    f"ALTER TABLE photos ADD COLUMN {column} {column_type}"
                )
        try:
            self.conn.execute("SELECT quality_input_recipe FROM photo_masks LIMIT 0")
        except sqlite3.OperationalError:
            self.conn.execute("ALTER TABLE photo_masks ADD COLUMN quality_input_recipe TEXT")
            self.conn.execute(
                "UPDATE photo_masks SET quality_input_recipe = ("
                "SELECT p.quality_input_recipe FROM photos p WHERE p.id=photo_masks.photo_id) "
                "WHERE variant = (SELECT p.active_mask_variant FROM photos p WHERE p.id=photo_masks.photo_id)"
            )
            # Earlier experimental builds recorded only the active recipe.
            # Inactive masks on RAW-analyzed photos have unknown provenance;
            # force a refresh when selected instead of assuming normal scores.
            self.conn.execute(
                "UPDATE photo_masks SET quality_input_recipe='unknown-raw-analysis-recipe' "
                "WHERE variant IS NOT (SELECT p.active_mask_variant FROM photos p WHERE p.id=photo_masks.photo_id) "
                "AND photo_id IN (SELECT d.photo_id FROM subject_raw_analysis a "
                "JOIN detections d ON d.id=a.detection_id)"
            )
        try:
            self.conn.execute("SELECT input_recipe FROM classifier_runs LIMIT 0")
        except sqlite3.OperationalError:
            self.conn.execute("ALTER TABLE classifier_runs ADD COLUMN input_recipe TEXT")
            # Older experimental runs did not record recipe ownership.
            self.conn.execute(
                "UPDATE classifier_runs SET input_recipe='unknown-raw-recipe' "
                "WHERE detection_id IN (SELECT detection_id FROM subject_raw_analysis)"
            )
        # Quality features belong to the mask/recipe that produced them.
        # Only the active variant can be backfilled from the old photo row.
        for column, column_type in (
            ("subject_clip_high", "REAL"), ("subject_clip_low", "REAL"),
            ("subject_y_median", "REAL"), ("bg_separation", "REAL"),
            ("phash_crop", "TEXT"), ("noise_estimate", "REAL"),
        ):
            try:
                self.conn.execute(f"SELECT {column} FROM photo_masks LIMIT 0")
            except sqlite3.OperationalError:
                self.conn.execute(f"ALTER TABLE photo_masks ADD COLUMN {column} {column_type}")
                self.conn.execute(
                    f"UPDATE photo_masks SET {column}=(SELECT p.{column} FROM photos p "
                    "WHERE p.id=photo_masks.photo_id) WHERE variant=(SELECT p.active_mask_variant "
                    "FROM photos p WHERE p.id=photo_masks.photo_id)"
                )
                self.conn.execute(
                    "UPDATE photo_masks SET quality_input_recipe='unknown-mask-quality-recipe' "
                    "WHERE variant IS NOT (SELECT p.active_mask_variant FROM photos p "
                    "WHERE p.id=photo_masks.photo_id)"
                )
        # Migration: integrity-verification markers. hash_checked_at is when
        # the file's content was last re-hashed against photos.file_hash;
        # hash_status records the verdict ('ok', 'modified', 'corrupt',
        # 'unreadable'). NULL means the file has never been verified.
        try:
            self.conn.execute("SELECT hash_checked_at FROM photos LIMIT 0")
        except sqlite3.OperationalError:
            self.conn.execute(
                "ALTER TABLE photos ADD COLUMN hash_checked_at TEXT"
            )
        try:
            self.conn.execute("SELECT hash_status FROM photos LIMIT 0")
        except sqlite3.OperationalError:
            self.conn.execute(
                "ALTER TABLE photos ADD COLUMN hash_status TEXT"
            )

        # Migration: collections carry the universal filter's visual clause
        # alongside rules — the clause deliberately lives outside the rule
        # tree, so without this column a saved expression with a visual
        # component would silently reopen as metadata-only.
        try:
            self.conn.execute("SELECT visual_json FROM collections LIMIT 0")
        except sqlite3.OperationalError:
            self.conn.execute(
                "ALTER TABLE collections ADD COLUMN visual_json TEXT"
            )

        # Migration: durable keyword-association provenance. Authorship used
        # to be inferred from ``edit_history``, which ``_prune_edit_history``
        # trims to ``max_edit_history`` rows, so evidence that a person added
        # a keyword could disappear while the keyword itself survived — and
        # provenance-driven cleanups would then misread it as generated.
        # 'manual' on the association row cannot be pruned.
        try:
            self.conn.execute("SELECT source FROM photo_keywords LIMIT 0")
        except sqlite3.OperationalError:
            self.conn.execute(
                "ALTER TABLE photo_keywords ADD COLUMN source TEXT"
            )

        # Migration: promote EXIF camera fields out of the exif_data JSON
        # blob into real columns so the universal filter engine can query
        # them with indexes and plain SQL (design:
        # docs/plans/2026-07-19-universal-filters-design.md). Scans populate
        # these for new/changed files; the one-shot backfill below covers
        # existing rows.
        for column, column_type in (
            ("camera_make", "TEXT"),
            ("camera_model", "TEXT"),
            ("lens", "TEXT"),
            ("aperture", "REAL"),
            ("shutter_speed", "REAL"),
            ("iso", "INTEGER"),
        ):
            try:
                self.conn.execute(f"SELECT {column} FROM photos LIMIT 0")
            except sqlite3.OperationalError:
                self.conn.execute(
                    f"ALTER TABLE photos ADD COLUMN {column} {column_type}"
                )
        # One-shot backfill from stored exif_data, gated by db_meta (not
        # user_version, which has drifted on live DBs). Rows whose exif_data
        # is the minimal "{}" marker were scanned with
        # ``extract_full_metadata=False`` before the promoted columns
        # existed — nothing to backfill from the JSON, and the scanner's
        # incremental pre-pass treats any non-NULL ``exif_data`` as
        # "already extracted", so leaving the marker in place would keep
        # camera/lens/iso NULL forever until a user manually forces a full
        # non-incremental scan. Clear those rows back to NULL so the next
        # scan re-runs ExifTool and populates the promoted columns
        # (``scanner._compute_file_features`` writes them whenever
        # ``file_meta`` is present, independent of the full-JSON flag).
        marker = self.conn.execute(
            "SELECT value FROM db_meta WHERE key='exif_summary_backfill_v1'"
        ).fetchone()
        if marker is None:
            from metadata import exif_summary_columns
            # Probe first: synthetic old-shape DBs in tests can predate the
            # exif_data column entirely. Nothing to backfill there — just
            # record the marker so we don't keep probing.
            try:
                self.conn.execute("SELECT exif_data FROM photos LIMIT 0")
            except sqlite3.OperationalError:
                rows = []
                exif_column_present = False
            else:
                rows = self.conn.execute(
                    "SELECT id, exif_data FROM photos "
                    "WHERE exif_data IS NOT NULL AND exif_data != '{}'"
                ).fetchall()
                exif_column_present = True
            for row in rows:
                try:
                    grouped = json.loads(row["exif_data"])
                except (TypeError, ValueError):
                    continue
                cols = exif_summary_columns(grouped)
                if not cols:
                    continue
                assignments = ", ".join(f"{col} = ?" for col in cols)
                self.conn.execute(
                    f"UPDATE photos SET {assignments} WHERE id = ?",
                    [*cols.values(), row["id"]],
                )
            if exif_column_present:
                # Clear the minimal ``'{}'`` marker left by older scans that
                # ran with ``extract_full_metadata=False``. Those rows have
                # no JSON to backfill from, and the scanner's incremental
                # pre-pass otherwise skips them forever (their ``exif_data``
                # is non-NULL, so they're treated as already extracted),
                # leaving the new camera/lens/aperture/... columns
                # permanently empty on upgraded libraries. Clearing to NULL
                # lets the pre-pass's ``summary_needs_extract`` query pick
                # them up on the next scan and populate the promoted
                # columns in a single re-extraction.
                self.conn.execute(
                    "UPDATE photos SET exif_data = NULL WHERE exif_data = '{}'"
                )
            self.conn.execute(
                "INSERT INTO db_meta(key, value) VALUES ('exif_summary_backfill_v1', '1')"
            )

        # Migration: add ON DELETE CASCADE foreign key on
        # local_workspace_folders.folder_id. Early builds of this table
        # declared folder_id as a bare INTEGER, so a folder DELETE on those
        # DBs would leave a dangling local-workspace mapping and break
        # sync/discard's catalog restore. SQLite can't add a FK via ALTER
        # TABLE, so rebuild the table when the constraint is absent.
        fk_rows = self.conn.execute(
            "PRAGMA foreign_key_list(local_workspace_folders)"
        ).fetchall()
        has_folder_fk = any(
            row["from"] == "folder_id" and row["table"] == "folders"
            for row in fk_rows
        )
        if not has_folder_fk:
            # Earlier migrations in this method may have executed DML (for
            # example the db_meta backfill marker above) which sqlite3
            # wraps in an implicit transaction. Toggling foreign_keys and
            # starting BEGIN IMMEDIATE both require no open transaction, so
            # commit any pending migration writes before the rebuild.
            self.conn.commit()
            self.conn.execute("PRAGMA foreign_keys=OFF")
            try:
                self.conn.execute("BEGIN IMMEDIATE")
                self.conn.execute(
                    """CREATE TABLE local_workspace_folders_new (
                        workspace_id    INTEGER NOT NULL REFERENCES workspaces(id) ON DELETE CASCADE,
                        folder_id       INTEGER NOT NULL REFERENCES folders(id) ON DELETE CASCADE,
                        source_path     TEXT NOT NULL,
                        local_path      TEXT NOT NULL,
                        original_status TEXT NOT NULL DEFAULT 'ok',
                        is_root         INTEGER NOT NULL DEFAULT 0,
                        root_index      INTEGER,
                        PRIMARY KEY (workspace_id, folder_id)
                    )"""
                )
                # Only carry over rows whose folder_id still exists; a
                # concurrent-with-migration folder delete on the old shape
                # is the exact bug this FK closes, and dragging a dangling
                # row into the new table would immediately trip the FK.
                self.conn.execute(
                    """INSERT INTO local_workspace_folders_new
                       SELECT lwf.* FROM local_workspace_folders lwf
                       JOIN folders f ON f.id = lwf.folder_id"""
                )
                self.conn.execute("DROP TABLE local_workspace_folders")
                self.conn.execute(
                    "ALTER TABLE local_workspace_folders_new "
                    "RENAME TO local_workspace_folders"
                )
                self.conn.commit()
            except BaseException:
                self.conn.rollback()
                raise
            finally:
                self.conn.execute("PRAGMA foreign_keys=ON")

        # Backfill pre-existing photos with mask_path set on the photos
        # row but no row in photo_masks. They get migrated to
        # variant='unknown' with a sentinel prompt; detector_model='unknown'
        # + prompt=-1 mean the staleness check will treat these masks as
        # stale on the next pipeline run, so they get regenerated against
        # whatever SAM2 variant the user has configured.
        #
        # Resumable: gating only on the per-photo NOT EXISTS clause means
        # a startup crash partway through (e.g. after inserting some
        # 'unknown' rows but before completing) still finishes the rest
        # of the legacy photos on the next startup. An earlier outer
        # ``if total_unknown_rows == 0`` guard caused remaining photos
        # to be skipped forever, leaving orphaned mask_path values that
        # variant-aware APIs and cleanup logic couldn't see.
        try:
            rows = self.conn.execute(
                "SELECT p.id, p.mask_path, p.subject_size, "
                "p.subject_tenengrad, p.bg_tenengrad, p.crop_complete "
                "FROM photos p "
                "WHERE p.mask_path IS NOT NULL "
                "  AND NOT EXISTS ("
                "    SELECT 1 FROM photo_masks pm WHERE pm.photo_id = p.id"
                "  )"
            ).fetchall()
        except sqlite3.OperationalError:
            rows = []
        now = int(time.time())
        for r in rows:
            self.conn.execute(
                "INSERT OR IGNORE INTO photo_masks "
                "(photo_id, variant, path, created_at, detector_model, "
                "prompt_x, prompt_y, prompt_w, prompt_h, "
                "subject_size, subject_tenengrad, bg_tenengrad, crop_complete) "
                "VALUES (?, 'unknown', ?, ?, 'unknown', -1, -1, -1, -1, ?, ?, ?, ?)",
                (r["id"], r["mask_path"], now,
                 r["subject_size"], r["subject_tenengrad"],
                 r["bg_tenengrad"], r["crop_complete"]),
            )
            self.conn.execute(
                "UPDATE photos SET active_mask_variant='unknown' "
                "WHERE id=? AND active_mask_variant IS NULL",
                (r["id"],),
            )

        # Seed user-editable saved processes once. db_meta-guarded (NOT
        # user_version-guarded) because the live DB's user_version can run
        # ahead of main on parallel branches, which would silently skip a
        # version-gated seed. The marker also means a user who deletes all
        # their processes never has the seeds reappear on the next Database
        # handle. The table itself is created in _create_tables above.
        import process_strategies as ps

        seeded = self.conn.execute(
            "SELECT value FROM db_meta WHERE key='saved_processes_seeded'"
        ).fetchone()
        if seeded is None:
            for order, seed in enumerate(ps.SEED_PROCESSES):
                flags = ps.seed_flags(seed)
                self.conn.execute(
                    "INSERT OR IGNORE INTO saved_processes "
                    "(name, skip_classify, skip_extract_masks, "
                    " skip_eye_keypoints, skip_regroup, miss_enabled, "
                    " review_mode, is_seed, sort_order) "
                    "VALUES (?, ?, ?, ?, ?, ?, ?, 1, ?)",
                    (
                        seed["name"],
                        int(flags["skip_classify"]),
                        int(flags["skip_extract_masks"]),
                        int(flags["skip_eye_keypoints"]),
                        int(flags["skip_regroup"]),
                        int(flags["miss_enabled"]),
                        flags["review_mode"],
                        order,
                    ),
                )
            self.conn.execute(
                "INSERT INTO db_meta(key, value) "
                "VALUES ('saved_processes_seeded', '1')"
            )

        # One-shot: migrate the former per-workspace pipeline.default_strategy
        # (a strategy name) to pipeline.default_process_id (a saved_processes
        # id). Unknown/removed names -> unset (import only). Runs after seeding
        # so the name->id lookup finds the seed rows; db_meta-guarded so a
        # later manual edit of the override isn't reverted on the next handle.
        migrated = self.conn.execute(
            "SELECT value FROM db_meta WHERE key='default_strategy_to_process_id'"
        ).fetchone()
        if migrated is None:
            name_to_id = {
                row["name"]: row["id"]
                for row in self.conn.execute(
                    "SELECT id, name FROM saved_processes"
                ).fetchall()
            }
            ws_rows = self.conn.execute(
                "SELECT id, config_overrides FROM workspaces "
                "WHERE config_overrides IS NOT NULL"
            ).fetchall()
            for row in ws_rows:
                try:
                    overrides = json.loads(row["config_overrides"])
                except (TypeError, ValueError):
                    continue
                if not isinstance(overrides, dict):
                    continue
                pipeline_ov = overrides.get("pipeline")
                if not isinstance(pipeline_ov, dict):
                    continue
                if "default_strategy" not in pipeline_ov:
                    continue
                old = pipeline_ov.pop("default_strategy")
                seed_name = (
                    ps.LEGACY_STRATEGY_NAMES.get(old)
                    if isinstance(old, str) else None
                )
                pid = name_to_id.get(seed_name) if seed_name else None
                # Always write ``default_process_id`` (even ``None``) so the
                # workspace's explicit override intent survives the migration.
                # An old ``default_strategy: null`` meant "import only"; without
                # this line, popping the legacy key would let
                # ``get_effective_config()``'s deep_merge inherit the *global*
                # default and silently start auto-processing on imports for a
                # workspace that had explicitly said otherwise. Same reasoning
                # for an unrecognized legacy name — the user's explicit choice
                # was not the current global default.
                pipeline_ov["default_process_id"] = pid
                self.conn.execute(
                    "UPDATE workspaces SET config_overrides = ? WHERE id = ?",
                    (json.dumps(overrides), row["id"]),
                )
            self.conn.execute(
                "INSERT INTO db_meta(key, value) "
                "VALUES ('default_strategy_to_process_id', '1')"
            )
        self.conn.commit()

    # ------------------------------------------------------------------
    # Saved processes (user-editable process presets; global, not scoped)
    # ------------------------------------------------------------------
    @staticmethod
    def _saved_process_row_to_dict(row):
        from repositories.processes import ProcessesRepository

        return ProcessesRepository._row_to_dict(row)

    def _processes_repository(self):
        """Build the saved-processes repository on this connection.

        Saved processes are global (not workspace-scoped), so no active
        workspace is needed.
        """
        from repositories.processes import ProcessesRepository

        return ProcessesRepository(self.conn)

    def get_saved_processes(self):
        """Return all saved processes ordered for display (sort_order, id)."""
        return self._processes_repository().list_all()

    def get_saved_process(self, process_id):
        """Return one saved process as a dict, or None if it doesn't exist."""
        return self._processes_repository().get(process_id)

    def resolve_process(self, process_id):
        """Expand a saved-process id into a full stage-flags dict over _BASE.

        Raises ValueError if the id doesn't exist, so callers surface a clean
        400/404 instead of an AttributeError deeper in the pipeline.
        """
        import process_strategies as ps

        proc = self.get_saved_process(process_id)
        if proc is None:
            raise ValueError(f"unknown process id: {process_id!r}")
        return {**ps._BASE, **{k: proc[k] for k in ps.FLAG_FIELDS}}

    @staticmethod
    def _normalize_process_fields(name, skip_classify, skip_extract_masks,
                                  skip_eye_keypoints, skip_regroup,
                                  miss_enabled, review_mode):
        """Validate + coerce process fields. Raises ValueError on bad input."""
        from repositories.processes import ProcessesRepository

        return ProcessesRepository._normalize_fields(
            name, skip_classify, skip_extract_masks, skip_eye_keypoints,
            skip_regroup, miss_enabled, review_mode,
        )

    def create_saved_process(self, name, *, skip_classify=False,
                             skip_extract_masks=False, skip_eye_keypoints=False,
                             skip_regroup=False, miss_enabled=True,
                             review_mode=None):
        """Insert a saved process and return its id.

        Raises ValueError on a blank/duplicate name or a bad review_mode.
        """
        return self._processes_repository().create(
            name,
            skip_classify=skip_classify,
            skip_extract_masks=skip_extract_masks,
            skip_eye_keypoints=skip_eye_keypoints,
            skip_regroup=skip_regroup,
            miss_enabled=miss_enabled,
            review_mode=review_mode,
        )

    def update_saved_process(self, process_id, *, name=None,
                             skip_classify=None, skip_extract_masks=None,
                             skip_eye_keypoints=None, skip_regroup=None,
                             miss_enabled=None, review_mode=_UNSET):
        """Update an existing saved process. Returns True if it existed.

        Any field left at its sentinel default (None, or _UNSET for
        review_mode which is legitimately None) is untouched. Raises
        ValueError on a blank/duplicate name or a bad review_mode.
        """
        current = self.get_saved_process(process_id)
        if current is None:
            return False
        return self._processes_repository().update(
            process_id,
            current,
            name=name,
            skip_classify=skip_classify,
            skip_extract_masks=skip_extract_masks,
            skip_eye_keypoints=skip_eye_keypoints,
            skip_regroup=skip_regroup,
            miss_enabled=miss_enabled,
            review_mode=review_mode,
        )

    def delete_saved_process(self, process_id):
        """Delete a saved process and null out every reference to it.

        Any workspace whose pipeline.default_process_id pointed here falls
        back to null ("import only"). Returns True if the process existed.
        The app-wide global default (config.json) is cleared by the caller,
        which owns config-file I/O. Does not run inside an outer transaction.
        """
        if self.get_saved_process(process_id) is None:
            return False
        return self._processes_repository().delete(process_id)

    def repair_missing_folder_parents(self):
        """Fill parent_id for legacy folder rows whose parent path is known."""
        self._folder_repository(scoped=False).repair_missing_parents()

    def repair_stale_folder_parents(self):
        """Repair parent links left behind by older folder moves.

        Only non-NULL links that contradict the saved paths are changed.
        Managed local copies deliberately retain their archive parentage, so
        defer links involving those folders until their local session ends.
        This runs on startup and is idempotent; no filesystem access or photo
        and workspace membership changes are needed, even for offline paths.
        """
        updates = self._folder_repository(scoped=False).repair_stale_parents(
            nearest_ancestor_id=self.nearest_ancestor_folder_id,
        )
        if updates:
            log.info("Repaired %d stale folder parent links", len(updates))
        return len(updates)

    # -- Workspaces --

    def set_active_workspace(self, workspace_id):
        """Set the active workspace for scoped queries."""
        self._active_workspace_id = workspace_id

    def _ws_id(self):
        """Return active workspace id, raising if none set."""
        if self._active_workspace_id is None:
            raise RuntimeError("No active workspace set")
        return self._active_workspace_id

    def get_new_images_for_workspace(self, workspace_id):
        """Return new-images result for workspace, using cache when fresh.

        Race-safe: we snapshot the cache generation before the (potentially
        slow) walk and pass it to ``set``. If an invalidation fires during
        the walk, the generation advances and the stale result is dropped
        on write — so the next reader recomputes instead of seeing the
        pre-invalidation value. The current caller still returns its own
        best-effort result.
        """
        import new_images
        cached = self._new_images_cache.get(self._db_path, workspace_id)
        if cached is not None:
            return cached
        generation = self._new_images_cache.get_generation(self._db_path, workspace_id)
        result = new_images.count_new_images_for_workspace(self, workspace_id)
        self._new_images_cache.set(
            self._db_path, workspace_id, result, generation=generation
        )
        return result

    def invalidate_new_images_cache_for_folders(self, folder_ids):
        """Clear cache for every workspace linked to any of the given folder_ids."""
        if not folder_ids:
            return
        ws_ids = self._workspace_repository(scoped=False).ids_for_folders(folder_ids)
        self._new_images_cache.invalidate_workspaces(self._db_path, ws_ids)

    def invalidate_new_images_cache_for_workspace(self, workspace_id):
        """Clear path-dependent new-image results for one workspace."""
        self._new_images_cache.invalidate_workspaces(
            self._db_path, [workspace_id]
        )

    def _photo_in_workspace(self, photo_id):
        """Return True if the photo belongs to a folder visible in the active workspace."""
        row = self.conn.execute(
            """SELECT 1 FROM photos p
               JOIN workspace_folders wf ON wf.folder_id = p.folder_id
               WHERE p.id = ? AND wf.workspace_id = ?""",
            (photo_id, self._ws_id()),
        ).fetchone()
        return row is not None

    def _verify_photo_in_workspace(self, photo_id):
        """Raise ValueError if the photo is not in the active workspace."""
        if not self._photo_in_workspace(photo_id):
            raise ValueError(
                f"Photo {photo_id} does not belong to the active workspace"
            )

    def _photo_syncable_in_workspace(self, photo_id):
        """Return True if the workspace may write this photo's sidecar.

        Library membership, or a ``workspace_sync_only_photos`` grant --
        the narrow record that tracked-merge collision handling writes so a
        remapped edit stays syncable without the workspace gaining
        visibility of every other photo in the folder. Falls back to
        ``workspace_sync_only_folders`` grants that pre-date the
        photo-keyed table (see the migration comment in ``__init__``): the
        legacy table stays around so a photo the migration could not
        identify -- say the survivor's ``last_move_source_folder_path`` was
        cleared after a same-stem drain -- still resolves through the
        legacy folder key when the photo's current folder or its
        provenance matches AND the workspace still has a pending edit on
        the photo (the same authorization the migration used). The
        pending-edit gate keeps a neighbour with no queued edit from
        gaining sync-only access just for sitting in a granted folder --
        the exact overgrant the migration excluded, and the same one every
        Codex review of this table has flagged.
        """
        if self._photo_in_workspace(photo_id):
            return True
        workspace_id = self._ws_id()
        row = self.conn.execute(
            "SELECT 1 FROM workspace_sync_only_photos "
            "WHERE photo_id = ? AND workspace_id = ?",
            (photo_id, workspace_id),
        ).fetchone()
        if row is not None:
            return True
        legacy = self.conn.execute(
            "SELECT 1 FROM sqlite_master "
            "WHERE type='table' AND name='workspace_sync_only_folders'"
        ).fetchone()
        if legacy is None:
            return False
        row = self.conn.execute(
            """SELECT 1
               FROM workspace_sync_only_folders sof
               JOIN photos p ON p.id = ?
               LEFT JOIN folders granted ON granted.id = sof.folder_id
               WHERE sof.workspace_id = ?
                 AND EXISTS (
                     SELECT 1 FROM pending_changes pc
                     WHERE pc.workspace_id = sof.workspace_id
                       AND pc.photo_id = p.id
                 )
                 AND (sof.folder_id = p.folder_id
                      OR (granted.path IS NOT NULL
                          AND granted.path
                              = p.last_move_source_folder_path))""",
            (photo_id, workspace_id),
        ).fetchone()
        return row is not None

    def _verify_photo_syncable_in_workspace(self, photo_id):
        """Raise ValueError if the workspace may not write this sidecar."""
        if not self._photo_syncable_in_workspace(photo_id):
            raise ValueError(
                f"Photo {photo_id} does not belong to the active workspace"
            )

    def create_workspace(self, name, config_overrides=None, ui_state=None):
        """Create a new workspace. Returns the workspace id."""
        workspace_id = self._workspace_repository(scoped=False).create(
            name, config_overrides, ui_state
        )
        # SQLite INTEGER PRIMARY KEY (without AUTOINCREMENT) can reuse a deleted
        # rowid, so a freshly created workspace may collide with the stale cache
        # entry of a prior workspace that shared this id. Clear any lingering
        # entry so the new workspace starts clean.
        self._new_images_cache.invalidate_workspaces(self._db_path, [workspace_id])
        return workspace_id

    def get_workspace(self, workspace_id):
        """Return a single workspace by id, or None."""
        return self._workspace_repository(scoped=False).get(workspace_id)

    def get_workspaces(self):
        """Return all workspaces, pinned first then alphabetical."""
        return self._workspace_repository(scoped=False).list_all()

    def update_workspace(self, workspace_id, name=None, config_overrides=_UNSET,
                         ui_state=_UNSET, last_opened_at=None,
                         pinned_at=_UNSET):
        """Update workspace fields. Only provided args are updated.

        For config_overrides and ui_state, pass None to clear the value
        (set DB column to NULL), or omit the argument to leave it unchanged.
        """
        self._workspace_repository(scoped=False).update(
            workspace_id,
            name=name,
            config_overrides=config_overrides,
            ui_state=ui_state,
            last_opened_at=last_opened_at,
            pinned_at=pinned_at,
        )

    def get_effective_config(self, global_config):
        """Return config with workspace overrides applied over global config.

        Args:
            global_config: dict from config.load()
        Returns:
            dict with workspace overrides deep-merged on top of global config
            so a nested override (e.g. ``{"pipeline": {"w_focus": 0.5}}``)
            replaces only the named leaf, not the whole parent dict.
        """
        from config import _deep_merge

        ws = self.get_workspace(self._active_workspace_id)
        if not ws or not ws["config_overrides"]:
            return global_config
        try:
            overrides = json.loads(ws["config_overrides"]) if isinstance(ws["config_overrides"], str) else ws["config_overrides"]
            if not isinstance(overrides, dict):
                return global_config
            return _deep_merge(global_config, overrides)
        except (json.JSONDecodeError, TypeError):
            return global_config

    def browse_stack_settings(self, global_config=None):
        """Resolve Browse's stack settings for the active workspace.

        Reads through ``get_effective_config`` so a per-workspace override of
        ``browse_stack_time_gap`` / ``browse_stack_split_mode`` applies — a
        workspace of 20fps flight sequences and one of hand-held portraits
        want different gaps.
        """
        if global_config is None:
            return normalize_browse_stack_config(None)
        return normalize_browse_stack_config(
            self.get_effective_config(global_config)
        )

    def min_detector_confidence_across_workspaces(self, global_config):
        """Return the minimum effective ``detector_confidence`` across all
        workspaces.

        Used by global mask-storage endpoints (``/api/storage/masks`` and
        ``/api/storage/masks/delete-stale``). Stale-mask scoring depends
        on the floor under which a detection is treated as too noisy to
        re-extract from. Picking the *active* workspace's floor would
        mean switching workspaces changes the global deletion set —
        masks valid under another workspace's lower floor could be
        deleted just because the user happened to be in a stricter
        workspace at the moment. The minimum is the most permissive
        view: a mask is only considered globally stale when **no**
        workspace would still consider it fresh.
        """
        from config import _deep_merge

        default = global_config.get("detector_confidence", 0.2)
        try:
            default = float(default)
        except (TypeError, ValueError):
            default = 0.2
        workspaces = self.get_workspaces()
        if not workspaces:
            return default
        values = []
        for ws in workspaces:
            overrides_raw = ws["config_overrides"]
            if not overrides_raw:
                values.append(default)
                continue
            try:
                overrides = (
                    json.loads(overrides_raw)
                    if isinstance(overrides_raw, str)
                    else overrides_raw
                )
            except (json.JSONDecodeError, TypeError):
                values.append(default)
                continue
            if not isinstance(overrides, dict):
                values.append(default)
                continue
            merged = _deep_merge(global_config, overrides)
            v = merged.get("detector_confidence", default)
            try:
                values.append(float(v))
            except (TypeError, ValueError):
                values.append(default)
        return min(values) if values else default

    def get_subject_types(self) -> set[str]:
        """Return the keyword types that count as 'identified' for the active
        workspace.

        config_overrides is arbitrary JSON (api_update_workspace persists
        whatever the client sends), so subject_types entries may not be
        strings — guard the membership test against unhashable values
        (nested lists/objects) to avoid TypeError downstream in callers
        like collection filtering and the classify skip-gate.
        """
        import config as cfg
        effective = self.get_effective_config(cfg.load())
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

    def filter_out_subject_tagged(self, photo_ids, subject_types):
        """Return the subset of photo_ids whose photos do NOT have any keyword
        of a type in subject_types. Empty subject_types or empty photo_ids
        returns photo_ids unchanged (preserving input order).

        Photo ids are chunked under SQLite's bind-variable limit so callers
        can safely pass arbitrarily large lists. The classify job sources
        photo ids from get_collection_photos(per_page=999999), which can
        exceed older SQLite builds' 999-variable cap and trip
        OperationalError: too many SQL variables.

        When ``'taxonomy'`` is among the requested types, legacy species rows
        (``is_species=1`` with a non-taxonomy ``type``) also count as
        subject-tagged. Upgraded databases carry these rows until the
        background ``mark_species_keywords`` pass retypes them; without this
        guard, already-identified photos would still be classified and would
        appear in 'Needs Identification' during that window.
        """
        return self._keyword_repository().filter_out_subject_tagged(photo_ids, subject_types)

    def filter_out_wildlife_excluded(self, photo_ids):
        """Return photo ids not explicitly excluded from wildlife classification."""
        return self._photos_repository(scoped=False).filter_out_wildlife_excluded(
            photo_ids, self._FILTER_SUBJECT_CHUNK,
        )

    def get_workspace_active_labels(self):
        """Return the active_labels list from workspace config_overrides, or None."""
        ws = self.get_workspace(self._ws_id())
        if not ws or not ws["config_overrides"]:
            return None
        try:
            overrides = json.loads(ws["config_overrides"]) if isinstance(ws["config_overrides"], str) else ws["config_overrides"]
            if not isinstance(overrides, dict):
                return None
            labels = overrides.get("active_labels")
            return labels if isinstance(labels, list) else None
        except (json.JSONDecodeError, TypeError):
            return None

    def get_tabs(self):
        """Return the active workspace's ordered list of pinned tab nav-ids.

        Entries not in ``ALL_NAV_IDS`` are dropped so that pages retired in
        past releases (e.g. ``zoom_test``) don't leave dead slots in the
        navbar's ``TABS`` array — a dead id makes cmd+number reserve a slot
        that renders nothing and makes ``adjacentTabId()`` return an id that
        ``pageById`` doesn't know, which throws on close-adjacent.
        """
        return self._workspace_repository().get_tabs()

    def set_tabs(self, tabs):
        """Replace the active workspace's tabs with the given ordered list.

        Validates every entry against ALL_NAV_IDS. Rejects duplicates so the
        UI invariant "each pinned page appears exactly once" is enforced at
        the storage layer.
        Returns the new list.
        """
        return self._workspace_repository().set_tabs(tabs)

    def pin_tab(self, nav_id):
        """Append nav_id to the active workspace's tabs if not present.

        Raises ValueError if nav_id is not in ALL_NAV_IDS.
        Returns the new list.
        """
        return self._workspace_repository().pin_tab(nav_id)

    def unpin_tab(self, nav_id):
        """Remove nav_id from the active workspace's tabs if present.

        Raises ValueError if nav_id is not in ALL_NAV_IDS.
        Returns the new list.
        """
        return self._workspace_repository().unpin_tab(nav_id)

    def _workspace_repository(self, *, scoped=True):
        """Build the workspace repository on this connection.

        ``scoped=True`` binds it to the active workspace (raising
        ``RuntimeError`` when none is set); catalog-wide methods pass
        ``scoped=False`` and take the workspace id as an argument.
        """
        from repositories.workspaces import WorkspaceRepository

        return WorkspaceRepository(
            self.conn,
            self._ws_id() if scoped else None,
            allowed_nav_ids=ALL_NAV_IDS,
            default_tabs=DEFAULT_TABS,
            nav_id_aliases=NAV_ID_ALIASES,
            chunk_size=_SQLITE_PARAM_CHUNK_SIZE,
        )

    def set_workspace_group_state(self, workspace_id, fingerprint, when_ts):
        """Record that grouping completed for `workspace_id` at `when_ts`
        with the given `fingerprint`. Pipeline page treats fingerprint
        mismatch as "Outdated" so the user knows a regroup is pending.
        """
        self._workspace_repository(scoped=False).set_group_state(
            workspace_id, fingerprint, when_ts
        )

    def set_workspace_active_labels(self, labels_files):
        """Store active_labels in the workspace's config_overrides."""
        ws = self.get_workspace(self._ws_id())
        overrides = {}
        if ws and ws["config_overrides"]:
            try:
                overrides = json.loads(ws["config_overrides"]) if isinstance(ws["config_overrides"], str) else ws["config_overrides"]
            except (json.JSONDecodeError, TypeError):
                overrides = {}
            if not isinstance(overrides, dict):
                overrides = {}
        overrides["active_labels"] = labels_files
        self.update_workspace(self._ws_id(), config_overrides=overrides)

    def forget_label_file(self, labels_file):
        """Drop a deleted label set from every workspace's selection.

        Deleting a set in Settings removes the file and the global active
        list, but a workspace override pointing at it used to survive —
        a selection naming a file that no longer exists, which no
        checkbox can clear because the UI only lists files it can find.
        Returns the number of workspaces changed.
        """
        return self._workspace_repository(scoped=False).forget_label_file(labels_file)

    def delete_workspace(self, workspace_id):
        """Delete a workspace and all its scoped data (cascade)."""
        self._workspace_repository(scoped=False).delete(workspace_id)
        # Drop any cached new-images payload for this workspace. Without this,
        # if the deleted id is later reused by SQLite for a new workspace,
        # ``get_new_images_for_workspace`` could serve the prior workspace's
        # data until TTL expiry.
        self._new_images_cache.invalidate_workspaces(self._db_path, [workspace_id])

    def _folder_subtree_ids_by_path(self, folder_id):
        """Return folder_id plus known descendants, using paths as fallback.

        Older databases can contain child folders whose ``parent_id`` is NULL
        even though their paths clearly live below a parent. Path-prefix
        matching keeps recursive workspace roots working for those rows too.
        Also includes folders whose ``local_folder_mappings.source_path`` lies
        under the target: staging rebases a descendant's ``folders.path`` under
        ``local-folders/``, so a pure ``folders.path`` walk would miss it and
        leave a later ancestor link with no ``workspace_folders`` row for the
        rebased descendant (:func:`workspace_local_root_ids` and
        :func:`affected_workspace_ids` both key off that link).
        """
        return self._folder_repository(scoped=False).subtree_ids_by_path(
            folder_id,
            local_source_descendant_ids=self._local_source_descendant_ids,
        )

    def _local_source_descendant_ids(self, root_path):
        """Return folder ids whose local_folder_mappings.source_path is under root_path.

        Staging rebases a descendant folder's ``folders.path`` under
        ``local-folders/`` but preserves the original location in
        ``local_folder_mappings.source_path``. Callers that walk descendants by
        ``folders.path`` (workspace folder linking / materialization) union
        this in so an ancestor added *after* the rebase still discovers the
        rebased descendant; otherwise later ``workspace_status``,
        ``affected_workspace_ids``, and ``workspace_local_root_ids`` reads
        would omit that workspace and the UI would report the ancestor root
        as fully remote.
        """
        return self._folder_repository(scoped=False).local_source_descendant_ids(root_path)

    def _workspace_folder_repository(self):
        """Build the workspace-folder membership repository on this connection.

        Every membership method takes its workspace id explicitly, so the
        repository is not bound to the active workspace; the wrappers that
        default to it call ``_ws_id()`` themselves.
        """
        from repositories.workspace_folders import WorkspaceFolderRepository

        return WorkspaceFolderRepository(
            self.conn,
            path_for_subtree_match=_path_for_subtree_match,
            chunk_size=_SQLITE_PARAM_CHUNK_SIZE,
        )

    def _add_workspace_folder_no_commit(
            self, workspace_id, folder_id, *, is_root=True, restore_removed=False):
        """Link a folder + descendants to a workspace WITHOUT committing.

        Same body as ``add_workspace_folder`` minus the ``commit()`` and cache
        invalidation. Intended for callers that already run inside a larger
        try/except+rollback transaction (e.g. ``merge_staged_tree_into_
        archive``) where a mid-body commit would break rollback safety by
        persisting a preceding UPDATE that an outer failure was meant to
        undo. The caller is responsible for committing (and for invalidating
        the workspace's new-images cache) after its own transaction closes.
        Internal merges preserve removed descendants unless the caller
        explicitly asks to restore them.
        """
        folder_ids = self._folder_subtree_ids_by_path(folder_id)
        if not restore_removed:
            removed = self._removed_workspace_folder_ids(workspace_id)
            # The directly imported/scanned folder is intentional. Known
            # descendants need their own scan or explicit add to restore
            # them; registering a parent must not resurrect missing rows.
            folder_ids = [fid for fid in folder_ids if fid == folder_id or fid not in removed]
        self._workspace_folder_repository().add_no_commit(
            workspace_id, folder_id, folder_ids,
            is_root=is_root, restore_removed=restore_removed)

    def add_workspace_folder(self, workspace_id, folder_id, *, is_root=True,
                             restore_removed=True):
        """Link a folder and descendants, restoring explicit removals by default.

        Scanner registration passes ``restore_removed=False``: only the
        directly visited folder restores membership, not removed descendants.
        """
        self._add_workspace_folder_no_commit(
            workspace_id, folder_id, is_root=is_root, restore_removed=restore_removed)
        self._workspace_folder_repository().commit()
        # The folder's untracked files now count toward this workspace's
        # new-images backlog. Drop any stale cached payload so the next read
        # recomputes against the updated folder set.
        self._new_images_cache.invalidate_workspaces(self._db_path, [workspace_id])

    def add_workspace_folder_exact(
            self, workspace_id, folder_id, *, is_root=False):
        """Link exactly one folder to a workspace, without its descendants.

        Scanner-owned restricted scopes use this when the caller has already
        selected the exact folders being touched. The regular
        :meth:`add_workspace_folder` deliberately materializes a whole known
        subtree for user-selected roots; using it here could attach unrelated
        catalog folders that happen to sit below the same filesystem parent.
        """
        self._workspace_folder_repository().add_exact(
            workspace_id, folder_id, is_root=is_root)
        self._new_images_cache.invalidate_workspaces(
            self._db_path, [workspace_id],
        )

    def _removed_workspace_folder_ids(self, workspace_id):
        return self._workspace_folder_repository().removed_ids(workspace_id)

    def _folder_removal_root_ids(self, folder_ids):
        """Find topmost surviving paths without walking every subtree again."""
        return self._workspace_folder_repository().removal_root_ids(folder_ids)

    def _remember_workspace_folder_removals(self, workspace_id, folder_ids, *, recursive=False):
        """Record removals in the caller's unlink/delete transaction."""
        folder_ids = list(folder_ids)
        roots = self._folder_removal_root_ids(folder_ids) if recursive else set()
        self._workspace_folder_repository().remember_removals(
            workspace_id, folder_ids, roots, recursive=recursive)

    def remove_workspace_folder(self, workspace_id, folder_id):
        """Unlink a single folder from a workspace."""
        self._remember_workspace_folder_removals(workspace_id, [folder_id])
        self._workspace_folder_repository().remove(workspace_id, folder_id)
        # The folder no longer contributes to this workspace's new-images
        # backlog. Drop the cached payload so the banner reflects the change.
        self._new_images_cache.invalidate_workspaces(self._db_path, [workspace_id])

    def remove_workspace_folder_tree(self, workspace_id, folder_id):
        """Unlink a folder and its path descendants from a workspace."""
        folder_ids = self._folder_subtree_ids_by_path(folder_id)
        self._remember_workspace_folder_removals(workspace_id, folder_ids, recursive=True)
        self._workspace_folder_repository().remove_tree(workspace_id, folder_ids)
        # The folder no longer contributes to this workspace's new-images
        # backlog. Drop the cached payload so the banner reflects the change.
        self._new_images_cache.invalidate_workspaces(self._db_path, [workspace_id])

    def _materialize_workspace_descendants(self, workspace_id):
        """Ensure linked folders include all known path descendants.

        Also picks up descendants whose ``folders.path`` was rebased under
        ``local-folders/`` by staging: a pure ``folders.path`` walk from the
        ancestor root no longer reaches them, but their
        ``local_folder_mappings.source_path`` still records the original
        location and lets us bridge the gap.
        """
        repo = self._workspace_folder_repository()
        candidate_ids = repo.unlinked_descendant_ids(workspace_id)
        for root_path in repo.linked_paths(workspace_id):
            candidate_ids.update(self._local_source_descendant_ids(root_path))
        if candidate_ids:
            candidate_ids -= repo.linked_ids(workspace_id)
        candidate_ids -= self._removed_workspace_folder_ids(workspace_id)
        if not candidate_ids:
            return
        repo.link_descendants(workspace_id, candidate_ids)
        self._new_images_cache.invalidate_workspaces(self._db_path, [workspace_id])

    def mark_workspace_folder_roots(self, workspace_id, folder_ids):
        """Mark specific linked folders as user-facing roots."""
        self._workspace_folder_repository().mark_roots(workspace_id, folder_ids)

    def get_workspace_folders(self, workspace_id):
        """Return all explicit folder links for a workspace.

        Parent folders are recursive roots: if a linked folder has known
        descendants in ``folders``, keep those descendants linked internally so
        existing workspace-scoped photo queries continue to work.
        """
        self._materialize_workspace_descendants(workspace_id)
        return self._workspace_folder_repository().list_folders(workspace_id)

    def get_folder_workspaces(self, folder_id):
        """Return every workspace in which ``folder_id`` is visible.

        Include direct links plus read-only inheritance from recursive roots.
        Do not materialize the inferred descendant row: some import and repair
        paths create deliberately restricted exact non-root links that must not
        expand merely because the user inspected a folder's memberships.
        """
        return self._workspace_folder_repository().list_workspaces_for_folder(folder_id)

    def get_workspace_root_folder_ids(self, workspace_id=None):
        """Return just the ids of the workspace's user-facing roots.

        ``get_workspace_folder_roots`` computes a per-root subtree photo
        count with a correlated prefix scan over ``photos`` — sub-second on
        a small catalog, ~0.75s on an 88k-photo library. Pollers that only
        need to know *which* roots exist (the Work Locally blocker poll runs
        every 15s on Browse) must not pay for counts they discard.

        Defaults to the active workspace (raising ``RuntimeError`` when none
        is active, like every workspace-scoped accessor); an explicit
        ``workspace_id`` is accepted for parity with
        ``get_workspace_folder_roots``, whose callers already hold one.
        """
        if workspace_id is None:
            workspace_id = self._ws_id()
        # Preserve the recovery side effect of get_workspace_folder_roots:
        # a descendant can be discovered after its recursive ancestor was
        # linked, and blocker polling must make that relationship visible to
        # workspace_local_root_ids even though this fast path skips photo
        # counts. This only writes when an unmaterialized descendant exists.
        self._materialize_workspace_descendants(workspace_id)
        return self._workspace_folder_repository().root_ids(workspace_id)

    def get_workspace_folder_roots(self, workspace_id):
        """Return user-facing workspace roots, hiding covered descendants.

        Each row carries ``workspace_photo_count``: the number of photos this
        root contributes to the workspace, counting the whole subtree (the
        root plus all of its descendant folders that are linked to this
        workspace), not just photos sitting directly in the root. This matches
        what the user actually sees in the workspace — visibility is scoped by
        ``workspace_folders`` membership, so a descendant detached from the
        workspace is correctly excluded. The ``folders.photo_count`` column is
        a direct-only count and would read as a misleading "0 photos" for a
        root whose images all live in subfolders.

        Descendants whose ``folders.path`` has been rebased under
        ``local-folders/`` by staging are matched via
        ``local_folder_mappings.source_path`` too, otherwise the ancestor
        workspace's root would underreport its photos while
        ``workspace_folders`` still makes them visible.
        """
        self._materialize_workspace_descendants(workspace_id)
        return self._workspace_folder_repository().roots(workspace_id)

    def get_workspace_extensions(self):
        """Return distinct lowercased file extensions for photos in the
        active workspace, sorted alphabetically.

        Used by the smart-collection rule editor to populate the Extension
        value dropdown — a free-text input silently failed when users typed
        ``JPG`` instead of ``.jpg`` or vice versa. Lowercasing here means the
        UI never has to think about case, and storing-side variations
        (``.jpg`` vs ``.JPG`` from older imports) collapse into one option.
        Empty/NULL extensions are skipped.

        Folders whose status is not ``'ok'`` or ``'partial'`` are excluded so
        the dropdown stays consistent with ``_build_collection_query``, which
        joins on the same status filter. Otherwise an extension found only in
        a missing folder would appear as a selectable option but match zero
        photos when used in a rule — exactly the silent-failure mode this
        change is meant to prevent.
        """
        ws = self._ws_id()
        return self._workspace_folder_repository().extensions(ws)

    def move_folders_to_workspace(self, source_ws_id, target_ws_id, folder_ids):
        """Move folders and their workspace-scoped data to another workspace.

        Moves: workspace_folders rows, pending_changes, prediction_review,
        photo_preferences, and species_highlights. Detections and predictions
        are global (no workspace_id), so they follow the folder via
        workspace_folders membership rather than being reassigned. Collections
        and edit_history stay behind.

        Returns:
            dict with keys: folders_moved, pending_changes_moved,
            photo_preferences_moved
        """
        if not self.get_workspace(source_ws_id):
            raise ValueError(f"Source workspace {source_ws_id} not found")
        if not self.get_workspace(target_ws_id):
            raise ValueError(f"Target workspace {target_ws_id} not found")
        if source_ws_id == target_ws_id:
            raise ValueError("Source and target workspace are the same")

        source_folders = self.get_workspace_folders(source_ws_id)
        source_folder_ids = {f["id"] for f in source_folders}
        for fid in folder_ids:
            if fid not in source_folder_ids:
                raise ValueError(
                    f"Folder {fid} does not belong to source workspace {source_ws_id}"
                )

        if not folder_ids:
                return {
                    "folders_moved": 0,
                    "pending_changes_moved": 0,
                    "photo_preferences_moved": 0,
                    "species_highlights_moved": 0,
                }

        selected_folder_ids = set(folder_ids)
        source_folder_paths = {
            folder["id"]: _path_for_subtree_match(folder["path"])
            for folder in source_folders
            if folder["path"]
        }
        remaining_source_paths = [
            path
            for fid, path in source_folder_paths.items()
            if fid not in selected_folder_ids
        ]
        for fid in selected_folder_ids:
            selected_path = source_folder_paths.get(fid)
            if selected_path and any(
                selected_path.startswith(path + "/") for path in remaining_source_paths
            ):
                raise ValueError(
                    "Cannot move a folder that is covered by another source "
                    "workspace folder; move the covering folder or remove it first"
                )

        moved_folder_ids = []
        seen_folder_ids = set()
        for fid in folder_ids:
            for subtree_id in self._folder_subtree_ids_by_path(fid):
                if subtree_id in source_folder_ids and subtree_id not in seen_folder_ids:
                    seen_folder_ids.add(subtree_id)
                    moved_folder_ids.append(subtree_id)

        (
            pending_changes_moved,
            photo_preferences_moved,
            species_highlights_moved,
        ) = self._workspace_folder_repository().move_folders(
            source_ws_id, target_ws_id, folder_ids, moved_folder_ids,
        )

        # Folders changed membership for BOTH workspaces, so each workspace's
        # new-images backlog needs to be recomputed on the next read.
        self._new_images_cache.invalidate_workspaces(
            self._db_path, [source_ws_id, target_ws_id]
        )

        return {
            "folders_moved": len(folder_ids),
            "pending_changes_moved": pending_changes_moved,
            "photo_preferences_moved": photo_preferences_moved,
            "species_highlights_moved": species_highlights_moved,
        }

    def ensure_default_workspace(self):
        """Create the Default workspace if it doesn't exist. Returns its id."""
        default_id = self._workspace_repository(scoped=False).default_id()
        if default_id is not None:
            return default_id
        return self.create_workspace("Default")

    def ensure_default_genre_keywords(self):
        """Insert the default genre keywords if none exist of type='genre'.

        Idempotent: a single existing genre keyword (user-created or otherwise)
        short-circuits the insert. Keywords are global, so this runs once per
        database (not per workspace).

        Upgrade path: if a same-name top-level keyword exists with type='general'
        (legacy free-form tag with the same name as a default genre), promote
        it to type='genre' rather than silently leaving it as 'general'. Other
        explicit user types (individual, location) are preserved — the user
        meant something specific.
        """
        return self._keyword_repository().ensure_default_genres()

    _RETIRED_WILDLIFE_GENRE_KEY = "retired_builtin_wildlife_genre_v1"

    def retire_builtin_wildlife_genre(self, force=False):
        """Detach the retired built-in ``Wildlife`` genre from photos.

        Older Vireo versions attached a top-level, ``type='genre'`` Wildlife
        keyword whenever a photo received its first taxonomy keyword. That
        duplicated the taxonomy fact and exposed a misleading independent
        removal action. Wildlife-processing eligibility now lives solely in
        ``photos.wildlife_excluded``.

        Associations are retired only from photos that also carry a taxonomy
        or legacy-species keyword, which is the shape the old automatic rule
        produced. A Wildlife genre on a photo without species metadata may be
        user-authored and is preserved. For retired associations a flat-only
        sidecar removal is queued. Flat-only is important: a user may have a
        real hierarchy such as ``Wildlife|Birds|House Sparrow``; retiring the
        generated flat term must not delete that hierarchy.

        Provenance is *latched*, not re-derived on every pass. Each run first
        stamps ``photo_keywords.source = 'manual'`` on any Wildlife
        association carrying authorship evidence — a not-yet-synced pending
        add, a retained ``keyword_add`` edit, a ``discard`` record for a
        manual add (``/api/sync/discard`` deliberately leaves no sidecar), or
        a flat ``Wildlife`` term in a readable XMP sidecar — commits that, and
        only then considers deletions. The latch matters because every one of
        those signals is transient (``pending_changes`` clears on sync,
        ``edit_history`` is pruned to ``max_edit_history``) while this
        migration legitimately re-runs across sessions: an offline sidecar
        defers the completion marker, and ``force=True`` re-runs it outright.
        Re-deriving authorship from an eroding record means the same
        association can read as manual on one startup and generated on the
        next; a column on the association cannot be pruned, so the first
        run's verdict is the last word. Associations created from here on are
        stamped at write time by ``tag_photo(..., source='manual')``, so the
        evidence hunt only ever applies to pre-existing rows.

        When a same-name top-level keyword of another type (e.g. an
        ``individual`` ``Wildlife`` alongside the generated ``genre`` row) or
        a preserved duplicate genre row still survives on the photo, the flat
        XMP subject represents that survivor too, so the removal is skipped to
        avoid silently stripping the user-authored tag from the sidecar; the
        generated DB association is still detached. The keyword row is
        retained so edit history, manual associations, and user-created
        children remain valid.

        When a photo's sidecar was previously imported (``xmp_mtime`` is
        set) but is currently unavailable (for example, its NAS is offline),
        the association is preserved conservatively AND the run leaves the
        catalog-wide completion marker unset so a subsequent startup — once
        the volume returns — re-inspects that photo. Otherwise the marker
        would freeze the migration in a partially-processed state and any
        genuinely generated Wildlife association on the deferred photos
        would persist forever.
        """
        if (
            not force
            and self.get_meta(self._RETIRED_WILDLIFE_GENRE_KEY) == "1"
        ):
            return 0

        wildlife_rows = self.conn.execute(
            """SELECT id, name FROM keywords
               WHERE name = 'Wildlife' COLLATE NOCASE
                 AND type = 'genre' AND parent_id IS NULL"""
        ).fetchall()
        if not wildlife_rows:
            self.set_meta(self._RETIRED_WILDLIFE_GENRE_KEY, "1")
            return 0

        keyword_ids = [row["id"] for row in wildlife_rows]
        placeholders = ",".join("?" for _ in keyword_ids)
        fallback_workspace = self._active_workspace_id
        if fallback_workspace is None:
            fallback_row = self.conn.execute(
                "SELECT MIN(id) AS id FROM workspaces"
            ).fetchone()
            fallback_workspace = fallback_row["id"] if fallback_row else None

        # Latch authorship BEFORE evaluating any deletion. Each signal below
        # lives in a table that empties or is trimmed over time
        # (``pending_changes`` clears on sync, ``edit_history`` is pruned to
        # ``max_edit_history``), while this migration may re-run on a later
        # startup — a deferred offline sidecar leaves the completion marker
        # unset, and ``force=True`` re-runs it outright. Copying the verdict
        # onto ``photo_keywords.source`` turns eroding evidence into a durable
        # fact at the earliest moment the migration can observe it.
        self.conn.execute(
            f"""UPDATE photo_keywords
                SET source = {keyword_source_max_sql(
                    "photo_keywords.source", f"'{KEYWORD_SOURCE_MANUAL}'",
                )}
                WHERE keyword_id IN ({placeholders})
                  AND (source IS NULL OR source <> 'manual')
                  AND (
                      EXISTS (
                          SELECT 1
                          FROM pending_changes pending_add
                          WHERE pending_add.photo_id = photo_keywords.photo_id
                            AND pending_add.change_type = 'keyword_add'
                            AND pending_add.value = 'Wildlife' COLLATE NOCASE
                            AND (
                                NOT EXISTS (
                                    -- Pending changes store only a name, not
                                    -- the keyword ID. Use that evidence
                                    -- directly when the association is
                                    -- unambiguous.
                                    SELECT 1
                                    FROM photo_keywords other_pk
                                    JOIN keywords other_k
                                      ON other_k.id = other_pk.keyword_id
                                    WHERE other_pk.photo_id
                                          = photo_keywords.photo_id
                                      AND other_pk.keyword_id
                                          <> photo_keywords.keyword_id
                                      AND other_k.name
                                          = 'Wildlife' COLLATE NOCASE
                                )
                                OR NOT EXISTS (
                                    -- With homonyms, an exact durable source
                                    -- or keyword_add history row identifies
                                    -- the survivor and the broad name must not
                                    -- stamp its generated sibling. If every
                                    -- exact signal has already been pruned,
                                    -- preserve all ambiguous associations:
                                    -- deleting one would risk user metadata.
                                    SELECT 1
                                    FROM photo_keywords evidenced_pk
                                    JOIN keywords evidenced_k
                                      ON evidenced_k.id
                                         = evidenced_pk.keyword_id
                                    WHERE evidenced_pk.photo_id
                                          = photo_keywords.photo_id
                                      AND evidenced_k.name
                                          = 'Wildlife' COLLATE NOCASE
                                      AND (
                                          evidenced_pk.source = 'manual'
                                          OR EXISTS (
                                              SELECT 1
                                              FROM edit_history_items exact_item
                                              JOIN edit_history exact_edit
                                                ON exact_edit.id
                                                   = exact_item.edit_id
                                              WHERE exact_item.photo_id
                                                    = photo_keywords.photo_id
                                                AND exact_edit.action_type
                                                    = 'keyword_add'
                                                AND exact_edit.undone = 0
                                                AND exact_item.new_value
                                                    = CAST(
                                                        evidenced_pk.keyword_id
                                                        AS TEXT
                                                    )
                                          )
                                      )
                                )
                            )
                      )
                      OR EXISTS (
                          SELECT 1
                          FROM edit_history_items manual_item
                          JOIN edit_history manual_edit
                            ON manual_edit.id = manual_item.edit_id
                          WHERE manual_item.photo_id = photo_keywords.photo_id
                            AND manual_edit.action_type = 'keyword_add'
                            AND manual_edit.undone = 0
                            AND manual_item.new_value
                                = CAST(photo_keywords.keyword_id AS TEXT)
                      )
                      OR EXISTS (
                          -- ``/api/sync/discard`` deliberately leaves no
                          -- sidecar but records the discarded add as
                          -- ``keyword_add:<value>`` in a ``discard`` item.
                          SELECT 1
                          FROM edit_history_items discard_item
                          JOIN edit_history discard_edit
                            ON discard_edit.id = discard_item.edit_id
                          WHERE discard_item.photo_id = photo_keywords.photo_id
                            AND discard_edit.action_type = 'discard'
                            AND discard_edit.undone = 0
                            AND discard_item.old_value
                                = 'keyword_add:Wildlife' COLLATE NOCASE
                            AND (
                                discard_item.new_value
                                    = CAST(
                                        photo_keywords.keyword_id AS TEXT
                                    )
                                OR (
                                    COALESCE(discard_item.new_value, '') = ''
                                    AND NOT EXISTS (
                                        -- Older discard rows retained only
                                        -- the name. Treat that evidence as
                                        -- exact only when no homonymous
                                        -- association makes it ambiguous.
                                        SELECT 1
                                        FROM photo_keywords discard_other_pk
                                        JOIN keywords discard_other_k
                                          ON discard_other_k.id
                                             = discard_other_pk.keyword_id
                                        WHERE discard_other_pk.photo_id
                                              = photo_keywords.photo_id
                                          AND discard_other_pk.keyword_id
                                              <> photo_keywords.keyword_id
                                          AND discard_other_k.name
                                              = 'Wildlife' COLLATE NOCASE
                                    )
                                )
                            )
                      )
                  )""",
            keyword_ids,
        )
        # Commit the latch on its own: preserving authorship must survive even
        # if the retirement pass below fails partway through.
        self.conn.commit()

        rows = self.conn.execute(
            f"""SELECT DISTINCT pk.photo_id, pk.keyword_id, pk.source,
                       p.filename, p.xmp_mtime, f.path AS folder_path,
                       EXISTS (
                           SELECT 1
                           FROM photo_keywords survivor_pk
                           JOIN keywords survivor_k
                             ON survivor_k.id = survivor_pk.keyword_id
                           WHERE survivor_pk.photo_id = pk.photo_id
                             AND survivor_k.name = 'Wildlife' COLLATE NOCASE
                             AND (
                                 survivor_k.id NOT IN ({placeholders})
                                 OR (
                                     survivor_k.id <> pk.keyword_id
                                     AND survivor_pk.source = 'manual'
                                 )
                             )
                       ) AS has_scan_survivor
                FROM photo_keywords pk
                JOIN photos p ON p.id = pk.photo_id
                JOIN folders f ON f.id = p.folder_id
                WHERE pk.keyword_id IN ({placeholders})
                  -- Authorship was latched above, so one durable predicate
                  -- replaces the pending/history/discard evidence hunt.
                  AND (pk.source IS NULL OR pk.source <> 'manual')
                  AND EXISTS (
                      SELECT 1
                      FROM photo_keywords species_pk
                      JOIN keywords species_k
                        ON species_k.id = species_pk.keyword_id
                      WHERE species_pk.photo_id = pk.photo_id
                        AND (species_k.type = 'taxonomy'
                             OR species_k.is_species = 1)
                  )""",
            [*keyword_ids, *keyword_ids],
        ).fetchall()

        # Group candidates per photo so their sidecar verdict and locked
        # retirement decision stay consistent across duplicate genre rows.
        photo_workspaces = {}
        # pid -> "manual" (readable sidecar carries a flat Wildlife term),
        # "defer" (sidecar unreadable/corrupt — decide on a later run), or
        # "generated" (readable sidecar with no Wildlife term), or "absent"
        # (no sidecar was ever imported, so a legacy NULL source cannot be
        # distinguished from metadata imported with write_xmp=False).
        sidecar_verdict_by_photo = {}
        unknown_without_sidecar_pairs = []
        deferred_sidecar = False
        for row in rows:
            pid = row["photo_id"]
            if pid not in sidecar_verdict_by_photo:
                base = os.path.splitext(row["filename"])[0]
                xmp_path = os.path.join(row["folder_path"], base + ".xmp")
                if not os.path.exists(xmp_path):
                    # A non-null mtime means a sidecar was imported earlier but
                    # is currently unavailable (for example, an offline NAS).
                    # Preserve rather than destroy metadata without being able
                    # to inspect its provenance, and flag the run as deferred
                    # so the completion marker stays unset — otherwise the
                    # catalog would be permanently frozen with this photo's
                    # generated Wildlife association still attached, even
                    # after the volume comes back online.
                    if row["xmp_mtime"] is not None:
                        sidecar_verdict_by_photo[pid] = "defer"
                        deferred_sidecar = True
                    else:
                        sidecar_verdict_by_photo[pid] = "absent"
                else:
                    # read_keywords() swallows ``ET.ParseError`` and returns
                    # an empty set for a corrupt sidecar, which is
                    # indistinguishable from a genuinely empty one. Parse
                    # explicitly so a malformed sidecar defers retirement
                    # (like the offline branch above) rather than silently
                    # classifying the tag as generated and stripping it.
                    import xml.etree.ElementTree as _ET
                    try:
                        _ET.parse(xmp_path)
                    except _ET.ParseError:
                        log.warning(
                            "Corrupt sidecar %s during Wildlife retirement; deferring",
                            xmp_path,
                        )
                        sidecar_verdict_by_photo[pid] = "defer"
                        deferred_sidecar = True
                    except Exception:
                        log.warning(
                            "Could not read sidecar %s during Wildlife retirement; deferring",
                            xmp_path,
                            exc_info=True,
                        )
                        sidecar_verdict_by_photo[pid] = "defer"
                        deferred_sidecar = True
                    else:
                        try:
                            from xmp import read_keywords

                            sidecar_verdict_by_photo[pid] = (
                                "manual"
                                if any(
                                    keyword_match_key(value) == "wildlife"
                                    for value in read_keywords(xmp_path)
                                )
                                else "generated"
                            )
                        except Exception:
                            log.warning(
                                "Could not inspect Wildlife provenance in %s; deferring",
                                xmp_path,
                                exc_info=True,
                            )
                            sidecar_verdict_by_photo[pid] = "defer"
                            deferred_sidecar = True
            verdict = sidecar_verdict_by_photo[pid]
            preserve_unknown_without_sidecar = (
                verdict == "absent" and row["source"] is None
            )
            if preserve_unknown_without_sidecar:
                # A legacy NULL-source association with no sidecar is also
                # preserved. Lightroom catalog imports historically called
                # execute_import(write_xmp=False) by default and attached the
                # keyword directly, leaving exactly the same observable shape
                # as the retired automatic rule. There is no safe retroactive
                # discriminator, so prefer metadata preservation and latch
                # the association as manual. New catalog imports are stamped
                # at write time and known generated sources can still retire.
                unknown_without_sidecar_pairs.append(
                    (pid, row["keyword_id"]),
                )
                continue
            if verdict == "defer":
                continue
            entry = photo_workspaces.setdefault(
                pid,
                {
                    "candidate_keyword_ids": set(),
                    "xmp_mtime": row["xmp_mtime"],
                    "manual_sidecar": verdict == "manual",
                    "scan_survivor": bool(row["has_scan_survivor"]),
                },
            )
            entry["candidate_keyword_ids"].add(row["keyword_id"])
            entry["scan_survivor"] = (
                entry["scan_survivor"]
                or bool(row["has_scan_survivor"])
            )

        if unknown_without_sidecar_pairs:
            self.conn.executemany(
                "UPDATE photo_keywords SET source = "
                + keyword_source_max_sql(
                    "photo_keywords.source", f"'{KEYWORD_SOURCE_MANUAL}'",
                )
                + " WHERE photo_id = ? AND keyword_id = ?",
                unknown_without_sidecar_pairs,
            )
            self.conn.commit()

        # Finalize in bounded writer transactions. The sidecar scan above can
        # take minutes on a large catalog, during which a person may re-add,
        # retype, or create another Wildlife keyword. Each chunk takes the
        # writer lock, revalidates those facts, queues cleanup, and deletes the
        # exact associations atomically. An edit before the lock is observed;
        # an edit after the commit follows the normal route and cancels any
        # now-stale flat removal. Bounding the chunk prevents this background
        # migration from monopolizing SQLite's single writer for the entire
        # catalog and turning otherwise-live edit requests into lock errors.
        retired_photo_ids = set()
        for photo_chunk in _chunks(
            photo_workspaces.items(),
            _WILDLIFE_RETIREMENT_WRITE_CHUNK_SIZE,
        ):
            self.conn.execute("BEGIN IMMEDIATE")
            try:
                still_retirable = {}
                for photo_id, entry in photo_chunk:
                    current_photo = self.conn.execute(
                        "SELECT xmp_mtime FROM photos WHERE id = ?",
                        (photo_id,),
                    ).fetchone()
                    if (
                        current_photo is not None
                        and current_photo["xmp_mtime"] != entry["xmp_mtime"]
                    ):
                        # An XMP-to-DB reconciliation won the race with the
                        # sidecar scan. Preserve this association and leave
                        # the marker unset so the next startup inspects the
                        # newly accepted sidecar state.
                        deferred_sidecar = True
                        continue
                    candidate_ids = {
                        keyword_id
                        for keyword_id in entry["candidate_keyword_ids"]
                        if self.conn.execute(
                            """SELECT 1
                               FROM photo_keywords candidate_pk
                               JOIN keywords candidate_k
                                 ON candidate_k.id = candidate_pk.keyword_id
                               WHERE candidate_pk.photo_id = ?
                                 AND candidate_pk.keyword_id = ?
                                 AND (candidate_pk.source IS NULL
                                      OR candidate_pk.source <> 'manual')
                                 AND candidate_k.name
                                     = 'Wildlife' COLLATE NOCASE
                                 AND candidate_k.type = 'genre'
                                 AND candidate_k.parent_id IS NULL""",
                            (photo_id, keyword_id),
                        ).fetchone() is not None
                    }
                    if candidate_ids:
                        candidate_placeholders = ",".join(
                            "?" for _ in candidate_ids
                        )
                        has_current_same_name_survivor = self.conn.execute(
                            f"""SELECT 1
                                FROM photo_keywords survivor_pk
                                JOIN keywords survivor_k
                                  ON survivor_k.id = survivor_pk.keyword_id
                                WHERE survivor_pk.photo_id = ?
                                  AND survivor_k.name
                                      = 'Wildlife' COLLATE NOCASE
                                  AND survivor_pk.keyword_id
                                      NOT IN ({candidate_placeholders})
                                LIMIT 1""",
                            [photo_id, *candidate_ids],
                        ).fetchone() is not None
                        if (
                            entry["manual_sidecar"]
                            and not has_current_same_name_survivor
                            and not entry["scan_survivor"]
                        ):
                            # The sidecar's flat Wildlife term belongs to the
                            # candidate only when no same-name association
                            # currently survives. Decide this under the writer
                            # lock: a survivor may have been added or removed
                            # while the sidecar was being read.
                            self.conn.executemany(
                                "UPDATE photo_keywords SET source = "
                                + keyword_source_max_sql(
                                    "photo_keywords.source",
                                    f"'{KEYWORD_SOURCE_MANUAL}'",
                                )
                                + " WHERE photo_id = ? AND keyword_id = ?",
                                [
                                    (photo_id, keyword_id)
                                    for keyword_id in candidate_ids
                                ],
                            )
                            continue
                        # A survivor present during the scan but absent now
                        # owned the observed sidecar term. Its concurrent
                        # removal must not promote the obsolete genre; retire
                        # the candidate and let the queued removal (or the
                        # flat-removal fallback below) clean up XMP.
                        still_retirable[photo_id] = (
                            entry,
                            candidate_ids,
                            has_current_same_name_survivor,
                        )

                for photo_id, (
                    _entry,
                    _candidate_ids,
                    has_current_same_name_survivor,
                ) in still_retirable.items():
                    if has_current_same_name_survivor:
                        # Another top-level 'Wildlife' keyword (for example,
                        # type='individual') still owns the flat XMP subject.
                        # Keep its sidecar term while detaching the generated
                        # genre association below.
                        continue
                    # Workspace links may also change while the sidecar scan
                    # is running. Re-read every current owner under this
                    # chunk's writer lock so each workspace's independent
                    # pending queue receives the cleanup.
                    ws_ids = {
                        row["workspace_id"]
                        for row in self.conn.execute(
                            """SELECT wf.workspace_id
                               FROM photos p
                               JOIN workspace_folders wf
                                 ON wf.folder_id = p.folder_id
                               WHERE p.id = ?""",
                            (photo_id,),
                        ).fetchall()
                    }
                    target_ws_ids = (
                        ws_ids
                        if ws_ids
                        else (
                            {fallback_workspace}
                            if fallback_workspace is not None
                            else set()
                        )
                    )
                    for ws_id in target_ws_ids:
                        existing_remove = self.conn.execute(
                            """SELECT 1 FROM pending_changes
                               WHERE photo_id = ? AND workspace_id = ?
                                 AND change_type IN (
                                     'keyword_remove', 'keyword_remove_flat'
                                 )
                                 AND value = 'Wildlife' COLLATE NOCASE
                               LIMIT 1""",
                            (photo_id, ws_id),
                        ).fetchone()
                        if existing_remove is None:
                            self.queue_change(
                                photo_id,
                                "keyword_remove_flat",
                                "Wildlife",
                                workspace_id=ws_id,
                                _commit=False,
                            )

                retired_associations = [
                    (photo_id, keyword_id)
                    for photo_id, (
                        _entry,
                        candidate_ids,
                        _has_current_same_name_survivor,
                    ) in still_retirable.items()
                    for keyword_id in candidate_ids
                ]
                if retired_associations:
                    # Delete only the exact candidates revalidated under this
                    # chunk's writer lock. The source predicate is repeated as
                    # a final fail-safe against future code movement.
                    self.conn.executemany(
                        """DELETE FROM photo_keywords
                           WHERE photo_id = ? AND keyword_id = ?
                             AND (source IS NULL OR source <> 'manual')""",
                        retired_associations,
                    )
                self.conn.commit()
                retired_photo_ids.update(still_retirable)
            except Exception:
                self.conn.rollback()
                raise
        # Only stamp the completion marker after a final locked population
        # check. A photo can become eligible while the sidecar scan is in
        # progress (for example, when a species is added), and XMP-to-DB
        # reconciliation uses this same writer lock from sidecar read through
        # its mtime stamp. The marker and this query therefore describe one
        # stable catalog state; any remaining or newly eligible candidate
        # leaves the marker unset for the next startup.
        self.conn.execute("BEGIN IMMEDIATE")
        try:
            remaining_candidate = self.conn.execute(
                """SELECT 1
                   FROM photo_keywords pk
                   JOIN keywords k ON k.id = pk.keyword_id
                   WHERE k.name = 'Wildlife' COLLATE NOCASE
                     AND k.type = 'genre'
                     AND k.parent_id IS NULL
                     AND (pk.source IS NULL OR pk.source <> 'manual')
                     AND EXISTS (
                         SELECT 1
                         FROM photo_keywords species_pk
                         JOIN keywords species_k
                           ON species_k.id = species_pk.keyword_id
                         WHERE species_pk.photo_id = pk.photo_id
                           AND (species_k.type = 'taxonomy'
                                OR species_k.is_species = 1)
                     )
                   LIMIT 1""",
            ).fetchone()
            if not deferred_sidecar and remaining_candidate is None:
                self.set_meta(
                    self._RETIRED_WILDLIFE_GENRE_KEY, "1", _commit=False,
                )
            self.conn.commit()
        except Exception:
            self.conn.rollback()
            raise
        return len(retired_photo_ids)

    _SPECIES_HIGHLIGHTS_BACKFILL_KEY = "species_highlights_from_preferences_backfill"
    _SPECIES_REPRESENTATIVES_BACKFILL_KEY = "species_representatives_from_preferences_backfill"

    def _species_curation_repository(self):
        """Build the species-curation repository on this connection.

        The active workspace is resolved lazily (``Database._ws_id`` is
        passed as a resolver), so methods that return early or canonicalize
        a species name first still raise at the point they always did. The
        façade methods the moved bodies call (species-name resolution,
        folder subtrees, ``set_meta``, and this domain's own public methods
        that other code composes with) are handed over bound, so
        monkeypatches of ``Database`` keep reaching the moved code.
        """
        from repositories.species_curation import SpeciesCurationRepository

        return SpeciesCurationRepository(
            self.conn,
            self._ws_id,
            chunks=_chunks,
            life_list_ancestor_suppression_clause=(
                _LIFE_LIST_ANCESTOR_SUPPRESSION_CLAUSE
            ),
            species_highlights_backfill_key=self._SPECIES_HIGHLIGHTS_BACKFILL_KEY,
            species_representatives_backfill_key=(
                self._SPECIES_REPRESENTATIVES_BACKFILL_KEY
            ),
            resolve_species_display_name=self.resolve_species_display_name,
            get_folder_subtree_ids=self.get_folder_subtree_ids,
            set_meta=self.set_meta,
            get_class_ancestors_for_taxa=self.get_class_ancestors_for_taxa,
            next_species_representative_order=(
                self._next_species_representative_order
            ),
            set_global_species_representative=(
                self._set_global_species_representative
            ),
            rename_species_representatives_species=(
                self.rename_species_representatives_species
            ),
        )

    def backfill_species_highlights_from_legacy_preferences(self):
        """One-shot backfill: seed ``species_highlights`` from legacy
        ``photo_preferences`` rows with ``purpose='highlights'``.

        Before ordered highlights existed, a "Highlights" pick was stored
        as a single ``photo_preferences`` row per (workspace, species).
        The new Highlights UI reads exclusively from ``species_highlights``,
        so upgraded databases would lose those picks — the pill/rank
        indicators and bucket ordering would not surface the old choice
        until the user manually re-added it. This copies each legacy pick
        into ``species_highlights`` at the end of any existing bucket
        (rank = MAX(rank) + 1) so pre-existing curated order is preserved
        and the legacy pick still appears as a highlight.

        Gated by a ``db_meta`` marker so it runs exactly once per DB.
        """
        return self._species_curation_repository().backfill_highlights_from_legacy_preferences()

    def _next_species_representative_order(self):
        return self._species_curation_repository().next_representative_order()

    def backfill_species_representatives_from_legacy_preferences(self):
        """One-shot backfill from old per-workspace representative rows.

        The current model stores representative markings globally and allows
        multiple photos per species. Older databases stored one row per
        (workspace, purpose, species), with ``species_representative`` taking
        precedence over ``life_list`` and ``highlights`` fallbacks. Copy those
        choices into the global list once so curated picks persist across
        workspaces after upgrade.
        """
        return self._species_curation_repository().backfill_representatives_from_legacy_preferences()

    def migrate_legacy_keyword_types(self):
        """One-shot migration of legacy keyword type names to the canonical
        enum. Idempotent — once all rows are migrated, the warm-path
        short-circuits cheaply (single SELECT 1 LIMIT 1) so this is safe to
        call from Database.__init__ on every instantiation.

        This runs before default genre seeding so old rows settle onto the
        canonical enum before same-name defaults are reconciled.
        """
        return self._keyword_repository().migrate_legacy_types()

    # -- Folders --

    def _folder_repository(self, *, scoped=True):
        """Build the folder repository on this connection.

        ``scoped=True`` binds it to the active workspace (raising
        ``RuntimeError`` when none is set) for the workspace-scoped reads;
        catalog-wide methods pass ``scoped=False``. ``commit_with_retry`` and
        the path helpers are looked up here, so monkeypatches of the ``db``
        module functions still reach the moved SQL.
        """
        from repositories.folders import FolderRepository

        return FolderRepository(
            self.conn,
            self._ws_id() if scoped else None,
            commit_with_retry=commit_with_retry,
            path_for_subtree_match=_path_for_subtree_match,
            stored_parent_path=_stored_parent_path,
            subtree_prefix=_subtree_prefix,
            subtree_relative=_subtree_relative,
            join_subtree_path=_join_subtree_path,
            chunk_size=_SQLITE_PARAM_CHUNK_SIZE,
        )

    def add_folder(self, path, name=None, parent_id=None, *,
                   workspace_root=True, link_to_workspace=True):
        """Insert a folder. Automatically links it to the active workspace.

        ``workspace_root`` controls whether that automatic link is a
        user-facing workspace root. Scanner-discovered descendants pass
        ``False`` so recursive roots do not expand over time.

        ``link_to_workspace`` controls whether the auto-link happens at
        all. Restricted scans (a subfolder of an existing archive tree)
        pass ``False`` for the parent chain leading up to the restrict
        roots: those parents only need to exist for ``folders.parent_id``
        integrity, and letting ``add_workspace_folder`` fire would
        subtree-cascade every pre-existing descendant of the destination
        into the active workspace. See PR #1107 review.

        Returns the folder id.
        """
        folder_id = self._folder_repository(scoped=False).add(path, name, parent_id)
        # Auto-link to active workspace
        if link_to_workspace and self._active_workspace_id is not None:
            self.add_workspace_folder(
                self._active_workspace_id,
                folder_id,
                is_root=workspace_root,
                restore_removed=False,
            )
        return folder_id

    def get_folder_tree(self):
        """Return folders for the active workspace.

        Includes folders whose status is ``'ok'`` or ``'partial'`` — a
        partially-scanned folder must stay in the tree so the browse sidebar
        can render its badge and the user can trigger a rescan. ``'missing'``
        folders are still excluded (they go through ``get_missing_folders``).

        ``parent_id`` is rewritten to the nearest ancestor that is also linked
        to the active workspace AND visible here. If no such ancestor exists,
        ``parent_id`` is NULL. This keeps the returned set a well-formed tree:
        callers that group by ``parent_id`` (notably the browse-page folder
        sidebar) never leave a linked folder dangling under an ancestor that
        was filtered out of the result.
        """
        return self._folder_repository().tree()

    def get_folder_subtree_ids(self, folder_id):
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

    def get_folder(self, folder_id):
        """Return a single folder row by id, or None if not found.

        Not scoped to the active workspace — callers that need workspace
        scoping should additionally verify membership via
        ``workspace_folders``.
        """
        return self._folder_repository(scoped=False).get(folder_id)

    def check_folder_health(self):
        """Check all folders for existence on disk. Update status column.

        ``'partial'`` is preserved while the path still exists on disk — only
        a successful rescan clears it. Otherwise the 10-minute health loop
        would auto-promote a partially-scanned folder back to ``'ok'`` and
        users would lose the visible marker that tells them to rescan. If
        the disk path is gone we still flip to ``'missing'`` regardless of
        prior status, since rescanning won't recover data that isn't there.

        Returns the number of folders whose status changed.
        """
        return self._folder_repository(scoped=False).check_health()

    # -- Library integrity verification --

    def _audit_repository(self, *, scoped=True):
        """Build the audit repository on this connection.

        ``scoped=True`` binds it to the active workspace (raising
        ``RuntimeError`` when none is set); the catalog-wide hash-check
        write passes ``scoped=False`` and takes the photo id as an argument.
        """
        from repositories.audit import AuditRepository

        return AuditRepository(
            self.conn,
            self._ws_id() if scoped else None,
            chunk_size=_SQLITE_PARAM_CHUNK_SIZE,
        )

    def record_audit_run(self, check_name, problem_count):
        """Record that an audit check ran now and what it found.

        One row per (workspace, check); re-running a check overwrites its
        previous row. The audit summary reads these to decide whether the
        archive can honestly be called intact.
        """
        self._audit_repository().record_run(check_name, problem_count)

    def get_audit_runs(self):
        """Return {check_name: {ran_at, problem_count}} for this workspace."""
        return self._audit_repository().get_runs()

    def get_integrity_photos(self):
        """Return workspace photos with the fields hash verification needs."""
        return self._audit_repository().get_integrity_photos()

    def get_integrity_flagged(self):
        """Return workspace photos whose last hash check found a problem."""
        return self._audit_repository().get_integrity_flagged()

    def get_integrity_stats(self):
        """Return hash-verification coverage for the active workspace.

        ``unchecked`` is load-bearing for the summary banner: photos added
        after the last verify run have hash_checked_at NULL, so a green
        light can't silently cover files that were never re-hashed.
        """
        return self._audit_repository().get_integrity_stats()

    def update_photo_hash_check(self, photo_id, status, file_hash=None,
                                commit=True, clear_file_hash=False):
        """Record a hash-verification verdict for one photo.

        When ``file_hash`` is given the stored baseline is replaced too
        (first-time baselining, or the user accepting an external edit).
        Set ``clear_file_hash=True`` to explicitly NULL the stored hash:
        used for zero-byte files so ``EMPTY_FILE_SHA256`` never lands in
        the ``file_hash`` column (it would otherwise collide as an exact
        duplicate of every other empty placeholder).
        """
        self._audit_repository(scoped=False).update_photo_hash_check(
            photo_id, status, file_hash=file_hash, commit=commit,
            clear_file_hash=clear_file_hash,
        )

    def get_missing_folders(self):
        """Return missing folders in the active workspace with photo counts."""
        return self._folder_repository().missing()

    def get_folder_health_version(self):
        """Return the monotonic version for folder-health-visible changes."""
        value = self.get_meta("folder_health_version")
        return int(value or 0)

    def get_missing_photos(
        self,
        folder_id=None,
        progress_callback=None,
        cancel_callback=None,
    ):
        """Return photos whose source file is missing from disk.

        Scoped to the active workspace. Skips photos in folders flagged
        ``'missing'`` — those are surfaced by ``get_missing_folders`` and
        listing them per-photo would just duplicate that signal at high cost.

        When ``folder_id`` is given, the result is further restricted to
        that folder and every folder beneath it in the tree. This backs the
        "rescan a specific folder" flow — the user asked about one folder,
        so the deleted-original review must not surface ghosts from unrelated
        parts of the library. Descendant discovery goes through
        ``_folder_subtree_ids_by_path`` so legacy rows whose ``parent_id`` is
        NULL still count as descendants of their path-prefixed root. ``None``
        (the default) keeps the whole-workspace behavior.

        Folder DB ``status`` is updated asynchronously by a 10-minute health
        loop, so a freshly unmounted volume can still show ``status='ok'``
        when this query runs. To avoid surfacing thousands of "ghosts" for
        a temporarily offline drive (and offering them up for bulk delete),
        we also treat any folder whose root no longer resolves on disk as
        if it were already flagged missing. Resolution is cached per folder
        within the call so a 1000-photo folder doesn't stat the same root
        a thousand times.

        Each row carries ``folder_path``, ``timestamp``, and
        ``working_copy_path`` so the caller can render rich UI without
        joining again.

        ``progress_callback`` is optional. When supplied, it receives dicts
        containing ``folders_checked``, ``photos_considered``, ``missing_found``,
        ``total_photos``, and ``current_folder``. Callback exceptions are
        logged and ignored so progress reporting cannot abort detection.

        ``cancel_callback`` is optional. When supplied, it is polled between
        filesystem operations and may abort the scan by returning true.
        """
        def check_cancelled():
            if cancel_callback is not None and cancel_callback():
                raise MissingPhotosCancelled("missing photos scan cancelled")

        check_cancelled()
        params = [self._ws_id()]
        subtree_clause = ""
        if folder_id is not None:
            # Restrict to the folder subtree. Path-prefix expansion (the same
            # helper used by add_workspace_folder etc.) covers legacy rows
            # whose parent_id is NULL — a plain recursive walk over parent_id
            # would silently drop those subfolders.
            subtree_ids = self._folder_subtree_ids_by_path(folder_id)
            if not subtree_ids:
                return []
            if len(subtree_ids) <= _SQLITE_PARAM_CHUNK_SIZE:
                placeholders = ",".join("?" for _ in subtree_ids)
                subtree_clause = f" AND f.id IN ({placeholders})"
                params.extend(subtree_ids)
            else:
                # A workspace root with thousands of descendant folders would
                # overflow SQLITE_MAX_VARIABLE_NUMBER (999 on legacy builds)
                # in a single IN(...) clause. Stage the ids in a
                # connection-local temp table and join through that instead.
                self._stage_scope_ids("missing_subtree_ids", subtree_ids)
                subtree_clause = (
                    " AND f.id IN (SELECT id FROM missing_subtree_ids)"
                )
        check_cancelled()
        rows = self._folder_repository(scoped=False).photos_in_present_folders(
            subtree_clause, params,
        )
        check_cancelled()
        # One readdir per folder instead of one stat per photo. On a 50k-photo
        # library across a network volume the per-photo `os.path.exists` was
        # costing minutes; a single scandir + set-membership check is orders
        # of magnitude faster and the dominant call site for this endpoint.
        # Misses fall back to a single os.path.exists to honor FS-specific
        # case rules without unconditionally case-folding (which would
        # silently collapse distinct files on case-sensitive volumes).
        folder_online: dict[int, bool] = {}
        folder_names: dict[int, set[str] | None] = {}
        missing = []
        photos_considered = 0
        folders_checked = 0
        reported_folders = set()
        progress_callback_enabled = True

        def report_progress(current_folder):
            nonlocal progress_callback_enabled
            if progress_callback is None or not progress_callback_enabled:
                return
            try:
                progress_callback({
                    "folders_checked": folders_checked,
                    "photos_considered": photos_considered,
                    "missing_found": len(missing),
                    "total_photos": len(rows),
                    "current_folder": current_folder,
                })
            except Exception:
                progress_callback_enabled = False
                log.exception("Missing photos progress callback failed")

        def report_photo_progress(current_folder):
            if photos_considered % _MISSING_PHOTOS_PROGRESS_INTERVAL == 0:
                report_progress(current_folder)

        for row in rows:
            check_cancelled()
            fid = row["folder_id"]
            if fid not in folder_online:
                folder_online[fid] = os.path.isdir(row["folder_path"])
            if not folder_online[fid]:
                # Whole folder is offline — surfaced by missing-folders flow.
                if fid not in reported_folders:
                    reported_folders.add(fid)
                    folders_checked += 1
                    report_progress(row["folder_path"])
                continue
            if fid not in folder_names:
                try:
                    names_set: set[str] = set()
                    with os.scandir(row["folder_path"]) as it:
                        for entry in it:
                            check_cancelled()
                            # Broken symlinks: scandir returns the basename even
                            # when the target is gone, but the prior os.path.exists
                            # check returned False. Filter them so missing
                            # originals tracked via symlinks still surface.
                            # is_symlink() uses cached lstat from scandir, so
                            # non-symlinks don't pay an extra stat.
                            if entry.is_symlink() and not os.path.exists(entry.path):
                                continue
                            names_set.add(_nfc(entry.name))
                    folder_names[fid] = names_set
                except OSError:
                    # Folder vanished between isdir and scandir, or unreadable;
                    # treat the same as "folder offline" so we don't bulk-flag
                    # every photo as a ghost.
                    folder_names[fid] = None
                if fid not in reported_folders:
                    reported_folders.add(fid)
                    folders_checked += 1
                    report_progress(row["folder_path"])
            names = folder_names[fid]
            photos_considered += 1
            if names is None:
                report_photo_progress(row["folder_path"])
                continue
            if _nfc(row["filename"]) in names:
                report_photo_progress(row["folder_path"])
                continue
            # NFC miss: defer to the kernel for case rules. On case-insensitive
            # volumes (APFS default, NTFS) os.path.exists resolves a
            # case-mismatched name; on case-sensitive volumes (most Linux
            # filesystems) it correctly reports the file as absent.
            check_cancelled()
            if not os.path.exists(os.path.join(row["folder_path"], row["filename"])):
                missing.append(row)
            report_photo_progress(row["folder_path"])
        check_cancelled()
        report_progress("")
        return missing

    def nearest_ancestor_folder_id(self, path, exclude_id=None):
        """Return the id of the folder whose stored path is the longest proper
        ancestor of ``path`` (platform-neutral prefix match), or None if no
        folder row is an ancestor.

        Used to keep ``parent_id`` consistent with ``path`` after relocations
        and moves. Those operations rewrite a folder's ``path`` but would
        otherwise leave ``parent_id`` pinned to the OLD location's parent,
        which mis-nests the folder in the browse tree (e.g. a date folder
        moved onto another volume staying linked to its original parent).
        Folder counts are small, so the linear scan is fine.
        """
        return self._folder_repository(scoped=False).nearest_ancestor_id(
            path, exclude_id=exclude_id,
        )

    def _relink_parents_by_path(self, folder_ids):
        """Re-derive ``parent_id`` from the current ``path`` for each folder.

        Relocations and merges rewrite ``path`` but leave ``parent_id``
        pinned to the pre-move parent, which mis-nests the folder in the
        browse tree. Call this after path rewrites — all affected paths must
        already be committed to the rows so ancestor lookup sees them.
        """
        self._folder_repository(scoped=False).relink_parents_by_path(
            folder_ids, nearest_ancestor_id=self.nearest_ancestor_folder_id,
        )

    def relocate_folder(self, folder_id, new_path):
        """Update folder path and set status to 'ok'.

        Also checks if missing child folders exist at corresponding paths
        under new_path. If they do, relocates them too.

        If new_path is already tracked by another folder, merges photos from
        the missing folder into the existing one and removes the missing folder.

        Returns list of child folder dicts that were also relocated.
        """
        return self._folder_repository(scoped=False).relocate(
            folder_id,
            new_path,
            merge_into_existing=self._merge_into_existing,
            relink_parents_by_path=self._relink_parents_by_path,
        )

    def _merge_into_existing(self, source_folder_id, target_folder_id, new_path, *, commit=True):
        """Merge photos from a missing folder into an existing folder at the same path.

        - Photos with matching filenames in the target are dropped from source
        - Other photos are reassigned to the target folder
        - The source folder entry is deleted
        - Missing child folders are cascade-relocated using old_path -> new_path

        Returns list of child folder dicts that were also relocated (same as relocate_folder).

        When ``commit`` is False the caller owns the surrounding transaction —
        used by ``services.local_workspace._restore_catalog`` so the whole
        catalog restore (merges + rebase + state-row cleanup) commits atomically.
        """
        return self._folder_repository(scoped=False).merge_into_existing(
            source_folder_id,
            target_folder_id,
            new_path,
            commit=commit,
            transfer_gps_review=self._transfer_gps_review_for_merge,
            relink_parents_by_path=self._relink_parents_by_path,
        )

    # -- Move operations --

    def _moves_merge_repository(self):
        """Build the moves/merge repository on this connection.

        Folders, photos and keywords are catalog-wide, so the repository takes
        no workspace id: the few workspace-scoped wrappers resolve
        ``self._ws_id()`` themselves (``merge_staged_tree_into_archive`` passes
        it as a callable, after its staged-root lookup). The subtree path
        helpers are ``db`` module functions and are passed in.
        """
        from repositories.moves_merge import MovesMergeRepository

        return MovesMergeRepository(
            self.conn,
            subtree_prefix=_subtree_prefix,
            subtree_relative=_subtree_relative,
            join_subtree_path=_join_subtree_path,
        )

    def create_move_rule(self, name, destination, criteria):
        """Create a saved move rule. Returns the rule id."""
        return self._moves_merge_repository().create_rule(name, destination, criteria)

    def get_move_rule(self, rule_id):
        """Return a single move rule by id."""
        return self._moves_merge_repository().get_rule(rule_id)

    def list_move_rules(self):
        """Return all saved move rules ordered by name."""
        return self._moves_merge_repository().list_rules()

    def update_move_rule(self, rule_id, name=_UNSET, destination=_UNSET, criteria=_UNSET):
        """Update fields on a move rule."""
        self._moves_merge_repository().update_rule(
            rule_id, name=name, destination=destination, criteria=criteria)

    def delete_move_rule(self, rule_id):
        """Delete a move rule."""
        self._moves_merge_repository().delete_rule(rule_id)

    def touch_move_rule(self, rule_id):
        """Update last_run_at timestamp on a move rule."""
        self._moves_merge_repository().touch_rule(rule_id)

    def batch_update_photo_folder(self, photo_ids, target_folder_id):
        """Move photos to target folder in a single transaction."""
        self._moves_merge_repository().batch_update_photo_folder(photo_ids, target_folder_id)

    def move_folder_path(self, folder_id, new_path, new_name=None):
        """Update a folder's path and cascade to all children.

        Unlike relocate_folder (which only updates missing children),
        this updates ALL child folders regardless of status. ``new_name`` is
        used when the root folder is renamed as part of the move; descendants
        keep their existing names.
        """
        self._moves_merge_repository().move_folder_path(
            folder_id, new_path, new_name,
            relink_parents_by_path=self._relink_parents_by_path,
        )

    def _active_ws_root_ancestor_exists(self, workspace_id, path):
        """True if ``workspace_id`` has an ``is_root=1`` folder that equals or
        is an ancestor of ``path``.

        Used by the merge to decide whether the archive base should itself
        become a workspace root. Comparison is platform-neutral (``\\`` folded
        to ``/``, trailing slashes stripped) so a Windows-style stored root
        still matches a forward-slash archive path.
        """
        return self._workspace_folder_repository().root_ancestor_exists(workspace_id, path)

    def _active_ws_root_descendant_exists(self, workspace_id, path):
        """True if ``workspace_id`` has a strict root descendant of ``path``."""
        return self._workspace_folder_repository().root_descendant_exists(workspace_id, path)

    def _prune_ws_nonroot_links_outside_roots(self, workspace_id, path):
        """Drop non-root links that could re-materialize ``path``'s subtree.

        Prunes uncovered non-root links that are ``path`` itself, strict
        descendants of ``path``, OR strict ancestors of ``path``. Ancestors
        matter because ``_materialize_workspace_descendants`` walks the whole
        subtree below every linked folder — a surviving non-root ancestor
        like ``/archive`` (left over from a restricted scan; see
        ``scanner.py`` ``_restrict_root_paths``) would immediately re-insert
        ``/archive/USA`` and any sibling like ``/archive/USA/2027`` after
        the caller pruned them, defeating a scoped merge into a workspace
        rooted at ``/archive/USA/2026``.
        """
        prune_ids = self._workspace_folder_repository().prune_nonroot_links_outside_roots(
            workspace_id, path)
        if prune_ids:
            self._new_images_cache.invalidate_workspaces(
                self._db_path, [workspace_id])

    def _newest_location_change_key(self, photo_id):
        """Sort key of the newest queued ``location`` change on ``photo_id``.

        ``None`` when the photo has no queued location change. The key is
        ``(created_at, id)`` so two changes queued inside the same
        ``datetime('now')`` second still order by insertion — the merge
        below decides which of two competing assignments is the user's
        latest intent, and a same-second tie must not fall back to
        "whichever row happens to be getting deleted". A NULL
        ``created_at`` (possible on rows written before the column had a
        default) sorts as the empty string, i.e. oldest.
        """
        return self._moves_merge_repository().newest_location_change_key(photo_id)

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
        loser_key = self._newest_location_change_key(losing_id)
        if loser_key is None:
            # Nothing queued on the row being deleted: its tags carry no
            # pending intent, and the survivor keeps whatever it has.
            return False
        survivor_key = self._newest_location_change_key(surviving_id)
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

    def _reconcile_conflicting_keyword_edits(self, losing_id, surviving_id):
        """Drop the older of two opposing keyword edits before a remap.

        The remap lands both rows' queued keyword changes on one photo, and
        ``sync._plan_photo_sync`` folds a photo's changes into sets. An
        ``keyword_add`` and a ``keyword_remove`` that share a normalized
        match key then look exactly like a normalization rename:
        ``_remove_planned_keywords`` strips the flat entry and
        ``add_keywords`` writes it straight back, so the keyword survives
        and BOTH queue rows are cleared as successfully written. A newer
        removal queued on one row would be silently reversed by an older
        addition queued on the other.

        Resolved the same way as the location state: the newest opposing
        intent wins, by ``(created_at, id)``, and every older opposing row
        is deleted. Resolved across every workspace AND across both
        photos, not per workspace or per photo: ``photo_keywords`` and the
        sidecar are both global, so once the remap lands both rows on one
        photo an add in workspace A and a remove in workspace B are two
        intents for one file and only the newer can be honored -- keeping
        both would just hand the outcome to whichever workspace syncs
        last. A photo that already holds an add in one workspace and a
        remove in another before the merge is the same shape once the
        merge collapses them onto the survivor, so the older row has to
        go too, not just the losing photo's row for that key.

        A photo that holds BOTH directions for a key in ONE workspace's
        queue is the deliberate rename pair ``_remove_planned_keywords``
        exists to handle, so its two rows travel as one presence-asserting
        atom -- the pair's net effect is "keep the tag with the canonical
        spelling" -- rather than as opposing halves the merge is creating.
        The pair reconciles or survives as one unit against every other
        opposing intent, by the pair's newest row. The exemption stays
        workspace-scoped -- an add in workspace A and a remove in
        workspace B on the same photo are two intents competing across
        workspaces, not a rename pair.

        The winning side's catalog state travels too. A ``keyword_add`` has
        already inserted the ``photo_keywords`` row on its photo and a
        ``keyword_remove`` has already deleted it -- the queue row only
        records what the sidecar still owes. Remapping the row alone would
        delete that association with the losing photo and leave the
        survivor reporting the opposite of what the next sync writes to
        XMP. ``keyword_remove_flat`` is deliberately excluded: it asks for
        the stale flat ``dc:subject`` line to go, not the association.

        Returns the number of queue rows dropped.
        """
        return self._moves_merge_repository().reconcile_conflicting_keyword_edits(
            losing_id, surviving_id,
            carry_keyword_associations=self._carry_keyword_associations_for_merge,
        )

    def _carry_keyword_associations_for_merge(
            self, losing_id, surviving_id, by_key):
        """Apply the losing row's winning keyword ops to the survivor's tags.

        ``by_key`` is the grouping built by
        ``_reconcile_conflicting_keyword_edits``; rows it has already
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
                for kw_id in self._photo_keyword_ids_matching(
                        losing_id, match_key):
                    self.tag_photo(surviving_id, kw_id, _commit=False)
            elif newest_loser["change_type"] == "keyword_remove":
                for kw_id in self._photo_keyword_ids_matching(
                        surviving_id, match_key):
                    self.untag_photo(surviving_id, kw_id, _commit=False)

    def _photo_keyword_ids_matching(self, photo_id, match_key):
        """Keyword ids on ``photo_id`` whose name shares ``match_key``.

        Matched in Python rather than SQL: SQLite's ``LOWER`` is ASCII-only,
        so a case or diacritic variant would slip past a SQL comparison the
        same way it does in the collision tracker above.
        """
        return self._moves_merge_repository().photo_keyword_ids_matching(photo_id, match_key)

    def _transfer_gps_review_for_merge(self, losing_id, surviving_id):
        """Carry the newest GPS keep decision across an identity merge.

        Prefer the survivor on timestamp ties. The fingerprint still has to
        match its coordinates, assigned place and sidecar before review will
        suppress a discrepancy; moving the decision cannot bless new data.
        The caller owns the transaction and deletion of the losing row.
        """
        self._moves_merge_repository().transfer_gps_review(losing_id, surviving_id)

    def _transfer_review_state_for_merge(self, losing_id, surviving_id):
        """Carry queued rating / flag state onto the survivor by chronology.

        ``photos.rating`` and ``photos.flag`` are written at edit time and
        the queue row only records what the sidecar still owes. Remapping
        the row alone would leave the survivor's catalog columns untouched:
        the next sync writes the queued value into the sidecar and clears
        the row, and the catalog is left permanently disagreeing with the
        file on disk.

        Two rows of the same type on one photo are also a problem in their
        own right -- ``sync._plan_photo_sync`` keeps whichever it folds
        last -- so every row but the newest is dropped. The catalog column
        then takes that row's value, which is exactly what the sync will
        write.

        Only queued state is compared. A rating set on the survivor and
        already synced has no row left to date it, so it loses to a
        still-queued older edit; the queue is the only evidence of intent
        that survives a sync.

        Returns the number of queue rows dropped.
        """
        return self._moves_merge_repository().transfer_review_state(
            losing_id, surviving_id,
            transfer_gps_review=self._transfer_gps_review_for_merge,
            transfer_edit_recipe=self._transfer_edit_recipe_for_merge,
        )

    def _transfer_edit_recipe_for_merge(self, losing_id, surviving_id):
        """Carry a queued edit recipe's catalog row onto the survivor.

        ``photo_edit_recipes`` is written when the edit is made and the
        queue row carries the same JSON for the sidecar. The row cascades
        away with the losing photo, so remapping the queue row alone leaves
        ``get_photo_edit_recipe`` on the survivor reporting its older
        recipe -- or none -- while the sync writes the queued one to XMP
        and clears the row. The UI and any future render then disagree with
        the sidecar.

        The queue row's ``value`` is the recipe JSON itself, so the catalog
        row is rebuilt from the newest queued change rather than copied
        across; an empty value is the "recipe cleared" edit and deletes the
        survivor's row. Duplicates within one workspace are dropped as
        elsewhere -- ``_plan_photo_sync`` keeps whichever it folds last.

        Returns the number of queue rows dropped.
        """
        return self._moves_merge_repository().transfer_edit_recipe(losing_id, surviving_id)

    def _link_survivor_for_sibling_edits(self, workspace_id, photo_id):
        """Grant a sibling workspace sync-only access to a remapped photo's folder.

        The collision loop remaps ``pending_changes`` by ``photo_id``, which
        also moves rows owned by workspaces other than the one running the
        merge. The merge links the destination subtree into the active
        workspace only, so without this a sibling workspace would be left
        holding a queued edit on a photo it cannot resolve:
        ``sync._resolve_xmp_paths`` builds its folder map from that
        workspace's ``get_folder_tree``, and a missing folder resolves to an
        empty path and fails every future sync as inaccessible — queued
        forever, with nothing saying why.

        Recorded in ``workspace_sync_only_photos`` rather than
        ``workspace_folders``. A ``workspace_folders`` link is library
        membership: every browse/library query joins on it by folder id
        alone, so any link -- root or not -- would make every photo sharing
        the survivor's folder visible in the sibling workspace, and the
        link would remain after the queued edit was synced. One queued
        edit on a colliding staged photo would permanently import an
        archive folder of unrelated photos into a workspace that never
        asked for it.

        Keyed by photo so the grant survives a later move: ``move_photos``
        rewrites ``photos.folder_id`` and knows nothing about this table, so
        a folder-keyed grant would stop applying the moment the active
        workspace moved the survivor -- the sibling's preserved edit would
        be unresolvable again, with nothing saying why.

        ``workspace_sync_only_photos`` is read only by
        ``get_sync_only_photo_paths``, which the sync engine consults
        one photo at a time (never as a folder-wide union) when building
        its sidecar-path map, and by ``_photo_syncable_in_workspace``.
        Every browse/library query stays folder-scoped through
        ``workspace_folders`` and sees no change.

        Returns ``True`` when a new grant was written.
        """
        return self._moves_merge_repository().link_survivor_for_sibling_edits(workspace_id, photo_id)

    def get_sync_only_photo_paths(self, workspace_id=None):
        """Return ``{photo_id: folders.path}`` for the workspace's sync-only grants.

        ``_resolve_xmp_paths`` uses this map to resolve one granted photo's
        sidecar at a time, keyed by the photo id -- never by the folder id.
        A folder-keyed union with the workspace's folder tree would widen
        the grant back to folder scope: an unrelated photo sitting in the
        same folder with its own inaccessible pending edit would get its
        sidecar written on the next sync, silently, without the workspace
        gaining a grant of its own. Absent from every browse/library query,
        which still joins on ``workspace_folders`` alone.

        Falls back to ``workspace_sync_only_folders`` for grants that
        pre-date the photo-keyed table (``#1661`` briefly recorded these
        by folder). The migration on upgrade recovers what it can identify,
        but ``move_photos`` clears ``last_move_source_folder_path`` after
        draining the last same-stem move from a source folder, and a photo
        moved out of the granted folder before upgrade can end up matching
        neither its current folder nor its stale provenance. The legacy
        table therefore stays around as a compatibility record, and any
        photo the sibling workspace still has a pending edge on -- that
        also sits in a legacy-granted folder or carries the provenance
        stamp for one -- is authorized here too.
        """
        if workspace_id is None:
            workspace_id = self._ws_id()
        return self._moves_merge_repository().sync_only_photo_paths(workspace_id)

    def merge_staged_tree_into_archive(self, staged_root_id, archive_path):
        """Fold a staged folder subtree into an existing tracked archive.

        The on-disk rsync merge has already happened: files that were under the
        staged root now also live under ``archive_path``. This reconciles the
        catalog so staged folder/photo rows become rows under the existing
        archive, with no duplicate ``folders.path`` and correct ``parent_id``.

        For each staged folder (root-first), the target path is the staged path
        rebased from the staged root onto ``archive_path``:

        * Target has no folder row -> repoint the staged row to the target path,
          fix its ``parent_id`` to the (now-existing) target-parent folder, and
          link it to the active workspace as a non-root (an existing archive
          root ancestor already covers it). Every staged photo in that folder is
          a newly-archived photo.
        * Target already has a folder row -> move the staged folder's photos
          into it (dropping any whose filename already exists there as an
          identical archived file), then delete the now-empty staged folder row.
          Each moved (not dropped) photo is a newly-archived photo.

        Returns a counts dict (all defined as the user-facing summary reads
        them):

        * ``new_photos`` — total staged photos newly placed into the archive,
          counting BOTH photos reparented into brand-new folders AND photos
          moved into a pre-existing target folder. This is the headline number.
        * ``new_folders`` — folders created under the archive (the staged folder
          had no pre-existing target row).
        * ``merged_folders`` — staged folders folded into a pre-existing target
          folder.
        * ``already_present`` — identical-filename staged photos dropped because
          the target folder already held that filename AND the target row's
          recorded bytes-identity (``file_hash``, falling back to ``file_size``)
          matches what's currently on disk. A filename collision whose target
          row is stale (its recorded bytes-identity doesn't match the on-disk
          file that rsync just copied into place) is treated as a phantom
          replacement instead — see the collision loop below.
        * ``dropped_photo_ids`` — staged photo ids that were deleted during
          the merge (``already_present`` collisions plus phantom target-row
          replacements). The caller passes these to
          ``cleanup_cached_files_for_deleted_photos`` so orphaned thumbnail /
          preview / working-copy files can't be inherited by a later import
          that reuses one of the freed SQLite rowids.
        * ``preserved_edit_count`` — pending edits that were queued against
          a photo the collision loop is about to delete (either the staged
          row on a byte-identical collision, or the phantom target row on a
          replacement) and were reparented onto the surviving row before
          ON DELETE CASCADE on ``pending_changes.photo_id`` could drop them.
          Reported up so a NAS transfer's residual check can add them to
          the "still need a sync" count rather than silently losing them.
        * ``preserved_off_staging_identities`` — active-workspace subset of
          the above whose survivor is NOT one of the staged photo ids the
          caller captured before the merge. Reported as a list of
          ``staged_sync_scope`` identity keys (``change_token`` or
          ``("id", id)``) so the caller can filter out edits its pre-transfer
          drain already classified as undeliverable (a flag under
          ``sync_flags_to_xmp`` off) and NOT count them as "queued during
          transfer" — they existed before, were considered, and were
          deliberately not written to XMP. Sibling-workspace edits are
          omitted for the same reason: this sync would not have written
          them either. The remaining in-staging remaps (phantom-target and
          intra-staged) are already found by the by-photo residual re-read
          scoped to the captured staged ids, so adding them here as well
          would report one edit as two.

        Every queued change type has catalog state written at edit time
        that the queue row does not carry, and all of it has to travel with
        the remap or the survivor ends up disagreeing with the sidecar the
        next sync writes:

        * ``location`` -> the photo's ``type='location'`` keyword links
          (``_move_location_state_for_merge``)
        * ``keyword_add`` / ``keyword_remove`` -> the ``photo_keywords``
          association (``_reconcile_conflicting_keyword_edits``);
          ``keyword_remove_flat`` is sidecar-only and has none
        * ``rating`` / ``flag`` -> the ``photos`` columns
          (``_transfer_review_state_for_merge``)
        * ``edit_recipe`` -> the ``photo_edit_recipes`` row
          (``_transfer_edit_recipe_for_merge``)

        Each resolves a competing change on the two rows by queue
        chronology, ``(created_at, id)``, and drops the older row so
        ``_plan_photo_sync`` is not left picking between two rows of the
        same type at random. Competition is judged across every workspace:
        the catalog state and the sidecar are both global, so two opposing
        intents on one photo cannot both be honored no matter who queued
        them.

        Side effect worth knowing about: when a remapped pending edit is
        owned by a workspace other than the active one, the survivor's
        photo is recorded in ``workspace_sync_only_photos`` for that
        workspace so the sync engine can resolve the sidecar path.
        ``workspace_folders`` is not touched -- a real library link would
        make every other photo in the survivor's folder visible in the
        sibling workspace and persist past the sync. See
        ``_link_survivor_for_sibling_edits``.
        """
        def case_insensitive_root(path):
            # Late-bound so a patched ``move._case_insensitive_root`` applies,
            # and imported at the point of use as before.
            from move import _case_insensitive_root
            return _case_insensitive_root(path)

        return self._moves_merge_repository().merge_staged_tree_into_archive(
            staged_root_id, archive_path,
            workspace_id_fn=self._ws_id,
            root_ancestor_exists=self._active_ws_root_ancestor_exists,
            root_descendant_exists=self._active_ws_root_descendant_exists,
            prune_nonroot_links_outside_roots=self._prune_ws_nonroot_links_outside_roots,
            materialize_workspace_descendants=self._materialize_workspace_descendants,
            add_workspace_folder=self.add_workspace_folder,
            add_workspace_folder_no_commit=self._add_workspace_folder_no_commit,
            case_insensitive_root=case_insensitive_root,
            move_location_state=self._move_location_state_for_merge,
            reconcile_keyword_edits=self._reconcile_conflicting_keyword_edits,
            transfer_review_state=self._transfer_review_state_for_merge,
            link_survivor_for_sibling_edits=self._link_survivor_for_sibling_edits,
            invalidate_new_images=lambda workspace_ids: (
                self._new_images_cache.invalidate_workspaces(
                    self._db_path, workspace_ids)),
            update_folder_counts=self.update_folder_counts,
        )

    def check_filename_collisions(self, photo_ids, target_folder_id):
        """Check if any photo filenames already exist in the target folder.

        Returns list of dicts with photo_id and filename for conflicts.
        """
        return self._moves_merge_repository().filename_collisions(photo_ids, target_folder_id)

    def query_move_rule_matches(self, criteria):
        """Return photo IDs matching move rule criteria.

        Criteria keys (all optional, AND logic):
          rating_min, flag, species, folder_ids,
          has_predictions, imported_before
        """
        workspace_id = self._ws_id()
        move_min_conf = None
        if "has_predictions" in criteria:
            # Apply the workspace-effective detector_confidence floor so the
            # rule matches what the UI actually shows: a photo whose only
            # predictions sit on below-threshold detections must NOT count
            # as "has predictions".
            import config as cfg
            move_min_conf = self.get_effective_config(cfg.load()).get(
                "detector_confidence", 0.2
            )
        return self._moves_merge_repository().query_rule_matches(criteria, workspace_id, move_min_conf)

    def _folders_linked_in_other_workspace(self, folder_ids, active_ws):
        """Return the subset of folder_ids that any workspace other than
        active_ws has a workspace_folders link for (root or not).

        Any foreign link — an explicit root import (is_root = 1) or a
        scanner-materialized descendant of one (is_root = 0) — is evidence
        the folder is still visible in that workspace, so it must never be
        deleted, only unlinked. With active_ws None, any link qualifies.
        """
        return self._folder_repository(scoped=False).linked_in_other_workspace(
            folder_ids, active_ws,
        )

    def delete_folder(self, folder_id):
        """Delete a folder, its descendant folders, and all their photos/data.

        Must cover the whole subtree: folders.parent_id has no ON DELETE
        action and the scanner registers every subdirectory with parent_id
        set, so deleting a non-leaf folder row alone trips the FK — and
        descendants of a deleted folder would be unreachable anyway. The
        subtree is collected by path prefix (``_folder_subtree_ids_by_path``),
        not a parent_id walk, so legacy rows whose parent_id is NULL but
        whose path lives under the target are deleted too — the same shape
        the workspace link/unlink paths already handle.

        A folder with any ``workspace_folders`` link from a workspace
        other than the active one is never deleted — it is only unlinked
        from the active workspace. A foreign link, root or
        scanner-materialized, means the folder is still reachable in that
        workspace (an is_root = 0 link is always covered by a root
        ancestor there). That applies to the target folder itself (in
        which case nothing is deleted at all: the folder, its subtree,
        and its photos survive untouched and only the active workspace's
        links are removed) and to any descendant (the descendant's
        subtree and photos are preserved; its head is reparented to NULL
        because the parent row is going away). Deleting in one workspace
        must not destroy data still reachable in another.

        Returns dict with 'deleted_photos' count and 'files' (list from
        delete_photos) so the caller can remove cached thumbnails, previews,
        and working copies — the FK cascade drops preview_cache rows but
        leaves the on-disk files, which would otherwise become untracked
        orphans that eviction can't reclaim. When the target is protected
        by another workspace's link, that's {'deleted_photos': 0,
        'files': []}.
        """
        active_ws = self._ws_id()
        deleted_ids, files = self._folder_repository(scoped=False).delete(
            folder_id,
            active_ws,
            subtree_ids_by_path=self._folder_subtree_ids_by_path,
            linked_in_other_workspace=self._folders_linked_in_other_workspace,
            delete_photos=self.delete_photos,
            remember_removals=self._remember_workspace_folder_removals,
        )
        self.prune_pipeline_cache_for_ids(deleted_ids)

        # delete_photos' finally-clause invalidation only covers folders that
        # had photo rows. Photo-less deleted folders (whose untracked on-disk
        # files counted as "new") and kept subtrees unlinked from the active
        # workspace above would otherwise keep serving a stale new-images
        # count until the TTL expires. Deleted folders are only ever linked
        # in the active workspace (foreign links protect from deletion), so
        # invalidating it post-commit covers every affected workspace.
        if active_ws is not None:
            self._new_images_cache.invalidate_workspaces(
                self._db_path, [active_ws]
            )

        return {"deleted_photos": len(deleted_ids), "files": files}

    # -- Photos --

    def _photos_repository(self, *, scoped=True):
        """Build the core photo-row repository on this connection.

        Photo rows are global, so ``scoped=False`` builds it without a
        workspace for the catalog-wide methods; ``scoped=True`` passes the
        active workspace id (raising if none is active) for the reads that
        go through ``workspace_folders``. The column lists and the ``db``
        module helpers are passed in, the helpers read at call time so tests
        that patch ``db.commit_with_retry`` / ``db.execute_with_retry`` still
        apply.
        """
        from repositories.photos import PhotoRepository

        return PhotoRepository(
            self.conn,
            self._ws_id() if scoped else None,
            chunk_size=_SQLITE_PARAM_CHUNK_SIZE,
            photo_cols=self.PHOTO_COLS,
            photo_detail_cols=self.PHOTO_DETAIL_COLS,
            execute_with_retry=execute_with_retry,
            commit_with_retry=commit_with_retry,
            inclusive_date_to=_inclusive_date_to,
            keyword_token_clause=_keyword_token_clause,
        )

    def add_photo(
        self,
        folder_id,
        filename,
        extension,
        file_size,
        file_mtime,
        timestamp=None,
        width=None,
        height=None,
        xmp_mtime=None,
        file_hash=None,
    ):
        """Insert a photo. Returns the photo id.

        If ``file_hash`` is provided and the insert creates a new row that
        collides with an existing non-rejected photo sharing the same hash,
        the duplicate auto-resolver runs and flags the loser(s) as rejected.
        The hook is wrapped in try/except so resolver bugs never break
        inserts.
        """
        photo_id = self._photos_repository(scoped=False).add(
            folder_id,
            filename,
            extension,
            file_size,
            file_mtime,
            timestamp=timestamp,
            width=width,
            height=height,
            xmp_mtime=xmp_mtime,
            file_hash=file_hash,
        )

        # Auto-resolve duplicates when we have a file_hash and >1 non-rejected
        # rows share it. Most scanner-path callers leave file_hash=None here
        # and set it later via an UPDATE; those callers must invoke
        # check_and_resolve_duplicates_for_hash themselves after the UPDATE.
        if file_hash:
            self.check_and_resolve_duplicates_for_hash(file_hash)

        return photo_id

    def _duplicates_repository(self):
        """Build the duplicates repository on this connection.

        Every duplicate method is catalog-wide (photos are global), so the
        repository takes no workspace id and never calls ``_ws_id()``.
        """
        from repositories.duplicates import DuplicatesRepository

        return DuplicatesRepository(
            self.conn, chunk_size=_SQLITE_PARAM_CHUNK_SIZE,
        )

    def check_and_resolve_duplicates_for_hash(self, file_hash: str) -> dict | None:
        """Look up non-rejected photos sharing this hash; if >=2, resolve.

        Returns the result dict from apply_duplicate_resolution when resolution
        ran, or None when no resolution was needed. Failures are logged and
        swallowed — this is a best-effort hook, not a correctness guarantee.
        """
        if not file_hash:
            return None
        try:
            dup_ids = self._duplicates_repository().live_ids_for_hash(file_hash)
            if len(dup_ids) > 1:
                return self.apply_duplicate_resolution(dup_ids)
        except sqlite3.Error as e:
            logging.getLogger(__name__).warning(
                "Duplicate auto-resolve failed for hash %s: %s", file_hash, e,
            )
        return None

    def find_duplicate_groups(self, include_resolved=False):
        """Return duplicate groups for the duplicate-scan job.

        Each group is ``{file_hash, photo_ids: [...], status}`` where
        ``status`` is either ``'unresolved'`` (2+ non-rejected rows; user
        action needed to pick a winner) or ``'resolved'`` (exactly one
        non-rejected row plus one or more rejected rows sharing the hash;
        the auto-resolver already handled it during scan, but the loser
        files may still be on disk).

        ``include_resolved=False`` (the default) returns only unresolved
        groups, preserving the legacy contract for callers that want
        actionable items. Pass True from the duplicates page to surface
        already-handled pairs so the user can clean up loser files from
        disk — those pairs are otherwise invisible.

        ``photo_ids`` includes both the kept and the rejected rows for
        resolved groups; downstream code disambiguates by re-querying
        ``flag`` per row.
        """
        return self._duplicates_repository().find_groups(
            include_resolved=include_resolved,
        )

    def apply_duplicate_resolution(self, photo_ids):
        """Resolve a group of photos sharing a file_hash.

        Picks a winner using :func:`vireo.duplicates.resolve_duplicates`,
        merges metadata (rating/keywords) from losers onto the winner, and
        flags the losers as rejected. Runs in a single transaction.

        Photos whose ``flag`` is already ``'rejected'`` are filtered out
        before resolving — we never un-reject previously handled losers.

        Returns ``{"winner_id": int|None, "loser_ids": [int], "rejected": int}``.
        If fewer than 2 non-rejected candidates remain, returns the no-op
        shape with ``winner_id=None``.
        """
        plan = self._duplicates_repository().resolution_plan(photo_ids)
        if plan is None:
            return {"winner_id": None, "loser_ids": [], "rejected": 0}
        winner_id, loser_ids = plan

        self._apply_winner_loser_merge(winner_id, loser_ids)

        return {
            "winner_id": winner_id,
            "loser_ids": list(loser_ids),
            "rejected": len(loser_ids),
        }

    def _apply_winner_loser_merge(self, winner_id, loser_ids):
        """Merge rating/keywords from losers onto winner, then flag losers
        as rejected. Single transaction. Shared between the resolver-based
        ``apply_duplicate_resolution`` and the user-driven
        ``bulk_resolve_by_folder``.
        """
        from duplicates import merge_metadata

        repo = self._duplicates_repository()
        winner_meta = repo.photo_metadata(winner_id)
        loser_metas = [repo.photo_metadata(lid) for lid in loser_ids]
        merge = merge_metadata(winner_meta, loser_metas)

        with repo.transaction():
            if merge.new_rating != winner_meta.rating:
                repo.set_rating(winner_id, merge.new_rating)
            # Carry every loser's durable provenance onto the winner,
            # including keyword IDs the winner already has. merge_metadata()
            # omits overlaps from keyword_ids_to_add, but a manual loser must
            # still upgrade a weaker winner association — so this loop is
            # driven by what the losers carry, not by what needs adding.
            # tag_photo's upsert folds against the winner's own stamp, so a
            # weak loser can never pull a stronger winner down.
            loser_keyword_sources = {}
            for row in repo.loser_keyword_rows(loser_ids):
                kw_id = row["keyword_id"]
                loser_keyword_sources[kw_id] = keyword_source_max(
                    row["source"], loser_keyword_sources.get(kw_id),
                )
            for kw_id, merged_source in loser_keyword_sources.items():
                self.tag_photo(
                    winner_id, kw_id, source=merged_source, _commit=False,
                )
            # TODO: pending-edit copy for duplicate merge — see plan Task 7.
            # Skipped because pending_changes is workspace-scoped and its
            # value/change_token columns are non-trivial to copy safely in
            # this transaction. Rare edge case; revisit if product needs it.
            # Collections are rule-based (no junction table) so
            # merge.collection_ids_to_add has nothing to write either.
            repo.reject(loser_ids)

        logging.getLogger(__name__).info(
            "Duplicate resolved: kept id=%s, rejected id(s)=%s",
            winner_id,
            loser_ids,
        )

    def bulk_resolve_by_folder(self, file_hashes, keep_folder):
        """Force-resolve many duplicate groups by keeping the photo whose
        folder matches ``keep_folder``. Companion to the bulk-decide UI.

        For each ``file_hash``: find non-rejected candidates, pick the one
        whose folder path equals ``keep_folder`` as winner, mark every
        other candidate as rejected, merge metadata. If multiple
        candidates live in ``keep_folder`` (rare — typically only happens
        when same-folder duplicates accumulate), runs the deterministic
        resolver against just those to pick a single winner.

        Skip reasons are surfaced rather than raised so a single bad hash
        doesn't poison a 1000-hash batch:
        - ``"no candidates"`` — file_hash has no DB rows (stale UI state)
        - ``"fewer than 2 candidates"`` — only one row left, nothing to do
        - ``"no candidate in keep_folder"`` — group exists but isn't
          actionable from this folder choice
        - ``"keep_folder candidate missing on disk"`` — the row(s) in
          keep_folder point at files that no longer exist; promoting them
          would reject the only surviving sibling (and, if the caller
          chains a delete, trash it).

        Returns ``{"resolved": [{"file_hash", "winner_id", "loser_ids"}],
        "skipped": [{"file_hash", "reason"}]}``.
        """
        # Normalize once. The bucket UI derives folder paths from
        # ``os.path.dirname(...)`` (never trailing-slashed), but
        # ``folders.path`` rows can carry a trailing separator from
        # manual relocation or legacy imports — a naive string compare
        # silently no-ops the action for those users.
        keep_folder_norm = os.path.normpath(keep_folder) if keep_folder else ""

        repo = self._duplicates_repository()
        resolved = []
        skipped = []
        for file_hash in file_hashes:
            winner_id, loser_ids, reason = repo.keep_folder_plan(
                file_hash, keep_folder_norm,
            )
            if reason is not None:
                skipped.append({"file_hash": file_hash, "reason": reason})
                continue
            self._apply_winner_loser_merge(winner_id, loser_ids)
            resolved.append({
                "file_hash": file_hash,
                "winner_id": winner_id,
                "loser_ids": loser_ids,
            })

        return {"resolved": resolved, "skipped": skipped}

    def reopen_duplicate_group(self, file_hash):
        """Un-reject all rejected rows sharing this file_hash.

        Used by the duplicate scan when the kept file has gone missing on
        disk but a rejected sibling still exists — clearing the rejection
        lets the next proposal pass run Rule 0 and promote the survivor.
        Returns the number of rows un-rejected.
        """
        return self._duplicates_repository().reopen(file_hash)

    # Columns to return in photo list queries (excludes large fields)
    PHOTO_COLS = """id, folder_id, filename, extension, file_size, file_mtime, xmp_mtime,
                    timestamp, width, height, rating, flag, thumb_path, sharpness,
                    subject_sharpness, subject_size, quality_score,
                    latitude, longitude, companion_path, working_copy_path,
                    wildlife_excluded, miss_no_subject, miss_clipped, miss_oof,
                    camera_make, camera_model, iso"""

    # Columns for single-photo detail queries (includes exif_data JSON +
    # eye-focus fields consumed by the review lightbox's crosshair overlay)
    PHOTO_DETAIL_COLS = (
        PHOTO_COLS
        + ", exif_data, eye_x, eye_y, eye_conf, eye_tenengrad,"
        + " working_copy_failed_at, working_copy_failed_mtime,"
        + " working_copy_failed_source"
    )

    def get_photo(self, photo_id, verify_workspace=False):
        """Return a single photo by id, including full metadata.

        Args:
            photo_id: the photo's primary key.
            verify_workspace: if True, only return the photo when it belongs
                to a folder visible in the active workspace.  Callers in
                route handlers should pass True; background jobs that already
                scope their photo lists can leave it False.
        """
        return self._photos_repository(scoped=verify_workspace).get(
            photo_id, verify_workspace,
        )

    def get_photo_filenames(self, photo_ids):
        """Return {photo_id: (folder_id, filename)} for the ids that exist.

        Path resolution needs two columns; selecting whole photo rows for
        thousands of photos drags along ``exif_data`` and every measurement
        column for nothing. Ids with no photo row are simply absent, which is
        how callers detect a deleted photo.
        """
        return self._photos_repository(scoped=False).get_filenames(photo_ids)

    def get_photos_by_ids(self, photo_ids, *, include_exif=False):
        """Return photos for a list of IDs.

        Returns a dict mapping photo_id -> Row for efficient lookup. Large
        id lists are chunked so the IN-clause stays under SQLite's
        bound-parameter cap (999 on legacy builds, 32766 modern).
        """
        return self._photos_repository(scoped=False).get_by_ids(
            photo_ids, include_exif=include_exif,
        )

    def get_photo_folder_statuses(self, photo_ids):
        """Return ``{photo_id: folder_status}`` for the requested photos."""
        return self._photos_repository(scoped=False).get_folder_statuses(photo_ids)

    def count_photos(self):
        """Return photo count for the active workspace.

        Filters out photos whose folder is flagged ``'missing'`` so callers
        like browse/cull/move see only photos they can actually act on. For
        a total inventory that survives an unmounted drive (e.g. the
        dashboard's headline number), use ``count_photos_in_workspace``.
        """
        return self._photos_repository().count()

    def count_photos_in_workspace(self):
        """Return total photo count for the active workspace, including
        photos in folders flagged ``'missing'``.

        The dashboard wants this number — when a drive unmounts, the photos
        are still part of the workspace's inventory, just temporarily
        inaccessible. Falling back to ``count_photos`` (which filters out
        missing folders) makes the dashboard say "0 photos" for an
        established workspace, hiding the fact that the data is fine and
        only the volume is offline.
        """
        return self._photos_repository().count_in_workspace()

    def count_folders(self):
        """Return folder count for the active workspace."""
        return self._folder_repository().count()

    def count_keywords(self):
        """Return count of keywords used by photos in the active workspace.

        Filters out keywords whose only photos sit in folders flagged
        ``'missing'``. For the dashboard's headline (which must agree with
        the unfiltered top_keywords chart in ``get_dashboard_stats``), use
        ``count_keywords_in_workspace`` instead.
        """
        return self._keyword_repository().count()

    def count_keywords_in_workspace(self):
        """Return count of keywords used by photos in the active workspace,
        including photos in folders flagged ``'missing'``.

        Pairs with the unfiltered ``top_keywords`` query in
        ``get_dashboard_stats`` so the dashboard's Keywords headline can't
        disagree with the Top Species / Other Keywords charts when a drive
        is unmounted (e.g. headline says 0 while charts list keywords).
        """
        return self._keyword_repository().count_in_workspace()

    def count_pending_changes(self):
        """Return pending changes count."""
        return self._sync_repository().count()

    def staged_sync_scope_by_photos(self, photo_ids):
        """Photo-id scoped variant of :meth:`staged_sync_scope`.

        Used by the post-transfer residual check for a NAS send: the
        tracked-merge path in ``send_pending_archive`` reparents each staged
        photo onto the destination folder id, so a folder-id-scoped re-read
        would miss any edit queued during the copy and the completed job
        would falsely claim no metadata missed the transfer. Photo ids
        survive the reparent, so the caller captures them before the move
        and passes them here. Return shape matches ``staged_sync_scope``.
        """
        return self._sync_repository().staged_scope_by_photos(photo_ids)

    def staged_sync_scope(self, folder_ids):
        """Return ``(changes, photos_here, photos_elsewhere, photos_here_with_sibling_edits)``.

        ``changes`` is a list of ``(identity, change_id, photo_id)``, where
        ``identity`` is the row's ``change_token`` -- a uuid assigned at
        insert. Callers comparing one read against the next must key on it
        rather than on the id: ``pending_changes.id`` is a bare rowid SQLite
        re-issues to the next insert, so a change queued right after a sync
        cleared one can arrive wearing the id that just left, and look to the
        caller like a row it has already dealt with. The column is nullable
        with no backfill, so rows predating it fall back to the id and keep
        exactly the exposure they have always had.

        ``change_ids`` and ``photos_here`` cover the active workspace only,
        matching what ``sync.sync_to_xmp`` will actually write: the queue is
        workspace-scoped by design and the ordinary sync job respects that.

        ``photos_elsewhere`` counts photos whose only queued edits belong to
        another workspace. The sidecar is global to the photo, so those edits
        are real and this sync will not write them -- the banner has to say so
        rather than let a number read as "everything is covered".

        ``photos_here_with_sibling_edits`` counts photos in ``photos_here``
        that *also* have queued edits in a sibling workspace. Those photos are
        already promised by the "here" number, so they must not double-count
        into ``photos_elsewhere`` (which would read as extra photos rather
        than the same photo carrying two workspaces' edits). The overlap is
        reported separately so the UI can still warn that the sibling's
        changes on those photos will remain unwritten after the pre-transfer
        sync -- the sidecar is shared and only the active workspace's edits
        travel with it.
        """
        return self._sync_repository().staged_scope(folder_ids)

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

    def _coverage_select_fragment(self):
        parts = [
            f"SUM(CASE WHEN {pred} THEN 1 ELSE 0 END) AS {key}"
            for key, pred in self._COVERAGE_PHOTO_COLUMNS
        ]
        return ",\n                ".join(parts)

    def _stats_repository(self, *, scoped=True):
        """Build the stats repository on this connection.

        ``scoped=True`` binds it to the active workspace (raising
        ``RuntimeError`` when none is set). ``scoped=False`` serves the
        readers that take an explicit workspace id (the classification
        inventory) and the temp-table scope staging.
        """
        from repositories.stats import StatsRepository

        return StatsRepository(
            self.conn,
            self._ws_id() if scoped else None,
            coverage_photo_columns=self._COVERAGE_PHOTO_COLUMNS,
        )

    def _dashboard_scope_clause(
        self,
        folder_id=None,
        collection_id=None,
        date_from=None,
        date_to=None,
        table_alias="p",
    ):
        """Return an ``AND ...`` clause for Dashboard-scoped photo queries.

        Unlike :meth:`_scope_clause`, this stays composable and does not
        materialize every matching photo id in Python.  That matters for a
        date-scoped dashboard over a million-photo workspace.  Collection
        rules are embedded as a subquery using the same rule compiler as
        Browse, so Dashboard and Browse agree about collection membership.
        """
        conditions = []
        params = []

        if folder_id is not None:
            if not self._stats_repository().folder_linked(folder_id):
                raise ValueError("folder not found in active workspace")
            subtree = self.get_folder_subtree_ids(folder_id)
            placeholders = ",".join("?" for _ in subtree)
            conditions.append(f"{table_alias}.folder_id IN ({placeholders})")
            params.extend(subtree)

        if collection_id is not None:
            # Dashboard scope keeps offline photos in totals, so the collection
            # subquery must not filter them out via the Browse-oriented
            # ``f.status IN ('ok', 'partial')`` join. Callers that need the
            # accessible-only view apply that filter in their outer query
            # (e.g. get_coverage_stats), so it's fine to be permissive here.
            parts = self._build_collection_query(
                collection_id, include_offline_folders=True,
            )
            if parts is None:
                raise ValueError("collection not found in active workspace")
            folder_join, join_clause, where, collection_params = parts
            collection_query = (
                "SELECT DISTINCT p.id FROM photos p "
                f"{folder_join} {join_clause} {where}"
            )
            conditions.append(f"{table_alias}.id IN ({collection_query})")
            params.extend(collection_params)

        if date_from is not None:
            conditions.append(f"{table_alias}.timestamp >= ?")
            params.append(date_from)
        if date_to is not None:
            conditions.append(f"{table_alias}.timestamp <= ?")
            params.append(_inclusive_date_to(date_to))

        if not conditions:
            return "", []
        return " AND " + " AND ".join(conditions), params

    def get_coverage_stats(
        self, folder_id=None, collection_id=None, date_from=None, date_to=None,
    ):
        """Return per-stage coverage counts for the active workspace.

        ``total`` is the number of photos in active (status ``'ok'`` or
        ``'partial'``) folders of the workspace. Each other key is the count of those photos for which
        the named pipeline stage has produced output. ``detected`` and
        ``classified`` are joined from the detections/predictions tables;
        everything else is a simple NOT NULL check on ``photos``.
        """
        repo = self._stats_repository()
        import config as cfg
        min_conf = self.get_effective_config(cfg.load()).get(
            "detector_confidence", 0.2
        )
        scope_sql, scope_params = self._dashboard_scope_clause(
            folder_id, collection_id, date_from, date_to,
        )
        return repo.get_coverage(
            min_conf, scope_sql, scope_params, self._coverage_select_fragment(),
        )

    def get_folder_coverage_stats(
        self, folder_id=None, collection_id=None, date_from=None, date_to=None,
    ):
        """Return a list of per-folder coverage counts for the active workspace.

        One row per folder that is linked to the workspace and has
        ``status`` of ``'ok'`` or ``'partial'``. Each row carries ``folder_id``, ``path``, ``name``,
        ``total`` (photos in that folder only — descendants are NOT rolled
        in), and the same coverage keys as :meth:`get_coverage_stats`.
        Folders with zero photos are included so the dashboard can still
        show them as 0 / 0 if it chooses.
        """
        repo = self._stats_repository()
        import config as cfg
        min_conf = self.get_effective_config(cfg.load()).get(
            "detector_confidence", 0.2
        )
        scope_sql, scope_params = self._dashboard_scope_clause(
            folder_id, collection_id, date_from, date_to,
        )
        # Date and collection constraints belong on the LEFT JOIN so folders
        # with zero matching photos remain visible as 0 / 0. Folder scope is
        # different: it controls which folder rows are enumerated, so keep it
        # as an outer WHERE condition on ``f.id``.
        photo_scope_sql, photo_scope_params = self._dashboard_scope_clause(
            None, collection_id, date_from, date_to,
        )
        subtree = None
        if folder_id is not None:
            if not repo.folder_linked(folder_id):
                raise ValueError("folder not found in active workspace")
            subtree = self.get_folder_subtree_ids(folder_id)
        return repo.get_folder_coverage(
            min_conf, scope_sql, scope_params, photo_scope_sql,
            photo_scope_params, subtree, self._coverage_select_fragment(),
        )

    def photos_by_paths(self, paths):
        """Return {abs_path: photo_id} for any of ``paths`` already in DB.

        Photos are global (not workspace-scoped), so the import-mode plan
        can ask "do these files already exist in Vireo?" without caring
        which workspace owns the folder. Paths missing from the result are
        genuinely new and the next pipeline run will create photo rows for
        them — that's what makes "Will run (N)" honest in import mode.

        Splits the input by directory so the SQL stays a single
        ``WHERE f.path = ? AND p.filename IN (...)`` per directory and
        respects SQLite's parameter cap.
        """
        return self._photos_repository(scoped=False).by_paths(paths)

    def workspace_unlinked_folder_count(self, folder_paths):
        """Count distinct paths in ``folder_paths`` whose folders are not
        linked to the active workspace.

        A folder path is "unlinked" when either no row exists in ``folders``
        for that path or a row exists but no ``workspace_folders`` entry
        connects it to the active workspace.

        Used by the import-mode pipeline plan to decide whether a scan over
        already-imported files would be a real no-op for the active
        workspace. ``scanner.scan`` calls ``_ensure_folder`` (which calls
        ``add_folder``) for each walked directory, and ``add_folder``
        auto-links the folder to the active workspace via
        ``workspace_folders``. So when the user re-imports files that were
        indexed in a different workspace, scan still mutates state by
        attaching folders to the active workspace — and the plan must
        report that as ``will-run`` instead of claiming ``done-prior``.
        """
        if not folder_paths:
            return 0
        ws = self._ws_id()
        unique = list({p for p in folder_paths if p})
        if not unique:
            return 0
        return self._workspace_folder_repository().unlinked_folder_count(ws, unique)

    def _stage_scope_ids(self, table, ids):
        """Stage a read scope without opening or committing a caller transaction."""
        self._stats_repository(scoped=False).stage_scope_ids(table, ids)

    def _scope_clause(self, photo_ids, table_alias="p"):
        """Build a (clause, params) pair to scope a query to photo_ids.

        Returns ('', []) when photo_ids is None (whole-workspace scope).
        Returns (' AND p.id IN (NULL)', []) for an empty set, which is the
        intentional "no photos in scope" sentinel — callers asked for
        "this collection" and the collection resolved to zero photos.

        Scopes larger than one parameter chunk are staged in a
        connection-local temp table instead of inline placeholders, which
        would exceed SQLITE_MAX_VARIABLE_NUMBER (999 on legacy builds) for
        big collections. The staged scope is only valid for the query the
        caller runs immediately after this call — the next large-scope call
        overwrites it.
        """
        if photo_ids is None:
            return "", []
        ids = list(photo_ids)
        if not ids:
            return f" AND {table_alias}.id IN (NULL)", []
        if len(ids) <= _SQLITE_PARAM_CHUNK_SIZE:
            placeholders = ",".join("?" for _ in ids)
            return f" AND {table_alias}.id IN ({placeholders})", ids
        self._stage_scope_ids("scope_ids", ids)
        return f" AND {table_alias}.id IN (SELECT id FROM scope_ids)", []

    def count_real_detections_in_scope(self, photo_ids=None, min_conf=None):
        """Count (photos_with_real_dets, total_real_dets) for the workspace.

        "Real" excludes detector_model='full-image' synthetic anchors.
        ``photo_ids`` scopes to a collection (set/list of ids); None = whole
        workspace.

        Used by the pipeline plan to compute classify scope.
        """
        repo = self._stats_repository()
        if min_conf is None:
            import config as cfg
            min_conf = self.get_effective_config(cfg.load()).get(
                "detector_confidence", 0.2,
            )
        scope_sql, scope_params = self._scope_clause(photo_ids)
        return repo.count_real_detections_in_scope(min_conf, scope_sql, scope_params)

    def count_primary_detections_in_scope(self, photo_ids=None, min_conf=None):
        """Count photos whose primary real detection is pipeline-classifiable.

        The streaming pipeline classifies at most one detection per photo: the
        highest-confidence non-full-image detection above the active threshold.
        This mirrors that gate for the Pipeline page plan so secondary boxes
        do not inflate pending classify work.
        """
        repo = self._stats_repository()
        if min_conf is None:
            import config as cfg
            min_conf = self.get_effective_config(cfg.load()).get(
                "detector_confidence", 0.2,
            )
        scope_sql, scope_params = self._scope_clause(photo_ids)
        return repo.count_primary_detections_in_scope(min_conf, scope_sql, scope_params)

    def count_classify_pending_pairs(
        self, classifier_model, labels_fingerprint,
        photo_ids=None, min_conf=None,
    ):
        """Count detections in scope that lack a classifier_runs row for
        (classifier_model, labels_fingerprint).

        Mirrors the gate in classify_job._classify_photos: a real detection
        with no row in classifier_runs for the given (model, fp) is one
        unit of pending work for the next classify run.
        """
        repo = self._stats_repository()
        if min_conf is None:
            import config as cfg
            min_conf = self.get_effective_config(cfg.load()).get(
                "detector_confidence", 0.2,
            )
        scope_sql, scope_params = self._scope_clause(photo_ids)
        return repo.count_classify_pending_pairs(
            classifier_model, labels_fingerprint, min_conf, scope_sql,
            scope_params,
        )

    def count_primary_classify_pending_pairs(
        self, classifier_model, labels_fingerprint,
        photo_ids=None, min_conf=None,
    ):
        """Count primary detections lacking a classifier run for (model, fp).

        Mirrors pipeline_job.classify_stage, which picks one primary detection
        per photo rather than classifying every detection row.
        """
        repo = self._stats_repository()
        if min_conf is None:
            import config as cfg
            min_conf = self.get_effective_config(cfg.load()).get(
                "detector_confidence", 0.2,
            )
        scope_sql, scope_params = self._scope_clause(photo_ids)
        return repo.count_primary_classify_pending_pairs(
            classifier_model, labels_fingerprint, min_conf, scope_sql,
            scope_params,
        )

    def count_classify_stale(
        self, classifier_model, labels_fingerprint,
        photo_ids=None, min_conf=None,
    ):
        """Count detections in scope that have a stale classifier_runs row
        for ``classifier_model`` (some non-current fingerprint) AND no row
        matching the current ``labels_fingerprint``.

        A detection with a current-fp row is "done" (not stale). A
        detection with no row at all is "never processed" (counted by
        :meth:`count_classify_pending_pairs`, not here). The stale set is
        their disjoint complement: previously processed under settings
        that no longer match.
        """
        repo = self._stats_repository()
        if min_conf is None:
            import config as cfg
            min_conf = self.get_effective_config(cfg.load()).get(
                "detector_confidence", 0.2,
            )
        scope_sql, scope_params = self._scope_clause(photo_ids)
        return repo.count_classify_stale(
            classifier_model, labels_fingerprint, min_conf, scope_sql,
            scope_params,
        )

    def count_primary_classify_stale(
        self, classifier_model, labels_fingerprint,
        photo_ids=None, min_conf=None,
    ):
        """Count stale classifier runs on primary detections only."""
        repo = self._stats_repository()
        if min_conf is None:
            import config as cfg
            min_conf = self.get_effective_config(cfg.load()).get(
                "detector_confidence", 0.2,
            )
        scope_sql, scope_params = self._scope_clause(photo_ids)
        return repo.count_primary_classify_stale(
            classifier_model, labels_fingerprint, min_conf, scope_sql,
            scope_params,
        )

    def count_full_image_fallback_photos(
        self, photo_ids=None, detector_model="megadetector-v6",
        min_conf=0,
    ):
        """Count photos eligible for full-image fallback classification.

        These are photos in the active workspace where the detector has run
        and produced no box the classifier could act on: either
        ``box_count = 0`` or every non-full-image row is below ``min_conf``
        (raw noise). Callers that want to mirror the runtime fallback gate
        pass the workspace's ``detector_confidence``; the default of ``0``
        preserves the pre-noise-fallback semantics where any real row
        disqualified the photo.
        """
        repo = self._stats_repository()
        scope_sql, scope_params = self._scope_clause(photo_ids)
        return repo.count_full_image_fallback_photos(
            detector_model, min_conf, scope_sql, scope_params,
        )

    def count_full_image_classify_pending_pairs(
        self, classifier_model, labels_fingerprint,
        photo_ids=None, detector_model="megadetector-v6",
        min_conf=0,
    ):
        """Count fallback photos lacking a classifier run for (model, fp).

        ``min_conf`` mirrors the runtime fallback gate: a photo qualifies
        when no non-full-image detection row is at or above the threshold,
        so a MegaDetector run that produced only noise (< ``min_conf``)
        counts alongside truly empty ``box_count = 0`` runs.
        """
        repo = self._stats_repository()
        scope_sql, scope_params = self._scope_clause(photo_ids)
        return repo.count_full_image_classify_pending_pairs(
            classifier_model, labels_fingerprint, detector_model, min_conf,
            scope_sql, scope_params,
        )

    def count_full_image_classify_stale(
        self, classifier_model, labels_fingerprint,
        photo_ids=None, detector_model="megadetector-v6",
        min_conf=0,
    ):
        """Count fallback anchors with stale runs and no current run.

        ``min_conf`` mirrors ``count_full_image_fallback_photos`` so
        photos whose only detections are noise (< ``min_conf``) join the
        stale-anchor scope alongside the ``box_count = 0`` cases.
        """
        repo = self._stats_repository()
        scope_sql, scope_params = self._scope_clause(photo_ids)
        return repo.count_full_image_classify_stale(
            classifier_model, labels_fingerprint, detector_model, min_conf,
            scope_sql, scope_params,
        )

    def get_classification_inventory(self, workspace_id, min_conf=None,
                                     median_sample_per_pair=2000):
        """Per-(model × fingerprint) coverage stats for ``workspace_id``.

        Returns::

            {
              "total_real_detections": int,
              "pairs": [
                {
                  "classifier_model": str,
                  "labels_fingerprint": str,
                  "classified_dets": int,
                  "photos_covered": int,
                  "last_run": str | None,
                  "predictions_count": int,
                  "median_top1_conf": float | None,
                  "median_sample_size": int,
                }
              ],
              "total_predictions_rows": int,
            }

        Caller (the endpoint) is responsible for joining this against the
        on-disk model registry / label files to identify never-run, stale,
        and legacy combinations.
        """
        if min_conf is None:
            import config as cfg
            saved_active = self._active_workspace_id
            try:
                self._active_workspace_id = workspace_id
                min_conf = self.get_effective_config(cfg.load()).get(
                    "detector_confidence", 0.2,
                )
            finally:
                self._active_workspace_id = saved_active

        total_real_detections, pair_rows, pred_counts = (
            self._stats_repository(scoped=False).classification_inventory_counts(
                workspace_id, min_conf,
            )
        )

        # Median top-1 confidence per pair, via a sampled top-1-per-detection set.
        # Bounded by median_sample_per_pair to keep total work small.
        medians = self._sampled_top1_medians(
            workspace_id, min_conf, median_sample_per_pair,
        )

        pairs = []
        for r in pair_rows:
            key = (r["classifier_model"], r["labels_fingerprint"])
            med, sample_size = medians.get(key, (None, 0))
            pairs.append({
                "classifier_model": r["classifier_model"],
                "labels_fingerprint": r["labels_fingerprint"],
                "classified_dets": r["classified_dets"] or 0,
                "photos_covered": r["photos_covered"] or 0,
                "last_run": r["last_run"],
                "predictions_count": pred_counts.get(key, 0),
                "median_top1_conf": med,
                "median_sample_size": sample_size,
            })

        total_pred_rows = sum(pred_counts.values())

        return {
            "total_real_detections": total_real_detections,
            "pairs": pairs,
            "total_predictions_rows": total_pred_rows,
        }

    def _sampled_top1_medians(self, workspace_id, min_conf, sample_per_pair):
        """Return {(model, fingerprint): (median, sample_size)} from a sampled
        set of top-1-per-detection prediction confidences.

        SQLite has no built-in median; we pull a per-pair sample (at most
        ``sample_per_pair`` rows) of the max confidence per (detection, model,
        fingerprint) tuple and median in Python. Sampling is fine for the UX
        signal — if classified_dets is small, the sample is the whole set.
        """
        return self._stats_repository(scoped=False).sampled_top1_medians(
            workspace_id, min_conf, sample_per_pair,
        )

    def count_photos_pending_masks(self, photo_ids=None, min_conf=None,
                                   sam2_variant=None):
        """Return (pending, eligible) for the extract-masks stage.

        eligible = photos in scope with at least one real detection above the
            workspace's effective detector_confidence
        pending  = eligible photos whose mask_path IS NULL, or when
            ``sam2_variant`` is supplied, whose ``photo_masks`` row for that
            variant is missing/incomplete

        The variant-aware mode mirrors extract_masks_stage's current per-photo
        cache check: masks made by another SAM variant do not make the selected
        variant complete.
        """
        repo = self._stats_repository()
        if min_conf is None:
            import config as cfg
            min_conf = self.get_effective_config(cfg.load()).get(
                "detector_confidence", 0.2,
            )
        scope_sql, scope_params = self._scope_clause(photo_ids)
        return repo.count_photos_pending_masks(
            min_conf, sam2_variant, scope_sql, scope_params,
        )

    def count_photos_missing_thumb(self, photo_ids=None):
        """Return (eligible, pending) for the thumbnails substage.

        eligible = photos in scope linked to the active workspace
        pending  = eligible photos whose ``thumb_path IS NULL``

        ``thumbnail_stage``'s per-photo gate is ``os.path.exists`` on
        the cache file, but the photos.thumb_path column is the fast
        proxy: app.py's startup backfill aligns the column with disk
        reality (populates it for legacy rows that already have files,
        clears it for rows whose file has since been deleted), so a
        NULL value is a reliable "needs generating" signal.
        """
        repo = self._stats_repository()
        scope_sql, scope_params = self._scope_clause(photo_ids)
        return repo.count_photos_missing_thumb(scope_sql, scope_params)

    def count_photos_missing_preview(self, size, photo_ids=None):
        """Return (eligible, pending) for the previews substage at ``size``.

        eligible = photos in scope linked to the active workspace
        pending  = eligible photos with no ``preview_cache`` row at ``size``

        ``previews_stage`` gates on ``os.path.exists`` of the cache
        file, but writes (or refreshes) a ``preview_cache`` row for
        every photo it processes — whether already-cached or freshly
        generated. Eviction (``preview_cache_max_mb``) deletes the
        file and the row together. So the table is a reliable index
        for "preview present on disk at this size".
        """
        repo = self._stats_repository()
        scope_sql, scope_params = self._scope_clause(photo_ids)
        return repo.count_photos_missing_preview(size, scope_sql, scope_params)

    def count_photos_missing_thumb_or_preview(self, size, photo_ids=None):
        """Return (eligible, pending) where ``pending`` counts photos
        missing a thumbnail OR a preview at ``size`` (or both) — i.e.
        the union of the two substages' work sets.

        The Thumbnails & Previews card needs the photo-level union for
        its "Resume (N left)" framing: a photo missing only a thumb and
        a different photo missing only a preview each represent one
        photo the next pipeline run will touch. Falling back to
        ``max(thumb_pending, preview_pending)`` undercounts whenever
        the two missing-sets aren't strict subsets of each other.
        """
        repo = self._stats_repository()
        scope_sql, scope_params = self._scope_clause(photo_ids)
        return repo.count_photos_missing_thumb_or_preview(size, scope_sql, scope_params)

    def count_extract_stale(self, sam2_variant, photo_ids=None,
                             detector_confidence=None):
        """Count photos in scope that "look done" (``photos.mask_path``
        is set) but whose ``photo_masks`` row for ``sam2_variant`` has a
        stored prompt that no longer matches the photo's primary
        detection.

        Reuses the staleness predicate from ``find_stale_masks`` — a
        mask is fresh only when its stored ``(detector_model,
        prompt_xywh)`` equals the selected non-full-image
        detection on the same photo (with optional ``detector_confidence``
        floor). Filtered by ``sam2_variant`` so a stale mask under a
        different variant doesn't pollute the count for the currently
        configured variant.

        Photos without a current primary detection (no non-full-image
        detection at or above ``detector_confidence``) are excluded:
        they aren't eligible for the extract stage, so a leftover
        ``photo_masks`` row from a prior detector run isn't "stale work
        to redo" — it's just an orphan that storage cleanup handles.
        Counting those would inflate ``detail.stale`` and keep the
        stage flagged Outdated/Will run forever in mixed workspaces.

        The active-mask and variant-path gates keep this count disjoint from
        ``count_photos_pending_masks``'s ``pending``. Photos with no active
        mask, no selected-variant row, or an incomplete selected-variant path
        are already pending — they will be re-extracted regardless of whether
        their ``photo_masks`` row's prompt matches — so counting them here
        would double-count when a planner combines ``pending + stale`` as
        total work.
        """
        import config as cfg
        repo = self._stats_repository()
        if detector_confidence is None:
            detector_confidence = self.get_effective_config(cfg.load()).get(
                "detector_confidence", 0.2,
            )
        scope_sql, scope_params = self._scope_clause(photo_ids)
        return repo.count_extract_stale(
            sam2_variant, detector_confidence, scope_sql, scope_params,
        )

    def count_eye_keypoint_eligible(self, photo_ids=None):
        """Count photos eligible for the eye-keypoint stage, ignoring the
        ``eye_tenengrad IS NULL`` idempotency gate.

        Eligibility = active mask present for the currently-selected
        primary detection + at least one prediction on that detection.
        Mirrors ``list_photos_for_eye_keypoint_stage``'s selected-primary
        + active-mask predicates minus the "not yet processed" filter,
        so the plan can distinguish "no eligible photos" from "all
        eligible photos already processed". Counting on the loose
        mask+detection+prediction join would include photos whose only
        prediction sits on a non-primary detection — the stage cannot
        produce eye keypoints for those, so review readiness would
        repeatedly flag missing keypoints while the plan reported the
        stage complete (Codex r4056621190).
        """
        import config as cfg
        repo = self._stats_repository()
        min_conf = self.get_effective_config(cfg.load()).get(
            "detector_confidence", 0.2,
        )
        scope_sql, scope_params = self._scope_clause(photo_ids)
        return repo.count_eye_keypoint_eligible(min_conf, scope_sql, scope_params)

    def count_eye_keypoint_stale(self, photo_ids=None):
        """Count photos in scope whose eye_tenengrad is set under a
        non-current eye_kp_fingerprint. Mirrors
        ``count_eye_keypoint_eligible``'s selected-primary + active-mask
        join shape and adds the staleness predicate.

        A NULL fingerprint on a row with eye_tenengrad set is treated as
        stale — only the migration backfill should produce that state,
        and even there the user is expected to re-run after a model
        change to restamp.
        """
        import config as cfg
        repo = self._stats_repository()
        min_conf = self.get_effective_config(cfg.load()).get(
            "detector_confidence", 0.2,
        )
        scope_sql, scope_params = self._scope_clause(photo_ids)
        return repo.count_eye_keypoint_stale(min_conf, scope_sql, scope_params)

    def count_eye_keypoint_attemptable(self, min_species_conf, photo_ids=None):
        """Count photos whose top-routable prediction would actually be
        attempted by the eye-keypoint stage under the current config.

        Tighter than ``count_eye_keypoint_eligible``: eligible photos
        include ones whose selected primary detection's top prediction
        will be skipped at Gate 1 (classifier confidence below
        ``min_species_conf``) or fail taxonomy routing (anything outside
        the keys of ``pipeline._EYE_KEYPOINT_MODEL_FOR_CLASS``). Those
        photos never get an ``eye_kp_fingerprint`` stamped — by design,
        so a future config change can retry them — so they would
        permanently inflate ``eye_target`` and trip the "computed
        without eye keypoints" banner on every run.

        Match ``list_photos_for_eye_keypoint_stage``'s selected-primary
        + active-mask predicates and its "best routable row per photo"
        selection so a taxonomy-bearing prediction wins over a
        taxonomy-less one on the *same* selected detection (Codex
        r4056621190). Predictions that route only via the scientific
        name → taxa-table fallback are *not* counted here (the SQL
        filter is taxonomy_class-only); those photos will still be
        attempted by the stage but will be undercounted in the target,
        which keeps ``attempts >= target`` and means the banner won't
        lie — at worst it stays quiet when it could have surfaced.
        """
        import config as cfg
        repo = self._stats_repository()
        min_conf = self.get_effective_config(cfg.load()).get(
            "detector_confidence", 0.2,
        )
        scope_sql, scope_params = self._scope_clause(photo_ids)
        return repo.count_eye_keypoint_attemptable(
            min_species_conf, min_conf, scope_sql, scope_params,
        )

    def get_dashboard_stats(
        self, folder_id=None, collection_id=None, date_from=None, date_to=None,
    ):
        """Return scoped aggregate statistics and actionable gaps.

        Dashboard scope is metadata-only: photos remain countable while their
        storage is offline.  Signals that require reading source files (such
        as preview generation) separately restrict themselves to accessible
        folders and expose that distinction through ``accessible_photos``.
        """
        repo = self._stats_repository()
        # Hoisted: multiple queries below need the workspace-effective
        # detector_confidence to keep classified_count / prediction_status /
        # detected_count in sync as the threshold moves.
        import config as cfg
        effective = self.get_effective_config(cfg.load())
        min_conf = effective.get("detector_confidence", 0.2)
        preview_size = effective.get("preview_max_size", 1920)
        scope_sql, scope_params = self._dashboard_scope_clause(
            folder_id, collection_id, date_from, date_to,
        )
        location_conditions = []
        self._append_location_status_filter(location_conditions, "none")
        return repo.get_dashboard(
            min_conf, preview_size, scope_sql, scope_params, location_conditions,
        )

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

    def _location_repository(self):
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
            photo_date_asc_order=_PHOTO_DATE_ASC_ORDER,
        )

    def get_photo_location_statuses(self, photo_ids):
        """Return ``{photo_id: exif|assigned|none}`` for the requested photos."""
        return self._location_repository().get_photo_statuses(photo_ids)

    def get_calendar_data(
        self,
        year,
        folder_id=None,
        collection_id=None,
        rules=None,
    ):
        """Return daily photo counts for a given year, scoped to active workspace.

        Metadata filtering arrives exclusively as a universal-filter ``rules``
        tree (the legacy per-field params were removed in Phase 5 once the
        filter bar became the only caller).
        """
        return self._photos_repository().get_calendar_data(
            year,
            folder_id=folder_id,
            collection_id=collection_id,
            rules=rules,
            get_folder_subtree_ids=self.get_folder_subtree_ids,
            build_collection_query=self._build_collection_query,
            build_query_from_rules=self._build_query_from_rules,
        )

    def get_photos(
        self,
        folder_id=None,
        collection_id=None,
        page=1,
        per_page=50,
        sort="date",
        rating_min=None,
        date_from=None,
        date_to=None,
        keyword=None,
        keyword_match_case=False,
        keyword_whole_word=False,
        color_label=None,
        flag=None,
        location_status=None,
    ):
        """Return paginated, filtered photo list scoped to active workspace."""
        return self._photos_repository().list_page(
            folder_id=folder_id,
            collection_id=collection_id,
            page=page,
            per_page=per_page,
            sort=sort,
            rating_min=rating_min,
            date_from=date_from,
            date_to=date_to,
            keyword=keyword,
            keyword_match_case=keyword_match_case,
            keyword_whole_word=keyword_whole_word,
            color_label=color_label,
            flag=flag,
            location_status=location_status,
            get_folder_subtree_ids=self.get_folder_subtree_ids,
            build_collection_query=self._build_collection_query,
            append_location_status_filter=self._append_location_status_filter,
            photo_sort_clause=self._photo_sort_clause,
        )

    def get_photo_ids(
        self,
        folder_id=None,
        collection_id=None,
        sort="date",
        rating_min=None,
        date_from=None,
        date_to=None,
        keyword=None,
        keyword_match_case=False,
        keyword_whole_word=False,
        color_label=None,
        flag=None,
        location_status=None,
    ):
        """Return all filtered photo IDs scoped to active workspace."""
        return self._photos_repository().get_ids(
            folder_id=folder_id,
            collection_id=collection_id,
            sort=sort,
            rating_min=rating_min,
            date_from=date_from,
            date_to=date_to,
            keyword=keyword,
            keyword_match_case=keyword_match_case,
            keyword_whole_word=keyword_whole_word,
            color_label=color_label,
            flag=flag,
            location_status=location_status,
            get_folder_subtree_ids=self.get_folder_subtree_ids,
            build_collection_query=self._build_collection_query,
            append_location_status_filter=self._append_location_status_filter,
            photo_sort_clause=self._photo_sort_clause,
        )

    def get_photo_position(
        self,
        photo_id,
        folder_id=None,
        collection_id=None,
        sort="date",
    ):
        """Return a photo's zero-based position without materializing all IDs."""
        return self._photos_repository().get_position(
            photo_id,
            folder_id=folder_id,
            collection_id=collection_id,
            sort=sort,
            get_folder_subtree_ids=self.get_folder_subtree_ids,
            build_collection_query=self._build_collection_query,
            photo_sort_clause=self._photo_sort_clause,
        )

    def count_filtered_photos(
        self,
        folder_id=None,
        collection_id=None,
        rating_min=None,
        date_from=None,
        date_to=None,
        keyword=None,
        keyword_match_case=False,
        keyword_whole_word=False,
        color_label=None,
        flag=None,
        location_status=None,
    ):
        """Return count of photos matching the given filters, scoped to active workspace."""
        return self._photos_repository().count_filtered(
            folder_id=folder_id,
            collection_id=collection_id,
            rating_min=rating_min,
            date_from=date_from,
            date_to=date_to,
            keyword=keyword,
            keyword_match_case=keyword_match_case,
            keyword_whole_word=keyword_whole_word,
            color_label=color_label,
            flag=flag,
            location_status=location_status,
            get_folder_subtree_ids=self.get_folder_subtree_ids,
            build_collection_query=self._build_collection_query,
            append_location_status_filter=self._append_location_status_filter,
        )

    def get_browse_summary(
        self,
        folder_id=None,
        collection_id=None,
        rules=None,
    ):
        """Return summary stats for the browse panel, scoped to active workspace and filters.

        ``rules`` (a universal-filter rule tree) restricts every aggregate to
        the matching photo set, exactly like the collection_id subquery path —
        the summary panel must describe the same photos the filtered grid
        shows. Raises ValueError on malformed rules.
        """
        def detector_confidence():
            import config as cfg
            return self.get_effective_config(cfg.load()).get(
                "detector_confidence", 0.2
            )

        return self._photos_repository().get_browse_summary(
            folder_id=folder_id,
            collection_id=collection_id,
            rules=rules,
            get_folder_subtree_ids=self.get_folder_subtree_ids,
            build_collection_query=self._build_collection_query,
            build_query_from_rules=self._build_query_from_rules,
            detector_confidence=detector_confidence,
        )

    def get_geolocated_photos(
        self,
        folder_id=None,
        rules=None,
    ):
        """Return all geolocated photos, scoped to active workspace.

        ``rules`` (a universal-filter rule tree) restricts the plottable set
        exactly like Browse's grid — the Map page passes the shared filter
        bar's expression here, which is the only metadata filtering this
        method supports (legacy per-field params removed in Phase 5).

        Returns photos that have either non-null EXIF latitude/longitude OR a
        ``type='location'`` keyword link whose keyword has non-null coords. No
        pagination — returns all matching photos for map rendering. Includes
        the photo's species keyword (or NULL if none), derived from
        photo_keywords joined to keywords where is_species = 1.

        Output columns include ``coord_source`` (``'exif'`` or ``'keyword'``)
        and ``keyword_location_name`` (the location keyword's name when EXIF is
        absent, NULL otherwise) so the map can show provenance.
        """
        return self._location_repository().get_geolocated_photos(
            folder_id,
            rules,
            folder_subtree_ids=self.get_folder_subtree_ids,
            build_query_from_rules=self._build_query_from_rules,
        )

    def get_assigned_photo_location(self, photo_id, verify_workspace=True,
                                    allow_sync_only=False):
        """Return linked location-keyword coordinates for one visible photo.

        ``allow_sync_only`` accepts a ``workspace_sync_only_photos`` grant
        as sufficient authorization. The sync engine passes it because that
        grant exists precisely to let a workspace write the sidecar of a
        photo it does not carry in its library: without it a remapped
        sibling-workspace ``location`` change resolves its path through the
        grant and then fails this check, staying queued on every retry.
        Browse and library callers keep the strict membership test.
        """
        if verify_workspace:
            if allow_sync_only:
                self._verify_photo_syncable_in_workspace(photo_id)
            else:
                self._verify_photo_in_workspace(photo_id)

        return self._location_repository().get_assigned(photo_id)

    def _get_photo_location_leaves(self, photo_ids):
        """Choose the effective exported location row for each photo."""
        return self._location_repository().get_photo_leaves(photo_ids)

    def get_photo_location_keyword_ids(self, photo_ids):
        """Return the keyword IDs owning each photo's exported location."""
        return {pid: row["id"] for pid, row in self._get_photo_location_leaves(photo_ids).items()}

    def get_photo_location_paths(self, photo_ids):
        """Return ``{photo_id: [broadest, ..., leaf]}`` location keyword names.

        The leaf is chosen exactly as :meth:`get_assigned_photo_location`
        chooses it -- coordinate-bearing first, then deepest in the chain,
        then most recent id as the tie-break -- so the keywords written to
        a sidecar always describe the same place as the GPS written beside
        them. When a photo carries both a coordinate-bearing location and
        a coordinate-less one (the generic keyword-add endpoint attaches
        another ``type='location'`` keyword without replacing the
        current), the coord-bearing row wins here just as it does in
        :meth:`get_assigned_photo_location`. Unlike that method this one
        does not *require* coordinates: a free-text location the user
        typed still has a name worth writing when there is no coord-bearing
        alternative.

        Photos with no linked location are absent from the result, which is
        how the sync engine tells "write these keywords" from "remove the ones
        we wrote".
        """
        leaves = self._get_photo_location_leaves(photo_ids)
        return self._location_repository().get_photo_paths(leaves)

    def has_pending_location_change(self, photo_id):
        """Return whether a ``location`` change is queued for ``photo_id``.

        Reads across workspaces for the same reason
        :meth:`get_pending_keyword_removal_keys` does: photo metadata is
        global even though the sync queue is presented per workspace. Import
        callers use it to decide whether a sidecar's Vireo-written location
        keywords are still current or describe a place the user has already
        changed in Vireo.
        """
        return self._location_repository().has_pending_change(photo_id)

    def count_photos_with_location(self):
        """Count photos in the active workspace carrying a location keyword."""
        return self._location_repository().count_photos_with_location()

    def queue_location_changes_for_tagged_photos(self):
        """Queue a ``location`` change for every located photo in the workspace.

        This is the backfill behind "write my existing locations to XMP":
        assigning a place queues the change at the time of assignment, so
        photos located before location writes were switched on have nothing
        queued and would never be written. What the queued change actually
        does to a sidecar still depends on the two location settings at sync
        time -- it writes coordinates, keywords, both, or removes what Vireo
        previously wrote -- and the sync review shows that per photo before
        anything is written.

        Returns ``{"photos": n, "queued": k, "already_queued": n - k}``.
        ``queue_change`` skips a duplicate, so re-running this is idempotent.
        """
        result = self._location_repository().queue_changes_for_tagged_photos(
            queue_change=self.queue_change,
        )
        log.info(
            "Queued %d location change(s) for %d located photo(s)",
            result["queued"], result["photos"],
        )
        return result

    def get_effective_photo_location(self, photo_id, verify_workspace=True):
        """Return the coordinates Vireo should use for a single photo.

        EXIF GPS is source metadata and wins when both axes are present. If
        EXIF GPS is absent or partial, fall back as a pair to the linked
        ``type='location'`` keyword coordinates. Returns ``None`` when neither
        source has a complete coordinate pair.
        """
        return self.get_effective_photo_locations(
            [photo_id], verify_workspace=verify_workspace
        ).get(photo_id)

    def get_effective_photo_locations(self, photo_ids, verify_workspace=True):
        """Return effective locations for many photos keyed by photo ID.

        Each chunk uses one query while preserving the single-photo lookup's
        EXIF-pair preference and linked-location fallback. Photos without a
        complete coordinate pair are omitted from the returned mapping.
        """
        return self._location_repository().get_effective(
            photo_ids, verify_workspace=verify_workspace,
        )

    def get_accepted_species(self):
        """Return distinct marker species from geolocated photos in the active workspace.

        Uses the same "geolocated" definition as get_geolocated_photos: a
        photo is included if it has EXIF coords OR a ``type='location'``
        keyword with coords. That keeps the species filter dropdown in sync
        with which photos can actually appear as markers — otherwise photos
        placed via location-keyword coords would render on the map but their
        species would be missing from the filter.
        """
        return self._keyword_repository().get_accepted_species()

    def count_photos_without_gps(self):
        """Backward-compatible alias for :meth:`count_photos_without_coordinates`."""
        return self.count_photos_without_coordinates()

    def count_photos_without_coordinates(self):
        """Count photos in the active workspace that the map can't plot.

        A photo IS plottable when either its EXIF lat/lng are both present
        OR it carries a ``type='location'`` keyword whose lat/lng are both
        present (matches :meth:`get_geolocated_photos`'s paired-fallback
        semantics). This counter excludes those.

        Used by the ``/api/photos/geo`` response to drive the map's
        "Showing N of M geolocated photos" label — keeping the two
        definitions in lockstep so M is never less than N.
        """
        return self._location_repository().count_photos_without_coordinates()

    def get_plottable_photo_ids(self, folder_id=None):
        """Return ids of every photo the Map endpoint could render.

        A photo is plottable when either its EXIF lat/lng are both present
        OR it carries a ``type='location'`` keyword whose lat/lng are both
        present — the same paired-fallback semantics
        :meth:`get_geolocated_photos` and
        :meth:`count_photos_without_coordinates` use.

        Callers pass this to ``VisualScope.apply_to_rules`` for the Map path so
        the visual candidate set is restricted to plottable photos BEFORE
        the embedding query runs. Without it, a workspace with embeddings
        on non-plottable photos but none on plottable ones returns
        ``status: "ok"`` with non-plottable ids; ``get_geolocated_photos``
        then intersects them away and the map silently renders zero
        markers with no fallback / no-index warning.
        """
        return self._location_repository().get_plottable_photo_ids(
            folder_id, folder_subtree_ids=self.get_folder_subtree_ids,
        )

    def update_photo_rating(self, photo_id, rating, verify_workspace=True):
        """Set photo rating (0-5).

        Args:
            verify_workspace: when True (the default), raises ValueError if
                the photo is not in the active workspace's folders.  Pass
                False from background jobs that already scope their photo
                lists, or from undo/redo where the edit history is already
                workspace-scoped.
        """
        self._photo_review_repository().set_rating(
            photo_id, rating, verify_workspace=verify_workspace
        )

    def batch_update_photo_rating(self, photo_ids, rating, verify_workspace=True):
        """Set rating for multiple photos in a single transaction.

        Args:
            verify_workspace: when True, raises ValueError if any photo is
                not in the active workspace.
        """
        self._photo_review_repository().set_ratings(
            photo_ids, rating, verify_workspace=verify_workspace
        )

    def update_photo_flag(self, photo_id, flag, verify_workspace=True, _commit=True):
        """Set photo flag ('none', 'flagged', 'rejected').

        Args:
            verify_workspace: when True (the default), raises ValueError if
                the photo is not in the active workspace's folders.
            _commit: If False, skip the internal commit (caller is responsible
                     for committing the transaction). Callers that hold
                     ``BEGIN IMMEDIATE`` — the prediction decision lock, for
                     example — must pass False so the writer lock is not
                     released mid-decision.
        """
        self._photo_review_repository().set_flag(
            photo_id, flag, verify_workspace=verify_workspace, _commit=_commit
        )

    def update_photo_wildlife_excluded(self, photo_id, excluded, verify_workspace=True):
        """Set whether a photo is excluded from wildlife detection/classification."""
        if verify_workspace:
            self._verify_photo_in_workspace(photo_id)
        self._photo_review_repository().set_wildlife_excluded(photo_id, excluded)

    def batch_update_photo_flag(self, photo_ids, flag, verify_workspace=True):
        """Set flag for multiple photos in a single transaction.

        Args:
            verify_workspace: when True, raises ValueError if any photo is
                not in the active workspace.
        """
        self._photo_review_repository().set_flags(
            photo_ids, flag, verify_workspace=verify_workspace
        )

    def _photo_review_repository(self):
        from repositories.photo_review import PhotoReviewRepository

        return PhotoReviewRepository(
            self.conn,
            self._active_workspace_id,
            chunk_size=_SQLITE_PARAM_CHUNK_SIZE,
        )

    from repositories.photo_labels import VALID_COLOR_LABELS

    def set_color_label(self, photo_id, color):
        """Set a color label for a photo in the active workspace."""
        self._photo_label_repository().set(photo_id, color)

    def remove_color_label(self, photo_id):
        """Remove the color label for a photo in the active workspace."""
        self._photo_label_repository().remove(photo_id)

    def get_color_label(self, photo_id):
        """Return the color label for a photo in the active workspace, or None."""
        return self._photo_label_repository().get(photo_id)

    def get_color_labels_for_photos(self, photo_ids):
        """Return a dict of {photo_id: color} for the active workspace."""
        return self._photo_label_repository().get_for_photos(photo_ids)

    def filter_photo_ids_in_workspace(self, photo_ids):
        """Return existing, active-workspace photo IDs in input order."""
        return self._photo_label_repository().visible_photo_ids(photo_ids)

    def batch_set_color_label(self, photo_ids, color):
        """Set or remove color label for multiple photos in the active workspace."""
        self._photo_label_repository().set_many(photo_ids, color)

    def get_color_label_descriptions(self):
        """Return color-label descriptions for the active workspace."""
        return self._photo_label_repository().get_descriptions()

    def set_color_label_description(self, color, description):
        """Set or clear one color-label description in the active workspace."""
        return self._photo_label_repository().set_description(color, description)

    def _photo_label_repository(self):
        from repositories.photo_labels import PhotoLabelRepository

        return PhotoLabelRepository(
            self.conn,
            self._ws_id(),
            chunk_size=_SQLITE_PARAM_CHUNK_SIZE,
        )

    def _edits_repository(self):
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

    def get_photo_edit_recipe(self, photo_id, verify_workspace=False):
        """Return the normalized edit recipe dict for a photo, or None."""
        if verify_workspace:
            self._verify_photo_in_workspace(photo_id)
        return self._edits_repository().get_photo_recipe(photo_id)

    def get_photo_edit_recipes(self, photo_ids):
        """Return {photo_id: normalized recipe dict} for the given photos."""
        return self._edits_repository().get_photo_recipes(photo_ids)

    def set_photo_edit_recipe(self, photo_id, recipe, verify_workspace=True, _commit=True):
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

    def clear_photo_edit_recipe(self, photo_id, verify_workspace=True):
        """Remove a photo's edit recipe. Returns True if a row was removed."""
        if verify_workspace:
            self._verify_photo_in_workspace(photo_id)
        return self._edits_repository().clear_photo_recipe(photo_id)

    # --- edit presets (global reusable development settings) ----------------------

    EDIT_PRESET_NAME_MAX = 80

    def list_edit_presets(self):
        """Return all edit presets, sorted case-insensitively by name.

        Presets are global (not workspace-scoped): they capture a look, and a
        look is the same look in every workspace.
        """
        return self._edits_repository().list_presets()

    def save_edit_preset(self, name, recipe, fields=None):
        """Create or overwrite (by trimmed name) a global edit preset.

        Explicit fields retain just the selected settings, including neutral
        values. Legacy callers keep adjustments-only preset semantics.
        Raises ValueError (or RecipeError, its subclass) for a blank or
        overlong name or malformed settings. Legacy calls also require an
        effective adjustment; explicit fields may store neutral resets.
        Returns the stored preset dict.
        """
        return self._edits_repository().save_preset(name, recipe, fields=fields)

    def delete_edit_preset(self, preset_id):
        """Delete an edit preset. Returns True if a row was removed."""
        return self._edits_repository().delete_preset(preset_id)

    def prune_pipeline_cache_for_ids(self, ids):
        """Remove ``ids`` from the workspace's pipeline review cache file.

        Split out from ``delete_photos`` so chunked callers can defer it to
        after their outer transaction commits — pruning the on-disk cache
        is not transactional, so running it per-chunk would leave the cache
        permanently stripped of rows that a later rollback restores.
        """
        if not ids or self._db_path == ":memory:" or not self._active_workspace_id:
            return
        try:
            from pipeline import prune_results
            prune_results(
                os.path.dirname(self._db_path),
                self._active_workspace_id,
                ids,
            )
        except BaseException as exc:
            if isinstance(exc, (KeyboardInterrupt, GeneratorExit)):
                raise
            log.exception("Failed to prune pipeline cache after delete")

    def count_photos_with_companions(self, photo_ids):
        """How many of ``photo_ids`` carry a companion file in the active
        workspace.

        The delete dialog offers "Also delete N companion files" off this
        number, and the browser cannot compute it: a Browse selection can
        hold photos it has never loaded — every frame of a collapsed stack,
        or a Select-all that reaches past the loaded page — and a count taken
        over only the loaded ones hides the checkbox, so a disk delete leaves
        the companions of the rest behind. Counted here, where every row is
        known. Chunked for the same reason as ``resolve_photos_for_delete``:
        callers pass whole selections.

        Ids whose folder is not visible in the active workspace do not count:
        this endpoint sits next to ``/api/photos/by-ids``, which explicitly
        scopes its ids to the caller's workspace, and metadata about other
        workspaces' photos must not leak here either.
        """
        return self._photos_repository().count_with_companions(photo_ids)

    def resolve_photos_for_delete(self, photo_ids, include_companions=False):
        """Resolve photo ids to the rows and paths a delete would remove.

        This is intentionally read-only. Disk-delete callers use it to move
        originals to Trash *before* deleting their catalog rows, so a failed
        filesystem operation leaves the photo visible and retryable in Vireo.

        Returns a dict with ``ids``, ``files``, and the private ``_rows`` used
        by :meth:`delete_photos`. ``files`` retains the long-standing cleanup
        shape and additionally includes ``folder_id`` for callers that need to
        group related paths.
        """
        return self._photos_repository(scoped=False).resolve_for_delete(
            photo_ids, include_companions=include_companions,
        )

    def delete_photos(self, photo_ids, include_companions=False, commit=True):
        """Delete photos and all associated data.

        Returns dict with 'deleted' count, 'ids' list of deleted photo IDs
        (post-companion-resolution), and 'files' list of
        {photo_id, folder_path, filename, companion_path} for file cleanup.

        When ``commit`` is ``False``, this call participates in an outer
        transaction managed by the caller: no ``commit()``/``rollback()`` is
        issued here, **and** the non-DB pipeline-cache prune is skipped so
        a later failed chunk can roll the DB back without leaving a
        permanently-mutated cache file on disk. The caller is responsible
        for invoking ``prune_pipeline_cache_for_ids`` with the union of
        returned ``ids`` after the outer commit succeeds.
        """
        resolved = self.resolve_photos_for_delete(
            photo_ids, include_companions=include_companions,
        )
        rows = resolved.pop("_rows")
        if not rows:
            return {"deleted": 0, "ids": [], "files": []}
        all_ids = resolved["ids"]
        files = resolved["files"]

        # Count photos per folder for decrementing
        folder_counts = {}
        deleted_stems_by_folder = {}
        folder_paths = {}
        for row in rows:
            folder_counts[row["folder_id"]] = folder_counts.get(row["folder_id"], 0) + 1
            deleted_stems_by_folder.setdefault(row["folder_id"], set()).add(
                os.path.splitext(row["filename"])[0]
            )
            folder_paths[row["folder_id"]] = row["folder_path"]

        # Collect affected folder ids BEFORE the delete so we can invalidate the
        # new-images cache even if the delete raises. In "Remove from Vireo"
        # mode the on-disk files stay put, so they become eligible for new-image
        # detection again the moment the photo rows are gone; without an
        # invalidation here, ``/api/workspaces/active/new-images`` would keep
        # serving the stale pre-delete ``new_count`` until the TTL expired.
        affected_folder_ids = list(folder_counts.keys())

        try:
            self._photos_repository(scoped=False).delete(
                all_ids,
                folder_counts,
                deleted_stems_by_folder,
                folder_paths,
                commit=commit,
                workspace_id_fn=self._ws_id,
            )
        finally:
            # Always invalidate — even on rollback we may have partially dirtied
            # state, and on success the removed rows mean untracked on-disk
            # files should re-surface as "new" on the next read.
            if affected_folder_ids:
                self.invalidate_new_images_cache_for_folders(affected_folder_ids)

        # Prune the pipeline review cache so deleted photos don't render as
        # blank cards on the pipeline review page. Skipped when ``commit`` is
        # False so chunked callers can defer this non-transactional side
        # effect until after their outer commit — otherwise a rolled-back
        # later chunk would leave the on-disk cache permanently stripped of
        # rows the DB just restored.
        if commit:
            self.prune_pipeline_cache_for_ids(all_ids)
        return {"deleted": len(all_ids), "ids": all_ids, "files": files}

    # ------------------------------------------------------------------
    # preview_cache LRU
    # ------------------------------------------------------------------
    def _caches_repository(self):
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

    def preview_cache_insert(self, photo_id, size, bytes_):
        """Insert or replace a preview_cache entry. last_access_at = now()."""
        self._caches_repository().preview_insert(photo_id, size, bytes_)

    def preview_cache_touch(self, photo_id, size):
        """Update last_access_at for an existing entry. No-op if missing."""
        self._caches_repository().preview_touch(photo_id, size)

    def preview_cache_delete(self, photo_id, size):
        """Delete a preview_cache entry (caller removes the file)."""
        self._caches_repository().preview_delete(photo_id, size)

    def preview_cache_total_bytes(self):
        """Return total bytes tracked in preview_cache."""
        return self._caches_repository().preview_total_bytes()

    def preview_cache_oldest_first(self):
        """Return all rows ordered by last_access_at ascending (oldest first)."""
        return self._caches_repository().preview_oldest_first()

    def preview_cache_get(self, photo_id, size):
        """Return the row for (photo_id, size), or None."""
        return self._caches_repository().preview_get(photo_id, size)

    # ------------------------------------------------------------------
    # offline original cache
    # ------------------------------------------------------------------
    def offline_original_upsert(
        self,
        photo_id,
        original_path,
        xmp_path,
        companion_path,
        bytes_,
        source_size,
        source_mtime,
        cached_at,
        status,
        error=None,
    ):
        self._caches_repository().offline_original_upsert(
            photo_id,
            original_path,
            xmp_path,
            companion_path,
            bytes_,
            source_size,
            source_mtime,
            cached_at,
            status,
            error,
        )

    def offline_original_get(self, photo_id):
        return self._caches_repository().offline_original_get(photo_id)

    def offline_original_delete(self, photo_id):
        self._caches_repository().offline_original_delete(photo_id)

    def offline_original_total_bytes(self):
        return self._caches_repository().offline_original_total_bytes()

    def update_photo_sharpness(self, photo_id, sharpness):
        """Set photo sharpness score."""
        self._photos_repository(scoped=False).update_sharpness(photo_id, sharpness)

    def update_photo_quality(
        self,
        photo_id,
        subject_sharpness=None,
        subject_size=None,
        quality_score=None,
        sharpness=None,
    ):
        """Update all quality-related scores for a photo."""
        self._photos_repository(scoped=False).update_quality(
            photo_id,
            subject_sharpness=subject_sharpness,
            subject_size=subject_size,
            quality_score=quality_score,
            sharpness=sharpness,
        )

    def _masks_features_repository(self, *, scoped=True):
        """Build the masks/features repository on this connection.

        Mask, feature and embedding rows are catalog-wide; ``scoped=True``
        binds the active workspace (raising ``RuntimeError`` when none is
        set) for the pipeline selectors that read through it.
        ``commit_with_retry`` is read from this module at call time so tests
        that patch ``db.commit_with_retry`` still apply.
        """
        from repositories.masks_features import MasksFeaturesRepository

        return MasksFeaturesRepository(
            self.conn,
            self._ws_id() if scoped else None,
            commit_with_retry=commit_with_retry,
        )

    def get_photo_mask(self, photo_id, variant):
        return self._masks_features_repository(scoped=False).get_mask(
            photo_id, variant,
        )

    def list_masks_for_photo(self, photo_id):
        return self._masks_features_repository(
            scoped=False,
        ).list_masks_for_photo(photo_id)

    def set_active_mask_variant(self, photo_id, variant, _commit=True, *, weak_rescue_min_conf=None):
        """Mark `variant` as active for `photo_id` and denormalize its
        fields into the photos row (mask_path + per-mask features) so
        downstream readers (scoring, pipeline) see the active mask.

        ``_commit=False`` lets bulk callers (e.g. the
        ``/api/pipeline/active-mask-variant`` endpoint) batch many
        per-photo updates into a single commit, instead of paying a WAL
        fsync per photo. Bulk callers MUST call ``commit_with_retry``
        themselves once the loop completes.
        """
        import config as cfg
        effective = self.get_effective_config(cfg.load())
        min_conf = effective.get("detector_confidence", 0.2)
        self._masks_features_repository(scoped=False).set_active_variant(
            photo_id, variant, min_conf, _commit,
            weak_rescue_min_conf=weak_rescue_min_conf,
        )

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

    def delete_masks_for_variant(self, variant):
        """Delete all photo_masks rows + files for a variant.
        Refuses if the variant is active for any photo (caller must
        switch active first)."""
        return self._masks_features_repository(scoped=False).delete_for_variant(
            variant, self._safe_remove_mask_file,
        )

    def delete_inactive_masks(self):
        """Delete all photo_masks rows + files except the active variant
        per photo. Returns the number of rows deleted.

        Photos whose ``active_mask_variant IS NULL`` are skipped entirely
        (we never delete the only mask we know about). The user must
        promote a variant to active first via the pipeline page; the
        sentinel migration variant ``'unknown'`` is set as active for
        legacy photos, so this is only the partial-state case where a
        prior pipeline run wrote ``photo_masks`` but crashed before
        ``set_active_mask_variant`` ran.
        """
        return self._masks_features_repository(scoped=False).delete_inactive(
            self._safe_remove_mask_file,
        )

    def find_stale_masks(self, detector_confidence=None):
        """Return masks whose prompts differ from the selected primary.

        Selection uses the same manual-choice, quality, confidence, and ID
        ordering as extraction. When supplied, ``detector_confidence`` hides
        boxes below the workspace floor before selection. A mask matching a
        secondary or now-hidden detection is stale even if its row remains
        cached for later reuse.
        """
        return self._masks_features_repository(scoped=False).find_stale(
            detector_confidence=detector_confidence,
        )

    def delete_stale_masks(self, detector_confidence=None):
        """Remove rows + files for masks whose prompt no longer matches
        the current primary detection. Skips active variants (caller can
        re-run them through the pipeline instead of dropping the
        currently-displayed mask).

        ``detector_confidence`` is forwarded to :meth:`find_stale_masks`
        so the deletion set matches the count the storage card shows.
        """
        stale = self.find_stale_masks(detector_confidence=detector_confidence)
        return self._masks_features_repository(scoped=False).delete_stale(
            stale, self._safe_remove_mask_file,
        )

    def mask_variant_coverage(self):
        """Per-variant photo coverage in the **active workspace**.

        photo_masks rows are global (a single mask file is shared across
        workspaces), but the pipeline page wants workspace-scoped numbers
        so a user with a small workspace doesn't see counts dominated by
        photos they can't see. For each variant present in photo_masks,
        return the count of distinct workspace photos that have a row for
        that variant, plus the count of those that also have it active.

        Returns: list of dicts {variant, count, active_count} ordered by
        variant name. Variants with zero workspace photos are omitted.
        """
        return self._masks_features_repository().variant_coverage()

    def sam_variant_rerun_warning(
        self,
        sam2_variant,
        photo_ids=None,
        min_conf=None,
        selected_max_ratio=0.25,
        alternate_min_ratio=0.80,
    ):
        """Warn when selected SAM coverage is poor but another variant is high.

        The target set matches the extract-masks stage's existing-photo
        eligibility: active-workspace photos in scope with at least one real
        detection above the workspace detector-confidence floor. This keeps a
        workspace-level SAM configuration from looking empty just because a
        different variant already produced masks for those same target photos.
        """
        if not sam2_variant or sam2_variant == "unknown":
            return None
        if min_conf is None:
            import config as cfg
            min_conf = self.get_effective_config(cfg.load()).get(
                "detector_confidence", 0.2,
            )

        repo = self._masks_features_repository()
        scope_sql, scope_params = self._scope_clause(photo_ids)
        return repo.sam_variant_rerun_warning(
            sam2_variant, min_conf, scope_sql, scope_params,
            selected_max_ratio=selected_max_ratio,
            alternate_min_ratio=alternate_min_ratio,
        )

    def mask_variants_summary(self):
        """Per-variant summary: count, total bytes (best-effort, sums
        on-disk file sizes), and active_count.

        Returns: list of dicts ordered by variant name.
        """
        return self._masks_features_repository(scoped=False).variants_summary()

    def upsert_photo_mask(
        self, photo_id, variant, path,
        detector_model, prompt_x, prompt_y, prompt_w, prompt_h,
        subject_size=None, subject_tenengrad=None,
        bg_tenengrad=None, crop_complete=None, _commit=True,
        quality_input_recipe=None,
        subject_clip_high=None, subject_clip_low=None, subject_y_median=None,
        bg_separation=None, phash_crop=None, noise_estimate=None,
    ):
        """Insert or replace a mask row for (photo_id, variant).

        ``_commit=False`` lets a caller include the row in a larger atomic
        per-photo persistence transaction.
        """
        self._masks_features_repository(scoped=False).upsert_mask(
            photo_id, variant, path,
            detector_model, prompt_x, prompt_y, prompt_w, prompt_h,
            subject_size=subject_size, subject_tenengrad=subject_tenengrad,
            bg_tenengrad=bg_tenengrad, crop_complete=crop_complete,
            _commit=_commit, quality_input_recipe=quality_input_recipe,
            subject_clip_high=subject_clip_high,
            subject_clip_low=subject_clip_low,
            subject_y_median=subject_y_median, bg_separation=bg_separation,
            phash_crop=phash_crop, noise_estimate=noise_estimate,
        )

    def save_subject_raw_analysis(self, detection_id, report, _commit=True):
        """Keep original and corrected measurements together for each detection."""
        self._masks_features_repository(
            scoped=False,
        ).save_subject_raw_analysis(detection_id, report, _commit=_commit)

    def update_photo_pipeline_features(
        self,
        photo_id,
        mask_path=_UNSET,
        subject_tenengrad=_UNSET,
        bg_tenengrad=_UNSET,
        crop_complete=_UNSET,
        bg_separation=_UNSET,
        subject_clip_high=_UNSET,
        subject_clip_low=_UNSET,
        subject_y_median=_UNSET,
        phash_crop=_UNSET,
        noise_estimate=_UNSET,
        eye_x=_UNSET,
        eye_y=_UNSET,
        eye_conf=_UNSET,
        eye_tenengrad=_UNSET,
        eye_kp_fingerprint=_UNSET,
        quality_input_recipe=_UNSET,
        _commit=True,
    ):
        """Update pipeline feature columns for a photo.

        Only updates columns whose values are explicitly provided (not _UNSET).
        ``_commit=False`` lets a caller include the update in a larger atomic
        per-photo persistence transaction.
        """
        self._masks_features_repository(scoped=False).update_pipeline_features(
            photo_id,
            mask_path=mask_path,
            subject_tenengrad=subject_tenengrad,
            bg_tenengrad=bg_tenengrad,
            crop_complete=crop_complete,
            bg_separation=bg_separation,
            subject_clip_high=subject_clip_high,
            subject_clip_low=subject_clip_low,
            subject_y_median=subject_y_median,
            phash_crop=phash_crop,
            noise_estimate=noise_estimate,
            eye_x=eye_x,
            eye_y=eye_y,
            eye_conf=eye_conf,
            eye_tenengrad=eye_tenengrad,
            eye_kp_fingerprint=eye_kp_fingerprint,
            quality_input_recipe=quality_input_recipe,
            _commit=_commit,
        )

    def get_photos_missing_masks(self, folder_ids=None):
        """Get photos that have detections but no masks yet.

        Returns photos that have at least one detection in the current workspace
        but no mask_path set. Each row includes the selected primary
        detection box.

        Args:
            folder_ids: optional list of folder IDs to filter by.
                        If None, returns all workspace photos without masks.
        Returns:
            list of dicts with id, folder_id, filename, detection_box (JSON string), detection_conf
        """
        import config as cfg
        repo = self._masks_features_repository()
        min_conf = self.get_effective_config(cfg.load()).get(
            "detector_confidence", 0.2
        )
        return repo.photos_missing_masks(folder_ids, min_conf)

    def list_photos_for_eye_keypoint_stage(self, photo_ids=None):
        """Return photos eligible for the eye-focus keypoint stage.

        Eligibility:
          * photo is in the active workspace (via workspace_folders)
          * mask_path is set (SAM2 produced a subject mask)
          * has at least one non-synthetic detection (excludes full-image
            rows that exist only to anchor predictions)
          * has at least one prediction on that detection
          * has not been attempted with the current keypoint model fingerprint
            yet. The stage stamps eye_kp_fingerprint even when no trustworthy
            eye is found, so no-eye photos do not rerun forever.
          * (optional) photo.id is in ``photo_ids`` when provided — lets the
            caller scope the stage to a collection so a pipeline run doesn't
            touch unrelated photos elsewhere in the workspace

        Returns one row per photo. Only the effective primary detection
        can supply eye predictions, resolved with the same
        threshold-aware ordering (``subjects.primary_order_sql``) that
        mask extraction and ``set_active_mask_variant`` use: user
        override in ``photo_subject_choices`` → subject-analysis quality
        score → detector confidence. Anchoring on the current floor
        (not the cached ``photo_subject_state``) means a workspace
        raising ``detector_confidence`` above the previously stored
        subject still surfaces the new primary for the eye stage.
        Within the chosen detection, predictions carrying routable
        taxonomy info (``taxonomy_class`` or ``scientific_name`` set)
        rank ahead of predictions missing both, so a top-confidence but
        taxonomy-less prediction never masks a routable one that
        ``_resolve_keypoint_model`` could actually run. Each row is a
        dict with the fields the eye stage needs to run without further
        DB calls: id, folder_id, filename, width, height, mask_path,
        box_x/y/w/h (normalized 0-1), species_conf, taxonomy_class,
        scientific_name, species.
        """
        import config as cfg
        from pipeline import EYE_KP_FINGERPRINT_VERSION
        repo = self._masks_features_repository()
        min_conf = self.get_effective_config(cfg.load()).get(
            "detector_confidence", 0.2
        )
        if photo_ids is not None:
            photo_ids = list(photo_ids)
            if not photo_ids:
                return []
        extra_where, scope_params = self._scope_clause(photo_ids)
        return repo.list_photos_for_eye_keypoint_stage(
            min_conf, extra_where, scope_params,
            eye_kp_fingerprint_version=EYE_KP_FINGERPRINT_VERSION,
        )

    def update_photo_embeddings(
        self, photo_id, dino_subject_embedding=None, dino_global_embedding=None,
        variant=None, _commit=True,
    ):
        """Store DINOv2 embedding BLOBs for a photo.

        Args:
            photo_id: photo ID
            dino_subject_embedding: bytes (float32 numpy array .tobytes())
            dino_global_embedding: bytes (float32 numpy array .tobytes())
            variant: DINOv2 variant name that produced the embeddings
                (e.g. "vit-b14"). Stored so the pipeline can detect stale
                embeddings after a variant switch and drop them instead of
                feeding mismatched-dim vectors to cosine similarity.
            _commit: commit immediately by default. Set False only when the
                caller owns a larger transaction and will commit it.
        """
        self._masks_features_repository(scoped=False).update_embeddings(
            photo_id,
            dino_subject_embedding=dino_subject_embedding,
            dino_global_embedding=dino_global_embedding,
            variant=variant,
            _commit=_commit,
        )

    # -- Keywords --

    def _keyword_repository(self):
        """Build the keyword repository on this connection.

        The active workspace is resolved lazily (``Database._ws_id`` is
        passed as a resolver), so methods keep raising at the point they
        always did. Every façade method a moved body calls is handed over
        bound, under its ``Database`` name, so monkeypatches keep reaching
        the moved code; the class attributes and ``db`` module names the
        bodies read (``_chunks``, ``log``, ``resolve_import_alias``, the
        keyword-type and sentinel constants) are read here, at call time.
        """
        from repositories.keywords import KeywordRepository

        return KeywordRepository(
            self.conn,
            self._ws_id,
            chunks=_chunks,
            log=log,
            keyword_types=KEYWORD_TYPES,
            auto_match_review_marker=AUTO_MATCH_REVIEW_MARKER,
            detect_case_convention_sentinel=_DETECT_CASE_CONVENTION,
            taxon_lookup_variants=_taxon_lookup_variants,
            resolve_import_alias=resolve_import_alias,
            filter_subject_chunk=self._FILTER_SUBJECT_CHUNK,
            duplicate_photo_species_repair_key=(
                self._DUPLICATE_PHOTO_SPECIES_REPAIR_KEY
            ),
            facade=self,
        )

    def detect_keyword_case_convention(self):
        """Detect the casing convention used by existing species keywords.

        Returns:
            'title' if most are Title Case (e.g. "Black Phoebe")
            'lower' if most are lowercase after first word (e.g. "Black phoebe")
            'upper' if most are ALL CAPS
            None if not enough data to determine
        """
        return self._keyword_repository().detect_case_convention()

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
        self, name, apply_case_convention=True,
        case_convention=_DETECT_CASE_CONVENTION,
    ):
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
        return self._keyword_repository().resolve_species_display(
            name, apply_case_convention=apply_case_convention, case_convention=case_convention,
        )

    def species_case_convention(self):
        """The casing ``add_keyword`` would apply to a brand-new species name.

        The configured override wins; otherwise it is detected from the
        existing species keywords. Both readings cost a config load and a
        scan of every species keyword, which is why bulk callers resolve it
        once and hand it back to ``resolve_species_display_name`` instead of
        paying for it per name.
        """
        import config as cfg
        override = cfg.get("keyword_case")
        if override and override != "auto":
            return override
        return self.detect_keyword_case_convention()

    def _species_root_name_for_taxon(self, taxon_id):
        """Canonical root keyword spelling for a species taxon, if any.

        ``resolve_species_display_name`` uses the same lookup to route
        curation keys through the canonical root when only a hierarchy
        alias is stored (e.g. a leaf ``Desert Verdin`` after
        ``repair_duplicate_photo_species`` detached the top-level
        ``Verdin``). Callers with the taxon id in hand can skip the
        name-based lookup and go straight to the root row.
        """
        return self._keyword_repository().species_root_name_for_taxon(taxon_id)

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
        return self._keyword_repository().lookup_taxon_id(
            name, prefer_species=prefer_species, species_only=species_only,
        )

    def _add_source_species_keyword(self, name, source_taxon_id, parent_id=None, _commit=True):
        """Bind accepted source evidence without reassigning same-name tags."""
        return self._keyword_repository().add_source_species(
            name, source_taxon_id, parent_id=parent_id, _commit=_commit,
        )

    def relink_source_species_keywords(self):
        """Refresh local foreign keys after importing source taxa; caller commits."""
        return self._keyword_repository().relink_source_species()

    def add_keyword(self, name, parent_id=None, is_species=False, kw_type=None, _commit=True, source_taxon_id=None,
                    _resolve_alias=False):
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

    def _upsert_one_keyword(
        self, name, parent_id, place_id=None, latitude=None, longitude=None,
        reuse_location_component=False,
    ):
        """Insert-or-fetch a single ``type='location'`` keyword row.

        Two dedupe modes:

        * ``place_id`` is given: dedupe on the partial unique index over
          ``place_id``. If a row with that ``place_id`` already exists, update
          its name/parent/coords (the user just re-picked the same Google
          place) and return its id.
        * ``place_id`` is ``None``: dedupe on ``(name, parent_id)`` among rows
          whose ``place_id`` is also NULL. SELECT-then-INSERT (rather than
          ``INSERT OR IGNORE``) so we never collide a coordless parent row
          with a place_id-bearing leaf that happens to share a name+parent.
        * ``reuse_location_component`` is true only for administrative address
          components. In that case, a matching location hierarchy row can be
          reused even if it already carries a place id. A coordless matching
          row can also absorb the selected administrative place's id and
          coordinates instead of creating a suffixed duplicate.

        Cross-type collision handling: the table-level ``UNIQUE(name,
        parent_id)`` constraint doesn't filter by ``type``, so a pre-existing
        keyword of a *different* type with the same ``(name, parent_id)`` can
        cause our INSERT to raise ``sqlite3.IntegrityError``. Rather than
        silently merging into an unrelated keyword (which would corrupt the
        user's existing tags), we catch that error, re-SELECT to confirm
        what's actually there, and raise a descriptive ``RuntimeError``. If
        the existing row turns out to be a coordless ``type='location'``
        row that our narrow SELECT somehow missed, we defensively return its
        id.
        """
        name = normalize_keyword_display(name)
        if not name:
            raise ValueError("keyword name is empty after normalization")
        if place_id is not None:
            if reuse_location_component:
                existing_place = self.conn.execute(
                    "SELECT id FROM keywords WHERE place_id = ?",
                    (place_id,),
                ).fetchone()
                if parent_id is None:
                    existing_component = self.conn.execute(
                        "SELECT id, type FROM keywords "
                        "WHERE name = ? AND parent_id IS NULL "
                        "  AND place_id IS NULL "
                        "ORDER BY CASE WHEN type = 'location' THEN 0 "
                        "  WHEN type = 'taxonomy' THEN 1 ELSE 2 END, id "
                        "LIMIT 1",
                        (name,),
                    ).fetchone()
                else:
                    existing_component = self.conn.execute(
                        "SELECT id, type FROM keywords "
                        "WHERE name = ? AND parent_id = ? "
                        "  AND place_id IS NULL "
                        "LIMIT 1",
                        (name, parent_id),
                    ).fetchone()
                if (
                    existing_component is not None
                    and (
                        existing_component["type"] == "location"
                        or self._restore_misclassified_location_ancestor(
                            existing_component["id"],
                            allow_leaf=True,
                        )
                    )
                ):
                    if (
                        existing_place is not None
                        and existing_place["id"] != existing_component["id"]
                    ):
                        # Legacy admin disambiguation could leave a suffixed
                        # place-bearing row (e.g. "California (suffix)")
                        # alongside the matching coordless hierarchy row.
                        # Re-selecting the same Google place would otherwise
                        # fail the ON CONFLICT(place_id) DO UPDATE with
                        # UNIQUE(name, parent_id) and re-suffix instead of
                        # reusing the hierarchy row. Merge the suffixed row
                        # into the hierarchy row and let it own the place
                        # metadata so future assignments settle on one row.
                        self._merge_keyword_into(
                            existing_place["id"], existing_component["id"],
                        )
                    self.conn.execute(
                        "UPDATE keywords SET place_id = ?, type = 'location', "
                        "is_species = 0, taxon_id = NULL, latitude = ?, "
                        "longitude = ? WHERE id = ?",
                        (
                            place_id,
                            latitude,
                            longitude,
                            existing_component["id"],
                        ),
                    )
                    return existing_component["id"]
            insert_sql = (
                "INSERT INTO keywords "
                "(name, parent_id, type, place_id, latitude, longitude) "
                "VALUES (?, ?, 'location', ?, ?, ?) "
                "ON CONFLICT(place_id) WHERE place_id IS NOT NULL DO UPDATE SET "
                "  name = excluded.name, "
                "  parent_id = excluded.parent_id, "
                "  type = 'location', "
                "  is_species = 0, "
                "  taxon_id = NULL, "
                "  latitude = excluded.latitude, "
                "  longitude = excluded.longitude "
                "RETURNING id"
            )
            try:
                cur = self.conn.execute(
                    insert_sql, (name, parent_id, place_id, latitude, longitude),
                )
                return cur.fetchone()["id"]
            except sqlite3.IntegrityError:
                if reuse_location_component:
                    place_row = self.conn.execute(
                        "SELECT id FROM keywords WHERE place_id = ?",
                        (place_id,),
                    ).fetchone()
                    if place_row is None:
                        if parent_id is None:
                            clash = self.conn.execute(
                                "SELECT id, type, place_id FROM keywords "
                                "WHERE name = ? AND parent_id IS NULL",
                                (name,),
                            ).fetchone()
                        else:
                            clash = self.conn.execute(
                                "SELECT id, type, place_id FROM keywords "
                                "WHERE name = ? AND parent_id = ?",
                                (name, parent_id),
                            ).fetchone()
                        if (
                            clash is not None
                            and clash["place_id"] is None
                            and (
                                clash["type"] == "location"
                                or self._restore_misclassified_location_ancestor(
                                    clash["id"],
                                    allow_leaf=True,
                                )
                            )
                        ):
                            self.conn.execute(
                                "UPDATE keywords SET place_id = ?, latitude = ?, "
                                "longitude = ? WHERE id = ?",
                                (place_id, latitude, longitude, clash["id"]),
                            )
                            return clash["id"]
                # ON CONFLICT(place_id) handles same-place-id re-picks. The
                # remaining failure mode is the table-level UNIQUE(name,
                # parent_id): a *different* keyword (different place_id, or
                # NULL place_id) already occupies this slot. Disambiguate the
                # new row's name by appending a short place_id suffix and
                # retry. Realistic case: two distinct Google places with the
                # same name under the same parent (e.g. two parks named
                # "Riverside Park" in the same state).
                suffix = place_id[-8:]
                disambiguated = f"{name} ({suffix})"
                try:
                    cur = self.conn.execute(
                        insert_sql,
                        (disambiguated, parent_id, place_id, latitude, longitude),
                    )
                    return cur.fetchone()["id"]
                except sqlite3.IntegrityError as inner_err:
                    raise RuntimeError(
                        f"keyword '{name}' (parent_id={parent_id}) collides "
                        f"with an existing row even after disambiguation"
                    ) from inner_err

        if parent_id is None:
            if reuse_location_component:
                # Google's address_components carry no per-component
                # place_id, so when the DB has any place-bearing root at
                # this name (e.g. Georgia the state saved with a
                # place_id), we cannot prove that a component-only parent
                # of the same name refers to that specific Google place —
                # a later same-name second place (Georgia the country)
                # would misparent under the first one's root. Reuse a
                # coordless root when one exists (safe shared anchor);
                # otherwise fall through to inserting a fresh coordless
                # anchor rather than silently attaching under whichever
                # place-bearing row happens to have the lower id.
                candidates = self.conn.execute(
                    "SELECT id, type, place_id FROM keywords "
                    "WHERE name = ? AND parent_id IS NULL "
                    "  AND type IN ('location', 'taxonomy') "
                    "ORDER BY "
                    "  CASE WHEN type = 'location' THEN 0 ELSE 1 END, "
                    "  CASE WHEN place_id IS NULL THEN 0 ELSE 1 END, "
                    "  id",
                    (name,),
                ).fetchall()
                existing = None
                if candidates:
                    top = candidates[0]
                    if top["place_id"] is None:
                        existing = top
            else:
                existing = self.conn.execute(
                    "SELECT id, type FROM keywords "
                    "WHERE name = ? AND parent_id IS NULL "
                    "  AND type = 'location' AND place_id IS NULL",
                    (name,),
                ).fetchone()
        else:
            existing = self.conn.execute(
                "SELECT id, type FROM keywords "
                "WHERE name = ? AND parent_id = ? "
                "  AND type = 'location' "
                + ("" if reuse_location_component else "AND place_id IS NULL"),
                (name, parent_id),
            ).fetchone()
        if existing:
            if existing["type"] == "location":
                return existing["id"]
            if self._restore_misclassified_location_ancestor(
                existing["id"],
                allow_leaf=reuse_location_component,
            ):
                return existing["id"]

        try:
            cur = self.conn.execute(
                "INSERT INTO keywords "
                "(name, parent_id, type, place_id, latitude, longitude) "
                "VALUES (?, ?, 'location', NULL, ?, ?)",
                (name, parent_id, latitude, longitude),
            )
            return cur.lastrowid
        except sqlite3.IntegrityError as integrity_err:
            # UNIQUE(name, parent_id) violated by a row our type-filtered
            # SELECT didn't see. Find out what's actually there.
            if parent_id is None:
                clash = self.conn.execute(
                    "SELECT id, type, place_id FROM keywords "
                    "WHERE name = ? AND parent_id IS NULL",
                    (name,),
                ).fetchone()
            else:
                clash = self.conn.execute(
                    "SELECT id, type, place_id FROM keywords "
                    "WHERE name = ? AND parent_id = ?",
                    (name, parent_id),
                ).fetchone()
            if clash is None:
                # Should be unreachable — re-raise the original error
                # rather than swallow it.
                raise
            if (
                clash["type"] == "location"
                and (
                    clash["place_id"] is None
                    or reuse_location_component
                )
            ):
                # Defensive: our narrow SELECT missed it (shouldn't happen,
                # but reusing it is safe and idempotent).
                return clash["id"]
            if self._restore_misclassified_location_ancestor(
                clash["id"],
                allow_leaf=reuse_location_component,
            ):
                # Older taxonomy marking could retype an administrative
                # location node such as United States -> California even
                # though location children still hung below it. Reuse the
                # repaired node instead of making every later Google-place
                # assignment fail on the stale type.
                return clash["id"]
            raise RuntimeError(
                f"keyword '{name}' (parent_id={parent_id}) exists with "
                f"type={clash['type']!r}, can't reuse for location chain"
            ) from integrity_err

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

    def repair_misclassified_location_ancestors(self):
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
                merge_duplicate_roots=self._merge_duplicate_location_roots,
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

    def _merge_duplicate_location_roots(self):
        """Collapse same-name ``type='location'`` roots into a single row.

        Retyping a place-bearing taxonomy root at startup can leave two
        location rows with the same ``name`` and ``parent_id IS NULL``
        because SQLite's ``UNIQUE(name, parent_id)`` index does not enforce
        NULL parents. Multiple coordless duplicates without any competing
        place-bearing root describe the same neutral hierarchy anchor and
        must collapse so children stay under one shared parent.

        Whenever any place-bearing root exists at this name, coordless
        roots are neutral hierarchy anchors that
        ``_upsert_one_keyword`` installs (or preserves) for
        component-only references — Google's per-component
        ``address_components`` carry no ``place_id``, so we cannot prove
        which specific Google place the caller meant. Merging a
        coordless anchor into the place-bearing root would silently
        reparent its descendants under that Google place — the wrong
        answer once a second same-name Google place appears. Leave
        place-bearing roots and their coordless anchors alone; only
        consolidate coordless duplicates so ambiguous children still
        share one shared parent.
        """
        return self._location_repository().merge_duplicate_roots(
            merge_keyword_into=self._merge_keyword_into,
        )

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
            component_id = self._upsert_one_keyword(
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

    def upsert_place_chain(self, details):
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
            leaf_id = self._upsert_one_keyword(
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

    def set_photo_location(self, photo_id, leaf_keyword_id):
        """Set ``photo_id``'s location to ``leaf_keyword_id``.

        Removes any existing ``type='location'`` keyword links for the photo,
        then inserts the new link. Atomic.

        Raises ``ValueError`` if ``leaf_keyword_id`` does not exist or its
        keyword type is not ``'location'`` — otherwise the DELETE would strip
        real location links and replace them with a non-location link that
        :meth:`clear_photo_location` could not clean up.
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

    def clear_photo_location(self, photo_id):
        """Remove any ``type='location'`` keyword links for ``photo_id``.

        Does NOT delete the keyword rows themselves — other photos may still
        reference them, and even if they don't, free-text/place-id keywords
        are part of the user's vocabulary.
        """
        self._location_repository().clear_photo(photo_id)

    def get_or_create_text_location(self, name):
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
            return self._upsert_one_keyword(
                name=stripped,
                parent_id=None,
                place_id=None,
                latitude=None,
                longitude=None,
            )

    def link_keyword_to_place(self, keyword_id, details):
        """Attach Google place data to an existing keyword.

        ``details`` has the same shape as :meth:`upsert_place_chain`'s input.
        Builds the parent chain, then tries to UPDATE the target keyword with
        ``place_id``, coords, name, and the deepest parent's id. If another
        keyword already has the target ``place_id`` (UNIQUE collision on the
        partial index), the existing canonical row absorbs all
        ``photo_keywords`` rows from the target, and the now-empty target
        row is deleted.

        Returns ``{"keyword_id": <final id>, "merged": <bool>}``. ``merged``
        is True when an existing place-bearing row absorbed the target.
        """
        if not details.get("place_id"):
            raise ValueError("link_keyword_to_place requires details['place_id']")

        row = self.conn.execute(
            "SELECT type FROM keywords WHERE id = ?", (keyword_id,),
        ).fetchone()
        if row is None:
            raise ValueError(f"keyword id {keyword_id} does not exist")
        # Reject non-location keywords. place_id is globally unique, so
        # attaching one to (say) a species or general keyword would let later
        # location upserts resolve to a non-location row, after which
        # set_photo_location rejects it and the place is effectively unusable
        # until the row is manually cleaned up.
        if row["type"] != "location":
            raise ValueError(
                f"keyword id {keyword_id} is type '{row['type']}', not 'location'"
            )

        place_id = details["place_id"]
        new_name = details.get("name", "")
        lat = details.get("lat")
        lng = details.get("lng")
        components = details.get("address_components") or []

        with self.conn:
            chain = self._upsert_location_parent_chain(
                components,
                leaf_name=new_name,
                leaf_types=details.get("types"),
            )
            parent_id = chain[-1] if chain else None

            # If the chain itself reused this very keyword anywhere — as the
            # deepest parent OR as a non-leaf ancestor (e.g. a free-text
            # "United States" promoted into the country slot while deeper
            # levels like NY/Manhattan were also discovered) — the UPDATE
            # below would create a cycle by reparenting the row onto a
            # descendant of itself. Guard by checking the full visited chain.
            if keyword_id in chain:
                # The keyword we were asked to "link" got reused inside the
                # chain. Nothing to merge from photo_keywords (it is already
                # the canonical row for its slot), so just return it.
                return {"keyword_id": keyword_id, "merged": False}

            update_sql = (
                "UPDATE keywords SET "
                "  place_id = ?, "
                "  latitude = ?, "
                "  longitude = ?, "
                "  name = ?, "
                "  parent_id = ? "
                "WHERE id = ?"
            )
            try:
                self.conn.execute(
                    update_sql,
                    (place_id, lat, lng, new_name, parent_id, keyword_id),
                )
                return {"keyword_id": keyword_id, "merged": False}
            except sqlite3.IntegrityError:
                # Two distinct constraints can fail here:
                #   (a) UNIQUE(place_id) — another row already has this
                #       place_id → merge case.
                #   (b) UNIQUE(name, parent_id) — another row already
                #       owns this (name, parent_id) slot with a different
                #       (or NULL) place_id → name-collision case.
                # Disambiguate by checking which.
                canonical = self.conn.execute(
                    "SELECT id FROM keywords WHERE place_id = ?", (place_id,),
                ).fetchone()
                if canonical is not None and canonical["id"] != keyword_id:
                    # Case (a): merge.
                    canonical_id = canonical["id"]
                    # FK on keywords.parent_id is enforced (foreign_keys=ON),
                    # so any descendants of the old keyword would block the
                    # final DELETE FROM keywords. Reparent them onto the
                    # canonical row first — the canonical row represents the
                    # same place, so its descendants inherit cleanly.
                    # Per-child reparent so a UNIQUE(name, parent_id) clash
                    # in the canonical's existing subtree (a child with the
                    # same name) doesn't blow up the bulk UPDATE. On clash,
                    # disambiguate the migrating child's name with a short
                    # id suffix — preserves both rows' photo links rather
                    # than losing data.
                    children = self.conn.execute(
                        "SELECT id, name FROM keywords WHERE parent_id = ?",
                        (keyword_id,),
                    ).fetchall()
                    for child in children:
                        try:
                            self.conn.execute(
                                "UPDATE keywords SET parent_id = ? WHERE id = ?",
                                (canonical_id, child["id"]),
                            )
                        except sqlite3.IntegrityError:
                            disambiguated = f"{child['name']} (id-{child['id']})"
                            try:
                                self.conn.execute(
                                    "UPDATE keywords SET parent_id = ?, name = ? "
                                    "WHERE id = ?",
                                    (canonical_id, disambiguated, child["id"]),
                                )
                            except sqlite3.IntegrityError as inner_err:
                                raise RuntimeError(
                                    f"child keyword '{child['name']}' "
                                    f"(id={child['id']}) collides with the "
                                    f"canonical row's subtree even after "
                                    f"disambiguation"
                                ) from inner_err
                    # Re-point photo_keywords from old → canonical, carrying
                    # each association's durable provenance with it. On a
                    # conflict the canonical row keeps the stronger of the two
                    # claims rather than silently dropping a stamp.
                    self.conn.execute(
                        f"""INSERT INTO photo_keywords (photo_id, keyword_id, source)
                           SELECT photo_id, ?, source
                           FROM photo_keywords WHERE keyword_id = ?
                           {KEYWORD_SOURCE_CONFLICT_SQL}""",
                        (canonical_id, keyword_id),
                    )
                    self.conn.execute(
                        "DELETE FROM photo_keywords WHERE keyword_id = ?",
                        (keyword_id,),
                    )
                    # Preserve remembered import paths before the foreign-key
                    # cascade deletes aliases of the absorbed place.
                    self.conn.execute(
                        'UPDATE keyword_import_aliases SET keyword_id = ? WHERE keyword_id = ?',
                        (canonical_id, keyword_id),
                    )
                    # Delete the now-empty old keyword row.
                    self.conn.execute(
                        "DELETE FROM keywords WHERE id = ?", (keyword_id,),
                    )
                    return {"keyword_id": canonical_id, "merged": True}

                # Case (b): name collision — a *different* keyword already
                # holds the (new_name, parent_id) slot. Disambiguate the
                # name by appending a short place_id suffix and retry.
                # Same approach as _upsert_one_keyword's leaf-collision path.
                if parent_id is None:
                    name_clash = self.conn.execute(
                        "SELECT id FROM keywords "
                        "WHERE name = ? AND parent_id IS NULL AND id != ?",
                        (new_name, keyword_id),
                    ).fetchone()
                else:
                    name_clash = self.conn.execute(
                        "SELECT id FROM keywords "
                        "WHERE name = ? AND parent_id = ? AND id != ?",
                        (new_name, parent_id, keyword_id),
                    ).fetchone()
                if name_clash is None:
                    # Neither place_id nor name conflict — shouldn't happen
                    # but re-raise rather than swallow.
                    raise
                suffix = place_id[-8:]
                disambiguated = f"{new_name} ({suffix})"
                try:
                    self.conn.execute(
                        update_sql,
                        (place_id, lat, lng, disambiguated, parent_id, keyword_id),
                    )
                    return {"keyword_id": keyword_id, "merged": False}
                except sqlite3.IntegrityError as inner_err:
                    raise RuntimeError(
                        f"keyword '{new_name}' (parent_id={parent_id}) "
                        f"collides with an existing row even after disambiguation"
                    ) from inner_err

    @staticmethod
    def _reverse_geocode_grid(lat, lng):
        """Round (lat, lng) to a ~110m grid cell.

        Used as the cache key for reverse-geocode lookups so two coords from
        the same neighborhood share one Google call.
        """
        return int(round(lat * 1000)), int(round(lng * 1000))

    def reverse_geocode_cache_get(self, lat, lng):
        """Look up cached reverse-geocode response for (lat, lng).

        Returns ``{"place_id": <str|None>, "response": <str>}`` on hit
        (``response`` is the raw JSON string the put-side stashed). A row
        with ``place_id=None`` is a cached negative result — Google was
        asked and returned no match — and is still a hit. Returns ``None``
        only on a true miss (the cell was never populated).
        """
        lat_grid, lng_grid = self._reverse_geocode_grid(lat, lng)
        return self._location_repository().reverse_geocode_cache_get(
            lat_grid, lng_grid,
        )

    def reverse_geocode_cache_put(self, lat, lng, place_id, response_json):
        """Upsert reverse-geocode result at the (lat, lng) grid cell.

        ``place_id`` may be ``None`` to cache a negative result (Google
        returned no match). ``response_json`` is already a JSON string —
        the caller serializes; we don't re-encode.
        """
        lat_grid, lng_grid = self._reverse_geocode_grid(lat, lng)
        self._location_repository().reverse_geocode_cache_put(
            lat_grid, lng_grid, place_id, response_json,
        )

    def merge_duplicate_keywords(self):
        """Find and merge normalized duplicate keywords in active workspace.

        Duplicates are grouped by (normalized name, parent_id, type,
        species-bearing) — name alone is not identity: the location system
        deliberately creates same-name keywords under different parents
        (Springfield under Illinois vs. Missouri), and same-name keywords of
        different types (species vs. genre) are distinct by design. Legacy
        ``type='general', is_species=1`` rows are also distinct from ordinary
        general homonyms. Merging across those slots retags photos with the
        wrong place/kind.

        A keyword is in scope when it — or any descendant — is tagged on a
        photo in the active workspace. XMP import only tags the leaf of a
        hierarchical keyword ("Birds > Heron" tags Heron, not Birds), so
        duplicate ancestors usually have no photo_keywords rows of their
        own; walking up from the tagged leaves brings them in scope while
        still leaving other workspaces' keywords untouched.

        Keeps the lowest ID (earliest created), moves all photo associations,
        reparents any child keywords onto the survivor (the parent_id FK
        would otherwise block the DELETE), and deletes the duplicates.
        Runs passes until convergence so case-duplicate parent chains
        ("Birds">"Heron" vs "birds">"heron") fully collapse: the children
        only become same-parent duplicates after their parents merge.
        The whole pass is all-or-nothing: an exception rolls back every
        pending merge instead of leaving a half-merged tree on the
        connection for a later unrelated commit to persist.
        Returns count of merges performed.
        """
        return self._keyword_repository().merge_duplicates()

    def _merge_duplicate_keywords_pass(self, ws):
        """Convergence loop for merge_duplicate_keywords. Caller commits."""
        total_merged = 0
        while True:
            rows = self._keyword_repository().duplicate_scope_rows(ws)
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
                alive = self._keyword_repository().live_ids(all_ids)
                if keep_id not in alive:
                    continue
                remove_ids = [x for x in all_ids if x != keep_id and x in alive]

                self._normalize_keyword_row_name(keep_id)
                for rid in remove_ids:
                    total_merged += self._merge_keyword_into(rid, keep_id)

        return total_merged

    def _normalize_keyword_row_name(self, keyword_id, disambiguate_on_conflict=False):
        """Trim stray edge punctuation from a surviving keyword row name.

        Post-migration, stored names are already normalized, so this is a
        no-op in the common case — it exists so the duplicate-cleanup and
        migration paths can canonicalize a survivor whose spelling predates
        normalization. ``_rename_keyword_dependents`` then carries the new
        spelling into every string that mirrors it.

        ``disambiguate_on_conflict`` — when a different-type keyword already
        occupies (cleaned, parent_id) and the UPDATE would hit
        ``UNIQUE(name, parent_id)``, retry with a ``<cleaned> (id-<id>)``
        suffix so no stored variant survives. Used by the one-shot
        migration so its completion marker can honestly assert the "no
        stored variant" invariant; the runtime dedup path leaves this
        False and keeps the stored spelling in the collision case.
        """
        return self._keyword_repository().normalize_row_name(
            keyword_id, disambiguate_on_conflict=disambiguate_on_conflict,
        )

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
        return self._keyword_repository().rename_dependents(keyword_id, old_name, new_name)

    def normalize_keyword_data(self):
        """One-shot, db_meta-gated wrapper around the normalization backfill.

        Runs at most once per database (``db_meta['keyword_names_normalized']``).
        All-or-nothing: an exception rolls the whole sweep back — including
        the marker — so a failed run retries on the next open instead of
        leaving a half-normalized keyword table.
        """
        return self._keyword_repository().normalize_data()

    def _fold_prediction_species_apostrophes(self):
        """Rewrite ``predictions.species`` into normalize_keyword_display form.

        Predictions are compared against ``keywords.name`` with exact and
        ``COLLATE NOCASE`` matches (see the predicted-species subqueries in
        :meth:`get_photos`), neither of which can fold U+2019. A prediction
        stored as ``Swinhoe’s White-eye`` therefore never matched the
        accepted ``Swinhoe's white-eye`` keyword, so the photo's own
        prediction looked unaccepted in the UI.

        ``predictions`` has UNIQUE(detection_id, classifier_model,
        labels_fingerprint, species); the constraint is BINARY, so a single
        (detection, model, fingerprint) scope can legally hold three
        NOCASE-equivalent variants at once (e.g. ``Say's Phoebe``,
        ``Say's phoebe``, ``Say’s phoebe``). A per-row ``fetchone`` peer
        lookup would only merge one of the ASCII neighbours: with the
        curly row winning by confidence, the subsequent
        ``UPDATE ... SET species = 'Say's phoebe'`` would then collide with
        the unmerged ASCII-lowercase row and abort the whole migration
        under UNIQUE — and on every open thereafter. Fetch every
        NOCASE-equivalent peer up-front and merge the entire collision set
        around a single winner so the final UPDATE has no peer left to
        clash with.

        Before deleting any loser, migrate its workspace review rows onto
        the surviving prediction so an accepted/rejected decision on a
        variant (``prediction_review.prediction_id`` uses
        ``ON DELETE CASCADE``) is not silently lost, and retarget any
        ``prediction_accept`` edit-history references from the loser id
        to the winner id so undo/redo can still find the prediction after
        the DELETE.
        """
        return self._keyword_repository().fold_prediction_species_apostrophes()

    def _merge_prediction_review_before_delete(self, loser_id, winner_id):
        """Move per-workspace review rows from ``loser_id`` onto ``winner_id``.

        ``prediction_review.prediction_id`` is ``ON DELETE CASCADE``, so a
        bare ``DELETE FROM predictions`` silently drops any accepted /
        rejected user decision (and its group metadata) attached to the
        losing row.  Called from ``_fold_prediction_species_apostrophes``
        just before it deletes a duplicate: the two predictions differ only
        in spelling, so a review on either applies to the same (detection,
        species) pair and must survive the collision-merge.

        For each ``(loser_id, workspace_id)`` review row:

        - If the winner has no row for that workspace, absence encodes an
          implicit ``pending`` state, so treat it as a real row rather than
          a slot to be filled. Move the loser's row onto the winner only
          when the loser carries a genuine user decision
          (``accepted``/``rejected``) or non-status metadata (``group_id``
          etc.) worth preserving. A bare ``status='alternative'`` row on
          the loser is dropped instead: transferring it would turn a
          higher-confidence pending primary into an alternative, hiding
          the sole top-1 prediction from the pending queue.  When the
          decided loser IS itself the auto-accepted taxonomy match (its
          ``individual`` carries ``AUTO_MATCH_REVIEW_MARKER``), the marker
          is preserved on the transfer: it's the provenance
          ``reconcile_match_review_state`` uses to delete the row later
          if the XMP match goes away; scrubbing it would leave a stale
          auto-accept looking like a manual decision no automation can
          revisit. A pending-loser transfer still scrubs the marker
          (a non-decided row carrying it is spurious historical state,
          not a real auto-accept).
        - If the winner already has a row for that workspace (both were
          reviewed independently), keep whichever encodes the stronger
          decision: a non-pending status beats pending, and among two
          non-pending decisions the later ``reviewed_at`` wins.  Ties keep
          the winner's row so the choice stays deterministic.
        - Independently of the status choice, backfill missing group
          metadata (``group_id`` / ``vote_count`` / ``total_votes`` /
          ``individual``) from whichever side carries it: a pending loser
          that carries the current burst's ``group_id`` while the winner is
          also pending would otherwise be cascaded away and the surviving
          prediction would silently drop out of its burst group. This is
          safe because two rows for the same (detection, species) pair
          across spelling variants are always about the same burst.
          ``individual`` is filled only when the source value is not the
          ``AUTO_MATCH_REVIEW_MARKER`` sentinel; that string is provenance
          for auto-accepted taxonomy matches, and copying it onto a
          manually chosen accept/reject would let later automation
          (``preserve_manual_review`` / ``reconcile_match_review_state``)
          overwrite or delete the user's decision.

        The loser's remaining rows are removed by the caller's DELETE via
        the ON DELETE CASCADE, so no explicit cleanup is needed here.
        """
        return self._keyword_repository().merge_prediction_review_before_delete(
            loser_id, winner_id,
        )

    def _merge_prediction_metadata_before_delete(self, loser_id, winner_id):
        """Backfill non-null loser columns onto the winner before DELETE.

        Two colliding predictions (same detection / model / fingerprint,
        spellings that differ only by apostrophe) can hold different amounts
        of enrichment if they were written by different code paths: the
        classifier-with-taxonomy path fills ``category`` / ``scientific_name``
        / ``taxonomy_*``, but a raw-classifier path (or an older insert made
        before the taxonomy lookup existed) can leave those NULL.  When the
        row selected as the winner (by ``confidence``) happens to be the
        one without the enrichment, deleting the loser strips fields that
        taxonomy filters and review displays rely on.

        Backfills a column only when the winner is currently NULL, so a
        deliberate override on the winner is preserved.  ``category`` also
        promotes from the schema default ``'new'`` to a more specific
        ``'match'`` / ``'change'`` when only the loser carried it, but
        never overrides an explicit non-default winner category.  Called
        from ``_fold_prediction_species_apostrophes`` right before the
        CASCADEd DELETE removes the loser row.
        """
        return self._keyword_repository().merge_prediction_metadata_before_delete(
            loser_id, winner_id,
        )

    def _retarget_prediction_edit_history(self, loser_id, winner_id):
        """Rewrite prediction-id references in edit history from loser to winner.

        Three action types anchor prediction ids into
        ``edit_history_items.old_value``:

        - ``prediction_accept`` (``api_accept_prediction`` /
          ``api_accept_subject_species``): stores either a bare-int string
          (single-model, changed-tag accept), JSON
          ``{"prediction_id": N, "no_tag": true}`` (single no-op accept),
          or JSON ``{"prediction_ids": [N, ...], "no_tag"?: true}``
          (accept-subject collecting agreeing sibling classifier models).
        - ``keyword_add`` and ``species_replace``
          (``api_highlights_relabel``): store a JSON payload whose
          ``prediction_id`` field points at the top prediction captured
          when the relabel ran, so undo can restore its ``pending`` status
          via ``_restore_edit_prediction_status`` (and redo can re-reject
          it via ``_reject_edit_prediction``). Bare-int ``old_value`` for
          these two action types encodes the previous keyword id, not a
          prediction id, so it is left alone.

        When the fold migration deletes a colliding prediction row, an
        undo/redo later would call ``update_prediction_status(loser_id,
        ...)`` on the vanished id: the ``INSERT`` into ``prediction_review``
        then fails the FK to ``predictions``, aborting the undo/redo, and
        for the ``prediction_accept`` accept-subject variant the missing
        id would silently be skipped by ``_apply_undo`` so the surviving
        prediction stays anchored in its accepted state.

        Retargeting the reference from ``loser_id`` -> ``winner_id`` before
        the DELETE keeps undo/redo sound: the two predictions differ only
        in spelling, so any status flip captured on either applies to the
        same (detection, model, labels_fingerprint) scope after the merge.
        """
        return self._keyword_repository().retarget_prediction_edit_history(loser_id, winner_id)

    def _align_curation_species_case(self):
        """Re-key curation rows whose species differs from the canonical
        spelling only by case.

        ``normalize_keyword_display()`` preserves case, so the punctuation
        sweep leaves a curation row keyed ``Saffron Finch`` untouched while
        the species keyword row is ``Saffron finch`` — and the eligible
        highlight/life-list queries compare those strings EXACT against
        ``keywords.name``, so the curated selection silently drops out.

        Two sources of the canonical spelling, mirroring
        ``resolve_species_display_name`` (the function
        ``collect_highlight_buckets`` uses to canonicalize prediction
        labels):

        1. A single surviving root species keyword for the match_key.
           Intentionally-distinct same-key homonyms (e.g. a legacy
           ``type='general', is_species=1`` ``Robin`` alongside a taxonomy
           ``robin``) must not have every curation row for the other
           spelling rewritten onto the picked one — the joined queries
           would then match a species keyword the photo doesn't carry.
           Ambiguous case-variant homonyms are left as-is.
        2. If no keyword row exists at all — e.g. a highlight starred from
           an unconfirmed prediction bucket before the photo was accepted
           — apply the detected case convention so the row lands on the
           string the bucket will produce after this migration. Without
           this, the bucket-side canonicalization drifts to (say)
           ``Common waxbill`` while the highlight stays at
           ``Common Waxbill``, silently un-starring the photo.

        Returns the number of rows moved; caller commits.
        """
        return self._keyword_repository().align_curation_species_case()

    def _align_curation_history_species(self):
        """Rewrite curation species snapshots in edit_history_items.old_value.

        Relabel undo/redo payloads carry snapshots of curation rows keyed
        by species name. Normalizing only the live tables leaves those
        JSON snapshots pointing at the legacy spelling, so a later undo
        would recreate orphaned curation rows that no longer compare
        equal to the string the bucket / eligibility queries expect.
        Route every species value captured by hl_prev/pref_prev/rep_prev
        through the same canonicalization ``_align_curation_species_case``
        applies to the live tables (unambiguous stored spelling, ambiguous
        homonyms left alone, no-keyword predictions case-converted).
        Idempotent on already-normalized rows, so it's safe to re-run in
        the v2 gate after v1 has already normalized the punctuation.
        Returns the number of history rows rewritten; caller commits.
        """
        return self._keyword_repository().align_curation_history_species()

    def _species_keyword_maps(self):
        """Return ``(unique_species_by_key, all_species_keys)`` for
        curation alignment.

        ``unique_species_by_key``: match_key → stored root species
        spelling, ONLY for keys resolving to a single distinct spelling.
        Homonyms (multiple distinct spellings for the same key) are
        omitted so callers can't rewrite curation across genuinely
        different keyword rows.

        ``all_species_keys``: set of match_keys with any root species
        keyword row (ambiguous or not). Used to distinguish "no keyword
        row at all" — safe to canonicalize a curation species via the
        detected case convention — from "ambiguous homonym", which
        must be left alone.
        """
        return self._keyword_repository().species_maps()

    def _canonical_curation_species(
        self, name, unique_species_by_key, all_species_keys,
    ):
        """Canonical spelling for a curation species value.

        Agrees with ``collect_highlight_buckets`` / ``resolve_species_display_name``:

        - Unambiguous keyword match → use the stored spelling.
        - Ambiguous homonym → leave alone (returns the punctuation-
          normalized input unchanged).
        - No keyword row for the match_key → apply the same case
          convention ``collect_highlight_buckets`` uses when it
          canonicalizes predicted species labels, so a highlight starred
          from a prediction-only bucket (no keyword exists yet because
          the photo hasn't been accepted) keys on the string the bucket
          will emit after this migration.

        Empty input returns unchanged.
        """
        clean = normalize_keyword_display(name or "")
        if not clean:
            return clean
        key = keyword_match_key(clean)
        stored = unique_species_by_key.get(key)
        if stored:
            return stored
        if key in all_species_keys:
            return clean
        return self.resolve_species_display_name(clean)

    _DUPLICATE_PHOTO_SPECIES_REPAIR_KEY = "duplicate_photo_species_repaired_v1"

    def has_possible_duplicate_photo_species(self):
        """Whether ``repair_duplicate_photo_species`` could find anything."""
        return self._keyword_repository().has_possible_duplicate_photo_species()

    def repair_duplicate_photo_species(self):
        """Remove redundant same-photo associations for one species taxon.

        Older imports preserved Lightroom hierarchy leaves, while later
        species confirmations attached a second top-level keyword row. Both
        rows are useful globally, but one photo should not carry both for the
        same species-rank taxon. Remove only top-level associations when at
        least one hierarchy-bearing association exists; multiple deliberate
        hierarchy placements remain intact. Leave photo-scoped curation on
        the root spelling because that keyword row remains the canonical
        species key, and leave all keyword rows intact.

        Pending keyword changes are name-based rather than keyword-id-based.
        Once the hierarchy association survives, a queued remove for either
        spelling would incorrectly erase the surviving XMP keyword. Cancel
        matching adds/removes; the hierarchical association originated from
        that sidecar and remains the source of truth.

        When a detached root's spelling does not survive on the photo (for
        example a root ``Verdin`` is detached because a hierarchical alias
        ``Birds|Desert Verdin`` is kept), the sidecar's previously synced
        ``dc:subject: Verdin`` still names a keyword the DB no longer
        carries. Left alone, the next XMP-to-DB scan would flat-import
        ``Verdin`` and re-attach the top-level row this repair just
        removed. Queue a ``keyword_remove`` for those orphaned spellings
        so ``sync_to_xmp`` clears them from the sidecar; skip the queue
        when a surviving row (species or general, hierarchical or not)
        already carries the same normalized name, since the scanner's
        per-photo dedup keeps the flat entry from re-tagging in that case
        and a hierarchical remove would strip the surviving keyword.
        """
        return self._keyword_repository().repair_duplicate_photo_species()

    def _normalize_keyword_data_once(self):
        """One-shot backfill: normalize every stored keyword/species name.

        Historically, keyword names were stored verbatim, so sidecars and
        imports could seed edge-quote variants like ``‘apapane`` alongside
        ``apapane``. After this runs — and with add_keyword /
        update_keyword / queue_change normalizing on write — the DB only
        ever contains ``normalize_keyword_display()`` spellings, so runtime
        code never needs per-call-site legacy-variant guards. Caller
        (normalize_keyword_data) commits.
        """
        # Keyword rows whose name normalizes to empty are pure stray
        # punctuation (e.g. a keyword literally named "'"). There is no
        # canonical spelling to merge into, so reparent children up, drop
        # tags, and delete the row.
        dropped_empty = 0
        for row in self.conn.execute(
            "SELECT id, name FROM keywords"
        ).fetchall():
            if keyword_match_key(row["name"]):
                continue
            # Re-read the parent: an earlier empty row in this loop may have
            # been this row's parent and already reparented it upward.
            cur_row = self.conn.execute(
                "SELECT parent_id FROM keywords WHERE id = ?", (row["id"],)
            ).fetchone()
            if cur_row is None:
                continue
            parent_id = cur_row["parent_id"]
            children = self.conn.execute(
                "SELECT id, name, type FROM keywords WHERE parent_id = ?",
                (row["id"],),
            ).fetchall()
            for child in children:
                try:
                    self.conn.execute(
                        "UPDATE keywords SET parent_id = ? WHERE id = ?",
                        (parent_id, child["id"]),
                    )
                except sqlite3.IntegrityError:
                    existing = self.conn.execute(
                        "SELECT id, type FROM keywords "
                        "WHERE parent_id = ? AND name = ?",
                        (parent_id, child["name"]),
                    ).fetchone()
                    if existing and existing["type"] == child["type"]:
                        self._merge_keyword_into(child["id"], existing["id"])
                    else:
                        # Same name + parent but different type: outside the
                        # (name, parent_id, type) dedup boundary, preserve
                        # both (mirrors _merge_keyword_into's child handling).
                        self.conn.execute(
                            "UPDATE keywords SET parent_id = ?, name = ? "
                            "WHERE id = ?",
                            (parent_id, f"{child['name']} (id-{child['id']})",
                             child["id"]),
                        )
            self.conn.execute(
                "DELETE FROM photo_keywords WHERE keyword_id = ?", (row["id"],)
            )
            self.conn.execute("DELETE FROM keywords WHERE id = ?", (row["id"],))
            dropped_empty += 1

        # Merge rows that collapse to the same normalized identity. Global
        # (all workspaces), grouped by (normalized key, parent_id):
        # same-type rows merge; a 'general' row folds into the
        # highest-priority specific-typed peer ONLY when its stored name
        # is un-normalized (a variant that would collide with the peer's
        # clean spelling after normalize_keyword_display). Clean generals
        # sharing a match_key with a specific-type peer are intentional
        # cross-type homonyms (e.g. general 'Robin' alongside individual
        # 'Robin', or a legacy `type='general', is_species=1` species row
        # coexisting with an individual person named 'Robin') and must
        # stay separate — otherwise all their tags migrate onto the
        # specific-type survivor and _merge_keyword_into then clears
        # species metadata for the cross-type merge, silently dropping
        # those photos out of species/life-list filters. Rows of two
        # different specific types are distinct by design and stay
        # separate. Convergence loop because merging duplicate parents
        # makes their children same-slot duplicates.
        type_priority = {
            "taxonomy": 0, "genre": 1, "individual": 2, "location": 3,
            "general": 4,
        }
        merged = 0
        while True:
            grouped = {}
            for row in self.conn.execute(
                "SELECT id, name, parent_id, type, is_species FROM keywords"
            ).fetchall():
                key = keyword_match_key(row["name"])
                if not key:
                    continue
                grouped.setdefault((key, row["parent_id"]), []).append(row)
            made_progress = False
            for group in grouped.values():
                if len(group) < 2:
                    continue
                specific_types = sorted(
                    {r["type"] for r in group if r["type"] != "general"},
                    key=lambda t: type_priority.get(t, 9),
                )
                generals = [r for r in group if r["type"] == "general"]
                if specific_types:
                    subgroups = [
                        [r for r in group if r["type"] == t]
                        for t in specific_types
                    ]
                    # Split generals by whether they need normalization.
                    # Variant generals fold into the top specific-type
                    # subgroup so the merge resolves the imminent name
                    # collision at rename time; clean generals form their
                    # own subgroup so they collapse among themselves (a
                    # SQLite case-sensitive UNIQUE lets two clean rows
                    # like 'Robin' and 'robin' coexist under the same
                    # parent — they still ARE duplicates by NOCASE and
                    # should merge) but do not cross into the specific
                    # types.
                    #
                    # Species-bearing variant generals (legacy
                    # `type='general', is_species=1` rows on upgraded DBs)
                    # get their own further split: folding one into a
                    # non-taxonomy specific-type subgroup triggers
                    # _merge_keyword_into's `leaks_species_into_nontaxonomy`
                    # branch, which clears the species flag on the
                    # destination — silently dropping every photo already
                    # tagged with that legacy row out of species/life-list
                    # filters. Route them to the taxonomy subgroup when
                    # present; otherwise keep them in their own subgroup
                    # so the disambiguating rename below preserves their
                    # species identity (top-level parent_id IS NULL rows
                    # can coexist because SQLite treats NULL parents as
                    # distinct for UNIQUE(name, parent_id); non-NULL
                    # parents fall back to a `<clean> (id-<id>)` name).
                    variant_generals = [
                        r for r in generals
                        if normalize_keyword_display(r["name"]) != r["name"]
                    ]
                    species_variant_generals = [
                        r for r in variant_generals if r["is_species"] == 1
                    ]
                    plain_variant_generals = [
                        r for r in variant_generals if r["is_species"] != 1
                    ]
                    clean_generals = [
                        r for r in generals
                        if normalize_keyword_display(r["name"]) == r["name"]
                    ]
                    # Variant generals fold into a taxonomy peer when one
                    # exists — that mirrors add_keyword's runtime
                    # general→taxonomy auto-promotion (a variant `‘apapane`
                    # add would already promote to the taxonomy `apapane`
                    # row), and the merge is semantically safe: the
                    # destination is species-bearing, so `_merge_keyword_into`
                    # doesn't strip species metadata off the survivor's
                    # existing photos. Across non-taxonomy type boundaries
                    # (individual / genre / location) the same fold is a
                    # cross-type retag — a legacy `‘Robin` general would
                    # migrate every generic-Robin photo tag onto an
                    # unrelated individual `Robin`. In that case merge
                    # variants with any clean-general homonym (same slot,
                    # same tag intent) but keep the whole general group
                    # separate from the specific-typed peer. Split by
                    # is_species so a legacy `type='general', is_species=1`
                    # row does not collapse onto a plain general and take
                    # its is_species flag along.
                    if specific_types[0] == "taxonomy":
                        subgroups[0] += plain_variant_generals
                        if species_variant_generals:
                            subgroups[0] += species_variant_generals
                        # Partition clean_generals by is_species so a clean
                        # species-bearing general (`type='general',
                        # is_species=1` on an upgraded DB, kept intentionally
                        # distinct from a plain homonym) does not collapse
                        # onto a plain general and either strip its species
                        # flag or (via _merge_keyword_into's same-type
                        # is_species CASE) stamp is_species=1 onto the plain
                        # general and every photo tagged with it. Mirrors the
                        # split the non-taxonomy branch and the no-peer
                        # branch below run: each stays as its own subgroup so
                        # they only collapse among themselves. Species-bearing
                        # clean generals are NOT folded into the taxonomy peer
                        # either — treating a legacy `type='general',
                        # is_species=1` row as identical to a taxonomy peer
                        # would migrate every general-Robin photo tag onto the
                        # taxonomy Robin, losing the intentional distinction
                        # and any curation rows keyed to the general
                        # spelling.
                        if clean_generals:
                            clean_species = [
                                r for r in clean_generals
                                if r["is_species"] == 1
                            ]
                            clean_plain = [
                                r for r in clean_generals
                                if r["is_species"] != 1
                            ]
                            if clean_species:
                                subgroups.append(clean_species)
                            if clean_plain:
                                subgroups.append(clean_plain)
                    else:
                        combined_generals = (
                            plain_variant_generals
                            + species_variant_generals
                            + clean_generals
                        )
                        non_species_generals = [
                            r for r in combined_generals if r["is_species"] != 1
                        ]
                        species_generals = [
                            r for r in combined_generals if r["is_species"] == 1
                        ]
                        if non_species_generals:
                            subgroups.append(non_species_generals)
                        if species_generals:
                            subgroups.append(species_generals)
                else:
                    # No specific-type peer: the generals still can't be
                    # collapsed indiscriminately. A legacy species-bearing
                    # general (`type='general', is_species=1` on upgraded
                    # DBs) sharing a match key with a plain
                    # `type='general', is_species=0` homonym is not the same
                    # keyword — species queries `is_species = 1 OR
                    # type = 'taxonomy'` distinguish them — so merging
                    # them into one general survivor would either strip
                    # the species flag off the legacy row's photos or, via
                    # _merge_keyword_into's same-type is_species CASE,
                    # stamp is_species=1 onto every photo tagged with the
                    # plain general. Split by is_species so the two
                    # subgroups collapse only among themselves.
                    species_generals = [
                        r for r in generals if r["is_species"] == 1
                    ]
                    nonspecies_generals = [
                        r for r in generals if r["is_species"] != 1
                    ]
                    subgroups = []
                    if species_generals:
                        subgroups.append(species_generals)
                    if nonspecies_generals:
                        subgroups.append(nonspecies_generals)
                for members in subgroups:
                    if len(members) < 2:
                        continue
                    # Survivor: highest-priority type first (so the merged
                    # row keeps its deliberate type), then an already-clean
                    # spelling, then the earliest id.
                    ordered = sorted(
                        members,
                        key=lambda r: (
                            type_priority.get(r["type"], 9),
                            normalize_keyword_display(r["name"]) != r["name"],
                            r["id"],
                        ),
                    )
                    keep_id = ordered[0]["id"]
                    ids = [r["id"] for r in ordered]
                    # A prior merge in this pass can cascade-delete ids from
                    # later groups (children merging under merged parents).
                    placeholders = ",".join("?" * len(ids))
                    alive = {
                        r["id"] for r in self.conn.execute(
                            f"SELECT id FROM keywords WHERE id IN ({placeholders})",
                            ids,
                        )
                    }
                    if keep_id not in alive:
                        continue
                    for rid in ids:
                        if rid == keep_id or rid not in alive:
                            continue
                        merged += self._merge_keyword_into(rid, keep_id)
                        made_progress = True
            if not made_progress:
                break

        # Rewrite the surviving variant spellings (also retargets each
        # row's scoped pending changes and curation snapshots). The
        # ``disambiguate_on_conflict`` flag guarantees no stored variant
        # survives even when the clean slot is held by a different-type
        # peer: the leftover falls back to a ``<clean> (id-<id>)`` name,
        # which is still in normalize_keyword_display() form. Without
        # this, the marker below would advertise the "no stored variant"
        # invariant while a quoted spelling persisted, and a later clean
        # add for the same (name, parent) slot could surface as an
        # uncaught IntegrityError/500.
        renamed = 0
        disambiguated = []
        for row in self.conn.execute("SELECT id, name FROM keywords").fetchall():
            clean = normalize_keyword_display(row["name"])
            if not clean or clean == row["name"]:
                continue
            self._normalize_keyword_row_name(
                row["id"], disambiguate_on_conflict=True
            )
            after = self.conn.execute(
                "SELECT name FROM keywords WHERE id = ?", (row["id"],)
            ).fetchone()
            if not after or after["name"] == row["name"]:
                # No forward progress — shouldn't happen with
                # disambiguate_on_conflict=True, but guard against a silent
                # regression in the fallback branch.
                continue
            if after["name"] == clean:
                renamed += 1
            else:
                disambiguated.append((row["name"], after["name"]))
        if disambiguated:
            log.warning(
                "keyword normalization migration: disambiguated %d name(s) "
                "with an id suffix because a different-type keyword already "
                "uses the normalized form under the same parent: %s",
                len(disambiguated),
                ", ".join(
                    f"{old!r} -> {new!r}" for old, new in disambiguated[:10]
                ),
            )

        # Pending sidecar changes: the queued value is written verbatim to
        # XMP, so normalize globally (the scoped rewrites above only cover
        # values tied to a surviving keyword row's tags).
        pending_fixed = 0
        cancelled_pending_ids = set()
        for row in self.conn.execute(
            "SELECT id, photo_id, change_type, value, workspace_id "
            "FROM pending_changes "
            "WHERE change_type IN ('keyword_add', 'keyword_remove')"
        ).fetchall():
            if row["id"] in cancelled_pending_ids:
                continue
            clean = normalize_keyword_display(row["value"] or "")
            if clean == (row["value"] or ""):
                continue
            # If normalization would surface an opposite-type pending
            # change at the same (photo, workspace) with the same clean
            # value, cancel both — mirrors the add/remove cancellation
            # queue_keyword_add and queue_keyword_remove enforce at
            # runtime. Without this, an unsynced
            # keyword_add('‘Apapane') alongside a
            # keyword_remove('Apapane') for the same photo would both
            # survive as add+remove(Apapane), and sync_to_xmp treats a
            # same-value add+remove pair as a paired rename and writes
            # the removed spelling back into the sidecar.
            opposite = None
            if clean:
                opposite_type = (
                    "keyword_remove" if row["change_type"] == "keyword_add"
                    else "keyword_add"
                )
                opposite = self.conn.execute(
                    "SELECT id FROM pending_changes "
                    "WHERE photo_id = ? AND change_type = ? AND value = ? "
                    "AND COALESCE(workspace_id, -1) = COALESCE(?, -1) "
                    "AND id != ?",
                    (row["photo_id"], opposite_type, clean,
                     row["workspace_id"], row["id"]),
                ).fetchone()
            if opposite is not None:
                self.conn.execute(
                    "DELETE FROM pending_changes WHERE id IN (?, ?)",
                    (row["id"], opposite["id"]),
                )
                cancelled_pending_ids.add(opposite["id"])
                pending_fixed += 1
                continue
            dup = None
            if clean:
                dup = self.conn.execute(
                    "SELECT id FROM pending_changes "
                    "WHERE photo_id = ? AND change_type = ? AND value = ? "
                    "AND COALESCE(workspace_id, -1) = COALESCE(?, -1) "
                    "AND id != ?",
                    (row["photo_id"], row["change_type"], clean,
                     row["workspace_id"], row["id"]),
                ).fetchone()
            if clean and dup is None:
                self.conn.execute(
                    "UPDATE pending_changes SET value = ? WHERE id = ?",
                    (clean, row["id"]),
                )
            else:
                self.conn.execute(
                    "DELETE FROM pending_changes WHERE id = ?", (row["id"],)
                )
            pending_fixed += 1

        # Species curation snapshots key rows by name string and are
        # compared exact against keywords.name. Route leftovers through the
        # existing rename methods (which rebucket highlight ranks and drop
        # duplicates), then clear any old-spelling stragglers the rename
        # skipped as duplicates.
        curation_fixed = 0
        for table, rename in (
            ("photo_preferences", self.rename_photo_preferences_species),
            ("species_representatives",
             self.rename_species_representatives_species),
            ("species_highlights", self.rename_species_highlights_species),
        ):
            names = [
                r["species"] for r in self.conn.execute(
                    f"SELECT DISTINCT species FROM {table}"
                ).fetchall()
            ]
            for old in names:
                clean = normalize_keyword_display(old or "")
                if clean == (old or ""):
                    continue
                if clean:
                    curation_fixed += rename(old, clean, _commit=False) or 0
                self.conn.execute(
                    f"DELETE FROM {table} WHERE species = ?", (old,)
                )

        # Second curation pass: align case-only mismatches with the stored
        # keyword spelling (see _align_curation_species_case for the
        # homonym-ambiguity rules).
        curation_fixed += self._align_curation_species_case()

        history_curation_fixed = self._align_curation_history_species()

        if (
            dropped_empty or merged or renamed or pending_fixed
            or curation_fixed or history_curation_fixed
        ):
            log.info(
                "keyword normalization migration: dropped %d empty-name "
                "keyword(s), merged %d duplicate row(s), renamed %d, "
                "rewrote %d pending change(s), moved %d curation row(s), "
                "rewrote %d curation history item(s)",
                dropped_empty, merged, renamed, pending_fixed, curation_fixed,
                history_curation_fixed,
            )

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
        return self._keyword_repository().reparent_disambiguated(child, dst_id, new_name)

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
        merged = 1
        self.conn.execute(
            'UPDATE keyword_import_aliases SET keyword_id = ? WHERE keyword_id = ?',
            (dst_id, src_id),
        )
        src = self.conn.execute(
            "SELECT name, type, is_species, latitude, longitude, taxon_id, "
            "source_taxon_id, place_id FROM keywords WHERE id = ?",
            (src_id,),
        ).fetchone()
        dst = self.conn.execute(
            "SELECT name, type, is_species, place_id FROM keywords WHERE id = ?",
            (dst_id,),
        ).fetchone()
        # Transfer the source's Google ``place_id`` onto the destination when
        # the destination lacks one before the row is deleted below. Without
        # this, merging a coordless sibling on top of a place-bearing sibling
        # (or the reverse) silently drops the Google place link — repeat
        # startup repair of duplicate location roots would otherwise strip
        # the place ID from a repaired child (e.g. ``United States ->
        # California`` present as both a coordless branch and a place-bearing
        # branch). The partial ``UNIQUE(place_id) WHERE place_id IS NOT NULL``
        # index requires clearing the source first before moving the value.
        # When the transfer happens, force the destination's coordinates to
        # match the source's — the metadata fold below only fills coordless
        # rows via ``COALESCE(latitude, ?)``, so a destination that carried
        # unrelated stale coords would otherwise represent the incoming
        # Google place at the wrong point (saved-suggestion ranking and map
        # markers then use the stale location instead of the place's actual
        # coordinates).
        place_id_transferred = (
            src is not None
            and dst is not None
            and src["place_id"] is not None
            and dst["place_id"] is None
        )
        if place_id_transferred:
            self.conn.execute(
                "UPDATE keywords SET place_id = NULL WHERE id = ?",
                (src_id,),
            )
            self.conn.execute(
                "UPDATE keywords SET place_id = ?, latitude = ?, longitude = ? "
                "WHERE id = ?",
                (src["place_id"], src["latitude"], src["longitude"], dst_id),
            )
        if src is not None:
            # A species-bearing row being RETYPED into a non-taxonomy
            # destination must not leak its species flag or taxon link
            # onto the survivor: species queries `is_species = 1 OR
            # type = 'taxonomy'` would otherwise keep matching every
            # photo already tagged with that individual/general row
            # (see update_keyword's retype-into-peer path, and the
            # migration's general→specific-type fold). "Species-
            # bearing" is `type='taxonomy'` OR `is_species=1` — legacy
            # rows can still be `type='general', is_species=1` on
            # upgraded DBs, and retyping them into an individual/general
            # peer would otherwise take the else branch below and stamp
            # is_species=1 onto the non-taxonomy destination. Gated on
            # `src.type != dst.type` so same-type case-variant collapses
            # (e.g. two `general, is_species=1` rows merging under one
            # normalized spelling) still keep their metadata-fold
            # behavior.
            leaks_species_into_nontaxonomy = (
                dst is not None
                and dst["type"] != "taxonomy"
                and src["type"] != dst["type"]
                and (src["type"] == "taxonomy" or src["is_species"] == 1)
            )
            if leaks_species_into_nontaxonomy:
                # Retype-into-peer path (see update_keyword): the survivor
                # is deliberately non-taxonomy, so the row must not stay
                # matched by species queries. Suppressing the source's
                # is_species/taxon_id is not enough — the destination may
                # carry a legacy is_species=1 (dirty pre-invariant data on
                # 'individual'/'general' rows) or a stale taxon_id, and
                # keeping either lets `is_species = 1 OR type = 'taxonomy'`
                # keep matching every photo that already used the dst row.
                # Clear all species claims (taxon_id AND source_taxon_id)
                # alongside the metadata fold; a lingering iNat
                # source_taxon_id would keep the survivor resolving to a
                # species identity the retype was meant to drop.
                self.conn.execute(
                    """UPDATE keywords
                       SET is_species        = 0,
                           latitude          = COALESCE(latitude, ?),
                           longitude         = COALESCE(longitude, ?),
                           taxon_id          = NULL,
                           source_taxon_id   = NULL
                       WHERE id = ?""",
                    (src["latitude"], src["longitude"], dst_id),
                )
            else:
                # Fold ``source_taxon_id`` alongside ``taxon_id``: a
                # source row can carry an iNat id without a resolved local
                # taxon (see ``_add_source_species_keyword``), and
                # ``keywords_claim_different_taxa`` treats a bare
                # ``source_taxon_id`` as identity. Without this COALESCE
                # the recursive child collapse would drop the only
                # external taxon claim and leave the survivor an unlinked
                # species row.
                self.conn.execute(
                    """UPDATE keywords
                       SET is_species        = CASE WHEN ? = 1 THEN 1 ELSE is_species END,
                           latitude          = COALESCE(latitude, ?),
                           longitude         = COALESCE(longitude, ?),
                           taxon_id          = COALESCE(taxon_id, ?),
                           source_taxon_id   = COALESCE(source_taxon_id, ?)
                       WHERE id = ?""",
                    (src["is_species"], src["latitude"], src["longitude"],
                     src["taxon_id"], src["source_taxon_id"], dst_id),
                )
        # Retarget pending keyword_add/keyword_remove rows queued under the
        # source name onto the destination name. A pending row that would
        # collide with an existing (photo_id, change_type, dst_name) row is
        # dropped rather than duplicated — matches the dedupe contract
        # queue_change enforces. Scope the rewrite to photos actually tagged
        # with either row: a value-only rewrite would otherwise affect every
        # workspace whose pending_changes carry the same name string for a
        # keyword row that was not merged. Captured before the
        # photo_keywords UPDATE below so the query still sees the src tags.
        if src is not None and dst is not None:
            src_name = src["name"]
            dst_name = dst["name"]
            if src_name and dst_name and src_name != dst_name:
                affected_pcx = [
                    r["photo_id"] for r in self.conn.execute(
                        "SELECT DISTINCT photo_id FROM photo_keywords WHERE keyword_id IN (?, ?)",
                        (src_id, src_id if pending_source_only else dst_id),
                    ).fetchall()
                ]
                for chunk in _chunks(affected_pcx):
                    placeholders = ",".join("?" for _ in chunk)
                    self.conn.execute(
                        f"""DELETE FROM pending_changes
                            WHERE change_type IN ('keyword_add', 'keyword_remove')
                              AND value = ?
                              AND photo_id IN ({placeholders})
                              AND EXISTS (
                                  SELECT 1 FROM pending_changes pc2
                                  WHERE pc2.photo_id = pending_changes.photo_id
                                    AND pc2.change_type = pending_changes.change_type
                                    AND pc2.value = ?
                                    AND COALESCE(pc2.workspace_id, -1)
                                        = COALESCE(pending_changes.workspace_id, -1)
                              )""",
                        [src_name, *chunk, dst_name],
                    )
                    self.conn.execute(
                        f"""UPDATE pending_changes
                            SET value = ?
                            WHERE change_type IN ('keyword_add', 'keyword_remove')
                              AND value = ?
                              AND photo_id IN ({placeholders})""",
                        [dst_name, src_name, *chunk],
                    )
                # Retarget species curation rows keyed to the deleted source
                # name onto the surviving destination name when either row is
                # a species/taxonomy keyword. The eligible curation queries
                # compare those strings exact against the surviving
                # keywords.name, so highlights/representatives keyed to the
                # source spelling would silently disappear after a merge even
                # though the tag itself was retained. Mirrors the scoped
                # rename _normalize_keyword_row_name runs on the survivor;
                # scoped to (photo, workspace) pairs that carried either row
                # so an unrelated workspace's same-species curation is not
                # retargeted onto a name it doesn't have tagged.
                is_species_merge = (
                    src["is_species"] == 1 or src["type"] == "taxonomy"
                    or dst["is_species"] == 1 or dst["type"] == "taxonomy"
                )
                if is_species_merge:
                    tag_rows = self.conn.execute(
                        """SELECT DISTINCT pk.photo_id, wf.workspace_id
                           FROM photo_keywords pk
                           JOIN photos p ON p.id = pk.photo_id
                           JOIN workspace_folders wf ON wf.folder_id = p.folder_id
                           WHERE pk.keyword_id IN (?, ?)""",
                        (src_id, dst_id),
                    ).fetchall()
                    photo_workspace_pairs = [
                        (r["photo_id"], r["workspace_id"]) for r in tag_rows
                    ]
                    if photo_workspace_pairs:
                        self.rename_species_highlights_species(
                            src_name, dst_name,
                            photo_workspace_pairs=photo_workspace_pairs,
                            _commit=False,
                        )
                        self.rename_photo_preferences_species(
                            src_name, dst_name,
                            photo_workspace_pairs=photo_workspace_pairs,
                            _commit=False,
                        )
        # Retarget edit_history entries that reference src_id as a
        # keyword id so undo/redo lands on the survivor instead of a
        # deleted row. Without this, undo of a recent keyword_add /
        # keyword_remove / prediction_accept / species_replace looks up
        # src_id, gets no keyword row (it's about to be deleted below),
        # and marks the entry undone without reversing the effect: the
        # tag stays on the photo and the pending sidecar change (already
        # rewritten to the survivor spelling above) is left in place.
        # Applies globally across workspaces — workspace_id scopes WHO
        # ran the edit, not which keyword row it references.
        _kw_id_actions = (
            'keyword_add', 'keyword_remove', 'prediction_accept',
            'species_replace',
        )
        src_str = str(src_id)
        dst_str = str(dst_id)
        _kw_placeholders = ",".join("?" * len(_kw_id_actions))
        # Pre-existing survivor tags: for an edit recorded against src_id,
        # an item whose photo already carried dst_id at merge time can't
        # be retargeted honestly — the UPDATE OR IGNORE on photo_keywords
        # below leaves the survivor row untouched and drops the src row,
        # so an undo/redo of the retargeted entry would touch the user's
        # pre-existing survivor tag that was never part of that edit.
        # Drop those items before retargeting so undo/redo iterates 0 (or
        # the still-legitimate) items only. Covers three action types:
        #   * `keyword_add`: undo calls untag_photo(pid, entry.new_value)
        #     per item; the retargeted entry.new_value = dst_id would
        #     remove the survivor.
        #   * `prediction_accept`: undo uses item.new_value for the tag.
        #     Retire only that tag mutation by converting the item to
        #     ``no_tag``; its prediction-status history remains undoable.
        #   * `keyword_remove`: undo tags on the survivor (INSERT OR
        #     IGNORE — no-op if dst pre-existed), BUT redo calls
        #     untag_photo(pid, entry.new_value); the retargeted
        #     entry.new_value = dst_id would strip the survivor on redo.
        #   * `species_replace`: undo calls untag_photo(pid,
        #     item.new_value) before restoring the old species (see
        #     `_apply_undo`); the retargeted item.new_value = dst_id would
        #     remove the survivor tag the edit never actually created.
        #     Redo similarly untags item.new_value again. Symmetric case
        #     on the OLD side: for a prior replace where src was the OLD
        #     species being swapped out, redo iterates
        #     old_kids (bare-string or JSON `keyword_ids`) and untags each
        #     — a src→dst retarget of those references would strip the
        #     pre-existing survivor. Drop those items too (bare-string in
        #     the second DELETE below, JSON in the payload rewrite pass).
        def _retire_tag_mutations(rows):
            # A prediction accept has two effects: the tag and review status.
            # When a merge makes its tag redundant, retain the status effect
            # and metadata so undo/redo still restores every prediction.
            for row in rows:
                if row["action_type"] != "prediction_accept":
                    self.conn.execute("DELETE FROM edit_history_items WHERE id = ?", (row["id"],))
                    continue
                try:
                    meta = json.loads(row["old_value"] or "{}")
                except (TypeError, ValueError):
                    meta = {}
                if not isinstance(meta, dict):
                    meta = {}
                meta["prediction_ids"] = self._edit_prediction_ids(meta, row["old_value"])
                meta["no_tag"] = True
                self.conn.execute(
                    "UPDATE edit_history_items SET old_value = ? WHERE id = ?",
                    (json.dumps(meta), row["id"]),
                )

        preexisting_dst_photos = [
            r["photo_id"] for r in self.conn.execute(
                "SELECT photo_id FROM photo_keywords WHERE keyword_id = ?",
                (dst_id,),
            ).fetchall()
        ]
        for chunk in _chunks(preexisting_dst_photos):
            ph = ",".join("?" for _ in chunk)
            # keyword_add + prediction_accept + species_replace:
            # item.new_value = str(kid). Deleting a species_replace item
            # here loses the retag-old-species side of that per-photo swap
            # on undo/redo, but leaving it retargeted would silently
            # untag the user's pre-existing survivor. Prediction accepts
            # instead keep a status-only record.
            # Identity is per item: for a mixed-alias prediction_accept
            # batch (see api_accept_predictions), the parent edit's
            # ``new_value`` records only the first alias, while each item's
            # ``new_value`` records its own resolved keyword id. Requiring
            # the parent to also equal ``src`` would miss items in that
            # batch whose alias is the one being merged, and the survivor
            # retarget below would then silently untag a pre-existing
            # ``dst`` tag on undo. For ``keyword_add`` and
            # ``species_replace`` the parent and item always agree, so
            # dropping the parent match only widens coverage where it was
            # under-matching before.
            # Status-only accepts must retain their prediction undo record,
            # and do not count as earlier/later tag additions in these checks.
            _retire_tag_mutations(self.conn.execute(
                f"""SELECT id, old_value,
                           (SELECT action_type FROM edit_history
                            WHERE id = edit_history_items.edit_id) AS action_type
                    FROM edit_history_items
                    WHERE new_value = ?
                      AND photo_id IN ({ph})
                      AND edit_id IN (
                          SELECT id FROM edit_history
                          WHERE action_type IN (
                              'keyword_add', 'species_replace'
                          ) OR (
                              action_type = 'prediction_accept'
                              AND COALESCE(edit_history_items.old_value, '') NOT LIKE '%"no_tag"%'
                          )
                      )
                      AND NOT EXISTS (
                          SELECT 1
                          FROM edit_history_items ehi2
                          JOIN edit_history eh2
                            ON eh2.id = ehi2.edit_id
                          WHERE ehi2.photo_id = edit_history_items.photo_id
                            AND ehi2.new_value IN (?, ?)
                            AND eh2.action_type IN (
                                'keyword_add',
                                'prediction_accept',
                                'species_replace'
                            )
                            AND (eh2.action_type != 'prediction_accept'
                                 OR COALESCE(ehi2.old_value, '') NOT LIKE '%"no_tag"%')
                            AND ehi2.id > edit_history_items.id
                      )""",
                [src_str, *chunk, src_str, dst_str],
            ).fetchall())
            # When the source add happened first and a later add created
            # the current survivor association, the later add becomes the
            # redundant operation after src and dst converge. The guarded
            # cleanup above deliberately preserves the earlier source item;
            # retire the later tag mutation instead so latest-first undo leaves
            # the merged tag in place until the original source add is
            # itself undone. Restrict this to add-like actions whose whole
            # per-photo effect is the tag association; species_replace has
            # an old-species restoration side that cannot be discarded.
            # The earlier-source lookup matches on the item's own
            # ``new_value`` alone, not the parent edit's, so a mixed-alias
            # prediction_accept batch (whose parent records only the first
            # alias) still counts as the earlier source add for a later
            # redundant item.
            _retire_tag_mutations(self.conn.execute(
                f"""SELECT id, old_value,
                           (SELECT action_type FROM edit_history
                            WHERE id = edit_history_items.edit_id) AS action_type
                    FROM edit_history_items
                    WHERE photo_id IN ({ph})
                      AND new_value IN (?, ?)
                      AND edit_id IN (
                          SELECT id FROM edit_history
                          WHERE action_type = 'keyword_add' OR (
                              action_type = 'prediction_accept'
                              AND COALESCE(edit_history_items.old_value, '') NOT LIKE '%"no_tag"%'
                          )
                      )
                      AND EXISTS (
                          SELECT 1
                          FROM edit_history_items ehi1
                          JOIN edit_history eh1
                            ON eh1.id = ehi1.edit_id
                          WHERE ehi1.photo_id = edit_history_items.photo_id
                            AND ehi1.new_value = ?
                            AND eh1.action_type IN (
                                'keyword_add', 'prediction_accept'
                            )
                            AND (eh1.action_type != 'prediction_accept'
                                 OR COALESCE(ehi1.old_value, '') NOT LIKE '%"no_tag"%')
                            AND ehi1.id < edit_history_items.id
                      )""",
                [*chunk, src_str, dst_str, src_str],
            ).fetchall())
            # keyword_remove: item.new_value is '' by convention (see
            # record_edit call sites in app.py); the keyword id lives in
            # item.old_value. Drop the item ONLY when the survivor
            # genuinely pre-existed THIS remove — i.e., no later edit
            # added the merged keyword back to the same photo. If dst
            # was tagged AFTER this remove, the current photo_keywords
            # row does not prove pre-existence and dropping the item
            # breaks undo: latest-first undo of the later add first
            # strips dst_id, and this remove's undo would then no-op
            # (no item), leaving the merged keyword missing when the
            # earlier remove is reversed. Keeping the item is safe in
            # that case:
            #   * undo of remove → tag_photo(pid, dst) is INSERT OR
            #     IGNORE and a no-op if dst is already present;
            #   * redo of remove → untag_photo(pid, dst) is consistent
            #     with replaying the historical remove of what became
            #     the merged keyword.
            # "Later add" covers keyword_add / prediction_accept and
            # the tagging half of species_replace (item.new_value =
            # str(kid)). Src-spelled adds count too — pre-migration
            # they refer to what will become the merged keyword.
            self.conn.execute(
                f"""DELETE FROM edit_history_items
                    WHERE old_value = ?
                      AND photo_id IN ({ph})
                      AND edit_id IN (
                          SELECT id FROM edit_history
                          WHERE new_value = ?
                            AND action_type = 'keyword_remove'
                      )
                      AND NOT EXISTS (
                          SELECT 1
                          FROM edit_history_items ehi2
                          JOIN edit_history eh2
                            ON eh2.id = ehi2.edit_id
                          WHERE ehi2.photo_id = edit_history_items.photo_id
                            AND ehi2.new_value IN (?, ?)
                            AND eh2.action_type IN (
                                'keyword_add',
                                'prediction_accept',
                                'species_replace'
                            )
                            AND (eh2.action_type != 'prediction_accept'
                                 OR COALESCE(ehi2.old_value, '') NOT LIKE '%"no_tag"%')
                            AND ehi2.id > edit_history_items.id
                      )""",
                [src_str, *chunk, src_str, src_str, dst_str],
            )
            # species_replace: item.old_value = str(old_kid) (bare-string
            # form) for a prior replace where src_id was the OLD species
            # being swapped out. The bare-string retarget below would
            # rewrite that to dst_str; _apply_redo then iterates
            # old_kids=[dst_id] and untag_photo(pid, dst_id), stripping
            # the survivor tag that pre-existed the merge and was never
            # created by that edit. Drop the item — same tradeoff as the
            # new_value / species_replace case above.
            self.conn.execute(
                f"""DELETE FROM edit_history_items
                    WHERE old_value = ?
                      AND photo_id IN ({ph})
                      AND edit_id IN (
                          SELECT id FROM edit_history
                          WHERE action_type = 'species_replace'
                      )""",
                [src_str, *chunk],
            )
        # 1) edit_history.new_value: the canonical keyword id per entry.
        self.conn.execute(
            f"""UPDATE edit_history
                SET new_value = ?
                WHERE new_value = ?
                  AND action_type IN ({_kw_placeholders})""",
            (dst_str, src_str, *_kw_id_actions),
        )
        # 2) edit_history_items new_value / old_value: bare keyword-id
        #    strings, but only the specific (action_type, column) pairs
        #    that actually store keyword ids. record_edit populates:
        #      keyword_add       → new_value=str(kid), old_value=''
        #      keyword_remove    → old_value=str(kid), new_value=''
        #      species_replace   → old_value=str(old_kid), new_value=str(kid)
        #      prediction_accept → old_value=str(prediction_id),
        #                          new_value=str(kid)
        #    prediction_accept.old_value is the prediction id, NOT a
        #    keyword id (see api_accept_prediction and _edit_prediction_id
        #    which falls back to the bare string). A blanket rewrite over
        #    every column would corrupt any prediction id whose numeric
        #    value happens to equal src_id — undo/redo would then act on
        #    the wrong prediction. Restrict each rewrite to the action
        #    types whose column contains a keyword id.
        _kw_id_by_col = {
            "new_value": (
                "keyword_add", "species_replace", "prediction_accept",
            ),
            "old_value": ("keyword_remove", "species_replace"),
        }
        for col, actions in _kw_id_by_col.items():
            col_placeholders = ",".join("?" * len(actions))
            self.conn.execute(
                f"""UPDATE edit_history_items
                    SET {col} = ?
                    WHERE {col} = ?
                      AND edit_id IN (
                          SELECT id FROM edit_history
                          WHERE action_type IN ({col_placeholders})
                      )""",
                (dst_str, src_str, *actions),
            )
        # 3) edit_history_items.old_value JSON payloads: species_replace
        #    and metadata-carrying keyword_add/prediction_accept entries
        #    store {"keyword_id": ..., "keyword_ids": [...], ...}. Load,
        #    rewrite, re-serialize per row. Scoped to values that look
        #    like JSON so bare id strings (already handled above) are
        #    skipped cheaply. Uses ? for the LIKE prefix to keep the
        #    format string free of literal SQL wildcard characters.
        #    For species_replace items whose photo already carried the
        #    survivor before the merge, a src→dst rewrite of the JSON
        #    old_kids would make _apply_redo untag the pre-existing
        #    survivor (see the bare-string DELETE above); drop those
        #    items instead of retargeting them.
        preexisting_set = set(preexisting_dst_photos)
        json_rows = self.conn.execute(
            f"""SELECT ehi.id, ehi.photo_id, ehi.old_value, eh.action_type
                FROM edit_history_items ehi
                JOIN edit_history eh ON eh.id = ehi.edit_id
                WHERE eh.action_type IN ({_kw_placeholders})
                  AND ehi.old_value IS NOT NULL
                  AND ehi.old_value LIKE ?""",
            (*_kw_id_actions, '{%'),
        ).fetchall()
        for row in json_rows:
            try:
                data = json.loads(row["old_value"])
            except (TypeError, ValueError):
                continue
            if not isinstance(data, dict):
                continue
            references_src = False
            raw_kid = data.get("keyword_id")
            if raw_kid is not None:
                try:
                    if int(raw_kid) == src_id:
                        references_src = True
                except (TypeError, ValueError):
                    pass
            if not references_src:
                for k in (data.get("keyword_ids") or []):
                    try:
                        if int(k) == src_id:
                            references_src = True
                            break
                    except (TypeError, ValueError):
                        continue
            if (
                references_src
                and row["action_type"] == "species_replace"
                and row["photo_id"] in preexisting_set
            ):
                self.conn.execute(
                    "DELETE FROM edit_history_items WHERE id = ?",
                    (row["id"],),
                )
                continue
            dirty = False
            if raw_kid is not None:
                try:
                    if int(raw_kid) == src_id:
                        data["keyword_id"] = dst_id
                        dirty = True
                except (TypeError, ValueError):
                    pass
            raw_kids = data.get("keyword_ids")
            if isinstance(raw_kids, list) and raw_kids:
                rewritten = []
                changed = False
                for k in raw_kids:
                    try:
                        k_int = int(k)
                    except (TypeError, ValueError):
                        rewritten.append(k)
                        continue
                    if k_int == src_id:
                        k_int = dst_id
                        changed = True
                    rewritten.append(k_int)
                if changed:
                    # Dedup preserving order: if the destination id was
                    # already in the list, don't repeat it after rewrite.
                    seen = []
                    for k in rewritten:
                        if k not in seen:
                            seen.append(k)
                    data["keyword_ids"] = seen
                    dirty = True
            if dirty:
                self.conn.execute(
                    "UPDATE edit_history_items SET old_value = ? WHERE id = ?",
                    (json.dumps(data, sort_keys=True), row["id"]),
                )
        # Preserve durable authorship before collapsing association conflicts.
        # UPDATE OR IGNORE below leaves the destination row untouched when a
        # photo already carries both keywords; without this fold, deleting the
        # source would also delete its only ``source='manual'`` stamp. The
        # fold runs in both directions — a weaker destination is raised to the
        # source's claim, and a stronger destination keeps its own.
        src_source_sql = (
            "(SELECT src_pk.source FROM photo_keywords src_pk "
            "WHERE src_pk.photo_id = dst_pk.photo_id "
            "AND src_pk.keyword_id = :src_id)"
        )
        self.conn.execute(
            f"""UPDATE photo_keywords AS dst_pk
               SET source = {keyword_source_max_sql(
                   "dst_pk.source", src_source_sql,
               )}
               WHERE dst_pk.keyword_id = :dst_id
                 AND EXISTS (
                     SELECT 1 FROM photo_keywords src_pk
                     WHERE src_pk.photo_id = dst_pk.photo_id
                       AND src_pk.keyword_id = :src_id
                 )""",
            {"dst_id": dst_id, "src_id": src_id},
        )
        # Move photo associations (ignore if already exists for dst_id),
        # then drop the leftovers. Non-conflicting rows carry their source
        # column through the UPDATE unchanged.
        self.conn.execute(
            "UPDATE OR IGNORE photo_keywords SET keyword_id = ? WHERE keyword_id = ?",
            (dst_id, src_id),
        )
        self.conn.execute("DELETE FROM photo_keywords WHERE keyword_id = ?", (src_id,))
        # Reparent children onto the destination before deleting, or the
        # keywords.parent_id FK aborts the merge mid-way.
        children = self.conn.execute(
            "SELECT id, name, type, place_id, taxon_id, source_taxon_id, is_species "
            "FROM keywords WHERE parent_id = ?",
            (src_id,),
        ).fetchall()
        for child in children:
            # Detect the collision explicitly: SQLite's UNIQUE(name, parent_id)
            # is BINARY, so `foo` reparenting under a destination that already
            # holds `Foo` would UPDATE cleanly and leave two semantic peers no
            # keyword lookup (all folded through ``keyword_match_key``) could
            # tell apart. Fold every sibling's name to check for either shape
            # of collision, and only reparent when the folded slot is free.
            siblings = self.conn.execute(
                "SELECT id, name, type, place_id, taxon_id, source_taxon_id, is_species "
                "FROM keywords WHERE parent_id = ? AND id != ?",
                (dst_id, child["id"]),
            ).fetchall()
            child_key = keyword_match_key(child["name"])
            existing = next(
                (s for s in siblings if keyword_match_key(s["name"]) == child_key),
                None,
            )
            if existing is None:
                self.conn.execute(
                    "UPDATE keywords SET parent_id = ? WHERE id = ?",
                    (dst_id, child["id"]),
                )
            else:
                # Every disambiguation below has to dodge the whole sibling
                # set, not just the row it collided with: the suffixed name
                # can itself be occupied (a user typed it, or an earlier
                # disambiguation produced it), and a second
                # UNIQUE(name, parent_id) violation here is uncaught.
                taken = {row["name"] for row in siblings}
                if keywords_claim_different_taxa(self, existing, child):
                    # Two same-named species rows that resolve to DIFFERENT
                    # taxa. A recursive merge keeps the destination's taxon
                    # claim (COALESCE folds only fill missing fields), so
                    # every photo under the migrating row would silently
                    # come out tagged as the other species. Same reasoning
                    # as the distinct place case below; keep both rows
                    # instead.
                    self._reparent_disambiguated(
                        child, dst_id, free_sibling_name(
                            taken, child["name"], f"id-{child['id']}"),
                    )
                elif (
                    existing["type"] == "location"
                    and child["type"] == "location"
                    and existing["place_id"] is not None
                    and child["place_id"] is not None
                    and existing["place_id"] != child["place_id"]
                ):
                    # Two location siblings sharing (name, parent_id) but
                    # pointing at distinct Google places (e.g. two direct
                    # ``United States -> Springfield`` rows created from
                    # different place IDs). A recursive merge here would
                    # delete the migrating row and silently retag its
                    # photos onto a sibling that represents a different
                    # Google place. Disambiguate the migrating child with a
                    # place-id suffix so both Google places survive.
                    self._reparent_disambiguated(
                        child, dst_id, free_sibling_name(
                            taken, child["name"], child["place_id"][-8:]),
                    )
                elif existing["type"] == child["type"]:
                    merged += self._merge_keyword_into(
                        child["id"], existing["id"], pending_source_only=pending_source_only,
                    )
                else:
                    # Same name + parent but different type: outside the
                    # (LOWER(name), parent_id, type) dedup boundary, so
                    # preserve both by renaming the migrating child rather
                    # than retagging photos across types.
                    self._reparent_disambiguated(
                        child, dst_id, free_sibling_name(
                            taken, child["name"], f"id-{child['id']}"),
                    )
        self.conn.execute("DELETE FROM keywords WHERE id = ?", (src_id,))
        return merged

    def get_keyword_tree(self):
        """Return keywords used by photos in the active workspace, plus ancestors."""
        return self._keyword_repository().get_tree()

    def tag_photo(
        self, photo_id, keyword_id, source=KEYWORD_SOURCE_MANUAL, _commit=True,
    ):
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
            _commit: If False, skip the internal commit (caller is responsible
                     for committing the transaction).
        """
        self.conn.execute(
            "INSERT INTO photo_keywords (photo_id, keyword_id, source) "
            "VALUES (?, ?, ?) " + KEYWORD_SOURCE_CONFLICT_SQL,
            (photo_id, keyword_id, source),
        )
        if _commit:
            self.conn.commit()

    def _meta_repository(self):
        """Build the (catalog-wide) db_meta repository on this connection."""
        from repositories.meta import MetaRepository

        return MetaRepository(self.conn)

    def get_meta(self, key):
        """Return the db_meta value for `key`, or None if unset."""
        return self._meta_repository().get(key)

    def set_meta(self, key, value, _commit=True):
        """Upsert a db_meta row."""
        self._meta_repository().set(key, value, _commit=_commit)

    def untag_photo(self, photo_id, keyword_id, _commit=True):
        """Remove a keyword association from a photo.

        Args:
            _commit: If False, skip the internal commit (caller is responsible
                     for committing the transaction).
        """
        return self._keyword_repository().untag(photo_id, keyword_id, _commit=_commit)

    def get_photo_keywords(self, photo_id):
        """Return all keywords for a photo."""
        return self._keyword_repository().get_for_photo(photo_id)

    def get_keywords_for_photos(self, photo_ids):
        """Return keywords for a batch of photos keyed by photo id."""
        return self._keyword_repository().get_for_photos(photo_ids)

    def get_species_keywords_for_photos(self, photo_ids, include_identities=False):
        """Return deduplicated species-rank keyword names for photos.

        Returns a dict mapping photo_id -> list of species name strings.
        With include_identities, each entry contains the stored name and a
        source-aware identity key for comparisons that must preserve homonyms.

        A linked taxon must actually have rank ``species``; linked family,
        genus, and other ancestor keywords remain taxonomy keywords but are
        not presented as species. Taxonomy rows without a resolvable taxon
        retain the legacy behavior so user-created/offline species tags do
        not disappear.

        Multiple keyword nodes can represent the same taxon (for example a
        Lightroom hierarchy leaf plus an older top-level confirmation row).
        Collapse those by taxon_id and canonicalize to the same-taxon root's
        stored spelling when one exists — species_representatives,
        species_highlights, and life-list preference rows key on that root
        spelling, so a photo whose only surviving species tag is a hierarchy
        leaf ("verdin" after repair detached the redundant "Verdin" root)
        would otherwise miss those curation lookups. Falls back to the
        row's own name when no root exists, and taxonomy-less legacy rows
        continue to use the normalized keyword name.
        """
        return self._keyword_repository().get_species_for_photos(
            photo_ids, include_identities=include_identities,
        )

    def get_photos_with_equivalent_species(
        self, photo_ids, keyword_id, exclude_keyword_ids=None,
    ):
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
        return self._keyword_repository().get_photos_with_equivalent_species(
            photo_ids, keyword_id, exclude_keyword_ids=exclude_keyword_ids,
        )

    def get_highlights_candidates(self, folder_id, min_quality=0.0, photo_id=None):
        """Return photos eligible for highlights selection.

        When ``folder_id`` is an int, returns photos in that folder and its
        descendant folders. When ``folder_id`` is ``None``, returns photos
        across every folder visible in the active workspace. When
        ``photo_id`` is set, the result is additionally restricted to that
        single photo so the photo-detail endpoint can compute its highlight
        eligibility without rebuilding every workspace bucket.

        Each row carries:
          * ``species`` — accepted species keyword (NULL if none accepted)
          * ``prediction_id`` / ``predicted_species`` /
            ``predicted_confidence`` — top-confidence non-rejected prediction
            across the photo's detections (NULL if no usable prediction
            exists)

        Photos with ``quality_score >= min_quality`` that are not
        user-rejected are returned. When ``min_quality <= 0`` (the default),
        photos with no ``quality_score`` yet (not analyzed) are also included
        so picks and other unscored photos still surface on the Highlights
        page; raising the quality floor above 0 excludes them, since they have
        no measured quality to compare. The API layer applies the final
        highlights ranking because it combines these persisted quality fields
        with prediction confidence and user ratings.
        """
        return self._species_curation_repository().get_highlights_candidates(
            folder_id, min_quality=min_quality, photo_id=photo_id,
        )

    def get_life_list_candidates(self, species=None):
        """Return (photo x accepted-identification-keyword) life-list rows.

        Every non-rejected photo in a workspace-visible folder carrying an
        accepted identification keyword (``is_species = 1`` or
        ``type = 'taxonomy'``) produces one row per keyword. Taxonomy names
        and ranks ride along from ``taxa`` when the keyword is linked. Linked
        higher-rank identifications are included so the Life List can show and
        filter genus-, family-, and other non-species-level observations; the
        Explorer continues to count only species-rank taxa through
        :meth:`get_life_list_taxon_ids`.

        A linked higher-rank taxonomy keyword is suppressed when the same
        photo carries another linked taxonomy keyword whose taxon is a
        strict descendant of it, so a species-tagged robin also carrying
        Lightroom-imported ``includeParents`` ancestors (``Turdus`` /
        ``Turdidae`` / ``Aves``) or classifier-added broader labels does
        not inflate every ancestor rank's Life List bucket. See
        :data:`_LIFE_LIST_ANCESTOR_SUPPRESSION_CLAUSE` for the shared
        SQL fragment.

        Unlike :meth:`get_highlights_candidates`, photos without a
        ``quality_score`` are included — a species the user confirmed but
        never ran through the pipeline still belongs on the life list. The
        API layer ranks each species' photos with the highlights scorer,
        which falls back gracefully when metric columns are NULL.

        When ``species`` is provided, return the bucket for that species.
        A photo's surviving hierarchy leaf can have a different stored
        spelling from the canonical root keyword (``verdin`` vs
        ``Verdin``) after ``repair_duplicate_photo_species`` detaches the
        redundant root row, but curation stays keyed on the root spelling
        — so the filter also accepts any keyword whose ``taxon_id`` links
        back to a root species keyword named ``species``. Otherwise
        ``/api/life-list/species?species=Verdin`` would 404 for photos
        whose only remaining tag is the hierarchy leaf.
        """
        return self._species_curation_repository().get_life_list_candidates(species=species)

    def get_explorer_root(self, name="Aves", rank="class"):
        """Return {id,name,common_name,rank} for the default explorer root class,
        or None when the reference taxonomy has not been downloaded."""
        return self._species_curation_repository().get_explorer_root(name=name, rank=rank)

    def get_life_list_taxon_ids(self):
        """Distinct species-rank taxa ids of workspace-scoped tagged species (same
        eligibility as get_life_list_candidates). Excludes species keywords with no
        taxon_id AND taxonomy tags that resolve to a taxon above species rank
        (genus, family, etc.). Higher-rank matches are surfaced via
        get_life_list_unmatched_species so the explorer's found/total math stays
        at species rank and non-species tags aren't silently undercounted."""
        return self._species_curation_repository().get_life_list_taxon_ids()

    def get_life_list_uncounted_identifications(self):
        """Workspace identifications that cannot contribute a species count.

        Return dictionaries with the stored label, linked rank/taxon (when
        available), class ancestor, reason, and affected-photo count. The same
        per-photo ancestor suppression used by the Life List itself is applied
        here: an imported family/order keyword is not reported when that photo
        also carries a descendant identification. If at least one photo carries
        only the broader label, that label remains in the result with a count of
        the photos for which it is genuinely the most specific identification.
        """
        return self._species_curation_repository().get_life_list_uncounted_identifications()

    def get_life_list_unmatched_species(self):
        """Backward-compatible list of uncounted identification names."""
        return [
            row["name"]
            for row in self.get_life_list_uncounted_identifications()
        ]

    def get_taxon_subtree(self, root_id, max_depth=12):
        """All taxa in the subtree rooted at root_id (inclusive), as dict rows
        with id, name, common_name, rank, parent_id. Uses the parent_id index."""
        return self._species_curation_repository().get_taxon_subtree(root_id, max_depth=max_depth)

    def get_classes_for_taxa(self, taxon_ids):
        """Distinct class-rank ancestors of the given taxa, for the explorer's
        class selector. Returns [{id,name,common_name}] ordered by name.

        Callers pass the full life-list `found` set, which can exceed SQLite's
        bound-parameter limit on large life lists — chunk the seed IDs and
        merge the distinct classes across chunks so the endpoint doesn't 500.
        """
        return self._species_curation_repository().get_classes_for_taxa(taxon_ids)

    def get_class_ancestors_for_taxa(self, taxon_ids):
        """Map each taxon id to its class-rank ancestor.

        Life List entries can be linked at any major rank, so preserve the
        starting taxon id while walking toward the root.  The depth cap mirrors
        :meth:`get_taxon_subtree` and also prevents malformed cyclic taxonomy
        data from making the recursive query run forever.
        """
        return self._species_curation_repository().get_class_ancestors(taxon_ids)

    def get_life_list_best_photo_by_taxon(self, taxon_ids):
        """Map taxon_id -> {id, filename} of a representative (highest quality_score,
        newest) workspace-scoped photo for that species. Missing taxa are absent."""
        return self._species_curation_repository().get_life_list_best_photo_by_taxon(taxon_ids)

    def get_taxon_by_id(self, taxon_id):
        """Thin getter for a single taxon row (for non-default explorer roots)."""
        return self._species_curation_repository().get_taxon_by_id(taxon_id)

    def get_photo_life_list_species(self, photo_id):
        """Return this photo's lifelist-eligible identification names in the
        active workspace, ordered by name.

        Same eligibility rule as :meth:`get_life_list_candidates`: an accepted
        identification keyword (``is_species = 1`` or ``type = 'taxonomy'``)
        on a non-rejected photo in a workspace-visible folder. Returns ``[]``
        when the photo carries no such identification (or is rejected /
        outside the workspace), which is exactly when no "Set Representative"
        affordance should appear. Linked higher-rank taxonomy identifications
        (genus, family, class, …) are included for the same reason they are
        in :meth:`get_life_list_candidates` — so a photo tagged only with a
        higher-rank identification exposes the shared representative row and
        can complete ``POST /api/photo-preferences`` for the entry it
        actually appears under on the Life List.

        Ancestor suppression mirrors :meth:`get_life_list_candidates`: a
        linked higher-rank taxonomy keyword on the photo is hidden when
        the photo also carries another linked taxonomy keyword whose
        taxon is a strict descendant of it, so the shared Set
        Representative row surfaces only under the specific
        identification the photo actually resolves to.

        Linked-taxon hierarchy leaves are canonicalized to the same-taxon
        root keyword's stored spelling — mirroring
        :meth:`get_species_keywords_for_photos` — so ``api_photo_detail`` can
        still match returned names against
        ``species_representative_lists``/``species_highlights``, which key on
        the canonical root. Without this, a photo whose only surviving species
        tag is a differently-spelled hierarchy leaf (``verdin`` after repair
        detached the ``Verdin`` root) would fail those lookups and the
        lightbox/context menu would offer to set it as representative again.

        Attached top-level rows keep their own stored spelling. When a photo
        carries a root alias such as ``Auriparus flaviceps`` and another root
        ``Verdin`` exists for the same taxon, curation writes preserve exact
        root-name matches, so rewriting the attached alias to an arbitrarily
        first same-taxon root would make representative/highlight state keyed
        to the actually attached name appear missing.

        Dedup identity mirrors :meth:`get_species_keywords_for_photos`:
        linked rows collapse by ``taxon_id`` and NULL-taxon rows key on
        the exact stored name. Two distinct linked homonyms (``Robin`` /
        ``robin`` pointing at different taxa) or preserved NULL-taxon case
        variants (root ``Foo`` alongside hierarchy leaf ``foo``) would
        otherwise collapse under an ASCII case-fold match key and hide
        one from ``api_photo_detail`` even though its keyword remains
        attached and its curation is keyed by the exact stored name.
        """
        return self._species_curation_repository().get_photo_life_list_species(photo_id)

    def get_life_list_locations(self, species=None):
        """Return {identification name: [location keyword names]} for the life list.

        A location is attributed to an identification when at least one
        workspace-visible, non-rejected photo carries both the
        identification keyword and a ``type = 'location'`` keyword.
        Higher-rank taxonomy identifications (genus, family, class, …) are
        eligible here for the same reason they are in
        :meth:`get_life_list_candidates` — so a genus-level entry rendered
        on the Life List keeps its location chips and CSV values instead of
        appearing with an empty ``locations`` list.

        When ``species`` is given, only that identification is scanned —
        used by the single-identification paging endpoint so incremental
        loads don't do catalog-wide work. Matching mirrors
        :meth:`get_life_list_candidates`: raw ``k.name`` first, then a
        taxon-linked root fallback so a hierarchy leaf surviving repair
        (``verdin`` vs canonical root ``Verdin``) still contributes its
        location keywords to the requested bucket.

        Ancestor suppression also mirrors :meth:`get_life_list_candidates`:
        a linked higher-rank taxonomy keyword's locations are dropped
        when the same photo carries another linked taxonomy keyword whose
        taxon is a strict descendant of it, so ``Aves`` doesn't inherit
        every location where a robin was tagged.
        """
        return self._species_curation_repository().get_life_list_locations(species=species)

    def get_photo_preferences(self, purpose):
        """Return {species: photo_id} preferences for the active workspace."""
        return self._species_curation_repository().get_photo_preferences(purpose)

    def get_species_representative_lists(self, eligible_only=False, species=None):
        """Return {species: [photo_id, ...]} representative photos.

        Representative markings are global, but this read is still scoped to
        the active workspace's folders. Life List callers still apply these
        rows only to actual species buckets, so a representative row alone
        does not make an untagged species appear on the list. Lists are
        newest-selection first, so item 0 is the main representative.

        When ``eligible_only`` is true, omit preferences whose photo is
        rejected, unavailable, or no longer carries the stored species keyword.
        The preference row remains intact for undo.

        When ``species`` is given, only return rows for that species — used
        by the single-species Life List paging endpoint so incremental
        loads don't scan every species' representatives.
        """
        return self._species_curation_repository().get_representative_lists(
            eligible_only=eligible_only, species=species,
        )

    def get_species_representatives(self, eligible_only=False):
        """Return {species: main_photo_id} for compatibility callers."""
        return {
            species: photo_ids[0]
            for species, photo_ids in self.get_species_representative_lists(
                eligible_only=eligible_only
            ).items()
            if photo_ids
        }

    def _set_global_species_representative(self, species, photo_id):
        return self._species_curation_repository().set_global_representative(species, photo_id)

    def _restore_species_representative(
        self, species, photo_id, selected_order=None,
    ):
        """Restore a global species_representatives row on undo.

        When ``selected_order`` is None (legacy edit-history payloads
        recorded before this field was captured), assign a fresh order via
        :meth:`_set_global_species_representative` — preserving the older
        promote-to-newest behavior for those undos. Otherwise write the
        captured order so undoing a relabel of a secondary representative
        does not push it above the pre-existing primary.
        """
        return self._species_curation_repository().restore_representative(
            species, photo_id, selected_order=selected_order,
        )

    def set_photo_preference(self, purpose, species, photo_id, _commit=True):
        """Set the preferred photo for a species/purpose in this workspace.

        ``species`` is canonicalized to the spelling ``add_keyword`` would
        store (existing keyword row first, casing convention otherwise), so
        curation keys written from prediction-cased bucket labels — e.g.
        starring a photo in an unconfirmed ``Common Waxbill`` bucket — land
        on the same key the keyword row will use once the species is
        accepted. The eligible highlight/life-list queries compare these
        strings exact against ``keywords.name``.
        """
        return self._species_curation_repository().set_photo_preference(
            purpose, species, photo_id, _commit=_commit,
        )

    def set_species_representative(self, species, photo_id, _commit=True):
        """Mark a photo as a representative for a species globally.

        Multiple photos can represent a species. Re-selecting an existing
        representative promotes it by assigning the newest selection order.
        A compatibility ``photo_preferences`` row is kept for older callers
        that still expect one main representative in the active workspace.
        """
        self.set_photo_preference(
            "species_representative", species, photo_id, _commit=_commit
        )

    def clear_photo_preference(self, purpose, species, _commit=True):
        """Clear the preferred photo for a species/purpose in this workspace."""
        return self._species_curation_repository().clear_photo_preference(
            purpose, species, _commit=_commit,
        )

    def clear_species_representative(self, species, _commit=True):
        """Clear all representative photos for a species globally."""
        return self._species_curation_repository().clear_representative(species, _commit=_commit)

    def get_species_highlights(self, species=None, eligible_only=False):
        """Return ordered highlighted photo ids for the active workspace.

        When ``eligible_only`` is true, omit rejected photos and photos that
        are no longer eligible for the Highlights page. Stored rows are kept
        intact so un-rejecting a photo restores its selection.

        Eligibility mirrors :meth:`get_highlights_candidates` at the default
        quality floor: not-yet-analyzed (``quality_score IS NULL``) photos
        stay eligible, because they now appear on the Highlights page and can
        be saved as highlights. Filtering them out here would silently drop a
        highlight the user just chose until analysis ran.

        Result shape is ``{species: {photo_id: rank}}``.
        """
        return self._species_curation_repository().get_highlights(
            species=species, eligible_only=eligible_only,
        )

    def add_species_highlight(self, species, photo_id, _commit=True):
        """Add a photo to a species' ordered highlights, appending if new.

        ``species`` is canonicalized to the spelling ``add_keyword`` would
        store (see :meth:`set_photo_preference`) so highlight rows written
        from prediction-cased bucket labels key on the same string the
        keyword row and eligibility queries use.
        """
        return self._species_curation_repository().add_highlight(
            species, photo_id, _commit=_commit,
        )

    def promote_species_highlight(self, species, photo_id, _commit=True):
        """Add a photo to a species' ordered highlights at rank 1."""
        return self._species_curation_repository().promote_highlight(
            species, photo_id, _commit=_commit,
        )

    def remove_species_highlight(self, species, photo_id, _commit=True):
        """Remove a photo from a species' ordered highlights."""
        return self._species_curation_repository().remove_highlight(
            species, photo_id, _commit=_commit,
        )

    def move_species_highlight(self, species, photo_id, direction, _commit=True):
        """Move a highlighted photo one step up/down within its species."""
        return self._species_curation_repository().move_highlight(
            species, photo_id, direction, _commit=_commit,
        )

    def rename_photo_preferences_species(
        self, old_species, new_species, photo_workspace_pairs=None, _commit=True,
    ):
        """Rename stored representative-photo preferences across workspaces."""
        return self._species_curation_repository().rename_photo_preferences(
            old_species, new_species, photo_workspace_pairs=photo_workspace_pairs, _commit=_commit,
        )

    def rename_species_representatives_species(
        self, old_species, new_species, photo_ids=None, _commit=True,
    ):
        """Rename global representative rows for a species.

        ``photo_ids`` limits the rename to selected photos, used by relabel
        operations that only retag a subset of a species bucket.
        """
        return self._species_curation_repository().rename_representatives(
            old_species, new_species, photo_ids=photo_ids, _commit=_commit,
        )

    def rename_species_highlights_species(
        self, old_species, new_species, photo_workspace_pairs=None, _commit=True,
    ):
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

    def get_folders_with_quality_data(self):
        """Return folders with at least one scored photo in their subtree.

        Used to populate the folder dropdown on the highlights page.
        ``photo_count`` is the count of scored photos across the folder and
        all of its descendant folders (restricted to folders whose ``status``
        is ``'ok'``) — matching the subtree scope of
        :meth:`get_highlights_candidates`.
        """
        return self._folder_repository().with_quality_data()

    def update_keyword(self, keyword_id, **kwargs):
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
        if 'type' in kwargs:
            kt = kwargs['type']
            # Guard the membership test against non-hashable JSON values —
            # api_update_keyword passes the request body through, and
            # `x in frozenset` raises TypeError on unhashable input. Treat
            # any non-string as invalid (raise ValueError so the route's
            # existing catch yields the documented 400).
            if not isinstance(kt, str) or kt not in KEYWORD_TYPES:
                raise ValueError(f"Invalid keyword type: {kt!r}")
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
            current = self._keyword_repository().get_update_target(keyword_id)
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
                    peer = self._keyword_repository().same_type_peer(
                        new_name, parent_id, effective_type, keyword_id,
                    )
                    if peer:
                        # Return the surviving id so callers
                        # (api_update_keyword) can retarget sidecar and
                        # preferences bookkeeping onto the surviving row.
                        self._merge_keyword_into(keyword_id, peer["id"])
                        self._keyword_repository().commit()
                        return peer["id"]
                    # No same-type peer, but a DIFFERENT-type peer at the
                    # same (name, parent_id) would hit the table-level
                    # UNIQUE(name, parent_id) constraint at UPDATE time for
                    # a non-NULL parent and surface as an uncaught
                    # IntegrityError/500. Detect it here and raise
                    # ValueError so api_update_keyword returns a documented
                    # 400. For NULL parents a cross-type peer is allowed to
                    # coexist (mirrors add_keyword).
                    if parent_id is not None:
                        cross = self._keyword_repository().cross_type_peer(
                            new_name, parent_id, keyword_id,
                        )
                        if cross is not None:
                            raise ValueError(
                                f"cannot rename to {new_name!r}: a "
                                f"{cross['type']!r} keyword with that name "
                                f"already exists under this parent"
                            )

                # Auto-retype on rename: same logic as add_keyword. Only
                # fires on an actual name change so idempotent PUT-style
                # updates (client re-sending the existing name) don't
                # unexpectedly reclassify a 'general' keyword once the taxa
                # table is populated.
                if name_changed:
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

        self._keyword_repository().apply_update(keyword_id, updates)
        return keyword_id

    def get_all_keywords(self):
        """Return keywords used in the active workspace (plus ancestors) with photo counts, type, and taxon info."""
        return self._keyword_repository().list_all()

    # -- Predictions --

    def _prediction_repository(self):
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

        return PredictionRepository(
            self.conn,
            self._ws_id,
            chunks=_chunks,
            commit_with_retry=commit_with_retry,
            log=log,
            auto_match_review_marker=AUTO_MATCH_REVIEW_MARKER,
            top_prediction_confidence_expr=_TOP_PREDICTION_CONFIDENCE_EXPR,
            mixed_species_group_repair_key=self._MIXED_SPECIES_GROUP_REPAIR_KEY,
            facade=self,
        )

    def add_prediction(
        self,
        detection_id,
        species,
        confidence,
        model,
        category="new",
        status="pending",
        group_id=None,
        vote_count=None,
        total_votes=None,
        individual=None,
        taxonomy=None,
        labels_fingerprint="legacy",
        labels_fingerprint_full=None,
        preserve_manual_review=False,
        match_score=None,
        from_fresh_inference=False,
        refresh_output=False,
    ):
        """Store a classification prediction for a detection.

        Uses INSERT OR IGNORE so re-running classification doesn't destroy
        existing predictions that the user may have already reviewed.
        Use clear_predictions() first if you want a fresh start.

        The `predictions` table stores only the raw, workspace-independent
        classifier output (species, confidence, classifier_model, taxonomy).
        Per-workspace review state (status, group_id, vote_count, individual)
        is written to ``prediction_review`` for the active workspace when the
        caller passes a non-default value.

        Args:
            detection_id: the detection ID (from detections table)
            taxonomy: optional dict with keys kingdom, phylum, class, order,
                      family, genus, scientific_name from taxonomy lookup
            labels_fingerprint: fingerprint of the label set used to classify
                (defaults to 'legacy' for backwards-compatible inserts).
            preserve_manual_review: when True, do not overwrite an existing
                accepted/rejected review row unless it was auto-created for an
                XMP taxonomy match.
            match_score: this species' raw pre-softmax score (cosine or logit,
                per the model). Optional; ``confidence`` alone cannot say
                whether the label fits, only that it fit better than the rest
                of the list.
            from_fresh_inference: True when ``match_score`` comes from a model
                that just ran on this detection, so it supersedes whatever is
                stored. False (the default) when the caller is replaying a
                value it read back from somewhere else — cache
                materialization, a backfill — in which case an existing score
                is left alone and only a NULL is filled.
            refresh_output: replace the output fields of an existing candidate
                while retaining its row ID and manual review decisions.
        """
        return self._prediction_repository().add(
            detection_id, species, confidence, model, category=category, status=status, group_id=group_id, vote_count=vote_count, total_votes=total_votes, individual=individual, taxonomy=taxonomy, labels_fingerprint=labels_fingerprint, labels_fingerprint_full=labels_fingerprint_full, preserve_manual_review=preserve_manual_review, match_score=match_score, from_fresh_inference=from_fresh_inference, refresh_output=refresh_output,
        )

    def retain_prediction_candidates(self, detection_id, model, labels_fingerprint, species):
        """Remove obsolete candidates after their replacement outputs were stored.

        Matching candidates keep their IDs and reviews in every workspace.
        Other detections, models, label sets and classifier run keys are untouched.
        """
        return self._prediction_repository().retain_candidates(
            detection_id, model, labels_fingerprint, species,
        )

    def reconcile_match_review_state(
        self,
        detection_id,
        classifier_model,
        labels_fingerprint,
        species,
        category,
        auto_accept=True,
    ):
        """Re-sync a cached prediction's category and auto-review on reuse.

        Taxonomy ``match`` predictions are auto-accepted and intentionally
        hidden from the pending review queue (``_store_match_prediction``
        writes ``status='accepted'`` with ``AUTO_MATCH_REVIEW_MARKER``).  That
        review row is durable, so when a detection stops being a match — e.g.
        the photo's XMP keywords were edited — a later non-reclassify run
        reuses the cached prediction but the stale auto-accepted row would keep
        it out of the queue until a full reclassify/clear is forced.  Only the
        marked auto-review row is safe to drop here; explicit user decisions
        from before a temporary XMP match must remain intact.

        ``auto_accept`` is False when the caller has decided this reuse must
        stay pending even though ``category`` is still ``match`` — e.g. the
        XMP later gained a second recognized taxon, so a single-species match
        is now ambiguous.  Without dropping the marker in that case,
        ``status='accepted'`` from the earlier unambiguous run would keep the
        detection hidden from the queue.

        The persisted ``category`` is always refreshed to the current value:
        ``add_prediction`` is INSERT-OR-IGNORE so it never updates it on
        reuse, and a stale ``match`` marker would defeat the downgrade above
        on the next flip (and mislead the ``/api/predictions``
        disagreement/refinement enrichment).
        """
        return self._prediction_repository().reconcile_match_review_state(
            detection_id, classifier_model, labels_fingerprint, species, category, auto_accept=auto_accept,
        )

    def clear_predictions(self, model=None, collection_photo_ids=None,
                          labels_fingerprint=None, clear_run_keys=True):
        """Clear predictions, optionally filtered by model, photo set, and fingerprint.

        The ``predictions`` table is now global (no workspace_id).  This
        still restricts the delete to photos visible in the active workspace
        via ``workspace_folders`` so that calling "clear" in one workspace
        does not nuke another workspace's cached classifier output.

        ``labels_fingerprint`` is strongly recommended for reclassify flows:
        in shared-folder setups where workspace A and workspace B classify
        the same photos with different label sets, a reclassify in A keyed
        only by ``model`` would wipe B's cached predictions under its own
        fingerprint. And because ``classifier_runs`` keys include
        fingerprint, B's later non-reclassify runs would skip inference and
        leave those detections unclassified until forced. With
        ``labels_fingerprint`` passed, we delete only A's rows AND the
        matching ``classifier_runs`` rows so A's next pass actually re-runs.

        ``clear_run_keys=False`` is for callers that have just written fresh
        ``classifier_runs`` rows for these detections and are about to
        replace the predictions in the same transaction (e.g. the pipeline's
        deferred reclassify clear that runs after the per-photo
        ``record_classifier_run`` calls).  Wiping the run keys in that case
        would force the next non-reclassify pass to re-infer the entire
        collection.  Default ``True`` matches the long-standing safety
        behavior — only opt out if the caller guarantees fresh run keys.
        """
        return self._prediction_repository().clear(
            model=model, collection_photo_ids=collection_photo_ids, labels_fingerprint=labels_fingerprint, clear_run_keys=clear_run_keys,
        )

    def get_prediction_states(self, photo_ids):
        """Explain, per photo, why it may have no predictions to show.

        An empty prediction list has four completely different meanings and a
        blank panel implies only the first, so Browse needs them separated:
        nothing has run yet, the detector ran and found no animals, detections
        exist but were never classified, or classification ran and produced
        nothing the user's confidence floor admits.

        ``threshold`` travels with the state so the panel can split the rows
        it already has into visible and below-the-floor, and say the hidden
        count out loud rather than dropping those rows silently.

        ``detection_count`` counts only what the rest of this file calls a
        real detection: ``detector_model = 'full-image'`` rows are the
        synthetic whole-frame anchor written *because* the detector found
        nothing, and rows under the workspace's ``detector_confidence`` floor
        are the noise the classifier never acts on. Counting either would
        report "detections exist but were never classified" for a photo whose
        detector plainly found no animal — the exact conflation this method
        exists to prevent. Same rule as ``count_real_detections_in_scope``.
        """
        return self._prediction_repository().get_states(photo_ids)

    def get_predictions(self, photo_ids=None, model=None, status=None,
                        rules=None):
        """Get predictions with photo, detection and review info.

        Workspace scoping is enforced by joining ``workspace_folders``; the
        per-workspace review state (status, group_id, individual, vote_count)
        is left-joined from ``prediction_review`` so absent rows naturally
        surface as ``status = 'pending'``.

        Predictions are filtered to the most recent ``labels_fingerprint``
        per ``(detection_id, classifier_model)`` so stale rows from prior
        label sets don't contaminate ``/api/predictions`` or
        ``/api/predictions/compare`` after re-classification.

        Row-level scoping for prediction-field rules: the workspace/rules
        subquery below is applied at the photo level (``p.id IN (...)``),
        which correctly limits *which photos* surface but returns every
        current prediction for those photos. When the rules tree references
        Review-only fields (``prediction_confidence``, ``prediction_status``,
        ``classifier_model``, ``taxonomy_*``), a photo with a sibling
        prediction that matches would still return low-confidence /
        already-accepted rows that don't satisfy the visible filter — the
        Review grid and Accept All would then act on rows the filter chip
        excluded. ``_filter_prediction_rows_by_rules`` re-evaluates those
        leaves against each returned row so the grid matches the chip. It
        applies only when the tree can be resolved safely per row — see
        that method's docstring for when it falls back to the SQL result.
        """
        return self._prediction_repository().get_rows(
            photo_ids=photo_ids, model=model, status=status, rules=rules,
        )

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

        def _contains_prediction_field(node):
            if not isinstance(node, dict):
                return False
            if "rules" in node and "field" not in node:
                return any(
                    _contains_prediction_field(c)
                    for c in node.get("rules") or []
                )
            return node.get("field") in self._PREDICTION_ROW_FIELDS

        def _walk(node, negated):
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
                    # siblings prune photos before the row filter sees
                    # them (see docstring). Drop the whole subgroup so
                    # SQL broadens; the row filter re-evaluates the
                    # ORIGINAL tree per row.
                    if (
                        child_negated
                        and is_group
                        and _contains_prediction_field(child)
                    ):
                        had_broad = True
                        continue
                    stripped = _walk(child, child_negated)
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
                    node.get("field") in self._PREDICTION_ROW_FIELDS:
                return None
            return node

        if isinstance(rules, list):
            wrapped = _walk({"mode": "all", "rules": rules}, False)
            if wrapped is broad or wrapped is None:
                return []
            return wrapped.get("rules", [])
        walked = _walk(rules, False)
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
        # the dict identity is what we look up in ``_leaf_row_match``.
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
                    leaf_photo_sets[key] = set(self.query_photo_ids([node]))
                except (ValueError, sqlite3.Error):
                    # Malformed / unresolvable leaf: fall back to matching
                    # every photo the SQL subquery kept. Never hide rows
                    # the outer filter should have allowed through.
                    leaf_photo_sets[key] = None

        _collect_alt_metadata_leaves(root)

        def _truthy(value):
            return value is True or value == 1 or value == "1" or value == "true"

        def _num_match(row_val, op, want):
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

        def _text_match(row_val, op, want):
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

        def _enum_match(row_val, op, want):
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

        def _leaf_row_match(rule, row):
            field = rule.get("field")
            op = rule.get("op", "")
            value = rule.get("value")
            if field == "prediction_confidence":
                return _num_match(row["confidence"], op, value)
            if field == "prediction_status":
                return _enum_match(row["status"], op, value)
            if field == "classifier_model":
                return _text_match(row["classifier_model"], op, value)
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
                    return _text_match(col, "contains", value)
                return _text_match(col, op, value)
            if field == "needs_review":
                is_pending = str(row["status"] or "pending") == "pending"
                return is_pending if _truthy(value) else not is_pending
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
            return _leaf_row_match(node, row)

        return [r for r in rows if _eval(root, r)]

    # The one definition of "this prediction's review is already settled".
    # ``app.py`` binds ``_DECIDED_PREDICTION_STATUSES`` to it, the grouped
    # accept expansion below uses it, and Browse's panel mirrors it in JS, so
    # no surface can drift on which rows are still actionable. See
    # ``app.py``'s ``_decided_prediction_ids`` for why ``alternative`` is
    # absent and what each endpoint then does with those rows.
    DECIDED_PREDICTION_STATUSES = ("accepted", "rejected", "reviewed")

    def update_prediction_status(self, prediction_id, status, _commit=True):
        """Update per-workspace review status for a prediction.

        Review state lives in ``prediction_review`` keyed by
        (prediction_id, workspace_id); we upsert here rather than UPDATE
        so the "first review in a fresh workspace" path still writes a row.

        Args:
            _commit: If False, skip the internal commit (caller is responsible
                     for committing the transaction).
        """
        return self._prediction_repository().update_status(prediction_id, status, _commit=_commit)

    def get_group_predictions(self, group_id):
        """Get all predictions and photo data for a burst group.

        ``group_id`` lives in the workspace-scoped ``prediction_review``
        table now, so we join there to find the member predictions.  Each
        returned row is a dict with an ``alternatives`` list containing the
        per-detection alternative species predictions (review status
        ``'alternative'``), sorted by confidence descending.
        """
        return self._prediction_repository().get_group(group_id)

    def update_predictions_status_by_photo(self, photo_id, status,
                                           _commit=True):
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

    _MIXED_SPECIES_GROUP_REPAIR_KEY = "prediction_review_mixed_species_groups_v1"

    def repair_mixed_species_prediction_groups(self):
        """Ungroup legacy bursts whose stored votes span more than one species.

        ``classify_job._store_grouped_predictions`` only stamps ``group_id``
        and ``individual`` when every frame in the burst folds to a single
        ``species_match_key`` — ``group_reviewable``. That gate arrived in
        #1165; bursts stored before it got a ``group_id`` and an
        unconditional multi-species vote dict regardless of whether the
        frames agreed.

        Those rows are actively wrong, not merely stale. ``accept_prediction``
        derives the species it applies from the vote dict, so a legacy row
        displays its own ``predictions.species`` and tags the vote winner:
        accepting a frame labelled ``Purple Finch`` writes ``Cassin's
        Finch``. That is the black box ``CORE_PHILOSOPHY.md`` forbids, and no
        amount of careful rendering fixes it — the button would still have to
        name one species and apply another.

        So repair the data rather than the symptom: for every review row
        whose ``individual`` holds more than one distinct species key, clear
        ``group_id``, ``individual``, ``vote_count`` and ``total_votes``.
        The row then reads exactly as the current classifier would have
        written it — an ungrouped prediction that accepts as its own species.
        ``status`` is untouched (a decision the user already made stays
        made), and nothing in ``predictions`` is read or written: no
        prediction is deleted, retitled or rescored, only the burst-grouping
        metadata that was never valid.

        Rows whose ``individual`` has several JSON keys that fold to *one*
        species are left grouped. ``species_match_key`` collapses okina,
        typographic-apostrophe and case variants, so ``{"Hawai'i 'Amakihi":
        4, "Hawai’i ’Amakihi": 2}`` is a unanimous burst spelled two
        ways and its grouping is legitimate. This is why the fold runs in
        Python: SQLite's ``lower()`` does not apply that normalization and
        would ungroup those rows.

        Not workspace-scoped. ``prediction_review`` is keyed by
        ``(prediction_id, workspace_id)`` and the same legacy classify run
        wrote rows in whichever workspaces were active at the time, so this
        deliberately runs across every workspace rather than through
        ``_ws_id()``.

        Idempotent and gated by a ``db_meta`` marker rather than
        ``PRAGMA user_version``: this repo has known ``user_version`` drift
        between branches, and a version-gated migration silently skips on a
        database whose number already ran ahead. After the first run the cost
        is one indexed ``db_meta`` lookup.

        Returns the number of review rows cleared.
        """
        return self._prediction_repository().repair_mixed_species_groups()

    def ungroup_prediction(self, prediction_id, _commit=True):
        """Remove a prediction from its group in the active workspace.

        ``group_id`` lives in ``prediction_review``; this only clears the
        review row for the current workspace.
        """
        return self._prediction_repository().ungroup(prediction_id, _commit=_commit)

    def get_existing_prediction_photo_ids(self, model, labels_fingerprint=None):
        """Return photo_ids with predictions for a (model, fingerprint), scoped to active workspace.

        The cache identity of a prediction is
        ``(detection_id, classifier_model, labels_fingerprint, species)``, so
        the photo-level short-circuit in classify_job / pipeline_job must key
        on both model AND fingerprint. Keying only on model means changing
        the workspace's label set leaves stale predictions and the classifier
        is never re-run until the user forces ``reclassify``.

        ``labels_fingerprint=None`` preserves the pre-fingerprint behavior
        for callers that haven't plumbed the fingerprint through yet.
        """
        return self._prediction_repository().get_existing_photo_ids(
            model, labels_fingerprint=labels_fingerprint,
        )

    def get_top_prediction_for_photo(self, photo_id, min_detector_confidence=None):
        """Return the highest-confidence *current* prediction for a photo.

        "Current" means: workspace-scoped via workspace_folders, and for
        each (detection, classifier_model) only the most recent
        labels_fingerprint's rows are considered — stale predictions from
        prior label sets on the same detection are skipped so callers
        like /api/inat/prepare don't prefill a taxon from an old label set.

        ``min_detector_confidence``: optional read-time threshold applied to
        the joined detection. With read-time thresholding, predictions tied
        to detections below the active threshold are visually hidden in the
        UI; callers like the iNat endpoints should pass the workspace-
        effective threshold so they don't surface a species from a now-
        hidden detection.

        Returns a dict with ``species``, ``scientific_name``, ``confidence``,
        ``detection_id`` or None if no eligible prediction exists.
        """
        return self._prediction_repository().get_top_for_photo(
            photo_id, min_detector_confidence=min_detector_confidence,
        )

    def get_top_prediction_confidences(self, photo_ids):
        """Map photo id → the confidence the Browse sorts rank on.

        Built from the same ``_TOP_PREDICTION_CONFIDENCE_EXPR`` the
        ``prediction_confidence`` sorts order by, so the number a card shows
        is by construction the number that put it where it is — a card badge
        computed from a second, nearly-identical query is exactly the kind of
        cheap proxy CORE_PHILOSOPHY's "no black boxes" rule rules out.

        Photos with no current, unrejected species prediction are absent from
        the mapping rather than present with a 0.0, which would read as "the
        classifier is certain this is nothing".
        """
        return self._prediction_repository().get_top_confidences(photo_ids)

    def get_prediction_for_photo(self, photo_id, model, labels_fingerprint=None):
        """Return species, confidence, and detection_id for a photo's prediction.

        Detections and predictions are global; the active workspace is
        enforced through ``workspace_folders``. Since prediction cache
        identity is (detection, model, fingerprint, species), callers
        should pass ``labels_fingerprint`` to avoid returning a row
        written under a different label set. ``labels_fingerprint=None``
        preserves the pre-refactor behavior (any row for the model).
        """
        return self._prediction_repository().get_for_photo(
            photo_id, model, labels_fingerprint=labels_fingerprint,
        )

    def get_photo_embedding(self, photo_id, model, variant=''):
        """Return the embedding blob for (photo_id, model, variant), or None."""
        return self._masks_features_repository(scoped=False).get_embedding(
            photo_id, model, variant,
        )

    def upsert_photo_embedding(self, photo_id, model, embedding_bytes,
                               variant='', verify_workspace=False):
        """Store an embedding blob for (photo_id, model, variant).

        Replaces any existing row with the same primary key. ``model`` is
        required because the storage philosophy keeps a per-model cache; a
        missing model name has no key in the table.

        Args:
            verify_workspace: when True, raises ValueError if the photo is
                not in the active workspace. Defaults to False because this
                method is typically called from background classify jobs that
                already iterate only over workspace-scoped photos.
        """
        if verify_workspace:
            self._verify_photo_in_workspace(photo_id)
        self._masks_features_repository(scoped=False).upsert_embedding(
            photo_id, model, embedding_bytes, variant,
        )

    def get_photos_with_embedding(
        self, model, variant='', photo_ids=None, include_offline_folders=False,
    ):
        """Return (photo_id, embedding_blob) pairs in the active workspace
        with a stored embedding for ``(model, variant)``.

        Pass ``photo_ids`` to restrict the result to a subset.
        """
        return self._masks_features_repository().photos_with_embedding(
            model, variant=variant, photo_ids=photo_ids,
            include_offline_folders=include_offline_folders,
        )

    def clear_prediction_group_info(self, detection_id, model,
                                    labels_fingerprint=None):
        """Drop stale group metadata from the cached prediction's review row.

        Used when a cached prediction that previously belonged to a
        reviewable burst is reused under a run where the burst is no longer
        group-reviewable (mixed species, singleton, etc.), so the caller
        would otherwise pass ``group_id=None`` to
        ``update_prediction_group_info`` and skip it. Without this, the old
        ``group_id`` / ``individual`` / vote counts stay attached and group
        actions retag the whole stale burst together.

        Only updates an existing ``prediction_review`` row — never inserts
        one — so the "absence == pending" invariant that ``add_prediction``
        enforces for un-reviewed detections stays intact.
        """
        return self._prediction_repository().clear_group_info(
            detection_id, model, labels_fingerprint=labels_fingerprint,
        )

    def update_prediction_group_info(self, detection_id, model, group_id,
                                     vote_count, total_votes, individual,
                                     labels_fingerprint=None):
        """Upsert group info for the primary prediction of
        (detection, classifier_model, labels_fingerprint) in the active
        workspace's ``prediction_review``.

        Alternative rows (review status ``'alternative'``) are intentionally
        skipped so they do not inherit grouping metadata that belongs to the
        primary pick.

        ``labels_fingerprint`` scopes the "primary" pick to one label set;
        omitting it picks the highest-confidence row across all fingerprints
        for back-compat with legacy callers, but current callers should
        always pass the active fingerprint so group metadata doesn't land
        on a row produced under a stale label set.
        """
        return self._prediction_repository().update_group_info(
            detection_id, model, group_id, vote_count, total_votes, individual, labels_fingerprint=labels_fingerprint,
        )

    def is_keyword_species(self, keyword_id):
        """Return True if the keyword represents a species-rank taxon.

        Preserve the legacy flag/type fallback when no taxonomy row can be
        resolved, but do not call a linked family/genus keyword a species.
        """
        return self._keyword_repository().is_species(keyword_id)

    def accept_prediction(
        self,
        prediction_id,
        replace_species=False,
        photo_ids=None,
        prediction_ids=None,
        _commit=True,
    ):
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

        All database changes are performed atomically in a single transaction
        unless ``_commit`` is False and the caller owns the transaction.
        """
        ws = self._ws_id()
        limited_photo_ids = None
        if photo_ids is not None:
            limited_photo_ids = {int(pid) for pid in photo_ids}
        limited_pred_ids = None
        if prediction_ids is not None:
            limited_pred_ids = {int(pid) for pid in prediction_ids}
        # Load taxonomy once for the whole call so replace_species can protect
        # keywords whose relationship to a neighbouring subject's prediction is
        # broader/same/narrower — not just exact-text matches. Loaded here
        # rather than inside _accept_for_photo so grouped accepts don't repeat
        # the JSON parse per photo. None (missing/corrupt file, or unrelated
        # import failure) cleanly degrades to exact-text protection.
        _replace_taxonomy = None
        _compare_pred_to_kws = None
        if replace_species:
            try:
                from compare import compare_prediction_to_keywords as _cpk
                from taxonomy import load_local_taxonomy as _llt
                _replace_taxonomy = _llt()
                _compare_pred_to_kws = _cpk
            except Exception:
                _replace_taxonomy = None
                _compare_pred_to_kws = None
        pred = self.conn.execute(
            """SELECT pr.*,
                      pr.classifier_model AS model,
                      pr_rev.group_id AS group_id,
                      pr_rev.individual AS individual,
                      d.photo_id
               FROM predictions pr
               JOIN detections d ON d.id = pr.detection_id
               LEFT JOIN prediction_review pr_rev
                 ON pr_rev.prediction_id = pr.id AND pr_rev.workspace_id = ?
               WHERE pr.id = ?""",
            (ws, prediction_id),
        ).fetchone()
        if not pred:
            return None
        from species_identity import SpeciesResolver
        identity = SpeciesResolver(db=self).consensus(pred)
        # A pure name lookup can flag a same-name keyword the async
        # ``mark_species_keywords`` pass has not linked yet. Routing the
        # accept through ``_add_source_species_keyword`` on that inferred
        # id refuses to reuse the unlinked row and mints a suffixed
        # duplicate; only explicit prediction evidence (a stored
        # ``source_taxon_id`` or a native tol/iNat scientific name)
        # earns that source-specific path.
        native_scientific = pred["scientific_name"] if (
            pred["labels_fingerprint"] == "tol" or pred["model"].startswith("iNat")
        ) else None
        has_explicit_evidence = pred["source_taxon_id"] is not None or bool(native_scientific)
        source_taxon_id = identity.taxon_id if has_explicit_evidence else None

        def _reject_siblings_of(this_pred_id):
            """Resolve the losing rows on one accepted row's detection.

            Rejects siblings for the same
            (detection, classifier_model, labels_fingerprint) in this
            workspace (covers both accepting an alternative and accepting the
            top-1). Scoping by fingerprint is critical — without it, accepting
            a prediction from a new label set would mark old label-set rows as
            rejected, silently rewriting review state for unrelated
            fingerprints. Review state is workspace-scoped, so we upsert each
            row rather than UPDATE the base predictions table.

            Run per accepted row, and only for rows this call accepts: a
            grouped accept decides every member's detection, so leaving the
            other members' alternatives at 'alternative' would keep photos in
            Review's queue that this call already settled — and would make it
            unsafe for a batch caller to skip a submitted row that a grouped
            accept covered. Equally, a row the caller's scope excludes must
            not have its alternatives resolved, so this never runs for the
            entry row before scope is settled.
            """
            row = self.conn.execute(
                """SELECT detection_id, classifier_model, labels_fingerprint
                   FROM predictions WHERE id = ?""",
                (this_pred_id,),
            ).fetchone()
            if row is None:
                return
            sibs = self.conn.execute(
                """SELECT pr.id FROM predictions pr
                   LEFT JOIN prediction_review pr_rev
                     ON pr_rev.prediction_id = pr.id AND pr_rev.workspace_id = ?
                   WHERE pr.detection_id = ?
                     AND pr.classifier_model = ?
                     AND pr.labels_fingerprint = ?
                     AND pr.id != ?
                     AND COALESCE(pr_rev.status, 'pending') IN ('pending', 'alternative')""",
                (ws, row["detection_id"], row["classifier_model"],
                 row["labels_fingerprint"], this_pred_id),
            ).fetchall()
            for s in sibs:
                self.conn.execute(
                    """INSERT INTO prediction_review
                         (prediction_id, workspace_id, status, reviewed_at)
                       VALUES (?, ?, 'rejected', datetime('now'))
                       ON CONFLICT(prediction_id, workspace_id)
                       DO UPDATE SET status = 'rejected',
                                     reviewed_at = datetime('now')""",
                    (s["id"], ws),
                )

        try:
            if has_explicit_evidence:
                species = identity.display_name if source_taxon_id else (identity.scientific_name or identity.display_name)
            else:
                # Preserve the raw label (or burst winner) for legacy
                # predictions: ``add_keyword``'s name-based dedup will
                # reuse an existing same-name keyword, even one
                # ``mark_species_keywords`` has not linked yet, without
                # introducing a suffixed duplicate.
                species = pred["species"]
                if pred["group_id"] and pred["individual"]:
                    try:
                        votes = json.loads(pred["individual"])
                        if isinstance(votes, dict) and votes:
                            species = max(votes, key=lambda sp: votes[sp])
                    except (TypeError, ValueError):
                        pass

            # Settle scope before the first write.
            #
            # ``limited_photo_ids`` / ``limited_pred_ids`` exist to make this
            # call a no-op outside the caller's submitted scope, so *every*
            # mutation — sibling rejection, keyword creation, status flips —
            # has to sit behind that decision rather than beside it. This
            # method used to reject the entry row's alternatives up front,
            # which silently resolved rows the caller never submitted while
            # the return value truthfully reported no accepts. The row set is
            # therefore computed from reads only; nothing below writes until
            # it is non-empty.
            def _in_scope(photo_id, this_pred_id):
                photo_allowed = (
                    limited_photo_ids is None
                    or photo_id in limited_photo_ids
                )
                # The row-level limit, checked independently: a group
                # member's photo being in scope does not make every row
                # that member carries in scope.
                row_allowed = (
                    limited_pred_ids is None
                    or this_pred_id in limited_pred_ids
                )
                return photo_allowed and row_allowed

            def _expansion_allowed(this_pred_id, status):
                """May group expansion *discover* this row?

                Group expansion reaches rows the caller never named — that is
                its whole point — so it must not reach rows whose decision is
                already made. Without this, accepting one burst member from
                Review re-accepts a sibling the user explicitly rejected
                earlier (tagging that photo with the species it was denied)
                and re-flips long-accepted siblings, whose "previous" status
                in the resulting history item is then a fiction: undo would
                knock them back to pending. ``reviewed`` is treated the same
                as ``accepted`` / ``rejected`` here: the user marked the row
                reviewed to say "I looked and chose not to act", and
                expanding a group into it would silently flip that decision
                to ``accepted`` without any audit trail describing the
                overwrite.

                Rows the caller *named* are exempt, because then the caller,
                not the expansion, chose them: the entry row itself, and any
                row listed in ``limited_pred_ids``. ``batch-accept`` never
                lists a decided row (``_decided_prediction_ids`` filters them
                out first), so in practice this only ever exempts a row a
                route was pointed at directly.
                """
                if this_pred_id == prediction_id:
                    return True
                if (
                    limited_pred_ids is not None
                    and this_pred_id in limited_pred_ids
                ):
                    return True
                return status not in self.DECIDED_PREDICTION_STATUSES

            # If grouped, accept every prediction in the group (in this
            # workspace) that survives the caller's scope.
            if pred["group_id"]:
                group_preds = self.conn.execute(
                    """SELECT pr.id, d.photo_id, pr_rev.status AS status
                       FROM predictions pr
                       JOIN prediction_review pr_rev
                         ON pr_rev.prediction_id = pr.id AND pr_rev.workspace_id = ?
                       JOIN detections d ON d.id = pr.detection_id
                       JOIN photos ph ON ph.id = d.photo_id
                       JOIN workspace_folders wf
                         ON wf.folder_id = ph.folder_id AND wf.workspace_id = ?
                       WHERE pr_rev.group_id = ? AND pr.classifier_model = ?""",
                    (ws, ws, pred["group_id"], pred["model"]),
                ).fetchall()
                targets = [
                    (gp["photo_id"], gp["id"])
                    for gp in group_preds
                    if _in_scope(gp["photo_id"], gp["id"])
                    and _expansion_allowed(gp["id"], gp["status"])
                ]
            elif _in_scope(pred["photo_id"], prediction_id):
                targets = [(pred["photo_id"], prediction_id)]
            else:
                targets = []

            if not targets:
                # Nothing this caller submitted is acceptable here, so leave
                # the database exactly as it was and report no changes.
                # ``add_keyword`` is a write too (it creates the species row),
                # so it also waits behind the scope decision; the read-only
                # lookup keeps ``keyword_id``/``species`` meaningful for
                # callers reconciling one batch's species without inventing a
                # keyword for an accept that never happened.
                display = normalize_keyword_display(species)
                existing = self.conn.execute(
                    """SELECT id, name FROM keywords
                       WHERE name = ? COLLATE NOCASE
                       ORDER BY id LIMIT 1""",
                    (display,),
                ).fetchone()
                return {
                    "species": existing["name"] if existing else display,
                    "species_key": identity.key,
                    "keyword_id": existing["id"] if existing else None,
                    "affected": [],
                    "accepted_prediction_ids": [],
                    "photo_ids": [],
                }

            source_args = {"source_taxon_id": source_taxon_id} if source_taxon_id is not None else {}
            kid = self.add_keyword(species, is_species=True, _commit=False, **source_args)
            # Re-read the stored keyword name so the queued sidecar changes,
            # curation renames, and returned history payload all reflect the
            # row actually tagged. add_keyword normalizes punctuation and
            # applies the species casing convention, so the stored spelling
            # can differ from the raw prediction label; using the raw value
            # downstream would queue pending add/remove pairs that no longer
            # cancel and write the un-normalized label to XMP.
            stored = self.conn.execute(
                "SELECT name FROM keywords WHERE id = ?", (kid,)
            ).fetchone()
            if stored and stored["name"]:
                species = stored["name"]
            # list of {"photo_id", "prediction_id", "old_species"}
            affected = []
            # Every row this call flips to accepted, including ones that
            # produced no ``affected`` entry (a status-only accept under
            # replace_species). Callers batching over a submitted id list use
            # it to avoid re-entering rows a grouped accept already covered.
            accepted_pred_ids = []

            def _accept_for_photo(photo_id, this_pred_id):
                accepted_pred_ids.append(this_pred_id)
                # Every accepted row resolves its own detection's losers,
                # including the entry row. Doing it here rather than up front
                # is what keeps an out-of-scope entry row untouched.
                _reject_siblings_of(this_pred_id)
                self.update_prediction_status(this_pred_id, "accepted", _commit=False)
                old_species = []
                already_has_species = photo_id in self.get_photos_with_equivalent_species(
                    [photo_id], kid
                )
                if replace_species:
                    # Replace corrects *this subject's* identity, so it must
                    # not strip a species that belongs to a different detection
                    # (another subject) on the same photo. Any species named by
                    # a live prediction on another box is protected; without
                    # this, correcting the teal's ID wiped the American Wigeon
                    # confirmed on the neighbouring box. On a single-detection
                    # photo no box is protected, so every species keyword is
                    # replaced exactly as before.
                    this_det = self.conn.execute(
                        "SELECT detection_id FROM predictions WHERE id = ?",
                        (this_pred_id,),
                    ).fetchone()
                    this_det_id = this_det["detection_id"] if this_det else None
                    # Mirror Compare's visibility filter when picking which
                    # neighbouring predictions may protect a species keyword:
                    #   * skip 'alternative' rows (Compare drops them at
                    #     web/predictions.py's api_predictions_compare, alongside
                    #     'rejected');
                    #   * skip detections below the workspace's effective
                    #     detector_confidence — Compare marks those "dormant"
                    #     and excludes their subjects entirely.
                    # Without this, a below-threshold neighbour or an
                    # alternative row on a real neighbour would keep an
                    # already-stale species keyword on the photo — replace
                    # would leave it in place and never queue a
                    # keyword_remove, so the sidecar would still list the
                    # dead species.
                    import config as _cfg
                    _det_threshold = self.get_effective_config(
                        _cfg.load()
                    ).get("detector_confidence", 0.2)
                    # Restrict to the latest labels_fingerprint per
                    # (detection, classifier_model) — mirrors get_predictions
                    # and the review/summary paths so stale rows from a prior
                    # label set on a re-classified neighbouring detection do
                    # not spuriously protect an obsolete species keyword.
                    # Fold both sides through keyword_match_key so a raw
                    # prediction species like `‘apapane` matches the stored
                    # keyword `apapane` (add_keyword normalizes on write, so
                    # a lower(trim(species)) SQL fold would otherwise miss
                    # the still-live neighbour and queue its removal).
                    neighbour_species = [
                        row["species"] for row in self.conn.execute(
                            """SELECT DISTINCT pr.species AS species
                               FROM predictions pr
                               JOIN detections d ON d.id = pr.detection_id
                               LEFT JOIN prediction_review pr_rev
                                 ON pr_rev.prediction_id = pr.id
                                AND pr_rev.workspace_id = ?
                               WHERE d.photo_id = ?
                                 AND pr.detection_id IS NOT ?
                                 AND COALESCE(pr_rev.status, 'pending')
                                     NOT IN ('rejected', 'alternative')
                                 AND d.detector_confidence >= ?
                                 AND pr.labels_fingerprint = (
                                     SELECT pr2.labels_fingerprint
                                     FROM predictions pr2
                                     WHERE pr2.detection_id = pr.detection_id
                                       AND pr2.classifier_model
                                           = pr.classifier_model
                                     ORDER BY pr2.created_at DESC, pr2.id DESC
                                     LIMIT 1
                                 )""",
                            (ws, photo_id, this_det_id, _det_threshold),
                        ).fetchall()
                        if row["species"]
                    ]
                    protected = {
                        keyword_match_key(s) for s in neighbour_species
                    }
                    existing = self.conn.execute(
                        """SELECT k.id, k.name, k.taxon_id
                           FROM photo_keywords pk
                           JOIN keywords k ON k.id = pk.keyword_id
                           LEFT JOIN taxa t ON t.id = k.taxon_id
                           WHERE pk.photo_id = ?
                             AND (k.is_species = 1 OR k.type = 'taxonomy')
                             AND (t.rank = 'species' OR t.rank IS NULL)""",
                        (photo_id,),
                    ).fetchall()
                    target_row = self.conn.execute(
                        "SELECT name, taxon_id FROM keywords WHERE id = ?",
                        (kid,),
                    ).fetchone()
                    # Mirror get_photos_with_equivalent_species: when another
                    # taxonomy/species keyword row shares the target's match
                    # key but points at a different taxon (e.g. legacy
                    # ``Robin`` alongside taxonomy ``robin``), the unlinked
                    # same-key row on the photo is ambiguous — it could be
                    # either species. Treating it as the target here would
                    # exclude it from ``to_remove``, so Replace Keywords
                    # would leave the wrong species attached while adding
                    # the correct one. Detect the homonym conflict once and
                    # gate the NULL-taxon fallback below.
                    #
                    # The same guard applies when the *target* is unlinked:
                    # a linked same-key row is a distinct species that must
                    # not be folded into the unlinked target, or Replace
                    # Keywords would exclude the linked homonym from
                    # ``to_remove`` and leave the wrong species attached.
                    _target_key = keyword_match_key(target_row["name"])
                    _target_homonym_conflict = False
                    if target_row["taxon_id"] is not None:
                        for _hrow in self.conn.execute(
                            """SELECT name FROM keywords
                               WHERE (is_species = 1 OR type = 'taxonomy')
                                 AND taxon_id IS NOT NULL
                                 AND taxon_id != ?""",
                            (target_row["taxon_id"],),
                        ).fetchall():
                            if keyword_match_key(_hrow["name"]) == _target_key:
                                _target_homonym_conflict = True
                                break
                    else:
                        for _hrow in self.conn.execute(
                            """SELECT name FROM keywords
                               WHERE (is_species = 1 OR type = 'taxonomy')
                                 AND taxon_id IS NOT NULL
                                 AND id != ?""",
                            (kid,),
                        ).fetchall():
                            if keyword_match_key(_hrow["name"]) == _target_key:
                                _target_homonym_conflict = True
                                break

                    def _is_target_species(row):
                        if target_row["taxon_id"] is not None:
                            if row["taxon_id"] == target_row["taxon_id"]:
                                return True
                            return (
                                row["taxon_id"] is None
                                and not _target_homonym_conflict
                                and keyword_match_key(row["name"])
                                == _target_key
                            )
                        # Unlinked target: when a distinct linked row shares
                        # this match key, only the exact target keyword row
                        # is safe to treat as equivalent.
                        if _target_homonym_conflict:
                            return row["id"] == kid
                        return (
                            keyword_match_key(row["name"]) == _target_key
                        )
                    # Compare treats a neighbouring subject's prediction as
                    # supporting an existing keyword under the taxonomy —
                    # match (same taxon), refinement (existing is broader
                    # than the prediction), broader (existing is more
                    # specific than the prediction). See compare.py's
                    # compare_prediction_to_keywords and the "keyword
                    # support" counters in templates/id_conflicts.html. Without
                    # this, a photo tagged with a broader ancestor keyword
                    # (e.g. Anatidae) that is only "held down" by a
                    # neighbour's American Wigeon prediction is stripped
                    # when a different box is replaced, and its curation
                    # (highlights, representatives) gets migrated onto the
                    # new species — the wrong subject. With no taxonomy
                    # available the check quietly no-ops and we fall back
                    # to exact-text protection, matching prior behaviour.
                    def _supported_by_neighbour_taxonomy(kw_name):
                        if not _replace_taxonomy or not neighbour_species:
                            return False
                        if _compare_pred_to_kws is None:
                            return False
                        for pred_species in neighbour_species:
                            cmp_result = _compare_pred_to_kws(
                                pred_species, [kw_name], _replace_taxonomy,
                            )
                            if cmp_result["category"] in (
                                "match", "refinement", "broader",
                            ):
                                return True
                        return False

                    to_remove = [
                        row for row in existing
                        if not _is_target_species(row)
                        and keyword_match_key(row["name"]) not in protected
                        and not _supported_by_neighbour_taxonomy(row["name"])
                    ]
                    old_species = [row["name"] for row in to_remove]
                    for row in to_remove:
                        self.conn.execute(
                            """DELETE FROM photo_keywords
                               WHERE photo_id = ? AND keyword_id = ?""",
                            (photo_id, row["id"]),
                        )
                    # The DB rows are gone, but sync_to_xmp only strips a
                    # keyword from the sidecar when a matching keyword_remove
                    # pending change exists. Queue one per removed species so a
                    # "replace" actually clears the stale tags downstream. A
                    # still-pending add for the same keyword cancels out
                    # instead of stacking (mirrors queue_keyword_remove).
                    new_species_lower = species.lower()
                    for old_name in old_species:
                        if old_name.lower() == new_species_lower:
                            continue
                        cancelled = self.remove_pending_changes(
                            photo_id, "keyword_add", old_name, _commit=False,
                        )
                        if cancelled == 0:
                            self.queue_change(
                                photo_id, "keyword_remove", old_name,
                                _commit=False,
                            )
                    # Migrate curated species state (representatives and
                    # ordered highlights) alongside the replaced species
                    # tag. Without this, a photo highlighted or set as
                    # representative under the old species keeps rows in
                    # species_highlights / photo_preferences under a name
                    # it no longer carries, so it stops driving Highlights
                    # and Life List for the new species. Mirrors the
                    # migration in api_highlights_relabel.
                    #
                    # Curation is canonicalized on write, so when
                    # ``repair_duplicate_photo_species`` detaches the
                    # root ``Verdin`` and leaves a hierarchy alias like
                    # ``Desert Verdin`` attached, existing highlights and
                    # representatives remain keyed on the canonical root
                    # ``Verdin``. Renaming only from the raw removed row
                    # name (the alias) would miss those rows and strand
                    # the curation under the old species. Look up the
                    # canonical root spelling for each removed row's
                    # taxon and rename from both source names so either
                    # layout migrates. Sidecar removes above still use
                    # the raw ``old_name`` because the XMP file carries
                    # the alias, not the root spelling.
                    # Dedupe by exact source name. Both Python's
                    # ``str.lower()`` and the ASCII-fold ``keyword_match_key``
                    # collapse intentionally distinct rows: ``str.lower()``
                    # folds non-ASCII case (``"Éclair".lower() == "éclair"``),
                    # and ``keyword_match_key`` folds ASCII case-variant
                    # homonyms like legacy ``Robin`` vs taxonomy ``robin``
                    # that ``add_keyword`` deliberately keeps as separate
                    # rows. Either fold would drop the second distinct
                    # removed row's spelling from the curation rename source
                    # list, leaving highlights / representatives keyed on it
                    # stranded under a species the photo no longer carries.
                    # Curation rows are keyed by the exact stored species
                    # name, so exact-string dedup preserves every distinct
                    # source without renaming the same source twice.
                    curation_sources = []
                    seen_sources = set()
                    for row in to_remove:
                        for candidate in (row["name"], self._species_root_name_for_taxon(row["taxon_id"])):
                            if not candidate or candidate in seen_sources:
                                continue
                            seen_sources.add(candidate)
                            curation_sources.append(candidate)
                    for source_name in curation_sources:
                        self.rename_species_highlights_species(
                            source_name, species, [(photo_id, ws)],
                            _commit=False,
                        )
                        self.rename_photo_preferences_species(
                            source_name, species, [(photo_id, ws)],
                            _commit=False,
                        )
                changed_tag = not already_has_species
                if changed_tag:
                    self.tag_photo(
                        photo_id, kid, source="manual", _commit=False,
                    )
                    self.queue_change(photo_id, "keyword_add", species, _commit=False)
                # Record every mutation, and — for regular accepts — also
                # record status-only no-ops so the prediction-status flip
                # is auditable and undoable. Three cases feed ``affected``:
                #   * ``changed_tag`` — the target species tag was newly
                #     added and undo must untag it;
                #   * ``old_species`` — replace_species stripped stale
                #     species rows and undo must retag them;
                #   * neither, with ``replace_species=False`` — the photo
                #     already carried the target via an equivalent
                #     hierarchical/root row so nothing was tagged or
                #     untagged, but ``update_prediction_status`` still
                #     flipped this prediction to ``accepted``. The accept
                #     API records ``prediction_accept`` history from
                #     ``affected`` alone, so without this branch the
                #     status change would be silently non-auditable and
                #     undo could not restore ``pending`` on the accepted
                #     prediction (or its siblings). ``changed_tag=False``
                #     with empty ``old_species`` marks the entry as
                #     status-only so ``_apply_undo`` / ``_apply_redo``
                #     skip tag mutations while still reversing the review
                #     state.
                # For ``replace_species=True``, a total no-op (photo
                # already has the target and nothing to remove) is left
                # out — the replace endpoint records
                # ``prediction_replace_species``, which is not undoable,
                # so a status-only aggregate would only produce a
                # misleading audit entry with an empty ``old_value``.
                if changed_tag or old_species:
                    affected.append({
                        "photo_id": photo_id,
                        "prediction_id": this_pred_id,
                        "old_species": old_species,
                        "changed_tag": changed_tag,
                    })
                elif not replace_species:
                    affected.append({
                        "photo_id": photo_id,
                        "prediction_id": this_pred_id,
                        "old_species": [],
                        "changed_tag": False,
                    })

            for target_photo_id, target_pred_id in targets:
                _accept_for_photo(target_photo_id, target_pred_id)

            if _commit:
                self.conn.commit()
            return {
                "species": species,
                "species_key": identity.key,
                "keyword_id": kid,
                "affected": affected,
                "accepted_prediction_ids": accepted_pred_ids,
                "photo_ids": list(dict.fromkeys(
                    photo_id for photo_id, _pred_id in targets
                )),
            }
        except Exception:
            if _commit:
                self.conn.rollback()
            raise

    def accept_subject_species(self, prediction_id, _commit=True):
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

    # -- Detections --

    def _model_runs_repository(self):
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

    def record_detector_run(
        self,
        photo_id,
        detector_model,
        box_count,
        runtime_fingerprint="legacy",
        input_fingerprint=None,
    ):
        """Record that `detector_model` was run on `photo_id`.

        Global across workspaces — the output is a pure function of (photo, model).
        """
        self._model_runs_repository().record_detector_run(
            photo_id,
            detector_model,
            box_count,
            runtime_fingerprint,
            input_fingerprint,
        )

    def get_global_detection_stats(self):
        """Return global (workspace-agnostic) detector-cache counts.

        `detector_runs` is shared across workspaces by design — switching
        workspaces or bumping a threshold never invalidates these rows —
        so the settings page surfaces this as a single "N photos x M
        models cached" figure.
        """
        return self._model_runs_repository().get_global_detection_stats()

    def detector_run_is_pinned(self, photo_id, detector_model):
        """Return whether any workspace manually reviewed this detector output.

        Review state belongs to predictions, but replacing a detector runtime
        can delete the detection and cascade through both predictions and
        reviews.  A real accepted/rejected decision therefore pins the whole
        (photo, detector_model) result.  Auto-created taxonomy-match reviews
        are reproducible cache state and deliberately do not pin it.
        """
        return self._model_runs_repository().detector_run_is_pinned(
            photo_id,
            detector_model,
        )

    def get_detector_run_photo_ids(
        self, detector_model, runtime_fingerprint=None,
    ):
        """Return the set of photo_ids with a consistent cached detector run.

        Includes empty-scene photos (box_count=0) — which is the whole point:
        without this, we'd re-run the model forever on photos with no animals.

        Excludes torn states where `detector_runs.box_count > 0` but no matching
        row exists in `detections`. That shape happens when a reclassify pass
        clears detections (via `clear_detections`) and then the job fails
        before writing fresh rows (model init error, etc.). Leaving such
        photos in the skip set would strand them on full-image fallback
        until the user manually forces another reclassify.
        """
        return self._model_runs_repository().get_detector_run_photo_ids(
            detector_model,
            runtime_fingerprint,
        )

    def record_classifier_run(
        self,
        detection_id,
        classifier_model,
        labels_fingerprint,
        prediction_count,
        labels_fingerprint_full=None,
        runtime_fingerprint="legacy",
        input_fingerprint=None,
        input_recipe=None,
    ):
        self._model_runs_repository().record_classifier_run(
            detection_id,
            classifier_model,
            labels_fingerprint,
            prediction_count,
            labels_fingerprint_full,
            runtime_fingerprint,
            input_fingerprint,
            input_recipe,
        )

    def record_classifier_match_score(
        self,
        detection_id,
        classifier_model,
        labels_fingerprint,
        max_match_score,
        match_margin=None,
        top_species=None,
        label_count=None,
        score_kind=None,
    ):
        """Record how well the best label in a list actually matched.

        Written for every completed run, including runs that produced no
        prediction at all — unlike ``record_classifier_run``, whose zero-count
        rows are suppressed because that table gates re-classification. A run
        that matched nothing is the most informative case here, so suppressing
        it would defeat the purpose.

        ``max_match_score`` must be the best score over the entire label list,
        not merely over the predictions that cleared the confidence threshold.
        """
        self._model_runs_repository().record_classifier_match_score(
            detection_id,
            classifier_model,
            labels_fingerprint,
            max_match_score,
            match_margin,
            top_species,
            label_count,
            score_kind,
        )

    def has_classifier_match_score(
        self, detection_id, classifier_model, labels_fingerprint,
    ):
        """True when ``classifier_match_scores`` records this exact run.

        A completed classifier run whose every label fell under the
        confidence floor writes a match-score row but no prediction rows —
        that outcome ("nothing in your list fits") is exactly what the
        feature exists to record, and the per-detection cache gate in
        ``classify_job._classify_photos`` uses this to honor it instead of
        re-running the model on a stored no-match.
        """
        return self._model_runs_repository().has_classifier_match_score(
            detection_id,
            classifier_model,
            labels_fingerprint,
        )

    def get_unscored_current_prediction_runs(self, photo_id):
        """Return ``(detection_id, classifier_model)`` pairs displayed without a score.

        A migrated catalog carries predictions from models that ran before
        ``classifier_match_scores`` existed: those predictions still surface in
        the panel because ``get_predictions`` pins to their (still latest)
        ``labels_fingerprint``, but the score table is empty for them. The
        blanket "no label in this list matches" verdict must not be applied
        over those rows — the legacy model was never judged.

        Returns one entry per current-fingerprint ``(detection, model)`` pair
        that has at least one prediction row on the photo but no row in
        ``classifier_match_scores`` under the same fingerprint. Callers hand
        these to ``match_confidence.summarize_photo`` so the photo-level
        rollup can degrade to ``uncalibrated`` rather than declaring every
        displayed prediction unlisted.

        Not workspace-scoped — ``photo_id`` is assumed already verified by the
        caller, as the existing per-photo routes do before reaching here.
        """
        return self._model_runs_repository().get_unscored_current_prediction_runs(
            photo_id,
        )

    def get_match_scores_for_photo(self, photo_id):
        """Return match-score rows for every detection on one photo.

        Rows are returned for all detections regardless of detector threshold:
        the caller decides what to show, and a detection hidden by the current
        threshold is often exactly the one a user is asking about.

        Every run is returned, including ones superseded by a later
        re-classification against a different label list — the Pipeline
        Inspector's per-run table deliberately shows the history. Each row is
        stamped ``is_current`` so the user-facing verdict can be built from the
        same label set as the predictions on screen: re-running a detection
        against a second list leaves the first list's row in this table, and a
        strong match from an abandoned list must not be allowed to certify the
        weak list that replaced it.

        ``is_current`` follows ``get_predictions``: the latest
        ``labels_fingerprint`` per ``(detection_id, classifier_model)`` as the
        predictions table orders it. A run that produced no prediction at all
        has no row to pin against — and that run is the single most important
        one here — so it falls back to the most recent match-score row for the
        same pair.

        Not workspace-scoped — ``photo_id`` is assumed already verified by the
        caller, as the existing per-photo routes do before reaching here.
        """
        return self._model_runs_repository().get_match_scores_for_photo(photo_id)

    def get_classifier_run_keys(self, detection_id, runtime_fingerprint=None):
        return self._model_runs_repository().get_classifier_run_keys(
            detection_id,
            runtime_fingerprint,
        )

    def get_classifier_run_key_gate(self, detection_id, runtime_fingerprint):
        """Return ``(accepted, rejected)`` classifier-run key sets for a detection.

        These gates serve normal-image runs. A RAW recipe always requires
        fresh normal inference, even when a manual decision pins its species.

        ``accepted`` mirrors what ``get_classifier_run_keys(detection_id,
        runtime_fingerprint=runtime_fingerprint)`` returns — keys whose row
        the runtime cache gate would honor for this detection.

        ``rejected`` are keys that DO have a classifier_runs row for the
        detection but whose row would be filtered out by the fingerprint
        rule (fingerprint mismatch, not ``'legacy'``, and no per-
        prediction ``prediction_review`` override marks them as still
        valid). The pipeline uses this set to reconcile the cache-hit
        preflight — ``count_classifier_runs`` counts every existing row
        regardless of runtime_fingerprint, so a photo whose only row is
        rejected here would otherwise sit in ``cached_est`` yet never
        register as a cache hit or as a fall-through miss, leaving
        ``_classification_eta_progress`` believing a phantom future cache
        hit is still coming.

        ``runtime_fingerprint`` must be provided; passing ``None`` would
        make every row look mismatched, which is not a useful signal
        (that's the reclassify path, where the gate is bypassed anyway).
        """
        return self._model_runs_repository().get_classifier_run_key_gate(
            detection_id,
            runtime_fingerprint,
        )

    def get_classifier_run_cache_hits(
        self,
        photo_ids,
        classifier_model,
        labels_fingerprint,
        *,
        contextual_weak_photo_ids=None,
        weak_confidence=None,
        fresh_detections_by_photo=None,
        fresh_processed_photo_ids=None,
        expected_classifier_runtime_by_detector_runtime=None,
    ):
        """Return the SET of photo IDs from ``photo_ids`` that the classify
        preflight would consider fully cached under
        (classifier_model, labels_fingerprint).

        Used by the streaming pipeline's classify stage to pre-flight how
        many photos will hit the cache vs. require fresh inference — and,
        because ``_classification_eta_progress`` reconciles the estimate
        from observed misses, also to know WHICH photos the preflight
        expected to be cached. Only observed misses on preflight-expected
        photos deflate the projected cache hits; misses on photos the
        preflight never counted are unrelated to the estimate and must
        not scale it down (Codex #1468 P2).

        Photo-scoped to match runtime accounting: the classify loop keeps
        its ``cached`` bucket photo-scoped (see pipeline_job.py — a photo
        lands there only if every processed detection is a cache hit; the
        moment any detection runs fresh inference it is promoted to
        ``count``). So the preflight only counts a photo as cached when
        NO qualifying detection would need fresh inference — otherwise a
        multi-subject photo with one cached and one uncached detection
        would inflate ``cached_estimate`` up to ``total`` and the banner
        would misread as "no work to classify" when there is real work
        remaining.

        Above-threshold animal detections must all have matching run keys.
        Photos with no usable animal target and no confident non-animal box
        count through their synthetic full-image anchor, including photos
        whose only animal detections are below the workspace floor.

        ``contextual_weak_photo_ids`` marks photos the classify loop will
        rescue with a lowered ``weak_confidence`` floor. For those photos
        the runtime picks one qualifying weak-threshold detection to
        classify, so the preflight counts the photo when at least one of
        its weak-threshold animal detections carries a matching run key.
        Without this, a rerun over a cached weak tail is scored as pure
        uncached work and the ETA can substantially overstate the time
        remaining (Codex #1468 P2).

        When the detector stage ran, ``fresh_detections_by_photo`` and
        ``fresh_processed_photo_ids`` scope processed photos to the exact
        in-memory candidates the classify runtime will consume. This avoids
        stale rows from another detector either adding uncached work or hiding
        a fresh uncached target. Photos the detector failed to process retain
        the DB fallback, matching the classify loop.

        ``expected_classifier_runtime_by_detector_runtime`` maps each
        ``detections.runtime_fingerprint`` value present in this batch to the
        classifier_runtime_fingerprint the runtime gate will accept for
        classifier_runs rows anchored on that detection. When provided, the
        preflight matches the runtime's per-detection gate: a
        ``classifier_runs`` row counts only when its ``runtime_fingerprint``
        equals the expected value for its detection's detector runtime, or is
        the ``'legacy'`` sentinel, or the prediction carries a real
        prediction_review override. Without this predicate the preflight
        overcounts obsolete-runtime rows the runtime will reject and the
        observation-based correction cannot deflate the estimate until those
        rejected rows are actually visited — leading
        ``_classification_eta_progress`` to report "finishing…" while
        substantial inference work still remains (Codex #1468 P2). Passing
        ``None`` retains the pre-Codex behaviour (no runtime filter); this
        is the correct choice on reclassify runs, where the runtime gate is
        bypassed anyway. Values may be ``None`` when the caller cannot
        compute an expected fingerprint (e.g. portable identity not wired
        up): matching detections then skip the runtime filter, mirroring
        the unfiltered ``get_classifier_run_keys`` fallback the runtime
        applies.
        """
        if not photo_ids:
            return set()
        import config as cfg
        min_conf = self.get_effective_config(cfg.load()).get(
            "detector_confidence", 0.2,
        )
        return self._model_runs_repository().get_classifier_run_cache_hits(
            photo_ids,
            classifier_model,
            labels_fingerprint,
            min_conf=min_conf,
            contextual_weak_photo_ids=contextual_weak_photo_ids,
            weak_confidence=weak_confidence,
            fresh_detections_by_photo=fresh_detections_by_photo,
            fresh_processed_photo_ids=fresh_processed_photo_ids,
            expected_classifier_runtime_by_detector_runtime=expected_classifier_runtime_by_detector_runtime,
        )

    def count_classifier_runs(
        self,
        photo_ids,
        classifier_model,
        labels_fingerprint,
        *,
        contextual_weak_photo_ids=None,
        weak_confidence=None,
        fresh_detections_by_photo=None,
        fresh_processed_photo_ids=None,
        expected_classifier_runtime_by_detector_runtime=None,
    ):
        """Return ``len(get_classifier_run_cache_hits(...))``.

        Retained as a thin count-only shim so callers that don't need the
        photo-id set (or preexisting tests) keep working; the id set is
        the newer surface the pipeline uses to scope its overcount
        tracker.
        """
        return len(self.get_classifier_run_cache_hits(
            photo_ids,
            classifier_model,
            labels_fingerprint,
            contextual_weak_photo_ids=contextual_weak_photo_ids,
            weak_confidence=weak_confidence,
            fresh_detections_by_photo=fresh_detections_by_photo,
            fresh_processed_photo_ids=fresh_processed_photo_ids,
            expected_classifier_runtime_by_detector_runtime=(
                expected_classifier_runtime_by_detector_runtime
            ),
        ))

    def get_unclassifiable_photos(
        self,
        photo_ids,
        *,
        contextual_weak_photo_ids=None,
        weak_confidence=None,
        fresh_detections_by_photo=None,
        fresh_processed_photo_ids=None,
    ):
        """Return photo IDs the classify runtime skips without inference.

        Photos with no usable animal crop normally receive full-image
        classification. The remaining deterministic skip is a photo with no
        eligible animal target but a confident non-animal (person/vehicle)
        detection. Excluding that tail keeps it out of the ETA's projected
        inference work.

        ``fresh_detections_by_photo`` is an in-memory ``{photo_id:
        [detection_dict, ...]}`` map (the ``detect_state["detections"]``
        that ``_detect_batch`` populated). When provided, the "any
        animal detection >= floor?" predicate is answered from this
        fresh set instead of the DB — mirroring what the runtime's
        ``photo_dets`` filter actually sees. This matters whenever the
        current detector pass replaces the runtime candidates while older
        rows from another detector model remain in the DB. Without this
        override, stale rows can make the preflight choose a different crop
        or skip outcome than runtime (Codex #1468 P2).
        Each detection dict must expose ``confidence`` (or
        ``detector_confidence``) and ``category`` (defaulting to
        ``"animal"`` when absent, matching ``_detect_batch``'s output).

        ``fresh_processed_photo_ids`` is the ``detect_state["processed_ids"]``
        set — the photos ``_detect_batch`` actually completed. When a
        MegaDetector pass raises for a single image, ``_detect_batch``
        leaves that photo out of both ``detections`` and
        ``processed_ids`` and the runtime's per-photo classify body
        falls back to ``db.get_detections()`` (see
        ``pipeline_job.py`` ``photo_dets`` else branch). Treating a
        missing entry in ``fresh_detections_by_photo`` as "no fresh
        animal above the floor" would mark such a photo unclassifiable
        even though runtime will infer, subtracting real work from the
        ETA's ``remaining_uncached`` and reporting "finishing…"
        prematurely. When both this set and ``fresh_detections_by_photo``
        are provided, photos NOT in the processed set defer to the DB
        predicate for that photo, matching what the runtime actually
        sees (Codex #1468 P2).
        """
        if not photo_ids:
            return set()
        import config as cfg
        min_conf = self.get_effective_config(cfg.load()).get(
            "detector_confidence", 0.2,
        )
        return self._model_runs_repository().get_unclassifiable_photos(
            photo_ids,
            min_conf=min_conf,
            contextual_weak_photo_ids=contextual_weak_photo_ids,
            weak_confidence=weak_confidence,
            fresh_detections_by_photo=fresh_detections_by_photo,
            fresh_processed_photo_ids=fresh_processed_photo_ids,
        )

    def get_labels_fingerprints(self):
        """Return all rows from the labels_fingerprints sidecar.

        Each row records the (fingerprint, sources, label_count) triple a
        classify run wrote — single-file runs list one source, merged-set
        runs list several. Used by the inventory endpoint to identify
        merged fingerprints that are still current (sources on disk and
        unchanged) so they don't get marked stale.
        """
        return self._model_runs_repository().get_labels_fingerprints()

    def upsert_labels_fingerprint(
        self,
        fingerprint,
        display_name,
        sources,
        label_count,
        full_fingerprint=None,
    ):
        self._model_runs_repository().upsert_labels_fingerprint(
            fingerprint,
            display_name,
            sources,
            label_count,
            full_fingerprint,
        )

    def get_review_status(self, prediction_id, workspace_id):
        return self._prediction_repository().get_review_status(prediction_id, workspace_id)

    def set_review_status(self, prediction_id, workspace_id, status,
                           individual=None, group_id=None):
        return self._prediction_repository().set_review_status(
            prediction_id, workspace_id, status, individual=individual, group_id=group_id,
        )

    def _detections_repository(self):
        """Build the detections repository on this connection.

        ``detections`` is catalog-wide, so the repository takes no workspace
        id; the misses wrappers resolve ``_ws_id()`` and the scope clause and
        pass them in, and the readers resolve their confidence floors here.
        """
        from repositories.detections import DetectionsRepository

        return DetectionsRepository(
            self.conn,
            chunk_size=_SQLITE_PARAM_CHUNK_SIZE,
            commit_with_retry=commit_with_retry,
        )

    def save_detections(
        self,
        photo_id,
        detections,
        detector_model,
        runtime_fingerprint="legacy",
    ):
        """Replace all detections for (photo_id, detector_model) with the given list.

        Global: no workspace scoping. The model's output is a pure function of
        (photo, model); any workspace re-running the same (photo, model) is a
        bug — callers should short-circuit via `get_detector_run_photo_ids`.

        IDs are content-addressed (see vireo.detection_id) so two pipelines
        writing the same (photo, model, detections) produce identical rows
        — the second writer's UPSERT is a no-op rather than a CASCADE-deleting
        DELETE+INSERT.

        Args:
            photo_id: the photo
            detections: list of dicts {box: {x,y,w,h}, confidence, category}
            detector_model: required, e.g. "megadetector-v6"
        Returns:
            list of detection IDs (empty if detections was empty).
        """
        if detector_model is None:
            raise ValueError("detector_model is required")
        return self._detections_repository().save(
            photo_id, detections, detector_model, runtime_fingerprint,
        )

    def _upsert_detection_rows(
        self,
        photo_id,
        detector_model,
        detections,
        runtime_fingerprint="legacy",
    ):
        """Content-addressed UPSERT of detection rows for one (photo, model).

        Returns the list of unique IDs in first-seen order. Does NOT commit —
        the caller controls the transaction so the detector_runs row can be
        written in the same commit (see `write_detection_batch`).
        """
        return self._detections_repository().upsert_rows(
            photo_id, detector_model, detections, runtime_fingerprint,
        )

    def write_detection_batch(
        self,
        photo_id,
        detector_model,
        detections,
        runtime_fingerprint="legacy",
        input_fingerprint=None,
        force_runtime_replace=False,
    ):
        """Atomically replace detections and record the detector_runs row.

        Combines `save_detections` and `record_detector_run` under a single
        transaction so readers never observe a torn state where one table
        reflects the new run and the other still reflects the old one.
        Callers in the detection write path (e.g. `_detect_batch`) should
        prefer this over invoking the two methods separately.

        Args:
            photo_id: the photo
            detector_model: required, e.g. "megadetector-v6"
            detections: list of dicts {box: {x,y,w,h}, confidence, category}.
                An empty list records an empty-scene run (box_count=0) and
                clears any prior detection rows for the same (photo, model).
        Returns:
            list of new detection IDs (empty if detections was empty).
        """
        if detector_model is None:
            raise ValueError("detector_model is required")
        # The pin check stays on the façade (model-runs domain), so a patched
        # ``detector_run_is_pinned`` still applies inside the transaction.
        return self._detections_repository().write_batch(
            photo_id,
            detector_model,
            detections,
            runtime_fingerprint,
            input_fingerprint,
            force_runtime_replace,
            is_pinned=self.detector_run_is_pinned,
        )

    def get_detections(self, photo_id, min_conf=None, detector_model=None):
        """Return all boxes for a photo above `min_conf`, globally.

        The detections table is global (no workspace_id). Threshold filtering
        happens at read time so raw boxes stay cached across workspaces.

        Args:
            photo_id: the photo
            min_conf: confidence floor. ``None`` pulls ``detector_confidence``
                from the active workspace's effective config (default 0.2).
                ``0`` returns raw rows with no filtering.
            detector_model: optional — filter to a single detector model.
        """
        if min_conf is None:
            import config as cfg
            effective = self.get_effective_config(cfg.load())
            min_conf = effective.get("detector_confidence", 0.2)
        return self._detections_repository().get(photo_id, min_conf, detector_model)

    def get_detections_for_photos(self, photo_ids, min_conf=None,
                                  detector_model=None):
        """Return {photo_id: [det_dict, ...]} for a batch of photos.

        Each det_dict has keys: id, x, y, w, h, confidence, category, and
        detector_model. Lists put the chosen primary first, then quality and
        detector confidence. The detections
        table is global — threshold filtering happens at read time. Photos
        with no detections above ``min_conf`` are omitted from the result.

        Args:
            photo_ids: iterable of photo ids
            min_conf: confidence floor. ``None`` resolves to the active
                workspace's effective ``detector_confidence`` (default 0.2).
                ``0`` returns raw rows.
            detector_model: optional — filter to a single detector model.
        """
        if not photo_ids:
            return {}
        if min_conf is None:
            import config as cfg
            effective = self.get_effective_config(cfg.load())
            min_conf = effective.get("detector_confidence", 0.2)
        return self._detections_repository().get_for_photos(
            photo_ids, min_conf, detector_model,
        )

    def get_predictions_for_detection(self, detection_id,
                                        min_classifier_conf=None,
                                        classifier_model=None,
                                        labels_fingerprint=None):
        """Return cached classifier predictions for a single detection.

        Reads the global ``predictions`` table. Review status (accepted /
        rejected / pending) lives in ``prediction_review`` and is joined in
        by callers that need workspace-scoped review state.

        Args:
            detection_id: the detection whose predictions to fetch.
            min_classifier_conf: confidence floor. ``None`` resolves to the
                active workspace's effective ``classifier_confidence`` (0.0
                if unset). ``0`` returns all rows.
            classifier_model: optional — filter to a single classifier model.
            labels_fingerprint: optional — filter to predictions produced
                against a specific label set.
        """
        if min_classifier_conf is None:
            import config as cfg
            effective = self.get_effective_config(cfg.load())
            min_classifier_conf = effective.get("classifier_confidence", 0.0)
        return self._detections_repository().get_predictions(
            detection_id, min_classifier_conf, classifier_model, labels_fingerprint,
        )

    def clear_detections(self, photo_id, detector_model=None):
        """Remove detections (and cascaded predictions) for a photo.

        Also clears the matching ``detector_runs`` rows so a subsequent
        non-reclassify pass actually re-runs MegaDetector. Without this,
        a reclassify that clears detections but leaves the run key behind
        (e.g. because model init then failed) would cause future runs to
        skip detection forever — the gate in ``_detect_subjects`` treats
        any ``detector_runs`` entry as authoritative.

        Global: no workspace scoping. If `detector_model` is None, all
        detector models for this photo are cleared; otherwise only the
        rows for that model.
        """
        self._detections_repository().clear(photo_id, detector_model)

    def get_existing_detection_photo_ids(self, detector_model="megadetector-v6"):
        """Back-compat shim — prefer get_detector_run_photo_ids."""
        return self.get_detector_run_photo_ids(detector_model)

    def list_misses(self, category=None, since=None, photo_ids=None):
        """Return photos flagged as misses in the active workspace.

        category: None | "no_subject" | "clipped" | "oof"
        since: optional ISO timestamp; if set, restricts to photos whose
            miss_computed_at >= since. Used by the pipeline-review step to
            scope results to the current run.
        photo_ids: optional iterable restricting results to an already-resolved
            collection or filter scope. An empty iterable matches no photos.

        Excludes photos already flagged as rejected. Scoped to folders
        linked to the active workspace. ``detection_box`` and
        ``detection_conf`` are sourced from the selected primary
        row in the ``detections`` table — the legacy ``photos`` columns are
        not populated by normal pipeline runs. ``raw_detection_conf`` exposes
        the best animal candidate even when it is below the effective detector
        floor so the Misses UI can explain the decision. Ordered by timestamp
        DESC.
        """
        ws_id = self._ws_id()
        repo = self._detections_repository()
        where = repo.miss_where(category)
        scope_clause, scope_params = self._scope_clause(photo_ids)
        photos = repo.list_miss_photos(ws_id, where, since, scope_clause, scope_params)
        if not photos:
            return photos

        import config as cfg
        min_conf = self.get_effective_config(cfg.load()).get(
            "detector_confidence", 0.2
        )
        return repo.attach_miss_detections(photos, min_conf)

    def clear_miss_flag(self, photo_id, category):
        """Set the given miss column to 0 on the given photo.

        Raises ValueError if the photo is not in the active workspace, so
        that `/api/misses/<id>/unflag` can't touch another workspace's photos.
        """
        self._verify_photo_in_workspace(photo_id)
        self._detections_repository().clear_miss_flag(photo_id, category)

    def bulk_reject_miss_category(self, category, since=None, photo_ids=None):
        """Set flag='rejected' on every photo flagged with that miss category
        in the active workspace and not already rejected.

        ``since`` mirrors the filter on ``list_misses``: when set, only
        photos whose ``miss_computed_at >= since`` are rejected. This
        keeps bulk reject scoped to the /misses view the user is looking
        at (e.g. the current pipeline run), so older misses not shown on
        screen aren't silently rejected.

        ``photo_ids`` further restricts the mutation to the collection and
        Browse-style filters currently visible on the Misses page.

        Returns a list of ``{"photo_id": int, "old_value": str}`` for each
        photo whose flag was changed. The caller (``/api/misses/reject``)
        uses this to write an ``edit_history`` entry so the bulk change is
        undoable/auditable like the other batch flag routes; without it,
        an accidental "Reject all" on /misses would be invisible to the
        undo flow.
        """
        repo = self._detections_repository()
        col = repo.miss_column(category)
        ws_id = self._ws_id()
        scope_clause, scope_params = self._scope_clause(photo_ids)
        return repo.reject_misses(col, ws_id, since, scope_clause, scope_params)

    def get_detection_ids_for_photos(self, photo_ids):
        """Return {photo_id: set(detection_id, ...)} for the given photo IDs.

        The detections table is global (no workspace_id). Used to snapshot
        pre-run detection IDs so that a reclassify pass can delete only the
        *stale* rows after fresh ones have been inserted, avoiding the
        cascade-delete that would destroy other-model predictions.

        No threshold filter: the caller needs to see every existing row,
        including low-confidence ones, so they can all be cleaned up.

        IDs are queried in chunks of at most 900 to stay safely under
        SQLite's default bound-parameter limit (SQLITE_LIMIT_VARIABLE_NUMBER,
        typically 999 in production builds).
        """
        return self._detections_repository().get_ids_for_photos(photo_ids)

    def delete_detections_by_ids(self, detection_ids):
        """Delete specific detection rows by primary key.

        Cascades to predictions via the FK constraint.  Does nothing if
        the list is empty.  Used by reclassify to purge only the stale
        rows for photos that have just been re-detected, without touching
        detection rows that belong to models not included in the current run.

        IDs are deleted in chunks of at most 900 to stay safely under
        SQLite's default bound-parameter limit (SQLITE_LIMIT_VARIABLE_NUMBER,
        typically 999 in production builds).
        """
        if not detection_ids:
            return
        repo = self._detections_repository()
        affected_subject_photos = repo.delete_by_ids(detection_ids)
        if affected_subject_photos:
            from subjects import sync_primary
            for photo_id in affected_subject_photos:
                sync_primary(self, photo_id)
        repo.commit()

    # -- Pending Changes --

    def _sync_repository(self):
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

    def queue_change(self, photo_id, change_type, value, workspace_id=None, _commit=True):
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

    def get_pending_changes(self):
        """Return all pending changes ordered by creation time."""
        return self._sync_repository().list_all()

    def claim_pending_changes_for_sync(self, changes):
        """Mark selected edits as possibly written and return surviving rows.

        A cancelled keyword must leave an opposing edit once a writer can
        have seen it. Persist that fact before any filesystem work, including
        across failed writes or a process restart. Match immutable tokens so
        a cancellation between selection and this claim cannot resurrect the
        old edit or claim a replacement that reused its rowid.
        """
        if not changes:
            return []
        return self._sync_repository().claim_for_sync(changes)

    def get_pending_keyword_removal_keys(self, photo_id, hierarchical=False):
        """Return normalized keyword keys awaiting removal for a photo.

        Reads across workspaces because photo metadata is global even though
        the sync queue is presented per workspace. ``keyword_remove_flat``
        suppresses flat XMP re-imports only; callers processing hierarchical
        entries request ``hierarchical=True`` and receive full removals only.
        """
        return self._sync_repository().keyword_removal_keys(
            photo_id, hierarchical=hierarchical,
        )

    def _pending_keyword_sidecar_alias(self, photo_id, workspace_id, value):
        """Return whether another queued keyword edit reaches this sidecar."""
        return self._sync_repository().keyword_sidecar_alias(
            photo_id, workspace_id, value,
        )

    def remove_pending_changes(self, photo_id, change_type=None, value=None, workspace_id=None, _commit=True):
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
                row["sync_started"] or self._pending_keyword_sidecar_alias(photo_id, ws_id, row["value"])
            ):
                kind = inverse[row["change_type"]]
                self.queue_change(photo_id, kind, row["value"], workspace_id=ws_id, _commit=False)
                repo.mark_sync_started(photo_id, ws_id, kind, row["value"])
        if _commit:
            repo.commit()
        return len(removed)

    def remove_pending_change_token(self, change_token):
        """Delete a single pending change by immutable token. Returns rows removed."""
        if not change_token:
            return 0
        return self._sync_repository().remove_token(change_token)

    def clear_pending(
        self, change_ids, *, clear_equivalent_flat_removals=False,
        expected_tokens=None,
    ):
        """Delete pending changes by id.

        When ``clear_equivalent_flat_removals`` is true, a successfully
        applied flat keyword removal also clears equivalent rows from sibling
        workspaces. The sidecar is global to the photo even though the review
        queue is workspace-scoped; leaving migration-generated duplicates in
        sibling queues would let a later sync replay a stale removal after the
        user had re-added the keyword.

        When ``expected_tokens`` is supplied (a list the same length as
        ``change_ids``), each delete is conditioned on the row's
        ``change_token`` matching too. That protects a concurrent sync from
        clobbering a replacement queued mid-run: the edit route deletes the
        selected pending row inside one transaction and immediately re-inserts,
        and SQLite reissues the freshly-freed rowid to the new row. A
        clear-by-id would drop that replacement without it ever being written
        to XMP -- for the pre-transfer sync, the stale sidecar would then
        travel to the NAS and the local original be removed. Rows predating
        the ``change_token`` column have a NULL token with no backfill, and
        for them the caller passes ``None`` in ``expected_tokens``; those fall
        back to id-only clearing scoped to null-token rows, so they keep
        exactly the exposure they always had.
        """
        if not change_ids:
            return
        repo = self._sync_repository()
        synced_changes = repo.delete_by_ids(
            change_ids,
            clear_equivalent_flat_removals=clear_equivalent_flat_removals,
            expected_tokens=expected_tokens,
        )
        if synced_changes:
            self.clear_equivalent_flat_removals(synced_changes, _commit=False)
        repo.commit()

    def clear_pending_by_token(
        self, change_tokens, *, clear_equivalent_flat_removals=False,
    ):
        """Delete pending changes named by their immutable tokens.

        ``pending_changes.id`` is a bare ``INTEGER PRIMARY KEY``, so SQLite
        re-issues a deleted row's rowid to the next insert into the table. A
        caller that selected rows a while ago -- the sidecar sync, whose
        writes take as long as the storage does -- can find those ids now
        naming a *replacement* row: ``queue_flag_change_if_enabled`` deletes
        the old flag and inserts the new value, so clearing by id would throw
        away the edit the user just made, unwritten. A token is a fresh uuid
        per insert, so it names the row that was actually written.

        ``clear_equivalent_flat_removals`` behaves as in :meth:`clear_pending`.
        """
        if not change_tokens:
            return
        repo = self._sync_repository()
        synced_changes = repo.delete_by_tokens(
            change_tokens,
            clear_equivalent_flat_removals=clear_equivalent_flat_removals,
        )
        if synced_changes:
            self.clear_equivalent_flat_removals(synced_changes, _commit=False)
        repo.commit()

    def clear_equivalent_flat_removals(self, changes, _commit=True):
        """Clear shared-sidecar flat removals represented by ``changes``."""
        self._sync_repository().clear_equivalent_flat_removals(changes, _commit=_commit)

    def queue_flag_change_if_enabled(self, photo_id, flag, workspace_id=None, _commit=True):
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
            import config as cfg

            enabled = bool(
                self.get_effective_config(cfg.load()).get("sync_flags_to_xmp", False)
            )
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

    def _edit_history_repository(self, *, scoped=True):
        """Build the edit-history repository on this connection.

        ``scoped=True`` binds it to the active workspace (raising
        ``RuntimeError`` when none is set); id-keyed helpers pass
        ``scoped=False``.
        """
        from repositories.edit_history import EditHistoryRepository

        return EditHistoryRepository(
            self.conn,
            self._ws_id() if scoped else None,
        )

    def record_edit(self, action_type, description, new_value, items, is_batch=False, _commit=True):
        """Record an edit action with per-photo before/after values.

        Clears the redo stack (any undone entries) since a new action invalidates them.

        Args:
            _commit: If False, skip the internal commit and the history prune
                     (caller is responsible for committing the transaction;
                     prune can be run later).
        """
        edit_id = self._edit_history_repository().record(
            action_type, description, new_value, items,
            is_batch=is_batch, _commit=_commit,
        )
        if _commit:
            self._prune_edit_history()
        return edit_id

    def get_edit_history(self, limit=50, offset=0):
        """Return recent edit history entries (most recent first) with item counts."""
        return self._edit_history_repository().list_recent(limit, offset)

    # Action types that appear in history but cannot be reversed
    _NON_UNDOABLE = (
        'prediction_reject', 'discard',
        # Location edits (set/clear/link) are auditable but not undoable
        # in v1 — _apply_undo has no handlers for them, so including them
        # would silently advance the undo cursor without reverting state.
        # Adding undo support is a follow-up if it becomes important.
        'location_set', 'location_clear', 'location_link', 'location_gps_review',
        # Compare-page review actions are auditable but not undoable in v1
        # for the same reason: _apply_undo/_apply_redo have no handlers, so
        # leaving them undoable would mark the entry undone without
        # restoring the prediction status or the replaced species keywords.
        'prediction_reviewed', 'prediction_replace_species',
    )

    def undo_last_edit(self):
        """Undo the most recent undoable edit. Returns the undone entry dict, or None.

        Non-undoable entries (prediction_reject, discard) are skipped.
        The entry is marked as undone (not deleted) so it can be redone.

        Stale cache-linked actions are retired and reported without applying
        another entry. The caller must refresh the controls so the next click
        names the edit it will actually reverse. Retirement commits on its own;
        valid combined photo/group edits retain the writer lock until complete.
        """
        found = self._edit_history_repository().next_undo(self._NON_UNDOABLE)
        if not found:
            return None
        entry, items = found
        history = self._edit_history_repository(scoped=False)

        if entry['action_type'] == 'pipeline_grouping':
            from services.grouping_history import (
                GroupingHistoryStale,
                apply_grouping_photo_edit,
                restore_grouping_edit,
            )

            try:
                with restore_grouping_edit(self, entry, undo=True):
                    apply_grouping_photo_edit(self, entry, items, undo=True)
                    history.mark_undone(entry['id'])
            except GroupingHistoryStale:
                self._retire_stale_grouping_entry(entry['id'])
                history.commit()
                raise GroupingHistoryStale(
                    'That grouping or species action was superseded by newer analysis. '
                    'History has been refreshed; review the next action before trying again.'
                ) from None
        elif entry['action_type'] == 'species_confirm_cache':
            from services.grouping_history import (
                GroupingHistoryStale,
                restore_species_confirm_cache_edit,
            )

            try:
                with restore_species_confirm_cache_edit(self, entry, undo=True):
                    history.mark_undone(entry['id'])
            except GroupingHistoryStale:
                self._retire_stale_grouping_entry(entry['id'])
                history.commit()
                raise GroupingHistoryStale(
                    'That grouping or species action was superseded by newer analysis. '
                    'History has been refreshed; review the next action before trying again.'
                ) from None
        else:
            with self._commits_held():
                self._apply_undo(entry, items)
                history.mark_undone(entry['id'])
        return entry

    def redo_last_undo(self):
        """Redo the most recently undone edit. Returns the entry dict, or None.

        Replays in chronological order (ASC) so sequential undos are redone
        correctly. Stale cache-linked actions are retired and reported without
        replaying another entry, just as in ``undo_last_edit``.
        """
        found = self._edit_history_repository().next_redo(self._NON_UNDOABLE)
        if not found:
            return None
        entry, items = found
        history = self._edit_history_repository(scoped=False)

        if entry['action_type'] == 'pipeline_grouping':
            from services.grouping_history import (
                GroupingHistoryStale,
                apply_grouping_photo_edit,
                restore_grouping_edit,
            )

            try:
                with restore_grouping_edit(self, entry, undo=False):
                    apply_grouping_photo_edit(self, entry, items, undo=False)
                    history.mark_redone(entry['id'])
            except GroupingHistoryStale:
                self._retire_stale_grouping_entry(entry['id'])
                history.commit()
                raise GroupingHistoryStale(
                    'That grouping or species action was superseded by newer analysis. '
                    'History has been refreshed; review the next action before trying again.'
                ) from None
        elif entry['action_type'] == 'species_confirm_cache':
            from services.grouping_history import (
                GroupingHistoryStale,
                restore_species_confirm_cache_edit,
            )

            try:
                with restore_species_confirm_cache_edit(self, entry, undo=False):
                    history.mark_redone(entry['id'])
            except GroupingHistoryStale:
                self._retire_stale_grouping_entry(entry['id'])
                history.commit()
                raise GroupingHistoryStale(
                    'That grouping or species action was superseded by newer analysis. '
                    'History has been refreshed; review the next action before trying again.'
                ) from None
        else:
            with self._commits_held():
                self._apply_redo(entry, items)
                history.mark_redone(entry['id'])
        return entry

    @contextlib.contextmanager
    def _commits_held(self):
        """Run an undo/redo replay as one transaction.

        The replay's setters each commit, which would end the caller's
        ``BEGIN IMMEDIATE`` (``api_undo`` / ``api_redo`` hold the
        prediction-decision lock) after the first write and leave the rest of
        the replay racing other decisions. Holding the connection's commits
        turns those into no-ops; the whole replay then commits once here, or
        rolls back if any step raises, so a failure part-way cannot leave half
        an edit reversed. A connection that cannot hold commits (a test's
        wrapper) runs the replay unheld, as before.
        """
        conn = self.conn
        if not isinstance(conn, _Connection):
            yield
            return
        conn._commit_holds += 1
        try:
            yield
        except BaseException:
            conn._commit_holds -= 1
            if not conn._commit_holds:
                conn.rollback()
            raise
        conn._commit_holds -= 1
        if not conn._commit_holds:
            conn.commit()

    def _retire_stale_grouping_entry(self, entry_id):
        """Retire stale cache state while retaining any reversible photo edit.

        The caller commits this retirement and reports it to the user before
        another action can run. This helper never applies a photo change.
        """
        self._edit_history_repository(scoped=False).retire_stale_grouping_entry(entry_id)

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
        for item in items:
            if setter is not None:
                setter(self, item['photo_id'], item[value_key])
            elif handler is not None:
                handler(self, entry, item)

    # -- plain per-photo fields (same setter for undo and redo) ----------

    def _edit_set_flag(self, pid, value):
        # Edit history is already workspace-scoped; skip re-verification
        self.update_photo_flag(pid, value, verify_workspace=False)
        self.queue_flag_change_if_enabled(pid, value)

    def _edit_set_wildlife_excluded(self, pid, value):
        self.update_photo_wildlife_excluded(
            pid, value == "1", verify_workspace=False
        )

    def _edit_set_color_label(self, pid, value):
        if value:
            self.set_color_label(pid, value)
        else:
            self.remove_color_label(pid)

    def _edit_set_edit_recipe(self, pid, value):
        self.set_photo_edit_recipe(
            pid, value if value else None, verify_workspace=False,
        )

    # -- rating -----------------------------------------------------------

    def _undo_rating(self, entry, item):
        pid, old_val = item['photo_id'], item['old_value']
        self.update_photo_rating(pid, int(old_val), verify_workspace=False)
        if old_val != entry['new_value']:
            self.remove_pending_changes(pid, 'rating', entry['new_value'])
            self.queue_change(pid, 'rating', old_val)

    def _redo_rating(self, entry, item):
        pid, old_val, new_val = item['photo_id'], item['old_value'], item['new_value']
        self.update_photo_rating(
            pid, int(new_val) if new_val else 0, verify_workspace=False,
        )
        if old_val != new_val:
            self.remove_pending_changes(pid, 'rating', old_val)
            self.queue_change(pid, 'rating', new_val)

    # -- keyword helpers shared by the keyword / species handlers ----------

    def _keyword_name(self, keyword_id):
        return self._edit_history_repository(scoped=False).keyword_name(keyword_id)

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
        name = self._keyword_name(keyword_id)
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
        name = self._keyword_name(keyword_id)
        if name:
            self._flip_pending_keyword_change(
                pid, name, 'keyword_add', 'keyword_remove',
            )
        return name

    def _prediction_scope(self, pred_id):
        """``(detection_id, classifier_model, labels_fingerprint)`` or None."""
        return self._edit_history_repository(scoped=False).prediction_scope(pred_id)

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
        skip_tag = action == 'prediction_accept' and old_meta.get('no_tag')
        raw_kid = (item['new_value'] if action == 'prediction_accept' else None) or entry['new_value']
        # A no-tag accept (a species-less burst pick) records no keyword.
        kid = int(raw_kid) if raw_kid or not skip_tag else None
        kw_name = self._keyword_name(kid) if kid is not None else None
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
                    if self._edit_history_repository(scoped=False).workspace_exists(
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
                self._restore_relabel_curation(
                    entry['workspace_id'], pid, kw_name,
                    old_meta.get('curation'),
                )
        if action == 'prediction_accept' and old_val:
            self._undo_prediction_accept_statuses(old_meta, old_val)

    def _redo_keyword_add(self, entry, item):
        pid, old_val = item['photo_id'], item['old_value']
        action = entry['action_type']
        old_meta = self._edit_old_value_meta(old_val)
        skip_tag = action == 'prediction_accept' and old_meta.get('no_tag')
        raw_kid = (item['new_value'] if action == 'prediction_accept' else None) or entry['new_value']
        # A no-tag accept (a species-less burst pick) records no keyword.
        kid = int(raw_kid) if raw_kid or not skip_tag else None
        kw_name = self._keyword_name(kid) if kid is not None else None
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
                self._reapply_relabel_curation(
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
            pred_ids, self._prediction_scope,
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
        history = self._edit_history_repository()
        accepted_by_scope = {}
        for pred_id in pred_ids:
            scope = self._prediction_scope(pred_id)
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
            self._restore_relabel_curation(
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
            self._reapply_relabel_curation(
                entry['workspace_id'], pid, new_kw_name,
                old_meta.get('curation'),
            )
        self._reject_edit_prediction(old_meta)

    _EDIT_FIELD_SETTERS = {
        'flag': _edit_set_flag,
        'wildlife_excluded': _edit_set_wildlife_excluded,
        'color_label': _edit_set_color_label,
        'edit_recipe': _edit_set_edit_recipe,
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

    def _restore_relabel_curation(
        self, workspace_id, photo_id, new_species, curation,
    ):
        """Undo the curation migration performed by ``api_highlights_relabel``.

        For each ``species_highlights`` row the relabel moved from an old
        species bucket to ``new_species``, delete the row at ``new_species``
        and re-insert it at the end of the old bucket (unless the photo
        already appears there). For each ``photo_preferences`` row moved
        by the relabel, delete the row at ``(new_species, purpose)`` and
        re-insert it at ``(old_species, purpose)``. For each rep-only
        ``species_representatives`` row moved with no matching
        ``photo_preferences`` row, delete the row at ``new_species`` and
        re-insert it at ``old_species``. Best-effort: if the target row no
        longer exists (state has changed since the relabel), the
        corresponding restore is a no-op.
        """
        self._edit_history_repository(scoped=False).restore_relabel_curation(
            workspace_id, photo_id, new_species, curation,
            restore_species_representative=self._restore_species_representative,
        )

    def _reapply_relabel_curation(
        self, workspace_id, photo_id, new_species, curation,
    ):
        """Redo the curation migration reversed by
        :meth:`_restore_relabel_curation`. Moves rows from each recorded
        old species back onto ``new_species``.
        """
        self._edit_history_repository(scoped=False).reapply_relabel_curation(
            workspace_id, photo_id, new_species, curation,
            restore_species_representative=self._restore_species_representative,
        )

    def _edit_old_value_meta(self, old_value):
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

    def _prune_edit_history(self):
        """Delete oldest entries beyond the configured max (excludes undone entries awaiting redo)."""
        import config as cfg
        max_entries = cfg.get('max_edit_history') or 1000
        preserve_wildlife_discard = (
            self.get_meta(self._RETIRED_WILDLIFE_GENRE_KEY) != "1"
        )
        self._edit_history_repository().prune(max_entries, preserve_wildlife_discard)

    # -- Collections --

    def _collection_repository(self):
        """Build the collections repository on this connection.

        The repository gets ``self._ws_id`` as a resolver rather than an id,
        so the active workspace is read exactly where these methods always
        read it: rule validation still fails before a missing workspace does,
        and ``create_default_collections(workspace_id=...)`` still needs no
        active workspace. The façade methods the rules engine consults
        (``get_effective_config``, ``get_subject_types``,
        ``get_folder_subtree_ids``) are passed bound, so monkeypatches of
        them keep reaching the moved SQL, and the module helpers and
        constants it reads are passed in at call time for the same reason.
        """
        from repositories.collections import CollectionRepository

        return CollectionRepository(
            self.conn,
            self._ws_id,
            get_effective_config=self.get_effective_config,
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
            no_location_information_rules=NO_LOCATION_INFORMATION_RULES,
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

    def add_collection(self, name, rules_json, visual_json=None):
        """Insert a smart collection. Returns the collection id.

        ``visual_json`` stores the universal filter's visual clause
        (``{prompt, strength}`` JSON) when the saved expression has one, so
        save → reopen reproduces the same result set instead of silently
        dropping to metadata-only.
        """
        return self._collection_repository().add(
            name,
            rules_json,
            visual_json=visual_json,
        )

    def get_collections(self):
        """Return all collections for the active workspace."""
        return self._collection_repository().list_all()

    def delete_collection(self, collection_id):
        """Delete a collection."""
        self._collection_repository().delete(collection_id)

    def rename_collection(self, collection_id, new_name):
        """Rename a collection within the active workspace.

        Raises ``ValueError`` if the collection isn't in the active workspace.
        """
        self._collection_repository().rename(collection_id, new_name)

    def duplicate_collection(self, collection_id):
        """Copy a collection (name + rules + visual clause) within the active workspace.

        The new collection's name is ``"{original} (copy)"``; if that name is
        already taken, append an incrementing counter like ``"(copy 2)"``.
        Rules and ``visual_json`` are copied verbatim, which means static
        collections (photo_ids rules) keep their memberships and visual
        collections keep their visual clause.

        Returns the new collection id. Raises ``ValueError`` if the source
        collection isn't in the active workspace.
        """
        return self._collection_repository().duplicate(collection_id)

    def create_new_images_snapshot(self, file_paths):
        """Persist a snapshot of new-image file paths for the active workspace.

        Returns the new snapshot id. An empty path list is allowed — the caller
        decides how to handle zero-file snapshots (the pipeline short-circuits).
        """
        return self._workspace_repository().create_new_images_snapshot(file_paths)

    def get_new_images_snapshot(self, snapshot_id):
        """Return snapshot metadata + file paths, or None if not found / cross-workspace.

        Isolation: a snapshot created in workspace A is invisible when workspace B
        is active. Callers treat None as 'expired / gone'.

        An id outside SQLite's signed 64-bit range can't match any stored row,
        so we short-circuit to None rather than let parameter binding raise
        OverflowError (which would surface as a 500 to API callers).
        """
        if not -(1 << 63) <= snapshot_id <= (1 << 63) - 1:
            return None
        return self._workspace_repository().get_new_images_snapshot(snapshot_id)

    def _build_collection_query(self, collection_id, include_offline_folders=False):
        """Build SQL clauses from collection rules.

        Returns (folder_join, join_clause, where, params) or None if collection
        not found. Pass ``include_offline_folders=True`` for metadata-only
        callers (Dashboard scope) that keep offline photos in their totals.
        """
        return self._collection_repository()._build_collection_query(
            collection_id,
            include_offline_folders=include_offline_folders,
        )

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

    def _photo_sort_clause(self, sort):
        """Return ``(order_by_sql, params)`` for a photo-list sort key.

        One definition for every unstacked photo list — the legacy
        ``/api/photos`` reads, the universal-filter reads, the collection
        reads, and the position probes that must rank the same order those
        page through. They used to carry six copies of the same dict; a sort
        added to one of them and missed in another silently degrades to
        capture-date in whichever query the caller happened to hit.

        Unknown keys fall back to capture-date ascending, matching the
        previous ``.get(sort, _PHOTO_DATE_ASC_ORDER)`` behaviour.

        Callers must splice ``params`` in at the ORDER BY's *textual*
        position: after the WHERE parameters for an ordinary
        ``... WHERE ... ORDER BY ...`` query, but before them when the
        clause sits inside a ``ROW_NUMBER() OVER (ORDER BY ...)`` in the
        select list.
        """
        return self._collection_repository()._photo_sort_clause(sort)

    def get_collection_photos(
        self,
        collection_id,
        page=1,
        per_page=50,
        photo_ids=None,
        sort="date",
        include_offline_folders=False,
    ):
        """Build SQL from collection rules and return matching photos.

        ``photo_ids`` optionally narrows the collection query to a small set.
        ID Conflicts uses this after a decision so it can refresh only the
        photos the write touched instead of rebuilding the whole collection.
        The collection rules still apply, which also lets the client detect a
        photo that left the collection because its species keywords changed.
        """
        return self._collection_repository().get_photos(
            collection_id,
            page=page,
            per_page=per_page,
            photo_ids=photo_ids,
            sort=sort,
            include_offline_folders=include_offline_folders,
        )

    def get_collection_photo_ids(self, collection_id, sort="date"):
        """Return all photo IDs matching a collection in display order.

        ``sort`` mirrors ``get_collection_photos`` so the ``/photo-ids``
        endpoint that drives ``selectedPhotos`` insertion order for
        Select-all-matching stays aligned with the grid the user sees —
        without this, Best Batch seed, burst-review order, and export
        preview would start from the date-ordered first photo even when
        the grid is sorted by name, rating, sharpness, or quality.
        """
        return self._collection_repository().get_photo_ids(collection_id, sort=sort)

    def count_collection_photos(
        self, collection_id, include_offline_folders=False,
    ):
        """Return the count of photos matching collection rules.

        The default is the actionable count from accessible folders. Pass
        ``include_offline_folders=True`` for stable collection membership:
        photos remain members while their storage is temporarily missing.
        """
        return self._collection_repository().count_photos(
            collection_id,
            include_offline_folders=include_offline_folders,
        )

    def count_collection_photo_availability(self, collection_id):
        """Return stable membership and actionable counts for a collection."""
        return self._collection_repository().count_photo_availability(collection_id)

    def rules_resolvable(self, rules):
        """Return True if a rule tree can be resolved to SQL clauses without
        executing them.

        Callers (notably ``/api/browse/init``) use this to flag degraded
        collections at first paint without running the full
        ``COUNT(DISTINCT p.id)`` per collection — that N+1 is what the
        Browse client's async ``loadCollectionCounts()`` was built to
        avoid. Only the query-building step is exercised.
        """
        try:
            self._build_query_from_rules(rules)
        except ValueError:
            return False
        return True

    def count_photos_for_rules(
        self,
        rules,
        collection_id=None,
        folder_id=None,
        include_offline_folders=False,
    ):
        """Return the number of photos in the active workspace that match
        an unsaved rules list. Used by the smart-collection modal preview
        and /api/photos/query totals.

        Raises ValueError on malformed input (propagated from
        ``_build_query_from_rules``).
        """
        return self._collection_repository().count_photos_for_rules(
            rules,
            collection_id=collection_id,
            folder_id=folder_id,
            include_offline_folders=include_offline_folders,
        )

    def _append_folder_restriction(self, folder_id, where, params):
        """AND a folder-subtree restriction onto built rule clauses, matching
        get_photos' folder_id semantics (the folder and its descendants).

        Wraps the existing WHERE body in parentheses before ANDing so a
        top-level ``any`` rule tree (``WHERE (A) OR (B)``) doesn't have the
        folder restriction bind only to the last OR branch — AND binds
        tighter than OR in SQL, and without wrapping photos outside the
        selected folder would leak through the earlier branches.
        """
        return self._collection_repository()._append_folder_restriction(
            folder_id,
            where,
            params,
        )

    def _append_collection_restriction(
        self,
        collection_id,
        where,
        params,
        include_offline_folders=False,
    ):
        """AND a collection-membership subquery onto built rule clauses.

        Lets /api/photos/query serve Browse's dashboard-scoped collection
        view (collection as a restriction on the filtered grid) without the
        rule tree needing to reference collections. Raises ValueError for a
        collection missing from the active workspace. Wraps the existing
        WHERE body in parentheses before ANDing for the same OR-precedence
        reason as ``_append_folder_restriction``.
        """
        return self._collection_repository()._append_collection_restriction(
            collection_id,
            where,
            params,
            include_offline_folders=include_offline_folders,
        )

    def query_photos(
        self,
        rules,
        sort="date",
        page=1,
        per_page=50,
        collection_id=None,
        folder_id=None,
        include_offline_folders=False,
    ):
        """Return paginated photos matching a universal-filter rule tree.

        The rules format is the smart-collection tree (see
        ``_build_query_from_rules``); ``count_photos_for_rules`` gives the
        matching total. Raises ValueError on malformed rules.
        """
        return self._collection_repository().query_photos(
            rules,
            sort=sort,
            page=page,
            per_page=per_page,
            collection_id=collection_id,
            folder_id=folder_id,
            include_offline_folders=include_offline_folders,
        )

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

    def _burst_run_ctes(self, settings):
        """Return the CTE chain that turns a ``burst_candidates`` CTE into one
        ``_burst_key`` per photo plus a ``burst_sizes`` tally, and the params
        it consumes.

        Shared verbatim by the stacked Browse SQL and by the visual-search
        collapse path, so the metadata grid and a relevance-ordered result
        can never disagree about where a burst starts and ends. The caller
        supplies a ``burst_candidates`` CTE exposing ``id``, ``folder_id``,
        ``timestamp``, ``_stack_species`` and ``_stack_location``, left open
        (this fragment closes it).
        """
        return self._collection_repository()._burst_run_ctes(settings)

    def _browse_stack_query_parts(self, rules, collection_id=None, folder_id=None,
                                  include_offline_folders=False,
                                  stack_config=None, sort=None):
        """Build the scoped CTE shared by stacked Browse list/count queries.

        ``sort`` is the sort the caller will order by. It only matters for
        the prediction-confidence sorts, which rank on a value that is not a
        ``photos`` column: for those the per-photo score is projected into
        ``scoped`` as ``_prediction_confidence`` so the window functions
        downstream can read it. Every other sort (and ``count_browse_stacks``,
        which does not sort at all) leaves the column — and its correlated
        subquery — out entirely.

        Stacks are a presentation of the current result set, not durable
        catalog state: filters apply to members first, then exact duplicates
        and camera bursts collapse only when at least two matching photos
        remain. Exact duplicates claim their members before bursts so one
        photo can never appear in two Browse items. Because the burst run is
        computed over the *matching* photos, a filter that removes the middle
        of a run can split it — that is the same "stacks describe what you
        are looking at" rule, applied to time instead of membership.

        A burst is a run of frames from one folder whose consecutive capture
        times are no more than ``browse_stack_time_gap`` seconds apart and
        which carry the same species and location keywords. Frames without a
        capture time never join a burst — there is nothing to measure them
        against — and offline frames are excluded as described below.

        Keyword agreement is part of the burst identity on purpose. An
        untagged frame sitting inside an otherwise-tagged run breaks out as
        its own item rather than hiding behind a tagged cover, which is what
        makes the remaining tagging work visible while culling; as those
        frames are tagged they rejoin the run. ``browse_stack_split_mode``
        chooses how a mid-run change is resolved: ``break`` (the default)
        starts a new stack at every change, so a run tagged A, B, A yields
        three stacks in shooting order, while ``partition`` groups the run by
        keyword set and yields two.

        Earlier versions keyed bursts on ``photos.burst_id`` (EXIF
        ImageUniqueID, the only value the scanner writes there). That is not
        a burst identifier: the cameras that write it at all reuse the value
        across the life of the body, so it produced a handful of enormous
        false stacks spanning years while every real burst — from cameras
        that omit the tag entirely, which is most of them — stayed unstacked.
        The column is still filterable; it is no longer a stacking signal.

        ``include_offline_folders`` is the opt-in offline collection view
        (PR #1563). Photos whose folder is offline are *shown* — they render
        as read-only placeholders — but they never join a stack: each one
        keeps its own ``photo:<id>`` key and is left out of every duplicate /
        burst tally. They are also left out of the run sequence entirely, so
        an offline frame in the middle of a burst neither splits it nor
        contributes its keywords to it. Three reasons, all about not lying to
        the user:

        * A stack's cover is its only interactive card, and
          ``_STACK_COVER_ORDER`` ranks on quality alone. An offline member
          could win the cover and turn a stack holding perfectly reachable
          frames into a dead placeholder.
        * The stack badge reads as "N photos here to cull". Counting frames
          the user cannot rate, flag, or delete would make that count a
          proxy rather than an answer (CORE_PHILOSOPHY, "no black boxes").
        * A frame that is not shown as part of the stack must not silently
          decide where the stack ends.
        """
        return self._collection_repository()._browse_stack_query_parts(
            rules,
            collection_id=collection_id,
            folder_id=folder_id,
            include_offline_folders=include_offline_folders,
            stack_config=stack_config,
            sort=sort,
        )

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

    def _stack_sort_spec(self, sort):
        return self._collection_repository()._stack_sort_spec(sort)

    def _stack_sort_clause(self, sort):
        return self._collection_repository()._stack_sort_clause(sort)

    def _ranked_stack_query(self, rules, sort="date", collection_id=None,
                            folder_id=None, include_offline_folders=False,
                            stack_config=None):
        """Return the CTE + ``ranked`` window-function block shared by every
        stack-projected query. Callers append their own outer SELECT (with
        ORDER BY and optional LIMIT/OFFSET).

        ``sort`` selects which member each stack's ``_stack_lead_*`` columns
        are read from, so it must match the ``_stack_sort_clause(sort)`` the
        caller orders by.
        """
        return self._collection_repository()._ranked_stack_query(
            rules,
            sort=sort,
            collection_id=collection_id,
            folder_id=folder_id,
            include_offline_folders=include_offline_folders,
            stack_config=stack_config,
        )

    def query_browse_stacks(self, rules, sort="date", page=1, per_page=50,
                            collection_id=None, folder_id=None,
                            include_offline_folders=False, stack_config=None):
        """Return one representative row per exact-duplicate or burst stack.

        The returned rows have three private columns consumed by the HTTP
        layer: ``_browse_stack_kind``, ``_browse_stack_count``, and
        ``_browse_stack_member_ids``. Singles carry a null kind and otherwise
        retain the ordinary photo-list shape.

        Under a prediction-confidence sort they carry a fourth,
        ``_stack_lead_prediction_confidence``: the score that actually
        positioned the item. A stack is placed by its *leading member* (see
        ``_STACK_SORT_SPECS``) while its cover is chosen on quality, so those
        are often different frames — and a badge showing the cover's own
        score would then name a number that did not decide where the card
        sits. The HTTP layer prefers this value for the card's
        ``prediction_confidence`` (Codex P2 on PR #1670).
        """
        return self._collection_repository().query_browse_stacks(
            rules,
            sort=sort,
            page=page,
            per_page=per_page,
            collection_id=collection_id,
            folder_id=folder_id,
            include_offline_folders=include_offline_folders,
            stack_config=stack_config,
        )

    def count_browse_stacks(self, rules, collection_id=None, folder_id=None,
                            include_offline_folders=False, stack_config=None):
        """Count logical Browse items after stack projection."""
        return self.browse_stack_totals(
            rules, collection_id=collection_id, folder_id=folder_id,
            include_offline_folders=include_offline_folders,
            stack_config=stack_config,
        )["total"]

    def browse_stack_totals(self, rules, collection_id=None, folder_id=None,
                           include_offline_folders=False, stack_config=None):
        """Count all items and multi-photo stacks in one scoped projection."""
        return self._collection_repository().browse_stack_totals(
            rules,
            collection_id=collection_id,
            folder_id=folder_id,
            include_offline_folders=include_offline_folders,
            stack_config=stack_config,
        )

    def query_browse_stack_position(self, rules, photo_id, sort="date",
                                    collection_id=None, folder_id=None,
                                    include_offline_folders=False,
                                    stack_config=None):
        """Return the zero-based position of the Browse item *containing*
        ``photo_id`` once stacks are projected, or ``None`` when the photo
        does not match.

        Stacked Browse pages logical items, not photos, so a hidden burst
        frame has no page of its own — the page that shows it is the one its
        cover sits on. Ranking the covers and joining back through
        ``_stack_key`` answers for members and singles alike, which is what
        lets a re-sort keep a selected stack member without Browse having to
        silently turn Stacks off the way focused deep links do.

        ``stack_config`` must be the grouping the caller pages with
        (``query_browse_stacks``): stacks are a configured projection of the
        result set, so a position read under different settings describes a
        different grid.

        Raises ValueError on malformed rules.
        """
        found = self.query_browse_stack_position_first(
            rules, [photo_id], sort=sort, collection_id=collection_id,
            folder_id=folder_id,
            include_offline_folders=include_offline_folders,
            stack_config=stack_config,
        )
        return found[1] if found is not None else None

    @staticmethod
    def _candidate_preference_case(column, ids):
        """ORDER BY fragment ranking ``ids`` in the order they were asked for.

        Position alone does not decide between frames of one stack: they all
        report their cover's position, so a plain ``ORDER BY position, id``
        would answer with whichever frame happens to hold the lowest ID. The
        caller asked about a card, and it named the frame that *is* that card
        first — answering with a hidden member instead would send the client
        off to expand a tray around a frame the user never opened. Binds one
        parameter per ID (the ``IN`` list binds them once more).
        """
        from repositories.collections import CollectionRepository

        return CollectionRepository._candidate_preference_case(column, ids)

    def query_browse_stack_position_first(self, rules, photo_ids, sort="date",
                                          collection_id=None, folder_id=None,
                                          include_offline_folders=False,
                                          stack_config=None):
        """The earliest-placed of ``photo_ids`` once stacks are projected.

        Returns ``(photo_id, position)`` for whichever candidate the grid
        shows first, or ``None`` when the result set contains none of them.

        Browse asks with a list when the card it wants to keep is a stack:
        the reload it is about to paint can drop any individual frame — a
        saved expression that matches only part of a burst, an undo that
        moves frames out of the filter — and any surviving frame the user
        picked identifies the same card. One ranking answers for all of
        them, so the fallback costs one query rather than one page request
        per frame. Ties (frames of one stack share their cover's position)
        resolve by ID so the answer is stable.

        Raises ValueError on malformed rules.
        """
        return self._collection_repository().query_browse_stack_position_first(
            rules,
            photo_ids,
            sort=sort,
            collection_id=collection_id,
            folder_id=folder_id,
            include_offline_folders=include_offline_folders,
            stack_config=stack_config,
        )

    def _stacked_photo_ids(self, rules, sort="date",
                           collection_id=None, folder_id=None,
                           stack_config=None):
        """Shared cover-first stacked ID projection used by every select-all
        endpoint that honors Stacks. For each stack (in the same order
        ``query_browse_stacks`` places covers) emits the cover ID first, then
        the remaining member IDs in the intra-stack order the projection
        exposes as ``browse_stack.photo_ids``. Singles carry themselves.
        """
        return self._collection_repository()._stacked_photo_ids(
            rules,
            sort=sort,
            collection_id=collection_id,
            folder_id=folder_id,
            stack_config=stack_config,
        )

    def get_collection_photo_ids_stacked(self, collection_id, sort="date",
                                         stack_config=None):
        """Return every photo ID matching a collection, in stack-projected
        order. See ``_stacked_photo_ids``.

        Without this, Select-all-matching with Stacks enabled would seed
        Best Batch, Burst Review, and the export-preview filename from a
        hidden member whenever a stack's quality-ranked cover isn't its
        earliest member under the selected sort (Codex P2 on PR #1561).
        """
        return self._stacked_photo_ids(
            [], sort=sort, collection_id=collection_id,
            stack_config=stack_config,
        )

    def query_photo_ids_stacked(self, rules, sort="date",
                                collection_id=None, folder_id=None,
                                stack_config=None):
        """Return every photo ID matching a universal-filter rule tree, in
        stack-projected order — the rules analog of
        ``get_collection_photo_ids_stacked``.

        Without this, Select-all-matching with Stacks enabled in the
        workspace, folder, dashboard-collection, or unsaved-filter paths
        would seed Best Batch, Burst Review, and the export-preview filename
        from a hidden member whenever a stack's quality-ranked cover isn't
        its earliest member under the selected sort (Codex P2 on PR #1561).
        """
        return self._stacked_photo_ids(
            rules, sort=sort, collection_id=collection_id, folder_id=folder_id,
            stack_config=stack_config,
        )

    def _burst_keys_for_ids(self, photo_ids, stack_config=None):
        """Return ``{photo_id: burst_key}`` for the ids that land in a burst
        of two or more, using the same CTE chain as the stacked Browse SQL.

        The candidate ids go through a TEMP table rather than an ``IN (...)``
        list: the burst key depends on each frame's neighbours in capture
        order, so chunking the ids would invent a run boundary wherever a
        real burst happened to straddle a chunk.
        """
        return self._collection_repository()._burst_keys_for_ids(
            photo_ids,
            stack_config=stack_config,
        )

    def collapse_browse_stack_photo_ids(self, photo_ids, standalone_ids=None,
                                        stack_config=None):
        """Collapse an already ordered ID result, preserving group order.

        Visual search has already materialized its relevance-ordered IDs, so
        re-running the metadata SQL would lose that order. This bounded helper
        applies the same duplicate-first overlap rule, then selects a
        quality-ranked cover for each logical item. Group *identity* comes
        from the shared projection (exact-duplicate hash, then
        ``_burst_keys_for_ids``) so a relevance-ordered result and the
        metadata grid always draw the same stack boundaries; only group
        *order* follows the relevance ranking, by first appearance.

        ``standalone_ids`` are photos that must never join a stack — the
        offline members of the opt-in offline collection view. They keep
        their place in the relevance order as their own single-photo item,
        and are left out of every duplicate / burst tally, matching the SQL
        projection in ``_browse_stack_query_parts``.
        """
        return self._collection_repository().collapse_browse_stack_photo_ids(
            photo_ids,
            standalone_ids=standalone_ids,
            stack_config=stack_config,
        )

    # (display_expr, group_expr) for suggest-capable columns.
    # ``group_expr`` folds the value the same way the corresponding filter
    # matches it — camera/lens rules use ``LOWER(col) = LOWER(?)`` so their
    # facets must group case-insensitively, or ``Sony A1`` and ``sony a1``
    # would split into two 1-count suggestions while selecting either would
    # match both photos. ``display_expr`` is what the picker shows; using
    # ``MIN(col)`` picks a stable representative spelling within each
    # case-insensitive bucket instead of always lower-casing the label.
    def query_photo_ids(
        self,
        rules,
        sort="date",
        collection_id=None,
        folder_id=None,
        include_offline_folders=False,
    ):
        """Return every photo id matching a universal-filter rule tree, in
        display order — the rules analog of ``get_photo_ids`` (select-all,
        visual-search candidate scope). Raises ValueError on malformed rules.
        """
        return self._collection_repository().query_photo_ids(
            rules,
            sort=sort,
            collection_id=collection_id,
            folder_id=folder_id,
            include_offline_folders=include_offline_folders,
        )

    def query_photo_position(
        self,
        rules,
        photo_id,
        sort="date",
        collection_id=None,
        folder_id=None,
        include_offline_folders=False,
    ):
        """Return a photo's zero-based position in a universal-filter result
        set, or ``None`` when it does not match — the rules analog of
        ``get_photo_position``. One-ID shorthand for
        ``query_photo_position_first``.
        """
        found = self.query_photo_position_first(
            rules, [photo_id], sort=sort, collection_id=collection_id,
            folder_id=folder_id,
            include_offline_folders=include_offline_folders,
        )
        return found[1] if found is not None else None

    def query_photo_position_first(
        self,
        rules,
        photo_ids,
        sort="date",
        collection_id=None,
        folder_id=None,
        include_offline_folders=False,
    ):
        """The earliest-placed of ``photo_ids`` in a universal-filter result
        set, as ``(photo_id, position)``, or ``None`` when the result set
        contains none of them.

        Browse asks with a list when the card it is holding onto stands for
        several photos: any frame of a selected stack identifies the same
        card, so a reload that dropped some of them can still be placed by
        the ones it kept. Ranking once answers for every candidate; ties
        resolve by ID so the answer is stable.

        Browse calls this (through ``focus_photo_id`` /
        ``focus_photo_ids`` on ``/api/photos/query`` and the collection
        photos endpoint) when a re-sort has to hold onto the card the user
        has selected. Materializing the ordered ID list and indexing it
        client-side would move O(result set) IDs over the wire on every sort
        change; probing serial pages until the photo appears would issue
        O(position / per_page) filtered queries. A ROW_NUMBER window over the
        same scoped rows the grid pages through answers it in one read.

        Raises ValueError on malformed rules.
        """
        return self._collection_repository().query_photo_position_first(
            rules,
            photo_ids,
            sort=sort,
            collection_id=collection_id,
            folder_id=folder_id,
            include_offline_folders=include_offline_folders,
        )

    _SUGGEST_VALUE_EXPRS = {
        "camera_make": ("MIN(p.camera_make)", "LOWER(p.camera_make)"),
        "camera_model": ("MIN(p.camera_model)", "LOWER(p.camera_model)"),
        "lens": ("MIN(p.lens)", "LOWER(p.lens)"),
        "extension": ("LOWER(p.extension)", "LOWER(p.extension)"),
    }

    def get_filter_field_values(self, field, rules=None, q=None, limit=20,
                                 folder_id=None, collection_id=None):
        """Distinct values (with photo counts) for a suggest-capable field.

        Counts respect the supplied rule tree, so the caller can pass the
        active expression minus the rule being edited and get live facet
        counts (design requirement: counts answer "how many results would I
        get", never a global COUNT(*)). ``folder_id``/``collection_id`` AND
        the same page-scope restrictions Browse applies to
        ``/api/photos/query``; without them the typeahead advertises counts
        computed over the whole workspace while the visible grid is
        folder/collection-scoped, so picking a suggestion can yield fewer
        (or zero) grid results than the badge promised. Raises ValueError
        for fields without value suggestions or malformed rules.
        """
        return self._collection_repository().get_filter_field_values(
            field,
            rules=rules,
            q=q,
            limit=limit,
            folder_id=folder_id,
            collection_id=collection_id,
        )

    def _folder_filter_values(self, folder_join, join_clause, where, params, q, limit):
        """Folder suggestions with subtree-aware counts.

        The ``folder`` field's engine operators are ``under``/``not_under``,
        which match a folder and every descendant. A count grouped by each
        photo's immediate ``f.path`` therefore misreports what selecting a
        suggested folder would return — a parent with no direct photos but
        matching descendants would be omitted entirely, and any folder with
        both direct and nested photos would undercount. Aggregate over each
        workspace folder's subtree so the facet answers "how many photos
        would ``folder under=<path>`` return" (design requirement: counts
        never lie about the rule they preview).
        """
        return self._collection_repository()._folder_filter_values(
            folder_join,
            join_clause,
            where,
            params,
            q,
            limit,
        )

    def collection_photo_ids(self, collection_id):
        """Return the set of photo IDs in the collection, workspace-scoped.

        Returns an empty set for a missing collection. Used by stages
        that need to restrict writes to the current pipeline-run scope
        without paging through full photo rows.
        """
        return self._collection_repository().photo_ids(collection_id)

    def update_folder_counts(self):
        """Recalculate photo_count for all folders."""
        self._folder_repository(scoped=False).update_counts()

    def _resolve_species_by_lineage(self, species_name, expected_ancestors):
        """Pick the species-rank taxon matching ``species_name`` and lineage.

        Every candidate, including a sole local row, must contain every name
        in ``expected_ancestors``. A taxonomy name index can retain only the
        wrong side of a homonym, so candidate count alone is not evidence that
        the local row belongs to the lookup's lineage. Missing lineage context,
        zero matches, or multiple matches all return ``None`` rather than
        permanently crediting the wrong Life List species/class.
        """
        return self._keyword_repository().resolve_species_by_lineage(
            species_name, expected_ancestors,
        )

    def mark_species_keywords(self, taxonomy):
        """Mark keywords that are recognized species in the taxonomy.

        Retypes untyped and ``type='general'`` keywords whose names match a
        taxon lookup, and repairs incomplete ``type='taxonomy'`` rows. Explicit
        non-taxonomy types such as ``location``, ``genre``, and ``individual``
        are user intent and must be preserved even when their names are taxon
        homonyms (for example, the location "California" is also a plant
        genus).

        Matching rows get ``is_species=1``, ``type='taxonomy'``, and (if the
        local taxa table is populated and the lookup resolves to a
        species-rank taxon) a ``taxon_id`` link by iNaturalist id. Lookup
        results below species rank (for example Eastern Fox Squirrel or
        Red-eared Slider subspecies) link to their species ancestor: the Life
        List keeps the precise stored label while the Explorer credits the
        containing species once.
        ``taxon_id`` is left NULL when the row had no prior link and only
        a higher-rank (genus/family) match exists — binding an unlinked
        row to a non-species-rank taxon would auto-promote it in a way the
        classifier callers never asked for. A ``type='taxonomy'`` row
        whose ``taxon_id`` was bound by the old species-agnostic lookup
        to a non-species-rank taxon (genus/family) is rebound to the
        species-rank taxon whenever ``taxonomy.lookup`` resolves to one;
        when no species-rank replacement exists, the higher-rank link is
        preserved so ``get_life_list_candidates`` can keep surfacing the
        row's ``taxon_rank`` / ``scientific_name`` / ``taxonomic_class``
        metadata for genus/family/class Life List filters.

        Uses the local taxonomy only (no network requests).

        Args:
            taxonomy: a Taxonomy instance with a lookup() method
        """
        return self._keyword_repository().mark_species(taxonomy)

    def create_default_collections(self, workspace_id=None):
        """Create default smart collections, skipping any that already exist by name.

        Workspace defaults to the active one. Pass ``workspace_id`` to seed a
        specific workspace without needing it to be active — used by
        ``api_create_workspace`` so brand-new workspaces get the defaults at
        creation time instead of relying on a future startup pass.
        """
        self._collection_repository().create_defaults(workspace_id=workspace_id)

    def create_default_collections_for_all_workspaces(self):
        """Create missing default smart collections in every workspace."""
        for ws in self.get_workspaces():
            self.create_default_collections(workspace_id=ws["id"])

    def migrate_default_location_collections(self):
        """Clarify default location collection names/rules across workspaces.

        - ``Needs Location`` was the default collection for photos that already
          have EXIF GPS but lack a structured Vireo location keyword. Rename
          exact default instances to the more literal
          ``GPS Without Location Keyword``.
        - Some workspaces had a hand-built ``No Location`` collection using the
          inverse of that rule. That actually meant "not GPS-without-keyword",
          not "has no location". For that exact legacy rule, replace it with a
          true ``No Location Information`` collection.
        """
        return self._collection_repository().migrate_default_location()

    def migrate_default_subject_collection(self):
        """Rename legacy 'Needs Classification' (with rule has_species==0)
        to 'Needs Identification' (rule has_subject==0) across ALL workspaces.

        Workspace activation does not re-run startup migrations, so an
        upgraded multi-workspace database would otherwise leave non-active
        workspaces stuck on the legacy rule. Skips collections the user has
        customized. Idempotent."""
        self._collection_repository().migrate_default_subject()

    def migrate_default_needs_identification_collection(self):
        """Upgrade the default Needs Identification rule to skip Not Wildlife.

        User-customized collections are left alone; only the exact previous
        default ``has_subject == 0`` rule is rewritten.
        """
        return self._collection_repository().migrate_default_needs_identification()

    def rewrite_legacy_miss_thresholds_in_workspaces(
        self, legacy_det, legacy_burst, new_det, new_burst
    ):
        """Rewrite the exact legacy miss-threshold default pair in every
        workspace's ``config_overrides``. Customized values are left alone.

        Called from ``config.migrate_legacy_miss_thresholds``, which gates
        the whole migration behind a one-time marker so this only runs
        once per install — a user who later explicitly re-saves the
        legacy pair via the settings UI keeps that setting.
        """
        return self._workspace_repository(scoped=False).rewrite_legacy_miss_thresholds(
            legacy_det, legacy_burst, new_det, new_burst
        )

    def rewrite_legacy_w_species_default_in_workspaces(self, legacy, new):
        """Rewrite the exact legacy ``pipeline.w_species`` default in every
        workspace's ``config_overrides``. Customized values are left alone.

        Called from ``config.migrate_legacy_w_species_default``, which gates
        the whole migration behind a one-time marker so this only runs once
        per install — a user who later explicitly re-saves the legacy value
        via the algorithm slider keeps that setting.

        Fingerprint invalidation isn't needed: ``compute_group_fingerprint``
        reads the effective ``w_species``, so any rewritten workspace's
        ``last_group_fingerprint`` will already stop matching on the next
        Process-page load.
        """
        return self._workspace_repository(scoped=False).rewrite_legacy_w_species_default(
            legacy, new
        )

    def rewrite_legacy_eye_detect_default_in_workspaces(self):
        """Rewrite the exact legacy eye-detection default in workspace overrides.

        Also nulls ``last_group_fingerprint`` on any rewritten workspace so the
        Process page treats its cached KEEP/REJECT decisions as outdated —
        those results were scored with eye detection on, and the workspace's
        effective ``eye_detect_enabled`` just changed to False.
        """
        return self._workspace_repository(scoped=False).rewrite_legacy_eye_detect_default()

    def invalidate_group_fingerprints_without_explicit_eye_false(self):
        """Clear last_group_fingerprint on workspaces without an explicit
        ``pipeline.eye_detect_enabled=False`` override.

        Called from ``migrate_eye_detect_default_off`` when the global
        default is about to flip from True to False. Workspaces that were
        relying on the global default were producing eye-enabled scoring;
        their cached triage must be treated as outdated so the Process page
        re-runs Group & Score with the new default. Workspaces with an
        explicit False override were already producing eye-disabled scoring
        and don't need invalidation.
        """
        repo = self._workspace_repository(scoped=False)
        return repo.invalidate_group_fingerprints_without_explicit_eye_false()

    # ------ iNaturalist submissions ------

    def record_inat_submission(self, photo_id, observation_id, observation_url):
        """Record a successful iNaturalist submission."""
        self._inat_repository().record_submission(
            photo_id, observation_id, observation_url
        )

    def get_inat_submissions(self, photo_ids):
        """Return {photo_id: {observation_id, observation_url, submitted_at}} for given IDs."""
        return self._inat_repository().get_submissions(photo_ids)

    def _inat_repository(self):
        """Build the (catalog-wide) iNaturalist repository on this connection."""
        from repositories.inat import InatRepository

        return InatRepository(self.conn, chunk_size=_SQLITE_PARAM_CHUNK_SIZE)
