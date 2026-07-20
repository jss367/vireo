"""SQLite database for Vireo photo browser metadata cache."""

import contextlib
import json
import logging
import os
import sqlite3
import time
import unicodedata
import uuid
from datetime import datetime

from keyword_normalization import keyword_match_key, normalize_keyword_display
from new_images import get_shared_cache

log = logging.getLogger(__name__)

_UNSET = object()  # sentinel for "not provided" vs explicit None

AUTO_MATCH_REVIEW_MARKER = "__vireo_auto_match__"

_SQLITE_PARAM_CHUNK_SIZE = 800
_MISSING_PHOTOS_PROGRESS_INTERVAL = 200


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
    diagnosable.
    """

    def __init__(self, db_path, cause=None):
        self.db_path = db_path
        self.cause = cause
        msg = (
            f"The database at {db_path} is from an incompatible older version "
            f"of Vireo and cannot be opened by this build"
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


# Canonical set of keyword type values stored in keywords.type.
# - taxonomy: a species/genus/etc. (linked to taxa via taxon_id)
# - individual: a named person, pet, or otherwise tracked individual
# - location: a named location ("Yosemite", "backyard")
# - genre: a non-subject visual category ("Landscape", "Sunset")
# - general: catch-all/free-form tag (the legacy default)
KEYWORD_TYPES = frozenset({"taxonomy", "individual", "location", "genre", "general"})

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
    "misses", "highlights", "life_list", "browse", "edit", "map", "location_review", "variants",
    "dashboard", "storage", "audit", "move", "compare",
    "settings", "workspace", "lightroom", "shortcuts",
    "keywords", "duplicates", "logs",
})

DEFAULT_TABS = [
    "import", "browse", "pipeline", "pipeline_review",
    "review", "cull", "jobs", "highlights", "misses", "storage", "settings",
]


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
        self.conn = sqlite3.connect(db_path, check_same_thread=False)
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
        self.ensure_default_workspace()
        # Idempotent legacy-type migration. MUST run before genre seeding
        # so an upgraded DB with e.g. 'descriptive'/'event'/'people' rows
        # named 'Wildlife' gets normalized first. Otherwise the seed's
        # UNIQUE(name, parent_id) INSERT OR IGNORE skips the Wildlife
        # genre, then the migration converts that legacy row to 'general',
        # leaving auto-Wildlife and backfill queries unable to find a
        # canonical 'Wildlife' / type='genre' row. Cheap warm-path (single
        # SELECT 1 LIMIT 1) once all legacy rows are gone.
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
        self._restore_active_workspace()

    def _restore_active_workspace(self):
        """Restore the last-used workspace on an already initialized schema."""
        last = self.conn.execute(
            "SELECT id FROM workspaces ORDER BY CASE WHEN last_opened_at IS NULL THEN 0 ELSE 1 END DESC, last_opened_at DESC, id ASC LIMIT 1"
        ).fetchone()
        if last is None:
            raise RuntimeError("Vireo database has no workspace after schema initialization")
        self.set_active_workspace(last[0])

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
                working_copy_failed_at   TEXT,
                working_copy_failed_mtime REAL,
                working_copy_failed_source TEXT,
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

            CREATE TABLE IF NOT EXISTS photo_keywords (
                photo_id    INTEGER REFERENCES photos(id),
                keyword_id  INTEGER REFERENCES keywords(id),
                PRIMARY KEY (photo_id, keyword_id)
            );

            -- Singleton key/value table for one-shot migration markers.
            -- Used to gate non-idempotent backfills (where re-running would
            -- overwrite user intent — e.g. Wildlife genre backfill that
            -- would clobber sticky-removed Wildlife rows).
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
                id          INTEGER PRIMARY KEY,
                name        TEXT,
                rules       TEXT,
                workspace_id INTEGER REFERENCES workspaces(id) ON DELETE CASCADE
            );

            CREATE TABLE IF NOT EXISTS pending_changes (
                id          INTEGER PRIMARY KEY,
                photo_id    INTEGER REFERENCES photos(id) ON DELETE CASCADE,
                change_type TEXT,
                value       TEXT,
                change_token TEXT,
                created_at  TEXT DEFAULT (datetime('now')),
                workspace_id INTEGER REFERENCES workspaces(id) ON DELETE CASCADE
            );

            CREATE TABLE IF NOT EXISTS detections (
                id                  INTEGER PRIMARY KEY,
                photo_id            INTEGER NOT NULL REFERENCES photos(id) ON DELETE CASCADE,
                detector_model      TEXT NOT NULL DEFAULT 'megadetector-v6',
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
                PRIMARY KEY (photo_id, variant)
            );

            CREATE TABLE IF NOT EXISTS predictions (
                id                   INTEGER PRIMARY KEY,
                detection_id         INTEGER NOT NULL REFERENCES detections(id) ON DELETE CASCADE,
                classifier_model     TEXT NOT NULL,
                labels_fingerprint   TEXT NOT NULL DEFAULT 'legacy',
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
                run_at          TEXT DEFAULT (datetime('now')),
                box_count       INTEGER NOT NULL DEFAULT 0,
                PRIMARY KEY (photo_id, detector_model)
            );

            CREATE TABLE IF NOT EXISTS classifier_runs (
                detection_id         INTEGER NOT NULL REFERENCES detections(id) ON DELETE CASCADE,
                classifier_model     TEXT NOT NULL,
                labels_fingerprint   TEXT NOT NULL,
                run_at               TEXT DEFAULT (datetime('now')),
                prediction_count     INTEGER NOT NULL DEFAULT 0,
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

            CREATE INDEX IF NOT EXISTS idx_photos_timestamp ON photos(timestamp);
            CREATE INDEX IF NOT EXISTS idx_photos_folder ON photos(folder_id);
            CREATE INDEX IF NOT EXISTS idx_photos_rating ON photos(rating);
            CREATE INDEX IF NOT EXISTS idx_photos_file_hash ON photos(file_hash);

            CREATE INDEX IF NOT EXISTS idx_keywords_name ON keywords(name);
            CREATE INDEX IF NOT EXISTS idx_keywords_parent_id ON keywords(parent_id);
            -- type is low-cardinality (5-value enum) but heavily filtered:
            -- has_subject rule, filter_out_subject_tagged, backfill_wildlife,
            -- and the warm-path migration probes all do WHERE type [IN/=] ...
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
            CREATE INDEX IF NOT EXISTS idx_photo_embeddings_model
                ON photo_embeddings(model, variant);
            CREATE INDEX IF NOT EXISTS idx_prediction_review_workspace
                ON prediction_review(workspace_id);
            CREATE INDEX IF NOT EXISTS idx_collections_workspace
                ON collections(workspace_id);
            CREATE INDEX IF NOT EXISTS idx_pending_workspace
                ON pending_changes(workspace_id);
        """
        )
        cur = self.conn.cursor()
        cur.execute("PRAGMA table_info(keywords)")
        kw_cols = {row[1] for row in cur.fetchall()}
        if "place_id" not in kw_cols:
            cur.execute("ALTER TABLE keywords ADD COLUMN place_id TEXT")
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
        # Migration: miss-classifier columns. PHOTO_COLS/get_collection_photos
        # and misses.py both reference these; without the fallback ALTER, any
        # DB created before the miss-classifier feature fails every photo-list
        # query with "no such column".
        for column, column_type in (
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
    _PROCESS_FLAG_COLS = (
        "skip_classify",
        "skip_extract_masks",
        "skip_eye_keypoints",
        "skip_regroup",
        "miss_enabled",
    )

    @staticmethod
    def _saved_process_row_to_dict(row):
        return {
            "id": row["id"],
            "name": row["name"],
            "skip_classify": bool(row["skip_classify"]),
            "skip_extract_masks": bool(row["skip_extract_masks"]),
            "skip_eye_keypoints": bool(row["skip_eye_keypoints"]),
            "skip_regroup": bool(row["skip_regroup"]),
            "miss_enabled": bool(row["miss_enabled"]),
            "review_mode": row["review_mode"],
            "is_seed": bool(row["is_seed"]),
            "sort_order": row["sort_order"],
        }

    def get_saved_processes(self):
        """Return all saved processes ordered for display (sort_order, id)."""
        rows = self.conn.execute(
            "SELECT * FROM saved_processes ORDER BY sort_order, id"
        ).fetchall()
        return [self._saved_process_row_to_dict(r) for r in rows]

    def get_saved_process(self, process_id):
        """Return one saved process as a dict, or None if it doesn't exist."""
        row = self.conn.execute(
            "SELECT * FROM saved_processes WHERE id = ?", (process_id,)
        ).fetchone()
        return self._saved_process_row_to_dict(row) if row else None

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
        if not isinstance(name, str) or not name.strip():
            raise ValueError("process name is required")
        if review_mode is not None and review_mode != "species":
            raise ValueError("review_mode must be 'species' or null")
        return (
            name.strip(),
            int(bool(skip_classify)),
            int(bool(skip_extract_masks)),
            int(bool(skip_eye_keypoints)),
            int(bool(skip_regroup)),
            int(bool(miss_enabled)),
            review_mode,
        )

    def create_saved_process(self, name, *, skip_classify=False,
                             skip_extract_masks=False, skip_eye_keypoints=False,
                             skip_regroup=False, miss_enabled=True,
                             review_mode=None):
        """Insert a saved process and return its id.

        Raises ValueError on a blank/duplicate name or a bad review_mode.
        """
        fields = self._normalize_process_fields(
            name, skip_classify, skip_extract_masks, skip_eye_keypoints,
            skip_regroup, miss_enabled, review_mode,
        )
        # New user processes sort after the seeds; ties break by id.
        next_order = self.conn.execute(
            "SELECT COALESCE(MAX(sort_order), -1) + 1 FROM saved_processes"
        ).fetchone()[0]
        try:
            cur = self.conn.execute(
                "INSERT INTO saved_processes "
                "(name, skip_classify, skip_extract_masks, skip_eye_keypoints, "
                " skip_regroup, miss_enabled, review_mode, is_seed, sort_order) "
                "VALUES (?, ?, ?, ?, ?, ?, ?, 0, ?)",
                (*fields, next_order),
            )
        except sqlite3.IntegrityError as e:
            raise ValueError(
                f"a process named {name.strip()!r} already exists"
            ) from e
        self.conn.commit()
        return cur.lastrowid

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
        merged = {
            "name": current["name"] if name is None else name,
            "skip_classify": current["skip_classify"] if skip_classify is None else skip_classify,
            "skip_extract_masks": current["skip_extract_masks"] if skip_extract_masks is None else skip_extract_masks,
            "skip_eye_keypoints": current["skip_eye_keypoints"] if skip_eye_keypoints is None else skip_eye_keypoints,
            "skip_regroup": current["skip_regroup"] if skip_regroup is None else skip_regroup,
            "miss_enabled": current["miss_enabled"] if miss_enabled is None else miss_enabled,
            "review_mode": current["review_mode"] if review_mode is _UNSET else review_mode,
        }
        fields = self._normalize_process_fields(
            merged["name"], merged["skip_classify"], merged["skip_extract_masks"],
            merged["skip_eye_keypoints"], merged["skip_regroup"],
            merged["miss_enabled"], merged["review_mode"],
        )
        try:
            self.conn.execute(
                "UPDATE saved_processes SET name=?, skip_classify=?, "
                "skip_extract_masks=?, skip_eye_keypoints=?, skip_regroup=?, "
                "miss_enabled=?, review_mode=? WHERE id=?",
                (*fields, process_id),
            )
        except sqlite3.IntegrityError as e:
            raise ValueError(
                f"a process named {merged['name'].strip()!r} already exists"
            ) from e
        self.conn.commit()
        return True

    def delete_saved_process(self, process_id):
        """Delete a saved process and null out every reference to it.

        Any workspace whose pipeline.default_process_id pointed here falls
        back to null ("import only"). Returns True if the process existed.
        The app-wide global default (config.json) is cleared by the caller,
        which owns config-file I/O. Does not run inside an outer transaction.
        """
        if self.get_saved_process(process_id) is None:
            return False
        self.conn.execute(
            "DELETE FROM saved_processes WHERE id = ?", (process_id,)
        )
        # Null the per-workspace default pointer wherever it referenced this id.
        # Write an explicit ``None`` (not ``pop``) so the workspace's effective
        # config resolves to "import only" instead of silently inheriting a
        # different global ``pipeline.default_process_id`` via _deep_merge.
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
            if pipeline_ov.get("default_process_id") == process_id:
                pipeline_ov["default_process_id"] = None
                self.conn.execute(
                    "UPDATE workspaces SET config_overrides = ? WHERE id = ?",
                    (json.dumps(overrides), row["id"]),
                )
        self.conn.commit()
        return True

    def repair_missing_folder_parents(self):
        """Fill parent_id for legacy folder rows whose parent path is known."""
        rows = self.conn.execute(
            "SELECT id, path, parent_id FROM folders"
        ).fetchall()
        path_to_id = {r["path"]: r["id"] for r in rows}
        updates = []
        for row in rows:
            if row["parent_id"] is not None:
                continue
            parent_path = _stored_parent_path(row["path"])
            parent_id = path_to_id.get(parent_path)
            if parent_id is None or parent_id == row["id"]:
                continue
            updates.append((parent_id, row["id"]))
        if not updates:
            return
        self.conn.executemany(
            "UPDATE folders SET parent_id = ? WHERE id = ?",
            updates,
        )
        self.conn.commit()

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
        # Chunk to stay well under SQLite's SQLITE_MAX_VARIABLE_NUMBER (default 999).
        # A scan of a deep tree can auto-register thousands of descendant folders;
        # a single IN (?, ?, ...) across all of them would raise
        # ``OperationalError: too many SQL variables``.
        CHUNK = 500
        ws_ids = set()
        folder_ids = list(folder_ids)
        for i in range(0, len(folder_ids), CHUNK):
            chunk = folder_ids[i:i + CHUNK]
            placeholders = ",".join("?" * len(chunk))
            rows = self.conn.execute(
                f"SELECT DISTINCT workspace_id FROM workspace_folders "
                f"WHERE folder_id IN ({placeholders})",
                tuple(chunk),
            ).fetchall()
            ws_ids.update(r["workspace_id"] for r in rows)
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

    def create_workspace(self, name, config_overrides=None, ui_state=None):
        """Create a new workspace. Returns the workspace id."""
        cur = self.conn.execute(
            """INSERT INTO workspaces (name, config_overrides, ui_state, tabs)
               VALUES (?, ?, ?, ?)""",
            (name,
             json.dumps(config_overrides) if config_overrides else None,
             json.dumps(ui_state) if ui_state else None,
             json.dumps(DEFAULT_TABS)),
        )
        self.conn.commit()
        workspace_id = cur.lastrowid
        # SQLite INTEGER PRIMARY KEY (without AUTOINCREMENT) can reuse a deleted
        # rowid, so a freshly created workspace may collide with the stale cache
        # entry of a prior workspace that shared this id. Clear any lingering
        # entry so the new workspace starts clean.
        self._new_images_cache.invalidate_workspaces(self._db_path, [workspace_id])
        return workspace_id

    def get_workspace(self, workspace_id):
        """Return a single workspace by id, or None."""
        return self.conn.execute(
            "SELECT * FROM workspaces WHERE id = ?", (workspace_id,)
        ).fetchone()

    def get_workspaces(self):
        """Return all workspaces, pinned first then alphabetical."""
        return self.conn.execute(
            "SELECT * FROM workspaces "
            "ORDER BY (pinned_at IS NULL), LOWER(name)"
        ).fetchall()

    def update_workspace(self, workspace_id, name=None, config_overrides=_UNSET,
                         ui_state=_UNSET, last_opened_at=None,
                         pinned_at=_UNSET):
        """Update workspace fields. Only provided args are updated.

        For config_overrides and ui_state, pass None to clear the value
        (set DB column to NULL), or omit the argument to leave it unchanged.
        """
        updates = []
        params = []
        if name is not None:
            updates.append("name = ?")
            params.append(name)
        if config_overrides is not _UNSET:
            updates.append("config_overrides = ?")
            params.append(json.dumps(config_overrides) if config_overrides is not None else None)
        if ui_state is not _UNSET:
            updates.append("ui_state = ?")
            params.append(json.dumps(ui_state) if ui_state is not None else None)
        if last_opened_at is not None:
            updates.append("last_opened_at = ?")
            params.append(last_opened_at)
        if pinned_at is not _UNSET:
            updates.append("pinned_at = ?")
            params.append(pinned_at)
        if not updates:
            return
        params.append(workspace_id)
        self.conn.execute(
            f"UPDATE workspaces SET {', '.join(updates)} WHERE id = ?", params
        )
        self.conn.commit()

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
        if not subject_types or not photo_ids:
            return list(photo_ids)
        types = [t for t in subject_types if t in KEYWORD_TYPES]
        if not types:
            return list(photo_ids)
        type_placeholders = ",".join("?" * len(types))
        type_clause = f"k.type IN ({type_placeholders})"
        if "taxonomy" in types:
            type_clause = f"({type_clause} OR k.is_species = 1)"
        photo_ids_list = list(photo_ids)
        excluded = set()
        chunk_size = self._FILTER_SUBJECT_CHUNK
        for i in range(0, len(photo_ids_list), chunk_size):
            chunk = photo_ids_list[i:i + chunk_size]
            pid_placeholders = ",".join("?" * len(chunk))
            rows = self.conn.execute(
                f"""SELECT DISTINCT pk.photo_id FROM photo_keywords pk
                    JOIN keywords k ON k.id = pk.keyword_id
                    WHERE {type_clause}
                      AND pk.photo_id IN ({pid_placeholders})""",
                types + chunk,
            ).fetchall()
            for r in rows:
                excluded.add(r["photo_id"])
        return [pid for pid in photo_ids_list if pid not in excluded]

    def filter_out_wildlife_excluded(self, photo_ids):
        """Return photo ids not explicitly excluded from wildlife classification."""
        if not photo_ids:
            return []
        photo_ids_list = list(photo_ids)
        excluded = set()
        chunk_size = self._FILTER_SUBJECT_CHUNK
        for chunk in _chunks(photo_ids_list, chunk_size):
            placeholders = ",".join("?" * len(chunk))
            rows = self.conn.execute(
                f"""SELECT id FROM photos
                    WHERE wildlife_excluded = 1
                      AND id IN ({placeholders})""",
                chunk,
            ).fetchall()
            excluded.update(r["id"] for r in rows)
        return [pid for pid in photo_ids_list if pid not in excluded]

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

    def _workspace_repository(self):
        from repositories.workspaces import WorkspaceRepository

        return WorkspaceRepository(
            self.conn,
            self._ws_id(),
            allowed_nav_ids=ALL_NAV_IDS,
            default_tabs=DEFAULT_TABS,
        )

    def set_workspace_group_state(self, workspace_id, fingerprint, when_ts):
        """Record that grouping completed for `workspace_id` at `when_ts`
        with the given `fingerprint`. Pipeline page treats fingerprint
        mismatch as "Outdated" so the user knows a regroup is pending.
        """
        self.conn.execute(
            "UPDATE workspaces SET last_grouped_at = ?, last_group_fingerprint = ? "
            "WHERE id = ?",
            (when_ts, fingerprint, workspace_id),
        )
        self.conn.commit()

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

    def delete_workspace(self, workspace_id):
        """Delete a workspace and all its scoped data (cascade)."""
        self.conn.execute("DELETE FROM workspaces WHERE id = ?", (workspace_id,))
        self.conn.commit()
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
        row = self.conn.execute(
            "SELECT path FROM folders WHERE id = ?", (folder_id,)
        ).fetchone()
        if row is None or not row["path"]:
            return [folder_id]
        root_path = _path_for_subtree_match(row["path"])
        prefix = root_path + "/"
        rows = self.conn.execute(
            """SELECT id FROM folders
               WHERE id = ?
                  OR substr(REPLACE(path, '\\', '/'), 1, ?) = ?""",
            (folder_id, len(prefix), prefix),
        ).fetchall()
        ids = {folder_id}
        ids.update(r["id"] for r in rows)
        ids.update(self._local_source_descendant_ids(row["path"]))
        return list(ids)

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
        if not root_path:
            return []
        prefix = _path_for_subtree_match(root_path) + "/"
        rows = self.conn.execute(
            """SELECT folder_id FROM local_folder_mappings
               WHERE source_path = ?
                  OR substr(REPLACE(source_path, '\\', '/'), 1, ?) = ?""",
            (root_path, len(prefix), prefix),
        ).fetchall()
        return [int(r["folder_id"]) for r in rows]

    def _add_workspace_folder_no_commit(
            self, workspace_id, folder_id, *, is_root=True):
        """Link a folder + descendants to a workspace WITHOUT committing.

        Same body as ``add_workspace_folder`` minus the ``commit()`` and cache
        invalidation. Intended for callers that already run inside a larger
        try/except+rollback transaction (e.g. ``merge_staged_tree_into_
        archive``) where a mid-body commit would break rollback safety by
        persisting a preceding UPDATE that an outer failure was meant to
        undo. The caller is responsible for committing (and for invalidating
        the workspace's new-images cache) after its own transaction closes.
        """
        folder_ids = self._folder_subtree_ids_by_path(folder_id)
        self.conn.executemany(
            """INSERT OR IGNORE INTO workspace_folders
               (workspace_id, folder_id, is_root) VALUES (?, ?, 0)""",
            [(workspace_id, fid) for fid in folder_ids],
        )
        if is_root:
            for chunk in _chunks(folder_ids):
                placeholders = ",".join("?" for _ in chunk)
                self.conn.execute(
                    f"""UPDATE workspace_folders
                        SET is_root = CASE WHEN folder_id = ? THEN 1 ELSE 0 END
                        WHERE workspace_id = ? AND folder_id IN ({placeholders})""",
                    [folder_id, workspace_id] + chunk,
                )

    def add_workspace_folder(self, workspace_id, folder_id, *, is_root=True):
        """Link a folder and its known descendants to a workspace."""
        self._add_workspace_folder_no_commit(
            workspace_id, folder_id, is_root=is_root)
        self.conn.commit()
        # The folder's untracked files now count toward this workspace's
        # new-images backlog. Drop any stale cached payload so the next read
        # recomputes against the updated folder set.
        self._new_images_cache.invalidate_workspaces(self._db_path, [workspace_id])

    def remove_workspace_folder(self, workspace_id, folder_id):
        """Unlink a single folder from a workspace."""
        self.conn.execute(
            "DELETE FROM workspace_folders WHERE workspace_id = ? AND folder_id = ?",
            (workspace_id, folder_id),
        )
        self.conn.commit()
        # The folder no longer contributes to this workspace's new-images
        # backlog. Drop the cached payload so the banner reflects the change.
        self._new_images_cache.invalidate_workspaces(self._db_path, [workspace_id])

    def remove_workspace_folder_tree(self, workspace_id, folder_id):
        """Unlink a folder and its path descendants from a workspace."""
        folder_ids = self._folder_subtree_ids_by_path(folder_id)
        for chunk in _chunks(folder_ids):
            placeholders = ",".join("?" for _ in chunk)
            self.conn.execute(
                f"""DELETE FROM workspace_folders
                    WHERE workspace_id = ? AND folder_id IN ({placeholders})""",
                [workspace_id] + chunk,
            )
        self.conn.commit()
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
        rows = self.conn.execute(
            """SELECT DISTINCT child.id
               FROM workspace_folders wf
               JOIN folders root ON root.id = wf.folder_id
               JOIN folders child
                 ON child.path = root.path
                 OR substr(
                      REPLACE(child.path, '\\', '/'),
                      1,
                      length(RTRIM(REPLACE(root.path, '\\', '/'), '/') || '/')
                    ) = RTRIM(REPLACE(root.path, '\\', '/'), '/') || '/'
               LEFT JOIN workspace_folders existing
                 ON existing.workspace_id = wf.workspace_id
                AND existing.folder_id = child.id
               WHERE wf.workspace_id = ?
                 AND existing.folder_id IS NULL""",
            (workspace_id,),
        ).fetchall()
        candidate_ids = {r["id"] for r in rows}
        root_rows = self.conn.execute(
            """SELECT f.path FROM workspace_folders wf
               JOIN folders f ON f.id = wf.folder_id
               WHERE wf.workspace_id = ?""",
            (workspace_id,),
        ).fetchall()
        for root_row in root_rows:
            candidate_ids.update(self._local_source_descendant_ids(root_row["path"]))
        if candidate_ids:
            existing = {
                r["folder_id"]
                for r in self.conn.execute(
                    "SELECT folder_id FROM workspace_folders WHERE workspace_id = ?",
                    (workspace_id,),
                ).fetchall()
            }
            candidate_ids -= existing
        if not candidate_ids:
            return
        self.conn.executemany(
            """INSERT OR IGNORE INTO workspace_folders
               (workspace_id, folder_id, is_root) VALUES (?, ?, 0)""",
            [(workspace_id, fid) for fid in candidate_ids],
        )
        self.conn.commit()
        self._new_images_cache.invalidate_workspaces(self._db_path, [workspace_id])

    def mark_workspace_folder_roots(self, workspace_id, folder_ids):
        """Mark specific linked folders as user-facing roots."""
        if not folder_ids:
            return
        for chunk in _chunks(folder_ids):
            placeholders = ",".join("?" for _ in chunk)
            self.conn.execute(
                f"""UPDATE workspace_folders
                    SET is_root = 1
                    WHERE workspace_id = ? AND folder_id IN ({placeholders})""",
                [workspace_id] + chunk,
            )
        self.conn.commit()

    def get_workspace_folders(self, workspace_id):
        """Return all explicit folder links for a workspace.

        Parent folders are recursive roots: if a linked folder has known
        descendants in ``folders``, keep those descendants linked internally so
        existing workspace-scoped photo queries continue to work.
        """
        self._materialize_workspace_descendants(workspace_id)
        return self.conn.execute(
            """SELECT f.* FROM folders f
               JOIN workspace_folders wf ON wf.folder_id = f.id
               WHERE wf.workspace_id = ?
               ORDER BY f.path""",
            (workspace_id,),
        ).fetchall()

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
        return self.conn.execute(
            """SELECT f.*, (
                   SELECT COUNT(*)
                   FROM photos p
                   JOIN folders cf ON cf.id = p.folder_id
                   JOIN workspace_folders cwf
                     ON cwf.folder_id = cf.id
                    AND cwf.workspace_id = wf.workspace_id
                   LEFT JOIN local_folder_mappings lfm
                     ON lfm.folder_id = cf.id
                   WHERE cf.path = f.path
                      OR substr(
                           REPLACE(cf.path, '\\', '/'),
                           1,
                           length(RTRIM(REPLACE(f.path, '\\', '/'), '/') || '/')
                         ) = RTRIM(REPLACE(f.path, '\\', '/'), '/') || '/'
                      OR lfm.source_path = f.path
                      OR substr(
                           REPLACE(lfm.source_path, '\\', '/'),
                           1,
                           length(RTRIM(REPLACE(f.path, '\\', '/'), '/') || '/')
                         ) = RTRIM(REPLACE(f.path, '\\', '/'), '/') || '/'
               ) AS workspace_photo_count
               FROM folders f
               JOIN workspace_folders wf ON wf.folder_id = f.id
               WHERE wf.workspace_id = ? AND wf.is_root = 1
               ORDER BY f.path""",
            (workspace_id,),
        ).fetchall()

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
        rows = self.conn.execute(
            """SELECT DISTINCT LOWER(p.extension) AS ext
               FROM photos p
               JOIN workspace_folders wf ON wf.folder_id = p.folder_id
               JOIN folders f ON f.id = p.folder_id
                              AND f.status IN ('ok', 'partial')
               WHERE wf.workspace_id = ?
                 AND p.extension IS NOT NULL
                 AND p.extension != ''
               ORDER BY ext""",
            (ws,),
        ).fetchall()
        return [r["ext"] for r in rows]

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

        try:
            # Move pending_changes
            pending_changes_moved = 0
            for chunk in _chunks(moved_folder_ids):
                placeholders = ",".join("?" for _ in chunk)
                cur = self.conn.execute(
                    f"""UPDATE pending_changes SET workspace_id = ?
                        WHERE workspace_id = ?
                        AND photo_id IN (SELECT id FROM photos WHERE folder_id IN ({placeholders}))""",
                    [target_ws_id, source_ws_id] + chunk,
                )
                pending_changes_moved += cur.rowcount

            # Move prediction_review rows for predictions whose photo is in
            # the moved folders. Without this the accepted/rejected/group
            # metadata stays attached to the source workspace_id and the
            # target reads all predictions as 'pending' — silently dropping
            # the user's review decisions during a folder move.
            #
            # INSERT OR IGNORE into the target first, then DELETE from the
            # source. That way if the target already has a review row for
            # the same (prediction_id), we keep the target's value rather
            # than overwriting it.
            for chunk in _chunks(moved_folder_ids):
                placeholders = ",".join("?" for _ in chunk)
                self.conn.execute(
                    f"""INSERT OR IGNORE INTO prediction_review
                          (prediction_id, workspace_id, status, reviewed_at,
                           individual, group_id, vote_count, total_votes)
                        SELECT pr_rev.prediction_id, ?, pr_rev.status,
                               pr_rev.reviewed_at, pr_rev.individual,
                               pr_rev.group_id, pr_rev.vote_count,
                               pr_rev.total_votes
                        FROM prediction_review pr_rev
                        JOIN predictions p ON p.id = pr_rev.prediction_id
                        JOIN detections d ON d.id = p.detection_id
                        WHERE pr_rev.workspace_id = ?
                          AND d.photo_id IN (
                              SELECT id FROM photos WHERE folder_id IN ({placeholders})
                          )""",
                    [target_ws_id, source_ws_id] + chunk,
                )
                self.conn.execute(
                    f"""DELETE FROM prediction_review
                        WHERE workspace_id = ?
                          AND prediction_id IN (
                              SELECT pr_rev.prediction_id
                              FROM prediction_review pr_rev
                              JOIN predictions p ON p.id = pr_rev.prediction_id
                              JOIN detections d ON d.id = p.detection_id
                              WHERE pr_rev.workspace_id = ?
                                AND d.photo_id IN (
                                    SELECT id FROM photos WHERE folder_id IN ({placeholders})
                                )
                          )""",
                    [source_ws_id, source_ws_id] + chunk,
                )

            # Move manually selected Life List / Highlights representative
            # photos with the folder. If the target already has a preference
            # for the same (purpose, species), keep the target value and drop
            # the now-stale source row.
            photo_preferences_moved = 0
            species_highlights_moved = 0
            for chunk in _chunks(moved_folder_ids):
                placeholders = ",".join("?" for _ in chunk)
                cur = self.conn.execute(
                    f"""INSERT OR IGNORE INTO photo_preferences
                          (workspace_id, purpose, species, photo_id,
                           created_at, updated_at)
                        SELECT ?, purpose, species, photo_id,
                               created_at, updated_at
                        FROM photo_preferences
                        WHERE workspace_id = ?
                          AND photo_id IN (
                              SELECT id FROM photos WHERE folder_id IN ({placeholders})
                          )""",
                    [target_ws_id, source_ws_id] + chunk,
                )
                photo_preferences_moved += cur.rowcount
                self.conn.execute(
                    f"""DELETE FROM photo_preferences
                        WHERE workspace_id = ?
                          AND photo_id IN (
                              SELECT id FROM photos WHERE folder_id IN ({placeholders})
                          )""",
                    [source_ws_id] + chunk,
                )

                # Append moved highlights after the target workspace's
                # existing rows per species. Preserving the source `rank`
                # verbatim would collide with the target's ranks (rank is
                # not part of the PK), corrupting the curated order the
                # target uses in `ORDER BY rank, created_at, photo_id`.
                src_highlights = self.conn.execute(
                    f"""SELECT species, photo_id, rank, created_at, updated_at
                        FROM species_highlights
                        WHERE workspace_id = ?
                          AND photo_id IN (
                              SELECT id FROM photos WHERE folder_id IN ({placeholders})
                          )
                        ORDER BY species, rank, created_at, photo_id""",
                    [source_ws_id] + chunk,
                ).fetchall()
                by_species = {}
                for src_row in src_highlights:
                    by_species.setdefault(src_row["species"], []).append(src_row)
                for sp, sp_rows in by_species.items():
                    next_rank = int(self.conn.execute(
                        """SELECT COALESCE(MAX(rank), 0) AS max_rank
                           FROM species_highlights
                           WHERE workspace_id = ? AND species = ?""",
                        (target_ws_id, sp),
                    ).fetchone()["max_rank"] or 0) + 1
                    for src_row in sp_rows:
                        cur = self.conn.execute(
                            """INSERT OR IGNORE INTO species_highlights
                                   (workspace_id, species, photo_id, rank,
                                    created_at, updated_at)
                               VALUES (?, ?, ?, ?, ?, ?)""",
                            (
                                target_ws_id,
                                sp,
                                src_row["photo_id"],
                                next_rank,
                                src_row["created_at"],
                                src_row["updated_at"],
                            ),
                        )
                        if cur.rowcount:
                            species_highlights_moved += 1
                            next_rank += 1
                self.conn.execute(
                    f"""DELETE FROM species_highlights
                        WHERE workspace_id = ?
                          AND photo_id IN (
                              SELECT id FROM photos WHERE folder_id IN ({placeholders})
                          )""",
                    [source_ws_id] + chunk,
                )

            # Move workspace_folders: remove from source, add to target
            for chunk in _chunks(moved_folder_ids):
                placeholders = ",".join("?" for _ in chunk)
                self.conn.execute(
                    f"""DELETE FROM workspace_folders
                        WHERE workspace_id = ? AND folder_id IN ({placeholders})""",
                    [source_ws_id] + chunk,
                )
            self.conn.executemany(
                """INSERT OR IGNORE INTO workspace_folders
                   (workspace_id, folder_id, is_root) VALUES (?, ?, 0)""",
                [(target_ws_id, fid) for fid in moved_folder_ids],
            )
            for chunk in _chunks(folder_ids):
                selected_placeholders = ",".join("?" for _ in chunk)
                self.conn.execute(
                    f"""UPDATE workspace_folders
                        SET is_root = 1
                        WHERE workspace_id = ?
                          AND folder_id IN ({selected_placeholders})""",
                    [target_ws_id] + chunk,
                )

            self.conn.commit()
        except Exception:
            self.conn.rollback()
            raise

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
        row = self.conn.execute(
            "SELECT id FROM workspaces WHERE name = 'Default'"
        ).fetchone()
        if row:
            return row[0]
        return self.create_workspace("Default")

    def ensure_default_genre_keywords(self):
        """Insert the default genre keywords if none exist of type='genre'.

        Idempotent: a single existing genre keyword (user-created or otherwise)
        short-circuits the insert. Keywords are global, so this runs once per
        database (not per workspace).

        Upgrade path: if a same-name top-level keyword exists with type='general'
        (legacy free-form tag with the same name as a default genre), promote
        it to type='genre' rather than silently leaving it as 'general'. The
        UNIQUE(name, parent_id) constraint would block INSERT OR IGNORE in
        that case, leaving e.g. an existing 'Wildlife' general keyword to
        defeat _maybe_apply_auto_wildlife and backfill_wildlife_genre. Other
        explicit user types (individual, location) are preserved — the user
        meant something specific.
        """
        defaults = ("Landscape", "Sunset", "Architecture", "Abstract", "Wildlife")
        # Warm-path short-circuit: if any genre row already exists, the
        # database has already been seeded — nothing to do. Cheap (single
        # SELECT 1 LIMIT 1).
        existing = self.conn.execute(
            "SELECT 1 FROM keywords WHERE type = 'genre' LIMIT 1"
        ).fetchone()
        if existing:
            return
        # Cold / upgrade path: promote any same-name top-level 'general'
        # rows to 'genre' first, so an upgraded DB with a hand-tagged
        # 'general' Wildlife (or other default name) ends up with a
        # canonical genre row.
        for name in defaults:
            self.conn.execute(
                """UPDATE keywords SET type = 'genre'
                   WHERE name = ? COLLATE NOCASE
                     AND parent_id IS NULL AND type = 'general'""",
                (name,),
            )
        # Always guarantee a canonical genre row for each default. Skip
        # only when a same-name + same-type ('genre') row already exists.
        # If a user has previously tagged e.g. 'Landscape' as 'location'
        # (a deliberate non-default type), we still create the genre
        # 'Landscape' alongside it so the lightbox "Not Wildlife" flow
        # (which tags with type='genre') has a canonical row to reuse.
        # This intentionally permits duplicates BY NAME across different
        # types — disambiguation is handled by add_keyword's lookup,
        # which prefers same-typed matches when kw_type is supplied.
        for name in defaults:
            existing_genre = self.conn.execute(
                """SELECT id FROM keywords
                   WHERE name = ? COLLATE NOCASE
                     AND parent_id IS NULL AND type = 'genre'
                   LIMIT 1""",
                (name,),
            ).fetchone()
            if existing_genre:
                continue
            self.conn.execute(
                "INSERT INTO keywords (name, type, is_species) VALUES (?, 'genre', 0)",
                (name,),
            )
        self.conn.commit()

    _SPECIES_HIGHLIGHTS_BACKFILL_KEY = "species_highlights_from_preferences_backfill"
    _SPECIES_REPRESENTATIVES_BACKFILL_KEY = "species_representatives_from_preferences_backfill"

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
        marker = self.conn.execute(
            "SELECT value FROM db_meta WHERE key = ?",
            (self._SPECIES_HIGHLIGHTS_BACKFILL_KEY,),
        ).fetchone()
        if marker is not None:
            return
        try:
            rows = self.conn.execute(
                """SELECT workspace_id, species, photo_id
                   FROM photo_preferences
                   WHERE purpose = 'highlights'"""
            ).fetchall()
        except sqlite3.OperationalError:
            rows = []
        for row in rows:
            ws = row["workspace_id"]
            sp = row["species"]
            pid = row["photo_id"]
            existing = self.conn.execute(
                """SELECT 1 FROM species_highlights
                   WHERE workspace_id = ? AND species = ? AND photo_id = ?""",
                (ws, sp, pid),
            ).fetchone()
            if existing:
                continue
            next_rank = int(self.conn.execute(
                """SELECT COALESCE(MAX(rank), 0) AS max_rank
                   FROM species_highlights
                   WHERE workspace_id = ? AND species = ?""",
                (ws, sp),
            ).fetchone()["max_rank"] or 0) + 1
            self.conn.execute(
                """INSERT INTO species_highlights
                       (workspace_id, species, photo_id, rank,
                        created_at, updated_at)
                   VALUES (?, ?, ?, ?, datetime('now'), datetime('now'))""",
                (ws, sp, pid, next_rank),
            )
        self.conn.execute(
            "INSERT INTO db_meta(key, value) VALUES (?, '1')",
            (self._SPECIES_HIGHLIGHTS_BACKFILL_KEY,),
        )
        self.conn.commit()

    def _next_species_representative_order(self):
        row = self.conn.execute(
            "SELECT COALESCE(MAX(selected_order), 0) + 1 AS next_order "
            "FROM species_representatives"
        ).fetchone()
        return int(row["next_order"] or 1)

    def backfill_species_representatives_from_legacy_preferences(self):
        """One-shot backfill from old per-workspace representative rows.

        The current model stores representative markings globally and allows
        multiple photos per species. Older databases stored one row per
        (workspace, purpose, species), with ``species_representative`` taking
        precedence over ``life_list`` and ``highlights`` fallbacks. Copy those
        choices into the global list once so curated picks persist across
        workspaces after upgrade.
        """
        marker = self.conn.execute(
            "SELECT value FROM db_meta WHERE key = ?",
            (self._SPECIES_REPRESENTATIVES_BACKFILL_KEY,),
        ).fetchone()
        if marker is not None:
            return
        try:
            rows = self.conn.execute(
                """SELECT workspace_id, purpose, species, photo_id,
                          COALESCE(updated_at, created_at, '') AS ts
                   FROM photo_preferences
                   WHERE purpose IN ('species_representative', 'life_list', 'highlights')
                   ORDER BY CASE purpose
                              WHEN 'highlights' THEN 0
                              WHEN 'life_list' THEN 1
                              ELSE 2
                            END,
                            ts,
                            workspace_id,
                            species"""
            ).fetchall()
        except sqlite3.OperationalError:
            rows = []
        # Rows are ordered so higher-priority purposes are inserted last
        # (highlights first, then life_list, then species_representative).
        # Use UPSERT so a later canonical species_representative row for a
        # (species, photo_id) already inserted by a fallback purpose promotes
        # its selected_order to the newest value. Otherwise INSERT OR IGNORE
        # would keep the fallback's low order and the reader (which sorts by
        # selected_order DESC) could rank an unrelated life_list photo ahead
        # of the canonical representative — inverting the pre-migration
        # precedence this backfill is supposed to preserve.
        for row in rows:
            order = self._next_species_representative_order()
            self.conn.execute(
                """INSERT INTO species_representatives
                       (species, photo_id, selected_order, created_at, updated_at)
                   VALUES (?, ?, ?, datetime('now'), datetime('now'))
                   ON CONFLICT(species, photo_id) DO UPDATE SET
                       selected_order = excluded.selected_order,
                       updated_at = excluded.updated_at""",
                (row["species"], row["photo_id"], order),
            )
        self.set_meta(
            self._SPECIES_REPRESENTATIVES_BACKFILL_KEY,
            "1",
            _commit=False,
        )
        self.conn.commit()

    def migrate_legacy_keyword_types(self):
        """One-shot migration of legacy keyword type names to the canonical
        enum. Idempotent — once all rows are migrated, the warm-path
        short-circuits cheaply (single SELECT 1 LIMIT 1) so this is safe to
        call from Database.__init__ on every instantiation.

        Order matters: this runs BEFORE ensure_default_genre_keywords in
        __init__ so a legacy 'descriptive'/'event'/'people'-typed Wildlife
        gets normalized first. Otherwise the seed's UNIQUE(name, parent_id)
        INSERT OR IGNORE would silently skip Wildlife, then this migration
        would convert the legacy row to 'general', leaving the auto-Wildlife
        and backfill queries (WHERE name='Wildlife' AND type='genre') with
        no canonical row to find.
        """
        legacy = self.conn.execute(
            "SELECT 1 FROM keywords WHERE type IN ('people', 'descriptive', 'event') LIMIT 1"
        ).fetchone()
        if not legacy:
            return
        self.conn.execute("UPDATE keywords SET type = 'individual' WHERE type = 'people'")
        self.conn.execute("UPDATE keywords SET type = 'general' WHERE type = 'descriptive'")
        self.conn.execute("UPDATE keywords SET type = 'general' WHERE type = 'event'")
        self.conn.commit()

    # -- Folders --

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
        cur = self.conn.execute(
            "INSERT OR IGNORE INTO folders (path, name, parent_id) VALUES (?, ?, ?)",
            (path, name, parent_id),
        )
        commit_with_retry(self.conn)
        if cur.rowcount > 0:
            folder_id = cur.lastrowid
        else:
            row = self.conn.execute(
                "SELECT id, parent_id FROM folders WHERE path = ?", (path,)
            ).fetchone()
            folder_id = row["id"]
            if (
                parent_id is not None
                and row["parent_id"] is None
                and folder_id != parent_id
            ):
                self.conn.execute(
                    "UPDATE folders SET parent_id = ? WHERE id = ?",
                    (parent_id, folder_id),
                )
                commit_with_retry(self.conn)
        # Auto-link to active workspace
        if link_to_workspace and self._active_workspace_id is not None:
            self.add_workspace_folder(
                self._active_workspace_id,
                folder_id,
                is_root=workspace_root,
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
        ws = self._ws_id()
        return self.conn.execute(
            """WITH RECURSIVE
               visible(id) AS (
                   SELECT f.id FROM folders f
                   JOIN workspace_folders wf ON wf.folder_id = f.id
                   WHERE wf.workspace_id = ? AND f.status IN ('ok', 'partial')
               ),
               walk(start_id, current_id) AS (
                   SELECT v.id, f.parent_id
                   FROM visible v
                   JOIN folders f ON f.id = v.id
                   UNION ALL
                   SELECT w.start_id, f.parent_id
                   FROM walk w
                   JOIN folders f ON f.id = w.current_id
                   WHERE w.current_id IS NOT NULL
                     AND w.current_id NOT IN (SELECT id FROM visible)
               ),
               effective AS (
                   SELECT start_id, current_id AS parent_id
                   FROM walk
                   WHERE current_id IS NULL
                      OR current_id IN (SELECT id FROM visible)
               )
               SELECT f.id, f.path, f.name,
                      e.parent_id AS parent_id,
                      f.photo_count, f.status
               FROM folders f
               JOIN visible v ON v.id = f.id
               JOIN effective e ON e.start_id = f.id
               ORDER BY f.path""",
            (ws,),
        ).fetchall()

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
        ws = self._ws_id()
        rows = self.conn.execute(
            """WITH RECURSIVE tree(id) AS (
                   SELECT ?
                   UNION ALL
                   SELECT f.id FROM folders f
                   JOIN tree t ON f.parent_id = t.id
                   JOIN workspace_folders wf_t
                     ON wf_t.folder_id = t.id AND wf_t.workspace_id = ?
                   JOIN workspace_folders wf_f
                     ON wf_f.folder_id = f.id AND wf_f.workspace_id = ?
               )
               SELECT id FROM tree""",
            (folder_id, ws, ws),
        ).fetchall()
        return [r["id"] for r in rows]

    def get_folder(self, folder_id):
        """Return a single folder row by id, or None if not found.

        Not scoped to the active workspace — callers that need workspace
        scoping should additionally verify membership via
        ``workspace_folders``.
        """
        return self.conn.execute(
            "SELECT id, path, name, parent_id, status, photo_count "
            "FROM folders WHERE id = ?",
            (folder_id,),
        ).fetchone()

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
        rows = self.conn.execute("SELECT id, path, status FROM folders").fetchall()
        changed = 0
        for row in rows:
            exists = os.path.exists(row["path"])
            if not exists:
                new_status = "missing"
            elif row["status"] == "partial":
                new_status = "partial"
            else:
                new_status = "ok"
            if new_status != row["status"]:
                self.conn.execute(
                    "UPDATE folders SET status = ? WHERE id = ?",
                    (new_status, row["id"]),
                )
                changed += 1
        if changed:
            self.conn.commit()
        return changed

    # -- Library integrity verification --

    def record_audit_run(self, check_name, problem_count):
        """Record that an audit check ran now and what it found.

        One row per (workspace, check); re-running a check overwrites its
        previous row. The audit summary reads these to decide whether the
        archive can honestly be called intact.
        """
        self.conn.execute(
            "INSERT OR REPLACE INTO audit_runs "
            "(workspace_id, check_name, ran_at, problem_count) "
            "VALUES (?, ?, ?, ?)",
            (self._ws_id(), check_name, datetime.now().isoformat(),
             int(problem_count)),
        )
        self.conn.commit()

    def get_audit_runs(self):
        """Return {check_name: {ran_at, problem_count}} for this workspace."""
        rows = self.conn.execute(
            "SELECT check_name, ran_at, problem_count FROM audit_runs "
            "WHERE workspace_id = ?",
            (self._ws_id(),),
        ).fetchall()
        return {
            r["check_name"]: {
                "ran_at": r["ran_at"],
                "problem_count": r["problem_count"],
            }
            for r in rows
        }

    def get_integrity_photos(self):
        """Return workspace photos with the fields hash verification needs."""
        rows = self.conn.execute(
            """SELECT p.id, p.filename, p.file_hash, p.file_mtime,
                      p.hash_status, p.hash_checked_at, f.path AS folder_path
               FROM photos p
               JOIN workspace_folders wf ON wf.folder_id = p.folder_id
                    AND wf.workspace_id = ?
               JOIN folders f ON f.id = p.folder_id
                    AND f.status IN ('ok', 'partial')
               ORDER BY p.id""",
            (self._ws_id(),),
        ).fetchall()
        return [dict(r) for r in rows]

    def get_integrity_flagged(self):
        """Return workspace photos whose last hash check found a problem."""
        rows = self.conn.execute(
            """SELECT p.id AS photo_id, p.filename, p.hash_status,
                      p.hash_checked_at, f.path AS folder_path
               FROM photos p
               JOIN workspace_folders wf ON wf.folder_id = p.folder_id
                    AND wf.workspace_id = ?
               JOIN folders f ON f.id = p.folder_id
                    AND f.status IN ('ok', 'partial')
               WHERE p.hash_status IN ('modified', 'corrupt', 'unreadable')
               ORDER BY p.hash_status, p.filename""",
            (self._ws_id(),),
        ).fetchall()
        return [dict(r) for r in rows]

    def get_integrity_stats(self):
        """Return hash-verification coverage for the active workspace.

        ``unchecked`` is load-bearing for the summary banner: photos added
        after the last verify run have hash_checked_at NULL, so a green
        light can't silently cover files that were never re-hashed.
        """
        row = self.conn.execute(
            """SELECT COUNT(*) AS total,
                      SUM(CASE WHEN p.hash_checked_at IS NOT NULL
                          THEN 1 ELSE 0 END) AS checked,
                      SUM(CASE WHEN p.hash_status IN
                          ('modified', 'corrupt', 'unreadable')
                          THEN 1 ELSE 0 END) AS flagged
               FROM photos p
               JOIN workspace_folders wf ON wf.folder_id = p.folder_id
                    AND wf.workspace_id = ?
               JOIN folders f ON f.id = p.folder_id
                    AND f.status IN ('ok', 'partial')""",
            (self._ws_id(),),
        ).fetchone()
        total = row["total"] or 0
        checked = row["checked"] or 0
        return {
            "total": total,
            "checked": checked,
            "unchecked": total - checked,
            "flagged": row["flagged"] or 0,
        }

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
        if clear_file_hash and file_hash is not None:
            raise ValueError(
                "clear_file_hash and file_hash are mutually exclusive"
            )
        now = datetime.now().isoformat()
        if clear_file_hash:
            self.conn.execute(
                "UPDATE photos SET hash_status = ?, hash_checked_at = ?, "
                "file_hash = NULL WHERE id = ?",
                (status, now, photo_id),
            )
        elif file_hash is not None:
            self.conn.execute(
                "UPDATE photos SET hash_status = ?, hash_checked_at = ?, "
                "file_hash = ? WHERE id = ?",
                (status, now, file_hash, photo_id),
            )
        else:
            self.conn.execute(
                "UPDATE photos SET hash_status = ?, hash_checked_at = ? "
                "WHERE id = ?",
                (status, now, photo_id),
            )
        if commit:
            self.conn.commit()

    def get_missing_folders(self):
        """Return missing folders in the active workspace with photo counts."""
        return self.conn.execute(
            """SELECT f.id, f.path, f.name, f.parent_id,
                      COUNT(p.id) as photo_count
               FROM folders f
               JOIN workspace_folders wf ON wf.folder_id = f.id
               LEFT JOIN photos p ON p.folder_id = f.id
               WHERE wf.workspace_id = ? AND f.status = 'missing'
               GROUP BY f.id
               ORDER BY f.path""",
            (self._ws_id(),),
        ).fetchall()

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
                self.conn.execute(
                    "CREATE TEMP TABLE IF NOT EXISTS missing_subtree_ids "
                    "(id INTEGER PRIMARY KEY)"
                )
                self.conn.execute("DELETE FROM missing_subtree_ids")
                self.conn.executemany(
                    "INSERT OR IGNORE INTO missing_subtree_ids (id) VALUES (?)",
                    [(i,) for i in subtree_ids],
                )
                subtree_clause = (
                    " AND f.id IN (SELECT id FROM missing_subtree_ids)"
                )
        check_cancelled()
        rows = self.conn.execute(
            f"""SELECT p.id, p.filename, p.extension, p.file_size,
                      p.timestamp, p.working_copy_path,
                      f.id AS folder_id, f.path AS folder_path
               FROM photos p
               JOIN folders f ON p.folder_id = f.id
               JOIN workspace_folders wf ON wf.folder_id = f.id
               WHERE wf.workspace_id = ? AND f.status != 'missing'{subtree_clause}
               ORDER BY f.path, p.filename""",
            params,
        ).fetchall()
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
        target = _path_for_subtree_match(path)
        best_id = None
        best_len = -1
        for row in self.conn.execute("SELECT id, path FROM folders"):
            if exclude_id is not None and row["id"] == exclude_id:
                continue
            cand = _path_for_subtree_match(row["path"])
            if target == cand or not target.startswith(cand + "/"):
                continue
            if len(cand) > best_len:
                best_id = row["id"]
                best_len = len(cand)
        return best_id

    def _relink_parents_by_path(self, folder_ids):
        """Re-derive ``parent_id`` from the current ``path`` for each folder.

        Relocations and merges rewrite ``path`` but leave ``parent_id``
        pinned to the pre-move parent, which mis-nests the folder in the
        browse tree. Call this after path rewrites — all affected paths must
        already be committed to the rows so ancestor lookup sees them.
        """
        for fid in folder_ids:
            row = self.conn.execute(
                "SELECT path FROM folders WHERE id = ?", (fid,)
            ).fetchone()
            if row is None:
                continue
            self.conn.execute(
                "UPDATE folders SET parent_id = ? WHERE id = ?",
                (self.nearest_ancestor_folder_id(row["path"], exclude_id=fid), fid),
            )

    def relocate_folder(self, folder_id, new_path):
        """Update folder path and set status to 'ok'.

        Also checks if missing child folders exist at corresponding paths
        under new_path. If they do, relocates them too.

        If new_path is already tracked by another folder, merges photos from
        the missing folder into the existing one and removes the missing folder.

        Returns list of child folder dicts that were also relocated.
        """
        # Check for duplicate path
        conflict = self.conn.execute(
            "SELECT id FROM folders WHERE path = ? AND id != ?",
            (new_path, folder_id),
        ).fetchone()
        if conflict:
            # Only merge if source folder is missing; for ok folders, reject
            source_row = self.conn.execute(
                "SELECT status, path FROM folders WHERE id = ?", (folder_id,)
            ).fetchone()
            if source_row and source_row["status"] == "missing":
                # Revalidate: if original path came back, refresh status instead
                if os.path.isdir(source_row["path"]):
                    self.conn.execute(
                        "UPDATE folders SET status = 'ok' WHERE id = ?",
                        (folder_id,),
                    )
                    self.conn.commit()
                    raise ValueError(
                        f"Path is already tracked as folder {conflict['id']}"
                    )
                return self._merge_into_existing(folder_id, conflict["id"], new_path)
            raise ValueError(
                f"Path is already tracked as folder {conflict['id']}"
            )

        old_row = self.conn.execute(
            "SELECT path FROM folders WHERE id = ?", (folder_id,)
        ).fetchone()
        old_path = old_row["path"] if old_row else ""

        self.conn.execute(
            "UPDATE folders SET path = ?, status = 'ok' WHERE id = ?",
            (new_path, folder_id),
        )

        # Check missing children for cascade
        cascaded = []
        skipped_prefixes = []
        children = self.conn.execute(
            """SELECT id, path FROM folders
               WHERE status = 'missing'
                 AND substr(REPLACE(path, '\\', '/'), 1, ?) = ?
               ORDER BY length(REPLACE(path, '\\', '/')),
                        REPLACE(path, '\\', '/')""",
            (len(_subtree_prefix(old_path)), _subtree_prefix(old_path)),
        ).fetchall()
        for child in children:
            # Skip descendants of conflicted folders
            child_match_path = _path_for_subtree_match(child["path"])
            if any(child_match_path.startswith(p + "/") for p in skipped_prefixes):
                continue
            relative = _subtree_relative(child["path"], old_path)
            candidate = _join_subtree_path(new_path, relative)
            if os.path.exists(candidate):
                # Skip if another folder already has this path
                child_conflict = self.conn.execute(
                    "SELECT id FROM folders WHERE path = ? AND id != ?",
                    (candidate, child["id"]),
                ).fetchone()
                if child_conflict:
                    skipped_prefixes.append(child_match_path)
                    continue
                self.conn.execute(
                    "UPDATE folders SET path = ?, status = 'ok' WHERE id = ?",
                    (candidate, child["id"]),
                )
                cascaded.append({"id": child["id"], "old_path": child["path"], "new_path": candidate})

        self._relink_parents_by_path([folder_id] + [c["id"] for c in cascaded])
        self.conn.commit()
        return cascaded

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
        old_row = self.conn.execute(
            "SELECT path FROM folders WHERE id = ?", (source_folder_id,)
        ).fetchone()
        old_path = old_row["path"] if old_row else ""

        # Get photos from the missing folder
        source_photos = self.conn.execute(
            "SELECT id, filename FROM photos WHERE folder_id = ?",
            (source_folder_id,),
        ).fetchall()

        # Reassign or drop each photo
        drop_ids = []
        for photo in source_photos:
            existing = self.conn.execute(
                "SELECT id FROM photos WHERE folder_id = ? AND filename = ?",
                (target_folder_id, photo["filename"]),
            ).fetchone()
            if existing:
                drop_ids.append(photo["id"])
            elif os.path.exists(os.path.join(new_path, photo["filename"])):
                self.conn.execute(
                    "UPDATE photos SET folder_id = ? WHERE id = ?",
                    (target_folder_id, photo["id"]),
                )
            else:
                # File doesn't exist on disk at target — drop phantom record
                drop_ids.append(photo["id"])

        # Delete duplicate photos and their associated data
        if drop_ids:
            ph = ",".join("?" for _ in drop_ids)
            self.conn.execute(f"DELETE FROM photo_keywords WHERE photo_id IN ({ph})", drop_ids)
            self.conn.execute(f"DELETE FROM pending_changes WHERE photo_id IN ({ph})", drop_ids)
            self.conn.execute(f"DELETE FROM detections WHERE photo_id IN ({ph})", drop_ids)
            self.conn.execute(f"DELETE FROM photos WHERE id IN ({ph})", drop_ids)

        # Reparent child folders from source to target
        self.conn.execute(
            "UPDATE folders SET parent_id = ? WHERE parent_id = ?",
            (target_folder_id, source_folder_id),
        )

        # Transfer workspace visibility from source to target while preserving
        # whether the source link was a user-facing root or a materialized
        # descendant.
        workspace_links = self.conn.execute(
            "SELECT workspace_id, is_root FROM workspace_folders WHERE folder_id = ?",
            (source_folder_id,),
        ).fetchall()
        for link in workspace_links:
            self.conn.execute(
                """INSERT OR IGNORE INTO workspace_folders
                   (workspace_id, folder_id, is_root) VALUES (?, ?, ?)""",
                (link["workspace_id"], target_folder_id, link["is_root"]),
            )
            if link["is_root"]:
                self.conn.execute(
                    """UPDATE workspace_folders
                       SET is_root = 1
                       WHERE workspace_id = ? AND folder_id = ?""",
                    (link["workspace_id"], target_folder_id),
                )

        # Remove source folder
        self.conn.execute(
            "DELETE FROM workspace_folders WHERE folder_id = ?",
            (source_folder_id,),
        )
        self.conn.execute(
            "DELETE FROM folders WHERE id = ?", (source_folder_id,)
        )

        # Ensure target folder is marked ok and recompute its photo count
        self.conn.execute(
            "UPDATE folders SET status = 'ok', photo_count = "
            "(SELECT COUNT(*) FROM photos WHERE folder_id = ?) "
            "WHERE id = ?",
            (target_folder_id, target_folder_id),
        )

        # Cascade to missing children (same logic as relocate_folder)
        cascaded = []
        skipped_prefixes = []
        children = self.conn.execute(
            """SELECT id, path FROM folders
               WHERE status = 'missing'
                 AND substr(REPLACE(path, '\\', '/'), 1, ?) = ?
               ORDER BY length(REPLACE(path, '\\', '/')),
                        REPLACE(path, '\\', '/')""",
            (len(_subtree_prefix(old_path)), _subtree_prefix(old_path)),
        ).fetchall()
        for child in children:
            child_match_path = _path_for_subtree_match(child["path"])
            if any(child_match_path.startswith(p + "/") for p in skipped_prefixes):
                continue
            relative = _subtree_relative(child["path"], old_path)
            candidate = _join_subtree_path(new_path, relative)
            if os.path.exists(candidate):
                child_conflict = self.conn.execute(
                    "SELECT id FROM folders WHERE path = ? AND id != ?",
                    (candidate, child["id"]),
                ).fetchone()
                if child_conflict:
                    skipped_prefixes.append(child_match_path)
                    continue
                self.conn.execute(
                    "UPDATE folders SET path = ?, status = 'ok' WHERE id = ?",
                    (candidate, child["id"]),
                )
                cascaded.append({"id": child["id"], "old_path": child["path"], "new_path": candidate})

        self._relink_parents_by_path([c["id"] for c in cascaded])
        if commit:
            self.conn.commit()
        return cascaded

    # -- Move operations --

    def create_move_rule(self, name, destination, criteria):
        """Create a saved move rule. Returns the rule id."""
        import json as _json
        cur = self.conn.execute(
            "INSERT INTO move_rules (name, destination, criteria) VALUES (?, ?, ?)",
            (name, destination, _json.dumps(criteria)),
        )
        self.conn.commit()
        return cur.lastrowid

    def get_move_rule(self, rule_id):
        """Return a single move rule by id."""
        return self.conn.execute(
            "SELECT * FROM move_rules WHERE id = ?", (rule_id,)
        ).fetchone()

    def list_move_rules(self):
        """Return all saved move rules ordered by name."""
        return self.conn.execute(
            "SELECT * FROM move_rules ORDER BY name"
        ).fetchall()

    def update_move_rule(self, rule_id, name=_UNSET, destination=_UNSET, criteria=_UNSET):
        """Update fields on a move rule."""
        import json as _json
        sets, params = [], []
        if name is not _UNSET:
            sets.append("name = ?")
            params.append(name)
        if destination is not _UNSET:
            sets.append("destination = ?")
            params.append(destination)
        if criteria is not _UNSET:
            sets.append("criteria = ?")
            params.append(_json.dumps(criteria))
        if not sets:
            return
        params.append(rule_id)
        self.conn.execute(f"UPDATE move_rules SET {', '.join(sets)} WHERE id = ?", params)
        self.conn.commit()

    def delete_move_rule(self, rule_id):
        """Delete a move rule."""
        self.conn.execute("DELETE FROM move_rules WHERE id = ?", (rule_id,))
        self.conn.commit()

    def touch_move_rule(self, rule_id):
        """Update last_run_at timestamp on a move rule."""
        self.conn.execute(
            "UPDATE move_rules SET last_run_at = datetime('now') WHERE id = ?",
            (rule_id,),
        )
        self.conn.commit()

    def batch_update_photo_folder(self, photo_ids, target_folder_id):
        """Move photos to target folder in a single transaction."""
        if not photo_ids:
            return
        placeholders = ",".join("?" for _ in photo_ids)
        self.conn.execute(
            f"UPDATE photos SET folder_id = ? WHERE id IN ({placeholders})",
            [target_folder_id] + list(photo_ids),
        )
        self.conn.commit()

    def move_folder_path(self, folder_id, new_path, new_name=None):
        """Update a folder's path and cascade to all children.

        Unlike relocate_folder (which only updates missing children),
        this updates ALL child folders regardless of status. ``new_name`` is
        used when the root folder is renamed as part of the move; descendants
        keep their existing names.
        """
        old_row = self.conn.execute(
            "SELECT path FROM folders WHERE id = ?", (folder_id,)
        ).fetchone()
        if not old_row:
            return
        old_path = old_row["path"]
        if new_name is None:
            self.conn.execute(
                "UPDATE folders SET path = ? WHERE id = ?", (new_path, folder_id)
            )
        else:
            self.conn.execute(
                "UPDATE folders SET path = ?, name = ? WHERE id = ?",
                (new_path, new_name, folder_id),
            )
        children = self.conn.execute(
            """SELECT id, path FROM folders
               WHERE substr(REPLACE(path, '\\', '/'), 1, ?) = ?""",
            (len(_subtree_prefix(old_path)), _subtree_prefix(old_path)),
        ).fetchall()
        for child in children:
            child_new = _join_subtree_path(
                new_path, _subtree_relative(child["path"], old_path)
            )
            self.conn.execute(
                "UPDATE folders SET path = ? WHERE id = ?", (child_new, child["id"])
            )
        self.conn.commit()

    def _active_ws_root_ancestor_exists(self, workspace_id, path):
        """True if ``workspace_id`` has an ``is_root=1`` folder that equals or
        is an ancestor of ``path``.

        Used by the merge to decide whether the archive base should itself
        become a workspace root. Comparison is platform-neutral (``\\`` folded
        to ``/``, trailing slashes stripped) so a Windows-style stored root
        still matches a forward-slash archive path.
        """
        target = _path_for_subtree_match(path)
        rows = self.conn.execute(
            """SELECT f.path FROM workspace_folders wf
               JOIN folders f ON f.id = wf.folder_id
               WHERE wf.workspace_id = ? AND wf.is_root = 1""",
            (workspace_id,),
        ).fetchall()
        for r in rows:
            root = _path_for_subtree_match(r["path"])
            if target == root or target.startswith(root + "/"):
                return True
        return False

    def _active_ws_root_descendant_exists(self, workspace_id, path):
        """True if ``workspace_id`` has a strict root descendant of ``path``."""
        target = _path_for_subtree_match(path)
        rows = self.conn.execute(
            """SELECT f.path FROM workspace_folders wf
               JOIN folders f ON f.id = wf.folder_id
               WHERE wf.workspace_id = ? AND wf.is_root = 1""",
            (workspace_id,),
        ).fetchall()
        prefix = target + "/"
        for r in rows:
            root = _path_for_subtree_match(r["path"])
            if root.startswith(prefix):
                return True
        return False

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
        target = _path_for_subtree_match(path)
        roots = [
            _path_for_subtree_match(r["path"])
            for r in self.conn.execute(
                """SELECT f.path FROM workspace_folders wf
                   JOIN folders f ON f.id = wf.folder_id
                   WHERE wf.workspace_id = ? AND wf.is_root = 1""",
                (workspace_id,),
            ).fetchall()
        ]
        rows = self.conn.execute(
            """SELECT wf.folder_id, f.path FROM workspace_folders wf
               JOIN folders f ON f.id = wf.folder_id
               WHERE wf.workspace_id = ? AND wf.is_root = 0""",
            (workspace_id,),
        ).fetchall()
        target_prefix = target + "/"
        prune_ids = []
        for row in rows:
            current = _path_for_subtree_match(row["path"])
            current_prefix = current + "/"
            if (current != target
                    and not current.startswith(target_prefix)
                    and not target.startswith(current_prefix)):
                continue
            if any(current == root or current.startswith(root + "/")
                   for root in roots):
                continue
            prune_ids.append(row["folder_id"])

        for chunk in _chunks(prune_ids):
            placeholders = ",".join("?" for _ in chunk)
            self.conn.execute(
                f"""DELETE FROM workspace_folders
                    WHERE workspace_id = ? AND folder_id IN ({placeholders})""",
                [workspace_id] + chunk,
            )
        if prune_ids:
            self.conn.commit()
            self._new_images_cache.invalidate_workspaces(
                self._db_path, [workspace_id])

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
        """
        staged_root = self.conn.execute(
            "SELECT path FROM folders WHERE id = ?", (staged_root_id,)
        ).fetchone()
        if not staged_root:
            return {"new_photos": 0, "new_folders": 0,
                    "merged_folders": 0, "already_present": 0,
                    "dropped_photo_ids": []}
        staged_root_path = staged_root["path"]
        ws = self._ws_id()

        # Ensure the existing archive base — and every folder row already
        # below it — is linked to the active workspace before any staged
        # photo is reparented onto one of those pre-existing folder rows.
        # If the archive was scanned only under a different workspace, the
        # else-branch UPDATE below would move photos onto a ``target["id"]``
        # that has no ``workspace_folders`` row for ``ws``; workspace-scoped
        # photo queries join ``workspace_folders`` on ``p.folder_id`` and
        # would silently drop every merged-in photo. ``add_workspace_folder``
        # pulls the whole subtree (path-prefix), so a single link on the
        # archive base covers every existing descendant the reconciliation
        # can hit.
        #
        # Root the base ONLY when the active workspace has no existing root
        # ancestor of it. For an ancestor merge (``/Photos`` is already a
        # workspace root, base ``/Photos/USA``), rooting the base would create
        # a SECOND overlapping workspace root inside the first — the exact
        # duplicate-root state the tracked-overlap guards exist to prevent
        # (``add_workspace_folder(is_root=True)`` only demotes rows inside the
        # base's own subtree, so the outer ``/Photos`` root would survive). In
        # that case just LINK the base non-root; the existing ancestor root
        # keeps covering it. When there is no root ancestor (the base IS the
        # archive the user imported into), the base is the natural root.
        #
        # When the base is ALREADY a workspace root for ``ws``, skip the
        # ``add_workspace_folder`` call entirely instead of passing
        # ``is_root=False`` — the descendant subtree is already linked from
        # when the base was rooted, and calling with ``is_root=False`` would
        # silently rely on ``add_workspace_folder``'s no-op-on-existing-row
        # behavior to preserve the root flag. Making the "already root" case
        # an explicit skip keeps the merge safe if that invariant ever changes.
        archive_row = self.conn.execute(
            "SELECT id, status FROM folders WHERE path = ?", (archive_path,)
        ).fetchone()
        if archive_row:
            existing_link = self.conn.execute(
                "SELECT is_root FROM workspace_folders "
                "WHERE workspace_id = ? AND folder_id = ?",
                (ws, archive_row["id"]),
            ).fetchone()
            if existing_link is None or existing_link["is_root"] == 0:
                # Not root of ws (unlinked, or linked non-root): link the
                # subtree, and root the base only if it would not replace an
                # existing narrower or broader root. A strict descendant root
                # means the workspace is intentionally scoped inside this
                # archive (e.g. ``/Photos/USA/2026`` while importing into
                # ``/Photos/USA``). In that shape, do not link the broad base
                # at all: even a non-root link materializes every descendant
                # and would make archive siblings part of workspace queries.
                has_root_ancestor = self._active_ws_root_ancestor_exists(
                    ws, archive_path)
                has_root_descendant = self._active_ws_root_descendant_exists(
                    ws, archive_path)
                if has_root_descendant and not has_root_ancestor:
                    self._prune_ws_nonroot_links_outside_roots(
                        ws, archive_path)
                    self._materialize_workspace_descendants(ws)
                else:
                    self.add_workspace_folder(
                        ws,
                        archive_row["id"],
                        is_root=not has_root_ancestor,
                    )
            # else: base is already a workspace root — nothing to do.
            # If the archive base was marked ``missing`` at a previous health
            # scan (drive unmounted at the time), the storage preflight has
            # since verified the volume is mounted and the rsync copy landed
            # files on disk — flip it back to ``ok`` so ws-scoped photo queries
            # (which filter ``folders.status IN ('ok', 'partial')``) show the
            # merged photos instead of hiding them until the next health scan
            # happens to reconcile. Only migrate ``missing`` → ``ok``: leave
            # ``partial`` alone (the row still has unverified photos) and
            # ``ok`` unchanged.
            if archive_row["status"] == "missing":
                self.conn.execute(
                    "UPDATE folders SET status = 'ok' WHERE id = ?",
                    (archive_row["id"],),
                )
        elif not self._active_ws_root_ancestor_exists(ws, archive_path):
            # ``archive_path`` has no folder row yet — it's a brand-new
            # subfolder inside an already-tracked archive that the staged
            # root will be repointed onto below. When the tracked archive
            # was scanned only under a DIFFERENT workspace, the active
            # workspace has no link to any of it, no root ancestor covers
            # ``archive_path``, and the staged-root demotion further down
            # (``UPDATE workspace_folders SET is_root = 0``) leaves ``ws``
            # with no ``is_root=1`` row for the merged tree at all —
            # ``get_workspace_folder_roots()`` filters on ``is_root=1``, so
            # the merged archive silently disappears from the active ws
            # even though the import reports success. Walk up to find the
            # deepest tracked ancestor and root it in ``ws`` so the merged
            # tree has a visible anchor. The intermediate-materialization
            # block below then links each freshly-created intermediate as a
            # non-root descendant under this new root.
            probe = os.path.dirname(archive_path)
            while probe and probe != os.path.dirname(probe):
                ancestor_row = self.conn.execute(
                    "SELECT id FROM folders WHERE path = ?", (probe,)
                ).fetchone()
                if ancestor_row is not None:
                    if not self._active_ws_root_descendant_exists(ws, probe):
                        self.add_workspace_folder(
                            ws, ancestor_row["id"], is_root=True)
                    else:
                        # Descendant-root guard fires: the workspace is
                        # scoped narrower than this ancestor, so rooting
                        # it would widen the scope past the intended root.
                        # But any pre-existing ``is_root=0`` link on this
                        # ancestor (or on descendants below it that no
                        # root still covers) would let
                        # ``_materialize_workspace_descendants`` — called
                        # by later ``get_workspace_folders()`` reads —
                        # pull the broader subtree back into the workspace
                        # and defeat the scoped merge. Prune those
                        # uncovered non-root links now, matching the
                        # cleanup the ``existing_link is None or
                        # is_root == 0`` branch above already performs
                        # via ``_prune_ws_nonroot_links_outside_roots``.
                        self._prune_ws_nonroot_links_outside_roots(
                            ws, probe)
                    break
                probe = os.path.dirname(probe)

        # Materialize any missing intermediate folder rows between the deepest
        # existing catalog ancestor and ``archive_path``'s parent (inclusive)
        # BEFORE the reconciliation loop reads ``parent_id`` by path.
        #
        # Nested archive destinations expose this gap: when ``/Photos`` is
        # tracked and the user imports to ``/Photos/2026/NewShoot``, the
        # storage preflight materializes ``/Photos/2026`` ON DISK (rsync needs
        # the transfer parent to exist) but never opens a folder row for it —
        # the scanner didn't visit that path. Without the row, the loop's
        # ``WHERE path = ?`` lookup for the staged root's target-parent
        # returns nothing, and the UPDATE below repoints the staged root to
        # ``archive_path`` with ``parent_id=NULL`` — floating it outside the
        # managed archive tree and breaking every parent-based subtree
        # operation (cascade path renames, ``_folder_subtree_ids_by_path``,
        # etc.). Walk up from the archive parent until an existing row shows
        # up (or the filesystem root). When no anchor is found (no tracked
        # ancestor row in the catalog), the destination is a brand-new
        # unrelated root — leave the loop's ``parent_id=NULL`` alone, which
        # is the expected shape for a root. Otherwise insert missing rows
        # top-down so each child's ``parent_id`` resolves to its freshly-
        # created parent, and link each to the active workspace non-root
        # ONLY when an existing workspace root actually covers the
        # intermediate. Linking non-root unconditionally would leak: if the
        # workspace is scoped to a narrower root (e.g. ``/archive/USA/2026``)
        # and the merge target is a sibling like ``/archive/USA/2027/Trip``,
        # the descendant-root guard above suppresses rooting ``/archive/USA``,
        # so no workspace root covers the ``/archive/USA/2027`` intermediate.
        # A non-root link there still makes 2027's subtree visible via
        # ``_materialize_workspace_descendants`` (called by
        # ``get_workspace_folders``), defeating the scoped-merge behavior.
        missing_intermediates = []
        probe = os.path.dirname(archive_path)
        anchor_found = False
        while probe and probe != os.path.dirname(probe):
            row = self.conn.execute(
                "SELECT id FROM folders WHERE path = ?", (probe,)
            ).fetchone()
            if row is not None:
                anchor_found = True
                break
            missing_intermediates.append(probe)
            probe = os.path.dirname(probe)
        if anchor_found:
            for mid_path in reversed(missing_intermediates):
                mid_parent = os.path.dirname(mid_path)
                parent_row = self.conn.execute(
                    "SELECT id FROM folders WHERE path = ?", (mid_parent,)
                ).fetchone()
                parent_id_for_mid = (
                    parent_row["id"] if parent_row else None)
                name = os.path.basename(mid_path) or mid_path
                # Two concurrent local-processing jobs targeting siblings
                # inside the same tracked archive (e.g. ``/Photos/2026/A`` and
                # ``/Photos/2026/B`` while only ``/Photos`` is tracked) can
                # each snapshot the shared intermediate (``/Photos/2026``)
                # as missing above, then race to insert its ``folders`` row
                # here. The final archive paths don't overlap, so the
                # storage-destination reservation doesn't serialize them; the
                # loser's plain INSERT would hit the ``folders.path`` UNIQUE
                # constraint AFTER all staging/processing work is done.
                # ``INSERT OR IGNORE`` + re-query keeps the loser's merge
                # progressing against whichever row won the race — the same
                # intermediate is folder-idempotent (same path, same tracked
                # ancestor parent). ``cur.lastrowid`` is 0 on an ignored
                # insert, so read the id back via ``WHERE path = ?``.
                cur = self.conn.execute(
                    "INSERT OR IGNORE INTO folders (path, name, parent_id) "
                    "VALUES (?, ?, ?)",
                    (mid_path, name, parent_id_for_mid),
                )
                if cur.rowcount:
                    mid_id = cur.lastrowid
                else:
                    mid_id = self.conn.execute(
                        "SELECT id FROM folders WHERE path = ?", (mid_path,)
                    ).fetchone()["id"]
                if self._active_ws_root_ancestor_exists(ws, mid_path):
                    self.add_workspace_folder(
                        ws, mid_id, is_root=False)

        # Snapshot staged folders root-first (shallowest path first) so a
        # parent's target row exists before its children are processed.
        prefix = _subtree_prefix(staged_root_path)
        staged_folders = self.conn.execute(
            """SELECT id, path FROM folders
               WHERE path = ? OR substr(REPLACE(path, '\\', '/'), 1, ?) = ?
               ORDER BY length(path) ASC""",
            (staged_root_path, len(prefix), prefix),
        ).fetchall()

        counts = {"new_photos": 0, "new_folders": 0,
                  "merged_folders": 0, "already_present": 0,
                  "dropped_photo_ids": []}
        # Staged folders that fold into an existing target row are deleted only
        # after every staged folder has been processed. Deleting eagerly would
        # hit a FK violation when a not-yet-reparented staged child still points
        # at the staged parent we are removing.
        to_delete = []
        # Map of target-path -> folder id for folders already processed in this
        # run, so a child can fall back to its parent's id (Fix I2) even if the
        # parent's row isn't yet findable by path lookup.
        last_target_parent = {}

        # Wrap the reconciliation body in try/except + rollback so a mid-run
        # exception (unexpected row shape, raised error from the
        # case-insensitivity probe, etc.) can't leave a partially-applied
        # merge sitting on the connection — an unrelated later commit would
        # otherwise persist half-reparented folders/photos. Matches the
        # convention used by other multi-step mutation methods in this file
        # (``delete_folder``, ``move_folders_to_workspace``,
        # ``_merge_duplicate_keywords_pass``). Archive-base linking and
        # missing-intermediate materialization above each commit through
        # their own ``add_workspace_folder`` calls, so their success is
        # persisted independently — that's the desired shape here: partial
        # progress on preparing the archive tree is a valid state a retry
        # can build on, but partial photo/folder reparenting is not.
        try:
            for sf in staged_folders:
                rel = _subtree_relative(sf["path"], staged_root_path)
                target_path = _join_subtree_path(archive_path, rel)
                target = self.conn.execute(
                    "SELECT id FROM folders WHERE path = ?", (target_path,)
                ).fetchone()
                parent_path = os.path.dirname(target_path)
                parent_row = self.conn.execute(
                    "SELECT id FROM folders WHERE path = ?", (parent_path,)
                ).fetchone()
                parent_id = parent_row["id"] if parent_row else None

                # Defensive: a non-root staged folder whose target-parent row is
                # missing would silently get parent_id=NULL, breaking the chain.
                # The scanner normally materializes every intermediate, so this
                # is an unenforced invariant — log it, and fall back to the last
                # processed target-parent id when we have one.
                if (parent_id is None
                        and target_path != archive_path
                        and parent_path and parent_path != target_path):
                    fallback = last_target_parent.get(parent_path)
                    if fallback is not None:
                        log.warning(
                            "merge_staged_tree_into_archive: no folder row "
                            "for target parent %r of %r; falling back to "
                            "id %s",
                            parent_path, target_path, fallback,
                        )
                        parent_id = fallback
                    else:
                        log.warning(
                            "merge_staged_tree_into_archive: no folder row "
                            "for target parent %r of %r; leaving parent_id "
                            "NULL",
                            parent_path, target_path,
                        )

                if target is None:
                    target_in_workspace = self._active_ws_root_ancestor_exists(
                        ws, target_path)
                    # New folder under the archive: repoint + reparent + link.
                    self.conn.execute(
                        "UPDATE folders SET path = ?, parent_id = ? "
                        "WHERE id = ?",
                        (target_path, parent_id, sf["id"]),
                    )
                    # Use the non-committing variant so the folder path/parent_
                    # id UPDATE just above stays in the outer transaction — the
                    # public ``add_workspace_folder`` commits, and a mid-loop
                    # commit would persist a partial reparent that the outer
                    # rollback (below) could no longer undo if a later staged
                    # folder raised.
                    if target_in_workspace:
                        self._add_workspace_folder_no_commit(
                            ws, sf["id"], is_root=False)
                        # The staging scan registers each photo-bearing leaf as
                        # its own workspace ROOT (scanner restrict_dirs =>
                        # is_root=1). Once that leaf is folded under the existing
                        # archive base it must become a plain descendant,
                        # otherwise the merge leaves a stray second workspace
                        # root inside the archive — the exact overlap the
                        # tracked-ancestor guard was meant to prevent.
                        # add_workspace_folder's INSERT OR IGNORE can't downgrade
                        # an existing is_root=1 row, so demote it explicitly here.
                        self.conn.execute(
                            "UPDATE workspace_folders SET is_root = 0 "
                            "WHERE workspace_id = ? AND folder_id = ?",
                            (ws, sf["id"]),
                        )
                    else:
                        self.conn.execute(
                            "DELETE FROM workspace_folders "
                            "WHERE workspace_id = ? AND folder_id = ?",
                            (ws, sf["id"]),
                        )
                    # Every staged photo in a brand-new folder is newly
                    # archived.
                    new_count = self.conn.execute(
                        "SELECT COUNT(*) c FROM photos WHERE folder_id = ?",
                        (sf["id"],),
                    ).fetchone()["c"]
                    counts["new_photos"] += new_count
                    counts["new_folders"] += 1
                    # Record this folder's id so a child whose path-parent is
                    # this folder can resolve its parent even before counts
                    # re-query.
                    last_target_parent[target_path] = sf["id"]
                else:
                    # Existing folder: move photos in, drop filename-collisions.
                    # Restore the target row to visible status if it was marked
                    # ``missing`` — same rationale as the archive-base status
                    # flip above. We just verified files exist at
                    # ``target_path`` (rsync + verify), so a lingering
                    # ``missing`` from an earlier health scan would hide the
                    # newly-merged photos from workspace-scoped queries. Only
                    # migrate ``missing`` → ``ok``; leave ``partial``/``ok``
                    # alone. Runs inside the outer try/except so a later
                    # exception still rolls this back.
                    self.conn.execute(
                        "UPDATE folders SET status = 'ok' "
                        "WHERE id = ? AND status = 'missing'",
                        (target["id"],),
                    )
                    staged_photos = list(self.conn.execute(
                        "SELECT id, filename, file_hash, file_size "
                        "FROM photos WHERE folder_id = ?",
                        (sf["id"],),
                    ))
                    # Detect collisions against the ACTUAL target volume's case
                    # rules — not SQLite's default case-sensitive TEXT compare.
                    # On a case-insensitive volume (default macOS APFS, Windows
                    # NTFS), a target row/file named ``IMG.RAF`` and a staged
                    # ``img.raf`` are the same on-disk file: rsync
                    # ``--ignore-existing`` treats them as already present and
                    # skips the copy. A case-sensitive SQL match would miss
                    # that, fall into the else-branch reparent below, and land
                    # TWO catalog rows in one folder pointing at the same
                    # on-disk file (SQLite text-equality is case-sensitive, so
                    # UNIQUE(folder_id, filename) would not fire to catch the
                    # mistake). Build the collision map by normalizing
                    # filenames with the target filesystem's case rules
                    # instead. Probes ``target_path``'s deepest existing
                    # ancestor, so a fresh subfolder inherits its mount's
                    # behavior.
                    from move import _case_insensitive_root
                    target_folds_case = (
                        _case_insensitive_root(target_path) is not None)
                    normalize = (str.casefold if target_folds_case
                                 else (lambda s: s))
                    existing_by_key = {}
                    for row in self.conn.execute(
                        "SELECT id, filename, file_hash, file_size "
                        "FROM photos WHERE folder_id = ?",
                        (target["id"],),
                    ):
                        # First writer wins on the (unlikely) chance two
                        # case-alias rows already coexist in the target folder
                        # from a pre-fix catalog.
                        existing_by_key.setdefault(
                            normalize(row["filename"]),
                            {"id": row["id"], "filename": row["filename"],
                             "file_hash": row["file_hash"],
                             "file_size": row["file_size"]},
                        )

                    # A filename-collision alone is NOT enough to drop the
                    # staged photo as ``already_present``. The drop is only
                    # safe when the collision is REAL on disk — i.e. the
                    # target row accurately describes the bytes at
                    # ``target_path/filename``. rsync ``--ignore-existing``
                    # skipped the staged copy only when a byte-identical
                    # archived file was already there (a DIFFERING file
                    # would have aborted the move upstream in the
                    # content-conflict check). If the target catalog row is
                    # stale — its file was MISSING on disk before the
                    # archive step — the upstream check never fired (no
                    # dest file to compare) and rsync COPIED the staged
                    # bytes into place. By the time we run here rsync has
                    # already finished, so ``os.path.exists`` returns True
                    # in BOTH the real-collision and the phantom-row cases
                    # and cannot tell them apart. Require MATCHING recorded
                    # ``file_hash`` on both the staged photo and the target
                    # row to call a collision "real": a hash match means
                    # the row correctly describes what is on disk (dropping
                    # the staged row is safe); a hash mismatch means rsync
                    # replaced a missing file with fresh staged bytes and
                    # the row is stale. When either recorded hash is
                    # missing there is no reliable post-copy signal —
                    # ``file_size`` alone can coincidentally match a
                    # phantom row's stored size (empty XMP sidecars, small
                    # metadata files), and hashing the on-disk file to
                    # compare against the STAGED hash matches trivially in
                    # BOTH the real-collision case (byte-identical by
                    # definition) and the phantom case (rsync wrote the
                    # staged bytes). Default to phantom-replacement below
                    # when unverifiable: it preserves the freshly-imported
                    # pipeline output at the cost of any accumulated
                    # metadata on an unhashed archive row, which is the
                    # strictly-safer direction — silently dropping the
                    # newly-imported photo behind a same-size stale row is
                    # the opposite (and worse) failure. In the phantom
                    # case delete the stale target row and reparent the
                    # staged photo in its place so the surviving catalog
                    # row describes the bytes actually on disk. This also
                    # avoids a UNIQUE(folder_id, filename) violation from
                    # moving the staged row onto a folder that still holds
                    # the same basename.
                    #
                    # ``staged_normalized_claimed`` tracks case-normalized
                    # filenames already reparented into ``target`` in this
                    # pass — the intra-staged analogue of
                    # ``existing_by_key``. On a case-insensitive target
                    # volume, two staged files whose names differ only in
                    # case (e.g. staged on a case-sensitive disk archiving
                    # to APFS/SMB) collide on the same on-disk destination:
                    # rsync ``--ignore-existing`` writes only the FIRST
                    # file and silently skips the rest, so any later
                    # staged row describes bytes that never landed on
                    # disk. Without this tracker every such row also gets
                    # reparented into ``target``, leaving multiple catalog
                    # rows for the same on-disk file (the SQL
                    # ``UNIQUE(folder_id, filename)`` doesn't fire because
                    # the recorded filenames differ in case). Drop
                    # subsequent case-alias staged rows as
                    # ``already_present`` — the safe direction since their
                    # bytes are unrepresented on disk. On case-sensitive
                    # targets ``normalize`` is identity, so different-case
                    # names have different keys and this tracker never
                    # triggers.
                    staged_normalized_claimed = set()
                    for staged in staged_photos:
                        pid = staged["id"]
                        staged_norm = normalize(staged["filename"])
                        collision = existing_by_key.get(staged_norm)
                        intra_staged_collision = (
                            staged_norm in staged_normalized_claimed)
                        # The archived filename may differ in case from the
                        # staged one; probe for the ACTUAL archived name so
                        # the on-disk existence check matches on
                        # case-sensitive volumes too (where any case-alias
                        # check is pointless anyway).
                        target_filename = (collision["filename"]
                                           if collision else None)
                        target_disk_path = (
                            _join_subtree_path(target_path, target_filename)
                            if target_filename is not None else None)
                        # A missing file on disk is definitely phantom
                        # (rsync would have written the staged bytes if
                        # this ever ran in production; the code path
                        # tolerates the isolated-unit-test case where no
                        # rsync happened).
                        target_on_disk = (
                            target_disk_path is not None
                            and os.path.exists(target_disk_path))
                        real_collision = False
                        if collision is not None and target_on_disk:
                            staged_hash = staged["file_hash"]
                            target_hash = collision["file_hash"]
                            if staged_hash and target_hash:
                                # Both hashes present → byte-identity
                                # comparison is reliable. Match → the
                                # target row's claim matches the file on
                                # disk (real collision). Mismatch → rsync
                                # replaced a missing file with fresh
                                # bytes; the target row is stale.
                                real_collision = (staged_hash == target_hash)
                            # else: at least one recorded hash is missing.
                            # Leave ``real_collision`` False so the
                            # phantom-replacement branch below runs — see
                            # the outer comment for why size alone (or a
                            # freshly-computed on-disk hash) can't safely
                            # stand in for the recorded-hash comparison
                            # here.
                        if real_collision or intra_staged_collision:
                            # photo_keywords.photo_id has no ON DELETE CASCADE
                            # (unlike every other photo_id FK), so clear
                            # keyword links before deleting the photo or the
                            # FK fires.
                            #
                            # ``intra_staged_collision`` shares this branch
                            # for the same net effect: the staged row's
                            # bytes are not represented on disk (an earlier
                            # staged case-alias already claimed the slot,
                            # rsync ``--ignore-existing`` skipped this
                            # file), so treating it as ``already_present``
                            # is correct.
                            self.conn.execute(
                                "DELETE FROM photo_keywords "
                                "WHERE photo_id = ?",
                                (pid,))
                            self.conn.execute(
                                "DELETE FROM photos WHERE id = ?", (pid,))
                            counts["already_present"] += 1
                            # The staged photo id is now free. Thumbnails,
                            # previews, working copies, and offline cache files
                            # were keyed off this id, and SQLite reuses freed
                            # rowids — a later import that lands on this id
                            # would inherit stale imagery. Report the id up so
                            # the caller can drop those files.
                            counts["dropped_photo_ids"].append(pid)
                        else:
                            if collision is not None:
                                # Filename collided (case-normalized) but the
                                # target row is a phantom — either the
                                # archived file was missing on disk (rsync
                                # copied the staged bytes into the empty
                                # slot) or the file is there but its
                                # bytes-identity (hash/size) doesn't match
                                # the row's claim (rsync replaced a missing
                                # file with fresh staged bytes). Either way
                                # the staged row correctly describes what's
                                # on disk. Drop the phantom by id so the
                                # reparent below can take its (folder_id,
                                # filename) slot and represent the real file.
                                # Deleting by id (not filename) is required on
                                # case-insensitive volumes where the staged
                                # and phantom filenames differ only in case:
                                # the SQL ``filename = ?`` lookup used earlier
                                # would miss the stale row and leave both
                                # intact.
                                self.conn.execute(
                                    "DELETE FROM photo_keywords "
                                    "WHERE photo_id = ?", (collision["id"],))
                                self.conn.execute(
                                    "DELETE FROM photos WHERE id = ?",
                                    (collision["id"],))
                                # The phantom target-row id is likewise freed —
                                # its cache files can be reused for a new
                                # photo. Report it up for cleanup too.
                                counts["dropped_photo_ids"].append(
                                    collision["id"])
                            self.conn.execute(
                                "UPDATE photos SET folder_id = ? "
                                "WHERE id = ?",
                                (target["id"], pid),
                            )
                            # A photo moved into a pre-existing archive folder
                            # is still a newly-archived photo from the user's
                            # view.
                            counts["new_photos"] += 1
                            # Claim the case-normalized slot so a later
                            # staged row whose filename case-folds to this
                            # name is dropped as ``already_present``
                            # instead of adding a second catalog row for
                            # the same on-disk destination.
                            staged_normalized_claimed.add(staged_norm)
                    to_delete.append(sf["id"])
                    counts["merged_folders"] += 1
                    last_target_parent[target_path] = target["id"]

            # Delete deepest-first: ``staged_folders`` (hence ``to_delete``)
            # is shallowest-first, so reverse to remove children before
            # parents and never orphan a still-referenced ``parent_id``.
            # Drop the folder's workspace links first —
            # ``workspace_folders.folder_id`` has no ON DELETE CASCADE, so
            # the folder delete would hit a FK violation.
            for fid in reversed(to_delete):
                self.conn.execute(
                    "DELETE FROM workspace_folders WHERE folder_id = ?",
                    (fid,))
                self.conn.execute("DELETE FROM folders WHERE id = ?", (fid,))

            self.conn.commit()
            self._new_images_cache.invalidate_workspaces(self._db_path, [ws])
        except Exception:
            self.conn.rollback()
            raise
        self.update_folder_counts()
        return counts

    def check_filename_collisions(self, photo_ids, target_folder_id):
        """Check if any photo filenames already exist in the target folder.

        Returns list of dicts with photo_id and filename for conflicts.
        """
        if not photo_ids:
            return []
        placeholders = ",".join("?" for _ in photo_ids)
        rows = self.conn.execute(
            f"""SELECT p.id AS photo_id, p.filename
                FROM photos p
                WHERE p.id IN ({placeholders})
                  AND EXISTS (
                    SELECT 1 FROM photos t
                    WHERE t.folder_id = ? AND t.filename = p.filename
                  )""",
            list(photo_ids) + [target_folder_id],
        ).fetchall()
        return [dict(r) for r in rows]

    def query_move_rule_matches(self, criteria):
        """Return photo IDs matching move rule criteria.

        Criteria keys (all optional, AND logic):
          rating_min, flag, species, folder_ids,
          has_predictions, imported_before
        """
        conditions = ["wf.workspace_id = ?"]
        params = [self._ws_id()]
        joins = ["JOIN workspace_folders wf ON wf.folder_id = p.folder_id",
                 "JOIN folders f ON f.id = p.folder_id AND f.status IN ('ok', 'partial')"]

        if "rating_min" in criteria:
            conditions.append("p.rating >= ?")
            params.append(criteria["rating_min"])
        if "flag" in criteria:
            conditions.append("p.flag = ?")
            params.append(criteria["flag"])
        if "folder_ids" in criteria and criteria["folder_ids"]:
            fph = ",".join("?" for _ in criteria["folder_ids"])
            conditions.append(f"p.folder_id IN ({fph})")
            params.extend(criteria["folder_ids"])
        if "has_predictions" in criteria:
            # Predictions no longer carry photo_id/workspace_id — they reference
            # a global detection, which references the photo. Workspace scoping
            # is already enforced by the outer workspace_folders JOIN, so the
            # EXISTS only needs to link prediction → detection → this photo.
            #
            # Apply the workspace-effective detector_confidence floor so the
            # rule matches what the UI actually shows: a photo whose only
            # predictions sit on below-threshold detections must NOT count
            # as "has predictions".
            import config as cfg
            move_min_conf = self.get_effective_config(cfg.load()).get(
                "detector_confidence", 0.2
            )
            if criteria["has_predictions"]:
                conditions.append(
                    "EXISTS (SELECT 1 FROM predictions pr "
                    "JOIN detections d ON d.id = pr.detection_id "
                    "WHERE d.photo_id = p.id "
                    "  AND d.detector_confidence >= ?)"
                )
                params.append(move_min_conf)
            else:
                conditions.append(
                    "NOT EXISTS (SELECT 1 FROM predictions pr "
                    "JOIN detections d ON d.id = pr.detection_id "
                    "WHERE d.photo_id = p.id "
                    "  AND d.detector_confidence >= ?)"
                )
                params.append(move_min_conf)
        if "imported_before" in criteria:
            conditions.append("p.timestamp < ?")
            params.append(criteria["imported_before"])
        if "species" in criteria and criteria["species"]:
            sph = ",".join("?" for _ in criteria["species"])
            joins.append("JOIN photo_keywords pk ON pk.photo_id = p.id")
            joins.append("JOIN keywords k ON k.id = pk.keyword_id AND k.is_species = 1")
            conditions.append(f"k.name IN ({sph})")
            params.extend(criteria["species"])

        join_sql = "\n".join(joins)
        where_sql = " AND ".join(conditions)
        rows = self.conn.execute(
            f"SELECT DISTINCT p.id FROM photos p {join_sql} WHERE {where_sql}",
            params,
        ).fetchall()
        return [r["id"] for r in rows]

    def _folders_linked_in_other_workspace(self, folder_ids, active_ws):
        """Return the subset of folder_ids that any workspace other than
        active_ws has a workspace_folders link for (root or not).

        Any foreign link — an explicit root import (is_root = 1) or a
        scanner-materialized descendant of one (is_root = 0) — is evidence
        the folder is still visible in that workspace, so it must never be
        deleted, only unlinked. With active_ws None, any link qualifies.
        """
        linked = set()
        for chunk in _chunks(folder_ids):
            placeholders = ",".join("?" for _ in chunk)
            sql = (
                f"SELECT DISTINCT folder_id FROM workspace_folders "
                f"WHERE folder_id IN ({placeholders})"
            )
            params = list(chunk)
            if active_ws is not None:
                sql += " AND workspace_id != ?"
                params.append(active_ws)
            linked.update(
                row["folder_id"]
                for row in self.conn.execute(sql, params).fetchall()
            )
        return linked

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
        # Everything under the target, by path, including legacy
        # NULL-parent_id descendants a parent_id walk would miss.
        candidates = set(self._folder_subtree_ids_by_path(folder_id))

        # Candidates that another workspace has a link for are never
        # deleted, and each protected folder keeps its whole subtree. Any
        # foreign link counts — root or scanner-materialized — since either
        # means the folder is still visible in that workspace. When the
        # target itself is protected, the kept set covers every candidate
        # and this degenerates to unlink-only: nothing is deleted, no
        # reparenting happens, and only the active workspace's links go.
        protected = self._folders_linked_in_other_workspace(candidates, active_ws)
        kept_subtree_ids = set()
        for fid in protected:
            kept_subtree_ids.update(self._folder_subtree_ids_by_path(fid))
        delete_ids = candidates - kept_subtree_ids

        # Kept folders whose parent row is being deleted must be reparented
        # to NULL before the folder DELETE — folders.parent_id has no ON
        # DELETE action. (Kept folders whose parent also survives keep
        # their chain intact.)
        kept_head_ids = []
        for chunk in _chunks(kept_subtree_ids):
            placeholders = ",".join("?" for _ in chunk)
            kept_head_ids.extend(
                row["id"]
                for row in self.conn.execute(
                    f"SELECT id, parent_id FROM folders WHERE id IN ({placeholders})",
                    chunk,
                ).fetchall()
                if row["parent_id"] in delete_ids
            )

        # Delete children before parents, else the multi-statement delete
        # trips the parent_id FK at the end of an earlier chunk's statement.
        # Order by path depth descending — a parent's normalized path always
        # has fewer separators than its child's, and parent_id order can't
        # be trusted for the legacy path-only rows.
        depth_by_id = {}
        for chunk in _chunks(delete_ids):
            placeholders = ",".join("?" for _ in chunk)
            for row in self.conn.execute(
                f"SELECT id, path FROM folders WHERE id IN ({placeholders})",
                chunk,
            ).fetchall():
                path = _path_for_subtree_match(row["path"] or "")
                depth_by_id[row["id"]] = path.count("/")
        ordered_delete_ids = sorted(
            delete_ids, key=lambda fid: depth_by_id.get(fid, 0), reverse=True
        )

        photo_ids = []
        for chunk in _chunks(ordered_delete_ids):
            placeholders = ",".join("?" for _ in chunk)
            photo_ids.extend(
                row["id"]
                for row in self.conn.execute(
                    f"SELECT id FROM photos WHERE folder_id IN ({placeholders})",
                    chunk,
                ).fetchall()
            )

        # One outer transaction so a failure partway can't commit the photo
        # deletes while leaving the folder rows behind. ``commit=False`` also
        # defers delete_photos' pipeline-cache prune (a non-transactional
        # file write) until after the commit succeeds.
        files = []
        deleted_ids = []
        try:
            for chunk in _chunks(photo_ids):
                inner = self.delete_photos(chunk, commit=False)
                files.extend(inner.get("files", []))
                deleted_ids.extend(inner.get("ids", []))
            # Reparent kept subtree heads before any folder DELETE — their
            # parent_id points at a row being deleted, and the FK has no ON
            # DELETE action.
            for chunk in _chunks(kept_head_ids):
                placeholders = ",".join("?" for _ in chunk)
                self.conn.execute(
                    f"UPDATE folders SET parent_id = NULL "
                    f"WHERE id IN ({placeholders})",
                    chunk,
                )
            # Kept subtrees disappear from this workspace's view: drop the
            # active workspace's links, leaving the other workspaces' links
            # (and the folder rows and photos) untouched.
            if active_ws is not None:
                for chunk in _chunks(kept_subtree_ids):
                    placeholders = ",".join("?" for _ in chunk)
                    self.conn.execute(
                        f"DELETE FROM workspace_folders WHERE workspace_id = ? "
                        f"AND folder_id IN ({placeholders})",
                        [active_ws] + chunk,
                    )
            for chunk in _chunks(ordered_delete_ids):
                placeholders = ",".join("?" for _ in chunk)
                self.conn.execute(
                    f"DELETE FROM workspace_folders WHERE folder_id IN ({placeholders})",
                    chunk,
                )
                self.conn.execute(
                    f"DELETE FROM folders WHERE id IN ({placeholders})",
                    chunk,
                )
            self.conn.commit()
        except Exception:
            self.conn.rollback()
            raise
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
        cur = execute_with_retry(
            self.conn,
            """INSERT OR IGNORE INTO photos
               (folder_id, filename, extension, file_size, file_mtime, xmp_mtime,
                timestamp, width, height, file_hash)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            (
                folder_id,
                filename,
                extension,
                file_size,
                file_mtime,
                xmp_mtime,
                timestamp,
                width,
                height,
                file_hash,
            ),
        )
        commit_with_retry(self.conn)
        if cur.rowcount > 0:
            photo_id = cur.lastrowid
        else:
            row = self.conn.execute(
                "SELECT id FROM photos WHERE folder_id = ? AND filename = ?",
                (folder_id, filename),
            ).fetchone()
            photo_id = row["id"]

        # Auto-resolve duplicates when we have a file_hash and >1 non-rejected
        # rows share it. Most scanner-path callers leave file_hash=None here
        # and set it later via an UPDATE; those callers must invoke
        # check_and_resolve_duplicates_for_hash themselves after the UPDATE.
        if file_hash:
            self.check_and_resolve_duplicates_for_hash(file_hash)

        return photo_id

    def check_and_resolve_duplicates_for_hash(self, file_hash: str) -> dict | None:
        """Look up non-rejected photos sharing this hash; if >=2, resolve.

        Returns the result dict from apply_duplicate_resolution when resolution
        ran, or None when no resolution was needed. Failures are logged and
        swallowed — this is a best-effort hook, not a correctness guarantee.
        """
        if not file_hash:
            return None
        try:
            dup_rows = self.conn.execute(
                "SELECT id FROM photos WHERE file_hash = ? AND (flag IS NULL OR flag != 'rejected')",
                (file_hash,),
            ).fetchall()
            if len(dup_rows) > 1:
                return self.apply_duplicate_resolution([r["id"] for r in dup_rows])
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
        unresolved_rows = self.conn.execute(
            """
            SELECT file_hash, GROUP_CONCAT(id) AS ids
            FROM photos
            WHERE file_hash IS NOT NULL AND (flag IS NULL OR flag != 'rejected')
            GROUP BY file_hash
            HAVING COUNT(*) > 1
            """
        ).fetchall()
        groups = [
            {
                "file_hash": r["file_hash"],
                "photo_ids": [int(x) for x in r["ids"].split(",")],
                "status": "unresolved",
            }
            for r in unresolved_rows
        ]

        if not include_resolved:
            return groups

        # Resolved groups: hashes where exactly 1 non-rejected row exists
        # AND at least 1 rejected row shares the hash. We exclude purely-
        # rejected hashes (e.g. user manually rejected the only copy of a
        # photo for non-duplicate reasons) — without the kept-row anchor
        # there is no "loser of a duplicate group" to clean up.
        resolved_rows = self.conn.execute(
            """
            SELECT file_hash,
                   GROUP_CONCAT(id) AS ids,
                   SUM(CASE WHEN flag IS NULL OR flag != 'rejected' THEN 1 ELSE 0 END) AS kept,
                   SUM(CASE WHEN flag  = 'rejected' THEN 1 ELSE 0 END) AS rejected
            FROM photos
            WHERE file_hash IS NOT NULL
            GROUP BY file_hash
            HAVING kept = 1 AND rejected >= 1
            """
        ).fetchall()
        groups.extend(
            {
                "file_hash": r["file_hash"],
                "photo_ids": [int(x) for x in r["ids"].split(",")],
                "status": "resolved",
            }
            for r in resolved_rows
        )
        return groups

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
        from duplicates import DupCandidate, resolve_duplicates

        if not photo_ids or len(photo_ids) < 2:
            return {"winner_id": None, "loser_ids": [], "rejected": 0}

        # Chunked — a single duplicate group can exceed the bound-parameter
        # cap (see duplicate_scan.py, which chunks its own reads).
        rows = []
        for chunk in _chunks(list(dict.fromkeys(photo_ids))):
            placeholders = ",".join("?" * len(chunk))
            rows.extend(self.conn.execute(
                f"""SELECT p.id, p.filename, p.file_mtime, p.rating, p.flag,
                           f.path AS folder_path
                    FROM photos p
                    LEFT JOIN folders f ON f.id = p.folder_id
                    WHERE p.id IN ({placeholders}) AND (p.flag IS NULL OR p.flag != 'rejected')""",
                list(chunk),
            ).fetchall())
        if len(rows) < 2:
            return {"winner_id": None, "loser_ids": [], "rejected": 0}

        candidates = []
        for r in rows:
            path = os.path.join(r["folder_path"] or "", r["filename"] or "")
            candidates.append(
                DupCandidate(
                    id=r["id"],
                    path=path,
                    mtime=r["file_mtime"] or 0.0,
                    # Stat each candidate so the resolver doesn't pick a
                    # winner whose file was moved/deleted on disk. The DB
                    # row would otherwise outvote a surviving twin solely
                    # on path-string heuristics.
                    exists=os.path.exists(path),
                )
            )
        winner_id, losers_with_reasons = resolve_duplicates(candidates)
        loser_ids = [lid for lid, _reason in losers_with_reasons]

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
        from duplicates import PhotoMetadata, merge_metadata

        def _meta(photo_id):
            r = self.conn.execute(
                "SELECT rating FROM photos WHERE id = ?", (photo_id,)
            ).fetchone()
            kw_rows = self.conn.execute(
                "SELECT keyword_id FROM photo_keywords WHERE photo_id = ?",
                (photo_id,),
            ).fetchall()
            pend = self.conn.execute(
                "SELECT 1 FROM pending_changes WHERE photo_id = ? LIMIT 1",
                (photo_id,),
            ).fetchone()
            return PhotoMetadata(
                id=photo_id,
                rating=(r["rating"] if r and r["rating"] is not None else 0),
                keyword_ids={kr["keyword_id"] for kr in kw_rows},
                # Collections in Vireo are rule-based (no junction table); skip.
                collection_ids=set(),
                has_pending_edit=pend is not None,
            )

        winner_meta = _meta(winner_id)
        loser_metas = [_meta(lid) for lid in loser_ids]
        merge = merge_metadata(winner_meta, loser_metas)

        with self.conn:  # transaction
            if merge.new_rating != winner_meta.rating:
                self.conn.execute(
                    "UPDATE photos SET rating = ? WHERE id = ?",
                    (merge.new_rating, winner_id),
                )
            for kw_id in merge.keyword_ids_to_add:
                self.conn.execute(
                    "INSERT OR IGNORE INTO photo_keywords (photo_id, keyword_id) "
                    "VALUES (?, ?)",
                    (winner_id, kw_id),
                )
            # TODO: pending-edit copy for duplicate merge — see plan Task 7.
            # Skipped because pending_changes is workspace-scoped and its
            # value/change_token columns are non-trivial to copy safely in
            # this transaction. Rare edge case; revisit if product needs it.
            # Collections are rule-based (no junction table) so
            # merge.collection_ids_to_add has nothing to write either.
            # Chunked — a single duplicate group's loser list can exceed the
            # bound-parameter cap.
            for chunk in _chunks(loser_ids):
                loser_placeholders = ",".join("?" * len(chunk))
                self.conn.execute(
                    f"UPDATE photos SET flag = 'rejected' WHERE id IN ({loser_placeholders})",
                    list(chunk),
                )

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
        from duplicates import DupCandidate, resolve_duplicates

        # Normalize once. The bucket UI derives folder paths from
        # ``os.path.dirname(...)`` (never trailing-slashed), but
        # ``folders.path`` rows can carry a trailing separator from
        # manual relocation or legacy imports — a naive string compare
        # silently no-ops the action for those users.
        keep_folder_norm = os.path.normpath(keep_folder) if keep_folder else ""

        resolved = []
        skipped = []
        for file_hash in file_hashes:
            rows = self.conn.execute(
                """SELECT p.id, p.filename, p.file_mtime, p.rating,
                          f.path AS folder_path
                   FROM photos p
                   LEFT JOIN folders f ON f.id = p.folder_id
                   WHERE p.file_hash = ? AND (p.flag IS NULL OR p.flag != 'rejected')""",
                (file_hash,),
            ).fetchall()
            if not rows:
                skipped.append({"file_hash": file_hash, "reason": "no candidates"})
                continue
            if len(rows) < 2:
                skipped.append({
                    "file_hash": file_hash,
                    "reason": "fewer than 2 candidates",
                })
                continue
            in_folder = [
                r for r in rows
                if os.path.normpath(r["folder_path"] or "") == keep_folder_norm
            ]
            if not in_folder:
                skipped.append({
                    "file_hash": file_hash,
                    "reason": "no candidate in keep_folder",
                })
                continue
            # Existence-check the keep_folder candidate(s) before promoting.
            # If the row's file has been deleted externally but a sibling in
            # another folder still exists, force-picking the missing row as
            # winner would reject the surviving copy — and chained delete
            # would then trash it. Skip the hash instead.
            in_folder_paths = [
                (r, os.path.join(r["folder_path"] or "", r["filename"] or ""))
                for r in in_folder
            ]
            present_in_folder = [
                (r, p) for (r, p) in in_folder_paths if os.path.exists(p)
            ]
            if not present_in_folder:
                skipped.append({
                    "file_hash": file_hash,
                    "reason": "keep_folder candidate missing on disk",
                })
                continue
            if len(present_in_folder) == 1:
                winner_id = present_in_folder[0][0]["id"]
            else:
                # Same-folder duplicates among the keep_folder candidates —
                # let the resolver pick deterministically among them. All
                # candidates passed in exist on disk (filtered above), so
                # Rule 0 is a no-op here.
                cands = [
                    DupCandidate(
                        id=r["id"], path=p,
                        mtime=r["file_mtime"] or 0.0,
                        exists=True,
                    )
                    for (r, p) in present_in_folder
                ]
                winner_id, _ = resolve_duplicates(cands)
            loser_ids = [r["id"] for r in rows if r["id"] != winner_id]
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
        with self.conn:
            cur = self.conn.execute(
                "UPDATE photos SET flag = 'none' "
                "WHERE file_hash = ? AND flag = 'rejected'",
                (file_hash,),
            )
            return cur.rowcount

    # Columns to return in photo list queries (excludes large fields)
    PHOTO_COLS = """id, folder_id, filename, extension, file_size, file_mtime, xmp_mtime,
                    timestamp, width, height, rating, flag, thumb_path, sharpness,
                    subject_sharpness, subject_size, quality_score,
                    latitude, longitude, companion_path, working_copy_path,
                    wildlife_excluded, miss_no_subject, miss_clipped, miss_oof"""

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
        if verify_workspace:
            return self.conn.execute(
                f"""SELECT {self.PHOTO_DETAIL_COLS} FROM photos
                    WHERE id = ? AND folder_id IN (
                        SELECT folder_id FROM workspace_folders
                        WHERE workspace_id = ?)""",
                (photo_id, self._ws_id()),
            ).fetchone()
        return self.conn.execute(
            f"SELECT {self.PHOTO_DETAIL_COLS} FROM photos WHERE id = ?", (photo_id,)
        ).fetchone()

    def get_photos_by_ids(self, photo_ids):
        """Return photos for a list of IDs.

        Returns a dict mapping photo_id -> Row for efficient lookup. Large
        id lists are chunked so the IN-clause stays under SQLite's
        bound-parameter cap (999 on legacy builds, 32766 modern).
        """
        if not photo_ids:
            return {}
        result = {}
        for chunk in _chunks(photo_ids):
            placeholders = ",".join("?" for _ in chunk)
            rows = self.conn.execute(
                f"SELECT {self.PHOTO_COLS} FROM photos WHERE id IN ({placeholders})",
                list(chunk),
            ).fetchall()
            for row in rows:
                result[row["id"]] = row
        return result

    def count_photos(self):
        """Return photo count for the active workspace.

        Filters out photos whose folder is flagged ``'missing'`` so callers
        like browse/cull/move see only photos they can actually act on. For
        a total inventory that survives an unmounted drive (e.g. the
        dashboard's headline number), use ``count_photos_in_workspace``.
        """
        return self.conn.execute(
            """SELECT COUNT(*) FROM photos p
               JOIN workspace_folders wf ON wf.folder_id = p.folder_id
               JOIN folders f ON f.id = p.folder_id AND f.status IN ('ok', 'partial')
               WHERE wf.workspace_id = ?""",
            (self._ws_id(),),
        ).fetchone()[0]

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
        return self.conn.execute(
            """SELECT COUNT(*) FROM photos p
               JOIN workspace_folders wf ON wf.folder_id = p.folder_id
               WHERE wf.workspace_id = ?""",
            (self._ws_id(),),
        ).fetchone()[0]

    def count_folders(self):
        """Return folder count for the active workspace."""
        return self.conn.execute(
            """SELECT COUNT(*) FROM folders f
               JOIN workspace_folders wf ON wf.folder_id = f.id
               WHERE wf.workspace_id = ? AND f.status IN ('ok', 'partial')""",
            (self._ws_id(),),
        ).fetchone()[0]

    def count_keywords(self):
        """Return count of keywords used by photos in the active workspace.

        Filters out keywords whose only photos sit in folders flagged
        ``'missing'``. For the dashboard's headline (which must agree with
        the unfiltered top_keywords chart in ``get_dashboard_stats``), use
        ``count_keywords_in_workspace`` instead.
        """
        return self.conn.execute(
            """SELECT COUNT(DISTINCT pk.keyword_id)
               FROM photo_keywords pk
               JOIN photos p ON p.id = pk.photo_id
               JOIN workspace_folders wf ON wf.folder_id = p.folder_id
               JOIN folders f ON f.id = p.folder_id AND f.status IN ('ok', 'partial')
               WHERE wf.workspace_id = ?""",
            (self._ws_id(),),
        ).fetchone()[0]

    def count_keywords_in_workspace(self):
        """Return count of keywords used by photos in the active workspace,
        including photos in folders flagged ``'missing'``.

        Pairs with the unfiltered ``top_keywords`` query in
        ``get_dashboard_stats`` so the dashboard's Keywords headline can't
        disagree with the Top Species / Other Keywords charts when a drive
        is unmounted (e.g. headline says 0 while charts list keywords).
        """
        return self.conn.execute(
            """SELECT COUNT(DISTINCT pk.keyword_id)
               FROM photo_keywords pk
               JOIN photos p ON p.id = pk.photo_id
               JOIN workspace_folders wf ON wf.folder_id = p.folder_id
               WHERE wf.workspace_id = ?""",
            (self._ws_id(),),
        ).fetchone()[0]

    def count_pending_changes(self):
        """Return pending changes count."""
        return self.conn.execute(
            "SELECT COUNT(*) FROM pending_changes WHERE workspace_id = ?",
            (self._ws_id(),),
        ).fetchone()[0]

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
            linked = self.conn.execute(
                "SELECT 1 FROM workspace_folders "
                "WHERE workspace_id = ? AND folder_id = ?",
                (self._ws_id(), folder_id),
            ).fetchone()
            if not linked:
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
        ws = self._ws_id()
        import config as cfg
        min_conf = self.get_effective_config(cfg.load()).get(
            "detector_confidence", 0.2
        )
        scope_sql, scope_params = self._dashboard_scope_clause(
            folder_id, collection_id, date_from, date_to,
        )
        photo_row = self.conn.execute(
            f"""SELECT
                COUNT(*) AS total,
                {self._coverage_select_fragment()}
            FROM photos p
            JOIN workspace_folders wf ON wf.folder_id = p.folder_id
            JOIN folders f ON f.id = p.folder_id AND f.status IN ('ok', 'partial')
            WHERE wf.workspace_id = ?{scope_sql}""",
            (ws, *scope_params),
        ).fetchone()
        detected = self.conn.execute(
            f"""SELECT COUNT(DISTINCT d.photo_id)
               FROM detections d
               JOIN photos p ON p.id = d.photo_id
               JOIN workspace_folders wf ON wf.folder_id = p.folder_id
               JOIN folders f ON f.id = p.folder_id AND f.status IN ('ok', 'partial')
               WHERE wf.workspace_id = ?
                 AND d.detector_confidence >= ?{scope_sql}""",
            (ws, min_conf, *scope_params),
        ).fetchone()[0] or 0
        classified = self.conn.execute(
            f"""SELECT COUNT(DISTINCT d.photo_id)
               FROM predictions pr
               JOIN detections d ON d.id = pr.detection_id
               JOIN photos p ON p.id = d.photo_id
               JOIN workspace_folders wf ON wf.folder_id = p.folder_id
               JOIN folders f ON f.id = p.folder_id AND f.status IN ('ok', 'partial')
               WHERE wf.workspace_id = ?
                 AND d.detector_confidence >= ?{scope_sql}""",
            (ws, min_conf, *scope_params),
        ).fetchone()[0] or 0
        result = {"total": photo_row["total"] or 0}
        for key, _ in self._COVERAGE_PHOTO_COLUMNS:
            result[key] = photo_row[key] or 0
        result["detected"] = detected
        result["classified"] = classified
        return result

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
        ws = self._ws_id()
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
        folder_filter_sql = ""
        folder_filter_params = []
        if folder_id is not None:
            linked = self.conn.execute(
                "SELECT 1 FROM workspace_folders "
                "WHERE workspace_id = ? AND folder_id = ?",
                (ws, folder_id),
            ).fetchone()
            if not linked:
                raise ValueError("folder not found in active workspace")
            subtree = self.get_folder_subtree_ids(folder_id)
            placeholders = ",".join("?" for _ in subtree)
            folder_filter_sql = f" AND f.id IN ({placeholders})"
            folder_filter_params = subtree
        photo_rows = self.conn.execute(
            f"""SELECT
                f.id AS folder_id,
                f.path AS path,
                f.name AS name,
                COUNT(p.id) AS total,
                {self._coverage_select_fragment()}
            FROM folders f
            JOIN workspace_folders wf ON wf.folder_id = f.id
            LEFT JOIN photos p ON p.folder_id = f.id{photo_scope_sql}
            WHERE wf.workspace_id = ? AND f.status IN ('ok', 'partial'){folder_filter_sql}
            GROUP BY f.id
            ORDER BY f.path""",
            (*photo_scope_params, ws, *folder_filter_params),
        ).fetchall()
        det_rows = self.conn.execute(
            f"""SELECT p.folder_id AS folder_id,
                      COUNT(DISTINCT d.photo_id) AS detected
               FROM detections d
               JOIN photos p ON p.id = d.photo_id
               JOIN workspace_folders wf ON wf.folder_id = p.folder_id
               JOIN folders f ON f.id = p.folder_id AND f.status IN ('ok', 'partial')
               WHERE wf.workspace_id = ?
                 AND d.detector_confidence >= ?{scope_sql}
               GROUP BY p.folder_id""",
            (ws, min_conf, *scope_params),
        ).fetchall()
        cls_rows = self.conn.execute(
            f"""SELECT p.folder_id AS folder_id,
                      COUNT(DISTINCT d.photo_id) AS classified
               FROM predictions pr
               JOIN detections d ON d.id = pr.detection_id
               JOIN photos p ON p.id = d.photo_id
               JOIN workspace_folders wf ON wf.folder_id = p.folder_id
               JOIN folders f ON f.id = p.folder_id AND f.status IN ('ok', 'partial')
               WHERE wf.workspace_id = ?
                 AND d.detector_confidence >= ?{scope_sql}
               GROUP BY p.folder_id""",
            (ws, min_conf, *scope_params),
        ).fetchall()
        det_by_folder = {r["folder_id"]: r["detected"] for r in det_rows}
        cls_by_folder = {r["folder_id"]: r["classified"] for r in cls_rows}
        out = []
        for r in photo_rows:
            entry = {
                "folder_id": r["folder_id"],
                "path": r["path"],
                "name": r["name"],
                "total": r["total"] or 0,
            }
            for key, _ in self._COVERAGE_PHOTO_COLUMNS:
                entry[key] = r[key] or 0
            entry["detected"] = det_by_folder.get(r["folder_id"], 0)
            entry["classified"] = cls_by_folder.get(r["folder_id"], 0)
            out.append(entry)
        return out

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
        if not paths:
            return {}
        by_dir = {}
        for p in paths:
            by_dir.setdefault(os.path.dirname(p), {}).setdefault(
                os.path.basename(p), []
            ).append(p)

        out = {}
        BATCH = 800  # leave headroom under SQLite's default 999-param cap
        for dir_path, originals_by_name in by_dir.items():
            fnames = list(originals_by_name)
            for i in range(0, len(fnames), BATCH):
                chunk = fnames[i:i + BATCH]
                placeholders = ",".join("?" for _ in chunk)
                rows = self.conn.execute(
                    f"""SELECT p.id, p.filename
                        FROM photos p
                        JOIN folders f ON f.id = p.folder_id
                        WHERE f.path = ? AND p.filename IN ({placeholders})""",
                    (dir_path, *chunk),
                ).fetchall()
                for r in rows:
                    for original_path in originals_by_name.get(r["filename"], []):
                        out[original_path] = r["id"]
        return out

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
        BATCH = 800
        linked = set()
        for i in range(0, len(unique), BATCH):
            chunk = unique[i:i + BATCH]
            placeholders = ",".join("?" for _ in chunk)
            rows = self.conn.execute(
                f"""SELECT f.path
                    FROM folders f
                    JOIN workspace_folders wf
                      ON wf.folder_id = f.id AND wf.workspace_id = ?
                    WHERE f.path IN ({placeholders})""",
                (ws, *chunk),
            ).fetchall()
            for r in rows:
                linked.add(r["path"])
        return len(unique) - len(linked)

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
        self.conn.execute(
            "CREATE TEMP TABLE IF NOT EXISTS scope_ids (id INTEGER PRIMARY KEY)"
        )
        self.conn.execute("DELETE FROM scope_ids")
        self.conn.executemany(
            "INSERT OR IGNORE INTO scope_ids (id) VALUES (?)",
            [(i,) for i in ids],
        )
        return f" AND {table_alias}.id IN (SELECT id FROM scope_ids)", []

    def count_real_detections_in_scope(self, photo_ids=None, min_conf=None):
        """Count (photos_with_real_dets, total_real_dets) for the workspace.

        "Real" excludes detector_model='full-image' synthetic anchors.
        ``photo_ids`` scopes to a collection (set/list of ids); None = whole
        workspace.

        Used by the pipeline plan to compute classify scope.
        """
        ws = self._ws_id()
        if min_conf is None:
            import config as cfg
            min_conf = self.get_effective_config(cfg.load()).get(
                "detector_confidence", 0.2,
            )
        scope_sql, scope_params = self._scope_clause(photo_ids)
        row = self.conn.execute(
            f"""SELECT COUNT(*) AS total_dets,
                       COUNT(DISTINCT d.photo_id) AS photos_with_dets
                FROM detections d
                JOIN photos p ON p.id = d.photo_id
                JOIN workspace_folders wf
                  ON wf.folder_id = p.folder_id AND wf.workspace_id = ?
                WHERE d.detector_model != 'full-image'
                  AND d.detector_confidence >= ?{scope_sql}""",
            (ws, min_conf, *scope_params),
        ).fetchone()
        return {
            "photos_with_dets": row["photos_with_dets"] or 0,
            "total_dets": row["total_dets"] or 0,
        }

    def count_primary_detections_in_scope(self, photo_ids=None, min_conf=None):
        """Count photos whose primary real detection is pipeline-classifiable.

        The streaming pipeline classifies at most one detection per photo: the
        highest-confidence non-full-image detection above the active threshold.
        This mirrors that gate for the Pipeline page plan so secondary boxes
        do not inflate pending classify work.
        """
        ws = self._ws_id()
        if min_conf is None:
            import config as cfg
            min_conf = self.get_effective_config(cfg.load()).get(
                "detector_confidence", 0.2,
            )
        scope_sql, scope_params = self._scope_clause(photo_ids)
        row = self.conn.execute(
            f"""WITH ranked AS (
                    SELECT d.id, d.photo_id,
                           ROW_NUMBER() OVER (
                             PARTITION BY d.photo_id
                             ORDER BY d.detector_confidence DESC, d.id ASC
                           ) AS rn
                      FROM detections d
                      JOIN photos p ON p.id = d.photo_id
                      JOIN workspace_folders wf
                        ON wf.folder_id = p.folder_id AND wf.workspace_id = ?
                     WHERE d.detector_model != 'full-image'
                       AND d.detector_confidence >= ?{scope_sql}
                )
                SELECT COUNT(*) AS primary_dets,
                       COUNT(DISTINCT photo_id) AS photos_with_dets
                  FROM ranked
                 WHERE rn = 1""",
            (ws, min_conf, *scope_params),
        ).fetchone()
        return {
            "photos_with_dets": row["photos_with_dets"] or 0,
            "total_dets": row["primary_dets"] or 0,
        }

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
        ws = self._ws_id()
        if min_conf is None:
            import config as cfg
            min_conf = self.get_effective_config(cfg.load()).get(
                "detector_confidence", 0.2,
            )
        scope_sql, scope_params = self._scope_clause(photo_ids)
        row = self.conn.execute(
            f"""SELECT COUNT(*) AS pending
                FROM detections d
                JOIN photos p ON p.id = d.photo_id
                JOIN workspace_folders wf
                  ON wf.folder_id = p.folder_id AND wf.workspace_id = ?
                LEFT JOIN classifier_runs cr
                  ON cr.detection_id = d.id
                 AND cr.classifier_model = ?
                 AND cr.labels_fingerprint = ?
                WHERE d.detector_model != 'full-image'
                  AND d.detector_confidence >= ?
                  AND cr.detection_id IS NULL{scope_sql}""",
            (ws, classifier_model, labels_fingerprint, min_conf, *scope_params),
        ).fetchone()
        return row["pending"] or 0

    def count_primary_classify_pending_pairs(
        self, classifier_model, labels_fingerprint,
        photo_ids=None, min_conf=None,
    ):
        """Count primary detections lacking a classifier run for (model, fp).

        Mirrors pipeline_job.classify_stage, which picks one primary detection
        per photo rather than classifying every detection row.
        """
        ws = self._ws_id()
        if min_conf is None:
            import config as cfg
            min_conf = self.get_effective_config(cfg.load()).get(
                "detector_confidence", 0.2,
            )
        scope_sql, scope_params = self._scope_clause(photo_ids)
        row = self.conn.execute(
            f"""WITH ranked AS (
                    SELECT d.id, d.photo_id,
                           ROW_NUMBER() OVER (
                             PARTITION BY d.photo_id
                             ORDER BY d.detector_confidence DESC, d.id ASC
                           ) AS rn
                      FROM detections d
                      JOIN photos p ON p.id = d.photo_id
                      JOIN workspace_folders wf
                        ON wf.folder_id = p.folder_id AND wf.workspace_id = ?
                     WHERE d.detector_model != 'full-image'
                       AND d.detector_confidence >= ?{scope_sql}
                )
                SELECT COUNT(*) AS pending
                  FROM ranked d
                  LEFT JOIN classifier_runs cr
                    ON cr.detection_id = d.id
                   AND cr.classifier_model = ?
                   AND cr.labels_fingerprint = ?
                 WHERE d.rn = 1
                   AND cr.detection_id IS NULL""",
            (ws, min_conf, *scope_params, classifier_model, labels_fingerprint),
        ).fetchone()
        return row["pending"] or 0

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
        ws = self._ws_id()
        if min_conf is None:
            import config as cfg
            min_conf = self.get_effective_config(cfg.load()).get(
                "detector_confidence", 0.2,
            )
        scope_sql, scope_params = self._scope_clause(photo_ids)
        row = self.conn.execute(
            f"""SELECT COUNT(DISTINCT d.id) AS n
                FROM detections d
                JOIN photos p ON p.id = d.photo_id
                JOIN workspace_folders wf
                  ON wf.folder_id = p.folder_id AND wf.workspace_id = ?
               WHERE d.detector_model != 'full-image'
                 AND d.detector_confidence >= ?
                 AND EXISTS (
                    SELECT 1 FROM classifier_runs cr_stale
                     WHERE cr_stale.detection_id = d.id
                       AND cr_stale.classifier_model = ?
                       AND cr_stale.labels_fingerprint != ?
                 )
                 AND NOT EXISTS (
                    SELECT 1 FROM classifier_runs cr_cur
                     WHERE cr_cur.detection_id = d.id
                       AND cr_cur.classifier_model = ?
                       AND cr_cur.labels_fingerprint = ?
                 ){scope_sql}""",
            (ws, min_conf, classifier_model, labels_fingerprint,
             classifier_model, labels_fingerprint, *scope_params),
        ).fetchone()
        return row["n"] or 0

    def count_primary_classify_stale(
        self, classifier_model, labels_fingerprint,
        photo_ids=None, min_conf=None,
    ):
        """Count stale classifier runs on primary detections only."""
        ws = self._ws_id()
        if min_conf is None:
            import config as cfg
            min_conf = self.get_effective_config(cfg.load()).get(
                "detector_confidence", 0.2,
            )
        scope_sql, scope_params = self._scope_clause(photo_ids)
        row = self.conn.execute(
            f"""WITH ranked AS (
                    SELECT d.id, d.photo_id,
                           ROW_NUMBER() OVER (
                             PARTITION BY d.photo_id
                             ORDER BY d.detector_confidence DESC, d.id ASC
                           ) AS rn
                      FROM detections d
                      JOIN photos p ON p.id = d.photo_id
                      JOIN workspace_folders wf
                        ON wf.folder_id = p.folder_id AND wf.workspace_id = ?
                     WHERE d.detector_model != 'full-image'
                       AND d.detector_confidence >= ?{scope_sql}
                )
                SELECT COUNT(*) AS n
                  FROM ranked d
                 WHERE d.rn = 1
                   AND EXISTS (
                      SELECT 1 FROM classifier_runs cr_stale
                       WHERE cr_stale.detection_id = d.id
                         AND cr_stale.classifier_model = ?
                         AND cr_stale.labels_fingerprint != ?
                   )
                   AND NOT EXISTS (
                      SELECT 1 FROM classifier_runs cr_cur
                       WHERE cr_cur.detection_id = d.id
                         AND cr_cur.classifier_model = ?
                         AND cr_cur.labels_fingerprint = ?
                   )""",
            (ws, min_conf, *scope_params, classifier_model, labels_fingerprint,
             classifier_model, labels_fingerprint),
        ).fetchone()
        return row["n"] or 0

    def count_full_image_fallback_photos(
        self, photo_ids=None, detector_model="megadetector-v6",
    ):
        """Count photos eligible for full-image fallback classification.

        These are photos in the active workspace where the detector has
        successfully run and found no boxes. Photos with any real detector row,
        including below-threshold noise, are excluded to match the pipeline's
        current runtime fallback gate.
        """
        ws = self._ws_id()
        scope_sql, scope_params = self._scope_clause(photo_ids)
        row = self.conn.execute(
            f"""SELECT COUNT(*) AS n
                  FROM photos p
                  JOIN workspace_folders wf
                    ON wf.folder_id = p.folder_id AND wf.workspace_id = ?
                  JOIN detector_runs dr
                    ON dr.photo_id = p.id
                   AND dr.detector_model = ?
                   AND dr.box_count = 0
                 WHERE NOT EXISTS (
                         SELECT 1 FROM detections d
                          WHERE d.photo_id = p.id
                            AND d.detector_model != 'full-image'
                       ){scope_sql}""",
            (ws, detector_model, *scope_params),
        ).fetchone()
        return row["n"] or 0

    def count_full_image_classify_pending_pairs(
        self, classifier_model, labels_fingerprint,
        photo_ids=None, detector_model="megadetector-v6",
    ):
        """Count fallback photos lacking a classifier run for (model, fp)."""
        ws = self._ws_id()
        scope_sql, scope_params = self._scope_clause(photo_ids)
        row = self.conn.execute(
            f"""WITH full_anchor AS (
                    SELECT photo_id, MIN(id) AS detection_id
                      FROM detections
                     WHERE detector_model = 'full-image'
                     GROUP BY photo_id
                  ),
                  fallback AS (
                    SELECT p.id AS photo_id, fa.detection_id
                      FROM photos p
                      JOIN workspace_folders wf
                        ON wf.folder_id = p.folder_id AND wf.workspace_id = ?
                      JOIN detector_runs dr
                        ON dr.photo_id = p.id
                       AND dr.detector_model = ?
                       AND dr.box_count = 0
                      LEFT JOIN full_anchor fa ON fa.photo_id = p.id
                     WHERE NOT EXISTS (
                             SELECT 1 FROM detections d
                              WHERE d.photo_id = p.id
                                AND d.detector_model != 'full-image'
                           ){scope_sql}
                  )
                SELECT COUNT(*) AS pending
                  FROM fallback f
                  LEFT JOIN classifier_runs cr
                    ON cr.detection_id = f.detection_id
                   AND cr.classifier_model = ?
                   AND cr.labels_fingerprint = ?
                 WHERE f.detection_id IS NULL
                    OR cr.detection_id IS NULL""",
            (
                ws, detector_model, *scope_params,
                classifier_model, labels_fingerprint,
            ),
        ).fetchone()
        return row["pending"] or 0

    def count_full_image_classify_stale(
        self, classifier_model, labels_fingerprint,
        photo_ids=None, detector_model="megadetector-v6",
    ):
        """Count fallback anchors with stale runs and no current run."""
        ws = self._ws_id()
        scope_sql, scope_params = self._scope_clause(photo_ids)
        row = self.conn.execute(
            f"""WITH full_anchor AS (
                    SELECT photo_id, MIN(id) AS detection_id
                      FROM detections
                     WHERE detector_model = 'full-image'
                     GROUP BY photo_id
                  ),
                  fallback AS (
                    SELECT p.id AS photo_id, fa.detection_id
                      FROM photos p
                      JOIN workspace_folders wf
                        ON wf.folder_id = p.folder_id AND wf.workspace_id = ?
                      JOIN detector_runs dr
                        ON dr.photo_id = p.id
                       AND dr.detector_model = ?
                       AND dr.box_count = 0
                      JOIN full_anchor fa ON fa.photo_id = p.id
                     WHERE NOT EXISTS (
                             SELECT 1 FROM detections d
                              WHERE d.photo_id = p.id
                                AND d.detector_model != 'full-image'
                           ){scope_sql}
                  )
                SELECT COUNT(*) AS n
                  FROM fallback f
                 WHERE EXISTS (
                         SELECT 1 FROM classifier_runs cr_stale
                          WHERE cr_stale.detection_id = f.detection_id
                            AND cr_stale.classifier_model = ?
                            AND cr_stale.labels_fingerprint != ?
                       )
                   AND NOT EXISTS (
                         SELECT 1 FROM classifier_runs cr_cur
                          WHERE cr_cur.detection_id = f.detection_id
                            AND cr_cur.classifier_model = ?
                            AND cr_cur.labels_fingerprint = ?
                       )""",
            (
                ws, detector_model, *scope_params,
                classifier_model, labels_fingerprint,
                classifier_model, labels_fingerprint,
            ),
        ).fetchone()
        return row["n"] or 0

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

        # Scalar: total real detections in scope.
        total_row = self.conn.execute(
            """SELECT COUNT(*) AS n
               FROM detections d
               JOIN photos p ON p.id = d.photo_id
               JOIN workspace_folders wf
                 ON wf.folder_id = p.folder_id AND wf.workspace_id = ?
               WHERE d.detector_model != 'full-image'
                 AND d.detector_confidence >= ?""",
            (workspace_id, min_conf),
        ).fetchone()
        total_real_detections = total_row["n"] or 0

        # Per-pair aggregates from classifier_runs joined to in-scope detections.
        pair_rows = self.conn.execute(
            """SELECT cr.classifier_model      AS classifier_model,
                      cr.labels_fingerprint    AS labels_fingerprint,
                      COUNT(DISTINCT cr.detection_id) AS classified_dets,
                      COUNT(DISTINCT d.photo_id)      AS photos_covered,
                      MAX(cr.run_at)           AS last_run
               FROM classifier_runs cr
               JOIN detections d ON d.id = cr.detection_id
               JOIN photos p ON p.id = d.photo_id
               JOIN workspace_folders wf
                 ON wf.folder_id = p.folder_id AND wf.workspace_id = ?
               WHERE d.detector_model != 'full-image'
                 AND d.detector_confidence >= ?
               GROUP BY cr.classifier_model, cr.labels_fingerprint""",
            (workspace_id, min_conf),
        ).fetchall()

        # Per-pair predictions row count (so the grand total can sum it).
        pred_count_rows = self.conn.execute(
            """SELECT pr.classifier_model      AS classifier_model,
                      pr.labels_fingerprint    AS labels_fingerprint,
                      COUNT(*)                 AS n
               FROM predictions pr
               JOIN detections d ON d.id = pr.detection_id
               JOIN photos p ON p.id = d.photo_id
               JOIN workspace_folders wf
                 ON wf.folder_id = p.folder_id AND wf.workspace_id = ?
               WHERE d.detector_model != 'full-image'
                 AND d.detector_confidence >= ?
               GROUP BY pr.classifier_model, pr.labels_fingerprint""",
            (workspace_id, min_conf),
        ).fetchall()
        pred_counts = {
            (r["classifier_model"], r["labels_fingerprint"]): r["n"]
            for r in pred_count_rows
        }

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
        # Top-1 per (detection, model, fp) — predictions UNIQUE on
        # (detection_id, classifier_model, labels_fingerprint, species), so
        # MAX(confidence) within that group is the top-1 confidence. The
        # outer window orders by RANDOM() so the per-pair cap picks an
        # unbiased sample rather than the oldest detection IDs (which would
        # under-represent recent reclassifications and bias the median).
        # Cap rows per (model, fp) pair in SQL via ROW_NUMBER so a workspace
        # with millions of predictions doesn't materialize them all in Python.
        rows = self.conn.execute(
            """SELECT classifier_model, labels_fingerprint, top1
               FROM (
                 SELECT classifier_model,
                        labels_fingerprint,
                        top1,
                        ROW_NUMBER() OVER (
                          PARTITION BY classifier_model, labels_fingerprint
                          ORDER BY RANDOM()
                        ) AS rn
                 FROM (
                   SELECT pr.classifier_model      AS classifier_model,
                          pr.labels_fingerprint    AS labels_fingerprint,
                          pr.detection_id          AS detection_id,
                          MAX(pr.confidence)       AS top1
                   FROM predictions pr
                   JOIN detections d ON d.id = pr.detection_id
                   JOIN photos p ON p.id = d.photo_id
                   JOIN workspace_folders wf
                     ON wf.folder_id = p.folder_id
                    AND wf.workspace_id = ?
                   WHERE d.detector_model != 'full-image'
                     AND d.detector_confidence >= ?
                     AND pr.confidence IS NOT NULL
                   GROUP BY pr.classifier_model, pr.labels_fingerprint,
                            pr.detection_id
                 )
               )
               WHERE rn <= ?""",
            (workspace_id, min_conf, sample_per_pair),
        ).fetchall()

        # Bucket by pair (already capped at sample_per_pair by SQL).
        buckets = {}
        for r in rows:
            key = (r["classifier_model"], r["labels_fingerprint"])
            buckets.setdefault(key, []).append(r["top1"])

        out = {}
        for key, vals in buckets.items():
            if not vals:
                out[key] = (None, 0)
                continue
            vals_sorted = sorted(vals)
            n = len(vals_sorted)
            mid = n // 2
            if n % 2 == 1:
                med = vals_sorted[mid]
            else:
                med = (vals_sorted[mid - 1] + vals_sorted[mid]) / 2.0
            out[key] = (float(med), n)
        return out

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
        ws = self._ws_id()
        if min_conf is None:
            import config as cfg
            min_conf = self.get_effective_config(cfg.load()).get(
                "detector_confidence", 0.2,
            )
        scope_sql, scope_params = self._scope_clause(photo_ids)
        if sam2_variant:
            row = self.conn.execute(
                f"""SELECT
                      COUNT(DISTINCT p.id) AS eligible,
                      COUNT(DISTINCT CASE
                        WHEN p.mask_path IS NULL
                          OR pm.photo_id IS NULL
                          OR pm.path IS NULL
                          OR pm.path = ''
                        THEN p.id END) AS pending
                    FROM photos p
                    JOIN workspace_folders wf
                      ON wf.folder_id = p.folder_id AND wf.workspace_id = ?
                    JOIN detections d
                      ON d.photo_id = p.id
                     AND d.detector_model != 'full-image'
                     AND d.detector_confidence >= ?
                    LEFT JOIN photo_masks pm
                      ON pm.photo_id = p.id AND pm.variant = ?
                    WHERE 1=1{scope_sql}""",
                (ws, min_conf, sam2_variant, *scope_params),
            ).fetchone()
        else:
            row = self.conn.execute(
                f"""SELECT
                      COUNT(DISTINCT p.id) AS eligible,
                      COUNT(DISTINCT CASE WHEN p.mask_path IS NULL THEN p.id END)
                        AS pending
                    FROM photos p
                    JOIN workspace_folders wf
                      ON wf.folder_id = p.folder_id AND wf.workspace_id = ?
                    JOIN detections d
                      ON d.photo_id = p.id
                     AND d.detector_model != 'full-image'
                     AND d.detector_confidence >= ?
                    WHERE 1=1{scope_sql}""",
                (ws, min_conf, *scope_params),
            ).fetchone()
        return {
            "eligible": row["eligible"] or 0,
            "pending": row["pending"] or 0,
        }

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
        ws = self._ws_id()
        scope_sql, scope_params = self._scope_clause(photo_ids)
        row = self.conn.execute(
            f"""SELECT
                  COUNT(*) AS eligible,
                  SUM(CASE WHEN p.thumb_path IS NULL THEN 1 ELSE 0 END)
                    AS pending
                FROM photos p
                JOIN workspace_folders wf
                  ON wf.folder_id = p.folder_id AND wf.workspace_id = ?
                WHERE 1=1{scope_sql}""",
            (ws, *scope_params),
        ).fetchone()
        return {
            "eligible": row["eligible"] or 0,
            "pending": row["pending"] or 0,
        }

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
        ws = self._ws_id()
        scope_sql, scope_params = self._scope_clause(photo_ids)
        row = self.conn.execute(
            f"""SELECT
                  COUNT(*) AS eligible,
                  SUM(CASE WHEN pc.photo_id IS NULL THEN 1 ELSE 0 END)
                    AS pending
                FROM photos p
                JOIN workspace_folders wf
                  ON wf.folder_id = p.folder_id AND wf.workspace_id = ?
                LEFT JOIN preview_cache pc
                  ON pc.photo_id = p.id AND pc.size = ?
                WHERE 1=1{scope_sql}""",
            (ws, size, *scope_params),
        ).fetchone()
        return {
            "eligible": row["eligible"] or 0,
            "pending": row["pending"] or 0,
        }

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
        ws = self._ws_id()
        scope_sql, scope_params = self._scope_clause(photo_ids)
        row = self.conn.execute(
            f"""SELECT
                  COUNT(*) AS eligible,
                  SUM(CASE
                        WHEN p.thumb_path IS NULL OR pc.photo_id IS NULL
                        THEN 1 ELSE 0 END) AS pending
                FROM photos p
                JOIN workspace_folders wf
                  ON wf.folder_id = p.folder_id AND wf.workspace_id = ?
                LEFT JOIN preview_cache pc
                  ON pc.photo_id = p.id AND pc.size = ?
                WHERE 1=1{scope_sql}""",
            (ws, size, *scope_params),
        ).fetchone()
        return {
            "eligible": row["eligible"] or 0,
            "pending": row["pending"] or 0,
        }

    def count_extract_stale(self, sam2_variant, photo_ids=None,
                             detector_confidence=None):
        """Count photos in scope that "look done" (``photos.mask_path``
        is set) but whose ``photo_masks`` row for ``sam2_variant`` has a
        stored prompt that no longer matches the photo's primary
        detection.

        Reuses the staleness predicate from ``find_stale_masks`` — a
        mask is fresh only when its stored ``(detector_model,
        prompt_xywh)`` equals the highest-confidence non-full-image
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
        ws = self._ws_id()
        if detector_confidence is None:
            detector_confidence = self.get_effective_config(cfg.load()).get(
                "detector_confidence", 0.2,
            )
        scope_sql, scope_params = self._scope_clause(photo_ids)
        row = self.conn.execute(
            f"""SELECT COUNT(DISTINCT pm.photo_id) AS n
                FROM photo_masks pm
                JOIN photos p ON p.id = pm.photo_id
                JOIN workspace_folders wf
                  ON wf.folder_id = p.folder_id AND wf.workspace_id = ?
               WHERE pm.variant = ?
                 AND p.mask_path IS NOT NULL
                 AND pm.path IS NOT NULL
                 AND pm.path != ''
                 AND EXISTS (
                    SELECT 1 FROM detections d0
                     WHERE d0.photo_id = pm.photo_id
                       AND d0.detector_model != 'full-image'
                       AND d0.detector_confidence >= ?
                 )
                 AND NOT EXISTS (
                    SELECT 1 FROM detections d
                     WHERE d.id = (
                           SELECT d2.id
                             FROM detections d2
                            WHERE d2.photo_id = pm.photo_id
                              AND d2.detector_model != 'full-image'
                              AND d2.detector_confidence >= ?
                            ORDER BY d2.detector_confidence DESC, d2.id ASC
                            LIMIT 1
                       )
                       AND d.detector_model = pm.detector_model
                       AND d.box_x = pm.prompt_x
                       AND d.box_y = pm.prompt_y
                       AND d.box_w = pm.prompt_w
                       AND d.box_h = pm.prompt_h
                 ){scope_sql}""",
            (ws, sam2_variant, detector_confidence, detector_confidence,
             *scope_params),
        ).fetchone()
        return row["n"] or 0

    def count_eye_keypoint_eligible(self, photo_ids=None):
        """Count photos eligible for the eye-keypoint stage, ignoring the
        ``eye_tenengrad IS NULL`` idempotency gate.

        Eligibility = mask present + at least one non-synthetic detection
        above min_conf + at least one prediction on that detection. Matches
        the join shape of ``list_photos_for_eye_keypoint_stage`` minus the
        "not yet processed" filter, so the plan can distinguish "no
        eligible photos" from "all eligible photos already processed".
        """
        import config as cfg
        ws = self._ws_id()
        min_conf = self.get_effective_config(cfg.load()).get(
            "detector_confidence", 0.2,
        )
        scope_sql, scope_params = self._scope_clause(photo_ids)
        row = self.conn.execute(
            f"""SELECT COUNT(DISTINCT p.id) AS n
                FROM photos p
                JOIN workspace_folders wf
                  ON wf.folder_id = p.folder_id AND wf.workspace_id = ?
                JOIN detections d
                  ON d.photo_id = p.id
                 AND d.detector_model != 'full-image'
                 AND d.detector_confidence >= ?
                JOIN predictions pr ON pr.detection_id = d.id
                WHERE p.mask_path IS NOT NULL{scope_sql}""",
            (ws, min_conf, *scope_params),
        ).fetchone()
        return row["n"] or 0

    def count_eye_keypoint_stale(self, photo_ids=None):
        """Count photos in scope whose eye_tenengrad is set under a
        non-current eye_kp_fingerprint. Mirrors
        ``count_eye_keypoint_eligible``'s join shape (workspace + mask +
        detection + prediction) and adds the staleness predicate.

        A NULL fingerprint on a row with eye_tenengrad set is treated as
        stale — only the migration backfill should produce that state,
        and even there the user is expected to re-run after a model
        change to restamp.
        """
        import config as cfg
        from pipeline import EYE_KP_FINGERPRINT_VERSION
        ws = self._ws_id()
        min_conf = self.get_effective_config(cfg.load()).get(
            "detector_confidence", 0.2,
        )
        scope_sql, scope_params = self._scope_clause(photo_ids)
        row = self.conn.execute(
            f"""SELECT COUNT(DISTINCT p.id) AS n
                FROM photos p
                JOIN workspace_folders wf
                  ON wf.folder_id = p.folder_id AND wf.workspace_id = ?
                JOIN detections d
                  ON d.photo_id = p.id
                 AND d.detector_model != 'full-image'
                 AND d.detector_confidence >= ?
                JOIN predictions pr ON pr.detection_id = d.id
                WHERE p.mask_path IS NOT NULL
                  AND p.eye_tenengrad IS NOT NULL
                  AND (p.eye_kp_fingerprint IS NULL
                       OR p.eye_kp_fingerprint != ?){scope_sql}""",
            (ws, min_conf, EYE_KP_FINGERPRINT_VERSION, *scope_params),
        ).fetchone()
        return row["n"] or 0

    def count_eye_keypoint_attemptable(self, min_species_conf, photo_ids=None):
        """Count photos whose top-routable prediction would actually be
        attempted by the eye-keypoint stage under the current config.

        Tighter than ``count_eye_keypoint_eligible``: that one matches the
        loose mask+detection+prediction join, which includes photos whose
        top prediction will be skipped at Gate 1 (classifier confidence
        below ``min_species_conf``) or fail taxonomy routing (anything
        outside the keys of ``pipeline._EYE_KEYPOINT_MODEL_FOR_CLASS``).
        Those photos never get an ``eye_kp_fingerprint`` stamped — by
        design, so a future config change can retry them — so they would
        permanently inflate ``eye_target`` and trip the "computed without
        eye keypoints" banner on every run.

        Match ``list_photos_for_eye_keypoint_stage``'s "best routable row
        per photo" selection so a taxonomy-bearing prediction wins over a
        taxonomy-less one. Predictions that route only via the scientific
        name → taxa-table fallback are *not* counted here (the SQL filter
        is taxonomy_class-only); those photos will still be attempted by
        the stage but will be undercounted in the target, which keeps
        ``attempts >= target`` and means the banner won't lie — at worst
        it stays quiet when it could have surfaced.
        """
        import config as cfg
        ws = self._ws_id()
        min_conf = self.get_effective_config(cfg.load()).get(
            "detector_confidence", 0.2,
        )
        scope_sql, scope_params = self._scope_clause(photo_ids)
        # Window function pins the same per-photo prediction the stage
        # would pick (taxonomy-present first, then detector_conf desc,
        # then species_conf desc) so the attemptable filter is applied to
        # the *winner*, not to any prediction the photo happens to carry.
        # The labels_fingerprint subquery mirrors
        # list_photos_for_eye_keypoint_stage so re-classified detections
        # only contribute their latest prediction set.
        row = self.conn.execute(
            f"""WITH ranked AS (
                    SELECT p.id AS photo_id,
                           pr.confidence AS species_conf,
                           pr.taxonomy_class,
                           ROW_NUMBER() OVER (
                               PARTITION BY p.id
                               ORDER BY
                                 CASE
                                     WHEN pr.taxonomy_class IS NOT NULL
                                       OR pr.scientific_name IS NOT NULL
                                     THEN 0 ELSE 1
                                 END,
                                 d.detector_confidence DESC,
                                 pr.confidence DESC
                           ) AS rn
                    FROM photos p
                    JOIN workspace_folders wf
                      ON wf.folder_id = p.folder_id
                     AND wf.workspace_id = ?
                    JOIN detections d
                      ON d.photo_id = p.id
                     AND d.detector_model != 'full-image'
                     AND d.detector_confidence >= ?
                    JOIN predictions pr ON pr.detection_id = d.id
                    WHERE p.mask_path IS NOT NULL
                      AND pr.labels_fingerprint = (
                          SELECT pr2.labels_fingerprint FROM predictions pr2
                          WHERE pr2.detection_id = pr.detection_id
                            AND pr2.classifier_model = pr.classifier_model
                          ORDER BY pr2.created_at DESC, pr2.id DESC
                          LIMIT 1
                      ){scope_sql}
                )
                SELECT COUNT(*) AS n FROM ranked
                WHERE rn = 1
                  AND taxonomy_class IN ('Aves', 'Mammalia')
                  AND species_conf >= ?""",
            (ws, min_conf, *scope_params, min_species_conf),
        ).fetchone()
        return row["n"] or 0

    def get_dashboard_stats(
        self, folder_id=None, collection_id=None, date_from=None, date_to=None,
    ):
        """Return scoped aggregate statistics and actionable gaps.

        Dashboard scope is metadata-only: photos remain countable while their
        storage is offline.  Signals that require reading source files (such
        as preview generation) separately restrict themselves to accessible
        folders and expose that distinction through ``accessible_photos``.
        """
        ws = self._ws_id()
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

        overview = self.conn.execute(
            f"""SELECT COUNT(DISTINCT p.id) AS total_photos,
                       COUNT(DISTINCT p.folder_id) AS folder_count,
                       COUNT(DISTINCT pk.keyword_id) AS keyword_count,
                       COUNT(DISTINCT CASE
                         WHEN f.status IN ('ok', 'partial') THEN p.id END
                       ) AS accessible_photos,
                       COUNT(DISTINCT CASE
                         WHEN f.status NOT IN ('ok', 'partial') THEN f.id END
                       ) AS missing_folder_count
                FROM photos p
                JOIN workspace_folders wf
                  ON wf.folder_id = p.folder_id AND wf.workspace_id = ?
                JOIN folders f ON f.id = p.folder_id
                LEFT JOIN photo_keywords pk ON pk.photo_id = p.id
                WHERE 1=1{scope_sql}""",
            (ws, *scope_params),
        ).fetchone()

        # The four pure-metadata aggregates below (top_keywords,
        # photos_by_month, rating_dist, flag_dist) intentionally don't filter
        # on folder status. They read DB-resident metadata that doesn't depend
        # on disk access, so an unmounted drive shouldn't blank the charts —
        # the dashboard should still describe the full workspace inventory.
        top_keywords = self.conn.execute(
            f"""SELECT k.name, k.is_species, COUNT(pk.photo_id) as photo_count
               FROM keywords k
               JOIN photo_keywords pk ON pk.keyword_id = k.id
               JOIN photos p ON p.id = pk.photo_id
               JOIN workspace_folders wf ON wf.folder_id = p.folder_id
               WHERE wf.workspace_id = ?{scope_sql}
               GROUP BY k.id
               ORDER BY photo_count DESC
               LIMIT 30""",
            (ws, *scope_params),
        ).fetchall()

        photos_by_month = self.conn.execute(
            f"""SELECT substr(p.timestamp, 1, 7) as month, COUNT(*) as count
            FROM photos p
            JOIN workspace_folders wf ON wf.folder_id = p.folder_id
            WHERE p.timestamp IS NOT NULL AND wf.workspace_id = ?{scope_sql}
            GROUP BY month
            ORDER BY month""",
            (ws, *scope_params),
        ).fetchall()

        rating_dist = self.conn.execute(
            f"""SELECT p.rating, COUNT(*) as count
            FROM photos p
            JOIN workspace_folders wf ON wf.folder_id = p.folder_id
            WHERE wf.workspace_id = ?{scope_sql}
            GROUP BY p.rating
            ORDER BY p.rating""",
            (ws, *scope_params),
        ).fetchall()

        flag_dist = self.conn.execute(
            f"""SELECT p.flag, COUNT(*) as count
            FROM photos p
            JOIN workspace_folders wf ON wf.folder_id = p.folder_id
            WHERE wf.workspace_id = ?{scope_sql}
            GROUP BY p.flag""",
            (ws, *scope_params),
        ).fetchall()

        # Review status lives in prediction_review (workspace-scoped).
        # Left-joining lets us count pending rows (those without a review row)
        # and bucket them into the pending column via COALESCE.
        #
        # Filter by detector_confidence so dashboard status counts stay in
        # sync with what the UI threshold actually shows, and scope to the
        # most recent labels_fingerprint per (detection, classifier_model)
        # so stale-label predictions from a prior label set don't drift the
        # totals away from the active labeling context.
        prediction_status = self.conn.execute(
            f"""SELECT COALESCE(pr_rev.status, 'pending') AS status,
                      COUNT(*) AS count
               FROM predictions pr
               JOIN detections d ON d.id = pr.detection_id
               JOIN photos p ON p.id = d.photo_id
               JOIN workspace_folders wf
                 ON wf.folder_id = p.folder_id AND wf.workspace_id = ?
               LEFT JOIN prediction_review pr_rev
                 ON pr_rev.prediction_id = pr.id AND pr_rev.workspace_id = ?
               WHERE d.detector_confidence >= ?
                 AND pr.labels_fingerprint = (
                    SELECT pr2.labels_fingerprint FROM predictions pr2
                    WHERE pr2.detection_id = pr.detection_id
                      AND pr2.classifier_model = pr.classifier_model
                    ORDER BY pr2.created_at DESC, pr2.id DESC
                    LIMIT 1
                 ){scope_sql}
               GROUP BY COALESCE(pr_rev.status, 'pending')""",
            (ws, ws, min_conf, *scope_params),
        ).fetchall()

        # Same threshold + fingerprint rules as prediction_status above, so
        # classified_count can't drift above detected_count as the threshold
        # moves.
        classified_count = self.conn.execute(
            f"""SELECT COUNT(DISTINCT d.photo_id)
               FROM predictions pr
               JOIN detections d ON d.id = pr.detection_id
               JOIN photos p ON p.id = d.photo_id
               JOIN workspace_folders wf
                 ON wf.folder_id = p.folder_id AND wf.workspace_id = ?
               WHERE d.detector_confidence >= ?
                 AND pr.labels_fingerprint = (
                    SELECT pr2.labels_fingerprint FROM predictions pr2
                    WHERE pr2.detection_id = pr.detection_id
                      AND pr2.classifier_model = pr.classifier_model
                    ORDER BY pr2.created_at DESC, pr2.id DESC
                    LIMIT 1
                 ){scope_sql}""",
            (ws, min_conf, *scope_params),
        ).fetchone()[0]

        # Needs Attention links only open photos that are currently
        # accessible. Keep the headline classification aggregate metadata-
        # complete above, but use this reachable subset for operational work.
        accessible_classified_count = self.conn.execute(
            f"""SELECT COUNT(DISTINCT d.photo_id)
               FROM predictions pr
               JOIN detections d ON d.id = pr.detection_id
               JOIN photos p ON p.id = d.photo_id
               JOIN workspace_folders wf
                 ON wf.folder_id = p.folder_id AND wf.workspace_id = ?
               JOIN folders f
                 ON f.id = p.folder_id AND f.status IN ('ok', 'partial')
               WHERE d.detector_confidence >= ?
                 AND pr.labels_fingerprint = (
                    SELECT pr2.labels_fingerprint FROM predictions pr2
                    WHERE pr2.detection_id = pr.detection_id
                      AND pr2.classifier_model = pr.classifier_model
                    ORDER BY pr2.created_at DESC, pr2.id DESC
                    LIMIT 1
                 ){scope_sql}""",
            (ws, min_conf, *scope_params),
        ).fetchone()[0]

        # photos_by_hour and quality_dist are also pure-metadata aggregates;
        # see the comment above the top_keywords block for the rationale.
        photos_by_hour = self.conn.execute(
            f"""SELECT CAST(substr(p.timestamp, 12, 2) AS INTEGER) as hour, COUNT(*) as count
            FROM photos p
            JOIN workspace_folders wf ON wf.folder_id = p.folder_id
            WHERE p.timestamp IS NOT NULL AND length(p.timestamp) >= 13
              AND wf.workspace_id = ?{scope_sql}
            GROUP BY hour
            ORDER BY hour""",
            (ws, *scope_params),
        ).fetchall()

        quality_dist = self.conn.execute(
            f"""SELECT
                CASE
                    WHEN p.quality_score IS NULL THEN -1
                    ELSE CAST(p.quality_score * 10 AS INTEGER)
                END as bucket,
                COUNT(*) as count
            FROM photos p
            JOIN workspace_folders wf ON wf.folder_id = p.folder_id
            WHERE wf.workspace_id = ?{scope_sql}
            GROUP BY bucket
            ORDER BY bucket""",
            (ws, *scope_params),
        ).fetchall()

        # min_conf already hoisted at top of get_dashboard_stats.
        # No folder-status filter — detections persist in the DB regardless
        # of disk presence, and prediction_status / classified_count above
        # don't filter either, so detected_count must match to keep the
        # dashboard's classified-vs-detected ratio internally consistent
        # when a folder is offline.
        detected_count = self.conn.execute(
            f"""SELECT COUNT(DISTINCT d.photo_id)
               FROM detections d
               JOIN photos p ON p.id = d.photo_id
               JOIN workspace_folders wf ON wf.folder_id = p.folder_id
               WHERE wf.workspace_id = ?
                 AND d.detector_confidence >= ?{scope_sql}""",
            (ws, min_conf, *scope_params),
        ).fetchone()[0]

        location_conditions = []
        self._append_location_status_filter(location_conditions, "none")
        missing_location = self.conn.execute(
            f"""SELECT COUNT(DISTINCT p.id)
                FROM photos p
                JOIN workspace_folders wf
                  ON wf.folder_id = p.folder_id AND wf.workspace_id = ?
                JOIN folders f
                  ON f.id = p.folder_id AND f.status IN ('ok', 'partial')
                WHERE {' AND '.join(location_conditions)}{scope_sql}""",
            (ws, *scope_params),
        ).fetchone()[0]

        pending_changes = self.conn.execute(
            f"""SELECT COUNT(*)
                FROM pending_changes pc
                JOIN photos p ON p.id = pc.photo_id
                JOIN workspace_folders wf
                  ON wf.folder_id = p.folder_id AND wf.workspace_id = ?
                WHERE pc.workspace_id = ?{scope_sql}""",
            (ws, ws, *scope_params),
        ).fetchone()[0]

        if preview_size:
            missing_previews = self.conn.execute(
                f"""SELECT COUNT(DISTINCT p.id)
                    FROM photos p
                    JOIN workspace_folders wf
                      ON wf.folder_id = p.folder_id AND wf.workspace_id = ?
                    JOIN folders f
                      ON f.id = p.folder_id
                     AND f.status IN ('ok', 'partial')
                    LEFT JOIN preview_cache pc
                      ON pc.photo_id = p.id AND pc.size = ?
                    WHERE pc.photo_id IS NULL{scope_sql}""",
                (ws, preview_size, *scope_params),
            ).fetchone()[0]
        else:
            missing_previews = 0

        duplicate_groups = self.conn.execute(
            f"""SELECT COUNT(*) FROM (
                  SELECT p.file_hash
                  FROM photos p
                  JOIN workspace_folders wf
                    ON wf.folder_id = p.folder_id AND wf.workspace_id = ?
                  JOIN folders f
                    ON f.id = p.folder_id AND f.status IN ('ok', 'partial')
                  WHERE p.file_hash IS NOT NULL
                    AND COALESCE(p.flag, 'none') != 'rejected'{scope_sql}
                  GROUP BY p.file_hash
                  HAVING COUNT(*) > 1
                )""",
            (ws, *scope_params),
        ).fetchone()[0]

        total_photos = overview["total_photos"] or 0

        return {
            "top_keywords": [dict(r) for r in top_keywords],
            "photos_by_month": [dict(r) for r in photos_by_month],
            "rating_distribution": [dict(r) for r in rating_dist],
            "flag_distribution": [dict(r) for r in flag_dist],
            "prediction_status": [dict(r) for r in prediction_status],
            "classified_count": classified_count,
            "photos_by_hour": [dict(r) for r in photos_by_hour],
            "quality_distribution": [dict(r) for r in quality_dist],
            "detected_count": detected_count,
            "total_photos": total_photos,
            "accessible_photos": overview["accessible_photos"] or 0,
            "missing_folder_count": overview["missing_folder_count"] or 0,
            "folder_count": overview["folder_count"] or 0,
            "keyword_count": overview["keyword_count"] or 0,
            "pending_changes": pending_changes,
            "attention": {
                "unclassified": max(
                    0,
                    (overview["accessible_photos"] or 0)
                    - accessible_classified_count,
                ),
                "missing_location": missing_location,
                "missing_previews": missing_previews,
                "preview_size": preview_size,
                "preview_enabled": bool(preview_size),
                "pending_sync": pending_changes,
                "duplicate_groups": duplicate_groups,
            },
        }

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

    def get_photo_location_statuses(self, photo_ids):
        """Return ``{photo_id: exif|assigned|none}`` for the requested photos."""
        if not photo_ids:
            return {}
        result = {}
        for chunk in _chunks(list(dict.fromkeys(photo_ids))):
            placeholders = ",".join("?" for _ in chunk)
            rows = self.conn.execute(
                f"""
                SELECT p.id,
                       CASE
                         WHEN p.latitude IS NOT NULL AND p.longitude IS NOT NULL
                           THEN 'exif'
                         WHEN EXISTS (
                           SELECT 1 FROM photo_keywords pk
                           JOIN keywords k ON k.id = pk.keyword_id
                           WHERE pk.photo_id = p.id
                             AND k.type = 'location'
                             AND k.latitude IS NOT NULL
                             AND k.longitude IS NOT NULL
                         ) THEN 'assigned'
                         ELSE 'none'
                       END AS location_status
                FROM photos p
                WHERE p.id IN ({placeholders})
                """,
                list(chunk),
            ).fetchall()
            result.update({row["id"]: row["location_status"] for row in rows})
        return result

    def get_calendar_data(
        self,
        year,
        folder_id=None,
        rating_min=None,
        keyword=None,
        keyword_match_case=False,
        keyword_whole_word=False,
        color_label=None,
        flag=None,
        location_status=None,
    ):
        """Return daily photo counts for a given year, scoped to active workspace."""
        ws = self._ws_id()
        conditions = ["wf.workspace_id = ?", "p.timestamp IS NOT NULL",
                      "substr(p.timestamp, 1, 4) = ?"]
        join_params = []
        where_params = [ws, str(year)]

        join_clause = ("JOIN workspace_folders wf ON wf.folder_id = p.folder_id"
                       "\nJOIN folders f ON f.id = p.folder_id AND f.status IN ('ok', 'partial')")

        if folder_id is not None:
            subtree = self.get_folder_subtree_ids(folder_id)
            placeholders = ",".join("?" for _ in subtree)
            conditions.append(f"p.folder_id IN ({placeholders})")
            where_params.extend(subtree)
        if rating_min is not None:
            conditions.append("p.rating >= ?")
            where_params.append(rating_min)
        if flag is not None:
            conditions.append("COALESCE(p.flag, 'none') = ?")
            where_params.append(flag)
        self._append_location_status_filter(conditions, location_status)
        if keyword is not None:
            kw_clause, kw_params = _keyword_token_clause(
                keyword,
                match_case=keyword_match_case,
                whole_word=keyword_whole_word,
            )
            if kw_clause:
                conditions.append(kw_clause)
                where_params.extend(kw_params)
        if color_label is not None:
            join_clause += "\nJOIN photo_color_labels pcl ON pcl.photo_id = p.id AND pcl.workspace_id = ?"
            join_params.append(ws)
            conditions.append("pcl.color = ?")
            where_params.append(color_label)

        params = join_params + where_params

        where = "WHERE " + " AND ".join(conditions)

        rows = self.conn.execute(
            f"""SELECT substr(p.timestamp, 1, 10) as day, COUNT(DISTINCT p.id) as count
            FROM photos p {join_clause} {where}
            GROUP BY day ORDER BY day""",
            params,
        ).fetchall()

        days = {r["day"]: r["count"] for r in rows}

        # Year bounds from all workspace photos (unfiltered)
        bounds = self.conn.execute(
            """SELECT MIN(substr(p.timestamp, 1, 4)) as min_y,
                      MAX(substr(p.timestamp, 1, 4)) as max_y
            FROM photos p
            JOIN workspace_folders wf ON wf.folder_id = p.folder_id
            JOIN folders f ON f.id = p.folder_id AND f.status IN ('ok', 'partial')
            WHERE wf.workspace_id = ? AND p.timestamp IS NOT NULL""",
            (ws,),
        ).fetchone()

        return {
            "year": year,
            "days": days,
            "min_year": int(bounds["min_y"]) if bounds["min_y"] else year,
            "max_year": int(bounds["max_y"]) if bounds["max_y"] else year,
        }

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
        conditions = ["wf.workspace_id = ?"]
        where_params = [self._ws_id()]
        join_params = []

        if folder_id is not None:
            subtree = self.get_folder_subtree_ids(folder_id)
            placeholders = ",".join("?" for _ in subtree)
            conditions.append(f"p.folder_id IN ({placeholders})")
            where_params.extend(subtree)
        if collection_id is not None:
            parts = self._build_collection_query(collection_id)
            if parts is None:
                raise ValueError("collection not found in active workspace")
            coll_folder_join, coll_join_clause, coll_where, coll_params = parts
            coll_subquery = (
                "SELECT DISTINCT p.id FROM photos p "
                f"{coll_folder_join} {coll_join_clause} {coll_where}"
            )
            conditions.append(f"p.id IN ({coll_subquery})")
            where_params.extend(coll_params)
        if rating_min is not None:
            conditions.append("p.rating >= ?")
            where_params.append(rating_min)
        if date_from is not None:
            conditions.append("p.timestamp >= ?")
            where_params.append(date_from)
        if date_to is not None:
            conditions.append("p.timestamp <= ?")
            where_params.append(_inclusive_date_to(date_to))
        if flag is not None:
            conditions.append("COALESCE(p.flag, 'none') = ?")
            where_params.append(flag)
        self._append_location_status_filter(conditions, location_status)

        join_clause = ("JOIN workspace_folders wf ON wf.folder_id = p.folder_id"
                       "\nJOIN folders f ON f.id = p.folder_id AND f.status IN ('ok', 'partial')")
        if keyword is not None:
            kw_clause, kw_params = _keyword_token_clause(
                keyword,
                match_case=keyword_match_case,
                whole_word=keyword_whole_word,
            )
            if kw_clause:
                conditions.append(kw_clause)
                where_params.extend(kw_params)

        if color_label is not None:
            join_clause += "\nJOIN photo_color_labels pcl ON pcl.photo_id = p.id AND pcl.workspace_id = ?"
            join_params.append(self._ws_id())
            conditions.append("pcl.color = ?")
            where_params.append(color_label)

        # join_params must precede where_params because JOIN placeholders appear
        # in the SQL before the WHERE placeholders.
        params = join_params + where_params

        where = "WHERE " + " AND ".join(conditions)

        sort_map = {
            "date": _PHOTO_DATE_ASC_ORDER,
            "date_desc": _PHOTO_DATE_DESC_ORDER,
            "name": "p.filename ASC, p.id ASC",
            "name_desc": "p.filename DESC, p.id ASC",
            "rating": "p.rating DESC, p.filename ASC, p.id ASC",
            "sharpness": "p.sharpness DESC, p.filename ASC, p.id ASC",
            "sharpness_asc": "p.sharpness ASC, p.filename ASC, p.id ASC",
            "quality": "p.quality_score DESC, p.filename ASC, p.id ASC",
        }
        order = sort_map.get(sort, _PHOTO_DATE_ASC_ORDER)

        page = max(1, page)
        offset = (page - 1) * per_page
        params.extend([per_page, offset])

        pcols = ", ".join(f"p.{c.strip()}" for c in self.PHOTO_COLS.split(","))
        distinct = "DISTINCT " if keyword is not None else ""
        query = f"""
            SELECT {distinct}{pcols} FROM photos p
            {join_clause}
            {where}
            ORDER BY {order}
            LIMIT ? OFFSET ?
        """
        return self.conn.execute(query, params).fetchall()

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
        conditions = ["wf.workspace_id = ?"]
        where_params = [self._ws_id()]
        join_params = []

        if folder_id is not None:
            subtree = self.get_folder_subtree_ids(folder_id)
            placeholders = ",".join("?" for _ in subtree)
            conditions.append(f"p.folder_id IN ({placeholders})")
            where_params.extend(subtree)
        if collection_id is not None:
            parts = self._build_collection_query(collection_id)
            if parts is None:
                raise ValueError("collection not found in active workspace")
            coll_folder_join, coll_join_clause, coll_where, coll_params = parts
            coll_subquery = (
                "SELECT DISTINCT p.id FROM photos p "
                f"{coll_folder_join} {coll_join_clause} {coll_where}"
            )
            conditions.append(f"p.id IN ({coll_subquery})")
            where_params.extend(coll_params)
        if rating_min is not None:
            conditions.append("p.rating >= ?")
            where_params.append(rating_min)
        if date_from is not None:
            conditions.append("p.timestamp >= ?")
            where_params.append(date_from)
        if date_to is not None:
            conditions.append("p.timestamp <= ?")
            where_params.append(_inclusive_date_to(date_to))
        if flag is not None:
            conditions.append("COALESCE(p.flag, 'none') = ?")
            where_params.append(flag)
        self._append_location_status_filter(conditions, location_status)

        join_clause = ("JOIN workspace_folders wf ON wf.folder_id = p.folder_id"
                       "\nJOIN folders f ON f.id = p.folder_id AND f.status IN ('ok', 'partial')")
        if keyword is not None:
            kw_clause, kw_params = _keyword_token_clause(
                keyword,
                match_case=keyword_match_case,
                whole_word=keyword_whole_word,
            )
            if kw_clause:
                conditions.append(kw_clause)
                where_params.extend(kw_params)

        if color_label is not None:
            join_clause += "\nJOIN photo_color_labels pcl ON pcl.photo_id = p.id AND pcl.workspace_id = ?"
            join_params.append(self._ws_id())
            conditions.append("pcl.color = ?")
            where_params.append(color_label)

        params = join_params + where_params
        where = "WHERE " + " AND ".join(conditions)

        sort_map = {
            "date": _PHOTO_DATE_ASC_ORDER,
            "date_desc": _PHOTO_DATE_DESC_ORDER,
            "name": "p.filename ASC, p.id ASC",
            "name_desc": "p.filename DESC, p.id ASC",
            "rating": "p.rating DESC, p.filename ASC, p.id ASC",
            "sharpness": "p.sharpness DESC, p.filename ASC, p.id ASC",
            "sharpness_asc": "p.sharpness ASC, p.filename ASC, p.id ASC",
            "quality": "p.quality_score DESC, p.filename ASC, p.id ASC",
        }
        order = sort_map.get(sort, _PHOTO_DATE_ASC_ORDER)
        distinct = "DISTINCT " if keyword is not None else ""
        query = f"""
            SELECT {distinct}p.id FROM photos p
            {join_clause}
            {where}
            ORDER BY {order}
        """
        return [row["id"] for row in self.conn.execute(query, params).fetchall()]

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
        conditions = ["wf.workspace_id = ?"]
        where_params = [self._ws_id()]
        join_params = []

        if folder_id is not None:
            subtree = self.get_folder_subtree_ids(folder_id)
            placeholders = ",".join("?" for _ in subtree)
            conditions.append(f"p.folder_id IN ({placeholders})")
            where_params.extend(subtree)
        if collection_id is not None:
            parts = self._build_collection_query(collection_id)
            if parts is None:
                raise ValueError("collection not found in active workspace")
            coll_folder_join, coll_join_clause, coll_where, coll_params = parts
            coll_subquery = (
                "SELECT DISTINCT p.id FROM photos p "
                f"{coll_folder_join} {coll_join_clause} {coll_where}"
            )
            conditions.append(f"p.id IN ({coll_subquery})")
            where_params.extend(coll_params)
        if rating_min is not None:
            conditions.append("p.rating >= ?")
            where_params.append(rating_min)
        if date_from is not None:
            conditions.append("p.timestamp >= ?")
            where_params.append(date_from)
        if date_to is not None:
            conditions.append("p.timestamp <= ?")
            where_params.append(_inclusive_date_to(date_to))
        if flag is not None:
            conditions.append("COALESCE(p.flag, 'none') = ?")
            where_params.append(flag)
        self._append_location_status_filter(conditions, location_status)

        join_clause = ("JOIN workspace_folders wf ON wf.folder_id = p.folder_id"
                       "\nJOIN folders f ON f.id = p.folder_id AND f.status IN ('ok', 'partial')")
        if keyword is not None:
            kw_clause, kw_params = _keyword_token_clause(
                keyword,
                match_case=keyword_match_case,
                whole_word=keyword_whole_word,
            )
            if kw_clause:
                conditions.append(kw_clause)
                where_params.extend(kw_params)

        if color_label is not None:
            join_clause += "\nJOIN photo_color_labels pcl ON pcl.photo_id = p.id AND pcl.workspace_id = ?"
            join_params.append(self._ws_id())
            conditions.append("pcl.color = ?")
            where_params.append(color_label)

        # join_params must precede where_params because JOIN placeholders appear
        # in the SQL before the WHERE placeholders.
        params = join_params + where_params

        where = "WHERE " + " AND ".join(conditions)

        query = f"""
            SELECT COUNT(DISTINCT p.id) FROM photos p
            {join_clause}
            {where}
        """
        return self.conn.execute(query, params).fetchone()[0]

    def get_browse_summary(
        self,
        folder_id=None,
        rating_min=None,
        date_from=None,
        date_to=None,
        keyword=None,
        keyword_match_case=False,
        keyword_whole_word=False,
        collection_id=None,
        color_label=None,
        flag=None,
        location_status=None,
    ):
        """Return summary stats for the browse panel, scoped to active workspace and filters."""
        ws = self._ws_id()

        # Build shared filter conditions
        conditions = ["wf.workspace_id = ?"]
        join_params = []
        where_params = [ws]
        if folder_id is not None:
            subtree = self.get_folder_subtree_ids(folder_id)
            placeholders = ",".join("?" for _ in subtree)
            conditions.append(f"p.folder_id IN ({placeholders})")
            where_params.extend(subtree)
        if rating_min is not None:
            conditions.append("p.rating >= ?")
            where_params.append(rating_min)
        if date_from is not None:
            conditions.append("p.timestamp >= ?")
            where_params.append(date_from)
        if date_to is not None:
            conditions.append("p.timestamp <= ?")
            where_params.append(_inclusive_date_to(date_to))
        if flag is not None:
            conditions.append("COALESCE(p.flag, 'none') = ?")
            where_params.append(flag)
        self._append_location_status_filter(conditions, location_status)

        # When browsing a collection, restrict photos to those matching the
        # collection's rules by using a subquery from _build_collection_query.
        if collection_id is not None:
            parts = self._build_collection_query(collection_id)
            if parts is not None:
                coll_folder_join, coll_join_clause, coll_where, coll_params = parts
                # Build a subquery that returns the photo IDs in this collection.
                # Use alias "p" to match the alias expected by _build_collection_query;
                # the subquery is wrapped in parentheses so "p" is scoped to it and
                # does not conflict with the outer query's "p" alias.
                coll_subquery = (
                    f"SELECT DISTINCT p.id FROM photos p "
                    f"{coll_folder_join} {coll_join_clause} {coll_where}"
                )
                conditions.append(f"p.id IN ({coll_subquery})")
                where_params.extend(coll_params)

        join_clause = ("JOIN workspace_folders wf ON wf.folder_id = p.folder_id"
                       "\nJOIN folders f ON f.id = p.folder_id AND f.status IN ('ok', 'partial')")
        if keyword is not None:
            kw_clause, kw_params = _keyword_token_clause(
                keyword,
                match_case=keyword_match_case,
                whole_word=keyword_whole_word,
            )
            if kw_clause:
                conditions.append(kw_clause)
                where_params.extend(kw_params)

        if color_label is not None:
            join_clause += "\nJOIN photo_color_labels pcl ON pcl.photo_id = p.id AND pcl.workspace_id = ?"
            join_params.append(self._ws_id())
            conditions.append("pcl.color = ?")
            where_params.append(color_label)

        # join_params must precede where_params because JOIN placeholders appear
        # in the SQL before the WHERE placeholders.
        params = join_params + where_params

        where = "WHERE " + " AND ".join(conditions)

        # Total (unfiltered) count
        total = self.conn.execute(
            """SELECT COUNT(*) FROM photos p
               JOIN workspace_folders wf ON wf.folder_id = p.folder_id
               JOIN folders f ON f.id = p.folder_id AND f.status IN ('ok', 'partial')
               WHERE wf.workspace_id = ?""",
            (ws,),
        ).fetchone()[0]

        # Filtered count
        filtered_total = self.conn.execute(
            f"SELECT COUNT(DISTINCT p.id) FROM photos p {join_clause} {where}",
            params,
        ).fetchone()[0]

        # Classified vs unclassified (within filter).  Detections and
        # predictions are global; workspace scoping comes from the outer
        # join_clause and the detector_confidence read-time threshold.
        import config as cfg
        min_conf = self.get_effective_config(cfg.load()).get(
            "detector_confidence", 0.2
        )
        classified = self.conn.execute(
            f"""SELECT COUNT(DISTINCT p.id) FROM photos p
                {join_clause}
                JOIN detections det ON det.photo_id = p.id
                JOIN predictions pred ON pred.detection_id = det.id
                {where}
                  AND det.detector_confidence >= ?""",
            params + [min_conf],
        ).fetchone()[0]

        # Top species (within filter).  Review status is workspace-scoped via
        # prediction_review; absent rows are treated as 'pending' (which is
        # included — we only want to exclude 'rejected' reviews).
        # Pin to the most recent labels_fingerprint per
        # (detection, classifier_model) so a workspace that rotated label
        # sets doesn't have stale higher-confidence rows from an old
        # fingerprint dominating the top-species ranking.
        top_species = self.conn.execute(
            f"""WITH best_pred AS (
                    SELECT det.photo_id, pred.species,
                           ROW_NUMBER() OVER (
                               PARTITION BY det.photo_id
                               ORDER BY pred.confidence DESC
                           ) AS rn
                    FROM predictions pred
                    JOIN detections det ON det.id = pred.detection_id
                    LEFT JOIN prediction_review pr_rev
                      ON pr_rev.prediction_id = pred.id
                     AND pr_rev.workspace_id = ?
                    WHERE det.detector_confidence >= ?
                      AND COALESCE(pr_rev.status, 'pending') != 'rejected'
                      AND pred.labels_fingerprint = (
                          SELECT pr2.labels_fingerprint FROM predictions pr2
                          WHERE pr2.detection_id = pred.detection_id
                            AND pr2.classifier_model = pred.classifier_model
                          ORDER BY pr2.created_at DESC, pr2.id DESC
                          LIMIT 1
                      )
                )
                SELECT bp.species, COUNT(DISTINCT p.id) as count
                FROM photos p
                {join_clause}
                JOIN best_pred bp ON bp.photo_id = p.id AND bp.rn = 1
                {where}
                GROUP BY bp.species
                ORDER BY count DESC
                LIMIT 5""",
            [ws, min_conf] + params,
        ).fetchall()

        # Folder breakdown (within filter)
        folder_counts = self.conn.execute(
            f"""SELECT f.id as folder_id, f.name, COUNT(DISTINCT p.id) as count
                FROM photos p
                {join_clause}
                {where}
                GROUP BY f.id
                ORDER BY count DESC""",
            params,
        ).fetchall()

        return {
            "total": total,
            "filtered_total": filtered_total,
            "classified": classified,
            "unclassified": filtered_total - classified,
            "top_species": [{"species": r["species"], "count": r["count"]} for r in top_species],
            "folder_counts": [{"folder_id": r["folder_id"], "name": r["name"], "count": r["count"]} for r in folder_counts],
        }

    def get_geolocated_photos(
        self,
        folder_id=None,
        rating_min=None,
        date_from=None,
        date_to=None,
        keyword=None,
        keyword_match_case=False,
        keyword_whole_word=False,
        species=None,
    ):
        """Return all geolocated photos with optional species, scoped to active workspace.

        Returns photos that have either non-null EXIF latitude/longitude OR a
        ``type='location'`` keyword link whose keyword has non-null coords. No
        pagination — returns all matching photos for map rendering. Includes
        the photo's species keyword (or NULL if none), derived from
        photo_keywords joined to keywords where is_species = 1.

        Output columns include ``coord_source`` (``'exif'`` or ``'keyword'``)
        and ``keyword_location_name`` (the location keyword's name when EXIF is
        absent, NULL otherwise) so the map can show provenance.
        """
        # Paired fallback: either BOTH EXIF axes win, or BOTH keyword axes win.
        # Per-axis COALESCE would let a photo with partial EXIF (only one axis
        # populated) emit a mixed pair, producing wrong markers.
        conditions = [
            "wf.workspace_id = ?",
            "((p.latitude IS NOT NULL AND p.longitude IS NOT NULL) "
            " OR (kl.latitude IS NOT NULL AND kl.longitude IS NOT NULL))",
        ]
        params = [self._ws_id()]

        if folder_id is not None:
            subtree = self.get_folder_subtree_ids(folder_id)
            placeholders = ",".join("?" for _ in subtree)
            conditions.append(f"p.folder_id IN ({placeholders})")
            params.extend(subtree)
        if rating_min is not None:
            conditions.append("p.rating >= ?")
            params.append(rating_min)
        if date_from is not None:
            conditions.append("p.timestamp >= ?")
            params.append(date_from)
        if date_to is not None:
            conditions.append("p.timestamp <= ?")
            params.append(_inclusive_date_to(date_to))

        # Pick one location keyword per photo. Ordering: prefer the deepest-
        # in-chain row (parent_id NOT NULL ranks before parent_id IS NULL),
        # tie-break by largest id (most recently inserted, typically the leaf).
        location_subquery = """
            LEFT JOIN (
                SELECT pk_loc.photo_id, k_loc.id AS id, k_loc.name AS name,
                       k_loc.latitude AS latitude, k_loc.longitude AS longitude,
                       ROW_NUMBER() OVER (
                         PARTITION BY pk_loc.photo_id
                         ORDER BY (k_loc.parent_id IS NULL) ASC, k_loc.id DESC
                       ) AS rn
                FROM photo_keywords pk_loc
                JOIN keywords k_loc ON k_loc.id = pk_loc.keyword_id
                WHERE k_loc.type = 'location'
                  AND k_loc.latitude IS NOT NULL
                  AND k_loc.longitude IS NOT NULL
            ) kl ON kl.photo_id = p.id AND kl.rn = 1
        """

        join_clause = (
            "JOIN workspace_folders wf ON wf.folder_id = p.folder_id"
            "\nJOIN folders f ON f.id = p.folder_id AND f.status IN ('ok', 'partial')"
            f"\n{location_subquery}"
        )
        if keyword is not None:
            kw_clause, kw_params = _keyword_token_clause(
                keyword,
                match_case=keyword_match_case,
                whole_word=keyword_whole_word,
            )
            if kw_clause:
                conditions.append(kw_clause)
                params.extend(kw_params)

        # Match any species tag on the photo, not just MIN(name) — a photo can be
        # tagged with multiple species keywords.
        if species is not None:
            conditions.append(
                """EXISTS (SELECT 1 FROM photo_keywords pk_f
                           JOIN keywords k_f ON k_f.id = pk_f.keyword_id
                           WHERE pk_f.photo_id = p.id
                             AND k_f.is_species = 1
                             AND k_f.name = ?)"""
            )
            params.append(species)

        where = "WHERE " + " AND ".join(conditions)

        # When filtering by species, surface that species in the row so the map
        # popup/legend match the active filter. Otherwise fall back to the
        # most-recently-tagged species keyword (highest rowid), which reflects
        # the user's latest confirmed identification when multiple tags exist.
        if species is not None:
            species_col_sql = "? AS species"
            species_col_params = [species]
        else:
            species_col_sql = (
                "(SELECT k2.name FROM photo_keywords pk2 "
                "JOIN keywords k2 ON k2.id = pk2.keyword_id "
                "WHERE pk2.photo_id = p.id AND k2.is_species = 1 "
                "ORDER BY pk2.rowid DESC LIMIT 1) AS species"
            )
            species_col_params = []

        query = f"""
            SELECT p.id,
                   CASE WHEN p.latitude IS NOT NULL AND p.longitude IS NOT NULL
                        THEN p.latitude ELSE kl.latitude END AS latitude,
                   CASE WHEN p.latitude IS NOT NULL AND p.longitude IS NOT NULL
                        THEN p.longitude ELSE kl.longitude END AS longitude,
                   CASE WHEN p.latitude IS NOT NULL AND p.longitude IS NOT NULL
                        THEN 'exif' ELSE 'keyword' END AS coord_source,
                   CASE WHEN p.latitude IS NOT NULL AND p.longitude IS NOT NULL
                        THEN NULL ELSE kl.name END AS keyword_location_name,
                   p.thumb_path, p.filename,
                   p.timestamp, p.rating, p.folder_id,
                   {species_col_sql}
            FROM photos p
            {join_clause}
            {where}
            GROUP BY p.id
            ORDER BY {_PHOTO_DATE_ASC_ORDER}
        """
        return self.conn.execute(query, species_col_params + params).fetchall()

    def get_assigned_photo_location(self, photo_id, verify_workspace=True):
        """Return linked location-keyword coordinates for one visible photo."""
        if verify_workspace:
            self._verify_photo_in_workspace(photo_id)

        row = self.conn.execute(
            """
            SELECT p.id,
                   kl.latitude AS latitude,
                   kl.longitude AS longitude,
                   kl.name AS keyword_location_name,
                   kl.place_id AS place_id
            FROM photos p
            LEFT JOIN (
                SELECT pk_loc.photo_id, k_loc.name, k_loc.place_id,
                       k_loc.latitude, k_loc.longitude,
                       ROW_NUMBER() OVER (
                         PARTITION BY pk_loc.photo_id
                         ORDER BY (k_loc.parent_id IS NULL) ASC, k_loc.id DESC
                       ) AS rn
                FROM photo_keywords pk_loc
                JOIN keywords k_loc ON k_loc.id = pk_loc.keyword_id
                WHERE pk_loc.photo_id = ?
                  AND k_loc.type = 'location'
                  AND k_loc.latitude IS NOT NULL
                  AND k_loc.longitude IS NOT NULL
            ) kl ON kl.photo_id = p.id AND kl.rn = 1
            WHERE p.id = ?
            """,
            (photo_id, photo_id),
        ).fetchone()
        if row is None or row["latitude"] is None or row["longitude"] is None:
            return None
        return {
            "photo_id": row["id"],
            "latitude": row["latitude"],
            "longitude": row["longitude"],
            "source": "keyword",
            "keyword_location_name": row["keyword_location_name"],
            "place_id": row["place_id"],
        }

    def get_effective_photo_location(self, photo_id, verify_workspace=True):
        """Return the coordinates Vireo should use for a single photo.

        EXIF GPS is source metadata and wins when both axes are present. If
        EXIF GPS is absent or partial, fall back as a pair to the linked
        ``type='location'`` keyword coordinates. Returns ``None`` when neither
        source has a complete coordinate pair.
        """
        if verify_workspace:
            self._verify_photo_in_workspace(photo_id)

        row = self.conn.execute(
            """
            SELECT p.id,
                   p.latitude AS photo_latitude,
                   p.longitude AS photo_longitude,
                   kl.latitude AS keyword_latitude,
                   kl.longitude AS keyword_longitude,
                   kl.name AS keyword_location_name,
                   kl.place_id AS place_id
            FROM photos p
            LEFT JOIN (
                SELECT pk_loc.photo_id, k_loc.name, k_loc.place_id,
                       k_loc.latitude, k_loc.longitude,
                       ROW_NUMBER() OVER (
                         PARTITION BY pk_loc.photo_id
                         ORDER BY (k_loc.parent_id IS NULL) ASC, k_loc.id DESC
                       ) AS rn
                FROM photo_keywords pk_loc
                JOIN keywords k_loc ON k_loc.id = pk_loc.keyword_id
                WHERE pk_loc.photo_id = ?
                  AND k_loc.type = 'location'
                  AND k_loc.latitude IS NOT NULL
                  AND k_loc.longitude IS NOT NULL
            ) kl ON kl.photo_id = p.id AND kl.rn = 1
            WHERE p.id = ?
            """,
            (photo_id, photo_id),
        ).fetchone()
        if row is None:
            return None

        if row["photo_latitude"] is not None and row["photo_longitude"] is not None:
            return {
                "photo_id": row["id"],
                "latitude": row["photo_latitude"],
                "longitude": row["photo_longitude"],
                "source": "exif",
                "keyword_location_name": None,
                "place_id": None,
            }

        if row["keyword_latitude"] is not None and row["keyword_longitude"] is not None:
            return {
                "photo_id": row["id"],
                "latitude": row["keyword_latitude"],
                "longitude": row["keyword_longitude"],
                "source": "keyword",
                "keyword_location_name": row["keyword_location_name"],
                "place_id": row["place_id"],
            }

        return None

    def get_accepted_species(self):
        """Return distinct marker species from geolocated photos in the active workspace.

        Uses the same "geolocated" definition as get_geolocated_photos: a
        photo is included if it has EXIF coords OR a ``type='location'``
        keyword with coords. That keeps the species filter dropdown in sync
        with which photos can actually appear as markers — otherwise photos
        placed via location-keyword coords would render on the map but their
        species would be missing from the filter.
        """
        ws = self._ws_id()
        return [
            row[0]
            for row in self.conn.execute(
                """
                SELECT DISTINCT k.name
                FROM photos p
                JOIN workspace_folders wf ON wf.folder_id = p.folder_id
                JOIN folders f ON f.id = p.folder_id AND f.status IN ('ok', 'partial')
                JOIN photo_keywords pk ON pk.photo_id = p.id
                JOIN keywords k ON k.id = pk.keyword_id AND k.is_species = 1
                WHERE wf.workspace_id = ?
                  AND (
                    (p.latitude IS NOT NULL AND p.longitude IS NOT NULL)
                    OR EXISTS (
                      SELECT 1
                      FROM photo_keywords pk_loc
                      JOIN keywords k_loc ON k_loc.id = pk_loc.keyword_id
                      WHERE pk_loc.photo_id = p.id
                        AND k_loc.type = 'location'
                        AND k_loc.latitude IS NOT NULL
                        AND k_loc.longitude IS NOT NULL
                    )
                  )
                ORDER BY k.name ASC
                """,
                (ws,),
            ).fetchall()
        ]

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
        row = self.conn.execute(
            """
            SELECT COUNT(*) FROM photos p
            JOIN workspace_folders wf ON wf.folder_id = p.folder_id
            JOIN folders f ON f.id = p.folder_id AND f.status IN ('ok', 'partial')
            WHERE wf.workspace_id = ?
              AND (p.latitude IS NULL OR p.longitude IS NULL)
              AND NOT EXISTS (
                SELECT 1 FROM photo_keywords pk
                JOIN keywords k ON k.id = pk.keyword_id
                WHERE pk.photo_id = p.id
                  AND k.type = 'location'
                  AND k.latitude IS NOT NULL
                  AND k.longitude IS NOT NULL
              )
            """,
            (self._ws_id(),),
        ).fetchone()
        return row[0]

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

    def update_photo_flag(self, photo_id, flag, verify_workspace=True):
        """Set photo flag ('none', 'flagged', 'rejected').

        Args:
            verify_workspace: when True (the default), raises ValueError if
                the photo is not in the active workspace's folders.
        """
        self._photo_review_repository().set_flag(
            photo_id, flag, verify_workspace=verify_workspace
        )

    def update_photo_wildlife_excluded(self, photo_id, excluded, verify_workspace=True):
        """Set whether a photo is excluded from wildlife detection/classification."""
        if verify_workspace:
            self._verify_photo_in_workspace(photo_id)
        self.conn.execute(
            "UPDATE photos SET wildlife_excluded = ? WHERE id = ?",
            (1 if excluded else 0, photo_id),
        )
        self.conn.commit()

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

    def _photo_label_repository(self):
        from repositories.photo_labels import PhotoLabelRepository

        return PhotoLabelRepository(
            self.conn,
            self._ws_id(),
            chunk_size=_SQLITE_PARAM_CHUNK_SIZE,
        )

    def get_photo_edit_recipe(self, photo_id, verify_workspace=False):
        """Return the normalized edit recipe dict for a photo, or None."""
        if verify_workspace:
            self._verify_photo_in_workspace(photo_id)
        row = self.conn.execute(
            "SELECT recipe_json FROM photo_edit_recipes WHERE photo_id = ?",
            (photo_id,),
        ).fetchone()
        if not row:
            return None
        try:
            from image_edits import copy_recipe
            return copy_recipe(row["recipe_json"])
        except Exception:
            log.warning("Invalid stored edit recipe for photo %s", photo_id, exc_info=True)
            return None

    def get_photo_edit_recipes(self, photo_ids):
        """Return {photo_id: normalized recipe dict} for the given photos."""
        if not photo_ids:
            return {}
        out = {}
        from image_edits import copy_recipe
        for chunk in _chunks(photo_ids):
            placeholders = ",".join("?" for _ in chunk)
            rows = self.conn.execute(
                f"SELECT photo_id, recipe_json FROM photo_edit_recipes "
                f"WHERE photo_id IN ({placeholders})",
                list(chunk),
            ).fetchall()
            for row in rows:
                try:
                    recipe = copy_recipe(row["recipe_json"])
                except Exception:
                    log.warning(
                        "Invalid stored edit recipe for photo %s",
                        row["photo_id"], exc_info=True,
                    )
                    continue
                if recipe:
                    out[row["photo_id"]] = recipe
        return out

    def set_photo_edit_recipe(self, photo_id, recipe, verify_workspace=True):
        """Set or clear a non-destructive edit recipe for a photo.

        Returns the normalized recipe dict, or None when the provided recipe is
        a no-op and the stored row was cleared.
        """
        if verify_workspace:
            self._verify_photo_in_workspace(photo_id)
        from image_edits import copy_recipe, recipe_to_json
        recipe_json = recipe_to_json(recipe)
        if recipe_json is None:
            self.conn.execute(
                "DELETE FROM photo_edit_recipes WHERE photo_id = ?",
                (photo_id,),
            )
            self.conn.commit()
            return None
        self.conn.execute(
            """INSERT INTO photo_edit_recipes (photo_id, recipe_json, updated_at)
               VALUES (?, ?, datetime('now'))
               ON CONFLICT(photo_id) DO UPDATE SET
                   recipe_json = excluded.recipe_json,
                   updated_at = excluded.updated_at""",
            (photo_id, recipe_json),
        )
        self.conn.commit()
        return copy_recipe(recipe_json)

    def clear_photo_edit_recipe(self, photo_id, verify_workspace=True):
        """Remove a photo's edit recipe. Returns True if a row was removed."""
        if verify_workspace:
            self._verify_photo_in_workspace(photo_id)
        cur = self.conn.execute(
            "DELETE FROM photo_edit_recipes WHERE photo_id = ?",
            (photo_id,),
        )
        self.conn.commit()
        return cur.rowcount > 0

    # --- edit presets (global, adjustments-only looks) ----------------------

    EDIT_PRESET_NAME_MAX = 80

    def list_edit_presets(self):
        """Return all edit presets, sorted case-insensitively by name.

        Presets are global (not workspace-scoped): they capture a look, and a
        look is the same look in every workspace.
        """
        from image_edits import copy_recipe

        rows = self.conn.execute(
            "SELECT id, name, recipe_json, updated_at FROM edit_presets"
        ).fetchall()
        out = []
        for row in rows:
            try:
                recipe = copy_recipe(row["recipe_json"])
            except Exception:
                log.warning(
                    "Invalid stored edit preset %s (%r)",
                    row["id"], row["name"], exc_info=True,
                )
                continue
            out.append({
                "id": row["id"],
                "name": row["name"],
                "recipe": recipe,
                "updated_at": row["updated_at"],
            })
        out.sort(key=lambda p: p["name"].casefold())
        return out

    def save_edit_preset(self, name, recipe):
        """Create or overwrite (by trimmed name) a global edit preset.

        Only the recipe's ``adjustments`` section is kept — geometry
        (rotation/flip/straighten/crop) describes one photo, not a look.
        Raises ValueError (or RecipeError, its subclass) for a blank or
        overlong name, a malformed recipe, or one with no effective
        adjustments. Returns the stored preset dict.
        """
        from image_edits import (
            RecipeError,
            copy_recipe,
            normalize_recipe,
            recipe_to_json,
        )

        if not isinstance(name, str) or not name.strip():
            raise ValueError("preset name must not be blank")
        name = name.strip()
        if len(name) > self.EDIT_PRESET_NAME_MAX:
            raise ValueError(
                f"preset name must be {self.EDIT_PRESET_NAME_MAX} "
                "characters or fewer"
            )

        if isinstance(recipe, str):
            recipe = normalize_recipe(recipe) or {}
        if not isinstance(recipe, dict):
            raise RecipeError("recipe must be an object")
        normalized = normalize_recipe(
            {"adjustments": recipe.get("adjustments") or {}}
        )
        if not (normalized or {}).get("adjustments"):
            raise ValueError("preset must include at least one adjustment")
        recipe_json = recipe_to_json(normalized)

        self.conn.execute(
            """INSERT INTO edit_presets (name, recipe_json, updated_at)
               VALUES (?, ?, datetime('now'))
               ON CONFLICT(name) DO UPDATE SET
                   recipe_json = excluded.recipe_json,
                   updated_at = excluded.updated_at""",
            (name, recipe_json),
        )
        self.conn.commit()
        row = self.conn.execute(
            "SELECT id, name, recipe_json, updated_at FROM edit_presets "
            "WHERE name = ?",
            (name,),
        ).fetchone()
        return {
            "id": row["id"],
            "name": row["name"],
            "recipe": copy_recipe(row["recipe_json"]),
            "updated_at": row["updated_at"],
        }

    def delete_edit_preset(self, preset_id):
        """Delete an edit preset. Returns True if a row was removed."""
        cur = self.conn.execute(
            "DELETE FROM edit_presets WHERE id = ?", (preset_id,)
        )
        self.conn.commit()
        return cur.rowcount > 0

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
        if not photo_ids:
            return {"deleted": 0, "ids": [], "files": []}

        # Resolve to actual existing photos. Chunked — callers like
        # /api/audit/remove-missing pass arbitrarily large id lists straight
        # from the request body.
        rows = []
        for chunk in _chunks(list(dict.fromkeys(photo_ids))):
            placeholders = ",".join("?" for _ in chunk)
            rows.extend(self.conn.execute(
                f"SELECT p.id, p.filename, p.companion_path, p.folder_id, f.path AS folder_path "
                f"FROM photos p JOIN folders f ON p.folder_id = f.id "
                f"WHERE p.id IN ({placeholders})",
                list(chunk),
            ).fetchall())

        if not rows:
            return {"deleted": 0, "ids": [], "files": []}

        # Resolve companions
        if include_companions:
            companion_ids = []
            for row in rows:
                if row["companion_path"]:
                    comp = self.conn.execute(
                        "SELECT id FROM photos WHERE folder_id = ? AND filename = ?",
                        (row["folder_id"], row["companion_path"]),
                    ).fetchone()
                    if comp and comp["id"] not in photo_ids:
                        companion_ids.append(comp["id"])
            if companion_ids:
                rows = list(rows)
                for chunk in _chunks(dict.fromkeys(companion_ids)):
                    comp_ph = ",".join("?" for _ in chunk)
                    rows.extend(self.conn.execute(
                        f"SELECT p.id, p.filename, p.companion_path, p.folder_id, f.path AS folder_path "
                        f"FROM photos p JOIN folders f ON p.folder_id = f.id "
                        f"WHERE p.id IN ({comp_ph})",
                        list(chunk),
                    ).fetchall())

        all_ids = list({row["id"] for row in rows})

        # Collect file info before deleting
        files = [
            {
                "photo_id": row["id"],
                "folder_path": row["folder_path"],
                "filename": row["filename"],
                "companion_path": row["companion_path"],
            }
            for row in rows
        ]

        # Count photos per folder for decrementing
        folder_counts = {}
        for row in rows:
            folder_counts[row["folder_id"]] = folder_counts.get(row["folder_id"], 0) + 1

        # Collect affected folder ids BEFORE the delete so we can invalidate the
        # new-images cache even if the delete raises. In "Remove from Vireo"
        # mode the on-disk files stay put, so they become eligible for new-image
        # detection again the moment the photo rows are gone; without an
        # invalidation here, ``/api/workspaces/active/new-images`` would keep
        # serving the stale pre-delete ``new_count`` until the TTL expired.
        affected_folder_ids = list(folder_counts.keys())

        # Chunk the all_ids IN-clauses. ``include_companions=True`` can double
        # the id count from the caller's input chunk (companions get merged in
        # above), so a 900-id outer chunk can reach ~1800 here — past the 999
        # SQLITE_MAX_VARIABLE_NUMBER on legacy builds. All chunked statements
        # share the same transaction, so partial-failure rollback still works.
        id_chunks = list(_chunks(all_ids))

        try:
            # Delete associated data (non-cascading FKs)
            for chunk in id_chunks:
                ph = ",".join("?" for _ in chunk)
                self.conn.execute(f"DELETE FROM photo_keywords WHERE photo_id IN ({ph})", chunk)
                self.conn.execute(f"DELETE FROM pending_changes WHERE photo_id IN ({ph})", chunk)
                # Deleting detections cascades to predictions via ON DELETE CASCADE
                self.conn.execute(f"DELETE FROM detections WHERE photo_id IN ({ph})", chunk)

            # Clean collection rules
            import json as _json
            collections = self.conn.execute(
                "SELECT id, rules FROM collections WHERE workspace_id = ?",
                (self._ws_id(),),
            ).fetchall()
            deleted_set = set(all_ids)
            def _remove_deleted_photo_ids(node):
                if isinstance(node, list):
                    changed_any = False
                    for child in node:
                        changed_any = _remove_deleted_photo_ids(child) or changed_any
                    return changed_any
                if not isinstance(node, dict):
                    return False
                changed_any = _remove_deleted_photo_ids(node.get("rules"))
                if node.get("field") == "photo_ids" and "value" in node:
                    values = node.get("value")
                    if not isinstance(values, list):
                        return changed_any
                    original_len = len(values)
                    node["value"] = [v for v in values if v not in deleted_set]
                    return changed_any or len(node["value"]) != original_len
                return changed_any

            for coll in collections:
                rules = _json.loads(coll["rules"])
                changed = _remove_deleted_photo_ids(rules)
                if changed:
                    self.conn.execute(
                        "UPDATE collections SET rules = ? WHERE id = ?",
                        (_json.dumps(rules), coll["id"]),
                    )

            # Delete photos (cascades to edit_history_items, inat_submissions)
            for chunk in id_chunks:
                ph = ",".join("?" for _ in chunk)
                self.conn.execute(f"DELETE FROM photos WHERE id IN ({ph})", chunk)

            # Update folder counts
            for fid, count in folder_counts.items():
                self.conn.execute(
                    "UPDATE folders SET photo_count = photo_count - ? WHERE id = ?",
                    (count, fid),
                )

            if commit:
                self.conn.commit()
        except Exception:
            if commit:
                self.conn.rollback()
            raise
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
    def preview_cache_insert(self, photo_id, size, bytes_):
        """Insert or replace a preview_cache entry. last_access_at = now()."""
        import time
        self.conn.execute(
            "INSERT OR REPLACE INTO preview_cache "
            "(photo_id, size, bytes, last_access_at) VALUES (?, ?, ?, ?)",
            (photo_id, size, bytes_, time.time()),
        )
        self.conn.commit()

    def preview_cache_touch(self, photo_id, size):
        """Update last_access_at for an existing entry. No-op if missing."""
        import time
        self.conn.execute(
            "UPDATE preview_cache SET last_access_at=? WHERE photo_id=? AND size=?",
            (time.time(), photo_id, size),
        )
        self.conn.commit()

    def preview_cache_delete(self, photo_id, size):
        """Delete a preview_cache entry (caller removes the file)."""
        self.conn.execute(
            "DELETE FROM preview_cache WHERE photo_id=? AND size=?",
            (photo_id, size),
        )
        self.conn.commit()

    def preview_cache_total_bytes(self):
        """Return total bytes tracked in preview_cache."""
        row = self.conn.execute(
            "SELECT COALESCE(SUM(bytes), 0) AS total FROM preview_cache"
        ).fetchone()
        return row["total"]

    def preview_cache_oldest_first(self):
        """Return all rows ordered by last_access_at ascending (oldest first)."""
        return self.conn.execute(
            "SELECT photo_id, size, bytes, last_access_at FROM preview_cache "
            "ORDER BY last_access_at ASC"
        ).fetchall()

    def preview_cache_get(self, photo_id, size):
        """Return the row for (photo_id, size), or None."""
        return self.conn.execute(
            "SELECT photo_id, size, bytes, last_access_at FROM preview_cache "
            "WHERE photo_id=? AND size=?",
            (photo_id, size),
        ).fetchone()

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
        execute_with_retry(
            self.conn,
            """INSERT OR REPLACE INTO offline_originals
               (photo_id, original_path, xmp_path, companion_path, bytes,
                source_size, source_mtime, cached_at, status, error)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            (
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
            ),
        )
        commit_with_retry(self.conn)

    def offline_original_get(self, photo_id):
        return self.conn.execute(
            """SELECT photo_id, original_path, xmp_path, companion_path, bytes,
                      source_size, source_mtime, cached_at, status, error
               FROM offline_originals WHERE photo_id=?""",
            (photo_id,),
        ).fetchone()

    def offline_original_delete(self, photo_id):
        execute_with_retry(
            self.conn,
            "DELETE FROM offline_originals WHERE photo_id=?",
            (photo_id,),
        )
        commit_with_retry(self.conn)

    def offline_original_total_bytes(self):
        row = self.conn.execute(
            "SELECT COALESCE(SUM(bytes), 0) AS total FROM offline_originals "
            "WHERE status='cached'"
        ).fetchone()
        return row["total"]

    def update_photo_sharpness(self, photo_id, sharpness):
        """Set photo sharpness score."""
        self.conn.execute(
            "UPDATE photos SET sharpness = ? WHERE id = ?", (sharpness, photo_id)
        )
        commit_with_retry(self.conn)

    def update_photo_quality(
        self,
        photo_id,
        subject_sharpness=None,
        subject_size=None,
        quality_score=None,
        sharpness=None,
    ):
        """Update all quality-related scores for a photo."""
        self.conn.execute(
            """UPDATE photos SET
               subject_sharpness=?, subject_size=?, quality_score=?, sharpness=?
               WHERE id=?""",
            (
                subject_sharpness,
                subject_size,
                quality_score,
                sharpness,
                photo_id,
            ),
        )
        commit_with_retry(self.conn)

    def get_photo_mask(self, photo_id, variant):
        row = self.conn.execute(
            "SELECT * FROM photo_masks WHERE photo_id=? AND variant=?",
            (photo_id, variant),
        ).fetchone()
        return dict(row) if row else None

    def list_masks_for_photo(self, photo_id):
        rows = self.conn.execute(
            "SELECT * FROM photo_masks WHERE photo_id=? ORDER BY created_at DESC",
            (photo_id,),
        ).fetchall()
        return [dict(r) for r in rows]

    def set_active_mask_variant(self, photo_id, variant, _commit=True):
        """Mark `variant` as active for `photo_id` and denormalize its
        fields into the photos row (mask_path + per-mask features) so
        downstream readers (scoring, pipeline) see the active mask.

        ``_commit=False`` lets bulk callers (e.g. the
        ``/api/pipeline/active-mask-variant`` endpoint) batch many
        per-photo updates into a single commit, instead of paying a WAL
        fsync per photo. Bulk callers MUST call ``commit_with_retry``
        themselves once the loop completes.
        """
        row = self.conn.execute(
            "SELECT path, subject_size, subject_tenengrad, bg_tenengrad, "
            "crop_complete FROM photo_masks WHERE photo_id=? AND variant=?",
            (photo_id, variant),
        ).fetchone()
        if row is None:
            raise ValueError(
                f"No photo_masks row for photo {photo_id} variant {variant!r}"
            )
        self.conn.execute(
            "UPDATE photos SET mask_path=?, active_mask_variant=?, "
            "subject_size=?, subject_tenengrad=?, bg_tenengrad=?, "
            "crop_complete=? WHERE id=?",
            (row["path"], variant, row["subject_size"],
             row["subject_tenengrad"], row["bg_tenengrad"],
             row["crop_complete"], photo_id),
        )
        if _commit:
            commit_with_retry(self.conn)

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
        active_count = self.conn.execute(
            "SELECT COUNT(*) FROM photos WHERE active_mask_variant=?",
            (variant,),
        ).fetchone()[0]
        if active_count > 0:
            raise ValueError(
                f"Variant {variant!r} is active for {active_count} photo(s); "
                "switch active variant before deleting"
            )
        rows = self.conn.execute(
            "SELECT path FROM photo_masks WHERE variant=?", (variant,),
        ).fetchall()
        for r in rows:
            self._safe_remove_mask_file(r["path"])
        self.conn.execute("DELETE FROM photo_masks WHERE variant=?", (variant,))
        commit_with_retry(self.conn)
        return len(rows)

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
        rows = self.conn.execute(
            "SELECT pm.photo_id, pm.variant, pm.path FROM photo_masks pm "
            "JOIN photos p ON p.id = pm.photo_id "
            "WHERE p.active_mask_variant IS NOT NULL "
            "  AND p.active_mask_variant != pm.variant"
        ).fetchall()
        for r in rows:
            self._safe_remove_mask_file(r["path"])
            self.conn.execute(
                "DELETE FROM photo_masks WHERE photo_id=? AND variant=?",
                (r["photo_id"], r["variant"]),
            )
        commit_with_retry(self.conn)
        return len(rows)

    def find_stale_masks(self, detector_confidence=None):
        """Return photo_masks rows whose stored prompt no longer matches
        the photo's current primary detection (highest-confidence,
        non full-image).

        A mask is fresh only if its stored ``(detector_model, prompt_*)``
        equals the photo's primary detection — the single
        highest-confidence non-``full-image`` row. Matching against any
        detection (e.g., a low-confidence secondary box still carrying
        the old coordinates, or another retained model's row) would
        leave stale cache entries lingering after detector/model
        changes, so we pick exactly one primary row per photo and
        require the prompt to equal that row.

        Tie-break: when multiple detections share the maximum
        confidence, both this query and the extraction code in
        ``api_job_extract_masks`` / ``get_detections`` resolve to the
        smallest ``detections.id`` (insertion order). Using
        ``MAX(detector_confidence)`` here would leave the primary
        ambiguous on ties — a mask matching either tied row could be
        treated as fresh even though extraction is now using the other
        one. ``ORDER BY detector_confidence DESC, id ASC LIMIT 1`` keeps
        stale-detection and extraction in sync.

        ``detector_confidence`` is an optional workspace floor (the same
        threshold both extraction paths apply when picking detections to
        run SAM on). When provided, detections below the floor are
        invisible to this query, so masks whose prompt only matches a
        below-threshold box — i.e. masks the pipeline would no longer
        regenerate from that detection — are correctly flagged stale.
        Without this filter, raising ``detector_confidence`` left the
        storage card under-counting stale masks and ``delete_stale_masks``
        leaving them on disk.
        """
        if detector_confidence is None:
            conf_pred = ""
            params = ()
        else:
            conf_pred = " AND d2.detector_confidence >= ?"
            params = (detector_confidence,)
        rows = self.conn.execute(
            f"""
            SELECT pm.photo_id, pm.variant, pm.path,
                   pm.detector_model, pm.prompt_x, pm.prompt_y,
                   pm.prompt_w, pm.prompt_h
              FROM photo_masks pm
             WHERE NOT EXISTS (
                SELECT 1 FROM detections d
                 WHERE d.id = (
                       SELECT d2.id
                         FROM detections d2
                        WHERE d2.photo_id = pm.photo_id
                          AND d2.detector_model != 'full-image'
                          {conf_pred}
                        ORDER BY d2.detector_confidence DESC, d2.id ASC
                        LIMIT 1
                   )
                   AND d.detector_model = pm.detector_model
                   AND d.box_x = pm.prompt_x
                   AND d.box_y = pm.prompt_y
                   AND d.box_w = pm.prompt_w
                   AND d.box_h = pm.prompt_h
             )
            """,
            params,
        ).fetchall()
        return [dict(r) for r in rows]

    def delete_stale_masks(self, detector_confidence=None):
        """Remove rows + files for masks whose prompt no longer matches
        the current primary detection. Skips active variants (caller can
        re-run them through the pipeline instead of dropping the
        currently-displayed mask).

        ``detector_confidence`` is forwarded to :meth:`find_stale_masks`
        so the deletion set matches the count the storage card shows.
        """
        stale = self.find_stale_masks(detector_confidence=detector_confidence)
        deleted = 0
        for s in stale:
            is_active = self.conn.execute(
                "SELECT 1 FROM photos WHERE id=? AND active_mask_variant=?",
                (s["photo_id"], s["variant"]),
            ).fetchone()
            if is_active:
                continue
            self._safe_remove_mask_file(s["path"])
            self.conn.execute(
                "DELETE FROM photo_masks WHERE photo_id=? AND variant=?",
                (s["photo_id"], s["variant"]),
            )
            deleted += 1
        commit_with_retry(self.conn)
        return deleted

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
        ws = self._ws_id()
        rows = self.conn.execute(
            """
            SELECT pm.variant,
                   COUNT(DISTINCT pm.photo_id) AS count,
                   SUM(CASE WHEN p.active_mask_variant = pm.variant
                            THEN 1 ELSE 0 END) AS active_count
              FROM photo_masks pm
              JOIN photos p ON p.id = pm.photo_id
              JOIN workspace_folders wf ON wf.folder_id = p.folder_id
             WHERE wf.workspace_id = ?
             GROUP BY pm.variant
             ORDER BY pm.variant
            """,
            (ws,),
        ).fetchall()
        return [
            {"variant": r["variant"],
             "count": r["count"] or 0,
             "active_count": r["active_count"] or 0}
            for r in rows
        ]

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

        ws = self._ws_id()
        scope_sql, scope_params = self._scope_clause(photo_ids)
        target_row = self.conn.execute(
            f"""SELECT COUNT(DISTINCT p.id) AS n
                  FROM photos p
                  JOIN workspace_folders wf
                    ON wf.folder_id = p.folder_id AND wf.workspace_id = ?
                  JOIN detections d
                    ON d.photo_id = p.id
                   AND d.detector_model != 'full-image'
                   AND d.detector_confidence >= ?
                 WHERE 1=1{scope_sql}""",
            (ws, min_conf, *scope_params),
        ).fetchone()
        target_count = target_row["n"] or 0
        if target_count == 0:
            return None

        coverage_rows = self.conn.execute(
            f"""SELECT pm.variant, COUNT(DISTINCT pm.photo_id) AS count
                  FROM photo_masks pm
                  JOIN photos p ON p.id = pm.photo_id
                  JOIN workspace_folders wf
                    ON wf.folder_id = p.folder_id AND wf.workspace_id = ?
                  JOIN detections d
                    ON d.photo_id = p.id
                   AND d.detector_model != 'full-image'
                   AND d.detector_confidence >= ?
                 WHERE pm.variant != 'unknown'
                   AND pm.path IS NOT NULL
                   AND pm.path != ''
                   AND p.mask_path IS NOT NULL{scope_sql}
                 GROUP BY pm.variant""",
            (ws, min_conf, *scope_params),
        ).fetchall()
        counts = {r["variant"]: r["count"] or 0 for r in coverage_rows}
        selected_count = counts.get(sam2_variant, 0)
        selected_ratio = selected_count / target_count
        if selected_ratio > selected_max_ratio:
            return None

        alternates = [
            (variant, count, count / target_count)
            for variant, count in counts.items()
            if variant != sam2_variant
        ]
        if not alternates:
            return None
        alt_variant, alt_count, alt_ratio = max(
            alternates, key=lambda item: (item[2], item[1], item[0])
        )
        if alt_ratio < alternate_min_ratio:
            return None

        return {
            "code": "sam_variant_rerun",
            "selected_variant": sam2_variant,
            "selected_count": selected_count,
            "selected_ratio": selected_ratio,
            "alternate_variant": alt_variant,
            "alternate_count": alt_count,
            "alternate_ratio": alt_ratio,
            "target_count": target_count,
            "message": (
                f"{sam2_variant} has masks for {selected_count} of "
                f"{target_count} target photos, while {alt_variant} already "
                f"has masks for {alt_count}. Starting will rerun SAM for the "
                f"selected variant."
            ),
        }

    def mask_variants_summary(self):
        """Per-variant summary: count, total bytes (best-effort, sums
        on-disk file sizes), and active_count.

        Returns: list of dicts ordered by variant name.
        """
        rows = self.conn.execute(
            """
            SELECT pm.variant,
                   COUNT(*) AS count,
                   SUM(CASE WHEN p.active_mask_variant = pm.variant
                            THEN 1 ELSE 0 END) AS active_count
              FROM photo_masks pm
              JOIN photos p ON p.id = pm.photo_id
             GROUP BY pm.variant
             ORDER BY pm.variant
            """
        ).fetchall()
        out = []
        for r in rows:
            paths = self.conn.execute(
                "SELECT path FROM photo_masks WHERE variant=?", (r["variant"],),
            ).fetchall()
            total = 0
            for pr in paths:
                try:
                    if pr["path"] and os.path.isfile(pr["path"]):
                        total += os.path.getsize(pr["path"])
                except OSError:
                    pass
            out.append({
                "variant": r["variant"],
                "count": r["count"],
                "active_count": r["active_count"],
                "bytes": total,
            })
        return out

    def upsert_photo_mask(
        self, photo_id, variant, path,
        detector_model, prompt_x, prompt_y, prompt_w, prompt_h,
        subject_size=None, subject_tenengrad=None,
        bg_tenengrad=None, crop_complete=None,
    ):
        """Insert or replace a mask row for (photo_id, variant)."""
        self.conn.execute(
            """
            INSERT INTO photo_masks (
                photo_id, variant, path, created_at,
                detector_model, prompt_x, prompt_y, prompt_w, prompt_h,
                subject_size, subject_tenengrad, bg_tenengrad, crop_complete
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(photo_id, variant) DO UPDATE SET
                path=excluded.path,
                created_at=excluded.created_at,
                detector_model=excluded.detector_model,
                prompt_x=excluded.prompt_x,
                prompt_y=excluded.prompt_y,
                prompt_w=excluded.prompt_w,
                prompt_h=excluded.prompt_h,
                subject_size=excluded.subject_size,
                subject_tenengrad=excluded.subject_tenengrad,
                bg_tenengrad=excluded.bg_tenengrad,
                crop_complete=excluded.crop_complete
            """,
            (photo_id, variant, path, int(time.time()),
             detector_model, prompt_x, prompt_y, prompt_w, prompt_h,
             subject_size, subject_tenengrad, bg_tenengrad, crop_complete),
        )
        commit_with_retry(self.conn)

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
    ):
        """Update pipeline feature columns for a photo.

        Only updates columns whose values are explicitly provided (not _UNSET).
        """
        cols = {
            "mask_path": mask_path,
            "subject_tenengrad": subject_tenengrad,
            "bg_tenengrad": bg_tenengrad,
            "crop_complete": crop_complete,
            "bg_separation": bg_separation,
            "subject_clip_high": subject_clip_high,
            "subject_clip_low": subject_clip_low,
            "subject_y_median": subject_y_median,
            "phash_crop": phash_crop,
            "noise_estimate": noise_estimate,
            "eye_x": eye_x,
            "eye_y": eye_y,
            "eye_conf": eye_conf,
            "eye_tenengrad": eye_tenengrad,
            "eye_kp_fingerprint": eye_kp_fingerprint,
        }
        # Filter to only provided values
        updates = {k: v for k, v in cols.items() if v is not _UNSET}
        if not updates:
            return
        set_clause = ", ".join(f"{k}=?" for k in updates)
        values = list(updates.values()) + [photo_id]
        self.conn.execute(
            f"UPDATE photos SET {set_clause} WHERE id=?", values
        )
        commit_with_retry(self.conn)

    def get_photos_missing_masks(self, folder_ids=None):
        """Get photos that have detections but no masks yet.

        Returns photos that have at least one detection in the current workspace
        but no mask_path set. Each row includes the primary (highest-confidence)
        detection box.

        Args:
            folder_ids: optional list of folder IDs to filter by.
                        If None, returns all workspace photos without masks.
        Returns:
            list of dicts with id, folder_id, filename, detection_box (JSON string), detection_conf
        """
        import config as cfg
        ws_id = self._ws_id()
        min_conf = self.get_effective_config(cfg.load()).get(
            "detector_confidence", 0.2
        )
        if folder_ids:
            # Detections are global post-refactor, so folder filtering alone
            # leaks photos from folders that belong to other workspaces if
            # the caller happens to pass foreign folder ids. Explicitly
            # JOIN workspace_folders to keep this helper workspace-scoped.
            placeholders = ",".join("?" * len(folder_ids))
            rows = self.conn.execute(
                f"""SELECT p.id, p.folder_id, p.filename,
                           d.box_x, d.box_y, d.box_w, d.box_h,
                           d.detector_confidence
                    FROM photos p
                    JOIN workspace_folders wf
                      ON wf.folder_id = p.folder_id AND wf.workspace_id = ?
                    JOIN detections d ON d.photo_id = p.id
                    WHERE p.folder_id IN ({placeholders})
                      AND p.mask_path IS NULL
                      AND d.detector_confidence >= ?
                    ORDER BY p.id, d.detector_confidence DESC, d.id ASC""",
                [ws_id, *folder_ids, min_conf],
            ).fetchall()
        else:
            rows = self.conn.execute(
                """SELECT p.id, p.folder_id, p.filename,
                          d.box_x, d.box_y, d.box_w, d.box_h,
                          d.detector_confidence
                   FROM photos p
                   JOIN workspace_folders wf ON wf.folder_id = p.folder_id
                   JOIN detections d ON d.photo_id = p.id
                   WHERE wf.workspace_id = ?
                     AND p.mask_path IS NULL
                     AND d.detector_confidence >= ?
                   ORDER BY p.id, d.detector_confidence DESC, d.id ASC""",
                (ws_id, min_conf),
            ).fetchall()

        # Deduplicate to one row per photo (primary detection = highest confidence)
        import json as _json
        seen = set()
        result = []
        for r in rows:
            if r["id"] in seen:
                continue
            seen.add(r["id"])
            result.append({
                "id": r["id"],
                "folder_id": r["folder_id"],
                "filename": r["filename"],
                "detection_box": _json.dumps({
                    "x": r["box_x"], "y": r["box_y"],
                    "w": r["box_w"], "h": r["box_h"],
                }),
                "detection_conf": r["detector_confidence"],
            })
        return result

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

        Returns one row per photo. The row chosen is the highest-confidence
        prediction on the highest-confidence real detection **among
        predictions that carry routable taxonomy info** (taxonomy_class or
        scientific_name set); predictions missing both fields are only
        chosen when nothing else is available. This prevents a top-ranked
        but taxonomy-less prediction from masking a lower-ranked prediction
        that ``_resolve_keypoint_model`` could actually route. Each row is
        a dict with the fields the eye stage needs to run without further
        DB calls: id, folder_id, filename, width, height, mask_path,
        box_x/y/w/h (normalized 0-1), species_conf, taxonomy_class,
        scientific_name, species.
        """
        import config as cfg
        from pipeline import EYE_KP_FINGERPRINT_VERSION
        ws_id = self._ws_id()
        min_conf = self.get_effective_config(cfg.load()).get(
            "detector_confidence", 0.2
        )
        if photo_ids is not None:
            photo_ids = list(photo_ids)
            if not photo_ids:
                return []
        extra_where, scope_params = self._scope_clause(photo_ids)
        params = (ws_id, min_conf, EYE_KP_FINGERPRINT_VERSION, *scope_params)
        rows = self.conn.execute(
            f"""SELECT p.id, p.folder_id, p.filename, p.width, p.height,
                      p.mask_path,
                      d.box_x, d.box_y, d.box_w, d.box_h,
                      d.detector_confidence,
                      pr.confidence AS species_conf,
                      pr.taxonomy_class,
                      pr.scientific_name,
                      pr.species
               FROM photos p
               JOIN workspace_folders wf
                 ON wf.folder_id = p.folder_id AND wf.workspace_id = ?
               JOIN detections d
                 ON d.photo_id = p.id
                AND d.detector_model != 'full-image'
                AND d.detector_confidence >= ?
               JOIN predictions pr ON pr.detection_id = d.id
               WHERE p.mask_path IS NOT NULL
                 AND (p.eye_kp_fingerprint IS NULL
                      OR p.eye_kp_fingerprint != ?){extra_where}
                 AND pr.labels_fingerprint = (
                    SELECT pr2.labels_fingerprint FROM predictions pr2
                    WHERE pr2.detection_id = pr.detection_id
                      AND pr2.classifier_model = pr.classifier_model
                    ORDER BY pr2.created_at DESC, pr2.id DESC
                    LIMIT 1
                 )
               ORDER BY p.id,
                        CASE
                            WHEN pr.taxonomy_class IS NOT NULL
                              OR pr.scientific_name IS NOT NULL THEN 0
                            ELSE 1
                        END,
                        d.detector_confidence DESC,
                        pr.confidence DESC""",
            params,
        ).fetchall()

        seen = set()
        result = []
        for r in rows:
            if r["id"] in seen:
                continue
            seen.add(r["id"])
            result.append({
                "id": r["id"],
                "folder_id": r["folder_id"],
                "filename": r["filename"],
                "width": r["width"],
                "height": r["height"],
                "mask_path": r["mask_path"],
                "box_x": r["box_x"],
                "box_y": r["box_y"],
                "box_w": r["box_w"],
                "box_h": r["box_h"],
                "species_conf": r["species_conf"],
                "taxonomy_class": r["taxonomy_class"],
                "scientific_name": r["scientific_name"],
                "species": r["species"],
            })
        return result

    def update_photo_embeddings(
        self, photo_id, dino_subject_embedding=None, dino_global_embedding=None,
        variant=None,
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
        """
        self.conn.execute(
            "UPDATE photos SET dino_subject_embedding=?, dino_global_embedding=?, "
            "dino_embedding_variant=? WHERE id=?",
            (dino_subject_embedding, dino_global_embedding, variant, photo_id),
        )
        commit_with_retry(self.conn)

    # -- Keywords --

    def detect_keyword_case_convention(self):
        """Detect the casing convention used by existing species keywords.

        Returns:
            'title' if most are Title Case (e.g. "Black Phoebe")
            'lower' if most are lowercase after first word (e.g. "Black phoebe")
            'upper' if most are ALL CAPS
            None if not enough data to determine
        """
        rows = self.conn.execute(
            "SELECT name FROM keywords WHERE is_species = 1"
        ).fetchall()
        if len(rows) < 3:
            return None

        title_count = 0
        lower_count = 0
        for r in rows:
            name = r["name"]
            words = name.split()
            if len(words) < 2:
                continue
            # Check the second word's casing
            second = words[1]
            if second[0].isupper():
                title_count += 1
            else:
                lower_count += 1

        if lower_count > title_count:
            return "lower"
        elif title_count > lower_count:
            return "title"
        return None

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

    def resolve_species_display_name(self, name):
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
        """
        name = normalize_keyword_display(name)
        if not name:
            return name
        rows = self.conn.execute(
            "SELECT name, type, is_species FROM keywords "
            "WHERE name = ? COLLATE NOCASE AND parent_id IS NULL "
            "AND type IN ('taxonomy', 'general') "
            "ORDER BY (type = 'taxonomy') DESC, id ASC",
            (name,),
        ).fetchall()
        if rows:
            species_rows = [
                r for r in rows
                if r["is_species"] or r["type"] == "taxonomy"
            ]
            if len(species_rows) == 1:
                return species_rows[0]["name"]
            if len(species_rows) > 1:
                for r in species_rows:
                    if r["name"] == name:
                        return r["name"]
                return name
            return rows[0]["name"]
        linked_taxa = self.conn.execute(
            """SELECT DISTINCT taxon_id FROM keywords
               WHERE name = ? COLLATE NOCASE
                 AND parent_id IS NOT NULL
                 AND (is_species = 1 OR type = 'taxonomy')
                 AND taxon_id IS NOT NULL""",
            (name,),
        ).fetchall()
        if len(linked_taxa) == 1:
            root = self.conn.execute(
                """SELECT name FROM keywords
                   WHERE parent_id IS NULL
                     AND taxon_id = ?
                     AND (is_species = 1 OR type = 'taxonomy')
                   ORDER BY id LIMIT 1""",
                (linked_taxa[0]["taxon_id"],),
            ).fetchone()
            if root is not None:
                return root["name"]
            # No canonical root row exists for this linked taxon (for
            # example a hierarchy-only accept whose top-level ``Verdin``
            # never got created). Return the matched leaf's stored
            # spelling so callers that gate on exact ``k.name``
            # (highlight/preference/life-list eligibility for a
            # hierarchy-only tag) still see a bucket the photo actually
            # carries — the case-convention fallback below would mint a
            # different spelling (``black phoebe`` -> ``Black Phoebe``)
            # and the saved highlight would disappear on reload.
            leaf = self.conn.execute(
                """SELECT name FROM keywords
                   WHERE name = ? COLLATE NOCASE
                     AND parent_id IS NOT NULL
                     AND (is_species = 1 OR type = 'taxonomy')
                     AND taxon_id = ?
                   ORDER BY id LIMIT 1""",
                (name, linked_taxa[0]["taxon_id"]),
            ).fetchone()
            if leaf is not None and leaf["name"]:
                return leaf["name"]
        import config as cfg
        override = cfg.get("keyword_case")
        if override and override != "auto":
            return self._apply_case_convention(name, override)
        convention = self.detect_keyword_case_convention()
        if convention:
            return self._apply_case_convention(name, convention)
        return name

    def _species_root_name_for_taxon(self, taxon_id):
        """Canonical root keyword spelling for a species taxon, if any.

        ``resolve_species_display_name`` uses the same lookup to route
        curation keys through the canonical root when only a hierarchy
        alias is stored (e.g. a leaf ``Desert Verdin`` after
        ``repair_duplicate_photo_species`` detached the top-level
        ``Verdin``). Callers with the taxon id in hand can skip the
        name-based lookup and go straight to the root row.
        """
        if taxon_id is None:
            return None
        row = self.conn.execute(
            """SELECT name FROM keywords
               WHERE parent_id IS NULL
                 AND taxon_id = ?
                 AND (is_species = 1 OR type = 'taxonomy')
               ORDER BY id LIMIT 1""",
            (taxon_id,),
        ).fetchone()
        if row is None:
            return None
        return row["name"] or None

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
        for variant in _taxon_lookup_variants(name):
            if prefer_species or species_only:
                direct = self.conn.execute(
                    """SELECT t.id, t.rank FROM taxa t
                       WHERE t.common_name = ? COLLATE NOCASE
                          OR t.name = ? COLLATE NOCASE
                       ORDER BY (t.rank = 'species') DESC, t.id ASC
                       LIMIT 1""",
                    (variant, variant),
                ).fetchone()
                if direct and direct["rank"] == "species":
                    return direct["id"]
                # Direct match (if any) is not species-rank. Consult the
                # common-names index for a species-rank alternate before
                # returning the higher-rank direct hit.
                common = self.conn.execute(
                    """SELECT tcn.taxon_id AS id, t.rank FROM taxa_common_names tcn
                       JOIN taxa t ON t.id = tcn.taxon_id
                       WHERE tcn.name = ? COLLATE NOCASE
                       ORDER BY (t.rank = 'species') DESC, t.id ASC
                       LIMIT 1""",
                    (variant,),
                ).fetchone()
                if common and common["rank"] == "species":
                    return common["id"]
                if species_only:
                    # Reject the higher-rank fallback: an explicit
                    # species add would stamp is_species=1 on this row
                    # and every rank reader would then hide the tag.
                    continue
                # No species-rank match on this variant; prefer the direct
                # (higher-rank) hit, otherwise fall back to the common-name
                # hit if one exists.
                if direct:
                    return direct["id"]
                if common:
                    return common["id"]
            else:
                taxon = self.conn.execute(
                    """SELECT t.id FROM taxa t
                       WHERE t.common_name = ? COLLATE NOCASE
                          OR t.name = ? COLLATE NOCASE
                       LIMIT 1""",
                    (variant, variant),
                ).fetchone()
                if taxon:
                    return taxon["id"]
                taxon = self.conn.execute(
                    """SELECT t.taxon_id AS id FROM taxa_common_names t
                       WHERE t.name = ? COLLATE NOCASE
                       LIMIT 1""",
                    (variant,),
                ).fetchone()
                if taxon:
                    return taxon["id"]
        return None

    def add_keyword(self, name, parent_id=None, is_species=False, kw_type=None, _commit=True):
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
        """
        if kw_type is not None and kw_type not in KEYWORD_TYPES:
            raise ValueError(f"invalid keyword type: {kw_type!r}")
        # Normalization choke point: every keywords.name write funnels
        # through here (or update_keyword / _upsert_one_keyword), and the
        # v5 migration normalized all pre-existing rows, so stored names
        # are always in normalize_keyword_display() form and the plain
        # COLLATE NOCASE dedupe below is sufficient.
        name = normalize_keyword_display(name)
        # Reject names that normalize to empty. Input like `"'"` is
        # non-empty before normalization (so the API boundary's `if not
        # name` guard passes), but the strip turns it into `""`. Without
        # this check, we would insert an invisible keyword row that could
        # still be tagged, synced to XMP, and reported in duplicate cleanup.
        if not name:
            raise ValueError("keyword name is empty after normalization")
        # Reconcile is_species and kw_type to keep the legacy column coherent
        # with the type enum.
        if is_species and kw_type is not None and kw_type != 'taxonomy':
            raise ValueError(
                f"is_species=True requires kw_type='taxonomy', got {kw_type!r}"
            )
        if kw_type == 'taxonomy':
            is_species = True
        # Symmetric reconciliation: callers like the prediction-accept and
        # pipeline-apply flows pass is_species=True with no kw_type. Treat
        # that as a typed taxonomy lookup so the candidate-filtering below
        # correctly excludes a same-name 'individual'/'location'/'genre'
        # row (e.g. a person tag named "Robin"). Without this, the
        # untyped lookup would return the homonym row, the typed promotion
        # would no-op (only 'general'/'taxonomy' get promoted), and the
        # caller would get back a non-taxonomy id — silently mis-tagging
        # the accepted species.
        if is_species and kw_type is None:
            kw_type = 'taxonomy'
        # Case-insensitive lookup with type-aware matching:
        #
        # When kw_type is supplied, only same-type or 'general' rows are
        # candidates. Same-type wins; 'general' is promotable to the
        # requested type via the UPDATE below. Other deliberate types
        # (location/individual/etc.) are intentionally NOT candidates —
        # returning one and finding the upgrade no-op'd would leave the
        # caller with a mismatched type. Falling through to INSERT
        # creates a new row of the requested type alongside the
        # deliberate one (duplicates by name across types are
        # intentional in this PR).
        #
        # When kw_type is None, prefer the most "structured"
        # interpretation in a fixed priority — taxonomy > genre >
        # individual > location > general — so a type-agnostic caller
        # (e.g. typing into a generic keyword input) doesn't silently
        # bind to a hand-tagged 'general' duplicate when a canonical
        # typed row exists. Tie-break by id for determinism.
        # NB: SQL literals here are constants, not parameter bindings.
        type_priority_case = (
            "CASE type "
            "WHEN 'taxonomy' THEN 0 "
            "WHEN 'genre' THEN 1 "
            "WHEN 'individual' THEN 2 "
            "WHEN 'location' THEN 3 "
            "ELSE 4 END"
        )
        if parent_id is None:
            if kw_type is None:
                existing = self.conn.execute(
                    f"SELECT id, type FROM keywords WHERE name = ? COLLATE NOCASE "
                    f"AND parent_id IS NULL "
                    f"ORDER BY {type_priority_case}, id ASC LIMIT 1",
                    (name,),
                ).fetchone()
            else:
                existing = self.conn.execute(
                    "SELECT id, type FROM keywords WHERE name = ? COLLATE NOCASE "
                    "AND parent_id IS NULL AND type IN (?, 'general') "
                    "ORDER BY (type = ?) DESC, id ASC LIMIT 1",
                    (name, kw_type, kw_type),
                ).fetchone()
        else:
            if kw_type is None:
                existing = self.conn.execute(
                    f"SELECT id, type FROM keywords WHERE name = ? COLLATE NOCASE "
                    f"AND parent_id = ? "
                    f"ORDER BY {type_priority_case}, id ASC LIMIT 1",
                    (name, parent_id),
                ).fetchone()
            else:
                existing = self.conn.execute(
                    "SELECT id, type FROM keywords WHERE name = ? COLLATE NOCASE "
                    "AND parent_id = ? AND type IN (?, 'general') "
                    "ORDER BY (type = ?) DESC, id ASC LIMIT 1",
                    (name, parent_id, kw_type, kw_type),
                ).fetchone()
        if existing:
            # Older confirmed-species rows can be correctly typed yet have a
            # NULL taxon_id because the historic is_species=True insert path
            # skipped taxonomy lookup. Backfill opportunistically whenever
            # such a row is resolved so future identity checks use taxon_id.
            # When the preferred lookup returns a species-rank taxon and the
            # row is already bound to a non-species-rank homonym (e.g. an
            # older catalog stamped ``Puma`` with the genus taxon before
            # ``prefer_species`` existed), overwrite that binding. Without
            # this rebind the row stays flagged is_species/taxonomy while
            # its taxon_id points at a genus/family, so every downstream
            # ``t.rank = 'species'`` filter (Life List, Compare, Explorer)
            # silently drops photos carrying the accepted keyword.
            if is_species or kw_type == 'taxonomy':
                # species_only: this branch stamps the row is_species=1
                # via the taxonomy promotion below, so binding to a
                # higher-rank homonym would silently hide the accepted
                # keyword behind every downstream rank='species' filter.
                taxon_id = self._lookup_taxon_id_for_keyword(
                    name, species_only=True,
                )
                if taxon_id:
                    self.conn.execute(
                        "UPDATE keywords SET taxon_id = ? "
                        "WHERE id = ? AND type IN ('general', 'taxonomy') "
                        "AND ("
                        "  taxon_id IS NULL "
                        "  OR ("
                        "    (SELECT rank FROM taxa WHERE id = ?) = 'species' "
                        "    AND COALESCE("
                        "      (SELECT rank FROM taxa WHERE id = keywords.taxon_id), ''"
                        "    ) != 'species'"
                        "  )"
                        ")",
                        (taxon_id, existing["id"], taxon_id),
                    )
                else:
                    # No species-rank taxon available. Clear a stale
                    # non-species taxon_id (e.g. a legacy row bound to
                    # the genus/family from the old species-agnostic
                    # lookup) so the row falls under the readers'
                    # ``t.rank IS NULL`` branch until a genuine
                    # species-rank match becomes available. Without
                    # this the is_species/taxonomy promotion below
                    # still succeeds, but every downstream
                    # ``t.rank = 'species' OR t.rank IS NULL`` filter
                    # (Life List, Compare, Explorer, highlight /
                    # preference eligibility) silently hides the
                    # just-accepted keyword.
                    self.conn.execute(
                        "UPDATE keywords SET taxon_id = NULL "
                        "WHERE id = ? AND type IN ('general', 'taxonomy') "
                        "AND taxon_id IS NOT NULL "
                        "AND COALESCE("
                        "  (SELECT rank FROM taxa WHERE id = keywords.taxon_id), ''"
                        ") != 'species'",
                        (existing["id"],),
                    )
            if kw_type is None and not is_species and existing["type"] == "general":
                taxon_id = self._lookup_taxon_id_for_keyword(
                    name, prefer_species=True,
                )
                if taxon_id:
                    self.conn.execute(
                        "UPDATE keywords SET is_species = 1, type = 'taxonomy', "
                        "taxon_id = CASE "
                        "  WHEN taxon_id IS NULL THEN ? "
                        "  WHEN (SELECT rank FROM taxa WHERE id = ?) = 'species' "
                        "    AND COALESCE("
                        "      (SELECT rank FROM taxa WHERE id = keywords.taxon_id), ''"
                        "    ) != 'species' THEN ? "
                        "  ELSE taxon_id END "
                        "WHERE id = ? AND type = 'general'",
                        (taxon_id, taxon_id, taxon_id, existing["id"]),
                    )
                    if _commit:
                        self.conn.commit()
            # Promote an unset row to taxonomy when this call indicates a
            # species. Restrict to 'general' (the legacy default for unknown
            # rows) so a deliberate user type — 'individual', 'location',
            # 'genre' — is preserved instead of silently rewritten when a
            # later caller passes is_species=True or kw_type='taxonomy'.
            if is_species:
                self.conn.execute(
                    "UPDATE keywords SET is_species = 1, type = 'taxonomy' "
                    "WHERE id = ? AND is_species = 0 AND type IN ('general', 'taxonomy')",
                    (existing["id"],),
                )
                if _commit:
                    self.conn.commit()
            # Upgrade an existing 'general' row to the explicitly requested type.
            # Without this, callers like the "Not Wildlife" button would hit the
            # case-insensitive fast path and silently get back a wrong-typed row.
            if kw_type and kw_type != 'general':
                self.conn.execute(
                    "UPDATE keywords SET type = ? WHERE id = ? AND type = 'general'",
                    (kw_type, existing["id"]),
                )
                if kw_type == 'taxonomy':
                    # Gate on type='taxonomy' so a preserved deliberate type
                    # (e.g. 'individual') doesn't get is_species=1 stamped on
                    # it when the type update above was a no-op. Otherwise
                    # _maybe_apply_auto_wildlife / backfill_wildlife_genre /
                    # subject filters with `OR is_species=1` would treat that
                    # non-taxonomy row as a species.
                    self.conn.execute(
                        "UPDATE keywords SET is_species = 1 "
                        "WHERE id = ? AND type = 'taxonomy'",
                        (existing["id"],),
                    )
                if _commit:
                    self.conn.commit()
            return existing["id"]

        # Apply casing convention for new species keywords
        if is_species:
            import config as cfg

            override = cfg.get("keyword_case")
            if override and override != "auto":
                name = self._apply_case_convention(name, override)
            else:
                convention = self.detect_keyword_case_convention()
                if convention:
                    name = self._apply_case_convention(name, convention)

        # Explicit species/taxonomy inserts still need their taxonomy link.
        # The earlier kw_type reconciliation turns is_species=True into
        # kw_type='taxonomy'; limiting lookup to kw_type is None therefore
        # created new confirmed-species rows with taxon_id=NULL, defeating
        # taxon-aware dedupe against hierarchical XMP leaves. Require a
        # species-rank taxon here (species_only=True) — the INSERT below
        # stamps is_species=1 as soon as any taxon is found, and
        # downstream rank filters would drop the just-linked keyword if
        # we bound it to a genus/family homonym. Leaving ``taxon_id``
        # NULL when no species-rank match exists keeps the tag visible
        # to readers via the ``t.rank IS NULL`` branch.
        taxon_id = (
            self._lookup_taxon_id_for_keyword(name, species_only=True)
            if kw_type == 'taxonomy'
            else None
        )
        if kw_type is None:
            # Auto-detect taxonomy type from taxa table
            kw_type = 'general'
            if is_species:
                kw_type = 'taxonomy'
            else:
                taxon_id = self._lookup_taxon_id_for_keyword(
                    name, prefer_species=True,
                )
                if taxon_id:
                    kw_type = 'taxonomy'

        cur = self.conn.execute(
            "INSERT INTO keywords (name, parent_id, is_species, type, taxon_id) VALUES (?, ?, ?, ?, ?)",
            (name, parent_id, 1 if is_species else (1 if taxon_id else 0), kw_type, taxon_id),
        )
        if _commit:
            self.conn.commit()
        return cur.lastrowid

    def _upsert_one_keyword(
        self, name, parent_id, place_id=None, latitude=None, longitude=None,
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
            insert_sql = (
                "INSERT INTO keywords "
                "(name, parent_id, type, place_id, latitude, longitude) "
                "VALUES (?, ?, 'location', ?, ?, ?) "
                "ON CONFLICT(place_id) WHERE place_id IS NOT NULL DO UPDATE SET "
                "  name = excluded.name, "
                "  parent_id = excluded.parent_id, "
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
            existing = self.conn.execute(
                "SELECT id FROM keywords "
                "WHERE name = ? AND parent_id IS NULL "
                "  AND type = 'location' AND place_id IS NULL",
                (name,),
            ).fetchone()
        else:
            existing = self.conn.execute(
                "SELECT id FROM keywords "
                "WHERE name = ? AND parent_id = ? "
                "  AND type = 'location' AND place_id IS NULL",
                (name, parent_id),
            ).fetchone()
        if existing:
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
            if clash["type"] == "location" and clash["place_id"] is None:
                # Defensive: our narrow SELECT missed it (shouldn't happen,
                # but reusing it is safe and idempotent).
                return clash["id"]
            raise RuntimeError(
                f"keyword '{name}' (parent_id={parent_id}) exists with "
                f"type={clash['type']!r}, can't reuse for location chain"
            ) from integrity_err

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

    def _upsert_location_parent_chain(self, components, leaf_name="", leaf_types=None):
        """Upsert a chain of parent location keywords from ``address_components``.

        Walks broadest → narrowest, returning the list of visited keyword ids
        in broadest → narrowest order. Returns an empty list if ``components``
        is empty / all entries lack a name. The deepest (narrowest) parent is
        ``chain[-1]`` if non-empty. Caller is responsible for the surrounding
        transaction.
        """
        chain: list[int] = []
        parent_id = None
        for comp in self._location_parent_components(components, leaf_name, leaf_types):
            if not comp.get("name"):
                continue
            parent_id = self._upsert_one_keyword(
                name=comp["name"],
                parent_id=parent_id,
                place_id=None,
                latitude=None,
                longitude=None,
            )
            chain.append(parent_id)
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

        with self.conn:
            chain = self._upsert_location_parent_chain(
                components,
                leaf_name=name,
                leaf_types=details.get("types"),
            )
            parent_id = chain[-1] if chain else None
            leaf_id = self._upsert_one_keyword(
                name=name,
                parent_id=parent_id,
                place_id=details["place_id"],
                latitude=lat,
                longitude=lng,
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
        row = self.conn.execute(
            "SELECT type FROM keywords WHERE id = ?", (leaf_keyword_id,),
        ).fetchone()
        if row is None:
            raise ValueError(f"keyword id {leaf_keyword_id} does not exist")
        if row["type"] != "location":
            raise ValueError(
                f"keyword {leaf_keyword_id} has type={row['type']!r}, "
                f"not 'location'"
            )
        with self.conn:
            self.conn.execute(
                "DELETE FROM photo_keywords WHERE photo_id = ? "
                "AND keyword_id IN (SELECT id FROM keywords WHERE type='location')",
                (photo_id,),
            )
            self.conn.execute(
                "INSERT OR IGNORE INTO photo_keywords (photo_id, keyword_id) "
                "VALUES (?, ?)",
                (photo_id, leaf_keyword_id),
            )

    def clear_photo_location(self, photo_id):
        """Remove any ``type='location'`` keyword links for ``photo_id``.

        Does NOT delete the keyword rows themselves — other photos may still
        reference them, and even if they don't, free-text/place-id keywords
        are part of the user's vocabulary.
        """
        with self.conn:
            self.conn.execute(
                "DELETE FROM photo_keywords WHERE photo_id = ? "
                "AND keyword_id IN (SELECT id FROM keywords WHERE type='location')",
                (photo_id,),
            )

    def get_or_create_text_location(self, name):
        """Find or create a free-text ``type='location'`` keyword.

        No ``place_id``, no coords, no parent. Whitespace is stripped from
        ``name``; raises ``ValueError`` if the stripped result is empty.
        Returns the keyword id.
        """
        if name is None:
            raise ValueError("location name must not be empty")
        stripped = name.strip()
        if not stripped:
            raise ValueError("location name must not be empty")
        with self.conn:
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
                    # Re-point photo_keywords from old → canonical.
                    self.conn.execute(
                        "INSERT OR IGNORE INTO photo_keywords (photo_id, keyword_id) "
                        "SELECT photo_id, ? FROM photo_keywords WHERE keyword_id = ?",
                        (canonical_id, keyword_id),
                    )
                    self.conn.execute(
                        "DELETE FROM photo_keywords WHERE keyword_id = ?",
                        (keyword_id,),
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
        row = self.conn.execute(
            "SELECT place_id, response FROM place_reverse_geocode_cache "
            "WHERE lat_grid = ? AND lng_grid = ?",
            (lat_grid, lng_grid),
        ).fetchone()
        if row is None:
            return None
        return {"place_id": row["place_id"], "response": row["response"]}

    def reverse_geocode_cache_put(self, lat, lng, place_id, response_json):
        """Upsert reverse-geocode result at the (lat, lng) grid cell.

        ``place_id`` may be ``None`` to cache a negative result (Google
        returned no match). ``response_json`` is already a JSON string —
        the caller serializes; we don't re-encode.
        """
        lat_grid, lng_grid = self._reverse_geocode_grid(lat, lng)
        fetched_at = int(time.time())
        with self.conn:
            self.conn.execute(
                "INSERT INTO place_reverse_geocode_cache "
                "  (lat_grid, lng_grid, place_id, response, fetched_at) "
                "VALUES (?, ?, ?, ?, ?) "
                "ON CONFLICT(lat_grid, lng_grid) DO UPDATE SET "
                "  place_id   = excluded.place_id, "
                "  response   = excluded.response, "
                "  fetched_at = excluded.fetched_at",
                (lat_grid, lng_grid, place_id, response_json, fetched_at),
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
        ws = self._ws_id()
        total_merged = 0
        try:
            total_merged = self._merge_duplicate_keywords_pass(ws)
        except Exception:
            self.conn.rollback()
            raise
        if total_merged:
            self.conn.commit()
        return total_merged

    def _merge_duplicate_keywords_pass(self, ws):
        """Convergence loop for merge_duplicate_keywords. Caller commits."""
        total_merged = 0
        while True:
            rows = self.conn.execute(
                """WITH RECURSIVE
                   tagged AS (
                       SELECT DISTINCT pk.keyword_id AS id
                       FROM photo_keywords pk
                       JOIN photos p ON p.id = pk.photo_id
                       JOIN workspace_folders wf ON wf.folder_id = p.folder_id
                       WHERE wf.workspace_id = ?
                   ),
                   in_scope AS (
                       SELECT id FROM tagged
                       UNION
                       SELECT k.parent_id
                       FROM keywords k
                       JOIN in_scope s ON s.id = k.id
                       WHERE k.parent_id IS NOT NULL
                   )
                   SELECT k.id, k.name, k.parent_id, k.type, k.is_species
                   FROM keywords k
                   JOIN in_scope s ON s.id = k.id""",
                (ws,),
            ).fetchall()
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
                placeholders = ",".join("?" * len(all_ids))
                alive = {
                    row["id"] for row in self.conn.execute(
                        f"SELECT id FROM keywords WHERE id IN ({placeholders})",
                        all_ids,
                    )
                }
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
        normalization, keeping every dependent name string (pending sidecar
        changes, species curation snapshots) in lockstep with the row.

        Retargeting of pending changes and species curation rows is scoped
        to photos that actually carry ``keyword_id`` (and, for pending
        changes, the workspaces those (photo, keyword) tags belong to), so
        a separate same-spelling keyword row elsewhere in the DB is never
        rewritten by side effect.

        ``disambiguate_on_conflict`` — when a different-type keyword already
        occupies (cleaned, parent_id) and the UPDATE would hit
        ``UNIQUE(name, parent_id)``, retry with a ``<cleaned> (id-<id>)``
        suffix so no stored variant survives. Used by the one-shot
        migration so its completion marker can honestly assert the "no
        stored variant" invariant; the runtime dedup path leaves this
        False and keeps the stored spelling in the collision case.
        """
        row = self.conn.execute(
            "SELECT name FROM keywords WHERE id = ?", (keyword_id,)
        ).fetchone()
        if row is None:
            return
        old_name = row["name"]
        cleaned = normalize_keyword_display(old_name)
        if not cleaned or cleaned == old_name:
            return
        # Collect (photo_id, workspace_id) for photos actually tagged with
        # this keyword row, scoped through workspace_folders. Captured
        # before the keywords UPDATE so a downstream _merge_keyword_into
        # still sees the same tags via photo_keywords.
        tag_rows = self.conn.execute(
            """SELECT DISTINCT pk.photo_id, wf.workspace_id
               FROM photo_keywords pk
               JOIN photos p ON p.id = pk.photo_id
               JOIN workspace_folders wf ON wf.folder_id = p.folder_id
               WHERE pk.keyword_id = ?""",
            (keyword_id,),
        ).fetchall()
        photo_workspace_pairs = [
            (r["photo_id"], r["workspace_id"]) for r in tag_rows
        ]
        affected_photo_ids = sorted({r["photo_id"] for r in tag_rows})
        # A same-name row in a different dedupe boundary (another type at
        # the same parent) can still occupy the table-level
        # UNIQUE(name, parent_id) slot. The links still merge correctly;
        # keep the stored spelling unchanged in that case, unless the
        # caller opts into disambiguation (migration path).
        try:
            self.conn.execute(
                "UPDATE keywords SET name = ? WHERE id = ?", (cleaned, keyword_id)
            )
        except sqlite3.IntegrityError:
            if not disambiguate_on_conflict:
                return
            # Fallback: append an id suffix so the row's name is still in
            # normalize_keyword_display() form (the parenthesized suffix is
            # ASCII and idempotent under the strip) while sidestepping the
            # UNIQUE(name, parent_id) slot the different-type peer holds.
            # The retarget below runs against this disambiguated name so
            # pending sidecar changes and species curation stay in lockstep
            # with the row's stored spelling.
            cleaned = f"{cleaned} (id-{keyword_id})"
            self.conn.execute(
                "UPDATE keywords SET name = ? WHERE id = ?", (cleaned, keyword_id)
            )
        # Retarget pending keyword_add/keyword_remove rows queued under the
        # pre-canonical spelling so a still-unsynced sidecar write can't
        # leak the legacy variant after the DB row was rewritten. A pending
        # row that would collide with an existing (photo_id, change_type,
        # cleaned) row is dropped rather than duplicated, matching
        # queue_change's dedupe contract.
        if affected_photo_ids:
            for chunk in _chunks(affected_photo_ids):
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
                    [old_name, *chunk, cleaned],
                )
                self.conn.execute(
                    f"""UPDATE pending_changes
                        SET value = ?
                        WHERE change_type IN ('keyword_add', 'keyword_remove')
                          AND value = ?
                          AND photo_id IN ({placeholders})""",
                    [cleaned, old_name, *chunk],
                )
        # Species curation tables key rows by the species name string, which
        # is compared exact against ``keywords.name``. Now that the UPDATE
        # above rewrote this row to the canonical spelling, rows still keyed
        # on the legacy spelling would drop out of the highlight/life-list
        # queries even though the tag was retained. Rename them for the same
        # old→clean mapping, scoped to the tagged (photo, workspace) pairs.
        # ``rename_photo_preferences_species`` also retargets
        # ``species_representatives`` in its scoped branch, so a separate
        # representatives rename isn't needed here.
        if photo_workspace_pairs:
            self.rename_species_highlights_species(
                old_name, cleaned,
                photo_workspace_pairs=photo_workspace_pairs, _commit=False,
            )
            self.rename_photo_preferences_species(
                old_name, cleaned,
                photo_workspace_pairs=photo_workspace_pairs, _commit=False,
            )

    def normalize_keyword_data(self):
        """One-shot, db_meta-gated wrapper around the normalization backfill.

        Runs at most once per database (``db_meta['keyword_names_normalized']``).
        All-or-nothing: an exception rolls the whole sweep back — including
        the marker — so a failed run retries on the next open instead of
        leaving a half-normalized keyword table.
        """
        if self.get_meta("keyword_names_normalized") != "1":
            try:
                self._normalize_keyword_data_once()
                self.set_meta("keyword_names_normalized", "1", _commit=False)
                self.conn.commit()
            except Exception:
                self.conn.rollback()
                raise
        # Second-generation curation case alignment. The v1 sweep above ran
        # on databases before curation writes canonicalized their species
        # input, so highlight/preference rows starred from prediction-cased
        # bucket labels between the v1 run and that fix are still keyed with
        # classifier casing (e.g. `Common Waxbill` vs the `Common waxbill`
        # keyword). Re-run just the case-alignment pass once under its own
        # marker; with the setters now canonicalizing on write, new
        # mismatches cannot form afterwards.
        if self.get_meta("curation_species_case_aligned_v2") != "1":
            try:
                aligned = self._align_curation_species_case()
                # Edit-history snapshots created after v1 but before the
                # setter canonicalization fix still carry prediction-cased
                # species in hl_prev/pref_prev/rep_prev; without this a
                # later undo would recreate the orphaned curation rows v2
                # is meant to repair.
                history_aligned = self._align_curation_history_species()
                if aligned or history_aligned:
                    log.info(
                        "curation case alignment v2: moved %d row(s), "
                        "rewrote %d history item(s)",
                        aligned, history_aligned,
                    )
                self.set_meta(
                    "curation_species_case_aligned_v2", "1", _commit=False
                )
                self.conn.commit()
            except Exception:
                self.conn.rollback()
                raise

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
        ``_collect_highlight_buckets`` uses to canonicalize prediction
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
        moved = 0
        unique_species_by_key, all_species_keys = self._species_keyword_maps()
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
                stored = self._canonical_curation_species(
                    old, unique_species_by_key, all_species_keys
                )
                if not stored or stored == old:
                    continue
                moved += rename(old, stored, _commit=False) or 0
                self.conn.execute(
                    f"DELETE FROM {table} WHERE species = ?", (old,)
                )
        return moved

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
        rewritten = 0
        unique_species_by_key, all_species_keys = self._species_keyword_maps()

        def _normalized_curation_species(value):
            return self._canonical_curation_species(
                value, unique_species_by_key, all_species_keys
            )

        history_rows = self.conn.execute(
            "SELECT id, old_value FROM edit_history_items "
            "WHERE old_value IS NOT NULL AND old_value LIKE ?",
            ('{%curation%',),
        ).fetchall()
        for row in history_rows:
            try:
                payload = json.loads(row["old_value"])
            except (TypeError, ValueError):
                continue
            if not isinstance(payload, dict):
                continue
            curation = payload.get("curation")
            if not isinstance(curation, dict):
                continue
            dirty = False
            highlights = curation.get("hl_prev")
            if isinstance(highlights, list):
                for index, entry in enumerate(highlights):
                    if isinstance(entry, str):
                        normalized = _normalized_curation_species(entry)
                        if normalized != entry:
                            highlights[index] = normalized
                            dirty = True
                    elif isinstance(entry, dict):
                        old = entry.get("species")
                        if isinstance(old, str):
                            normalized = _normalized_curation_species(old)
                            if normalized != old:
                                entry["species"] = normalized
                                dirty = True
            for key in ("pref_prev", "rep_prev"):
                entries = curation.get(key)
                if not isinstance(entries, list):
                    continue
                for entry in entries:
                    if not isinstance(entry, dict):
                        continue
                    old = entry.get("species")
                    if not isinstance(old, str):
                        continue
                    normalized = _normalized_curation_species(old)
                    if normalized != old:
                        entry["species"] = normalized
                        dirty = True
            if dirty:
                self.conn.execute(
                    "UPDATE edit_history_items SET old_value = ? WHERE id = ?",
                    (json.dumps(payload, sort_keys=True), row["id"]),
                )
                rewritten += 1
        return rewritten

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
        species_by_key = {}
        for row in self.conn.execute(
            "SELECT name FROM keywords "
            "WHERE parent_id IS NULL AND (is_species = 1 OR type = 'taxonomy')"
        ).fetchall():
            species_by_key.setdefault(keyword_match_key(row["name"]), []).append(
                row["name"]
            )
        unique = {
            key: names[0] for key, names in species_by_key.items()
            if len(set(names)) == 1
        }
        return unique, set(species_by_key.keys())

    def _canonical_curation_species(
        self, name, unique_species_by_key, all_species_keys,
    ):
        """Canonical spelling for a curation species value.

        Agrees with ``_collect_highlight_buckets`` / ``resolve_species_display_name``:

        - Unambiguous keyword match → use the stored spelling.
        - Ambiguous homonym → leave alone (returns the punctuation-
          normalized input unchanged).
        - No keyword row for the match_key → apply the same case
          convention ``_collect_highlight_buckets`` uses when it
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
        if self.get_meta(self._DUPLICATE_PHOTO_SPECIES_REPAIR_KEY) == "1":
            return 0
        if self.conn.execute(
            "SELECT 1 FROM taxa WHERE rank = 'species' LIMIT 1"
        ).fetchone() is None:
            # Taxonomy JSON can exist before the download job has populated
            # the local taxa table. Without species rows, differently-spelled
            # aliases cannot yet be grouped; leave the marker unset to retry.
            return 0
        removed_count = 0
        try:
            rows = self.conn.execute(
                """SELECT pk.photo_id, k.id AS keyword_id, k.name,
                          k.parent_id, k.taxon_id, t.rank AS taxon_rank
                   FROM photo_keywords pk
                   JOIN keywords k ON k.id = pk.keyword_id
                   LEFT JOIN taxa t ON t.id = k.taxon_id
                   WHERE (k.is_species = 1 OR k.type = 'taxonomy')
                     AND (t.rank = 'species' OR k.taxon_id IS NULL)
                   ORDER BY pk.photo_id,
                            CASE WHEN k.parent_id IS NULL THEN 1 ELSE 0 END,
                            k.id"""
            ).fetchall()
            by_photo = {}
            for row in rows:
                by_photo.setdefault(row["photo_id"], []).append(row)

            # A key with multiple linked taxa anywhere in the catalog is an
            # ambiguous homonym (e.g. legacy ``Robin`` alongside taxonomy
            # ``robin`` bound to different taxa). ``get_photos_with_equivalent_species``
            # gates its NULL-taxon fallback the same way; without the guard
            # here, a NULL-taxon leaf on a photo that also carries one linked
            # root gets folded into that root's group, the root is treated
            # as a redundant duplicate, and the accepted taxonomy species is
            # detached from the photo.
            homonym_keys = set()
            key_taxa = {}
            for row in self.conn.execute(
                """SELECT DISTINCT k.name, k.taxon_id
                   FROM keywords k
                   WHERE (k.is_species = 1 OR k.type = 'taxonomy')
                     AND k.taxon_id IS NOT NULL"""
            ).fetchall():
                key = keyword_match_key(row["name"])
                taxa = key_taxa.setdefault(key, set())
                taxa.add(row["taxon_id"])
                if len(taxa) > 1:
                    homonym_keys.add(key)

            grouped = {}
            for photo_id, photo_rows in by_photo.items():
                linked = {}
                unlinked = {}
                for row in photo_rows:
                    if row["taxon_id"] is not None:
                        linked.setdefault(row["taxon_id"], []).append(row)
                    else:
                        unlinked.setdefault(
                            keyword_match_key(row["name"]), []
                        ).append(row)
                # Fold a legacy NULL-taxon spelling into a unique linked
                # species group on the same photo. If multiple linked taxa
                # share that common name, or the same key is a known homonym
                # bound to different taxa elsewhere, leave it alone rather
                # than guessing.
                for name_key, null_rows in unlinked.items():
                    if name_key in homonym_keys:
                        grouped[(photo_id, "name", name_key)] = null_rows
                        continue
                    candidates = [
                        taxon_id for taxon_id, linked_rows in linked.items()
                        if any(
                            keyword_match_key(row["name"]) == name_key
                            for row in linked_rows
                        )
                    ]
                    if len(candidates) == 1:
                        linked[candidates[0]].extend(null_rows)
                    else:
                        grouped[(photo_id, "name", name_key)] = null_rows
                for taxon_id, linked_rows in linked.items():
                    grouped[(photo_id, "taxon", taxon_id)] = linked_rows

            for group_key, group in grouped.items():
                photo_id = group_key[0]
                if len(group) < 2:
                    continue
                nested = sorted(
                    (row for row in group if row["parent_id"] is not None),
                    key=lambda row: row["keyword_id"],
                )
                remove = [row for row in group if row["parent_id"] is None]
                if not nested or not remove:
                    continue
                if group_key[1] == "name":
                    # Unlinked (NULL-taxon) rows are grouped by
                    # ``keyword_match_key`` only. Curation/eligibility for
                    # unlinked species keys is compared with exact
                    # ``k.name`` — there is no taxon fallback that maps a
                    # differently-spelled leaf back to the root spelling.
                    # Detaching root ``Foo`` while only leaf ``foo``
                    # remains would strand highlights/representatives/
                    # life-list preferences saved under ``Foo``. Restrict
                    # removal to root rows whose exact spelling matches at
                    # least one surviving leaf so exact-name eligibility
                    # keeps applying; different-spelling unlinked
                    # duplicates stay attached until a taxon link makes
                    # canonicalization safe.
                    nested_names = {row["name"] for row in nested}
                    remove = [row for row in remove if row["name"] in nested_names]
                    if not remove:
                        continue
                # Preserve every hierarchy placement; detach only root rows.
                remove_ids = [row["keyword_id"] for row in remove]
                placeholders = ",".join("?" for _ in remove_ids)
                self.conn.execute(
                    f"""DELETE FROM photo_keywords
                        WHERE photo_id = ? AND keyword_id IN ({placeholders})""",
                    [photo_id, *remove_ids],
                )
                removed_count += len(remove_ids)

                # Drop this photo's undo/redo items that reference a root tag
                # the repair detached. The keyword_add, keyword_remove, and
                # prediction_accept handlers read the shared parent
                # edit_history.new_value rather than the per-photo value, so
                # merely retargeting edit_history_items would let redo attach
                # the redundant root again. Deleting only the affected item
                # preserves other photos in a batch; empty parent edits are
                # removed below. Scope by action/column so an unrelated rating
                # or prediction id with the same numeric value is untouched.
                # ``no_tag`` prediction_accept items (JSON old_value carrying
                # ``"no_tag": true``) already skip tag mutations on undo/redo
                # because the photo carried the species via an equivalent
                # row, so keeping them cannot reattach the detached root and
                # dropping them would erase the only audit/undo record of
                # the accepted prediction-status flip.
                for removed in remove:
                    removed_id = str(removed["keyword_id"])
                    self.conn.execute(
                        """DELETE FROM edit_history_items
                           WHERE photo_id = ?
                             AND edit_id IN (
                                 SELECT id FROM edit_history
                                 WHERE (
                                     action_type = 'keyword_add'
                                     AND edit_history_items.new_value = ?
                                 ) OR (
                                     action_type = 'prediction_accept'
                                     AND edit_history_items.new_value = ?
                                     AND (
                                         edit_history_items.old_value IS NULL
                                         OR edit_history_items.old_value
                                             NOT LIKE '%"no_tag"%'
                                     )
                                 ) OR (
                                     action_type = 'keyword_remove'
                                     AND edit_history_items.old_value = ?
                                 ) OR (
                                     action_type = 'species_replace'
                                     AND (
                                         edit_history_items.old_value = ?
                                         OR edit_history_items.new_value = ?
                                     )
                                 )
                             )""",
                        (photo_id, removed_id, removed_id, removed_id,
                         removed_id, removed_id),
                    )

                # species_replace items can store ``old_value`` as a JSON
                # payload carrying ``keyword_id``/``keyword_ids`` when the
                # replace swapped out multiple old species rows for one
                # photo. A bare-string equality misses those, so an undo/redo
                # would parse the JSON and re-tag the detached root, undoing
                # the repair. Scan JSON payloads on this photo and drop any
                # species_replace item whose keyword_id(s) contains the
                # detached root.
                removed_id_ints = {int(row["keyword_id"]) for row in remove}
                json_items = self.conn.execute(
                    """SELECT ehi.id, ehi.old_value
                       FROM edit_history_items ehi
                       JOIN edit_history eh ON eh.id = ehi.edit_id
                       WHERE ehi.photo_id = ?
                         AND eh.action_type = 'species_replace'
                         AND ehi.old_value IS NOT NULL
                         AND ehi.old_value LIKE '{%'""",
                    (photo_id,),
                ).fetchall()
                for item in json_items:
                    try:
                        payload = json.loads(item["old_value"])
                    except (TypeError, ValueError):
                        continue
                    if not isinstance(payload, dict):
                        continue
                    references_removed = False
                    raw_kid = payload.get("keyword_id")
                    if raw_kid is not None:
                        try:
                            if int(raw_kid) in removed_id_ints:
                                references_removed = True
                        except (TypeError, ValueError):
                            pass
                    if not references_removed:
                        for k in (payload.get("keyword_ids") or []):
                            try:
                                if int(k) in removed_id_ints:
                                    references_removed = True
                                    break
                            except (TypeError, ValueError):
                                continue
                    if references_removed:
                        self.conn.execute(
                            "DELETE FROM edit_history_items WHERE id = ?",
                            (item["id"],),
                        )

                self.conn.execute(
                    """DELETE FROM edit_history
                       WHERE id NOT IN (
                           SELECT DISTINCT edit_id FROM edit_history_items
                       )"""
                )

                # Only cancel pending changes for the root spellings actually
                # being detached. Using every name in ``group`` here would
                # also match preserved hierarchy leaves — e.g. a leaf
                # ``Desert Verdin`` that a user tagged shortly before the
                # repair runs. That pending ``keyword_add`` must still reach
                # the sidecar, otherwise ``sync_to_xmp`` writes only the
                # root cleanup and the preserved hierarchy never appears
                # in XMP.
                remove_keys = {
                    keyword_match_key(row["name"]) for row in remove
                }
                pending = self.conn.execute(
                    """SELECT id, change_type, value FROM pending_changes
                       WHERE photo_id = ?
                         AND change_type IN ('keyword_add', 'keyword_remove')""",
                    (photo_id,),
                ).fetchall()
                pending_ids = []
                cancelled_add_keys = set()
                for row in pending:
                    key = keyword_match_key(row["value"] or "")
                    if key not in remove_keys:
                        continue
                    pending_ids.append(row["id"])
                    if row["change_type"] == "keyword_add":
                        cancelled_add_keys.add(key)
                for chunk in _chunks(pending_ids):
                    pending_placeholders = ",".join("?" for _ in chunk)
                    self.conn.execute(
                        f"DELETE FROM pending_changes WHERE id IN ({pending_placeholders})",
                        chunk,
                    )

                # Split post-repair surviving names into two buckets:
                #
                # * attached-leaf keys — names of keywords still directly
                #   tagged on the photo. The scanner's flat dedup during
                #   a later XMP re-import already skips a matching
                #   ``dc:subject`` entry, so no sidecar remove is needed.
                # * ancestor-only keys — names that appear only in the
                #   parent chain of a surviving hierarchical leaf. The
                #   scanner does NOT count these when building its
                #   per-photo ``existing_keys`` (it uses attached leaf
                #   names only, see ``scanner._import_keywords_for_photo``),
                #   so a stale ``dc:subject: Verdin`` next to a preserved
                #   ``Verdin|Desert Verdin`` will be reimported and
                #   reattach the flat root — recreating the very
                #   duplicate this repair just removed. Queue a
                #   flat-only sidecar remove for these; a plain
                #   ``keyword_remove`` cannot be used because
                #   ``sync_to_xmp`` applies it hierarchically and would
                #   strip the preserved ``lr:hierarchicalSubject`` entry.
                attached_leaf_keys = set()
                ancestor_only_keys = set()
                for row in self.conn.execute(
                    """WITH RECURSIVE anc(id, name, parent_id, is_leaf) AS (
                           SELECT k.id, k.name, k.parent_id, 1
                             FROM photo_keywords pk
                             JOIN keywords k ON k.id = pk.keyword_id
                            WHERE pk.photo_id = ?
                           UNION
                           SELECT k.id, k.name, k.parent_id, 0
                             FROM keywords k
                             JOIN anc ON anc.parent_id = k.id
                       )
                       SELECT name, MAX(is_leaf) AS leaf
                         FROM anc GROUP BY id""",
                    (photo_id,),
                ).fetchall():
                    key = keyword_match_key(row["name"])
                    if not key:
                        continue
                    if row["leaf"]:
                        attached_leaf_keys.add(key)
                    else:
                        ancestor_only_keys.add(key)
                # The repair scans photo_keywords globally, but
                # pending_changes are filtered by workspace at read
                # time (get_pending_changes uses the active workspace).
                # A photo whose folder is not in the active workspace
                # would otherwise get its sidecar remove queued under
                # a workspace that will never sync it, leaving the
                # stale root spelling in XMP for the real workspace(s)
                # to re-import. Queue the remove for every workspace
                # that actually contains this photo; fall back to the
                # active workspace only when the photo has no
                # workspace membership at all.
                photo_workspaces = [
                    row["workspace_id"]
                    for row in self.conn.execute(
                        """SELECT DISTINCT wf.workspace_id
                           FROM photos p
                           JOIN workspace_folders wf
                             ON wf.folder_id = p.folder_id
                           WHERE p.id = ?""",
                        (photo_id,),
                    ).fetchall()
                ]
                for removed in remove:
                    key = keyword_match_key(removed["name"])
                    if not key or key in attached_leaf_keys:
                        continue
                    if key in cancelled_add_keys:
                        # The flat root add was still pending — cancelling
                        # it above already prevents the sidecar from ever
                        # receiving it, so no remove is required.
                        continue
                    # Ancestor-only survivors need flat-only cleanup:
                    # strip the stale ``dc:subject`` entry without the
                    # hierarchical sweep that would also drop the
                    # preserved ``lr:hierarchicalSubject`` line.
                    change_type = (
                        "keyword_remove_flat"
                        if key in ancestor_only_keys
                        else "keyword_remove"
                    )
                    if photo_workspaces:
                        for ws_id in photo_workspaces:
                            self.queue_change(
                                photo_id, change_type, removed["name"],
                                workspace_id=ws_id, _commit=False,
                            )
                    else:
                        self.queue_change(
                            photo_id, change_type, removed["name"],
                            _commit=False,
                        )

            self.set_meta(
                self._DUPLICATE_PHOTO_SPECIES_REPAIR_KEY, "1", _commit=False
            )
            self.conn.commit()
        except Exception:
            self.conn.rollback()
            raise
        if removed_count:
            log.info(
                "repaired %d redundant same-photo species keyword association(s)",
                removed_count,
            )
        return removed_count

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
            # _queue_keyword_add and _queue_keyword_remove enforce at
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

    def _merge_keyword_into(self, src_id, dst_id):
        """Merge keyword ``src_id`` into ``dst_id`` and delete the source.

        Moves photo associations, then reparents the source's children onto
        the destination. A child whose exact name already exists under the
        destination (UNIQUE(name, parent_id) clash) merges into that sibling
        recursively only when both share the same ``type`` — "Birds > Heron"
        and "birds > Heron" must converge on one Heron. When the existing
        sibling has a different ``type`` (e.g. a 'general' Macro vs. a
        'genre' Macro), the dedup boundary is (LOWER(name), parent_id, type),
        so they are NOT duplicates; preserve both by disambiguating the
        migrating child's name with an id suffix. Case-variant children
        don't clash (the UNIQUE index is case-sensitive); they reparent
        cleanly and collapse on the caller's next convergence pass. Cycles
        are impossible: parent_id chains are acyclic by construction.
        Non-link metadata (is_species, coordinates, taxon_id) folds into
        the destination when it lacks its own, so deleting the source can't
        silently drop species/location info that only the duplicate carried.

        Rewrites pending_changes so an unsynced keyword_add/keyword_remove
        queued under the source spelling points at the surviving name after
        the merge. Without this, the merge deletes the source row but leaves
        the pending change referring to the old spelling, so the next
        ``sync_to_xmp`` writes a keyword the DB no longer has.

        Returns the number of keyword rows merged away (>= 1). Caller
        commits.
        """
        merged = 1
        src = self.conn.execute(
            "SELECT name, type, is_species, latitude, longitude, taxon_id "
            "FROM keywords WHERE id = ?",
            (src_id,),
        ).fetchone()
        dst = self.conn.execute(
            "SELECT name, type, is_species FROM keywords WHERE id = ?",
            (dst_id,),
        ).fetchone()
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
                # Clear both alongside the metadata fold.
                self.conn.execute(
                    """UPDATE keywords
                       SET is_species = 0,
                           latitude   = COALESCE(latitude, ?),
                           longitude  = COALESCE(longitude, ?),
                           taxon_id   = NULL
                       WHERE id = ?""",
                    (src["latitude"], src["longitude"], dst_id),
                )
            else:
                self.conn.execute(
                    """UPDATE keywords
                       SET is_species = CASE WHEN ? = 1 THEN 1 ELSE is_species END,
                           latitude   = COALESCE(latitude, ?),
                           longitude  = COALESCE(longitude, ?),
                           taxon_id   = COALESCE(taxon_id, ?)
                       WHERE id = ?""",
                    (src["is_species"], src["latitude"], src["longitude"],
                     src["taxon_id"], dst_id),
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
                        (src_id, dst_id),
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
        #   * `prediction_accept`: shares the keyword_add branch in
        #     _apply_undo — the same untag_photo(pid, entry.new_value)
        #     runs. Trade-off: dropping the item also loses that item's
        #     prediction-status restoration on undo. Accepted because the
        #     alternative silently removes a legitimate user tag; the
        #     prediction row itself remains and the user can re-manage
        #     it. Only affects the narrow case of a legacy DB merging a
        #     src that was ever prediction-accepted onto a photo that
        #     already held the survivor.
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
            # untag the user's pre-existing survivor — the tradeoff
            # mirrors the prediction_accept case above.
            self.conn.execute(
                f"""DELETE FROM edit_history_items
                    WHERE new_value = ?
                      AND photo_id IN ({ph})
                      AND edit_id IN (
                          SELECT id FROM edit_history
                          WHERE new_value = ?
                            AND action_type IN (
                                'keyword_add', 'prediction_accept',
                                'species_replace'
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
                            AND ehi2.id > edit_history_items.id
                      )""",
                [src_str, *chunk, src_str, src_str, dst_str],
            )
            # When the source add happened first and a later add created
            # the current survivor association, the later add becomes the
            # redundant operation after src and dst converge. The guarded
            # DELETE above deliberately preserves the earlier source item;
            # drop the later add item instead so latest-first undo leaves
            # the merged tag in place until the original source add is
            # itself undone. Restrict this to add-like actions whose whole
            # per-photo effect is the tag association; species_replace has
            # an old-species restoration side that cannot be discarded.
            self.conn.execute(
                f"""DELETE FROM edit_history_items
                    WHERE photo_id IN ({ph})
                      AND new_value IN (?, ?)
                      AND edit_id IN (
                          SELECT id FROM edit_history
                          WHERE action_type IN (
                              'keyword_add', 'prediction_accept'
                          )
                      )
                      AND EXISTS (
                          SELECT 1
                          FROM edit_history_items ehi1
                          JOIN edit_history eh1
                            ON eh1.id = ehi1.edit_id
                          WHERE ehi1.photo_id = edit_history_items.photo_id
                            AND ehi1.new_value = ?
                            AND eh1.new_value = ?
                            AND eh1.action_type IN (
                                'keyword_add', 'prediction_accept'
                            )
                            AND ehi1.id < edit_history_items.id
                      )""",
                [*chunk, src_str, dst_str, src_str, src_str],
            )
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
        # Move photo associations (ignore if already exists for dst_id),
        # then drop the leftovers.
        self.conn.execute(
            "UPDATE OR IGNORE photo_keywords SET keyword_id = ? WHERE keyword_id = ?",
            (dst_id, src_id),
        )
        self.conn.execute("DELETE FROM photo_keywords WHERE keyword_id = ?", (src_id,))
        # Reparent children onto the destination before deleting, or the
        # keywords.parent_id FK aborts the merge mid-way.
        children = self.conn.execute(
            "SELECT id, name, type FROM keywords WHERE parent_id = ?", (src_id,)
        ).fetchall()
        for child in children:
            try:
                self.conn.execute(
                    "UPDATE keywords SET parent_id = ? WHERE id = ?",
                    (dst_id, child["id"]),
                )
            except sqlite3.IntegrityError:
                existing = self.conn.execute(
                    "SELECT id, type FROM keywords WHERE parent_id = ? AND name = ?",
                    (dst_id, child["name"]),
                ).fetchone()
                if existing["type"] == child["type"]:
                    merged += self._merge_keyword_into(child["id"], existing["id"])
                else:
                    # Same name + parent but different type: outside the
                    # (LOWER(name), parent_id, type) dedup boundary, so
                    # preserve both by renaming the migrating child rather
                    # than retagging photos across types.
                    disambiguated = f"{child['name']} (id-{child['id']})"
                    self.conn.execute(
                        "UPDATE keywords SET parent_id = ?, name = ? WHERE id = ?",
                        (dst_id, disambiguated, child["id"]),
                    )
        self.conn.execute("DELETE FROM keywords WHERE id = ?", (src_id,))
        return merged

    def get_keyword_tree(self):
        """Return keywords used by photos in the active workspace, plus ancestors."""
        return self.conn.execute(
            """WITH RECURSIVE
               leaf_kw AS (
                   SELECT DISTINCT pk.keyword_id AS id
                   FROM photo_keywords pk
                   JOIN photos p ON p.id = pk.photo_id
                   JOIN workspace_folders wf ON wf.folder_id = p.folder_id
                   WHERE wf.workspace_id = ?
               ),
               ancestors AS (
                   SELECT id FROM leaf_kw
                   UNION
                   SELECT k.parent_id
                   FROM keywords k
                   JOIN ancestors a ON a.id = k.id
                   WHERE k.parent_id IS NOT NULL
               )
               SELECT k.id, k.name, k.parent_id, k.type
               FROM keywords k
               JOIN ancestors a ON a.id = k.id
               ORDER BY k.name""",
            (self._ws_id(),),
        ).fetchall()

    def tag_photo(self, photo_id, keyword_id, _commit=True):
        """Associate a keyword with a photo.

        Args:
            _commit: If False, skip the internal commit (caller is responsible
                     for committing the transaction).
        """
        cur = self.conn.execute(
            "INSERT OR IGNORE INTO photo_keywords (photo_id, keyword_id) VALUES (?, ?)",
            (photo_id, keyword_id),
        )
        # Only fire auto-Wildlife when we actually inserted a new association.
        # A no-op INSERT OR IGNORE (re-tag of an already-tagged keyword) must
        # not retrigger the rule — otherwise removing Wildlife and re-tagging
        # the same species would silently re-add Wildlife and break sticky
        # removal.
        if cur.rowcount > 0:
            self._maybe_apply_auto_wildlife(photo_id, keyword_id)
        if _commit:
            self.conn.commit()

    def _maybe_apply_auto_wildlife(self, photo_id, just_added_keyword_id):
        """If just_added_keyword_id is a species keyword (taxonomy type OR
        legacy is_species=1) AND it's the only such keyword on this photo,
        also add the Wildlife genre.

        Treats ``is_species=1`` as a species candidate too: upgraded databases
        carry legacy species rows whose ``type`` hasn't been retyped to
        ``taxonomy`` yet by the background ``mark_species_keywords`` pass,
        and the auto-Wildlife trigger needs to fire for those tags during
        that window."""
        row = self.conn.execute(
            "SELECT type, is_species FROM keywords WHERE id = ?",
            (just_added_keyword_id,),
        ).fetchone()
        if not row or (row["type"] != "taxonomy" and not row["is_species"]):
            return
        # Count species keywords on this photo. If > 1, this isn't the first
        # — skip (sticky removal).
        species_count = self.conn.execute(
            """SELECT COUNT(*) AS n FROM photo_keywords pk
               JOIN keywords k ON k.id = pk.keyword_id
               WHERE pk.photo_id = ?
                 AND (k.type = 'taxonomy' OR k.is_species = 1)""",
            (photo_id,),
        ).fetchone()["n"]
        if species_count != 1:
            return
        wildlife_row = self.conn.execute(
            "SELECT id FROM keywords WHERE name = 'Wildlife' AND type = 'genre' LIMIT 1"
        ).fetchone()
        if not wildlife_row:
            return
        self.conn.execute(
            "INSERT OR IGNORE INTO photo_keywords (photo_id, keyword_id) VALUES (?, ?)",
            (photo_id, wildlife_row["id"]),
        )

    def get_meta(self, key):
        """Return the db_meta value for `key`, or None if unset."""
        row = self.conn.execute(
            "SELECT value FROM db_meta WHERE key = ?", (key,)
        ).fetchone()
        return row["value"] if row else None

    def set_meta(self, key, value, _commit=True):
        """Upsert a db_meta row."""
        self.conn.execute(
            "INSERT INTO db_meta (key, value) VALUES (?, ?) "
            "ON CONFLICT(key) DO UPDATE SET value = excluded.value",
            (key, str(value)),
        )
        if _commit:
            self.conn.commit()

    _WILDLIFE_BACKFILL_DONE_KEY = "wildlife_backfill_done"

    def backfill_wildlife_genre(self, force=False):
        """One-shot backfill: every photo that has at least one species
        keyword AND no Wildlife genre keyword gets Wildlife added.

        Gated by a db_meta marker so it runs at most once per database.
        Re-running unconditionally would clobber sticky-removed Wildlife rows
        (a user who intentionally removed Wildlife from a species-tagged
        photo would see it re-added on the next app restart).

        Matches keywords by ``type='taxonomy' OR is_species=1``. Plain-text
        species tags on upgraded DBs start as ``is_species=0`` /
        non-taxonomy and won't be matched until ``mark_species_keywords``
        retypes them — so callers must run that pass *before* this backfill
        on upgraded databases, otherwise the one-shot marker gets set on a
        zero-row scan and species photos are permanently missed.

        Args:
            force: re-run even if the marker is set. Used by tests; not for
                   normal startup.
        """
        if not force and self.get_meta(self._WILDLIFE_BACKFILL_DONE_KEY) == "1":
            return
        wildlife_row = self.conn.execute(
            "SELECT id FROM keywords WHERE name = 'Wildlife' AND type = 'genre' LIMIT 1"
        ).fetchone()
        if not wildlife_row:
            return  # No Wildlife keyword exists yet (very early init); nothing to do.
        wildlife_id = wildlife_row["id"]
        self.conn.execute(
            """INSERT OR IGNORE INTO photo_keywords (photo_id, keyword_id)
               SELECT DISTINCT pk.photo_id, ?
               FROM photo_keywords pk
               JOIN keywords k ON k.id = pk.keyword_id
               WHERE k.type = 'taxonomy' OR k.is_species = 1""",
            (wildlife_id,),
        )
        self.set_meta(self._WILDLIFE_BACKFILL_DONE_KEY, "1", _commit=False)
        self.conn.commit()

    def untag_photo(self, photo_id, keyword_id, _commit=True):
        """Remove a keyword association from a photo.

        Args:
            _commit: If False, skip the internal commit (caller is responsible
                     for committing the transaction).
        """
        self.conn.execute(
            "DELETE FROM photo_keywords WHERE photo_id = ? AND keyword_id = ?",
            (photo_id, keyword_id),
        )
        if _commit:
            self.conn.commit()

    def get_photo_keywords(self, photo_id):
        """Return all keywords for a photo."""
        return self.conn.execute(
            """SELECT k.id, k.name, k.parent_id, k.type
               FROM keywords k
               JOIN photo_keywords pk ON pk.keyword_id = k.id
               WHERE pk.photo_id = ?
               ORDER BY k.name""",
            (photo_id,),
        ).fetchall()

    def get_keywords_for_photos(self, photo_ids):
        """Return keywords for a batch of photos keyed by photo id."""
        if not photo_ids:
            return {}
        # Dedup-preserving-order: chunking that re-queries the same id
        # in a later chunk would double-append it under setdefault.
        photo_ids = list(dict.fromkeys(photo_ids))
        result = {}
        for chunk in _chunks(photo_ids):
            placeholders = ",".join("?" for _ in chunk)
            rows = self.conn.execute(
                f"""SELECT pk.photo_id, k.id, k.name, k.parent_id, k.type,
                           k.is_species, k.taxon_id, t.rank AS taxon_rank
                    FROM photo_keywords pk
                    JOIN keywords k ON k.id = pk.keyword_id
                    LEFT JOIN taxa t ON t.id = k.taxon_id
                    WHERE pk.photo_id IN ({placeholders})
                    ORDER BY k.type, k.name""",
                list(chunk),
            ).fetchall()
            for r in rows:
                result.setdefault(r["photo_id"], []).append(dict(r))
        return result

    def get_species_keywords_for_photos(self, photo_ids):
        """Return deduplicated species-rank keyword names for photos.

        Returns a dict mapping photo_id -> list of species name strings.

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
        if not photo_ids:
            return {}
        # Dedup-preserving-order: chunking that re-queries the same id
        # in a later chunk would double-append it under setdefault.
        photo_ids = list(dict.fromkeys(photo_ids))
        chosen = {}
        for chunk in _chunks(photo_ids):
            placeholders = ",".join("?" for _ in chunk)
            rows = self.conn.execute(
                f"""SELECT pk.photo_id, k.id, k.name, k.parent_id,
                           k.taxon_id, t.rank AS taxon_rank
                    FROM photo_keywords pk
                    JOIN keywords k ON k.id = pk.keyword_id
                    LEFT JOIN taxa t ON t.id = k.taxon_id
                    WHERE pk.photo_id IN ({placeholders})
                      AND (k.is_species = 1 OR k.type = 'taxonomy')
                      AND (t.rank = 'species' OR t.rank IS NULL)
                    ORDER BY pk.photo_id,
                             CASE WHEN k.parent_id IS NULL THEN 1 ELSE 0 END,
                             k.id""",
                list(chunk),
            ).fetchall()
            for r in rows:
                if r["taxon_id"] is not None:
                    identity = ("taxon", r["taxon_id"])
                else:
                    # Preserve exact spelling for NULL-taxon rows: unlinked
                    # curation compares by exact ``k.name``, so a root ``Foo``
                    # and a hierarchy leaf ``foo`` on the same photo must
                    # both appear here — folding by ``keyword_match_key``
                    # would drop the root and strand its representative /
                    # highlight lookups.
                    identity = ("name", r["name"])
                # Track whether the chosen row is itself a root: attached root
                # rows must keep their own stored spelling (curation is keyed
                # by the actually attached ``k.name``, so a same-taxon sibling
                # root row must not rewrite it). Only hierarchy leaves need the
                # root-name fallback.
                is_root = r["parent_id"] is None
                chosen.setdefault(r["photo_id"], {}).setdefault(
                    identity, (r["name"], is_root)
                )
        taxon_ids = {
            identity[1]
            for by_identity in chosen.values()
            for identity in by_identity
            if identity[0] == "taxon"
        }
        canonical_roots = {}
        if taxon_ids:
            for chunk in _chunks(list(taxon_ids)):
                placeholders = ",".join("?" for _ in chunk)
                root_rows = self.conn.execute(
                    f"""SELECT taxon_id, name FROM keywords
                        WHERE taxon_id IN ({placeholders})
                          AND parent_id IS NULL
                          AND (is_species = 1 OR type = 'taxonomy')
                        ORDER BY id""",
                    list(chunk),
                ).fetchall()
                for row in root_rows:
                    canonical_roots.setdefault(row["taxon_id"], row["name"])
        result = {}
        for photo_id, by_identity in chosen.items():
            names = []
            for identity, (name, is_root) in by_identity.items():
                if identity[0] == "taxon" and not is_root:
                    names.append(canonical_roots.get(identity[1], name))
                else:
                    names.append(name)
            result[photo_id] = sorted(names, key=lambda n: keyword_match_key(n))
        return result

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
        if not photo_ids:
            return set()
        target = self.conn.execute(
            "SELECT name, taxon_id FROM keywords WHERE id = ?", (keyword_id,)
        ).fetchone()
        if target is None:
            return set()
        result = set()
        ids = list(dict.fromkeys(int(pid) for pid in photo_ids))
        target_key = keyword_match_key(target["name"])
        excluded = tuple(dict.fromkeys(int(x) for x in (exclude_keyword_ids or ())))
        excl_placeholders = ",".join("?" for _ in excluded)
        excl_clause = f" AND k.id NOT IN ({excl_placeholders})" if excluded else ""
        # When another taxonomy/species keyword row shares the target's
        # match key but points at a different taxon (e.g. legacy
        # ``Robin`` alongside taxonomy ``robin``), the taxon_id-is-NULL
        # fallback below is ambiguous: an unlinked same-key row on the
        # photo could be either species. Treating it as the target would
        # let a confirm/accept skip ``tag_photo``/``queue_change`` and
        # leave the intended species keyword absent. Detect the homonym
        # conflict once and gate the fallback.
        #
        # The same guard applies when the *target* itself is unlinked:
        # if any other same-key row IS linked to a taxon, that linked
        # row is a distinct species that must not be folded into the
        # unlinked target during accept/confirm. In that case the
        # name-only fallback matches only the exact target row.
        homonym_conflict = False
        if target["taxon_id"] is not None:
            for row in self.conn.execute(
                """SELECT name FROM keywords
                   WHERE (is_species = 1 OR type = 'taxonomy')
                     AND taxon_id IS NOT NULL
                     AND taxon_id != ?""",
                (target["taxon_id"],),
            ).fetchall():
                if keyword_match_key(row["name"]) == target_key:
                    homonym_conflict = True
                    break
        else:
            for row in self.conn.execute(
                """SELECT name FROM keywords
                   WHERE (is_species = 1 OR type = 'taxonomy')
                     AND taxon_id IS NOT NULL
                     AND id != ?""",
                (keyword_id,),
            ).fetchall():
                if keyword_match_key(row["name"]) == target_key:
                    homonym_conflict = True
                    break
        for chunk in _chunks(ids):
            placeholders = ",".join("?" for _ in chunk)
            if target["taxon_id"] is not None:
                # Match rows linked to the same taxon first — the fast,
                # authoritative case that survives any display-name rename.
                rows = self.conn.execute(
                    f"""SELECT DISTINCT pk.photo_id
                        FROM photo_keywords pk
                        JOIN keywords k ON k.id = pk.keyword_id
                        WHERE pk.photo_id IN ({placeholders})
                          AND k.taxon_id = ?
                          AND (k.is_species = 1 OR k.type = 'taxonomy')
                          {excl_clause}""",
                    [*chunk, target["taxon_id"], *excluded],
                ).fetchall()
                result.update(row["photo_id"] for row in rows)
                if homonym_conflict:
                    continue
                # Fallback: upgraded libraries can carry a hierarchical
                # species leaf typed as taxonomy/is_species that
                # mark_species_keywords hasn't yet linked to a taxon_id
                # (see the explicit ``taxon_id IS NULL`` branch it
                # handles). A strict taxon_id equality above would miss
                # those legacy leaves, so a follow-up
                # confirm/accept-species after add_keyword created a
                # linked top-level root would not recognize the existing
                # hierarchy and would queue a duplicate root tag plus
                # sidecar add. Match unlinked rows by normalized display
                # name to preserve the hierarchy. Guarded above so an
                # ambiguous same-key homonym doesn't get folded in.
                rows = self.conn.execute(
                    f"""SELECT pk.photo_id, k.name
                        FROM photo_keywords pk
                        JOIN keywords k ON k.id = pk.keyword_id
                        WHERE pk.photo_id IN ({placeholders})
                          AND k.taxon_id IS NULL
                          AND (k.is_species = 1 OR k.type = 'taxonomy')
                          {excl_clause}""",
                    [*chunk, *excluded],
                ).fetchall()
                result.update(
                    row["photo_id"] for row in rows
                    if keyword_match_key(row["name"]) == target_key
                )
                continue
            # Target is unlinked. When a distinct linked row shares this
            # match key, any same-key row on the photo could be either
            # species; only the exact target keyword row is safe to
            # treat as equivalent.
            if homonym_conflict:
                rows = self.conn.execute(
                    f"""SELECT pk.photo_id
                        FROM photo_keywords pk
                        WHERE pk.photo_id IN ({placeholders})
                          AND pk.keyword_id = ?""",
                    [*chunk, keyword_id],
                ).fetchall()
                result.update(row["photo_id"] for row in rows)
                continue
            rows = self.conn.execute(
                f"""SELECT pk.photo_id, k.name
                    FROM photo_keywords pk
                    JOIN keywords k ON k.id = pk.keyword_id
                    WHERE pk.photo_id IN ({placeholders})
                      AND (k.is_species = 1 OR k.type = 'taxonomy')
                      {excl_clause}""",
                [*chunk, *excluded],
            ).fetchall()
            result.update(
                row["photo_id"] for row in rows
                if keyword_match_key(row["name"]) == target_key
            )
        return result

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
        ws = self._ws_id()
        if folder_id is None:
            folder_filter = ""
            folder_params = ()
        else:
            subtree = self.get_folder_subtree_ids(folder_id)
            placeholders = ",".join("?" for _ in subtree)
            folder_filter = f"AND p.folder_id IN ({placeholders})"
            folder_params = tuple(subtree)
        if photo_id is None:
            photo_filter = ""
            photo_params = ()
            bp_filter = ""
            bp_params = ()
            tp_filter = ""
            tp_params = ()
            kw_filter = ""
            kw_params = ()
        else:
            photo_filter = "AND p.id = ?"
            photo_params = (photo_id,)
            # Push the single-photo predicate into every derived subquery so
            # SQLite never materializes workspace-wide keyword/prediction
            # aggregations just to discard them at the outer join.
            bp_filter = "AND pk.photo_id = ?"
            bp_params = (photo_id,)
            tp_filter = "AND d.photo_id = ?"
            tp_params = (photo_id,)
            kw_filter = "WHERE pk.photo_id = ?"
            kw_params = (photo_id,)
        rows = self.conn.execute(
            f"""SELECT p.id, p.folder_id, p.filename, p.extension,
                      p.timestamp, p.width, p.height, p.rating, p.flag,
                      f.name AS folder_name, f.path AS folder_path,
                      p.thumb_path, p.quality_score, p.subject_sharpness,
                      p.subject_size, p.sharpness, p.phash_crop,
                      p.mask_path, p.subject_tenengrad, p.bg_tenengrad,
                      p.crop_complete, p.bg_separation,
                      p.subject_clip_high, p.subject_clip_low,
                      p.subject_y_median, p.noise_estimate,
                      p.eye_tenengrad,
                      p.dino_subject_embedding, p.dino_global_embedding,
                      bp.species,
                      tp.prediction_id,
                      tp.predicted_species,
                      tp.predicted_confidence,
                      kw.keyword_names
               FROM photos p
               JOIN workspace_folders wf ON wf.folder_id = p.folder_id
               JOIN folders f ON f.id = p.folder_id AND f.status IN ('ok', 'partial')
               LEFT JOIN (
                   SELECT photo_id, name AS species FROM (
                       SELECT pk.photo_id, k.name,
                              ROW_NUMBER() OVER (
                                  PARTITION BY pk.photo_id
                                  ORDER BY pk.rowid DESC
                              ) AS rn
                       FROM photo_keywords pk
                       JOIN keywords k ON k.id = pk.keyword_id
                       LEFT JOIN taxa t ON t.id = k.taxon_id
                       WHERE (k.is_species = 1 OR k.type = 'taxonomy')
                         AND (t.rank = 'species' OR t.rank IS NULL)
                         {bp_filter}
                   ) WHERE rn = 1
               ) bp ON bp.photo_id = p.id
               LEFT JOIN (
                   SELECT photo_id,
                          id AS prediction_id,
                          species AS predicted_species,
                          confidence AS predicted_confidence
                   FROM (
                       SELECT d.photo_id, pr.id, pr.species, pr.confidence,
                              ROW_NUMBER() OVER (
                                  PARTITION BY d.photo_id
                                  ORDER BY pr.confidence DESC, pr.id DESC
                              ) AS rn
                       FROM detections d
                       JOIN predictions pr ON pr.detection_id = d.id
                       LEFT JOIN prediction_review pr_rev
                         ON pr_rev.prediction_id = pr.id
                        AND pr_rev.workspace_id = ?
                       WHERE pr.species IS NOT NULL
                         AND COALESCE(pr_rev.status, 'pending') != 'rejected'
                         AND pr.labels_fingerprint = (
                             SELECT pr2.labels_fingerprint FROM predictions pr2
                             WHERE pr2.detection_id = pr.detection_id
                               AND pr2.classifier_model = pr.classifier_model
                             ORDER BY pr2.created_at DESC, pr2.id DESC
                             LIMIT 1
                         )
                         {tp_filter}
                   ) WHERE rn = 1
               ) tp ON tp.photo_id = p.id
               LEFT JOIN (
                   SELECT pk.photo_id,
                          group_concat(DISTINCT k.name) AS keyword_names
                   FROM photo_keywords pk
                   JOIN keywords k ON k.id = pk.keyword_id
                   {kw_filter}
                   GROUP BY pk.photo_id
               ) kw ON kw.photo_id = p.id
               WHERE wf.workspace_id = ?
                 {folder_filter}
                 {photo_filter}
                 AND (p.quality_score >= ?
                      OR (? <= 0 AND p.quality_score IS NULL))
                 AND (p.flag IS NULL OR p.flag != 'rejected')
               ORDER BY p.quality_score DESC""",
            (
                *bp_params,
                ws,
                *tp_params,
                *kw_params,
                ws,
                *folder_params,
                *photo_params,
                min_quality,
                min_quality,
            ),
        ).fetchall()
        return rows

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
        ws = self._ws_id()
        if species is not None:
            species_filter = """
                 AND (
                     k.name = ?
                     OR (
                         k.taxon_id IS NOT NULL
                         AND EXISTS (
                             SELECT 1 FROM keywords rk
                             WHERE rk.parent_id IS NULL
                               AND rk.taxon_id = k.taxon_id
                               AND (rk.is_species = 1 OR rk.type = 'taxonomy')
                               AND rk.name = ?
                         )
                     )
                 )"""
            params = (ws, species, species)
        else:
            species_filter = ""
            params = (ws,)
        return self.conn.execute(
            f"""SELECT p.id, p.folder_id, p.filename, p.timestamp,
                      p.rating, p.flag, p.quality_score,
                      p.subject_sharpness, p.subject_size, p.sharpness,
                      p.mask_path, p.subject_tenengrad, p.bg_tenengrad,
                      p.crop_complete, p.bg_separation,
                      p.subject_clip_high, p.subject_clip_low,
                      p.subject_y_median, p.noise_estimate,
                      p.eye_tenengrad,
                      k.name AS species,
                      t.id AS taxon_id,
                      t.rank AS taxon_rank,
                      t.name AS scientific_name,
                      t.common_name
               FROM photo_keywords pk
               JOIN keywords k ON k.id = pk.keyword_id
                AND (k.is_species = 1 OR k.type = 'taxonomy')
               JOIN photos p ON p.id = pk.photo_id
               JOIN workspace_folders wf ON wf.folder_id = p.folder_id
                AND wf.workspace_id = ?
               JOIN folders f ON f.id = p.folder_id
                AND f.status IN ('ok', 'partial')
               LEFT JOIN taxa t ON t.id = k.taxon_id
               WHERE COALESCE(p.flag, 'none') != 'rejected'
                 {species_filter}
               ORDER BY k.name, p.timestamp""",
            params,
        ).fetchall()

    def get_explorer_root(self, name="Aves", rank="class"):
        """Return {id,name,common_name,rank} for the default explorer root class,
        or None when the reference taxonomy has not been downloaded."""
        row = self.conn.execute(
            "SELECT id, name, common_name, rank FROM taxa"
            " WHERE name = ? AND rank = ? LIMIT 1",
            (name, rank),
        ).fetchone()
        return dict(row) if row else None

    def get_life_list_taxon_ids(self):
        """Distinct species-rank taxa ids of workspace-scoped tagged species (same
        eligibility as get_life_list_candidates). Excludes species keywords with no
        taxon_id AND taxonomy tags that resolve to a taxon above species rank
        (genus, family, etc.). Higher-rank matches are surfaced via
        get_life_list_unmatched_species so the explorer's found/total math stays
        at species rank and non-species tags aren't silently undercounted."""
        ws = self._ws_id()
        rows = self.conn.execute(
            """SELECT DISTINCT k.taxon_id AS tid
               FROM photo_keywords pk
               JOIN keywords k ON k.id = pk.keyword_id
                AND (k.is_species = 1 OR k.type = 'taxonomy')
               JOIN taxa t ON t.id = k.taxon_id AND t.rank = 'species'
               JOIN photos p ON p.id = pk.photo_id
               JOIN workspace_folders wf ON wf.folder_id = p.folder_id
                AND wf.workspace_id = ?
               JOIN folders f ON f.id = p.folder_id
                AND f.status IN ('ok', 'partial')
               WHERE COALESCE(p.flag, 'none') != 'rejected'""",
            (ws,),
        ).fetchall()
        return {r["tid"] for r in rows}

    def get_life_list_unmatched_species(self):
        """Names of workspace-scoped tagged species keywords that can't be counted
        at species rank — either no linked taxon at all, or a link that lands on
        a taxon above species (genus/family/etc.). Surfaced honestly in the
        explorer as 'not counted' so higher-rank matches aren't silently dropped."""
        ws = self._ws_id()
        rows = self.conn.execute(
            """SELECT DISTINCT k.name AS name
               FROM photo_keywords pk
               JOIN keywords k ON k.id = pk.keyword_id
                AND (k.is_species = 1 OR k.type = 'taxonomy')
               LEFT JOIN taxa t ON t.id = k.taxon_id
               JOIN photos p ON p.id = pk.photo_id
               JOIN workspace_folders wf ON wf.folder_id = p.folder_id
                AND wf.workspace_id = ?
               JOIN folders f ON f.id = p.folder_id
                AND f.status IN ('ok', 'partial')
               WHERE COALESCE(p.flag, 'none') != 'rejected'
                 AND (k.taxon_id IS NULL OR t.rank IS NULL OR t.rank != 'species')
               ORDER BY k.name""",
            (ws,),
        ).fetchall()
        return [r["name"] for r in rows]

    def get_taxon_subtree(self, root_id, max_depth=12):
        """All taxa in the subtree rooted at root_id (inclusive), as dict rows
        with id, name, common_name, rank, parent_id. Uses the parent_id index."""
        # The depth cap is safe because the taxonomy loader keeps only major
        # ranks (see vireo/taxonomy.py MAJOR_RANK_LEVELS), giving a real
        # class->species depth of ~4. Raise the cap if intermediate ranks are
        # ever kept.
        return [dict(r) for r in self.conn.execute(
            """WITH RECURSIVE subtree(id, name, common_name, rank, parent_id, depth) AS (
                   SELECT id, name, common_name, rank, parent_id, 0
                   FROM taxa WHERE id = ?
                   UNION ALL
                   SELECT t.id, t.name, t.common_name, t.rank, t.parent_id, s.depth + 1
                   FROM taxa t JOIN subtree s ON t.parent_id = s.id
                   WHERE s.depth < ?
               )
               SELECT id, name, common_name, rank, parent_id FROM subtree""",
            (root_id, max_depth),
        ).fetchall()]

    def get_classes_for_taxa(self, taxon_ids):
        """Distinct class-rank ancestors of the given taxa, for the explorer's
        class selector. Returns [{id,name,common_name}] ordered by name.

        Callers pass the full life-list `found` set, which can exceed SQLite's
        bound-parameter limit on large life lists — chunk the seed IDs and
        merge the distinct classes across chunks so the endpoint doesn't 500.
        """
        ids = [t for t in taxon_ids if t is not None]
        if not ids:
            return []
        seen = {}
        for chunk in _chunks(ids):
            placeholders = ",".join("?" for _ in chunk)
            rows = self.conn.execute(
                f"""WITH RECURSIVE up(id) AS (
                        SELECT id FROM taxa WHERE id IN ({placeholders})
                        UNION
                        SELECT t.parent_id FROM taxa t JOIN up u ON t.id = u.id
                        WHERE t.parent_id IS NOT NULL
                    )
                    SELECT DISTINCT t.id, t.name, t.common_name
                    FROM up u JOIN taxa t ON t.id = u.id
                    WHERE t.rank = 'class'""",
                chunk,
            ).fetchall()
            for r in rows:
                if r["id"] not in seen:
                    seen[r["id"]] = dict(r)
        return sorted(seen.values(),
                      key=lambda r: r["common_name"] or r["name"])

    def get_class_ancestors_for_taxa(self, taxon_ids):
        """Map each taxon id to its class-rank ancestor.

        Life List entries can be linked at any major rank, so preserve the
        starting taxon id while walking toward the root.  The depth cap mirrors
        :meth:`get_taxon_subtree` and also prevents malformed cyclic taxonomy
        data from making the recursive query run forever.
        """
        ids = [taxon_id for taxon_id in taxon_ids if taxon_id is not None]
        if not ids:
            return {}
        classes = {}
        for chunk in _chunks(ids):
            placeholders = ",".join("?" for _ in chunk)
            rows = self.conn.execute(
                f"""WITH RECURSIVE up(
                           origin_id, id, parent_id, rank, name, common_name, depth
                       ) AS (
                           SELECT id, id, parent_id, rank, name, common_name, 0
                           FROM taxa WHERE id IN ({placeholders})
                           UNION ALL
                           SELECT u.origin_id, t.id, t.parent_id, t.rank,
                                  t.name, t.common_name, u.depth + 1
                           FROM up u JOIN taxa t ON t.id = u.parent_id
                           WHERE u.depth < 12
                       )
                       SELECT origin_id, id, name, common_name
                       FROM up WHERE rank = 'class'""",
                chunk,
            ).fetchall()
            for row in rows:
                classes.setdefault(row["origin_id"], {
                    "id": row["id"],
                    "name": row["name"],
                    "common_name": row["common_name"],
                })
        return classes

    def get_life_list_best_photo_by_taxon(self, taxon_ids):
        """Map taxon_id -> {id, filename} of a representative (highest quality_score,
        newest) workspace-scoped photo for that species. Missing taxa are absent."""
        ids = [t for t in taxon_ids if t is not None]
        if not ids:
            return {}
        ws = self._ws_id()
        placeholders = ",".join("?" for _ in ids)
        rows = self.conn.execute(
            f"""SELECT k.taxon_id AS tid, p.id, p.filename, p.quality_score, p.timestamp
                FROM photo_keywords pk
                JOIN keywords k ON k.id = pk.keyword_id AND k.taxon_id IN ({placeholders})
                JOIN photos p ON p.id = pk.photo_id
                JOIN workspace_folders wf ON wf.folder_id = p.folder_id
                 AND wf.workspace_id = ?
                JOIN folders f ON f.id = p.folder_id AND f.status IN ('ok','partial')
                WHERE COALESCE(p.flag, 'none') != 'rejected'
                ORDER BY k.taxon_id,
                         COALESCE(p.quality_score, -1) DESC,
                         COALESCE(p.timestamp, '') DESC""",
            ids + [ws],
        ).fetchall()
        best = {}
        for r in rows:
            if r["tid"] not in best:  # first row per taxon is the best by ORDER BY
                best[r["tid"]] = {"id": r["id"], "filename": r["filename"]}
        return best

    def get_taxon_by_id(self, taxon_id):
        """Thin getter for a single taxon row (for non-default explorer roots)."""
        row = self.conn.execute(
            "SELECT id, name, common_name, rank, parent_id FROM taxa WHERE id = ?",
            (taxon_id,),
        ).fetchone()
        return dict(row) if row else None

    def get_photo_life_list_species(self, photo_id):
        """Return this photo's lifelist-eligible species names in the active
        workspace, ordered by name.

        Same eligibility rule as :meth:`get_life_list_candidates`: an accepted
        species keyword (``is_species = 1`` or ``type = 'taxonomy'``) on a
        non-rejected photo in a workspace-visible folder. Returns ``[]`` when
        the photo carries no such species (or is rejected / outside the
        workspace), which is exactly when no "Add to Life List" affordance
        should appear.

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
        ws = self._ws_id()
        rows = self.conn.execute(
            """SELECT k.name, k.parent_id, k.taxon_id
               FROM photo_keywords pk
               JOIN keywords k ON k.id = pk.keyword_id
                AND (k.is_species = 1 OR k.type = 'taxonomy')
               LEFT JOIN taxa t ON t.id = k.taxon_id
               JOIN photos p ON p.id = pk.photo_id
                AND COALESCE(p.flag, 'none') != 'rejected'
               JOIN workspace_folders wf ON wf.folder_id = p.folder_id
                AND wf.workspace_id = ?
               JOIN folders f ON f.id = p.folder_id
                AND f.status IN ('ok', 'partial')
               WHERE pk.photo_id = ?
                 AND (t.rank = 'species' OR t.rank IS NULL)
               ORDER BY CASE WHEN k.parent_id IS NULL THEN 1 ELSE 0 END,
                        k.id""",
            (ws, photo_id),
        ).fetchall()
        if not rows:
            return []
        taxon_ids = {r["taxon_id"] for r in rows if r["taxon_id"] is not None}
        canonical_roots = {}
        if taxon_ids:
            for chunk in _chunks(list(taxon_ids)):
                placeholders = ",".join("?" for _ in chunk)
                root_rows = self.conn.execute(
                    f"""SELECT taxon_id, name FROM keywords
                        WHERE taxon_id IN ({placeholders})
                          AND parent_id IS NULL
                          AND (is_species = 1 OR type = 'taxonomy')
                        ORDER BY id""",
                    list(chunk),
                ).fetchall()
                for row in root_rows:
                    canonical_roots.setdefault(row["taxon_id"], row["name"])
        chosen = {}
        for r in rows:
            if r["taxon_id"] is not None:
                identity = ("taxon", r["taxon_id"])
                is_root = r["parent_id"] is None
                if is_root:
                    name = r["name"]
                else:
                    name = canonical_roots.get(r["taxon_id"], r["name"])
            else:
                identity = ("name", r["name"])
                name = r["name"]
            chosen.setdefault(identity, name)
        return sorted(chosen.values(), key=lambda n: keyword_match_key(n))

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
        """
        ws = self._ws_id()
        species_filter = ""
        params = []
        if species:
            species_filter = """
                 AND (
                     k.name = ?
                     OR (
                         k.taxon_id IS NOT NULL
                         AND EXISTS (
                             SELECT 1 FROM keywords rk
                             WHERE rk.parent_id IS NULL
                               AND rk.taxon_id = k.taxon_id
                               AND (rk.is_species = 1 OR rk.type = 'taxonomy')
                               AND rk.name = ?
                         )
                     )
                 )"""
            params.extend([species, species])
        params.append(ws)
        rows = self.conn.execute(
            f"""SELECT DISTINCT k.name AS species, lk.name AS location
               FROM photo_keywords pk
               JOIN keywords k ON k.id = pk.keyword_id
                AND (k.is_species = 1 OR k.type = 'taxonomy')
                {species_filter}
               JOIN photos p ON p.id = pk.photo_id
                AND COALESCE(p.flag, 'none') != 'rejected'
               JOIN workspace_folders wf ON wf.folder_id = p.folder_id
                AND wf.workspace_id = ?
               JOIN folders f ON f.id = p.folder_id
                AND f.status IN ('ok', 'partial')
               JOIN photo_keywords plk ON plk.photo_id = p.id
               JOIN keywords lk ON lk.id = plk.keyword_id
                AND lk.type = 'location'
               ORDER BY k.name, lk.name""",
            tuple(params),
        ).fetchall()
        result = {}
        for r in rows:
            result.setdefault(r["species"], []).append(r["location"])
        return result

    def get_photo_preferences(self, purpose):
        """Return {species: photo_id} preferences for the active workspace."""
        ws = self._ws_id()
        rows = self.conn.execute(
            """SELECT species, photo_id
               FROM photo_preferences
               WHERE workspace_id = ? AND purpose = ?""",
            (ws, purpose),
        ).fetchall()
        return {r["species"]: r["photo_id"] for r in rows}

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
        if species is not None:
            species = self.resolve_species_display_name(species)
        ws = self._ws_id()
        eligibility_filter = ""
        if eligible_only:
            # Accept a hierarchy leaf whose taxon links back to a root
            # species with the curation-keyed name. After
            # repair_duplicate_photo_species detaches a redundant root but
            # leaves the hierarchical leaf attached, the leaf's stored
            # spelling may differ from the root ("verdin" vs "Verdin"), yet
            # the photo still represents the same species via a shared
            # taxon_id. An exact k.name = sr.species compare would then
            # silently drop that photo from Life List / Representative
            # eligibility even though curation was intentionally preserved
            # on the root key.
            # Restrict the eligibility EXISTS to species-rank taxonomy rows.
            # `mark_species_keywords` stamps `is_species=1`/`type='taxonomy'`
            # regardless of the linked taxon's rank, so a genus/family
            # keyword whose display name happens to match a species curation
            # key (e.g. a genus row named `Puma`) would otherwise satisfy
            # eligibility for the `Puma` species representative even though
            # sibling queries (get_life_list_candidates, get_species_keywords_for_photos,
            # etc.) exclude the photo from the species bucket via the same
            # `(t.rank = 'species' OR t.rank IS NULL)` guard.
            eligibility_filter = """
                 AND COALESCE(p.flag, 'none') != 'rejected'
                 AND f.status IN ('ok', 'partial')
                 AND EXISTS (
                     SELECT 1
                     FROM photo_keywords pk
                     JOIN keywords k ON k.id = pk.keyword_id
                      AND (k.is_species = 1 OR k.type = 'taxonomy')
                     LEFT JOIN taxa t ON t.id = k.taxon_id
                     WHERE pk.photo_id = sr.photo_id
                       AND (t.rank = 'species' OR t.rank IS NULL)
                       AND (
                           k.name = sr.species
                           OR (
                               k.taxon_id IS NOT NULL
                               AND EXISTS (
                                   SELECT 1 FROM keywords root
                                   WHERE root.parent_id IS NULL
                                     AND (root.is_species = 1
                                          OR root.type = 'taxonomy')
                                     AND root.taxon_id = k.taxon_id
                                     AND root.name = sr.species
                               )
                           )
                       )
                 )"""
        species_filter = ""
        params = [ws]
        if species:
            species_filter = " AND sr.species = ?"
            params.append(species)
        rows = self.conn.execute(
            f"""SELECT sr.species, sr.photo_id
               FROM species_representatives sr
               JOIN photos p ON p.id = sr.photo_id
               JOIN workspace_folders wf ON wf.folder_id = p.folder_id
                AND wf.workspace_id = ?
               JOIN folders f ON f.id = p.folder_id
                 {eligibility_filter}
                 {species_filter}
               ORDER BY sr.species, sr.selected_order DESC, sr.id DESC""",
            tuple(params),
        ).fetchall()
        result = {}
        for row in rows:
            ids = result.setdefault(row["species"], [])
            if row["photo_id"] not in ids:
                ids.append(row["photo_id"])
        return result

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
        order = self._next_species_representative_order()
        self.conn.execute(
            """INSERT INTO species_representatives
                   (species, photo_id, selected_order, created_at, updated_at)
               VALUES (?, ?, ?, datetime('now'), datetime('now'))
               ON CONFLICT(species, photo_id) DO UPDATE SET
                   selected_order = excluded.selected_order,
                   updated_at = excluded.updated_at""",
            (species, photo_id, order),
        )

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
        if selected_order is None:
            self._set_global_species_representative(species, photo_id)
            return
        try:
            order = int(selected_order)
        except (TypeError, ValueError):
            self._set_global_species_representative(species, photo_id)
            return
        self.conn.execute(
            """INSERT INTO species_representatives
                   (species, photo_id, selected_order, created_at, updated_at)
               VALUES (?, ?, ?, datetime('now'), datetime('now'))
               ON CONFLICT(species, photo_id) DO UPDATE SET
                   selected_order = excluded.selected_order,
                   updated_at = excluded.updated_at""",
            (species, photo_id, order),
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
        species = self.resolve_species_display_name(species)
        ws = self._ws_id()
        self.conn.execute(
            """INSERT INTO photo_preferences
                   (workspace_id, purpose, species, photo_id, created_at, updated_at)
               VALUES (?, ?, ?, ?, datetime('now'), datetime('now'))
               ON CONFLICT(workspace_id, purpose, species) DO UPDATE SET
                   photo_id = excluded.photo_id,
                   updated_at = excluded.updated_at""",
            (ws, purpose, species, photo_id),
        )
        if purpose in {"species_representative", "life_list", "highlights"}:
            self._set_global_species_representative(species, photo_id)
        if _commit:
            self.conn.commit()

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
        species = self.resolve_species_display_name(species)
        ws = self._ws_id()
        self.conn.execute(
            """DELETE FROM photo_preferences
               WHERE workspace_id = ? AND purpose = ? AND species = ?""",
            (ws, purpose, species),
        )
        if _commit:
            self.conn.commit()

    def clear_species_representative(self, species, _commit=True):
        """Clear all representative photos for a species globally."""
        species = self.resolve_species_display_name(species)
        ws = self._ws_id()
        self.conn.execute(
            "DELETE FROM species_representatives WHERE species = ?",
            (species,),
        )
        self.conn.execute(
            """DELETE FROM photo_preferences
               WHERE workspace_id = ?
                 AND species = ?
                 AND purpose IN ('species_representative', 'life_list', 'highlights')""",
            (ws, species),
        )
        if _commit:
            self.conn.commit()

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
        if species is not None:
            species = self.resolve_species_display_name(species)
        ws = self._ws_id()
        eligibility_joins = ""
        eligibility_filter = ""
        if eligible_only:
            eligibility_joins = """
                   JOIN photos p ON p.id = sh.photo_id
                   JOIN workspace_folders wf ON wf.folder_id = p.folder_id
                    AND wf.workspace_id = sh.workspace_id
                   JOIN folders f ON f.id = p.folder_id
                    AND f.status IN ('ok', 'partial')"""
            # Two-branch species eligibility. Accepted keyword compares
            # exact: same-NOCASE homonyms preserved by the migration
            # (legacy general ``Robin`` alongside taxonomy ``robin``) are
            # genuinely different species — a stored highlight for
            # ``Robin`` must not become eligible for a photo whose
            # accepted keyword resolves to ``robin``. The taxon-linked
            # fallback below (mirroring
            # :meth:`get_species_representative_lists`) preserves that
            # boundary: it only matches when a photo keyword's
            # ``taxon_id`` links back to a root species keyword whose
            # name equals ``sh.species``, so a photo carrying only
            # ``robin`` still cannot satisfy a ``Robin`` highlight
            # unless the two keywords share a taxon. It rescues the
            # case where ``repair_duplicate_photo_species`` detached
            # the redundant root and the surviving hierarchical leaf
            # has a different stored spelling from the canonical root
            # (``verdin`` vs ``Verdin``), so a preserved highlight
            # under ``Verdin`` still applies to that photo. Prediction
            # fallback compares NOCASE: the classifier emits an
            # external vocabulary spelling (e.g. ``Common Waxbill``)
            # while sh.species stores the canonical keyword spelling
            # (``Common waxbill``), so an exact compare would drop
            # every highlight starred from an unconfirmed bucket.
            # Applying NOCASE only to the fallback keeps the
            # ambiguous-homonym boundary intact.
            # The prediction subquery also routes ``pr.species`` through
            # the same hierarchy-alias → root canonicalization
            # ``resolve_species_display_name`` applies, so a highlight
            # saved from a canonicalized prediction bucket (leaf
            # ``Desert Verdin`` bucketed under root ``Verdin`` when the
            # linked taxon has a top-level row) still matches on
            # reload. ``COALESCE`` falls back to the raw prediction
            # spelling whenever the alias has no linked species-rank
            # root, mirroring ``add_species_highlight`` /
            # ``_collect_highlight_buckets``.
            # The accepted-keyword branch mirrors
            # :meth:`get_species_representative_lists`: restrict to
            # species-rank taxonomy rows so a genus/family keyword named
            # identically to a species curation key (e.g. a genus `Puma`
            # row named `Puma`) does not satisfy a species highlight —
            # sibling species queries already exclude that photo from the
            # species bucket via `(t.rank = 'species' OR t.rank IS NULL)`.
            # The prediction-fallback branch below applies the same
            # `(t.rank = 'species' OR t.rank IS NULL)` filter to the
            # NOT EXISTS condition: a photo carrying only a higher-rank
            # taxonomy keyword (e.g. genus/family) is no longer part of
            # any species accepted bucket, and get_highlights_candidates
            # / _collect_highlight_buckets will place it in a prediction
            # bucket (bp.species is NULL). Without the rank filter here,
            # the saved highlight would vanish on reload because the
            # NOT EXISTS still sees the higher-rank taxonomy row.
            eligibility_filter = """
                 AND COALESCE(p.flag, 'none') != 'rejected'
                 AND (
                     EXISTS (
                         SELECT 1
                         FROM photo_keywords pk
                         JOIN keywords k ON k.id = pk.keyword_id
                          AND (k.is_species = 1 OR k.type = 'taxonomy')
                         LEFT JOIN taxa t ON t.id = k.taxon_id
                         WHERE pk.photo_id = sh.photo_id
                           AND (t.rank = 'species' OR t.rank IS NULL)
                           AND (
                               k.name = sh.species
                               OR (
                                   k.taxon_id IS NOT NULL
                                   AND EXISTS (
                                       SELECT 1 FROM keywords root
                                       WHERE root.parent_id IS NULL
                                         AND (root.is_species = 1
                                              OR root.type = 'taxonomy')
                                         AND root.taxon_id = k.taxon_id
                                         AND root.name = sh.species
                                   )
                               )
                           )
                     )
                     OR (
                         NOT EXISTS (
                             SELECT 1
                             FROM photo_keywords pk
                             JOIN keywords k ON k.id = pk.keyword_id
                              AND (k.is_species = 1 OR k.type = 'taxonomy')
                             LEFT JOIN taxa t ON t.id = k.taxon_id
                             WHERE pk.photo_id = sh.photo_id
                               AND (t.rank = 'species' OR t.rank IS NULL)
                         )
                         AND sh.species = (
                             SELECT COALESCE(
                                 (
                                     -- Root-first, exact spelling:
                                     -- mirror ``resolve_species_display_name``'s
                                     -- case 2 preference for an
                                     -- exact-spelling same-name top-level
                                     -- species row. Keeps intentional
                                     -- ASCII/typography homonyms (e.g.
                                     -- general ``Robin`` +
                                     -- taxonomy ``robin``) routed to
                                     -- their own stored spelling instead
                                     -- of collapsing them onto one.
                                     -- (SQLite doesn't support
                                     -- correlated column references in a
                                     -- subquery ORDER BY, so the exact
                                     -- preference is expressed as its
                                     -- own subquery rather than an
                                     -- ORDER BY key on a NOCASE match.)
                                     SELECT root_exact.name
                                     FROM keywords root_exact
                                     WHERE root_exact.name = pr.species
                                       AND root_exact.parent_id IS NULL
                                       AND (
                                           root_exact.is_species = 1
                                           OR root_exact.type = 'taxonomy'
                                       )
                                     ORDER BY (root_exact.type = 'taxonomy') DESC,
                                              root_exact.id ASC
                                     LIMIT 1
                                 ),
                                 (
                                     -- Root-first, NOCASE fallback:
                                     -- mirror
                                     -- ``resolve_species_display_name``'s
                                     -- case 1 — when a single same-name
                                     -- top-level species row exists,
                                     -- ``add_species_highlight`` stored
                                     -- the highlight under its stored
                                     -- spelling regardless of the
                                     -- caller's casing. Without this
                                     -- branch, the hierarchy-alias
                                     -- canonicalization below could pick
                                     -- a different-taxon leaf sharing
                                     -- the label (e.g. root ``Robin``
                                     -- for one taxon plus a hierarchy
                                     -- leaf ``Robin`` under a different
                                     -- taxon), canonicalize to that
                                     -- leaf's root spelling, and drop
                                     -- the saved highlight on reload.
                                     SELECT root_direct.name
                                     FROM keywords root_direct
                                     WHERE root_direct.name = pr.species COLLATE NOCASE
                                       AND root_direct.parent_id IS NULL
                                       AND (
                                           root_direct.is_species = 1
                                           OR root_direct.type = 'taxonomy'
                                       )
                                     ORDER BY (root_direct.type = 'taxonomy') DESC,
                                              root_direct.id ASC
                                     LIMIT 1
                                 ),
                                 (
                                     -- Hierarchy-alias fallback:
                                     -- canonicalize only when the raw
                                     -- prediction label resolves to a
                                     -- unique linked taxon. When multiple
                                     -- hierarchy leaves share the label
                                     -- but point at different taxa, keep
                                     -- the raw ``pr.species`` so buckets
                                     -- key on the ambiguous label instead
                                     -- of an arbitrary root.
                                     -- Ambiguity count and the target-leaf
                                     -- restriction here must mirror
                                     -- ``resolve_species_display_name`` — the
                                     -- canonicalizer ``add_species_highlight``
                                     -- and ``_collect_highlight_buckets`` use
                                     -- when writing ``sh.species``. That
                                     -- helper considers all linked hierarchy
                                     -- taxa regardless of rank. If this
                                     -- subquery restricted the count to
                                     -- species-rank taxa, a mixed
                                     -- species+higher-rank alias (e.g.
                                     -- species ``Puma`` and genus ``Puma``)
                                     -- would count as 1 here and canonicalize
                                     -- to the species root while
                                     -- ``add_species_highlight`` kept the raw
                                     -- ``pr.species`` — the eligibility
                                     -- compare below would then drop the
                                     -- highlight on reload.
                                     SELECT root.name
                                     FROM keywords k_pred
                                     JOIN keywords root
                                       ON root.parent_id IS NULL
                                      AND root.taxon_id = k_pred.taxon_id
                                      AND (
                                          root.is_species = 1
                                          OR root.type = 'taxonomy'
                                      )
                                     WHERE k_pred.name = pr.species COLLATE NOCASE
                                       AND k_pred.parent_id IS NOT NULL
                                       AND k_pred.taxon_id IS NOT NULL
                                       AND (
                                           k_pred.is_species = 1
                                           OR k_pred.type = 'taxonomy'
                                       )
                                       AND (
                                           SELECT COUNT(DISTINCT k2.taxon_id)
                                           FROM keywords k2
                                           WHERE k2.name = pr.species COLLATE NOCASE
                                             AND k2.parent_id IS NOT NULL
                                             AND k2.taxon_id IS NOT NULL
                                             AND (
                                                 k2.is_species = 1
                                                 OR k2.type = 'taxonomy'
                                             )
                                       ) = 1
                                     ORDER BY root.id
                                     LIMIT 1
                                 ),
                                 pr.species
                             )
                             FROM detections d
                             JOIN predictions pr ON pr.detection_id = d.id
                             LEFT JOIN prediction_review pr_rev
                              ON pr_rev.prediction_id = pr.id
                             AND pr_rev.workspace_id = sh.workspace_id
                             WHERE d.photo_id = sh.photo_id
                               AND pr.species IS NOT NULL
                               AND COALESCE(pr_rev.status, 'pending') != 'rejected'
                               AND pr.labels_fingerprint = (
                                   SELECT pr2.labels_fingerprint
                                   FROM predictions pr2
                                   WHERE pr2.detection_id = pr.detection_id
                                     AND pr2.classifier_model = pr.classifier_model
                                   ORDER BY pr2.created_at DESC, pr2.id DESC
                                   LIMIT 1
                               )
                             ORDER BY pr.confidence DESC, pr.id DESC
                             LIMIT 1
                         ) COLLATE NOCASE
                     )
                 )"""
        if species:
            rows = self.conn.execute(
                f"""SELECT sh.species, sh.photo_id, sh.rank
                   FROM species_highlights sh
                   {eligibility_joins}
                   WHERE sh.workspace_id = ? AND sh.species = ?
                   {eligibility_filter}
                   ORDER BY sh.rank, sh.created_at, sh.photo_id""",
                (ws, species),
            ).fetchall()
        else:
            rows = self.conn.execute(
                f"""SELECT sh.species, sh.photo_id, sh.rank
                   FROM species_highlights sh
                   {eligibility_joins}
                   WHERE sh.workspace_id = ?
                   {eligibility_filter}
                   ORDER BY sh.species, sh.rank, sh.created_at, sh.photo_id""",
                (ws,),
            ).fetchall()
        result = {}
        for r in rows:
            result.setdefault(r["species"], {})[r["photo_id"]] = r["rank"]
        return result

    def add_species_highlight(self, species, photo_id, _commit=True):
        """Add a photo to a species' ordered highlights, appending if new.

        ``species`` is canonicalized to the spelling ``add_keyword`` would
        store (see :meth:`set_photo_preference`) so highlight rows written
        from prediction-cased bucket labels key on the same string the
        keyword row and eligibility queries use.
        """
        species = self.resolve_species_display_name(species)
        ws = self._ws_id()
        row = self.conn.execute(
            """SELECT rank FROM species_highlights
               WHERE workspace_id = ? AND species = ? AND photo_id = ?""",
            (ws, species, photo_id),
        ).fetchone()
        if row:
            self.conn.execute(
                """UPDATE species_highlights
                   SET updated_at = datetime('now')
                   WHERE workspace_id = ? AND species = ? AND photo_id = ?""",
                (ws, species, photo_id),
            )
            if _commit:
                self.conn.commit()
            return row["rank"]
        max_rank = self.conn.execute(
            """SELECT COALESCE(MAX(rank), 0) AS max_rank
               FROM species_highlights
               WHERE workspace_id = ? AND species = ?""",
            (ws, species),
        ).fetchone()["max_rank"]
        rank = int(max_rank or 0) + 1
        self.conn.execute(
            """INSERT INTO species_highlights
                   (workspace_id, species, photo_id, rank, created_at, updated_at)
               VALUES (?, ?, ?, ?, datetime('now'), datetime('now'))""",
            (ws, species, photo_id, rank),
        )
        if _commit:
            self.conn.commit()
        return rank

    def promote_species_highlight(self, species, photo_id, _commit=True):
        """Add a photo to a species' ordered highlights at rank 1."""
        species = self.resolve_species_display_name(species)
        ws = self._ws_id()
        rows = self.conn.execute(
            """SELECT photo_id
               FROM species_highlights
               WHERE workspace_id = ? AND species = ?
               ORDER BY rank, created_at, photo_id""",
            (ws, species),
        ).fetchall()
        ids = [r["photo_id"] for r in rows if r["photo_id"] != photo_id]
        ids.insert(0, photo_id)

        self.conn.execute(
            """INSERT INTO species_highlights
                   (workspace_id, species, photo_id, rank, created_at, updated_at)
               VALUES (?, ?, ?, 1, datetime('now'), datetime('now'))
               ON CONFLICT(workspace_id, species, photo_id) DO UPDATE SET
                   rank = excluded.rank,
                   updated_at = excluded.updated_at""",
            (ws, species, photo_id),
        )
        for rank, pid in enumerate(ids, start=1):
            self.conn.execute(
                """UPDATE species_highlights
                   SET rank = ?, updated_at = datetime('now')
                   WHERE workspace_id = ? AND species = ? AND photo_id = ?""",
                (rank, ws, species, pid),
            )
        if _commit:
            self.conn.commit()
        return 1

    def remove_species_highlight(self, species, photo_id, _commit=True):
        """Remove a photo from a species' ordered highlights."""
        species = self.resolve_species_display_name(species)
        ws = self._ws_id()
        cur = self.conn.execute(
            """DELETE FROM species_highlights
               WHERE workspace_id = ? AND species = ? AND photo_id = ?""",
            (ws, species, photo_id),
        )
        if _commit:
            self.conn.commit()
        return cur.rowcount

    def move_species_highlight(self, species, photo_id, direction, _commit=True):
        """Move a highlighted photo one step up/down within its species."""
        species = self.resolve_species_display_name(species)
        ws = self._ws_id()
        rows = self.conn.execute(
            """SELECT photo_id
               FROM species_highlights
               WHERE workspace_id = ? AND species = ?
               ORDER BY rank, created_at, photo_id""",
            (ws, species),
        ).fetchall()
        ids = [r["photo_id"] for r in rows]
        if photo_id not in ids:
            return False
        idx = ids.index(photo_id)
        if direction == "up":
            new_idx = max(0, idx - 1)
        elif direction == "down":
            new_idx = min(len(ids) - 1, idx + 1)
        else:
            return False
        if new_idx == idx:
            return True
        ids.insert(new_idx, ids.pop(idx))
        for rank, pid in enumerate(ids, start=1):
            self.conn.execute(
                """UPDATE species_highlights
                   SET rank = ?, updated_at = datetime('now')
                   WHERE workspace_id = ? AND species = ? AND photo_id = ?""",
                (rank, ws, species, pid),
            )
        if _commit:
            self.conn.commit()
        return True

    def rename_photo_preferences_species(
        self, old_species, new_species, photo_workspace_pairs=None, _commit=True,
    ):
        """Rename stored representative-photo preferences across workspaces."""
        if not old_species or not new_species or old_species == new_species:
            return 0
        global_photo_ids = None
        if photo_workspace_pairs is not None:
            global_photo_ids = sorted({
                photo_id for photo_id, _workspace_id in photo_workspace_pairs
            })
        self.rename_species_representatives_species(
            old_species, new_species, photo_ids=global_photo_ids, _commit=False
        )
        if photo_workspace_pairs is not None:
            moved = 0
            seen = set()
            for photo_id, workspace_id in photo_workspace_pairs:
                key = (photo_id, workspace_id)
                if key in seen:
                    continue
                seen.add(key)
                cur = self.conn.execute(
                    """INSERT OR IGNORE INTO photo_preferences
                          (workspace_id, purpose, species, photo_id,
                           created_at, updated_at)
                       SELECT workspace_id, purpose, ?, photo_id,
                              created_at, datetime('now')
                       FROM photo_preferences
                       WHERE workspace_id = ?
                         AND species = ?
                         AND photo_id = ?""",
                    (new_species, workspace_id, old_species, photo_id),
                )
                moved += cur.rowcount
                self.conn.execute(
                    """DELETE FROM photo_preferences
                       WHERE workspace_id = ?
                         AND species = ?
                         AND photo_id = ?""",
                    (workspace_id, old_species, photo_id),
                )
            if _commit:
                self.conn.commit()
            return moved

        cur = self.conn.execute(
            """INSERT OR IGNORE INTO photo_preferences
                  (workspace_id, purpose, species, photo_id,
                   created_at, updated_at)
               SELECT workspace_id, purpose, ?, photo_id,
                      created_at, datetime('now')
               FROM photo_preferences
               WHERE species = ?""",
            (new_species, old_species),
        )
        self.conn.execute(
            "DELETE FROM photo_preferences WHERE species = ?",
            (old_species,),
        )
        if _commit:
            self.conn.commit()
        return cur.rowcount

    def rename_species_representatives_species(
        self, old_species, new_species, photo_ids=None, _commit=True,
    ):
        """Rename global representative rows for a species.

        ``photo_ids`` limits the rename to selected photos, used by relabel
        operations that only retag a subset of a species bucket.
        """
        if not old_species or not new_species or old_species == new_species:
            return 0
        if photo_ids is None:
            cur = self.conn.execute(
                """INSERT OR IGNORE INTO species_representatives
                       (species, photo_id, selected_order, created_at, updated_at)
                    SELECT ?, photo_id, selected_order, created_at, datetime('now')
                    FROM species_representatives
                    WHERE species = ?""",
                (new_species, old_species),
            )
            moved = cur.rowcount
            self.conn.execute(
                "DELETE FROM species_representatives WHERE species = ?",
                (old_species,),
            )
            if _commit:
                self.conn.commit()
            return moved
        ids = [int(pid) for pid in photo_ids]
        if not ids:
            return 0
        # Chunk to stay under SQLite's SQLITE_MAX_VARIABLE_NUMBER (default
        # 999 on legacy builds). A species can be tagged on tens of
        # thousands of photos, so a bulk relabel or keyword-rename that
        # funnels every affected photo through here would otherwise raise
        # "too many SQL variables" before any rows move.
        moved = 0
        for chunk in _chunks(ids):
            placeholders = ",".join("?" for _ in chunk)
            cur = self.conn.execute(
                f"""INSERT OR IGNORE INTO species_representatives
                       (species, photo_id, selected_order, created_at, updated_at)
                    SELECT ?, photo_id, selected_order, created_at, datetime('now')
                    FROM species_representatives
                    WHERE species = ?
                      AND photo_id IN ({placeholders})""",
                [new_species, old_species, *chunk],
            )
            moved += cur.rowcount
            self.conn.execute(
                f"""DELETE FROM species_representatives
                    WHERE species = ?
                      AND photo_id IN ({placeholders})""",
                [old_species, *chunk],
            )
        if _commit:
            self.conn.commit()
        return moved

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
        if not old_species or not new_species or old_species == new_species:
            return 0

        if photo_workspace_pairs is not None:
            by_ws = {}
            seen = set()
            for photo_id, workspace_id in photo_workspace_pairs:
                key = (photo_id, workspace_id)
                if key in seen:
                    continue
                seen.add(key)
                by_ws.setdefault(workspace_id, []).append(photo_id)
            workspace_scopes = list(by_ws.items())
        else:
            workspace_ids = [
                r["workspace_id"] for r in self.conn.execute(
                    """SELECT DISTINCT workspace_id
                       FROM species_highlights
                       WHERE species = ?""",
                    (old_species,),
                ).fetchall()
            ]
            workspace_scopes = [(ws, None) for ws in workspace_ids]

        moved = 0
        for workspace_id, photo_ids in workspace_scopes:
            if photo_ids is None:
                src_rows = self.conn.execute(
                    """SELECT photo_id
                       FROM species_highlights
                       WHERE workspace_id = ? AND species = ?
                       ORDER BY rank, created_at, photo_id""",
                    (workspace_id, old_species),
                ).fetchall()
            else:
                # Chunk the IN(...) clause: photo_ids can carry every photo
                # tagged with the renamed keyword in this workspace, which on
                # legacy SQLite builds (SQLITE_MAX_VARIABLE_NUMBER=999) blows
                # the parameter cap once a species passes ~997 tagged photos.
                # Chunk in memory then re-sort so the rebucket order matches
                # the single-query path.
                raw_rows = []
                for chunk in _chunks(photo_ids):
                    placeholders = ",".join("?" for _ in chunk)
                    raw_rows.extend(self.conn.execute(
                        f"""SELECT photo_id, rank, created_at
                            FROM species_highlights
                            WHERE workspace_id = ? AND species = ?
                              AND photo_id IN ({placeholders})""",
                        (workspace_id, old_species, *chunk),
                    ).fetchall())
                raw_rows.sort(
                    key=lambda r: (r["rank"], r["created_at"], r["photo_id"])
                )
                src_rows = raw_rows
            if not src_rows:
                continue
            existing = {
                r["photo_id"] for r in self.conn.execute(
                    """SELECT photo_id FROM species_highlights
                       WHERE workspace_id = ? AND species = ?""",
                    (workspace_id, new_species),
                ).fetchall()
            }
            next_rank = int(self.conn.execute(
                """SELECT COALESCE(MAX(rank), 0) AS max_rank
                   FROM species_highlights
                   WHERE workspace_id = ? AND species = ?""",
                (workspace_id, new_species),
            ).fetchone()["max_rank"] or 0) + 1
            for r in src_rows:
                pid = r["photo_id"]
                self.conn.execute(
                    """DELETE FROM species_highlights
                       WHERE workspace_id = ? AND species = ? AND photo_id = ?""",
                    (workspace_id, old_species, pid),
                )
                if pid in existing:
                    continue
                self.conn.execute(
                    """INSERT INTO species_highlights
                           (workspace_id, species, photo_id, rank,
                            created_at, updated_at)
                       VALUES (?, ?, ?, ?, datetime('now'), datetime('now'))""",
                    (workspace_id, new_species, pid, next_rank),
                )
                next_rank += 1
                moved += 1
        if _commit:
            self.conn.commit()
        return moved

    def get_folders_with_quality_data(self):
        """Return folders with at least one scored photo in their subtree.

        Used to populate the folder dropdown on the highlights page.
        ``photo_count`` is the count of scored photos across the folder and
        all of its descendant folders (restricted to folders whose ``status``
        is ``'ok'``) — matching the subtree scope of
        :meth:`get_highlights_candidates`.
        """
        ws = self._ws_id()
        # The recursive step also joins workspace_folders on the current
        # folder: propagation stops at any ancestor that is not in the active
        # workspace, which matches get_folder_subtree_ids and keeps the
        # dropdown counts aligned with get_highlights_candidates.
        return self.conn.execute(
            """WITH RECURSIVE ancestors(photo_id, folder_id, timestamp) AS (
                   SELECT p.id, p.folder_id, p.timestamp
                   FROM photos p
                   JOIN folders f0 ON f0.id = p.folder_id AND f0.status IN ('ok', 'partial')
                   JOIN workspace_folders wf0
                     ON wf0.folder_id = p.folder_id AND wf0.workspace_id = ?
                   WHERE p.quality_score IS NOT NULL
                   UNION ALL
                   SELECT a.photo_id, f.parent_id, a.timestamp
                   FROM ancestors a
                   JOIN folders f ON f.id = a.folder_id
                   JOIN workspace_folders wf_step
                     ON wf_step.folder_id = f.id AND wf_step.workspace_id = ?
                   WHERE f.parent_id IS NOT NULL
               )
               SELECT f.id, f.path, f.name,
                      COUNT(a.photo_id) as photo_count,
                      MAX(a.timestamp) as latest_photo
               FROM folders f
               JOIN workspace_folders wf ON wf.folder_id = f.id
               JOIN ancestors a ON a.folder_id = f.id
               WHERE wf.workspace_id = ?
                 AND f.status IN ('ok', 'partial')
               GROUP BY f.id
               ORDER BY latest_photo DESC""",
            (ws, ws, ws),
        ).fetchall()

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
            current = self.conn.execute(
                "SELECT name, type, taxon_id, parent_id FROM keywords WHERE id = ?",
                (keyword_id,),
            ).fetchone()
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
                    if parent_id is None:
                        peer = self.conn.execute(
                            "SELECT id FROM keywords "
                            "WHERE name = ? COLLATE NOCASE "
                            "AND parent_id IS NULL AND type = ? AND id != ? LIMIT 1",
                            (new_name, effective_type, keyword_id),
                        ).fetchone()
                    else:
                        peer = self.conn.execute(
                            "SELECT id FROM keywords "
                            "WHERE name = ? COLLATE NOCASE "
                            "AND parent_id = ? AND type = ? AND id != ? LIMIT 1",
                            (new_name, parent_id, effective_type, keyword_id),
                        ).fetchone()
                    if peer:
                        # Return the surviving id so callers
                        # (api_update_keyword) can retarget sidecar and
                        # preferences bookkeeping onto the surviving row.
                        self._merge_keyword_into(keyword_id, peer["id"])
                        self.conn.commit()
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
                        cross = self.conn.execute(
                            "SELECT id, type FROM keywords "
                            "WHERE name = ? COLLATE NOCASE "
                            "AND parent_id = ? AND id != ? LIMIT 1",
                            (new_name, parent_id, keyword_id),
                        ).fetchone()
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

        set_clause = ", ".join(f"{k} = ?" for k in updates)
        values = list(updates.values()) + [keyword_id]
        self.conn.execute(f"UPDATE keywords SET {set_clause} WHERE id = ?", values)
        self.conn.commit()
        return keyword_id

    def get_all_keywords(self):
        """Return keywords used in the active workspace (plus ancestors) with photo counts, type, and taxon info."""
        ws = self._ws_id()
        return self.conn.execute(
            """WITH RECURSIVE
               ws_kw AS (
                   SELECT DISTINCT pk.keyword_id AS id
                   FROM photo_keywords pk
                   JOIN photos p ON p.id = pk.photo_id
                   JOIN workspace_folders wf ON wf.folder_id = p.folder_id
                   WHERE wf.workspace_id = ?
               ),
               ancestors AS (
                   SELECT id FROM ws_kw
                   UNION
                   SELECT k.parent_id
                   FROM keywords k
                   JOIN ancestors a ON a.id = k.id
                   WHERE k.parent_id IS NOT NULL
               ),
               descendants(ancestor_id, descendant_id, depth) AS (
                   SELECT id, id, 0 FROM ancestors
                   UNION ALL
                   SELECT d.ancestor_id, k.id, d.depth + 1
                   FROM descendants d
                   JOIN keywords k ON k.parent_id = d.descendant_id
                   WHERE d.depth < 20
               )
               SELECT k.id, k.name, k.parent_id, k.type, k.taxon_id,
                      k.latitude, k.longitude, k.place_id,
                      t.name AS taxon_name, t.common_name AS taxon_common_name,
                      COUNT(DISTINCT ws_desc.photo_id) AS photo_count,
                      COUNT(DISTINCT ws_direct.photo_id) AS direct_photo_count
               FROM keywords k
               JOIN ancestors a ON a.id = k.id
               LEFT JOIN taxa t ON t.id = k.taxon_id
               LEFT JOIN (
                   SELECT pk.keyword_id, pk.photo_id
                   FROM photo_keywords pk
                   JOIN photos p ON p.id = pk.photo_id
                   JOIN workspace_folders wf ON wf.folder_id = p.folder_id
                   WHERE wf.workspace_id = ?
               ) ws_direct ON ws_direct.keyword_id = k.id
               LEFT JOIN descendants d ON d.ancestor_id = k.id
               LEFT JOIN (
                   SELECT pk.keyword_id, pk.photo_id
                   FROM photo_keywords pk
                   JOIN photos p ON p.id = pk.photo_id
                   JOIN workspace_folders wf ON wf.folder_id = p.folder_id
                   WHERE wf.workspace_id = ?
               ) ws_desc ON ws_desc.keyword_id = d.descendant_id
               GROUP BY k.id
               ORDER BY k.name""",
            (ws, ws, ws),
        ).fetchall()

    # -- Predictions --

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
        preserve_manual_review=False,
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
        """
        if detection_id is None:
            raise ValueError(
                "add_prediction requires a non-null detection_id; "
                "predictions without a detection row are orphaned and "
                "invisible to workspace-scoped queries"
            )
        tax = taxonomy or {}
        cur = self.conn.execute(
            """INSERT OR IGNORE INTO predictions
               (detection_id, classifier_model, labels_fingerprint,
                species, confidence, category,
                taxonomy_kingdom, taxonomy_phylum, taxonomy_class,
                taxonomy_order, taxonomy_family, taxonomy_genus, scientific_name)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            (
                detection_id,
                model,
                labels_fingerprint,
                species,
                confidence,
                category,
                tax.get("kingdom"),
                tax.get("phylum"),
                tax.get("class"),
                tax.get("order"),
                tax.get("family"),
                tax.get("genus"),
                tax.get("scientific_name"),
            ),
        )
        # SQLite's ``cur.lastrowid`` stays at the previous successful insert
        # even when this INSERT OR IGNORE was skipped by the UNIQUE
        # collision — relying on it silently upserted prediction_review for
        # the wrong prediction_id. Use rowcount (0 on IGNORE, 1 on insert)
        # to decide, then always re-query by the unique key.
        if cur.rowcount == 1:
            pred_id = cur.lastrowid
        else:
            row = self.conn.execute(
                """SELECT id FROM predictions
                   WHERE detection_id = ? AND classifier_model = ?
                     AND labels_fingerprint = ? AND species IS ?""",
                (detection_id, model, labels_fingerprint, species),
            ).fetchone()
            pred_id = row["id"] if row else None
        # Write workspace-scoped review state only when the caller actually
        # supplied something beyond the defaults. Keeping pending rows out of
        # prediction_review is intentional: absence == pending.
        has_review_state = (
            status != "pending"
            or group_id is not None
            or vote_count is not None
            or total_votes is not None
            or individual is not None
        )
        if pred_id is not None and has_review_state:
            ws_id = self._ws_id()
            if preserve_manual_review:
                review = self.conn.execute(
                    """SELECT status, individual FROM prediction_review
                       WHERE prediction_id = ? AND workspace_id = ?""",
                    (pred_id, ws_id),
                ).fetchone()
                if (
                    review is not None
                    and review["status"] in {"accepted", "rejected"}
                    and review["individual"] != AUTO_MATCH_REVIEW_MARKER
                ):
                    self.conn.commit()
                    return
            self.conn.execute(
                """INSERT INTO prediction_review
                     (prediction_id, workspace_id, status, reviewed_at,
                      individual, group_id, vote_count, total_votes)
                   VALUES (?, ?, ?, datetime('now'), ?, ?, ?, ?)
                   ON CONFLICT(prediction_id, workspace_id)
                   DO UPDATE SET status      = excluded.status,
                                 reviewed_at = excluded.reviewed_at,
                                 individual  = COALESCE(excluded.individual, individual),
                                 group_id    = COALESCE(excluded.group_id,   group_id),
                                 vote_count  = COALESCE(excluded.vote_count, vote_count),
                                 total_votes = COALESCE(excluded.total_votes,total_votes)""",
                (pred_id, ws_id, status, individual, group_id,
                 vote_count, total_votes),
            )
        self.conn.commit()

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
        ws = self._ws_id()
        row = self.conn.execute(
            """SELECT id, category FROM predictions
               WHERE detection_id = ? AND classifier_model = ?
                 AND labels_fingerprint = ? AND species IS ?""",
            (detection_id, classifier_model, labels_fingerprint, species),
        ).fetchone()
        if row is None:
            return
        pred_id = row["id"]
        if row["category"] == "match" and (category != "match" or not auto_accept):
            self.conn.execute(
                "DELETE FROM prediction_review "
                "WHERE prediction_id = ? AND workspace_id = ? "
                "AND status = 'accepted' AND individual = ?",
                (pred_id, ws, AUTO_MATCH_REVIEW_MARKER),
            )
        if row["category"] != category:
            self.conn.execute(
                "UPDATE predictions SET category = ? WHERE id = ?",
                (category, pred_id),
            )
        commit_with_retry(self.conn)

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
        ws = self._ws_id()
        # Build a reusable (cond, params) pair for the predictions subquery.
        extra_conds = []
        extra_params = []
        if model:
            extra_conds.append("pr.classifier_model = ?")
            extra_params.append(model)
        if labels_fingerprint is not None:
            extra_conds.append("pr.labels_fingerprint = ?")
            extra_params.append(labels_fingerprint)

        # The photo-id filter is chunked (one DELETE per id chunk) — a
        # reclassify over a collection larger than SQLite's bound-parameter
        # cap would otherwise fail with "too many SQL variables" after the
        # model already loaded. Chunks partition disjoint photo ids, so the
        # union of chunked DELETEs equals the single big one.
        if collection_photo_ids is not None:
            id_chunks = list(_chunks(collection_photo_ids))
        else:
            id_chunks = [None]

        for chunk in id_chunks:
            conds = list(extra_conds)
            params = list(extra_params)
            if chunk is not None:
                placeholders = ",".join("?" for _ in chunk)
                conds.append(f"d.photo_id IN ({placeholders})")
                params.extend(chunk)
            where_clause = (" WHERE " + " AND ".join(conds)) if conds else ""
            self.conn.execute(
                f"""DELETE FROM predictions WHERE id IN (
                    SELECT pr.id FROM predictions pr
                    JOIN detections d ON d.id = pr.detection_id
                    JOIN photos ph ON ph.id = d.photo_id
                    JOIN workspace_folders wf
                      ON wf.folder_id = ph.folder_id AND wf.workspace_id = ?
                    {where_clause}
                )""",
                [ws, *params],
            )

        if not clear_run_keys:
            self.conn.commit()
            return

        # Also clear matching classifier_runs rows so the next pass actually
        # re-runs the classifier. Without this, the skip gate at
        # classifier_runs would still report "done" even though the cached
        # predictions are gone, leaving detections permanently unclassified
        # unless the user forces a reclassify.
        #
        # classifier_runs has PK (detection_id, classifier_model,
        # labels_fingerprint), so delete by the full composite key, not by
        # detection_id alone — otherwise another fingerprint's run key on
        # the same detection would be wiped too.
        #
        # Run for every clear_predictions() call: when model is None we just
        # built a workspace-wide DELETE on predictions, so leaving the run
        # keys behind would strand those detections (the (detection, model,
        # fingerprint) gate would treat them as already classified).
        base_run_conds = []
        base_run_params = []
        if model is not None:
            base_run_conds.append("cr.classifier_model = ?")
            base_run_params.append(model)
        if labels_fingerprint is not None:
            base_run_conds.append("cr.labels_fingerprint = ?")
            base_run_params.append(labels_fingerprint)
        # Set-based DELETE via a rowid subquery — the previous
        # SELECT + per-row DELETE loop issued one statement per matching
        # run, which on a reclassify of a multi-thousand-detection
        # workspace dominates wall time on the startup-blocking thread.
        # Match semantics are identical: the subquery shape is the same
        # (JOIN through detections/photos/workspace_folders, same
        # optional filters), and rowid uniquely identifies each
        # classifier_runs row under the implicit-rowid default.
        # Photo-id chunking mirrors the predictions DELETE above.
        for chunk in id_chunks:
            run_conds = list(base_run_conds)
            run_params = list(base_run_params)
            if chunk is not None:
                placeholders = ",".join("?" for _ in chunk)
                run_conds.append(f"d.photo_id IN ({placeholders})")
                run_params.extend(chunk)
            run_where = (" WHERE " + " AND ".join(run_conds)) if run_conds else ""
            self.conn.execute(
                f"""DELETE FROM classifier_runs
                    WHERE rowid IN (
                        SELECT cr.rowid
                        FROM classifier_runs cr
                        JOIN detections d ON d.id = cr.detection_id
                        JOIN photos ph ON ph.id = d.photo_id
                        JOIN workspace_folders wf
                          ON wf.folder_id = ph.folder_id AND wf.workspace_id = ?
                        {run_where}
                    )""",
                [ws, *run_params],
            )
        self.conn.commit()

    def get_predictions(self, photo_ids=None, model=None, status=None):
        """Get predictions with photo, detection and review info.

        Workspace scoping is enforced by joining ``workspace_folders``; the
        per-workspace review state (status, group_id, individual, vote_count)
        is left-joined from ``prediction_review`` so absent rows naturally
        surface as ``status = 'pending'``.

        Predictions are filtered to the most recent ``labels_fingerprint``
        per ``(detection_id, classifier_model)`` so stale rows from prior
        label sets don't contaminate ``/api/predictions`` or
        ``/api/predictions/compare`` after re-classification.
        """
        ws = self._ws_id()
        base_conditions = ["wf.workspace_id = ?"]
        base_params = [ws]
        if model:
            base_conditions.append("pr.classifier_model = ?")
            base_params.append(model)
        if status:
            base_conditions.append("COALESCE(pr_rev.status, 'pending') = ?")
            base_params.append(status)
        # Latest-fingerprint-per-(detection, classifier_model) filter — same
        # pattern used by /api/species/summary and get_top_prediction_for_photo.
        base_conditions.append(
            "pr.labels_fingerprint = ("
            "SELECT pr2.labels_fingerprint FROM predictions pr2 "
            "WHERE pr2.detection_id = pr.detection_id "
            "AND pr2.classifier_model = pr.classifier_model "
            "ORDER BY pr2.created_at DESC, pr2.id DESC LIMIT 1)"
        )
        # The photo-id filter is chunked — /api/predictions passes the full
        # resolved collection scope, which can exceed SQLite's bound-parameter
        # cap. Chunks partition disjoint photo ids; the merged rows are
        # re-sorted in Python to preserve the single-query ORDER BY.
        if photo_ids is not None:
            id_chunks = list(_chunks(list(dict.fromkeys(photo_ids))))
        else:
            id_chunks = [None]
        rows = []
        for chunk in id_chunks:
            conditions = list(base_conditions)
            # first ? = pr_rev.workspace_id, rest = WHERE params
            params = [ws, *base_params]
            if chunk is not None:
                placeholders = ",".join("?" for _ in chunk)
                conditions.append(f"d.photo_id IN ({placeholders})")
                params.extend(chunk)
            where = "WHERE " + " AND ".join(conditions)
            rows.extend(self.conn.execute(
                f"""SELECT pr.*,
                           pr.classifier_model AS model,
                           COALESCE(pr_rev.status, 'pending') AS status,
                           pr_rev.individual AS individual,
                           pr_rev.group_id AS group_id,
                           pr_rev.vote_count AS vote_count,
                           pr_rev.total_votes AS total_votes,
                           d.photo_id, d.box_x, d.box_y, d.box_w, d.box_h,
                           d.detector_confidence, d.detector_model,
                           p.filename, p.timestamp
                    FROM predictions pr
                    JOIN detections d ON d.id = pr.detection_id
                    JOIN photos p ON p.id = d.photo_id
                    JOIN workspace_folders wf ON wf.folder_id = p.folder_id
                    LEFT JOIN prediction_review pr_rev
                      ON pr_rev.prediction_id = pr.id AND pr_rev.workspace_id = ?
                    {where} ORDER BY pr.confidence DESC""",
                params,
            ).fetchall())
        if len(id_chunks) > 1:
            # Match SQL "ORDER BY pr.confidence DESC" (NULLs sort last).
            rows.sort(
                key=lambda r: (r["confidence"] is None, -(r["confidence"] or 0))
            )
        return rows

    def update_prediction_status(self, prediction_id, status, _commit=True):
        """Update per-workspace review status for a prediction.

        Review state lives in ``prediction_review`` keyed by
        (prediction_id, workspace_id); we upsert here rather than UPDATE
        so the "first review in a fresh workspace" path still writes a row.

        Args:
            _commit: If False, skip the internal commit (caller is responsible
                     for committing the transaction).
        """
        ws = self._ws_id()
        self.conn.execute(
            """INSERT INTO prediction_review
                 (prediction_id, workspace_id, status, reviewed_at)
               VALUES (?, ?, ?, datetime('now'))
               ON CONFLICT(prediction_id, workspace_id)
               DO UPDATE SET status = excluded.status,
                             reviewed_at = excluded.reviewed_at""",
            (prediction_id, ws, status),
        )
        if _commit:
            self.conn.commit()

    def get_group_predictions(self, group_id):
        """Get all predictions and photo data for a burst group.

        ``group_id`` lives in the workspace-scoped ``prediction_review``
        table now, so we join there to find the member predictions.  Each
        returned row is a dict with an ``alternatives`` list containing the
        per-detection alternative species predictions (review status
        ``'alternative'``), sorted by confidence descending.
        """
        ws = self._ws_id()
        primaries = self.conn.execute(
            """SELECT pr.*,
                      pr.classifier_model AS model,
                      COALESCE(pr_rev.status, 'pending') AS status,
                      pr_rev.individual AS individual,
                      pr_rev.group_id AS group_id,
                      pr_rev.vote_count AS vote_count,
                      pr_rev.total_votes AS total_votes,
                      d.photo_id, d.box_x, d.box_y, d.box_w, d.box_h,
                      d.detector_confidence, p.filename, p.timestamp, p.sharpness,
                      p.quality_score, p.subject_sharpness, p.subject_size,
                      p.rating, p.flag, p.width, p.height
               FROM predictions pr
               JOIN prediction_review pr_rev
                 ON pr_rev.prediction_id = pr.id AND pr_rev.workspace_id = ?
               JOIN detections d ON d.id = pr.detection_id
               JOIN photos p ON p.id = d.photo_id
               JOIN workspace_folders wf
                 ON wf.folder_id = p.folder_id AND wf.workspace_id = ?
               WHERE pr_rev.group_id = ?
               ORDER BY p.quality_score DESC""",
            (ws, ws, group_id),
        ).fetchall()
        rows = [dict(r) for r in primaries]
        if not rows:
            return rows
        # Alternatives are correlated by
        # (detection_id, classifier_model, labels_fingerprint): a detection
        # may have been classified by multiple models or multiple label
        # sets (and those may share a group), so we must not merge
        # alternatives across any of those dimensions — otherwise stale
        # label-set rows would bleed into the group UI's alternatives
        # column. Alternatives are scoped per-workspace through
        # prediction_review.
        det_keys = {
            (r['detection_id'], r.get('model'), r.get('labels_fingerprint'))
            for r in rows if r.get('detection_id') is not None
        }
        alts_by_key = {k: [] for k in det_keys}
        det_ids = list({did for did, _, _ in det_keys})
        if det_ids:
            placeholders = ','.join('?' * len(det_ids))
            alt_rows = self.conn.execute(
                f"""SELECT pr.detection_id,
                           pr.classifier_model AS model,
                           pr.labels_fingerprint,
                           pr.species, pr.confidence
                    FROM predictions pr
                    JOIN prediction_review pr_rev
                      ON pr_rev.prediction_id = pr.id AND pr_rev.workspace_id = ?
                    WHERE pr_rev.status = 'alternative'
                      AND pr.detection_id IN ({placeholders})
                    ORDER BY pr.confidence DESC""",
                [ws, *det_ids],
            ).fetchall()
            for a in alt_rows:
                key = (a['detection_id'], a['model'], a['labels_fingerprint'])
                if key in alts_by_key:
                    alts_by_key[key].append(
                        {'species': a['species'], 'confidence': a['confidence']}
                    )
        for r in rows:
            r['alternatives'] = alts_by_key.get(
                (r.get('detection_id'), r.get('model'),
                 r.get('labels_fingerprint')),
                [],
            )
        return rows

    def update_predictions_status_by_photo(self, photo_id, status):
        """Upsert review status for every prediction of a photo in the active workspace.

        Review state is workspace-scoped (``prediction_review``); detections
        and predictions are global.  We enumerate the prediction ids via the
        detections join and upsert each review row.
        """
        ws = self._ws_id()
        rows = self.conn.execute(
            """SELECT pr.id FROM predictions pr
               JOIN detections d ON d.id = pr.detection_id
               WHERE d.photo_id = ?""",
            (photo_id,),
        ).fetchall()
        for r in rows:
            self.conn.execute(
                """INSERT INTO prediction_review
                     (prediction_id, workspace_id, status, reviewed_at)
                   VALUES (?, ?, ?, datetime('now'))
                   ON CONFLICT(prediction_id, workspace_id)
                   DO UPDATE SET status = excluded.status,
                                 reviewed_at = excluded.reviewed_at""",
                (r["id"], ws, status),
            )
        self.conn.commit()

    def ungroup_prediction(self, prediction_id):
        """Remove a prediction from its group in the active workspace.

        ``group_id`` lives in ``prediction_review``; this only clears the
        review row for the current workspace.
        """
        self.conn.execute(
            """UPDATE prediction_review SET group_id = NULL
               WHERE prediction_id = ? AND workspace_id = ?""",
            (prediction_id, self._ws_id()),
        )
        self.conn.commit()

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
        if labels_fingerprint is None:
            rows = self.conn.execute(
                """SELECT DISTINCT d.photo_id FROM predictions pr
                   JOIN detections d ON d.id = pr.detection_id
                   JOIN photos p ON p.id = d.photo_id
                   JOIN workspace_folders wf
                     ON wf.folder_id = p.folder_id AND wf.workspace_id = ?
                   WHERE pr.classifier_model = ?""",
                (self._ws_id(), model),
            ).fetchall()
        else:
            rows = self.conn.execute(
                """SELECT DISTINCT d.photo_id FROM predictions pr
                   JOIN detections d ON d.id = pr.detection_id
                   JOIN photos p ON p.id = d.photo_id
                   JOIN workspace_folders wf
                     ON wf.folder_id = p.folder_id AND wf.workspace_id = ?
                   WHERE pr.classifier_model = ?
                     AND pr.labels_fingerprint = ?""",
                (self._ws_id(), model, labels_fingerprint),
            ).fetchall()
        return {r["photo_id"] for r in rows}

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
        if min_detector_confidence is None:
            return self.conn.execute(
                """SELECT pr.species, pr.scientific_name, pr.confidence,
                          pr.detection_id
                   FROM predictions pr
                   JOIN detections d ON d.id = pr.detection_id
                   JOIN photos p ON p.id = d.photo_id
                   JOIN workspace_folders wf
                     ON wf.folder_id = p.folder_id AND wf.workspace_id = ?
                   WHERE d.photo_id = ?
                     AND pr.labels_fingerprint = (
                        SELECT pr2.labels_fingerprint FROM predictions pr2
                        WHERE pr2.detection_id = pr.detection_id
                          AND pr2.classifier_model = pr.classifier_model
                        ORDER BY pr2.created_at DESC, pr2.id DESC
                        LIMIT 1
                     )
                   ORDER BY pr.confidence DESC LIMIT 1""",
                (self._ws_id(), photo_id),
            ).fetchone()
        return self.conn.execute(
            """SELECT pr.species, pr.scientific_name, pr.confidence,
                      pr.detection_id
               FROM predictions pr
               JOIN detections d ON d.id = pr.detection_id
               JOIN photos p ON p.id = d.photo_id
               JOIN workspace_folders wf
                 ON wf.folder_id = p.folder_id AND wf.workspace_id = ?
               WHERE d.photo_id = ?
                 AND d.detector_confidence >= ?
                 AND pr.labels_fingerprint = (
                    SELECT pr2.labels_fingerprint FROM predictions pr2
                    WHERE pr2.detection_id = pr.detection_id
                      AND pr2.classifier_model = pr.classifier_model
                    ORDER BY pr2.created_at DESC, pr2.id DESC
                    LIMIT 1
                 )
               ORDER BY pr.confidence DESC LIMIT 1""",
            (self._ws_id(), photo_id, min_detector_confidence),
        ).fetchone()

    def get_prediction_for_photo(self, photo_id, model, labels_fingerprint=None):
        """Return species, confidence, and detection_id for a photo's prediction.

        Detections and predictions are global; the active workspace is
        enforced through ``workspace_folders``. Since prediction cache
        identity is (detection, model, fingerprint, species), callers
        should pass ``labels_fingerprint`` to avoid returning a row
        written under a different label set. ``labels_fingerprint=None``
        preserves the pre-refactor behavior (any row for the model).
        """
        if labels_fingerprint is None:
            return self.conn.execute(
                """SELECT pr.species, pr.confidence, pr.detection_id FROM predictions pr
                   JOIN detections d ON d.id = pr.detection_id
                   JOIN photos p ON p.id = d.photo_id
                   JOIN workspace_folders wf
                     ON wf.folder_id = p.folder_id AND wf.workspace_id = ?
                   WHERE d.photo_id = ? AND pr.classifier_model = ?""",
                (self._ws_id(), photo_id, model),
            ).fetchone()
        return self.conn.execute(
            """SELECT pr.species, pr.confidence, pr.detection_id FROM predictions pr
               JOIN detections d ON d.id = pr.detection_id
               JOIN photos p ON p.id = d.photo_id
               JOIN workspace_folders wf
                 ON wf.folder_id = p.folder_id AND wf.workspace_id = ?
               WHERE d.photo_id = ?
                 AND pr.classifier_model = ?
                 AND pr.labels_fingerprint = ?""",
            (self._ws_id(), photo_id, model, labels_fingerprint),
        ).fetchone()

    def get_photo_embedding(self, photo_id, model, variant=''):
        """Return the embedding blob for (photo_id, model, variant), or None."""
        row = self.conn.execute(
            "SELECT embedding FROM photo_embeddings "
            "WHERE photo_id = ? AND model = ? AND variant = ?",
            (photo_id, model, variant),
        ).fetchone()
        return row["embedding"] if row else None

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
        self.conn.execute(
            """INSERT INTO photo_embeddings (photo_id, model, variant, embedding)
               VALUES (?, ?, ?, ?)
               ON CONFLICT(photo_id, model, variant)
               DO UPDATE SET embedding = excluded.embedding,
                             created_at = datetime('now')""",
            (photo_id, model, variant, embedding_bytes),
        )
        self.conn.commit()

    def get_photos_with_embedding(self, model, variant='', photo_ids=None):
        """Return (photo_id, embedding_blob) pairs in the active workspace
        with a stored embedding for ``(model, variant)``.

        Pass ``photo_ids`` to restrict the result to a subset.
        """
        ws = self._ws_id()
        sql = (
            "SELECT pe.photo_id, pe.embedding FROM photo_embeddings pe "
            "JOIN photos p ON p.id = pe.photo_id "
            "JOIN folders f ON f.id = p.folder_id AND f.status IN ('ok', 'partial') "
            "JOIN workspace_folders wf "
            "  ON wf.folder_id = p.folder_id AND wf.workspace_id = ? "
            "WHERE pe.model = ? AND pe.variant = ?"
        )
        params = [ws, model, variant]
        if photo_ids is not None:
            if not photo_ids:
                return []
            placeholders = ",".join("?" * len(photo_ids))
            sql += f" AND pe.photo_id IN ({placeholders})"
            params.extend(photo_ids)
        rows = self.conn.execute(sql, params).fetchall()
        return [(row["photo_id"], row["embedding"]) for row in rows]

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
        ws = self._ws_id()
        if labels_fingerprint is not None:
            row = self.conn.execute(
                """SELECT pr.id FROM predictions pr
                   LEFT JOIN prediction_review pr_rev
                     ON pr_rev.prediction_id = pr.id AND pr_rev.workspace_id = ?
                   WHERE pr.detection_id = ? AND pr.classifier_model = ?
                     AND pr.labels_fingerprint = ?
                     AND COALESCE(pr_rev.status, 'pending') != 'alternative'
                   ORDER BY pr.confidence DESC LIMIT 1""",
                (ws, detection_id, model, labels_fingerprint),
            ).fetchone()
        else:
            row = self.conn.execute(
                """SELECT pr.id FROM predictions pr
                   LEFT JOIN prediction_review pr_rev
                     ON pr_rev.prediction_id = pr.id AND pr_rev.workspace_id = ?
                   WHERE pr.detection_id = ? AND pr.classifier_model = ?
                     AND COALESCE(pr_rev.status, 'pending') != 'alternative'
                   ORDER BY pr.confidence DESC LIMIT 1""",
                (ws, detection_id, model),
            ).fetchone()
        if not row:
            return
        self.conn.execute(
            """UPDATE prediction_review
                  SET individual  = NULL,
                      group_id    = NULL,
                      vote_count  = NULL,
                      total_votes = NULL
                WHERE prediction_id = ? AND workspace_id = ?""",
            (row["id"], ws),
        )
        commit_with_retry(self.conn)

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
        ws = self._ws_id()
        # Identify the primary prediction row for this (detection, model,
        # [fingerprint]), excluding any prediction already marked
        # 'alternative' in this workspace.
        if labels_fingerprint is not None:
            row = self.conn.execute(
                """SELECT pr.id FROM predictions pr
                   LEFT JOIN prediction_review pr_rev
                     ON pr_rev.prediction_id = pr.id AND pr_rev.workspace_id = ?
                   WHERE pr.detection_id = ? AND pr.classifier_model = ?
                     AND pr.labels_fingerprint = ?
                     AND COALESCE(pr_rev.status, 'pending') != 'alternative'
                   ORDER BY pr.confidence DESC LIMIT 1""",
                (ws, detection_id, model, labels_fingerprint),
            ).fetchone()
        else:
            row = self.conn.execute(
                """SELECT pr.id FROM predictions pr
                   LEFT JOIN prediction_review pr_rev
                     ON pr_rev.prediction_id = pr.id AND pr_rev.workspace_id = ?
                   WHERE pr.detection_id = ? AND pr.classifier_model = ?
                     AND COALESCE(pr_rev.status, 'pending') != 'alternative'
                   ORDER BY pr.confidence DESC LIMIT 1""",
                (ws, detection_id, model),
            ).fetchone()
        if not row:
            return
        pred_id = row["id"]
        self.conn.execute(
            """INSERT INTO prediction_review
                 (prediction_id, workspace_id, status, reviewed_at,
                  individual, group_id, vote_count, total_votes)
               VALUES (?, ?, 'pending', datetime('now'), ?, ?, ?, ?)
               ON CONFLICT(prediction_id, workspace_id)
               DO UPDATE SET individual  = excluded.individual,
                             group_id    = excluded.group_id,
                             vote_count  = excluded.vote_count,
                             total_votes = excluded.total_votes,
                             reviewed_at = excluded.reviewed_at""",
            (pred_id, ws, individual, group_id, vote_count, total_votes),
        )
        commit_with_retry(self.conn)

    def is_keyword_species(self, keyword_id):
        """Return True if the keyword represents a species-rank taxon.

        Preserve the legacy flag/type fallback when no taxonomy row can be
        resolved, but do not call a linked family/genus keyword a species.
        """
        row = self.conn.execute(
            """SELECT k.is_species, k.type, t.rank AS taxon_rank
               FROM keywords k
               LEFT JOIN taxa t ON t.id = k.taxon_id
               WHERE k.id = ?""",
            (keyword_id,),
        ).fetchone()
        if not row or not (row["is_species"] or row["type"] == "taxonomy"):
            return False
        return row["taxon_rank"] in (None, "species")

    def accept_prediction(
        self,
        prediction_id,
        replace_species=False,
        photo_ids=None,
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

        All database changes are performed atomically in a single transaction
        unless ``_commit`` is False and the caller owns the transaction.
        """
        ws = self._ws_id()
        limited_photo_ids = None
        if photo_ids is not None:
            limited_photo_ids = {int(pid) for pid in photo_ids}
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

        try:
            # Reject sibling predictions for the same
            # (detection, classifier_model, labels_fingerprint) in this
            # workspace (covers both accepting an alternative and accepting
            # the top-1). Scoping by fingerprint is critical — without it,
            # accepting a prediction from a new label set would mark old
            # label-set rows as rejected, silently rewriting review state
            # for unrelated fingerprints. Review state is workspace-scoped,
            # so we upsert each row rather than UPDATE the base predictions
            # table.
            sibs = self.conn.execute(
                """SELECT pr.id FROM predictions pr
                   LEFT JOIN prediction_review pr_rev
                     ON pr_rev.prediction_id = pr.id AND pr_rev.workspace_id = ?
                   WHERE pr.detection_id = ?
                     AND pr.classifier_model = ?
                     AND pr.labels_fingerprint = ?
                     AND pr.id != ?
                     AND COALESCE(pr_rev.status, 'pending') IN ('pending', 'alternative')""",
                (ws, pred["detection_id"], pred["model"],
                 pred["labels_fingerprint"], prediction_id),
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

            # For grouped predictions, derive consensus from individual votes
            species = pred["species"]
            if pred["group_id"] and pred["individual"]:
                import json as _json

                try:
                    votes = _json.loads(pred["individual"])
                    best = max(votes, key=lambda sp: votes[sp])
                    species = best
                except Exception:
                    pass

            kid = self.add_keyword(species, is_species=True, _commit=False)
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

            def _accept_for_photo(photo_id, this_pred_id):
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
                    #     app.py's api_predictions_compare, alongside
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
                    # support" counters in templates/compare.html. Without
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
                    # instead of stacking (mirrors _queue_keyword_remove).
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
                    self.tag_photo(photo_id, kid, _commit=False)
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

            # If grouped, accept all predictions in the group (in this workspace).
            if pred["group_id"]:
                group_preds = self.conn.execute(
                    """SELECT pr.id, d.photo_id
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
                for gp in group_preds:
                    if (
                        limited_photo_ids is not None
                        and gp["photo_id"] not in limited_photo_ids
                    ):
                        continue
                    _accept_for_photo(gp["photo_id"], gp["id"])
            else:
                if (
                    limited_photo_ids is not None
                    and pred["photo_id"] not in limited_photo_ids
                ):
                    if _commit:
                        self.conn.commit()
                    return {"species": species, "keyword_id": kid, "affected": []}
                _accept_for_photo(pred["photo_id"], prediction_id)

            if _commit:
                self.conn.commit()
            return {"species": species, "keyword_id": kid, "affected": affected}
        except Exception:
            if _commit:
                self.conn.rollback()
            raise

    def accept_subject_species(self, prediction_id):
        """Accept agreeing model predictions for one detected subject.

        Compare uses this for an additional-species suggestion: the species
        keyword is added once to the photo, the existing species keywords are
        preserved, and every current-model prediction that names the same
        species on the same detection is resolved together. Grouped
        predictions are explicitly limited to this photo so accepting a
        subject in Compare cannot silently tag the rest of a burst.
        """
        ws = self._ws_id()
        target = self.conn.execute(
            """SELECT pr.id, pr.detection_id, pr.species, d.photo_id
               FROM predictions pr
               JOIN detections d ON d.id = pr.detection_id
               JOIN photos ph ON ph.id = d.photo_id
               JOIN workspace_folders wf
                 ON wf.folder_id = ph.folder_id AND wf.workspace_id = ?
               WHERE pr.id = ?""",
            (ws, prediction_id),
        ).fetchone()
        if target is None:
            return None

        agreeing = self.conn.execute(
            """SELECT pr.id
               FROM predictions pr
               LEFT JOIN prediction_review pr_rev
                 ON pr_rev.prediction_id = pr.id AND pr_rev.workspace_id = ?
               WHERE pr.detection_id = ?
                 AND lower(trim(pr.species)) = lower(trim(?))
                 AND COALESCE(pr_rev.status, 'pending') != 'rejected'
                 AND pr.labels_fingerprint = (
                     SELECT pr2.labels_fingerprint FROM predictions pr2
                     WHERE pr2.detection_id = pr.detection_id
                       AND pr2.classifier_model = pr.classifier_model
                     ORDER BY pr2.created_at DESC, pr2.id DESC
                     LIMIT 1
                 )
               ORDER BY pr.confidence DESC, pr.id ASC""",
            (ws, target["detection_id"], target["species"]),
        ).fetchall()

        accepted_ids = []
        affected = []
        result = None
        try:
            for row in agreeing:
                accepted = self.accept_prediction(
                    row["id"],
                    photo_ids=[target["photo_id"]],
                    _commit=False,
                )
                if accepted is not None:
                    result = accepted
                    accepted_ids.append(row["id"])
                    affected.extend(accepted["affected"])
            self.conn.commit()
        except Exception:
            self.conn.rollback()
            raise

        if result is None:
            return None
        return {
            "species": result["species"],
            "keyword_id": result["keyword_id"],
            "photo_id": target["photo_id"],
            "prediction_ids": accepted_ids,
            "affected": affected,
        }

    # -- Detections --

    def record_detector_run(self, photo_id, detector_model, box_count):
        """Record that `detector_model` was run on `photo_id`.

        Global across workspaces — the output is a pure function of (photo, model).
        """
        self.conn.execute(
            """INSERT INTO detector_runs (photo_id, detector_model, box_count)
               VALUES (?, ?, ?)
               ON CONFLICT(photo_id, detector_model)
               DO UPDATE SET box_count = excluded.box_count,
                             run_at = datetime('now')""",
            (photo_id, detector_model, box_count),
        )
        self.conn.commit()

    def get_global_detection_stats(self):
        """Return global (workspace-agnostic) detector-cache counts.

        `detector_runs` is shared across workspaces by design — switching
        workspaces or bumping a threshold never invalidates these rows —
        so the settings page surfaces this as a single "N photos x M
        models cached" figure.
        """
        r = self.conn.execute(
            """SELECT COUNT(DISTINCT photo_id) AS photo_count,
                      COUNT(DISTINCT detector_model) AS model_count
               FROM detector_runs"""
        ).fetchone()
        return {"photo_count": r["photo_count"] or 0,
                "model_count": r["model_count"] or 0}

    def get_detector_run_photo_ids(self, detector_model):
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
        rows = self.conn.execute(
            """SELECT dr.photo_id
               FROM detector_runs dr
               WHERE dr.detector_model = ?
                 AND (dr.box_count = 0
                      OR EXISTS (SELECT 1 FROM detections d
                                 WHERE d.photo_id = dr.photo_id
                                   AND d.detector_model = dr.detector_model))""",
            (detector_model,),
        ).fetchall()
        return {r["photo_id"] for r in rows}

    def record_classifier_run(self, detection_id, classifier_model,
                               labels_fingerprint, prediction_count):
        self.conn.execute(
            """INSERT INTO classifier_runs
                 (detection_id, classifier_model, labels_fingerprint, prediction_count)
               VALUES (?, ?, ?, ?)
               ON CONFLICT(detection_id, classifier_model, labels_fingerprint)
               DO UPDATE SET prediction_count = excluded.prediction_count,
                             run_at = datetime('now')""",
            (detection_id, classifier_model, labels_fingerprint, prediction_count),
        )
        commit_with_retry(self.conn)

    def get_classifier_run_keys(self, detection_id):
        rows = self.conn.execute(
            """SELECT classifier_model, labels_fingerprint
               FROM classifier_runs
               WHERE detection_id = ?""",
            (detection_id,),
        ).fetchall()
        return {(r["classifier_model"], r["labels_fingerprint"]) for r in rows}

    def count_classifier_runs(self, photo_ids, classifier_model, labels_fingerprint):
        """Count distinct photos in `photo_ids` where EVERY runtime-classifiable
        target has a classifier_runs row matching the given
        (classifier_model, labels_fingerprint).

        Used by the streaming pipeline's classify stage to pre-flight how
        many photos will hit the cache vs. require fresh inference.

        Photo-scoped to match runtime accounting: the classify loop keeps its
        ``cached`` bucket photo-scoped (see pipeline_job.py — a photo lands
        there only if every processed detection is a cache hit; the moment
        any detection runs fresh inference it is promoted to ``count``).
        So the preflight only counts a photo as cached when NO qualifying
        detection would need fresh inference — otherwise a multi-subject
        photo with one cached and one uncached detection would inflate
        ``cached_estimate`` up to ``total`` and the banner would misread as
        "no work to classify" when there is real work remaining.

        Above-threshold real detections must all have matching run keys,
        and photos where MegaDetector ran with ``box_count=0`` still count
        through their synthetic full-image anchor. Below-threshold real
        detections are ignored because the pipeline still skips them
        instead of falling back.
        """
        if not photo_ids:
            return 0
        import config as cfg
        min_conf = self.get_effective_config(cfg.load()).get(
            "detector_confidence", 0.2,
        )
        # Chunk to stay under SQLITE_MAX_VARIABLE_NUMBER (default 999).
        # Match the 500-element chunks used elsewhere in this file.
        CHUNK = 500
        matched = set()
        for i in range(0, len(photo_ids), CHUNK):
            chunk = photo_ids[i:i + CHUNK]
            placeholders = ",".join("?" * len(chunk))
            # A photo counts as fully cached iff it has at least one
            # above-threshold real detection AND every above-threshold real
            # detection carries a matching (classifier_model,
            # labels_fingerprint) run key. The outer NOT EXISTS is the
            # "no uncached qualifying detection remains" clause; the outer
            # WHERE also requires at least one qualifying detection so
            # empty-detection photos don't fall through this branch (they
            # are handled by the full-image anchor branch below).
            # The category='animal' predicate on both the outer and inner
            # detection scans mirrors the runtime classify loop's
            # non-animal skip (MegaDetector can return person/vehicle
            # boxes above the confidence threshold, and the classifier
            # stage filters them out before inference). Without matching
            # the runtime filter here, a photo with cached animal
            # detections plus one uncached person/vehicle box would be
            # excluded from ``cached_estimate`` even though that photo
            # will actually be entirely cache-served at runtime, so the
            # UI would understate cached work.
            rows = self.conn.execute(
                f"SELECT DISTINCT d.photo_id "
                f"FROM detections d "
                f"WHERE d.detector_model != 'full-image' "
                f"  AND d.category = 'animal' "
                f"  AND d.detector_confidence >= ? "
                f"  AND d.photo_id IN ({placeholders}) "
                f"  AND NOT EXISTS ( "
                f"    SELECT 1 FROM detections d2 "
                f"    WHERE d2.photo_id = d.photo_id "
                f"      AND d2.detector_model != 'full-image' "
                f"      AND d2.category = 'animal' "
                f"      AND d2.detector_confidence >= ? "
                f"      AND NOT EXISTS ( "
                f"        SELECT 1 FROM classifier_runs cr "
                f"        WHERE cr.detection_id = d2.id "
                f"          AND cr.classifier_model = ? "
                f"          AND cr.labels_fingerprint = ? "
                f"      ) "
                f"  )",
                [min_conf, *chunk, min_conf, classifier_model,
                 labels_fingerprint],
            ).fetchall()
            for r in rows:
                matched.add(r["photo_id"])
            rows = self.conn.execute(
                f"""WITH full_anchor AS (
                        SELECT photo_id, MIN(id) AS detection_id
                          FROM detections
                         WHERE detector_model = 'full-image'
                           AND photo_id IN ({placeholders})
                         GROUP BY photo_id
                      )
                    SELECT DISTINCT fa.photo_id
                      FROM full_anchor fa
                      JOIN detector_runs dr
                        ON dr.photo_id = fa.photo_id
                       AND dr.detector_model = 'megadetector-v6'
                       AND dr.box_count = 0
                      JOIN classifier_runs cr
                        ON cr.detection_id = fa.detection_id
                       AND cr.classifier_model = ?
                       AND cr.labels_fingerprint = ?
                     WHERE NOT EXISTS (
                             SELECT 1 FROM detections d
                              WHERE d.photo_id = fa.photo_id
                                AND d.detector_model != 'full-image'
                           )""",
                [*chunk, classifier_model, labels_fingerprint],
            ).fetchall()
            for r in rows:
                matched.add(r["photo_id"])
        return len(matched)

    def get_labels_fingerprints(self):
        """Return all rows from the labels_fingerprints sidecar.

        Each row records the (fingerprint, sources, label_count) triple a
        classify run wrote — single-file runs list one source, merged-set
        runs list several. Used by the inventory endpoint to identify
        merged fingerprints that are still current (sources on disk and
        unchanged) so they don't get marked stale.
        """
        import json
        rows = self.conn.execute(
            "SELECT fingerprint, display_name, sources_json, label_count "
            "FROM labels_fingerprints"
        ).fetchall()
        out = []
        for r in rows:
            try:
                sources = json.loads(r["sources_json"] or "[]")
            except (TypeError, ValueError):
                sources = []
            out.append({
                "fingerprint": r["fingerprint"],
                "display_name": r["display_name"],
                "sources": sources,
                "label_count": r["label_count"],
            })
        return out

    def upsert_labels_fingerprint(self, fingerprint, display_name, sources, label_count):
        import json
        self.conn.execute(
            """INSERT INTO labels_fingerprints
                 (fingerprint, display_name, sources_json, label_count)
               VALUES (?, ?, ?, ?)
               ON CONFLICT(fingerprint)
               DO UPDATE SET display_name = excluded.display_name,
                             sources_json = excluded.sources_json,
                             label_count  = excluded.label_count""",
            (fingerprint, display_name, json.dumps(sources or []), label_count),
        )
        self.conn.commit()

    def get_review_status(self, prediction_id, workspace_id):
        row = self.conn.execute(
            """SELECT status FROM prediction_review
               WHERE prediction_id = ? AND workspace_id = ?""",
            (prediction_id, workspace_id),
        ).fetchone()
        return row["status"] if row else "pending"

    def set_review_status(self, prediction_id, workspace_id, status,
                           individual=None, group_id=None):
        self.conn.execute(
            """INSERT INTO prediction_review
                 (prediction_id, workspace_id, status, reviewed_at, individual, group_id)
               VALUES (?, ?, ?, datetime('now'), ?, ?)
               ON CONFLICT(prediction_id, workspace_id)
               DO UPDATE SET status      = excluded.status,
                             reviewed_at = excluded.reviewed_at,
                             individual  = COALESCE(excluded.individual, individual),
                             group_id    = COALESCE(excluded.group_id,   group_id)""",
            (prediction_id, workspace_id, status, individual, group_id),
        )
        self.conn.commit()

    def save_detections(self, photo_id, detections, detector_model):
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
        ids = self._upsert_detection_rows(photo_id, detector_model, detections)
        commit_with_retry(self.conn)
        return ids

    def _upsert_detection_rows(self, photo_id, detector_model, detections):
        """Content-addressed UPSERT of detection rows for one (photo, model).

        Returns the list of unique IDs in first-seen order. Does NOT commit —
        the caller controls the transaction so the detector_runs row can be
        written in the same commit (see `write_detection_batch`).
        """
        from detection_id import detection_id as _detection_id

        unique = {}
        ordered_ids = []
        for idx, det in enumerate(detections):
            box = det["box"]
            category = det.get("category", "animal")
            det_id = _detection_id(
                photo_id, detector_model,
                (box["x"], box["y"], box["w"], box["h"]),
                category,
            )
            if det_id not in unique:
                ordered_ids.append(det_id)
                unique[det_id] = (det, category, idx)
                continue
            prev_det, _prev_category, prev_idx = unique[det_id]
            if (
                det["confidence"] > prev_det["confidence"]
                or (
                    det["confidence"] == prev_det["confidence"]
                    and idx > prev_idx
                )
            ):
                unique[det_id] = (det, category, idx)

        ids = []
        for det_id in ordered_ids:
            det, category, _idx = unique[det_id]
            box = det["box"]
            # INSERT ON CONFLICT DO UPDATE — true UPSERT. Do NOT use
            # `INSERT OR REPLACE`, which DELETEs the conflicting row before
            # re-inserting; that DELETE fires `predictions.detection_id`
            # `ON DELETE CASCADE` and silently wipes any predictions another
            # pipeline has already written for this detection.
            self.conn.execute(
                """INSERT INTO detections
                     (id, photo_id, detector_model, box_x, box_y, box_w, box_h,
                      detector_confidence, category)
                   VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                   ON CONFLICT(id) DO UPDATE SET
                     photo_id = excluded.photo_id,
                     detector_model = excluded.detector_model,
                     box_x = excluded.box_x,
                     box_y = excluded.box_y,
                     box_w = excluded.box_w,
                     box_h = excluded.box_h,
                     detector_confidence = excluded.detector_confidence,
                     category = excluded.category""",
                (det_id, photo_id, detector_model,
                 box["x"], box["y"], box["w"], box["h"],
                 det["confidence"], category),
            )
            ids.append(det_id)

        # Retire rows the new run no longer produces. Narrow DELETE: only
        # rows whose ID is NOT in the new set. Safe under concurrent writers
        # because two writers with the same detections compute the same
        # `new_ids` set, so neither deletes the other's rows.
        new_ids = set(ids)
        existing = [r["id"] for r in self.conn.execute(
            "SELECT id FROM detections WHERE photo_id = ? AND detector_model = ?",
            (photo_id, detector_model),
        ).fetchall()]
        stale = [eid for eid in existing if eid not in new_ids]
        # Chunk to stay under SQLite's compile-time SQLITE_MAX_VARIABLE_NUMBER
        # (defaults to 999 in older builds, 32766 in newer). A single photo
        # rarely has >1k detections today, but the chunking is cheap insurance
        # against future detectors that produce many small boxes.
        CHUNK = 500
        for i in range(0, len(stale), CHUNK):
            chunk = stale[i:i + CHUNK]
            placeholders = ",".join("?" for _ in chunk)
            self.conn.execute(
                f"DELETE FROM detections WHERE id IN ({placeholders})",
                chunk,
            )
        return ids

    def write_detection_batch(self, photo_id, detector_model, detections):
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
        try:
            ids = self._upsert_detection_rows(photo_id, detector_model, detections)
            self.conn.execute(
                """INSERT INTO detector_runs (photo_id, detector_model, box_count)
                   VALUES (?, ?, ?)
                   ON CONFLICT(photo_id, detector_model)
                   DO UPDATE SET box_count = excluded.box_count,
                                 run_at = datetime('now')""",
                (photo_id, detector_model, len(ids)),
            )
            commit_with_retry(self.conn)
            return ids
        except Exception:
            self.conn.rollback()
            raise

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
        q = ("SELECT * FROM detections WHERE photo_id = ? "
             "AND detector_confidence >= ?")
        params = [photo_id, min_conf]
        if detector_model is not None:
            q += " AND detector_model = ?"
            params.append(detector_model)
        # Explicit ``id ASC`` tie-break so callers that take the first
        # row (mask extraction's primary detection picker) agree with
        # ``find_stale_masks`` on which row is the primary when two
        # detections share the maximum confidence. Without this,
        # SQLite's row order on ties is implementation-defined.
        q += " ORDER BY detector_confidence DESC, id ASC"
        return self.conn.execute(q, params).fetchall()

    def get_detections_for_photos(self, photo_ids, min_conf=None,
                                  detector_model=None):
        """Return {photo_id: [det_dict, ...]} for a batch of photos.

        Each det_dict has keys: id, x, y, w, h, confidence, category, and
        detector_model. Lists are ordered by confidence DESC. The detections
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
        # Dedup-preserving-order: same id appearing in two chunks would
        # cause setdefault(...).append(...) below to emit each row twice.
        photo_ids = list(dict.fromkeys(photo_ids))
        result = {}
        for chunk in _chunks(photo_ids):
            placeholders = ",".join("?" for _ in chunk)
            q = (
                f"SELECT id, photo_id, box_x, box_y, box_w, box_h, "
                f"       detector_confidence, category, detector_model "
                f"FROM detections "
                f"WHERE photo_id IN ({placeholders}) "
                f"  AND detector_confidence >= ?"
            )
            params = [*chunk, min_conf]
            if detector_model is not None:
                q += " AND detector_model = ?"
                params.append(detector_model)
            q += " ORDER BY photo_id, detector_confidence DESC"
            rows = self.conn.execute(q, params).fetchall()
            for r in rows:
                result.setdefault(r["photo_id"], []).append({
                    "id": r["id"],
                    "x": r["box_x"],
                    "y": r["box_y"],
                    "w": r["box_w"],
                    "h": r["box_h"],
                    "confidence": r["detector_confidence"],
                    "category": r["category"],
                    "detector_model": r["detector_model"],
                })
        return result

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
        q = ("SELECT * FROM predictions WHERE detection_id = ? "
             "AND confidence >= ?")
        params = [detection_id, min_classifier_conf]
        if classifier_model is not None:
            q += " AND classifier_model = ?"
            params.append(classifier_model)
        if labels_fingerprint is not None:
            q += " AND labels_fingerprint = ?"
            params.append(labels_fingerprint)
        q += " ORDER BY confidence DESC"
        return self.conn.execute(q, params).fetchall()

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
        if detector_model is None:
            self.conn.execute(
                "DELETE FROM detections WHERE photo_id = ?", (photo_id,)
            )
            self.conn.execute(
                "DELETE FROM detector_runs WHERE photo_id = ?", (photo_id,)
            )
        else:
            self.conn.execute(
                "DELETE FROM detections WHERE photo_id = ? AND detector_model = ?",
                (photo_id, detector_model),
            )
            self.conn.execute(
                "DELETE FROM detector_runs WHERE photo_id = ? AND detector_model = ?",
                (photo_id, detector_model),
            )
        self.conn.commit()

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
        ``detection_conf`` are sourced from the primary (highest-confidence)
        row in the ``detections`` table — the legacy ``photos`` columns are
        not populated by normal pipeline runs. Ordered by timestamp DESC.
        """
        ws_id = self._ws_id()
        if category is None:
            where = (
                "p.miss_no_subject=1 OR p.miss_clipped=1 OR p.miss_oof=1"
            )
        else:
            col = {
                "no_subject": "miss_no_subject",
                "clipped":    "miss_clipped",
                "oof":        "miss_oof",
            }[category]
            where = f"p.{col}=1"

        params = [ws_id]
        if since:
            where = f"({where}) AND p.miss_computed_at >= ?"
            params.append(since)
        scope_clause, scope_params = self._scope_clause(photo_ids)
        params.extend(scope_params)

        rows = self.conn.execute(
            f"SELECT p.id, p.folder_id, p.filename, p.companion_path, "
            f"       p.timestamp, p.burst_id, "
            f"       p.subject_size, p.crop_complete, "
            f"       p.subject_tenengrad, p.bg_tenengrad, "
            f"       p.miss_no_subject, p.miss_clipped, p.miss_oof, "
            f"       p.miss_computed_at, p.flag "
            f"FROM photos p "
            f"JOIN workspace_folders wf ON wf.folder_id = p.folder_id "
            f"WHERE wf.workspace_id = ? "
            f"  AND ({where}) "
            f"  AND (p.flag IS NULL OR p.flag != 'rejected') "
            f"  {scope_clause} "
            f"ORDER BY p.timestamp DESC",
            params,
        ).fetchall()
        photos = [dict(r) for r in rows]
        if not photos:
            return photos

        import json as _json

        import config as cfg
        min_conf = self.get_effective_config(cfg.load()).get(
            "detector_confidence", 0.2
        )
        photo_ids = [p["id"] for p in photos]
        # Chunk to stay under SQLite's SQLITE_MAX_VARIABLE_NUMBER (default 999).
        # A workspace with thousands of flagged misses would otherwise raise
        # ``OperationalError: too many SQL variables``.
        CHUNK = 500
        primary = {}
        for i in range(0, len(photo_ids), CHUNK):
            chunk = photo_ids[i:i + CHUNK]
            placeholders = ",".join("?" * len(chunk))
            det_rows = self.conn.execute(
                f"SELECT photo_id, box_x, box_y, box_w, box_h, "
                f"       detector_confidence "
                f"FROM detections "
                f"WHERE detector_confidence >= ? AND photo_id IN ({placeholders}) "
                f"ORDER BY photo_id, detector_confidence DESC",
                [min_conf, *chunk],
            ).fetchall()
            for d in det_rows:
                primary.setdefault(d["photo_id"], d)
        for p in photos:
            d = primary.get(p["id"])
            if d is not None:
                p["detection_box"] = _json.dumps({
                    "x": d["box_x"], "y": d["box_y"],
                    "w": d["box_w"], "h": d["box_h"],
                })
                p["detection_conf"] = d["detector_confidence"]
            else:
                p["detection_box"] = None
                p["detection_conf"] = None
        return photos

    def clear_miss_flag(self, photo_id, category):
        """Set the given miss column to 0 on the given photo.

        Raises ValueError if the photo is not in the active workspace, so
        that `/api/misses/<id>/unflag` can't touch another workspace's photos.
        """
        self._verify_photo_in_workspace(photo_id)
        col = {
            "no_subject": "miss_no_subject",
            "clipped":    "miss_clipped",
            "oof":        "miss_oof",
        }[category]
        self.conn.execute(
            f"UPDATE photos SET {col}=0 WHERE id=?", (photo_id,)
        )
        self.conn.commit()

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
        col = {
            "no_subject": "miss_no_subject",
            "clipped":    "miss_clipped",
            "oof":        "miss_oof",
        }[category]
        params = [self._ws_id()]
        since_clause = ""
        if since:
            since_clause = "    AND p.miss_computed_at >= ? "
            params.append(since)
        scope_clause, scope_params = self._scope_clause(photo_ids)
        params.extend(scope_params)
        rows = self.conn.execute(
            f"SELECT p.id, p.flag FROM photos p "
            f"JOIN workspace_folders wf ON wf.folder_id = p.folder_id "
            f"WHERE wf.workspace_id = ? "
            f"  AND p.{col}=1 "
            f"  AND (p.flag IS NULL OR p.flag != 'rejected') "
            f"{since_clause}"
            f"{scope_clause}",
            params,
        ).fetchall()
        # Preserve NULL flag values in old_value so undo is lossless.
        # Coercing NULL to "" would make _apply_undo restore an empty
        # string instead of the original NULL, leaving rows in a
        # non-canonical state that bypasses code paths expecting
        # none/flagged/rejected (or NULL).
        affected = [
            {"photo_id": r["id"], "old_value": r["flag"]}
            for r in rows
        ]
        if not affected:
            return []
        ids = [a["photo_id"] for a in affected]
        # Chunk to stay under SQLite's SQLITE_MAX_VARIABLE_NUMBER (default 999).
        _CHUNK = 500
        for i in range(0, len(ids), _CHUNK):
            chunk = ids[i:i + _CHUNK]
            placeholders = ",".join("?" * len(chunk))
            self.conn.execute(
                f"UPDATE photos SET flag='rejected' WHERE id IN ({placeholders})",
                chunk,
            )
        self.conn.commit()
        return affected

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
        if not photo_ids:
            return {}
        result: dict = {}
        ids = list(photo_ids)
        _CHUNK = 900
        for i in range(0, len(ids), _CHUNK):
            chunk = ids[i : i + _CHUNK]
            placeholders = ",".join("?" * len(chunk))
            rows = self.conn.execute(
                f"SELECT id, photo_id FROM detections "
                f"WHERE photo_id IN ({placeholders})",
                tuple(chunk),
            ).fetchall()
            for row in rows:
                result.setdefault(row["photo_id"], set()).add(row["id"])
        return result

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
        ids = list(detection_ids)
        _CHUNK = 900
        for i in range(0, len(ids), _CHUNK):
            chunk = ids[i : i + _CHUNK]
            placeholders = ",".join("?" * len(chunk))
            self.conn.execute(
                f"DELETE FROM detections WHERE id IN ({placeholders})",
                chunk,
            )
        self.conn.commit()

    # -- Pending Changes --

    def queue_change(self, photo_id, change_type, value, workspace_id=None, _commit=True):
        """Add a change to the sync queue (skips if already queued).

        Returns the inserted pending change token, or None if an identical row already exists.
        If workspace_id is not provided, uses the active workspace.

        Args:
            _commit: If False, skip the internal commit (caller is responsible
                     for committing the transaction).
        """
        # Normalization choke point for sidecar-bound keyword names: the
        # queued value is written verbatim into XMP by sync_to_xmp, so a
        # stray-quote variant here would leak into sidecars even though
        # add_keyword stores the clean spelling. Normalizing in one place
        # also keeps the (photo_id, change_type, value) dedupe below and
        # the add/remove cancellation in app.py working on one spelling.
        if change_type in ("keyword_add", "keyword_remove", "keyword_remove_flat"):
            value = normalize_keyword_display(value)
            if not value:
                return None
        ws_id = workspace_id if workspace_id is not None else self._ws_id()
        existing = self.conn.execute(
            "SELECT id FROM pending_changes WHERE photo_id = ? AND change_type = ? AND value = ? AND workspace_id = ?",
            (photo_id, change_type, value, ws_id),
        ).fetchone()
        if existing:
            return None
        change_token = str(uuid.uuid4())
        self.conn.execute(
            "INSERT INTO pending_changes (photo_id, change_type, value, change_token, workspace_id) VALUES (?, ?, ?, ?, ?)",
            (photo_id, change_type, value, change_token, ws_id),
        )
        if _commit:
            self.conn.commit()
        return change_token

    def get_pending_changes(self):
        """Return all pending changes ordered by creation time."""
        return self.conn.execute(
            "SELECT * FROM pending_changes WHERE workspace_id = ? ORDER BY created_at",
            (self._ws_id(),),
        ).fetchall()

    def remove_pending_changes(self, photo_id, change_type=None, value=None, workspace_id=None, _commit=True):
        """Delete matching pending changes. Returns rows removed.

        Args:
            _commit: If False, skip the internal commit (caller is responsible
                     for committing the transaction).
        """
        ws_id = workspace_id if workspace_id is not None else self._ws_id()
        clauses = ["photo_id = ?", "workspace_id = ?"]
        params = [photo_id, ws_id]
        if change_type is not None:
            clauses.append("change_type = ?")
            params.append(change_type)
        if value is not None:
            clauses.append("value = ?")
            params.append(value)

        cur = self.conn.execute(
            f"DELETE FROM pending_changes WHERE {' AND '.join(clauses)}",
            params,
        )
        if _commit:
            self.conn.commit()
        return cur.rowcount

    def remove_pending_change_token(self, change_token):
        """Delete a single pending change by immutable token. Returns rows removed."""
        if not change_token:
            return 0
        cur = self.conn.execute(
            "DELETE FROM pending_changes WHERE change_token = ? AND workspace_id = ?",
            (change_token, self._ws_id()),
        )
        self.conn.commit()
        return cur.rowcount

    def clear_pending(self, change_ids):
        """Delete pending changes by id."""
        if not change_ids:
            return
        placeholders = ",".join("?" for _ in change_ids)
        self.conn.execute(
            f"DELETE FROM pending_changes WHERE id IN ({placeholders}) AND workspace_id = ?",
            list(change_ids) + [self._ws_id()],
        )
        self.conn.commit()

    def queue_flag_change_if_enabled(self, photo_id, flag, workspace_id=None, _commit=True):
        """Queue a flag write when the active config opts into XMP flag sync."""
        ws_id = workspace_id if workspace_id is not None else self._ws_id()
        flag = flag or "none"
        self.remove_pending_changes(photo_id, "flag", workspace_id=ws_id, _commit=False)
        if flag not in {"none", "flagged", "rejected"}:
            log.warning("Not queueing invalid XMP flag value for photo %s: %r", photo_id, flag)
            if _commit:
                self.conn.commit()
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
                self.conn.commit()
            return None

        token = self.queue_change(
            photo_id, "flag", flag, workspace_id=ws_id, _commit=False
        )
        if _commit:
            self.conn.commit()
        return token

    # -- Edit History --

    def record_edit(self, action_type, description, new_value, items, is_batch=False, _commit=True):
        """Record an edit action with per-photo before/after values.

        Clears the redo stack (any undone entries) since a new action invalidates them.

        Args:
            _commit: If False, skip the internal commit and the history prune
                     (caller is responsible for committing the transaction;
                     prune can be run later).
        """
        # Clear redo stack — new edit invalidates undone entries
        self.conn.execute(
            "DELETE FROM edit_history WHERE workspace_id = ? AND undone = 1",
            (self._ws_id(),),
        )
        cur = self.conn.execute(
            "INSERT INTO edit_history (workspace_id, action_type, description, new_value, is_batch) VALUES (?, ?, ?, ?, ?)",
            (self._ws_id(), action_type, description, new_value, 1 if is_batch else 0),
        )
        edit_id = cur.lastrowid
        for item in items:
            self.conn.execute(
                "INSERT INTO edit_history_items (edit_id, photo_id, old_value, new_value) VALUES (?, ?, ?, ?)",
                (edit_id, item['photo_id'], item['old_value'], item['new_value']),
            )
        if _commit:
            self.conn.commit()
            self._prune_edit_history()
        return edit_id

    def get_edit_history(self, limit=50, offset=0):
        """Return recent edit history entries (most recent first) with item counts."""
        rows = self.conn.execute(
            """SELECT eh.*, COUNT(ehi.id) as item_count
               FROM edit_history eh
               LEFT JOIN edit_history_items ehi ON ehi.edit_id = eh.id
               WHERE eh.workspace_id = ? AND eh.undone = 0
               GROUP BY eh.id
               ORDER BY eh.created_at DESC, eh.id DESC
               LIMIT ? OFFSET ?""",
            (self._ws_id(), limit, offset),
        ).fetchall()
        return [dict(r) for r in rows]

    # Action types that appear in history but cannot be reversed
    _NON_UNDOABLE = (
        'prediction_reject', 'discard',
        # Location edits (set/clear/link) are auditable but not undoable
        # in v1 — _apply_undo has no handlers for them, so including them
        # would silently advance the undo cursor without reverting state.
        # Adding undo support is a follow-up if it becomes important.
        'location_set', 'location_clear', 'location_link',
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
        """
        placeholders = ",".join("?" for _ in self._NON_UNDOABLE)
        entry = self.conn.execute(
            f"SELECT * FROM edit_history WHERE workspace_id = ? AND undone = 0 AND action_type NOT IN ({placeholders}) "
            "ORDER BY created_at DESC, id DESC LIMIT 1",
            (self._ws_id(), *self._NON_UNDOABLE),
        ).fetchone()
        if not entry:
            return None
        entry = dict(entry)
        items = self.conn.execute(
            "SELECT * FROM edit_history_items WHERE edit_id = ?",
            (entry['id'],),
        ).fetchall()

        self._apply_undo(entry, items)

        self.conn.execute("UPDATE edit_history SET undone = 1 WHERE id = ?", (entry['id'],))
        self.conn.commit()
        return entry

    def redo_last_undo(self):
        """Redo the most recently undone edit. Returns the entry dict, or None.

        Replays in chronological order (ASC) so sequential undos are redone correctly.
        """
        placeholders = ",".join("?" for _ in self._NON_UNDOABLE)
        entry = self.conn.execute(
            f"SELECT * FROM edit_history WHERE workspace_id = ? AND undone = 1 AND action_type NOT IN ({placeholders}) "
            "ORDER BY created_at ASC, id ASC LIMIT 1",
            (self._ws_id(), *self._NON_UNDOABLE),
        ).fetchone()
        if not entry:
            return None
        entry = dict(entry)
        items = self.conn.execute(
            "SELECT * FROM edit_history_items WHERE edit_id = ?",
            (entry['id'],),
        ).fetchall()

        self._apply_redo(entry, items)

        self.conn.execute("UPDATE edit_history SET undone = 0 WHERE id = ?", (entry['id'],))
        self.conn.commit()
        return entry

    def _apply_undo(self, entry, items):
        """Reverse the effects of an edit entry."""
        for item in items:
            old_val = item['old_value']
            pid = item['photo_id']
            if entry['action_type'] == 'rating':
                # Edit history is already workspace-scoped; skip re-verification
                self.update_photo_rating(pid, int(old_val), verify_workspace=False)
                if old_val != entry['new_value']:
                    self.remove_pending_changes(pid, 'rating', entry['new_value'])
                    self.queue_change(pid, 'rating', old_val)
            elif entry['action_type'] == 'flag':
                self.update_photo_flag(pid, old_val, verify_workspace=False)
                self.queue_flag_change_if_enabled(pid, old_val)
            elif entry['action_type'] == 'wildlife_excluded':
                self.update_photo_wildlife_excluded(
                    pid, old_val == "1", verify_workspace=False
                )
            elif entry['action_type'] == 'color_label':
                if old_val:
                    self.set_color_label(pid, old_val)
                else:
                    self.remove_color_label(pid)
            elif entry['action_type'] == 'edit_recipe':
                self.set_photo_edit_recipe(
                    pid,
                    old_val if old_val else None,
                    verify_workspace=False,
                )
            elif entry['action_type'] in ('keyword_add', 'prediction_accept'):
                old_meta = self._edit_old_value_meta(old_val)
                # ``no_tag`` marks a prediction_accept where the photo
                # already carried an equivalent species, so no tag was
                # actually added. Undo must not untag a keyword the user
                # deliberately kept — only the prediction status flip
                # below is reversed.
                _skip_tag_undo = (
                    entry['action_type'] == 'prediction_accept'
                    and old_meta.get('no_tag')
                )
                kw = self.conn.execute("SELECT name FROM keywords WHERE id = ?",
                                       (int(entry['new_value']),)).fetchone()
                if not _skip_tag_undo:
                    self.untag_photo(pid, int(entry['new_value']))
                    if kw:
                        self.remove_pending_changes(pid, 'keyword_add', kw['name'])
                if entry['action_type'] == 'keyword_add':
                    self._restore_edit_prediction_status(old_meta)
                    # Predicted-only relabels (no prior species tag)
                    # record their action as `keyword_add` but still
                    # carry a `curation` payload when the photo held
                    # highlight/representative rows under other species.
                    # Restore those rows here too, mirroring the
                    # `species_replace` undo path.
                    if kw:
                        self._restore_relabel_curation(
                            entry['workspace_id'], pid, kw['name'],
                            old_meta.get('curation'),
                        )
                if entry['action_type'] == 'prediction_accept' and old_val:
                    pred_ids = self._edit_prediction_ids(old_meta, old_val)
                    if not pred_ids:
                        continue
                    ws = self._ws_id()
                    # Restore predictions to pre-accept state. Scope by
                    # labels_fingerprint too — without it, undoing an accept
                    # in one label set would flip statuses of predictions
                    # produced under a different fingerprint and could
                    # promote the wrong fingerprint's top-confidence row
                    # back to 'pending'.
                    #
                    # Accept-subject no-tag accepts can span multiple
                    # classifier models on one detection, so iterate and
                    # dedupe by (detection, model, fingerprint) so each
                    # unique sibling scope is reset exactly once.
                    seen_scopes = set()
                    touched = False
                    for pred_id in pred_ids:
                        pred_row = self.conn.execute(
                            """SELECT detection_id, classifier_model AS model,
                                      labels_fingerprint
                               FROM predictions WHERE id = ?""",
                            (pred_id,),
                        ).fetchone()
                        if not pred_row:
                            continue
                        scope = (
                            pred_row["detection_id"], pred_row["model"],
                            pred_row["labels_fingerprint"],
                        )
                        if scope in seen_scopes:
                            continue
                        seen_scopes.add(scope)
                        # Identify every sibling prediction for
                        # (detection, classifier_model, labels_fingerprint).
                        siblings = self.conn.execute(
                            """SELECT id, confidence FROM predictions
                               WHERE detection_id = ?
                                 AND classifier_model = ?
                                 AND labels_fingerprint = ?
                               ORDER BY confidence DESC""",
                            scope,
                        ).fetchall()
                        # Flip any accepted/rejected review rows in this
                        # workspace back to 'alternative' — scoped to the
                        # same fingerprint so other label sets' statuses
                        # are preserved.
                        self.conn.execute(
                            """UPDATE prediction_review SET status = 'alternative',
                                                          reviewed_at = datetime('now')
                               WHERE workspace_id = ?
                                 AND status IN ('accepted', 'rejected')
                                 AND prediction_id IN (
                                    SELECT id FROM predictions
                                    WHERE detection_id = ?
                                      AND classifier_model = ?
                                      AND labels_fingerprint = ?
                                 )""",
                            (ws, *scope),
                        )
                        # Promote highest-confidence sibling back to 'pending'
                        # in this workspace.
                        if siblings:
                            top_id = siblings[0]["id"]
                            self.conn.execute(
                                """INSERT INTO prediction_review
                                     (prediction_id, workspace_id, status, reviewed_at)
                                   VALUES (?, ?, 'pending', datetime('now'))
                                   ON CONFLICT(prediction_id, workspace_id)
                                   DO UPDATE SET status = 'pending',
                                                 reviewed_at = datetime('now')""",
                                (top_id, ws),
                            )
                        touched = True
                    if touched:
                        self.conn.commit()
            elif entry['action_type'] == 'keyword_remove':
                self.tag_photo(pid, int(entry['new_value']))
                kw = self.conn.execute("SELECT name FROM keywords WHERE id = ?",
                                       (int(entry['new_value']),)).fetchone()
                if kw:
                    # Symmetric with `_queue_keyword_remove`: the original
                    # remove either queued a `keyword_remove` or, when a
                    # not-yet-synced `keyword_add` was pending, cancelled
                    # that add. Reversing needs to restore whichever side
                    # the remove touched — otherwise an add → remove → undo
                    # flow leaves the tag on the photo with no pending
                    # sidecar write, and the restored keyword never syncs.
                    cancelled = self.remove_pending_changes(
                        pid, 'keyword_remove', kw['name']
                    )
                    if cancelled == 0:
                        self.queue_change(pid, 'keyword_add', kw['name'])
            elif entry['action_type'] == 'species_replace':
                # Atomic swap: the edit replaced old_value's species with
                # new_value's. Undo untags the new species and retags the
                # old one, symmetrically reversing the pending-change queue.
                old_meta = self._edit_old_value_meta(old_val)
                new_kid = int(item['new_value']) if item['new_value'] else None
                old_kids = old_meta.get("keyword_ids") or []
                new_kw_name = None
                if new_kid:
                    self.untag_photo(pid, new_kid)
                    new_kw = self.conn.execute(
                        "SELECT name FROM keywords WHERE id = ?", (new_kid,)
                    ).fetchone()
                    if new_kw:
                        new_kw_name = new_kw['name']
                        cancelled = self.remove_pending_changes(
                            pid, 'keyword_add', new_kw['name']
                        )
                        if cancelled == 0:
                            self.queue_change(pid, 'keyword_remove', new_kw['name'])
                for old_kid in old_kids:
                    self.tag_photo(pid, old_kid)
                    old_kw = self.conn.execute(
                        "SELECT name FROM keywords WHERE id = ?", (old_kid,)
                    ).fetchone()
                    if old_kw:
                        cancelled = self.remove_pending_changes(
                            pid, 'keyword_remove', old_kw['name']
                        )
                        if cancelled == 0:
                            self.queue_change(pid, 'keyword_add', old_kw['name'])
                # Restore any species_highlights / photo_preferences rows the
                # original relabel migrated to `new_kw_name`. Without this,
                # the photo is back in its old species bucket but the
                # curated Highlight/Representative rows stay stranded under
                # the new species. See PR #1161.
                if new_kw_name:
                    self._restore_relabel_curation(
                        entry['workspace_id'], pid, new_kw_name,
                        old_meta.get('curation'),
                    )
                self._restore_edit_prediction_status(old_meta)

    def _apply_redo(self, entry, items):
        """Re-apply the effects of an undone edit entry."""
        for item in items:
            new_val = item['new_value']
            pid = item['photo_id']
            if entry['action_type'] == 'rating':
                # Edit history is already workspace-scoped; skip re-verification
                self.update_photo_rating(pid, int(new_val) if new_val else 0, verify_workspace=False)
                old_val = item['old_value']
                if old_val != new_val:
                    self.remove_pending_changes(pid, 'rating', old_val)
                    self.queue_change(pid, 'rating', new_val)
            elif entry['action_type'] == 'flag':
                self.update_photo_flag(pid, new_val, verify_workspace=False)
                self.queue_flag_change_if_enabled(pid, new_val)
            elif entry['action_type'] == 'wildlife_excluded':
                self.update_photo_wildlife_excluded(
                    pid, new_val == "1", verify_workspace=False
                )
            elif entry['action_type'] == 'color_label':
                if new_val:
                    self.set_color_label(pid, new_val)
                else:
                    self.remove_color_label(pid)
            elif entry['action_type'] == 'edit_recipe':
                self.set_photo_edit_recipe(
                    pid,
                    new_val if new_val else None,
                    verify_workspace=False,
                )
            elif entry['action_type'] in ('keyword_add', 'prediction_accept'):
                old_meta = self._edit_old_value_meta(item['old_value'])
                # Symmetric with the undo branch: a ``no_tag``
                # prediction_accept records only the status flip, so
                # redo must not re-tag or re-queue a keyword that was
                # never touched — the photo already carried the species
                # through an equivalent hierarchical/root row.
                _skip_tag_redo = (
                    entry['action_type'] == 'prediction_accept'
                    and old_meta.get('no_tag')
                )
                kw = self.conn.execute("SELECT name FROM keywords WHERE id = ?",
                                       (int(entry['new_value']),)).fetchone()
                if not _skip_tag_redo:
                    self.tag_photo(pid, int(entry['new_value']))
                    if kw:
                        self.queue_change(pid, 'keyword_add', kw['name'])
                if entry['action_type'] == 'keyword_add':
                    self._reject_edit_prediction(old_meta)
                    # Mirror of the `keyword_add` undo branch: predicted-
                    # only relabels record curation on `keyword_add`, so
                    # redo must re-apply it here rather than only in
                    # `species_replace`.
                    if kw:
                        self._reapply_relabel_curation(
                            entry['workspace_id'], pid, kw['name'],
                            old_meta.get('curation'),
                        )
                if entry['action_type'] == 'prediction_accept' and item['old_value']:
                    pred_ids = self._edit_prediction_ids(old_meta, item['old_value'])
                    if not pred_ids:
                        continue
                    ws = self._ws_id()
                    # Accept-subject no-tag accepts can span multiple
                    # classifier models on one detection, so re-accept
                    # every recorded id and reject its siblings within
                    # that classifier's fingerprint scope. Track which
                    # ids acted as the accepted row per scope so we
                    # don't reject a prediction that was itself part of
                    # the original accept batch.
                    accepted_by_scope = {}
                    for pred_id in pred_ids:
                        pred_row = self.conn.execute(
                            """SELECT detection_id, classifier_model AS model,
                                      labels_fingerprint
                               FROM predictions WHERE id = ?""",
                            (pred_id,),
                        ).fetchone()
                        if not pred_row:
                            continue
                        scope = (
                            pred_row["detection_id"], pred_row["model"],
                            pred_row["labels_fingerprint"],
                        )
                        self.update_prediction_status(pred_id, 'accepted')
                        accepted_by_scope.setdefault(scope, set()).add(pred_id)
                    for scope, accepted_ids in accepted_by_scope.items():
                        placeholders = ",".join("?" * len(accepted_ids))
                        # Re-reject siblings, scoped to the same
                        # labels_fingerprint so the redo matches the
                        # original accept's scope and doesn't touch
                        # predictions from other label sets. Exclude
                        # every id that was itself accepted in this batch.
                        sibs = self.conn.execute(
                            f"""SELECT pr.id FROM predictions pr
                               LEFT JOIN prediction_review pr_rev
                                 ON pr_rev.prediction_id = pr.id
                                AND pr_rev.workspace_id = ?
                               WHERE pr.detection_id = ?
                                 AND pr.classifier_model = ?
                                 AND pr.labels_fingerprint = ?
                                 AND pr.id NOT IN ({placeholders})
                                 AND COALESCE(pr_rev.status, 'pending')
                                     IN ('pending', 'alternative')""",
                            (ws, *scope, *accepted_ids),
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
                    if accepted_by_scope:
                        self.conn.commit()
            elif entry['action_type'] == 'keyword_remove':
                self.untag_photo(pid, int(entry['new_value']))
                kw = self.conn.execute("SELECT name FROM keywords WHERE id = ?",
                                       (int(entry['new_value']),)).fetchone()
                if kw:
                    # Mirror the undo path: if undo re-queued a
                    # `keyword_add`, redo should cancel it rather than
                    # stack a conflicting `keyword_remove` alongside it.
                    cancelled = self.remove_pending_changes(
                        pid, 'keyword_add', kw['name']
                    )
                    if cancelled == 0:
                        self.queue_change(pid, 'keyword_remove', kw['name'])
            elif entry['action_type'] == 'species_replace':
                # Re-apply the swap: untag old, retag new, mirror pending queue.
                old_meta = self._edit_old_value_meta(item['old_value'])
                new_kid = int(new_val) if new_val else None
                old_kids = old_meta.get("keyword_ids") or []
                new_kw_name = None
                for old_kid in old_kids:
                    self.untag_photo(pid, old_kid)
                    old_kw = self.conn.execute(
                        "SELECT name FROM keywords WHERE id = ?", (old_kid,)
                    ).fetchone()
                    if old_kw:
                        cancelled = self.remove_pending_changes(
                            pid, 'keyword_add', old_kw['name']
                        )
                        if cancelled == 0:
                            self.queue_change(pid, 'keyword_remove', old_kw['name'])
                if new_kid:
                    self.tag_photo(pid, new_kid)
                    new_kw = self.conn.execute(
                        "SELECT name FROM keywords WHERE id = ?", (new_kid,)
                    ).fetchone()
                    if new_kw:
                        new_kw_name = new_kw['name']
                        cancelled = self.remove_pending_changes(
                            pid, 'keyword_remove', new_kw['name']
                        )
                        if cancelled == 0:
                            self.queue_change(pid, 'keyword_add', new_kw['name'])
                if new_kw_name:
                    self._reapply_relabel_curation(
                        entry['workspace_id'], pid, new_kw_name,
                        old_meta.get('curation'),
                    )
                self._reject_edit_prediction(old_meta)

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
        if not curation:
            return
        hl_prev = curation.get("hl_prev") or []
        pref_prev = curation.get("pref_prev") or []
        rep_prev = curation.get("rep_prev") or []
        for hl in hl_prev:
            # Newer relabels record {species, rank, dst_existed}; entries
            # from older relabels (before PR #1161 landed rank capture)
            # are plain species-name strings and fall back to
            # append-at-end with dst_existed=False.
            if isinstance(hl, dict):
                old_species = hl.get("species")
                target_rank = hl.get("rank")
                dst_existed = bool(hl.get("dst_existed", False))
            else:
                old_species = hl
                target_rank = None
                dst_existed = False
            if not old_species or old_species == new_species:
                continue
            if not dst_existed:
                # Only delete the destination row when the relabel
                # actually created it. If the photo was already
                # highlighted at `new_species` before the relabel,
                # rename_species_highlights_species skipped inserting a
                # duplicate — undo must not remove the pre-existing row.
                self.conn.execute(
                    """DELETE FROM species_highlights
                       WHERE workspace_id = ? AND species = ? AND photo_id = ?""",
                    (workspace_id, new_species, photo_id),
                )
            existing = self.conn.execute(
                """SELECT 1 FROM species_highlights
                   WHERE workspace_id = ? AND species = ? AND photo_id = ?""",
                (workspace_id, old_species, photo_id),
            ).fetchone()
            if existing:
                continue
            if target_rank is None:
                rank = int(self.conn.execute(
                    """SELECT COALESCE(MAX(rank), 0) AS max_rank
                       FROM species_highlights
                       WHERE workspace_id = ? AND species = ?""",
                    (workspace_id, old_species),
                ).fetchone()["max_rank"] or 0) + 1
            else:
                try:
                    rank = int(target_rank)
                except (TypeError, ValueError):
                    rank = int(self.conn.execute(
                        """SELECT COALESCE(MAX(rank), 0) AS max_rank
                           FROM species_highlights
                           WHERE workspace_id = ? AND species = ?""",
                        (workspace_id, old_species),
                    ).fetchone()["max_rank"] or 0) + 1
            self.conn.execute(
                """INSERT INTO species_highlights
                       (workspace_id, species, photo_id, rank,
                        created_at, updated_at)
                   VALUES (?, ?, ?, ?, datetime('now'), datetime('now'))""",
                (workspace_id, old_species, photo_id, rank),
            )
        for pref in pref_prev:
            if not isinstance(pref, dict):
                continue
            purpose = pref.get("purpose")
            old_species = pref.get("species")
            dst_existed = bool(pref.get("dst_existed", False))
            rep_dst_existed = bool(pref.get("rep_dst_existed", False))
            rep_selected_order = pref.get("rep_selected_order")
            if not purpose or not old_species or old_species == new_species:
                continue
            # Only delete the (new_species, purpose) row when the relabel
            # created it. When the destination slot was already taken
            # before the relabel — either by this photo or a different
            # one — rename_photo_preferences_species's INSERT OR IGNORE
            # was ignored and no new row was written for this photo, so
            # undo must leave the destination alone.
            if not dst_existed:
                self.conn.execute(
                    """DELETE FROM photo_preferences
                       WHERE workspace_id = ? AND purpose = ?
                         AND species = ? AND photo_id = ?""",
                    (workspace_id, purpose, new_species, photo_id),
                )
            # Only delete the (new_species, photo_id) rep row when the
            # relabel created it. If the photo was already a global
            # representative for new_species before the retag — e.g. a
            # multi-species photo picked as rep for both A and B before
            # relabeling A→B — rename_species_representatives_species's
            # INSERT OR IGNORE skipped a duplicate and the destination
            # rep row is pre-existing; undo must leave it alone.
            # rep_dst_existed defaults to False for edit-history rows
            # written before this field was added, preserving the older
            # (over-eager) behavior for legacy undos.
            if not rep_dst_existed:
                self.conn.execute(
                    """DELETE FROM species_representatives
                       WHERE species = ? AND photo_id = ?""",
                    (new_species, photo_id),
                )
            # Restore the old-species preference unconditionally. The
            # previous gate on finding a `(new_species, purpose,
            # photo_id)` row skipped restore when the relabel collided
            # with a different photo holding the destination slot,
            # stranding the old species' representative.
            self.conn.execute(
                """INSERT OR IGNORE INTO photo_preferences
                       (workspace_id, purpose, species, photo_id,
                        created_at, updated_at)
                   VALUES (?, ?, ?, ?, datetime('now'), datetime('now'))""",
                (workspace_id, purpose, old_species, photo_id),
            )
            self._restore_species_representative(
                old_species, photo_id, selected_order=rep_selected_order,
            )
        for rep in rep_prev:
            if not isinstance(rep, dict):
                continue
            old_species = rep.get("species")
            dst_existed = bool(rep.get("dst_existed", False))
            rep_selected_order = rep.get("selected_order")
            if not old_species or old_species == new_species:
                continue
            # Only delete the (new_species, photo_id) rep row when the
            # relabel actually created it. If the photo was already a
            # global representative for new_species before the retag,
            # rename_species_representatives_species's INSERT OR IGNORE
            # skipped a duplicate and the destination row is pre-existing;
            # undo must leave it alone.
            if not dst_existed:
                self.conn.execute(
                    """DELETE FROM species_representatives
                       WHERE species = ? AND photo_id = ?""",
                    (new_species, photo_id),
                )
            self._restore_species_representative(
                old_species, photo_id, selected_order=rep_selected_order,
            )

    def _reapply_relabel_curation(
        self, workspace_id, photo_id, new_species, curation,
    ):
        """Redo the curation migration reversed by
        :meth:`_restore_relabel_curation`. Moves rows from each recorded
        old species back onto ``new_species``.
        """
        if not curation:
            return
        hl_prev = curation.get("hl_prev") or []
        pref_prev = curation.get("pref_prev") or []
        rep_prev = curation.get("rep_prev") or []
        for hl in hl_prev:
            # Accept both new dict form ({species, rank}) and legacy
            # string form for compatibility with older edit-history rows.
            if isinstance(hl, dict):
                old_species = hl.get("species")
            else:
                old_species = hl
            if not old_species or old_species == new_species:
                continue
            src = self.conn.execute(
                """SELECT 1 FROM species_highlights
                   WHERE workspace_id = ? AND species = ? AND photo_id = ?""",
                (workspace_id, old_species, photo_id),
            ).fetchone()
            if not src:
                continue
            self.conn.execute(
                """DELETE FROM species_highlights
                   WHERE workspace_id = ? AND species = ? AND photo_id = ?""",
                (workspace_id, old_species, photo_id),
            )
            existing = self.conn.execute(
                """SELECT 1 FROM species_highlights
                   WHERE workspace_id = ? AND species = ? AND photo_id = ?""",
                (workspace_id, new_species, photo_id),
            ).fetchone()
            if existing:
                continue
            next_rank = int(self.conn.execute(
                """SELECT COALESCE(MAX(rank), 0) AS max_rank
                   FROM species_highlights
                   WHERE workspace_id = ? AND species = ?""",
                (workspace_id, new_species),
            ).fetchone()["max_rank"] or 0) + 1
            self.conn.execute(
                """INSERT INTO species_highlights
                       (workspace_id, species, photo_id, rank,
                        created_at, updated_at)
                   VALUES (?, ?, ?, ?, datetime('now'), datetime('now'))""",
                (workspace_id, new_species, photo_id, next_rank),
            )
        for pref in pref_prev:
            if not isinstance(pref, dict):
                continue
            purpose = pref.get("purpose")
            old_species = pref.get("species")
            if not purpose or not old_species or old_species == new_species:
                continue
            rep_selected_order = pref.get("rep_selected_order")
            row = self.conn.execute(
                """SELECT 1 FROM photo_preferences
                   WHERE workspace_id = ? AND purpose = ?
                     AND species = ? AND photo_id = ?""",
                (workspace_id, purpose, old_species, photo_id),
            ).fetchone()
            if not row:
                continue
            self.conn.execute(
                """DELETE FROM photo_preferences
                   WHERE workspace_id = ? AND purpose = ?
                     AND species = ? AND photo_id = ?""",
                (workspace_id, purpose, old_species, photo_id),
            )
            self.conn.execute(
                """DELETE FROM species_representatives
                   WHERE species = ? AND photo_id = ?""",
                (old_species, photo_id),
            )
            self.conn.execute(
                """INSERT OR IGNORE INTO photo_preferences
                       (workspace_id, purpose, species, photo_id,
                        created_at, updated_at)
                   VALUES (?, ?, ?, ?, datetime('now'), datetime('now'))""",
                (workspace_id, purpose, new_species, photo_id),
            )
            # Reuse the captured pre-relabel selected_order rather than
            # allocating a fresh MAX+1. The original relabel preserved the
            # source row's order via rename_species_representatives_species,
            # so redoing must restore that same order — otherwise an
            # undo/redo round trip can promote a secondary representative
            # above a pre-existing primary for new_species.
            self._restore_species_representative(
                new_species, photo_id, selected_order=rep_selected_order,
            )
        for rep in rep_prev:
            if not isinstance(rep, dict):
                continue
            old_species = rep.get("species")
            if not old_species or old_species == new_species:
                continue
            rep_selected_order = rep.get("selected_order")
            src = self.conn.execute(
                """SELECT 1 FROM species_representatives
                   WHERE species = ? AND photo_id = ?""",
                (old_species, photo_id),
            ).fetchone()
            if not src:
                continue
            self.conn.execute(
                """DELETE FROM species_representatives
                   WHERE species = ? AND photo_id = ?""",
                (old_species, photo_id),
            )
            self._restore_species_representative(
                new_species, photo_id, selected_order=rep_selected_order,
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
        self.conn.execute(
            """DELETE FROM edit_history WHERE workspace_id = ? AND undone = 0 AND id NOT IN (
                 SELECT id FROM edit_history WHERE workspace_id = ? AND undone = 0
                 ORDER BY created_at DESC, id DESC LIMIT ?
               )""",
            (self._ws_id(), self._ws_id(), max_entries),
        )
        self.conn.commit()

    # -- Collections --

    def add_collection(self, name, rules_json):
        """Insert a smart collection. Returns the collection id."""
        cur = self.conn.execute(
            "INSERT INTO collections (name, rules, workspace_id) VALUES (?, ?, ?)",
            (name, rules_json, self._ws_id()),
        )
        self.conn.commit()
        return cur.lastrowid

    def get_collections(self):
        """Return all collections for the active workspace."""
        return self.conn.execute(
            "SELECT id, name, rules FROM collections WHERE workspace_id = ? ORDER BY name",
            (self._ws_id(),),
        ).fetchall()

    def delete_collection(self, collection_id):
        """Delete a collection."""
        self.conn.execute(
            "DELETE FROM collections WHERE id = ? AND workspace_id = ?",
            (collection_id, self._ws_id()),
        )
        self.conn.commit()

    def rename_collection(self, collection_id, new_name):
        """Rename a collection within the active workspace.

        Raises ``ValueError`` if the collection isn't in the active workspace.
        """
        ws = self._ws_id()
        cur = self.conn.execute(
            "UPDATE collections SET name = ? WHERE id = ? AND workspace_id = ?",
            (new_name, collection_id, ws),
        )
        if cur.rowcount == 0:
            raise ValueError("collection not found")
        self.conn.commit()

    def duplicate_collection(self, collection_id):
        """Copy a collection (name + rules) within the active workspace.

        The new collection's name is ``"{original} (copy)"``; if that name is
        already taken, append an incrementing counter like ``"(copy 2)"``.
        Rules are copied verbatim, which means static collections (photo_ids
        rules) keep their memberships.

        Returns the new collection id. Raises ``ValueError`` if the source
        collection isn't in the active workspace.
        """
        ws = self._ws_id()
        row = self.conn.execute(
            "SELECT name, rules FROM collections WHERE id = ? AND workspace_id = ?",
            (collection_id, ws),
        ).fetchone()
        if not row:
            raise ValueError("collection not found")

        existing = {
            c["name"]
            for c in self.conn.execute(
                "SELECT name FROM collections WHERE workspace_id = ?", (ws,)
            ).fetchall()
        }
        base = f"{row['name']} (copy)"
        new_name = base
        n = 2
        while new_name in existing:
            new_name = f"{row['name']} (copy {n})"
            n += 1

        cur = self.conn.execute(
            "INSERT INTO collections (name, rules, workspace_id) VALUES (?, ?, ?)",
            (new_name, row["rules"], ws),
        )
        self.conn.commit()
        return cur.lastrowid

    def create_new_images_snapshot(self, file_paths):
        """Persist a snapshot of new-image file paths for the active workspace.

        Returns the new snapshot id. An empty path list is allowed — the caller
        decides how to handle zero-file snapshots (the pipeline short-circuits).
        """
        ws_id = self._ws_id()
        unique_paths = sorted(set(file_paths or []))
        cur = self.conn.execute(
            "INSERT INTO new_image_snapshots (workspace_id, created_at, file_count) "
            "VALUES (?, datetime('now'), ?)",
            (ws_id, len(unique_paths)),
        )
        snap_id = cur.lastrowid
        if unique_paths:
            self.conn.executemany(
                "INSERT INTO new_image_snapshot_files (snapshot_id, file_path) VALUES (?, ?)",
                [(snap_id, p) for p in unique_paths],
            )
        self.conn.commit()
        return snap_id

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
        row = self.conn.execute(
            "SELECT id, workspace_id, created_at, file_count "
            "FROM new_image_snapshots WHERE id = ? AND workspace_id = ?",
            (snapshot_id, self._ws_id()),
        ).fetchone()
        if row is None:
            return None
        paths = [
            r["file_path"]
            for r in self.conn.execute(
                "SELECT file_path FROM new_image_snapshot_files WHERE snapshot_id = ? "
                "ORDER BY file_path",
                (snapshot_id,),
            ).fetchall()
        ]
        return {
            "id": row["id"],
            "workspace_id": row["workspace_id"],
            "created_at": row["created_at"],
            "file_count": row["file_count"],
            "file_paths": paths,
        }

    def _build_collection_query(self, collection_id, include_offline_folders=False):
        """Build SQL clauses from collection rules.

        Returns (folder_join, join_clause, where, params) or None if collection
        not found. Pass ``include_offline_folders=True`` for metadata-only
        callers (Dashboard scope) that keep offline photos in their totals.
        """
        row = self.conn.execute(
            "SELECT rules FROM collections WHERE id = ? AND workspace_id = ?",
            (collection_id, self._ws_id()),
        ).fetchone()
        if not row:
            return None

        rules = json.loads(row["rules"])
        return self._build_query_from_rules(
            rules, include_offline_folders=include_offline_folders,
        )

    def _build_query_from_rules(self, rules, include_offline_folders=False):
        """Build SQL clauses from a smart-collection rule tree.

        Returns (folder_join, join_clause, where, params). Raises ValueError on
        malformed input — callers that accept rules from untrusted sources
        (e.g. the live-preview API) should catch and surface a 400.

        By default the folder join filters to accessible folders
        (``status IN ('ok', 'partial')``), matching what Browse and pipeline
        callers need. Pass ``include_offline_folders=True`` for metadata-only
        callers (e.g. Dashboard totals) that count photos even when their
        storage is currently missing.

        Backward compatibility: the original collection format was a flat list
        of rule objects, implicitly combined with AND. Newer collections may use
        a grouped tree:

            {"mode": "all"|"any"|"none", "rules": [rule_or_group, ...]}
        """
        if isinstance(rules, list):
            root = {"mode": "all", "rules": rules}
        elif isinstance(rules, dict) and "rules" in rules:
            root = rules
        else:
            raise ValueError("rules must be a list or group object")

        def _is_scalar(value):
            return value is None or isinstance(value, str | int | float | bool)

        def _validate_node(node):
            if not isinstance(node, dict):
                raise ValueError("each rule must be an object")
            if "rules" in node and "field" not in node:
                mode = node.get("mode", "all")
                if mode not in ("all", "any", "none"):
                    raise ValueError("rule group mode must be all, any, or none")
                children = node.get("rules")
                if not isinstance(children, list):
                    raise ValueError("rule group rules must be a list")
                for child in children:
                    _validate_node(child)
                return
            if "field" not in node:
                raise ValueError("each rule must have a 'field'")
            field = node.get("field")
            op = node.get("op")
            value = node.get("value")
            if op == "recent":
                if not isinstance(value, dict):
                    raise ValueError("recent rules take a {n, unit} object value")
                n = value.get("n")
                if not isinstance(n, int) or isinstance(n, bool) or n < 1:
                    raise ValueError("recent n must be a positive integer")
                if value.get("unit", "days") not in ("days", "weeks", "months", "years"):
                    raise ValueError("recent unit must be days, weeks, months, or years")
                return
            list_allowed = (
                field == "photo_ids"
                or op in ("in", "not_in", "between")
            )
            if isinstance(value, list):
                if not list_allowed:
                    raise ValueError(f"rule field {field!r} does not accept a list value")
                if op == "between" and len(value) != 2:
                    raise ValueError("between rules take a [low, high] value")
                for item in value:
                    if not _is_scalar(item):
                        raise ValueError("rule list values must be scalars")
                return
            if op in ("in", "not_in", "between"):
                raise ValueError(f"rule op {op!r} requires a list value")
            if not _is_scalar(value):
                raise ValueError("rule value must be a scalar")

        def _truthy(value):
            return value is True or value == 1 or value == "1" or value == "true"

        def _falsey(value):
            return value is False or value == 0 or value == "0" or value == "false"

        # Lazily read the workspace-effective detector_confidence floor —
        # cfg.load() reads a JSON file, so only pay for it when a prediction
        # rule actually references it. Cached across every _prediction_exists
        # call in the same query so a rule tree with multiple prediction
        # predicates doesn't reread the config per predicate.
        _conf_cache = {}

        def _min_detector_conf():
            if "value" not in _conf_cache:
                import config as cfg
                _conf_cache["value"] = float(
                    self.get_effective_config(cfg.load()).get(
                        "detector_confidence", 0.2
                    )
                )
            return _conf_cache["value"]

        def _boolean_predicate(has_sql, op, value, params=None):
            """Boolean-typed leaf builder used by every ``BOOLEAN_OPS`` field.

            Guards ``op`` so a malformed rule such as
            ``{"field":"has_edits","op":"contains","value":1}`` surfaces as a
            ``ValueError`` (→ 400 at the API layer) instead of the silent
            match the numeric branch already rejects — otherwise a client
            can slip past the registry by inventing an op. Also guards
            ``value``: without this, ``{"field":"has_gps","op":"is",
            "value":"yes"}`` would fall to the negative branch (``_truthy``
            only accepts True/1/"1"/"true") and quietly return the ``is
            false`` predicate, flipping the client's intent.
            """
            if op not in ("equals", "is", "is not"):
                raise ValueError(f"unsupported boolean rule op: {op!r}")
            if _truthy(value):
                want_true = True
            elif _falsey(value):
                want_true = False
            else:
                raise ValueError(
                    f"boolean rule value must be true/false, got {value!r}"
                )
            if op == "is not":
                want_true = not want_true
            return (has_sql if want_true else f"NOT ({has_sql})"), list(params or [])

        def _numeric_condition(column, op, value, allow_null=False):
            if op == ">=":
                return f"{column} >= ?", [value]
            if op == "<=":
                return f"{column} <= ?", [value]
            if op == ">":
                return f"{column} > ?", [value]
            if op == "<":
                return f"{column} < ?", [value]
            if op == "between":
                return f"({column} >= ? AND {column} <= ?)", [value[0], value[1]]
            if op in ("equals", "is"):
                return f"{column} = ?", [value]
            if op == "is not":
                prefix = f"{column} IS NULL OR " if allow_null else ""
                return f"({prefix}{column} != ?)", [value]
            # Reject the op instead of silently emitting a constant-false
            # predicate — /api/photos/query catches ValueError and returns
            # 400, so a malformed rule like ``{"field":"file_size","op":
            # "contains","value":1}`` surfaces as a validation error instead
            # of a 200 with an empty result set.
            raise ValueError(f"unsupported numeric rule op: {op!r}")

        def _text_condition(column, op, value, case_sensitive=False):
            """Text-field predicate. Returns (cond, params) or None for an
            unrecognized op (caller falls through to the unsupported-rule
            error). Case-insensitive by default, matching SQLite LIKE; the
            case-sensitive variants avoid PRAGMA case_sensitive_like, which
            cannot be scoped to one query.
            """
            text = str(value if value is not None else "")
            if op in ("contains", "not_contains"):
                if case_sensitive:
                    cond, params = f"instr({column}, ?) > 0", [text]
                else:
                    cond = f"{column} LIKE ? ESCAPE '\\'"
                    params = [f"%{_escape_like(text)}%"]
                if op == "not_contains":
                    return f"({column} IS NULL OR NOT ({cond}))", params
                return cond, params
            if op in ("starts_with", "ends_with"):
                if not text:
                    return "1", []
                if case_sensitive:
                    if op == "starts_with":
                        return f"substr({column}, 1, {len(text)}) = ?", [text]
                    return f"substr({column}, -{len(text)}) = ?", [text]
                pattern = (
                    _escape_like(text) + "%" if op == "starts_with"
                    else "%" + _escape_like(text)
                )
                return f"{column} LIKE ? ESCAPE '\\'", [pattern]
            if op in ("equals", "is"):
                if case_sensitive:
                    return f"{column} = ?", [text]
                return f"LOWER({column}) = LOWER(?)", [text]
            if op == "is not":
                if case_sensitive:
                    return f"({column} IS NULL OR {column} != ?)", [text]
                return f"({column} IS NULL OR LOWER({column}) != LOWER(?))", [text]
            return None

        def _keyword_exists(predicate, predicate_params):
            return (
                "EXISTS (SELECT 1 FROM photo_keywords pk "
                "JOIN keywords k ON k.id = pk.keyword_id "
                f"WHERE pk.photo_id = p.id AND {predicate})",
                list(predicate_params),
            )

        def _keyword_not_exists(predicate, predicate_params):
            return (
                "NOT EXISTS (SELECT 1 FROM photo_keywords pk "
                "JOIN keywords k ON k.id = pk.keyword_id "
                f"WHERE pk.photo_id = p.id AND {predicate})",
                list(predicate_params),
            )

        def _prediction_exists(predicate, predicate_params, review_join=False):
            # Pin to the most recent labels_fingerprint per
            # (detection_id, classifier_model), matching how the dashboard
            # (get_top_prediction_for_photo, prediction_status queries)
            # decides which prediction row is "current". Without this pin,
            # a rerun classifier with a new fingerprint leaves an older
            # accepted row around and universal-filter rules
            # (prediction_status is accepted, prediction_confidence >= X,
            # classifier_model is Y, taxonomy_* is Z) still match the
            # stale prediction — disagreeing with the review UI that only
            # shows the current fingerprint.
            fingerprint_pin = (
                " AND pred.labels_fingerprint = ("
                "SELECT pr2.labels_fingerprint FROM predictions pr2 "
                "WHERE pr2.detection_id = pred.detection_id "
                "AND pr2.classifier_model = pred.classifier_model "
                "ORDER BY pr2.created_at DESC, pr2.id DESC LIMIT 1)"
            )
            # Always join prediction_review so alternatives can be filtered
            # out below — the review_join parameter is retained for callers
            # that also read prv.status in their predicate.
            review = (
                " LEFT JOIN prediction_review prv "
                "ON prv.prediction_id = pred.id AND prv.workspace_id = ?"
            )
            # Filters that represent the *displayed* prediction (confidence,
            # classifier_model, taxonomy_*, plus the status predicates that
            # already read prv.status) must ignore runner-up rows stored
            # with prv.status = 'alternative'. Otherwise a top prediction
            # at 0.95 with an alternative at 0.10 would satisfy
            # prediction_confidence <= 0.2 even though /api/predictions
            # (app.py:12386-12388) drops alternatives from top-level results.
            not_alternative = (
                " AND COALESCE(prv.status, 'pending') != 'alternative'"
            )
            # Gate by the workspace-effective detector_confidence floor.
            # /api/photos/query's response goes through
            # get_detections_for_photos() (which applies this threshold),
            # and the dashboard prediction counters + query_move_rule_matches
            # apply it too. Without gating here, a below-threshold hidden
            # detection whose accepted/high-confidence prediction is stale
            # from a previous run would still satisfy prediction_status,
            # prediction_confidence, classifier_model, and taxonomy_* rules,
            # so the universal filter would include a photo the Browse view
            # shows with no visible detection context.
            conf_filter = " AND det.detector_confidence >= ?"
            params = (
                [self._ws_id()]
                + [_min_detector_conf()]
                + list(predicate_params)
            )
            return (
                "EXISTS (SELECT 1 FROM detections det "
                "JOIN predictions pred ON pred.detection_id = det.id"
                f"{review} WHERE det.photo_id = p.id{conf_filter}"
                f"{not_alternative} "
                f"AND {predicate}{fingerprint_pin})",
                params,
            )

        def _build_leaf(rule):
            field = rule["field"]
            op = rule.get("op", "")
            value = rule.get("value")

            if field == "all":
                # Sentinel for defaults like "All Photos" — adds no condition,
                # so the workspace-folder join alone determines matches.
                return None, []
            if field == "photo_ids":
                ids = value if isinstance(value, list) else []
                if not ids:
                    return "0", []
                # Inline integer ids as SQL literals instead of binding one
                # parameter per id — a static collection created from a large
                # selection would otherwise exceed SQLite's bound-parameter
                # cap on every query against the collection, permanently. A
                # temp table (as _scope_clause uses) isn't composable here:
                # the returned clause may be embedded in queries that run
                # later and repeatedly. Only ints are inlined (injection-safe);
                # any non-int leftovers (malformed rules, rare) keep the
                # parameter-binding path so comparison semantics are unchanged.
                int_ids = [
                    v for v in ids
                    if isinstance(v, int) and not isinstance(v, bool)
                ]
                other = [
                    v for v in ids
                    if not (isinstance(v, int) and not isinstance(v, bool))
                ]
                parts = []
                params = []
                if int_ids:
                    parts.append(
                        "p.id IN (%s)" % ",".join(str(v) for v in int_ids)
                    )
                if other:
                    placeholders = ",".join("?" for _ in other)
                    parts.append(f"p.id IN ({placeholders})")
                    params = list(other)
                if len(parts) == 1:
                    return parts[0], params
                return "(" + " OR ".join(parts) + ")", params
            if field in ("rating", "quality_score", "sharpness",
                         "subject_sharpness", "noise_estimate",
                         "crop_complete"):
                column = "p.subject_tenengrad" if field == "subject_sharpness" else f"p.{field}"
                return _numeric_condition(column, op, value, allow_null=True)
            if field == "keyword":
                if op == "contains":
                    # Escape LIKE metacharacters so ``%``/``_`` in the value
                    # stay literal — otherwise ``keyword contains "%"`` would
                    # match every keyworded photo, breaking parity with the
                    # escaped filename/camera/species text rules and the
                    # escaped folder/keyword typeahead paths.
                    like = f"%{_escape_like(str(value or ''))}%"
                    return _keyword_exists("k.name LIKE ? ESCAPE '\\'", [like])
                if op == "not_contains":
                    like = f"%{_escape_like(str(value or ''))}%"
                    return _keyword_not_exists("k.name LIKE ? ESCAPE '\\'", [like])
                if op in ("equals", "is"):
                    return _keyword_exists("k.name = ?", [value])
                if op == "is not":
                    return _keyword_not_exists("k.name = ?", [value])
            if field == "folder":
                # Match the folder itself plus separator-delimited descendants.
                # A bare prefix LIKE would also match siblings ("/photos/2023"
                # matching "/photos/2023-trip") and treat _/% in the value as
                # wildcards. Stored folder paths use the platform separator
                # (``str(Path(...))`` in scanner.scan — backslashes on Windows),
                # so normalize both sides to forward slashes the same way
                # ``_folder_subtree_ids_by_path`` does; otherwise a Windows
                # library's ``C:\Photos\Birds`` row never matches a rule whose
                # LIKE pattern hard-codes ``C:/Photos/%``.
                base = _path_for_subtree_match(str(value or ""))
                subtree_params = [base, _escape_like(base) + "/%"]
                norm = "REPLACE(f.path, '\\', '/')"
                # Legacy folder collections were saved with op "is"/"is not"
                # before the rule vocabulary switched to "under"/"not_under".
                # Treat the old ops as aliases so those rows keep resolving
                # (a single unrecognized rule used to raise ValueError and 500
                # the whole /api/collections list). "is" always meant the
                # folder and its descendants, which is exactly "under".
                if op in ("under", "is", "equals"):
                    return f"({norm} = ? OR {norm} LIKE ? ESCAPE '\\')", subtree_params
                if op in ("not_under", "is not"):
                    return (
                        f"(f.path IS NULL OR ({norm} != ? AND {norm} NOT LIKE ? ESCAPE '\\'))",
                        subtree_params,
                    )
            if field == "flag":
                # NULL means unflagged for legacy rows (older ingests, undo
                # paths). Browse's inline filter already COALESCEs; the rule
                # engine must agree or the Unflagged chip silently drops
                # those rows once Browse moves onto this path.
                col = "COALESCE(p.flag, 'none')"
                if op in ("equals", "is"):
                    return f"{col} = ?", [value]
                if op == "is not":
                    return f"{col} != ?", [value]
                if op in ("in", "not_in"):
                    values = list(value or [])
                    if not values:
                        # in [] matches nothing; not_in [] excludes nothing.
                        # Emit constants (not None) so any/none group
                        # semantics stay exact.
                        return ("0" if op == "in" else "1"), []
                    placeholders = ",".join("?" * len(values))
                    negate = "NOT " if op == "not_in" else ""
                    return f"{col} {negate}IN ({placeholders})", values
            if field == "color_label":
                def _color_exists(colors):
                    placeholders = ",".join("?" * len(colors))
                    return (
                        "EXISTS (SELECT 1 FROM photo_color_labels pcl "
                        "WHERE pcl.photo_id = p.id AND pcl.workspace_id = ? "
                        f"AND pcl.color IN ({placeholders}))",
                        [self._ws_id(), *colors],
                    )
                if op in ("equals", "is"):
                    return _color_exists([value])
                if op == "is not":
                    cond, params = _color_exists([value])
                    return f"NOT {cond}", params
                if op in ("in", "not_in"):
                    values = list(value or [])
                    if not values:
                        return ("0" if op == "in" else "1"), []
                    cond, params = _color_exists(values)
                    if op == "not_in":
                        # "is not one of" = carries no label from the set;
                        # unlabeled photos match, mirroring "is not".
                        return f"NOT {cond}", params
                    return cond, params
            if field == "has_species":
                # Mirror the species-filter / get_species_keywords_for_photos
                # eligibility: a keyword counts as a species when either the
                # legacy ``is_species`` flag is set OR it is a taxonomy row,
                # AND its linked taxon (if any) has rank ``species``. Falling
                # back to ``k.is_species = 1`` would exclude photos whose
                # species is a ``type='taxonomy', is_species=0`` row —
                # exactly the shape upgraded libraries store — so the "Has
                # species" chip would disagree with everywhere the species
                # is actually shown.
                has_species_exists = (
                    "EXISTS (SELECT 1 FROM photo_keywords pk "
                    "JOIN keywords k ON k.id = pk.keyword_id "
                    "LEFT JOIN taxa t ON t.id = k.taxon_id "
                    "WHERE pk.photo_id = p.id "
                    "AND (k.is_species = 1 OR k.type = 'taxonomy') "
                    "AND (t.rank = 'species' OR t.rank IS NULL))"
                )
                if op in ("equals", "is") and _falsey(value):
                    return f"NOT {has_species_exists}", []
                if op in ("equals", "is") and _truthy(value):
                    return has_species_exists, []
            if field == "has_subject":
                subject_types = sorted(self.get_subject_types())
                if not subject_types:
                    # No subject types configured → no photo can have a
                    # subject. Route through ``_boolean_predicate`` so a
                    # malformed rule like
                    # ``{"field":"has_subject","op":"contains","value":1}``
                    # raises ValueError (→ 400) in this configuration too,
                    # instead of silently dropping the rule via
                    # ``return None, []``.
                    return _boolean_predicate("0", op, value)
                placeholders = ",".join("?" * len(subject_types))
                type_clause = f"k.type IN ({placeholders})"
                if "taxonomy" in subject_types:
                    type_clause = f"({type_clause} OR k.is_species = 1)"
                exists, params = _keyword_exists(type_clause, subject_types)
                return _boolean_predicate(exists, op, value, params)
            if field == "wildlife_excluded":
                excluded = "p.wildlife_excluded = 1"
                if op in ("equals", "is"):
                    return (excluded if _truthy(value) else f"NOT ({excluded})"), []
                if op == "is not":
                    return (f"NOT ({excluded})" if _truthy(value) else excluded), []
            if field == "keyword_count":
                expr = "(SELECT COUNT(*) FROM photo_keywords pk2 WHERE pk2.photo_id = p.id)"
                return _numeric_condition(expr, op, value)
            if field == "timestamp":
                if op == "between" and isinstance(value, list) and len(value) == 2:
                    return "p.timestamp >= ? AND p.timestamp <= ?", [
                        value[0],
                        _rule_upper_bound(value[1]),
                    ]
                if op == "recent_days":
                    # ``strftime`` with an explicit ``T`` separator so the
                    # cutoff format matches the scanner's ``dt.isoformat()``
                    # storage. SQLite's plain ``datetime('now', ?)`` returns
                    # ``YYYY-MM-DD HH:MM:SS`` (space separator); comparing a
                    # T-separated timestamp against a space-separated cutoff
                    # is a lexical mismatch where ``T`` (0x54) sorts after
                    # ``' '`` (0x20), so any photo on the cutoff day would
                    # spuriously satisfy ``>=`` even when its clock time is
                    # earlier than the cutoff's clock time.
                    return (
                        "p.timestamp >= strftime('%Y-%m-%dT%H:%M:%S', 'now', ?)",
                        [f"-{value} days"],
                    )
                if op == "recent":
                    n = value["n"]
                    unit = value.get("unit", "days")
                    modifier = {
                        "days": f"-{n} days",
                        "weeks": f"-{n * 7} days",
                        "months": f"-{n} months",
                        "years": f"-{n} years",
                    }[unit]
                    return (
                        "p.timestamp >= strftime('%Y-%m-%dT%H:%M:%S', 'now', ?)",
                        [modifier],
                    )
                # Comparison ops for "on or after / before" style rules.
                # Timestamps are ISO strings, so lexical compare is correct;
                # date-only bounds are inclusive of the named day on the
                # upper side, matching the Browse date_to behavior.
                if op == ">=":
                    return "p.timestamp >= ?", [value]
                if op == ">":
                    # Only advance a bare ``YYYY-MM-DD`` to end-of-day —
                    # ``> 2024-01-01`` means "strictly after that day".
                    # Padding an already-precise timestamp
                    # (``2024-01-01T12:00:00``) would spuriously exclude
                    # sub-second photos in the same clock second
                    # (``12:00:00.5``) that ARE strictly greater than the
                    # requested instant.
                    return "p.timestamp > ?", [_rule_upper_bound(value)]
                if op == "<=":
                    # Symmetric to ``>`` above: only pad bare dates. A
                    # precise ``<= 2024-01-01T12:00:00`` request means the
                    # exact instant, not the whole clock second — padding
                    # would spuriously *include* ``12:00:00.5`` photos
                    # that are after the requested instant.
                    return "p.timestamp <= ?", [_rule_upper_bound(value)]
                if op == "<":
                    return "p.timestamp < ?", [value]
            if field == "extension":
                if op in ("equals", "is"):
                    return "LOWER(p.extension) = LOWER(?)", [value]
                if op == "is not":
                    return "LOWER(p.extension) != LOWER(?)", [value]
                if op in ("in", "not_in"):
                    values = list(value or [])
                    if not values:
                        return ("0" if op == "in" else "1"), []
                    placeholders = ",".join("LOWER(?)" for _ in values)
                    negate = "NOT " if op == "not_in" else ""
                    return f"LOWER(p.extension) {negate}IN ({placeholders})", values
            if field in (
                "taxonomy_kingdom",
                "taxonomy_phylum",
                "taxonomy_class",
                "taxonomy_order",
                "taxonomy_family",
                "taxonomy_genus",
            ):
                col = f"pred.{field}"
                if op in ("equals", "is"):
                    return _prediction_exists(f"{col} = ?", [value])
                if op == "is not":
                    exists, params = _prediction_exists(f"{col} = ?", [value])
                    return "NOT " + exists, params
                if op == "contains":
                    return _prediction_exists(f"{col} LIKE ?", [f"%{value}%"])
            if field == "prediction_confidence":
                cond, cond_params = _numeric_condition("pred.confidence", op, value)
                return _prediction_exists(cond, cond_params)
            if field == "classifier_model":
                if op in ("equals", "is"):
                    return _prediction_exists("pred.classifier_model = ?", [value])
                if op == "is not":
                    exists, params = _prediction_exists(
                        "pred.classifier_model = ?", [value],
                    )
                    return "NOT " + exists, params
                if op == "contains":
                    # Escape LIKE metacharacters so a value like ``%`` or ``_``
                    # stays literal — matches the other advertised text
                    # contains predicates (filename, camera fields, keyword,
                    # species) and blocks ``value="%"`` from matching every
                    # classified photo.
                    like = f"%{_escape_like(str(value or ''))}%"
                    return _prediction_exists(
                        "pred.classifier_model LIKE ? ESCAPE '\\'", [like]
                    )
            if field == "prediction_status":
                if op in ("equals", "is"):
                    return _prediction_exists(
                        "COALESCE(prv.status, 'pending') = ?",
                        [value],
                        review_join=True,
                    )
                if op == "is not":
                    exists, params = _prediction_exists(
                        "COALESCE(prv.status, 'pending') = ?",
                        [value],
                        review_join=True,
                    )
                    return "NOT " + exists, params
                if op in ("in", "not_in"):
                    values = list(value or [])
                    if not values:
                        return ("0" if op == "in" else "1"), []
                    placeholders = ",".join("?" * len(values))
                    exists, params = _prediction_exists(
                        f"COALESCE(prv.status, 'pending') IN ({placeholders})",
                        values,
                        review_join=True,
                    )
                    if op == "not_in":
                        return "NOT " + exists, params
                    return exists, params
            if field == "needs_review":
                exists, params = _prediction_exists(
                    "COALESCE(prv.status, 'pending') = 'pending'",
                    [],
                    review_join=True,
                )
                return (exists if _truthy(value) else "NOT " + exists), params
            if field == "has_mask":
                has = "p.mask_path IS NOT NULL"
                return (has if _truthy(value) else f"NOT ({has})"), []
            if field == "has_jpeg_companion":
                # SQLite LIKE is case-insensitive for ASCII by default, so
                # LOWER() would be redundant here.
                has = (
                    "p.companion_path IS NOT NULL AND "
                    "(p.companion_path LIKE '%.jpg' OR "
                    "p.companion_path LIKE '%.jpeg')"
                )
                return (f"({has})" if _truthy(value) else f"NOT ({has})"), []
            if field == "active_mask_variant":
                if op in ("equals", "is"):
                    return "p.active_mask_variant = ?", [value]
                if op == "is not":
                    return "(p.active_mask_variant IS NULL OR p.active_mask_variant != ?)", [value]
                if op == "contains":
                    return "p.active_mask_variant LIKE ?", [f"%{value}%"]
            if field == "has_gps":
                has = "p.latitude IS NOT NULL AND p.longitude IS NOT NULL"
                return _boolean_predicate(has, op, value)
            if field == "has_location_keyword":
                has = (
                    "EXISTS (SELECT 1 FROM photo_keywords pk "
                    "JOIN keywords k ON k.id = pk.keyword_id "
                    "WHERE pk.photo_id = p.id AND k.type = 'location')"
                )
                return _boolean_predicate(has, op, value)
            if field == "location_keyword_missing":
                gps = "p.latitude IS NOT NULL AND p.longitude IS NOT NULL"
                no_loc = (
                    "NOT EXISTS (SELECT 1 FROM photo_keywords pk "
                    "JOIN keywords k ON k.id = pk.keyword_id "
                    "WHERE pk.photo_id = p.id AND k.type = 'location')"
                )
                cond = f"({gps}) AND ({no_loc})"
                return (cond if _truthy(value) else f"NOT ({cond})"), []
            if field == "inat_submitted":
                has = "EXISTS (SELECT 1 FROM inat_submissions ins WHERE ins.photo_id = p.id)"
                return (has if _truthy(value) else f"NOT {has}"), []
            if field == "is_duplicate":
                # Catalog-wide by file_hash to match find_duplicate_groups()
                # and apply_duplicate_resolution — a photo whose only duplicate
                # lives in another workspace is still a duplicate here (the
                # Duplicates workflow will act on it), so Browse must not hide
                # that membership behind a workspace_folders join.
                has = (
                    "p.file_hash IS NOT NULL AND EXISTS ("
                    "SELECT 1 FROM photos p2 "
                    "WHERE p2.id != p.id AND p2.file_hash = p.file_hash "
                    "AND (p2.flag IS NULL OR p2.flag != 'rejected'))"
                )
                return _boolean_predicate(has, op, value)
            if field in ("file_size", "width", "height", "focal_length",
                         "aperture", "shutter_speed", "iso"):
                return _numeric_condition(f"p.{field}", op, value, allow_null=True)
            if field == "gps_lat":
                return _numeric_condition("p.latitude", op, value, allow_null=True)
            if field == "gps_lng":
                return _numeric_condition("p.longitude", op, value, allow_null=True)
            if field in ("filename", "camera_make", "camera_model", "lens"):
                result = _text_condition(
                    f"p.{field}", op, value,
                    case_sensitive=bool(rule.get("case")),
                )
                if result is not None:
                    return result
            if field == "burst_id":
                if op in ("equals", "is"):
                    return "p.burst_id = ?", [value]
                if op == "is not":
                    return "(p.burst_id IS NULL OR p.burst_id != ?)", [value]
            if field == "in_burst":
                has = "p.burst_id IS NOT NULL"
                return _boolean_predicate(has, op, value)
            if field == "duplicate_group":
                # Duplicate groups have no id table; membership is identity
                # on file_hash (see find_duplicate_groups).
                if op in ("equals", "is"):
                    return "p.file_hash = ?", [value]
                if op == "is not":
                    return "(p.file_hash IS NULL OR p.file_hash != ?)", [value]
            if field == "has_edits":
                has = ("EXISTS (SELECT 1 FROM photo_edit_recipes per "
                       "WHERE per.photo_id = p.id)")
                return _boolean_predicate(has, op, value)
            if field == "has_visual_index":
                # Optional rule key "model" narrows to one embedding model.
                # The universal-filter API layer injects the active visual
                # model onto UI-emitted rules missing this key so the
                # filter agrees with visual search (which only loads
                # embeddings for the active model). Saved smart
                # collections without a ``model`` keep matching any row —
                # a portable "some embedding exists" check.
                model = rule.get("model")
                if model:
                    has = ("EXISTS (SELECT 1 FROM photo_embeddings pe "
                           "WHERE pe.photo_id = p.id AND pe.model = ?)")
                    params = [model]
                else:
                    has = ("EXISTS (SELECT 1 FROM photo_embeddings pe "
                           "WHERE pe.photo_id = p.id)")
                    params = []
                return _boolean_predicate(has, op, value, params)
            if field == "species":
                # Confirmed species ride photo_keywords→keywords(→taxa);
                # a photo with several species matches when ANY matches
                # (multi-species model). Match by taxon identity for
                # linked rows so any keyword whose canonical name matches
                # the value pulls in every photo tagged with that same
                # taxon — a photo tagged only with a hierarchy leaf
                # (``Desert Verdin``) still matches the canonical species
                # (``Verdin``) that ``get_species_keywords_for_photos`` —
                # and therefore Browse, life list, and species_representative
                # lookups — report for it. Falls back to raw ``k.name`` for
                # taxonomy-less legacy rows so user-created/offline species
                # tags continue to filter.
                def _species_exists(name_op):
                    # ``name_op`` is a SQL fragment with ``{name_col}`` for
                    # the column reference — e.g. ``{name_col} = ?`` or
                    # ``{name_col} LIKE ?``. Formatted four times, once per
                    # branch below; parameter order below must match this
                    # ordering.
                    legacy_pred = name_op.format(name_col="k.name")
                    # An attached root (``k.parent_id IS NULL``) surfaces
                    # under its own stored spelling everywhere else:
                    # ``get_species_keywords_for_photos`` keeps it (its
                    # ``is_root`` guard) and ``/api/filters/values``
                    # groups by ``kv.name``. Match only that spelling —
                    # never a same-taxon sibling root — so a taxon with
                    # multiple roots (say ``American Crow`` MIN(id) and
                    # ``crow (american)``) doesn't cross-match: selecting
                    # the ``American Crow`` suggestion must not return
                    # photos the typeahead counts under ``crow (american)``.
                    self_pred = name_op.format(name_col="k.name")
                    # A hierarchy leaf (``k.parent_id IS NOT NULL``) is
                    # displayed as the canonical MIN(id) root spelling of
                    # its taxon — that's what
                    # ``get_species_keywords_for_photos``'s
                    # ``canonical_roots`` and ``/api/filters/values``'s
                    # ``root_kv.id = MIN(id)`` both surface. Match only
                    # that MIN(id) root's name so the filter agrees with
                    # what typeahead offers and Browse shows; any-root
                    # matching would let a leaf photo satisfy a rule for
                    # a sibling-root spelling that never appears in the
                    # UI.
                    root_pred = name_op.format(name_col="root.name")
                    # A hierarchy leaf whose taxon has no top-level root row
                    # in ``keywords`` (repair detached the ``Verdin`` root
                    # and left only the ``Desert Verdin`` leaf) is shown as
                    # its own leaf spelling by
                    # ``get_species_keywords_for_photos`` and by
                    # ``/api/filters/values`` (``COALESCE(root.name, k.name)``).
                    # Fall back to matching the leaf's own ``k.name`` so a
                    # ``species is "Desert Verdin"`` rule matches those
                    # photos — otherwise the filter would silently exclude
                    # them (and ``is not`` would silently include them).
                    leaf_no_root_pred = name_op.format(name_col="k.name")
                    return (
                        "EXISTS (SELECT 1 FROM photo_keywords pk "
                        "JOIN keywords k ON k.id = pk.keyword_id "
                        "LEFT JOIN taxa t ON t.id = k.taxon_id "
                        "WHERE pk.photo_id = p.id "
                        "AND (k.is_species = 1 OR k.type = 'taxonomy') "
                        "AND (t.rank = 'species' OR t.rank IS NULL) "
                        "AND ("
                        f"(k.taxon_id IS NULL AND {legacy_pred})"
                        " OR (k.taxon_id IS NOT NULL AND ("
                        f"(k.parent_id IS NULL AND {self_pred})"
                        " OR (k.parent_id IS NOT NULL AND ("
                        "EXISTS ("
                        "SELECT 1 FROM keywords root "
                        "WHERE root.taxon_id = k.taxon_id "
                        "AND root.parent_id IS NULL "
                        "AND (root.is_species = 1 OR root.type = 'taxonomy') "
                        "AND root.id = ("
                        "SELECT MIN(id) FROM keywords "
                        "WHERE taxon_id = k.taxon_id "
                        "AND parent_id IS NULL "
                        "AND (is_species = 1 OR type = 'taxonomy')) "
                        f"AND {root_pred})"
                        " OR (NOT EXISTS ("
                        "SELECT 1 FROM keywords rootless "
                        "WHERE rootless.taxon_id = k.taxon_id "
                        "AND rootless.parent_id IS NULL "
                        "AND (rootless.is_species = 1 OR rootless.type = 'taxonomy')"
                        f") AND {leaf_no_root_pred})"
                        "))))))"
                    )
                if op == "contains":
                    # Escape user LIKE metacharacters so ``%``/``_`` in the
                    # value stay literal — matches the other text/folder
                    # rules and blocks a ``value="%"`` request from matching
                    # every species-tagged photo. One param each for the
                    # legacy no-taxon branch, the attached-root branch, the
                    # same-taxon root lookup, and the rootless-leaf fallback.
                    like = f"%{_escape_like(str(value or ''))}%"
                    return (
                        _species_exists("{name_col} LIKE ? ESCAPE '\\'"),
                        [like, like, like, like],
                    )
                if op == "not_contains":
                    like = f"%{_escape_like(str(value or ''))}%"
                    return (
                        "NOT " + _species_exists("{name_col} LIKE ? ESCAPE '\\'"),
                        [like, like, like, like],
                    )
                if op in ("equals", "is"):
                    return _species_exists("{name_col} = ?"), [value, value, value, value]
                if op == "is not":
                    return "NOT " + _species_exists("{name_col} = ?"), [value, value, value, value]
            raise ValueError(f"unsupported collection rule field/op: {field}/{op}")

        def _build_node(node):
            if "rules" in node and "field" not in node:
                mode = node.get("mode", "all")
                child_sql = []
                params = []
                for child in node.get("rules", []):
                    sql, child_params = _build_node(child)
                    if sql:
                        child_sql.append(f"({sql})")
                        params.extend(child_params)
                if not child_sql:
                    return ("0", []) if mode == "any" else (None, [])
                if mode == "all":
                    return " AND ".join(child_sql), params
                if mode == "any":
                    return " OR ".join(child_sql), params
                return "NOT (" + " OR ".join(child_sql) + ")", params
            return _build_leaf(node)

        _validate_node(root)
        condition, params = _build_node(root)

        # Always join folders for folder-under rules, scoped to workspace.
        # For metadata-only callers (Dashboard scope) drop the accessible-
        # folder filter so photos in an offline folder still count toward
        # the collection's membership; every other caller keeps the Browse-
        # oriented ``status IN ('ok', 'partial')`` filter that excludes them.
        if include_offline_folders:
            folder_join = " JOIN folders f ON f.id = p.folder_id"
        else:
            folder_join = " JOIN folders f ON f.id = p.folder_id AND f.status IN ('ok', 'partial')"
        folder_join += " JOIN workspace_folders wf ON wf.folder_id = f.id AND wf.workspace_id = ?"

        # folder_join comes before join_clause in the query, so its param goes first
        params.insert(0, self._ws_id())

        where = f"WHERE {condition}" if condition else ""

        return folder_join, "", where, params

    def get_collection_photos(self, collection_id, page=1, per_page=50):
        """Build SQL from collection rules and return matching photos."""
        parts = self._build_collection_query(collection_id)
        if parts is None:
            return []

        folder_join, join_clause, where, params = parts
        page = max(1, page)
        offset = (page - 1) * per_page
        params.extend([per_page, offset])

        pcols = ", ".join(f"p.{c.strip()}" for c in self.PHOTO_COLS.split(","))
        query = f"""
            SELECT DISTINCT {pcols} FROM photos p
            {folder_join}
            {join_clause}
            {where}
            ORDER BY {_PHOTO_DATE_ASC_ORDER}
            LIMIT ? OFFSET ?
        """
        return self.conn.execute(query, params).fetchall()

    def get_collection_photo_ids(self, collection_id):
        """Return all photo IDs matching a collection in display order."""
        parts = self._build_collection_query(collection_id)
        if parts is None:
            return []

        folder_join, join_clause, where, params = parts
        query = f"""
            SELECT DISTINCT p.id FROM photos p
            {folder_join}
            {join_clause}
            {where}
            ORDER BY {_PHOTO_DATE_ASC_ORDER}
        """
        return [row["id"] for row in self.conn.execute(query, params).fetchall()]

    def count_collection_photos(self, collection_id):
        """Return total count of photos matching collection rules."""
        parts = self._build_collection_query(collection_id)
        if parts is None:
            return 0

        folder_join, join_clause, where, params = parts
        query = f"""
            SELECT COUNT(DISTINCT p.id) FROM photos p
            {folder_join}
            {join_clause}
            {where}
        """
        return self.conn.execute(query, params).fetchone()[0]

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

    def count_photos_for_rules(self, rules):
        """Return the number of photos in the active workspace that match
        an unsaved rules list. Used by the smart-collection modal preview.

        Raises ValueError on malformed input (propagated from
        ``_build_query_from_rules``).
        """
        folder_join, join_clause, where, params = self._build_query_from_rules(rules)
        query = f"""
            SELECT COUNT(DISTINCT p.id) FROM photos p
            {folder_join}
            {join_clause}
            {where}
        """
        return self.conn.execute(query, params).fetchone()[0]

    def query_photos(self, rules, sort="date", page=1, per_page=50):
        """Return paginated photos matching a universal-filter rule tree.

        The rules format is the smart-collection tree (see
        ``_build_query_from_rules``); ``count_photos_for_rules`` gives the
        matching total. Raises ValueError on malformed rules.
        """
        folder_join, join_clause, where, params = self._build_query_from_rules(rules)
        sort_map = {
            "date": _PHOTO_DATE_ASC_ORDER,
            "date_desc": _PHOTO_DATE_DESC_ORDER,
            "name": "p.filename ASC, p.id ASC",
            "name_desc": "p.filename DESC, p.id ASC",
            "rating": "p.rating DESC, p.filename ASC, p.id ASC",
            "sharpness": "p.sharpness DESC, p.filename ASC, p.id ASC",
            "sharpness_asc": "p.sharpness ASC, p.filename ASC, p.id ASC",
            "quality": "p.quality_score DESC, p.filename ASC, p.id ASC",
        }
        order = sort_map.get(sort, _PHOTO_DATE_ASC_ORDER)
        page = max(1, page)
        offset = (page - 1) * per_page
        pcols = ", ".join(f"p.{c.strip()}" for c in self.PHOTO_COLS.split(","))
        query = f"""
            SELECT DISTINCT {pcols} FROM photos p
            {folder_join}
            {join_clause}
            {where}
            ORDER BY {order}
            LIMIT ? OFFSET ?
        """
        return self.conn.execute(query, [*params, per_page, offset]).fetchall()

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
        "extension": ("LOWER(p.extension)", "LOWER(p.extension)"),
    }

    def get_filter_field_values(self, field, rules=None, q=None, limit=20):
        """Distinct values (with photo counts) for a suggest-capable field.

        Counts respect the supplied rule tree, so the caller can pass the
        active expression minus the rule being edited and get live facet
        counts (design requirement: counts answer "how many results would I
        get", never a global COUNT(*)). Raises ValueError for fields without
        value suggestions or malformed rules.
        """
        folder_join, join_clause, where, params = self._build_query_from_rules(
            rules if rules is not None else []
        )
        limit = max(1, min(int(limit or 20), 50))
        if field == "folder":
            return self._folder_filter_values(
                folder_join, join_clause, where, params, q=q, limit=limit
            )
        conditions = []
        extra_joins = ""
        extra_params = []
        if field in self._SUGGEST_VALUE_EXPRS:
            display_expr, group_expr = self._SUGGEST_VALUE_EXPRS[field]
        elif field in ("keyword", "species"):
            extra_joins = (
                " JOIN photo_keywords pkv ON pkv.photo_id = p.id"
                " JOIN keywords kv ON kv.id = pkv.keyword_id"
            )
            if field == "species":
                # Canonicalize hierarchy leaves (e.g. ``Desert Verdin``) to
                # the same-taxon top-level root (``Verdin``) that
                # ``get_species_keywords_for_photos`` — and therefore Browse,
                # life-list, and species_representative lookups — report.
                # Otherwise the typeahead would suggest the raw leaf spelling
                # for a photo whose species is displayed as ``Verdin`` in
                # every other view. Deterministic root pick (MIN(id)) mirrors
                # the ``setdefault`` in ``get_species_keywords_for_photos``.
                #
                # BUT: only rewrite hierarchy leaves. When ``kv`` is itself
                # a top-level root (``parent_id IS NULL``),
                # ``get_species_keywords_for_photos`` deliberately keeps the
                # attached root's stored spelling (see its ``is_root`` guard)
                # so curation lookups keyed on that name still resolve. If a
                # taxon has multiple species roots and a photo is tagged
                # with the non-MIN(id) root, rewriting to MIN(id) here would
                # drop the species name Browse shows and make the typeahead
                # return nothing for that root.
                extra_joins += (
                    " LEFT JOIN taxa tv ON tv.id = kv.taxon_id"
                    " LEFT JOIN keywords root_kv"
                    " ON kv.taxon_id IS NOT NULL"
                    " AND kv.parent_id IS NOT NULL"
                    " AND root_kv.taxon_id = kv.taxon_id"
                    " AND root_kv.parent_id IS NULL"
                    " AND (root_kv.is_species = 1 OR root_kv.type = 'taxonomy')"
                    " AND root_kv.id = ("
                    "SELECT MIN(id) FROM keywords"
                    " WHERE taxon_id = kv.taxon_id"
                    " AND parent_id IS NULL"
                    " AND (is_species = 1 OR type = 'taxonomy'))"
                )
                conditions.append("(kv.is_species = 1 OR kv.type = 'taxonomy')")
                conditions.append("(tv.rank = 'species' OR tv.rank IS NULL)")
                display_expr = (
                    "CASE WHEN kv.parent_id IS NULL THEN kv.name"
                    " ELSE COALESCE(root_kv.name, kv.name) END"
                )
                group_expr = display_expr
            else:
                display_expr = "kv.name"
                group_expr = display_expr
        else:
            raise ValueError(f"field {field!r} does not support value suggestions")
        # IS NOT NULL and the typeahead LIKE compare against ``group_expr``
        # so a facet grouped case-insensitively (camera fields, extension)
        # also matches case-insensitively — otherwise ``camera_model``
        # would list a single ``Sony A1`` bucket but drop it as soon as the
        # user typed ``sony``.
        conditions.append(f"{group_expr} IS NOT NULL")
        if q:
            conditions.append(f"{group_expr} LIKE ? ESCAPE '\\'")
            q_norm = str(q).lower() if group_expr.startswith("LOWER(") else str(q)
            extra_params.append(f"%{_escape_like(q_norm)}%")
        joined = " AND ".join(conditions)
        # ``where`` from ``_build_query_from_rules`` is ``WHERE (A) OR (B)``
        # for a top-level ``any`` group. Appending ``AND {joined}`` without
        # wrapping would bind to the last OR branch (AND binds tighter than
        # OR in SQL) and let first-branch rows leak through even when the
        # facet field is NULL or the typeahead doesn't match — so the count
        # no longer answers "how many results would selecting this suggestion
        # return".
        if where:
            where_full = f"WHERE ({where[len('WHERE '):]}) AND {joined}"
        else:
            where_full = f"WHERE {joined}"
        query = f"""
            SELECT {display_expr} AS value, COUNT(DISTINCT p.id) AS count
            FROM photos p
            {folder_join}
            {join_clause}
            {extra_joins}
            {where_full}
            GROUP BY {group_expr}
            ORDER BY count DESC, value ASC
            LIMIT ?
        """
        rows = self.conn.execute(query, [*params, *extra_params, limit]).fetchall()
        return [{"value": row["value"], "count": row["count"]} for row in rows]

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
        q_condition = ""
        q_params = []
        if q:
            q_condition = " AND ff.path LIKE ? ESCAPE '\\'"
            q_params.append(f"%{_escape_like(str(q))}%")
        # Inner select: photos matching sibling rules with their folder
        # path. Reused verbatim so every rule-engine feature (workspace
        # scope, status filter, sibling predicates) applies unchanged.
        inner = f"""
            SELECT DISTINCT p.id AS pid, f.path AS ppath
            FROM photos p
            {folder_join}
            {join_clause}
            {where}
        """
        # Path normalization matches ``_build_query_from_rules`` for the
        # ``folder``/``under`` op so a Windows library's backslash paths
        # aggregate the same way here as they filter there. Both sides
        # collapse ``\`` to ``/`` and strip trailing separators — the
        # engine feeds its value through ``_path_for_subtree_match`` which
        # ``rstrip("/")``s, so folder roots stored with a trailing separator
        # (Windows drive root ``D:\`` normalizes to ``D:/``, POSIX root
        # ``/`` normalizes to ``/``) would otherwise concat to a LIKE
        # pattern with a doubled slash (``D://%`` / ``//%``) that never
        # matches real descendant paths — the suggested root would count 0
        # while ``folder under D:\`` actually returns every photo on the
        # drive. ``norm_folder_like`` additionally escapes SQL LIKE
        # metacharacters (``%`` / ``_``) so a folder like ``/pics/my_dir``
        # only aggregates its own subtree and does not double-count photos
        # under a sibling ``/pics/myXdir``; the engine's ``folder under``
        # op escapes the same characters via ``_escape_like``. The ``\\``
        # escape char is safe to introduce here because path normalization
        # above has already collapsed every literal ``\`` to ``/``.
        norm_photo = "RTRIM(REPLACE(matched.ppath, '\\', '/'), '/')"
        norm_folder = "RTRIM(REPLACE(ff.path, '\\', '/'), '/')"
        norm_folder_like = (
            f"REPLACE(REPLACE({norm_folder}, '%', '\\%'), '_', '\\_')"
        )
        query = f"""
            SELECT ff.path AS value, COUNT(DISTINCT matched.pid) AS count
            FROM folders ff
            JOIN workspace_folders ff_wf
              ON ff_wf.folder_id = ff.id AND ff_wf.workspace_id = ?
            JOIN ({inner}) matched ON (
                {norm_photo} = {norm_folder}
                OR {norm_photo} LIKE {norm_folder_like} || '/%' ESCAPE '\\'
            )
            WHERE ff.path IS NOT NULL{q_condition}
            GROUP BY ff.path
            ORDER BY count DESC, value ASC
            LIMIT ?
        """
        rows = self.conn.execute(
            query, [self._ws_id(), *params, *q_params, limit]
        ).fetchall()
        return [{"value": row["value"], "count": row["count"]} for row in rows]

    def collection_photo_ids(self, collection_id):
        """Return the set of photo IDs in the collection, workspace-scoped.

        Returns an empty set for a missing collection. Used by stages
        that need to restrict writes to the current pipeline-run scope
        without paging through full photo rows.
        """
        parts = self._build_collection_query(collection_id)
        if parts is None:
            return set()

        folder_join, join_clause, where, params = parts
        query = f"""
            SELECT DISTINCT p.id FROM photos p
            {folder_join}
            {join_clause}
            {where}
        """
        return {row["id"] for row in self.conn.execute(query, params)}

    def update_folder_counts(self):
        """Recalculate photo_count for all folders."""
        self.conn.execute(
            """
            UPDATE folders SET photo_count = (
                SELECT COUNT(*) FROM photos WHERE photos.folder_id = folders.id
            )
        """
        )
        self.conn.commit()

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
        species-rank taxon) a ``taxon_id`` link by iNaturalist id.
        ``taxon_id`` is left NULL when only a higher-rank (genus/family)
        match exists — binding to a non-species-rank taxon would satisfy the
        marking pass but make the row invisible to every rank-filtered
        reader that restricts to ``t.rank = 'species' OR t.rank IS NULL``.
        A ``type='taxonomy'`` row whose ``taxon_id`` was bound by the old
        species-agnostic lookup to a non-species-rank taxon (genus/family)
        is rebound to the species-rank taxon whenever ``taxonomy.lookup``
        resolves to one; otherwise the new ``rank = 'species'`` filters
        would silently drop every photo carrying the accepted keyword.

        Uses the local taxonomy only (no network requests).

        Args:
            taxonomy: a Taxonomy instance with a lookup() method
        """
        # Also include already-typed taxonomy keywords whose taxon_id is
        # still NULL — those were created before the local taxa table was
        # populated (e.g. via add_keyword(..., is_species=True) from the
        # classifier), and still need their hierarchy link filled in. Also
        # revisit already-typed taxonomy keywords whose taxon_id points at a
        # non-species-rank taxon so we can rebind them to a species-rank
        # taxon when one exists. Deliberate non-taxonomy types are excluded
        # before lookup so a taxon homonym can never silently retype them.
        keywords = self.conn.execute(
            "SELECT k.id, k.name, k.type, k.taxon_id, k.is_species, "
            "       t.rank AS taxon_rank "
            "FROM keywords k "
            "LEFT JOIN taxa t ON t.id = k.taxon_id "
            "WHERE (k.type IS NULL OR k.type IN ('general', 'taxonomy')) "
            "  AND ("
            "    k.is_species = 0 OR k.type IS NULL OR k.type != 'taxonomy' "
            "    OR k.taxon_id IS NULL "
            "    OR (t.rank IS NOT NULL AND t.rank != 'species')"
            "  )"
        ).fetchall()
        updated = 0
        for kw in keywords:
            taxon = taxonomy.lookup(kw["name"])
            if not taxon:
                continue
            local_taxon_id = kw["taxon_id"]
            lookup_local_id = None
            lookup_rank = None
            inat_id = taxon.get("taxon_id")
            if inat_id is not None:
                row = self.conn.execute(
                    "SELECT id, rank FROM taxa WHERE inat_id = ?", (inat_id,)
                ).fetchone()
                if row:
                    lookup_local_id = row["id"]
                    lookup_rank = row["rank"]
            if local_taxon_id is None and lookup_rank == "species":
                local_taxon_id = lookup_local_id
            # Only bind ``taxon_id`` to a species-rank local taxon. Binding a
            # species-marked keyword to a genus/family id would let the new
            # rank filters (Life List, Compare, highlight/preference
            # eligibility) silently drop every photo carrying that keyword,
            # because those readers require ``t.rank = 'species' OR
            # t.rank IS NULL``. Leaving ``taxon_id`` NULL for non-species
            # matches mirrors ``add_keyword``'s species-add path (see
            # ``test_add_species_leaves_taxon_null_when_only_higher_rank_matches``)
            # so upgraded catalogs stay visible under the ``rank IS NULL``
            # branch until a species-rank taxon becomes available.
            #
            # Rebind an existing higher-rank link when the taxonomy lookup
            # resolves to a species-rank local taxon. Without this pass,
            # mark_species_keywords skipped fully typed rows and legacy
            # keywords bound by the old species-agnostic lookup to a
            # genus/family stayed bound; the new ``t.rank = 'species'``
            # filter (Life List, Compare) then silently dropped every
            # photo carrying those keywords after upgrade.
            rebind_taxon_id = None
            if (
                kw["taxon_id"] is not None
                and kw["taxon_rank"] is not None
                and kw["taxon_rank"] != "species"
                and lookup_local_id is not None
                and lookup_rank == "species"
                and lookup_local_id != kw["taxon_id"]
            ):
                rebind_taxon_id = lookup_local_id
            # When the existing binding is non-species-rank and no
            # species-rank replacement is available, clear ``taxon_id`` to
            # NULL. Leaving the higher-rank link in place would satisfy
            # ``mark_species_keywords`` but make the row invisible to
            # every rank-filtered reader (Life List, Compare,
            # highlight/preference eligibility) that restricts to
            # ``t.rank = 'species' OR t.rank IS NULL``; NULLing it lets
            # the accepted keyword stay visible via the ``rank IS NULL``
            # fallback until a species-rank taxon becomes available,
            # mirroring ``add_keyword``'s reuse-path clearing (see
            # ``test_add_species_clears_higher_rank_taxon_when_no_species_match``).
            should_clear_taxon = (
                kw["taxon_id"] is not None
                and kw["taxon_rank"] is not None
                and kw["taxon_rank"] != "species"
                and rebind_taxon_id is None
            )
            # Skip no-op updates so the "updated" count reflects real
            # changes. A matched row is fully consistent when type is
            # 'taxonomy', is_species is 1, and (taxon_id is already set to
            # a species-rank id OR we have no local id to link it to).
            is_type_change = kw["type"] != "taxonomy"
            is_species_fix = kw["is_species"] != 1
            is_taxon_link = kw["taxon_id"] is None and local_taxon_id is not None
            is_rebind = rebind_taxon_id is not None
            if not (
                is_type_change
                or is_species_fix
                or is_taxon_link
                or is_rebind
                or should_clear_taxon
            ):
                continue
            if is_rebind:
                self.conn.execute(
                    "UPDATE keywords SET is_species = 1, type = 'taxonomy', "
                    "taxon_id = ? WHERE id = ?",
                    (rebind_taxon_id, kw["id"]),
                )
            elif should_clear_taxon:
                self.conn.execute(
                    "UPDATE keywords SET is_species = 1, type = 'taxonomy', "
                    "taxon_id = NULL WHERE id = ?",
                    (kw["id"],),
                )
            else:
                self.conn.execute(
                    "UPDATE keywords SET is_species = 1, type = 'taxonomy', "
                    "taxon_id = COALESCE(taxon_id, ?) WHERE id = ?",
                    (local_taxon_id, kw["id"]),
                )
            updated += 1
        if updated:
            self.conn.commit()
        return updated

    def create_default_collections(self, workspace_id=None):
        """Create default smart collections, skipping any that already exist by name.

        Workspace defaults to the active one. Pass ``workspace_id`` to seed a
        specific workspace without needing it to be active — used by
        ``api_create_workspace`` so brand-new workspaces get the defaults at
        creation time instead of relying on a future startup pass.
        """
        ws_id = workspace_id if workspace_id is not None else self._ws_id()
        existing_names = {
            row["name"] for row in self.conn.execute(
                "SELECT name FROM collections WHERE workspace_id = ?", (ws_id,),
            ).fetchall()
        }

        defaults = [
            ("All Photos", [{"field": "all"}]),
            (
                "Needs Identification",
                NEEDS_IDENTIFICATION_RULES,
            ),
            ("Untagged", [{"field": "keyword_count", "op": "equals", "value": 0}]),
            ("Flagged", [{"field": "flag", "op": "equals", "value": "flagged"}]),
            (
                "Recent Import",
                [{"field": "timestamp", "op": "recent_days", "value": 30}],
            ),
            (
                "GPS Without Location Keyword",
                GPS_WITHOUT_LOCATION_KEYWORD_RULES,
            ),
        ]
        for name, rules in defaults:
            if name not in existing_names:
                self.conn.execute(
                    "INSERT INTO collections (name, rules, workspace_id) VALUES (?, ?, ?)",
                    (name, json.dumps(rules), ws_id),
                )
        self.conn.commit()

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
        updated = 0
        gps_rules = [
            GPS_WITHOUT_LOCATION_KEYWORD_RULES,
            {
                "mode": "all",
                "rules": GPS_WITHOUT_LOCATION_KEYWORD_RULES,
            },
        ]
        no_location_inverse_rules = [
            [{"field": "location_keyword_missing", "op": "equals", "value": 0}],
            {
                "mode": "all",
                "rules": [
                    {"field": "location_keyword_missing", "op": "equals", "value": 0},
                ],
            },
        ]

        rows = self.conn.execute(
            "SELECT id, workspace_id, name, rules FROM collections "
            "WHERE name IN ('Needs Location', 'No Location')"
        ).fetchall()
        for row in rows:
            try:
                current = json.loads(row["rules"])
            except (TypeError, ValueError):
                continue

            if row["name"] == "Needs Location" and current in gps_rules:
                self.conn.execute(
                    "UPDATE collections SET name = ?, rules = ? WHERE id = ?",
                    (
                        "GPS Without Location Keyword",
                        json.dumps(GPS_WITHOUT_LOCATION_KEYWORD_RULES),
                        row["id"],
                    ),
                )
                updated += 1
                continue

            if row["name"] == "No Location" and current in no_location_inverse_rules:
                self.conn.execute(
                    "UPDATE collections SET name = ?, rules = ? WHERE id = ?",
                    (
                        "No Location Information",
                        json.dumps(NO_LOCATION_INFORMATION_RULES),
                        row["id"],
                    ),
                )
                updated += 1

        if updated:
            self.conn.commit()
        return updated

    def migrate_default_subject_collection(self):
        """Rename legacy 'Needs Classification' (with rule has_species==0)
        to 'Needs Identification' (rule has_subject==0) across ALL workspaces.

        Workspace activation does not re-run startup migrations, so an
        upgraded multi-workspace database would otherwise leave non-active
        workspaces stuck on the legacy rule. Skips collections the user has
        customized. Idempotent."""
        rows = self.conn.execute(
            "SELECT id, workspace_id, rules FROM collections WHERE name = ?",
            ("Needs Classification",),
        ).fetchall()
        legacy_rule = [{"field": "has_species", "op": "equals", "value": 0}]
        for row in rows:
            try:
                current = json.loads(row["rules"])
            except (TypeError, ValueError):
                continue
            if current != legacy_rule:
                continue
            # Don't clobber an existing "Needs Identification" in the SAME
            # workspace (each workspace has its own default collections).
            existing = self.conn.execute(
                "SELECT 1 FROM collections WHERE workspace_id = ? AND name = ?",
                (row["workspace_id"], "Needs Identification"),
            ).fetchone()
            if existing:
                continue
            self.conn.execute(
                "UPDATE collections SET name = ?, rules = ? WHERE id = ?",
                (
                    "Needs Identification",
                    json.dumps(NEEDS_IDENTIFICATION_RULES),
                    row["id"],
                ),
            )
        self.conn.commit()

    def migrate_default_needs_identification_collection(self):
        """Upgrade the default Needs Identification rule to skip Not Wildlife.

        User-customized collections are left alone; only the exact previous
        default ``has_subject == 0`` rule is rewritten.
        """
        old_rule = [{"field": "has_subject", "op": "equals", "value": 0}]
        rows = self.conn.execute(
            "SELECT id, rules FROM collections WHERE name = ?",
            ("Needs Identification",),
        ).fetchall()
        updated = 0
        for row in rows:
            try:
                current = json.loads(row["rules"])
            except (TypeError, ValueError):
                continue
            if current != old_rule:
                continue
            self.conn.execute(
                "UPDATE collections SET rules = ? WHERE id = ?",
                (json.dumps(NEEDS_IDENTIFICATION_RULES), row["id"]),
            )
            updated += 1
        if updated:
            self.conn.commit()
        return updated

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
        rows = self.conn.execute(
            "SELECT id, config_overrides FROM workspaces "
            "WHERE config_overrides IS NOT NULL"
        ).fetchall()
        updated = 0
        for row in rows:
            raw = row["config_overrides"]
            try:
                overrides = json.loads(raw) if isinstance(raw, str) else raw
            except (json.JSONDecodeError, TypeError):
                continue
            if not isinstance(overrides, dict):
                continue
            pipeline = overrides.get("pipeline")
            if not isinstance(pipeline, dict):
                continue
            if (
                pipeline.get("miss_det_confidence") != legacy_det
                or pipeline.get("miss_det_confidence_burst") != legacy_burst
            ):
                continue
            pipeline["miss_det_confidence"] = new_det
            pipeline["miss_det_confidence_burst"] = new_burst
            self.conn.execute(
                "UPDATE workspaces SET config_overrides = ? WHERE id = ?",
                (json.dumps(overrides), row["id"]),
            )
            updated += 1
        if updated:
            self.conn.commit()
        return updated

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
        rows = self.conn.execute(
            "SELECT id, config_overrides FROM workspaces "
            "WHERE config_overrides IS NOT NULL"
        ).fetchall()
        updated = 0
        for row in rows:
            raw = row["config_overrides"]
            try:
                overrides = json.loads(raw) if isinstance(raw, str) else raw
            except (json.JSONDecodeError, TypeError):
                continue
            if not isinstance(overrides, dict):
                continue
            pipeline = overrides.get("pipeline")
            if not isinstance(pipeline, dict):
                continue
            if pipeline.get("w_species") != legacy:
                continue
            pipeline["w_species"] = new
            self.conn.execute(
                "UPDATE workspaces SET config_overrides = ? WHERE id = ?",
                (json.dumps(overrides), row["id"]),
            )
            updated += 1
        if updated:
            self.conn.commit()
        return updated

    def rewrite_legacy_eye_detect_default_in_workspaces(self):
        """Rewrite the exact legacy eye-detection default in workspace overrides.

        Also nulls ``last_group_fingerprint`` on any rewritten workspace so the
        Process page treats its cached KEEP/REJECT decisions as outdated —
        those results were scored with eye detection on, and the workspace's
        effective ``eye_detect_enabled`` just changed to False.
        """
        rows = self.conn.execute(
            "SELECT id, config_overrides FROM workspaces "
            "WHERE config_overrides IS NOT NULL"
        ).fetchall()
        updated = 0
        for row in rows:
            raw = row["config_overrides"]
            try:
                overrides = json.loads(raw) if isinstance(raw, str) else raw
            except (json.JSONDecodeError, TypeError):
                continue
            if not isinstance(overrides, dict):
                continue
            pipeline = overrides.get("pipeline")
            if not isinstance(pipeline, dict):
                continue
            if pipeline.get("eye_detect_enabled") is not True:
                continue
            pipeline["eye_detect_enabled"] = False
            self.conn.execute(
                "UPDATE workspaces "
                "SET config_overrides = ?, last_group_fingerprint = NULL "
                "WHERE id = ?",
                (json.dumps(overrides), row["id"]),
            )
            updated += 1
        if updated:
            self.conn.commit()
        return updated

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
        rows = self.conn.execute(
            "SELECT id, config_overrides FROM workspaces "
            "WHERE last_group_fingerprint IS NOT NULL"
        ).fetchall()
        to_invalidate = []
        for row in rows:
            raw = row["config_overrides"]
            has_explicit_false = False
            if raw is not None:
                try:
                    overrides = json.loads(raw) if isinstance(raw, str) else raw
                except (json.JSONDecodeError, TypeError):
                    overrides = None
                if isinstance(overrides, dict):
                    pipeline = overrides.get("pipeline")
                    if isinstance(pipeline, dict) and pipeline.get("eye_detect_enabled") is False:
                        has_explicit_false = True
            if not has_explicit_false:
                to_invalidate.append(row["id"])
        if to_invalidate:
            for chunk in _chunks(to_invalidate):
                placeholders = ",".join("?" * len(chunk))
                self.conn.execute(
                    f"UPDATE workspaces SET last_group_fingerprint = NULL "
                    f"WHERE id IN ({placeholders})",
                    list(chunk),
                )
            self.conn.commit()
        return len(to_invalidate)

    # ------ iNaturalist submissions ------

    def record_inat_submission(self, photo_id, observation_id, observation_url):
        """Record a successful iNaturalist submission."""
        self.conn.execute(
            """INSERT OR IGNORE INTO inat_submissions
               (photo_id, observation_id, observation_url)
               VALUES (?, ?, ?)""",
            (photo_id, observation_id, observation_url),
        )
        self.conn.commit()

    def get_inat_submissions(self, photo_ids):
        """Return {photo_id: {observation_id, observation_url, submitted_at}} for given IDs."""
        if not photo_ids:
            return {}
        result = {}
        for chunk in _chunks(list(dict.fromkeys(photo_ids))):
            placeholders = ",".join("?" * len(chunk))
            rows = self.conn.execute(
                f"SELECT photo_id, observation_id, observation_url, submitted_at"
                f" FROM inat_submissions WHERE photo_id IN ({placeholders})"
                f" ORDER BY submitted_at DESC, id DESC",
                list(chunk),
            ).fetchall()
            # Rows arrive newest-first; keep the first seen per photo so each
            # photo maps to its most recent submission (a dict comprehension
            # here would let older rows overwrite newer ones).
            for r in rows:
                if r["photo_id"] not in result:
                    result[r["photo_id"]] = dict(r)
        return result
