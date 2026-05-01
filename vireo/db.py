"""SQLite database for Vireo photo browser metadata cache."""

import contextlib
import json
import logging
import os
import sqlite3
import time
import unicodedata
import uuid

from new_images import get_shared_cache

log = logging.getLogger(__name__)

_UNSET = object()  # sentinel for "not provided" vs explicit None


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

# Canonical set of keyword type values stored in keywords.type.
# - taxonomy: a species/genus/etc. (linked to taxa via taxon_id)
# - individual: a named person, pet, or otherwise tracked individual
# - location: a named location ("Yosemite", "backyard")
# - genre: a non-subject visual category ("Landscape", "Sunset")
# - general: catch-all/free-form tag (the legacy default)
KEYWORD_TYPES = frozenset({"taxonomy", "individual", "location", "genre", "general"})

# Default set of types that count as "identifying" a photo for queue
# membership / classifier skip purposes. Workspaces can override.
SUBJECT_TYPES_DEFAULT = frozenset({"taxonomy", "individual", "genre"})

ALL_NAV_IDS = frozenset({
    "pipeline", "jobs", "pipeline_review", "review", "cull",
    "misses", "highlights", "browse", "map", "variants",
    "dashboard", "audit", "compare",
    "settings", "workspace", "lightroom", "shortcuts",
    "keywords", "duplicates", "logs",
})

DEFAULT_TABS = [
    "browse", "pipeline", "pipeline_review",
    "review", "cull", "jobs",
    "highlights", "misses", "settings",
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


class Database:
    """Local SQLite database that caches photo metadata from XMP sidecars.

    Args:
        db_path: path to the SQLite database file (created if missing)
    """

    def __init__(self, db_path):
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
        self.conn.execute("PRAGMA journal_mode=WAL")
        self.conn.execute("PRAGMA foreign_keys=ON")
        self.conn.execute("PRAGMA synchronous=NORMAL")
        self.conn.execute("PRAGMA cache_size=-10000")  # 10 MB
        self.conn.execute("PRAGMA temp_store=MEMORY")
        self.conn.execute("PRAGMA mmap_size=30000000")  # 30 MB
        self.conn.execute("PRAGMA busy_timeout=30000")  # 30 s — tolerate parallel scan writers
        self._active_workspace_id = None
        self._new_images_cache = get_shared_cache()
        self._create_tables()
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
        # Restore last-used workspace, or fall back to Default
        last = self.conn.execute(
            "SELECT id FROM workspaces ORDER BY CASE WHEN last_opened_at IS NULL THEN 0 ELSE 1 END DESC, last_opened_at DESC, id ASC LIMIT 1"
        ).fetchone()
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
                focal_length             REAL,
                burst_id                 TEXT,
                file_hash                TEXT,
                companion_path           TEXT,
                exif_data                TEXT,
                working_copy_path        TEXT,
                working_copy_failed_at   TEXT,
                working_copy_failed_mtime REAL,
                eye_x                    REAL,
                eye_y                    REAL,
                eye_conf                 REAL,
                eye_tenengrad            REAL,
                eye_kp_fingerprint       TEXT,
                miss_no_subject          INTEGER,
                miss_clipped             INTEGER,
                miss_oof                 INTEGER,
                miss_computed_at         TEXT,
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
                last_grouped_at         INTEGER,
                last_group_fingerprint  TEXT
            );

            CREATE TABLE IF NOT EXISTS workspace_folders (
                workspace_id    INTEGER REFERENCES workspaces(id) ON DELETE CASCADE,
                folder_id       INTEGER REFERENCES folders(id),
                PRIMARY KEY (workspace_id, folder_id)
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

            CREATE TABLE IF NOT EXISTS preview_cache (
                photo_id INTEGER NOT NULL,
                size INTEGER NOT NULL,
                bytes INTEGER NOT NULL,
                last_access_at REAL NOT NULL,
                PRIMARY KEY (photo_id, size),
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

            CREATE TABLE IF NOT EXISTS place_reverse_geocode_cache (
                lat_grid    INTEGER NOT NULL,
                lng_grid    INTEGER NOT NULL,
                place_id    TEXT,
                response    TEXT NOT NULL,
                fetched_at  INTEGER NOT NULL,
                PRIMARY KEY (lat_grid, lng_grid)
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
            CREATE INDEX IF NOT EXISTS preview_cache_last_access
                ON preview_cache(last_access_at);
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
        # Gated by db_meta so it runs exactly once per DB.
        marker = self.conn.execute(
            "SELECT value FROM db_meta WHERE key='eye_kp_fingerprint_backfill'"
        ).fetchone()
        if marker is None:
            from pipeline import EYE_KP_FINGERPRINT_VERSION
            self.conn.execute(
                "UPDATE photos SET eye_kp_fingerprint = ? "
                "WHERE eye_tenengrad IS NOT NULL AND eye_kp_fingerprint IS NULL",
                (EYE_KP_FINGERPRINT_VERSION,),
            )
            self.conn.execute(
                "INSERT INTO db_meta(key, value) VALUES ('eye_kp_fingerprint_backfill', '1')"
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
        """Return all workspaces ordered by last_opened_at desc."""
        return self.conn.execute(
            "SELECT * FROM workspaces ORDER BY last_opened_at DESC"
        ).fetchall()

    def update_workspace(self, workspace_id, name=None, config_overrides=_UNSET,
                         ui_state=_UNSET, last_opened_at=None):
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

    def get_workspace_active_labels(self):
        """Return the active_labels list from workspace config_overrides, or None."""
        ws = self.get_workspace(self._ws_id())
        if not ws or not ws["config_overrides"]:
            return None
        try:
            overrides = json.loads(ws["config_overrides"]) if isinstance(ws["config_overrides"], str) else ws["config_overrides"]
            labels = overrides.get("active_labels")
            return labels if isinstance(labels, list) else None
        except (json.JSONDecodeError, TypeError):
            return None

    def get_tabs(self):
        """Return the active workspace's ordered list of pinned tab nav-ids."""
        ws = self.get_workspace(self._ws_id())
        if not ws or not ws["tabs"]:
            return list(DEFAULT_TABS)
        try:
            value = json.loads(ws["tabs"]) if isinstance(ws["tabs"], str) else ws["tabs"]
            return value if isinstance(value, list) else list(DEFAULT_TABS)
        except (json.JSONDecodeError, TypeError):
            return list(DEFAULT_TABS)

    def set_tabs(self, tabs):
        """Replace the active workspace's tabs with the given ordered list.

        Validates every entry against ALL_NAV_IDS. Rejects duplicates so the
        UI invariant "each pinned page appears exactly once" is enforced at
        the storage layer.
        Returns the new list.
        """
        if not isinstance(tabs, list):
            raise ValueError("tabs must be a list")
        seen = set()
        for nav_id in tabs:
            if not isinstance(nav_id, str):
                raise ValueError(f"tab id must be a string, got {type(nav_id).__name__}")
            if nav_id not in ALL_NAV_IDS:
                raise ValueError(f"{nav_id!r} is not a known nav id")
            if nav_id in seen:
                raise ValueError(f"{nav_id!r} appears more than once")
            seen.add(nav_id)
        self.conn.execute(
            "UPDATE workspaces SET tabs = ? WHERE id = ?",
            (json.dumps(tabs), self._ws_id()),
        )
        self.conn.commit()
        return list(tabs)

    def pin_tab(self, nav_id):
        """Append nav_id to the active workspace's tabs if not present.

        Raises ValueError if nav_id is not in ALL_NAV_IDS.
        Returns the new list.
        """
        if nav_id not in ALL_NAV_IDS:
            raise ValueError(f"{nav_id!r} is not a known nav id")
        tabs = self.get_tabs()
        if nav_id not in tabs:
            tabs.append(nav_id)
            self.conn.execute(
                "UPDATE workspaces SET tabs = ? WHERE id = ?",
                (json.dumps(tabs), self._ws_id()),
            )
            self.conn.commit()
        return tabs

    def unpin_tab(self, nav_id):
        """Remove nav_id from the active workspace's tabs if present.

        Raises ValueError if nav_id is not in ALL_NAV_IDS.
        Returns the new list.
        """
        if nav_id not in ALL_NAV_IDS:
            raise ValueError(f"{nav_id!r} is not a known nav id")
        tabs = self.get_tabs()
        if nav_id in tabs:
            tabs = [t for t in tabs if t != nav_id]
            self.conn.execute(
                "UPDATE workspaces SET tabs = ? WHERE id = ?",
                (json.dumps(tabs), self._ws_id()),
            )
            self.conn.commit()
        return tabs

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

    def add_workspace_folder(self, workspace_id, folder_id):
        """Link a folder to a workspace."""
        self.conn.execute(
            "INSERT OR IGNORE INTO workspace_folders (workspace_id, folder_id) VALUES (?, ?)",
            (workspace_id, folder_id),
        )
        self.conn.commit()
        # The folder's untracked files now count toward this workspace's
        # new-images backlog. Drop any stale cached payload so the next read
        # recomputes against the updated folder set.
        self._new_images_cache.invalidate_workspaces(self._db_path, [workspace_id])

    def remove_workspace_folder(self, workspace_id, folder_id):
        """Unlink a folder from a workspace."""
        self.conn.execute(
            "DELETE FROM workspace_folders WHERE workspace_id = ? AND folder_id = ?",
            (workspace_id, folder_id),
        )
        self.conn.commit()
        # The folder no longer contributes to this workspace's new-images
        # backlog. Drop the cached payload so the banner reflects the change.
        self._new_images_cache.invalidate_workspaces(self._db_path, [workspace_id])

    def get_workspace_folders(self, workspace_id):
        """Return all folders linked to a workspace."""
        return self.conn.execute(
            """SELECT f.* FROM folders f
               JOIN workspace_folders wf ON wf.folder_id = f.id
               WHERE wf.workspace_id = ?
               ORDER BY f.path""",
            (workspace_id,),
        ).fetchall()

    def move_folders_to_workspace(self, source_ws_id, target_ws_id, folder_ids):
        """Move folders and their workspace-scoped data to another workspace.

        Moves: workspace_folders rows and pending_changes. Detections and
        predictions are global (no workspace_id), so they follow the folder
        via workspace_folders membership rather than being reassigned.
        Collections and edit_history stay behind.

        Returns:
            dict with keys: folders_moved, pending_changes_moved
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
            return {"folders_moved": 0, "pending_changes_moved": 0}

        placeholders = ",".join("?" for _ in folder_ids)

        try:
            # Move pending_changes
            cur = self.conn.execute(
                f"""UPDATE pending_changes SET workspace_id = ?
                    WHERE workspace_id = ?
                    AND photo_id IN (SELECT id FROM photos WHERE folder_id IN ({placeholders}))""",
                [target_ws_id, source_ws_id] + list(folder_ids),
            )
            pending_changes_moved = cur.rowcount

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
                [target_ws_id, source_ws_id] + list(folder_ids),
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
                [source_ws_id, source_ws_id] + list(folder_ids),
            )

            # Move workspace_folders: remove from source, add to target
            self.conn.execute(
                f"DELETE FROM workspace_folders WHERE workspace_id = ? AND folder_id IN ({placeholders})",
                [source_ws_id] + list(folder_ids),
            )
            for fid in folder_ids:
                self.conn.execute(
                    "INSERT OR IGNORE INTO workspace_folders (workspace_id, folder_id) VALUES (?, ?)",
                    (target_ws_id, fid),
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

    def add_folder(self, path, name=None, parent_id=None):
        """Insert a folder. Automatically links it to the active workspace.

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
                "SELECT id FROM folders WHERE path = ?", (path,)
            ).fetchone()
            folder_id = row["id"]
        # Auto-link to active workspace
        if self._active_workspace_id is not None:
            self.add_workspace_folder(self._active_workspace_id, folder_id)
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

    def get_missing_photos(self):
        """Return photos whose source file is missing from disk.

        Scoped to the active workspace. Skips photos in folders flagged
        ``'missing'`` — those are surfaced by ``get_missing_folders`` and
        listing them per-photo would just duplicate that signal at high cost.

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
        """
        rows = self.conn.execute(
            """SELECT p.id, p.filename, p.extension, p.file_size,
                      p.timestamp, p.working_copy_path,
                      f.id AS folder_id, f.path AS folder_path
               FROM photos p
               JOIN folders f ON p.folder_id = f.id
               JOIN workspace_folders wf ON wf.folder_id = f.id
               WHERE wf.workspace_id = ? AND f.status != 'missing'
               ORDER BY f.path, p.filename""",
            (self._ws_id(),),
        ).fetchall()
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
        for row in rows:
            fid = row["folder_id"]
            if fid not in folder_online:
                folder_online[fid] = os.path.isdir(row["folder_path"])
            if not folder_online[fid]:
                # Whole folder is offline — surfaced by missing-folders flow.
                continue
            if fid not in folder_names:
                try:
                    names_set: set[str] = set()
                    with os.scandir(row["folder_path"]) as it:
                        for entry in it:
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
            names = folder_names[fid]
            if names is None:
                continue
            if _nfc(row["filename"]) in names:
                continue
            # NFC miss: defer to the kernel for case rules. On case-insensitive
            # volumes (APFS default, NTFS) os.path.exists resolves a
            # case-mismatched name; on case-sensitive volumes (most Linux
            # filesystems) it correctly reports the file as absent.
            if not os.path.exists(os.path.join(row["folder_path"], row["filename"])):
                missing.append(row)
        return missing

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
            "SELECT id, path FROM folders WHERE status = 'missing' AND path LIKE ? ORDER BY path",
            (old_path + "/%",),
        ).fetchall()
        for child in children:
            # Skip descendants of conflicted folders
            if any(child["path"].startswith(p + "/") for p in skipped_prefixes):
                continue
            relative = child["path"][len(old_path):]  # e.g. "/sub/dir"
            candidate = new_path + relative
            if os.path.exists(candidate):
                # Skip if another folder already has this path
                child_conflict = self.conn.execute(
                    "SELECT id FROM folders WHERE path = ? AND id != ?",
                    (candidate, child["id"]),
                ).fetchone()
                if child_conflict:
                    skipped_prefixes.append(child["path"])
                    continue
                self.conn.execute(
                    "UPDATE folders SET path = ?, status = 'ok' WHERE id = ?",
                    (candidate, child["id"]),
                )
                cascaded.append({"id": child["id"], "old_path": child["path"], "new_path": candidate})

        self.conn.commit()
        return cascaded

    def _merge_into_existing(self, source_folder_id, target_folder_id, new_path):
        """Merge photos from a missing folder into an existing folder at the same path.

        - Photos with matching filenames in the target are dropped from source
        - Other photos are reassigned to the target folder
        - The source folder entry is deleted
        - Missing child folders are cascade-relocated using old_path -> new_path

        Returns list of child folder dicts that were also relocated (same as relocate_folder).
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

        # Transfer workspace visibility from source to target
        self.conn.execute(
            "INSERT OR IGNORE INTO workspace_folders (workspace_id, folder_id) "
            "SELECT workspace_id, ? FROM workspace_folders WHERE folder_id = ?",
            (target_folder_id, source_folder_id),
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
            "SELECT id, path FROM folders WHERE status = 'missing' AND path LIKE ? ORDER BY path",
            (old_path + "/%",),
        ).fetchall()
        for child in children:
            if any(child["path"].startswith(p + "/") for p in skipped_prefixes):
                continue
            relative = child["path"][len(old_path):]
            candidate = new_path + relative
            if os.path.exists(candidate):
                child_conflict = self.conn.execute(
                    "SELECT id FROM folders WHERE path = ? AND id != ?",
                    (candidate, child["id"]),
                ).fetchone()
                if child_conflict:
                    skipped_prefixes.append(child["path"])
                    continue
                self.conn.execute(
                    "UPDATE folders SET path = ?, status = 'ok' WHERE id = ?",
                    (candidate, child["id"]),
                )
                cascaded.append({"id": child["id"], "old_path": child["path"], "new_path": candidate})

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

    def move_folder_path(self, folder_id, new_path):
        """Update a folder's path and cascade to all children.

        Unlike relocate_folder (which only updates missing children),
        this updates ALL child folders regardless of status.
        """
        old_row = self.conn.execute(
            "SELECT path FROM folders WHERE id = ?", (folder_id,)
        ).fetchone()
        if not old_row:
            return
        old_path = old_row["path"]
        self.conn.execute(
            "UPDATE folders SET path = ? WHERE id = ?", (new_path, folder_id)
        )
        children = self.conn.execute(
            "SELECT id, path FROM folders WHERE path LIKE ?",
            (old_path + "/%",),
        ).fetchall()
        for child in children:
            child_new = new_path + child["path"][len(old_path):]
            self.conn.execute(
                "UPDATE folders SET path = ? WHERE id = ?", (child_new, child["id"])
            )
        self.conn.commit()

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

    def delete_folder(self, folder_id):
        """Delete a folder and all its photos/data from the database.

        Returns dict with 'deleted_photos' count and 'files' (list from
        delete_photos) so the caller can remove cached thumbnails, previews,
        and working copies — the FK cascade drops preview_cache rows but
        leaves the on-disk files, which would otherwise become untracked
        orphans that eviction can't reclaim.
        """
        photo_ids = [
            row["id"]
            for row in self.conn.execute(
                "SELECT id FROM photos WHERE folder_id = ?", (folder_id,)
            ).fetchall()
        ]

        files = []
        if photo_ids:
            inner = self.delete_photos(photo_ids)
            files = inner.get("files", [])

        # Remove folder from workspace_folders and folders
        self.conn.execute("DELETE FROM workspace_folders WHERE folder_id = ?", (folder_id,))
        self.conn.execute("DELETE FROM folders WHERE id = ?", (folder_id,))
        self.conn.commit()

        return {"deleted_photos": len(photo_ids), "files": files}

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
                "SELECT id FROM photos WHERE file_hash = ? AND flag != 'rejected'",
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
            WHERE file_hash IS NOT NULL AND flag != 'rejected'
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
                   SUM(CASE WHEN flag != 'rejected' THEN 1 ELSE 0 END) AS kept,
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

        placeholders = ",".join("?" * len(photo_ids))
        rows = self.conn.execute(
            f"""SELECT p.id, p.filename, p.file_mtime, p.rating, p.flag,
                       f.path AS folder_path
                FROM photos p
                LEFT JOIN folders f ON f.id = p.folder_id
                WHERE p.id IN ({placeholders}) AND p.flag != 'rejected'""",
            list(photo_ids),
        ).fetchall()
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
            if loser_ids:
                loser_placeholders = ",".join("?" * len(loser_ids))
                self.conn.execute(
                    f"UPDATE photos SET flag = 'rejected' WHERE id IN ({loser_placeholders})",
                    loser_ids,
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
                   WHERE p.file_hash = ? AND p.flag != 'rejected'""",
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
                    latitude, longitude, companion_path, working_copy_path"""

    # Columns for single-photo detail queries (includes exif_data JSON +
    # eye-focus fields consumed by the review lightbox's crosshair overlay)
    PHOTO_DETAIL_COLS = (
        PHOTO_COLS + ", exif_data, eye_x, eye_y, eye_conf, eye_tenengrad"
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
        """Return photos for a list of IDs in a single query.

        Returns a dict mapping photo_id -> Row for efficient lookup.
        """
        if not photo_ids:
            return {}
        placeholders = ",".join("?" for _ in photo_ids)
        rows = self.conn.execute(
            f"SELECT {self.PHOTO_COLS} FROM photos WHERE id IN ({placeholders})",
            photo_ids,
        ).fetchall()
        return {row["id"]: row for row in rows}

    def count_photos(self):
        """Return photo count for the active workspace."""
        return self.conn.execute(
            """SELECT COUNT(*) FROM photos p
               JOIN workspace_folders wf ON wf.folder_id = p.folder_id
               JOIN folders f ON f.id = p.folder_id AND f.status IN ('ok', 'partial')
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
        """Return count of keywords used by photos in the active workspace."""
        return self.conn.execute(
            """SELECT COUNT(DISTINCT pk.keyword_id)
               FROM photo_keywords pk
               JOIN photos p ON p.id = pk.photo_id
               JOIN workspace_folders wf ON wf.folder_id = p.folder_id
               JOIN folders f ON f.id = p.folder_id AND f.status IN ('ok', 'partial')
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

    def get_coverage_stats(self):
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
        photo_row = self.conn.execute(
            f"""SELECT
                COUNT(*) AS total,
                {self._coverage_select_fragment()}
            FROM photos p
            JOIN workspace_folders wf ON wf.folder_id = p.folder_id
            JOIN folders f ON f.id = p.folder_id AND f.status IN ('ok', 'partial')
            WHERE wf.workspace_id = ?""",
            (ws,),
        ).fetchone()
        detected = self.conn.execute(
            """SELECT COUNT(DISTINCT d.photo_id)
               FROM detections d
               JOIN photos p ON p.id = d.photo_id
               JOIN workspace_folders wf ON wf.folder_id = p.folder_id
               JOIN folders f ON f.id = p.folder_id AND f.status IN ('ok', 'partial')
               WHERE wf.workspace_id = ?
                 AND d.detector_confidence >= ?""",
            (ws, min_conf),
        ).fetchone()[0] or 0
        classified = self.conn.execute(
            """SELECT COUNT(DISTINCT d.photo_id)
               FROM predictions pr
               JOIN detections d ON d.id = pr.detection_id
               JOIN photos p ON p.id = d.photo_id
               JOIN workspace_folders wf ON wf.folder_id = p.folder_id
               JOIN folders f ON f.id = p.folder_id AND f.status IN ('ok', 'partial')
               WHERE wf.workspace_id = ?
                 AND d.detector_confidence >= ?""",
            (ws, min_conf),
        ).fetchone()[0] or 0
        result = {"total": photo_row["total"] or 0}
        for key, _ in self._COVERAGE_PHOTO_COLUMNS:
            result[key] = photo_row[key] or 0
        result["detected"] = detected
        result["classified"] = classified
        return result

    def get_folder_coverage_stats(self):
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
        photo_rows = self.conn.execute(
            f"""SELECT
                f.id AS folder_id,
                f.path AS path,
                f.name AS name,
                COUNT(p.id) AS total,
                {self._coverage_select_fragment()}
            FROM folders f
            JOIN workspace_folders wf ON wf.folder_id = f.id
            LEFT JOIN photos p ON p.folder_id = f.id
            WHERE wf.workspace_id = ? AND f.status IN ('ok', 'partial')
            GROUP BY f.id
            ORDER BY f.path""",
            (ws,),
        ).fetchall()
        det_rows = self.conn.execute(
            """SELECT p.folder_id AS folder_id,
                      COUNT(DISTINCT d.photo_id) AS detected
               FROM detections d
               JOIN photos p ON p.id = d.photo_id
               JOIN workspace_folders wf ON wf.folder_id = p.folder_id
               JOIN folders f ON f.id = p.folder_id AND f.status IN ('ok', 'partial')
               WHERE wf.workspace_id = ?
                 AND d.detector_confidence >= ?
               GROUP BY p.folder_id""",
            (ws, min_conf),
        ).fetchall()
        cls_rows = self.conn.execute(
            """SELECT p.folder_id AS folder_id,
                      COUNT(DISTINCT d.photo_id) AS classified
               FROM predictions pr
               JOIN detections d ON d.id = pr.detection_id
               JOIN photos p ON p.id = d.photo_id
               JOIN workspace_folders wf ON wf.folder_id = p.folder_id
               JOIN folders f ON f.id = p.folder_id AND f.status IN ('ok', 'partial')
               WHERE wf.workspace_id = ?
                 AND d.detector_confidence >= ?
               GROUP BY p.folder_id""",
            (ws, min_conf),
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

    def get_pipeline_feature_counts(self):
        """Return counts of photos with masks, detections, and sharpness data.

        Detections are global: the per-workspace scope comes from
        ``workspace_folders``, and low-confidence rows are filtered out at
        read time using the workspace-effective ``detector_confidence``.
        """
        import config as cfg
        ws = self._ws_id()
        min_conf = self.get_effective_config(cfg.load()).get(
            "detector_confidence", 0.2
        )
        row = self.conn.execute(
            """SELECT
                SUM(CASE WHEN p.mask_path IS NOT NULL THEN 1 ELSE 0 END) as masks,
                SUM(CASE WHEN p.subject_tenengrad IS NOT NULL THEN 1 ELSE 0 END) as sharpness
            FROM photos p
            JOIN workspace_folders wf ON wf.folder_id = p.folder_id
            WHERE wf.workspace_id = ?""",
            (ws,),
        ).fetchone()
        det_count = self.conn.execute(
            """SELECT COUNT(DISTINCT d.photo_id)
               FROM detections d
               JOIN photos p ON p.id = d.photo_id
               JOIN workspace_folders wf ON wf.folder_id = p.folder_id
               WHERE wf.workspace_id = ?
                 AND d.detector_confidence >= ?""",
            (ws, min_conf),
        ).fetchone()[0]
        return {
            "masks": row["masks"] or 0,
            "detections": det_count or 0,
            "sharpness": row["sharpness"] or 0,
        }

    def get_dashboard_stats(self):
        """Return aggregate statistics for the dashboard."""
        ws = self._ws_id()
        # Hoisted: multiple queries below need the workspace-effective
        # detector_confidence to keep classified_count / prediction_status /
        # detected_count in sync as the threshold moves.
        import config as cfg
        min_conf = self.get_effective_config(cfg.load()).get(
            "detector_confidence", 0.2
        )

        top_keywords = self.conn.execute(
            """SELECT k.name, k.is_species, COUNT(pk.photo_id) as photo_count
               FROM keywords k
               JOIN photo_keywords pk ON pk.keyword_id = k.id
               JOIN photos p ON p.id = pk.photo_id
               JOIN workspace_folders wf ON wf.folder_id = p.folder_id
               JOIN folders f ON f.id = p.folder_id AND f.status IN ('ok', 'partial')
               WHERE wf.workspace_id = ?
               GROUP BY k.id
               ORDER BY photo_count DESC
               LIMIT 30""",
            (ws,),
        ).fetchall()

        photos_by_month = self.conn.execute(
            """SELECT substr(p.timestamp, 1, 7) as month, COUNT(*) as count
            FROM photos p
            JOIN workspace_folders wf ON wf.folder_id = p.folder_id
            JOIN folders f ON f.id = p.folder_id AND f.status IN ('ok', 'partial')
            WHERE p.timestamp IS NOT NULL AND wf.workspace_id = ?
            GROUP BY month
            ORDER BY month""",
            (ws,),
        ).fetchall()

        rating_dist = self.conn.execute(
            """SELECT p.rating, COUNT(*) as count
            FROM photos p
            JOIN workspace_folders wf ON wf.folder_id = p.folder_id
            JOIN folders f ON f.id = p.folder_id AND f.status IN ('ok', 'partial')
            WHERE wf.workspace_id = ?
            GROUP BY p.rating
            ORDER BY p.rating""",
            (ws,),
        ).fetchall()

        flag_dist = self.conn.execute(
            """SELECT p.flag, COUNT(*) as count
            FROM photos p
            JOIN workspace_folders wf ON wf.folder_id = p.folder_id
            JOIN folders f ON f.id = p.folder_id AND f.status IN ('ok', 'partial')
            WHERE wf.workspace_id = ?
            GROUP BY p.flag""",
            (ws,),
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
            """SELECT COALESCE(pr_rev.status, 'pending') AS status,
                      COUNT(*) AS count
               FROM predictions pr
               JOIN detections d ON d.id = pr.detection_id
               JOIN photos ph ON ph.id = d.photo_id
               JOIN workspace_folders wf
                 ON wf.folder_id = ph.folder_id AND wf.workspace_id = ?
               LEFT JOIN prediction_review pr_rev
                 ON pr_rev.prediction_id = pr.id AND pr_rev.workspace_id = ?
               WHERE d.detector_confidence >= ?
                 AND pr.labels_fingerprint = (
                    SELECT pr2.labels_fingerprint FROM predictions pr2
                    WHERE pr2.detection_id = pr.detection_id
                      AND pr2.classifier_model = pr.classifier_model
                    ORDER BY pr2.created_at DESC, pr2.id DESC
                    LIMIT 1
                 )
               GROUP BY COALESCE(pr_rev.status, 'pending')""",
            (ws, ws, min_conf),
        ).fetchall()

        # Same threshold + fingerprint rules as prediction_status above, so
        # classified_count can't drift above detected_count as the threshold
        # moves.
        classified_count = self.conn.execute(
            """SELECT COUNT(DISTINCT d.photo_id)
               FROM predictions pr
               JOIN detections d ON d.id = pr.detection_id
               JOIN photos ph ON ph.id = d.photo_id
               JOIN workspace_folders wf
                 ON wf.folder_id = ph.folder_id AND wf.workspace_id = ?
               WHERE d.detector_confidence >= ?
                 AND pr.labels_fingerprint = (
                    SELECT pr2.labels_fingerprint FROM predictions pr2
                    WHERE pr2.detection_id = pr.detection_id
                      AND pr2.classifier_model = pr.classifier_model
                    ORDER BY pr2.created_at DESC, pr2.id DESC
                    LIMIT 1
                 )""",
            (ws, min_conf),
        ).fetchone()[0]

        photos_by_hour = self.conn.execute(
            """SELECT CAST(substr(p.timestamp, 12, 2) AS INTEGER) as hour, COUNT(*) as count
            FROM photos p
            JOIN workspace_folders wf ON wf.folder_id = p.folder_id
            JOIN folders f ON f.id = p.folder_id AND f.status IN ('ok', 'partial')
            WHERE p.timestamp IS NOT NULL AND length(p.timestamp) >= 13 AND wf.workspace_id = ?
            GROUP BY hour
            ORDER BY hour""",
            (ws,),
        ).fetchall()

        quality_dist = self.conn.execute(
            """SELECT
                CASE
                    WHEN p.quality_score IS NULL THEN -1
                    ELSE CAST(p.quality_score * 10 AS INTEGER)
                END as bucket,
                COUNT(*) as count
            FROM photos p
            JOIN workspace_folders wf ON wf.folder_id = p.folder_id
            JOIN folders f ON f.id = p.folder_id AND f.status IN ('ok', 'partial')
            WHERE wf.workspace_id = ?
            GROUP BY bucket
            ORDER BY bucket""",
            (ws,),
        ).fetchall()

        # min_conf already hoisted at top of get_dashboard_stats.
        detected_count = self.conn.execute(
            """SELECT COUNT(DISTINCT d.photo_id)
               FROM detections d
               JOIN photos p ON p.id = d.photo_id
               JOIN workspace_folders wf ON wf.folder_id = p.folder_id
               JOIN folders f ON f.id = p.folder_id AND f.status IN ('ok', 'partial')
               WHERE wf.workspace_id = ?
                 AND d.detector_confidence >= ?""",
            (ws, min_conf),
        ).fetchone()[0]

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
        }

    def get_calendar_data(self, year, folder_id=None, rating_min=None, keyword=None, color_label=None):
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
        if keyword is not None:
            join_clause += """
                LEFT JOIN photo_keywords pk ON pk.photo_id = p.id
                LEFT JOIN keywords k ON k.id = pk.keyword_id
            """
            conditions.append("(k.name LIKE ? OR p.filename LIKE ?)")
            where_params.append(f"%{keyword}%")
            where_params.append(f"%{keyword}%")
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
        page=1,
        per_page=50,
        sort="date",
        rating_min=None,
        date_from=None,
        date_to=None,
        keyword=None,
        color_label=None,
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
        if rating_min is not None:
            conditions.append("p.rating >= ?")
            where_params.append(rating_min)
        if date_from is not None:
            conditions.append("p.timestamp >= ?")
            where_params.append(date_from)
        if date_to is not None:
            conditions.append("p.timestamp <= ?")
            where_params.append(_inclusive_date_to(date_to))

        join_clause = ("JOIN workspace_folders wf ON wf.folder_id = p.folder_id"
                       "\nJOIN folders f ON f.id = p.folder_id AND f.status IN ('ok', 'partial')")
        if keyword is not None:
            join_clause += """
                LEFT JOIN photo_keywords pk ON pk.photo_id = p.id
                LEFT JOIN keywords k ON k.id = pk.keyword_id
            """
            conditions.append("(k.name LIKE ? OR p.filename LIKE ?)")
            where_params.append(f"%{keyword}%")
            where_params.append(f"%{keyword}%")

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
            "date": "p.timestamp ASC, p.filename ASC, p.id ASC",
            "date_desc": "p.timestamp DESC, p.filename ASC, p.id ASC",
            "name": "p.filename ASC, p.id ASC",
            "name_desc": "p.filename DESC, p.id ASC",
            "rating": "p.rating DESC, p.filename ASC, p.id ASC",
            "sharpness": "p.sharpness DESC, p.filename ASC, p.id ASC",
            "sharpness_asc": "p.sharpness ASC, p.filename ASC, p.id ASC",
            "quality": "p.quality_score DESC, p.filename ASC, p.id ASC",
        }
        order = sort_map.get(sort, "p.timestamp ASC, p.filename ASC, p.id ASC")

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

    def count_filtered_photos(
        self,
        folder_id=None,
        rating_min=None,
        date_from=None,
        date_to=None,
        keyword=None,
        color_label=None,
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
        if rating_min is not None:
            conditions.append("p.rating >= ?")
            where_params.append(rating_min)
        if date_from is not None:
            conditions.append("p.timestamp >= ?")
            where_params.append(date_from)
        if date_to is not None:
            conditions.append("p.timestamp <= ?")
            where_params.append(_inclusive_date_to(date_to))

        join_clause = ("JOIN workspace_folders wf ON wf.folder_id = p.folder_id"
                       "\nJOIN folders f ON f.id = p.folder_id AND f.status IN ('ok', 'partial')")
        if keyword is not None:
            join_clause += """
                LEFT JOIN photo_keywords pk ON pk.photo_id = p.id
                LEFT JOIN keywords k ON k.id = pk.keyword_id
            """
            conditions.append("(k.name LIKE ? OR p.filename LIKE ?)")
            where_params.append(f"%{keyword}%")
            where_params.append(f"%{keyword}%")

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
        collection_id=None,
        color_label=None,
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
            join_clause += """
                LEFT JOIN photo_keywords pk ON pk.photo_id = p.id
                LEFT JOIN keywords k ON k.id = pk.keyword_id
            """
            conditions.append("(k.name LIKE ? OR p.filename LIKE ?)")
            where_params.append(f"%{keyword}%")
            where_params.append(f"%{keyword}%")

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
            join_clause += """
                LEFT JOIN photo_keywords pk ON pk.photo_id = p.id
                LEFT JOIN keywords k ON k.id = pk.keyword_id
            """
            conditions.append("(k.name LIKE ? OR p.filename LIKE ?)")
            params.append(f"%{keyword}%")
            params.append(f"%{keyword}%")

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
            ORDER BY p.timestamp ASC, p.filename ASC, p.id ASC
        """
        return self.conn.execute(query, species_col_params + params).fetchall()

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
        if verify_workspace:
            self._verify_photo_in_workspace(photo_id)
        self.conn.execute(
            "UPDATE photos SET rating = ? WHERE id = ?", (rating, photo_id)
        )
        self.conn.commit()

    def batch_update_photo_rating(self, photo_ids, rating, verify_workspace=True):
        """Set rating for multiple photos in a single transaction.

        Args:
            verify_workspace: when True, raises ValueError if any photo is
                not in the active workspace.
        """
        if not photo_ids:
            return
        if verify_workspace:
            for pid in photo_ids:
                self._verify_photo_in_workspace(pid)
        placeholders = ",".join("?" for _ in photo_ids)
        self.conn.execute(
            f"UPDATE photos SET rating = ? WHERE id IN ({placeholders})",
            [rating] + list(photo_ids),
        )
        self.conn.commit()

    def update_photo_flag(self, photo_id, flag, verify_workspace=True):
        """Set photo flag ('none', 'flagged', 'rejected').

        Args:
            verify_workspace: when True (the default), raises ValueError if
                the photo is not in the active workspace's folders.
        """
        if verify_workspace:
            self._verify_photo_in_workspace(photo_id)
        self.conn.execute("UPDATE photos SET flag = ? WHERE id = ?", (flag, photo_id))
        self.conn.commit()

    def batch_update_photo_flag(self, photo_ids, flag, verify_workspace=True):
        """Set flag for multiple photos in a single transaction.

        Args:
            verify_workspace: when True, raises ValueError if any photo is
                not in the active workspace.
        """
        if not photo_ids:
            return
        if verify_workspace:
            for pid in photo_ids:
                self._verify_photo_in_workspace(pid)
        placeholders = ",".join("?" for _ in photo_ids)
        self.conn.execute(
            f"UPDATE photos SET flag = ? WHERE id IN ({placeholders})",
            [flag] + list(photo_ids),
        )
        self.conn.commit()

    VALID_COLOR_LABELS = ('red', 'yellow', 'green', 'blue', 'purple')

    def set_color_label(self, photo_id, color):
        """Set a color label for a photo in the active workspace."""
        if color not in self.VALID_COLOR_LABELS:
            raise ValueError(f"Invalid color label: {color}. Must be one of {self.VALID_COLOR_LABELS}")
        self.conn.execute(
            "INSERT OR REPLACE INTO photo_color_labels (photo_id, workspace_id, color) VALUES (?, ?, ?)",
            (photo_id, self._ws_id(), color),
        )
        self.conn.commit()

    def remove_color_label(self, photo_id):
        """Remove the color label for a photo in the active workspace."""
        self.conn.execute(
            "DELETE FROM photo_color_labels WHERE photo_id = ? AND workspace_id = ?",
            (photo_id, self._ws_id()),
        )
        self.conn.commit()

    def get_color_label(self, photo_id):
        """Return the color label for a photo in the active workspace, or None."""
        row = self.conn.execute(
            "SELECT color FROM photo_color_labels WHERE photo_id = ? AND workspace_id = ?",
            (photo_id, self._ws_id()),
        ).fetchone()
        return row['color'] if row else None

    def get_color_labels_for_photos(self, photo_ids):
        """Return a dict of {photo_id: color} for the active workspace."""
        if not photo_ids:
            return {}
        placeholders = ",".join("?" for _ in photo_ids)
        rows = self.conn.execute(
            f"SELECT photo_id, color FROM photo_color_labels WHERE workspace_id = ? AND photo_id IN ({placeholders})",
            [self._ws_id()] + list(photo_ids),
        ).fetchall()
        return {row['photo_id']: row['color'] for row in rows}

    def batch_set_color_label(self, photo_ids, color):
        """Set or remove color label for multiple photos in the active workspace."""
        if not photo_ids:
            return
        ws_id = self._ws_id()
        if color is None:
            placeholders = ",".join("?" for _ in photo_ids)
            self.conn.execute(
                f"DELETE FROM photo_color_labels WHERE workspace_id = ? AND photo_id IN ({placeholders})",
                [ws_id] + list(photo_ids),
            )
        else:
            if color not in self.VALID_COLOR_LABELS:
                raise ValueError(f"Invalid color label: {color}. Must be one of {self.VALID_COLOR_LABELS}")
            for pid in photo_ids:
                self.conn.execute(
                    "INSERT OR REPLACE INTO photo_color_labels (photo_id, workspace_id, color) VALUES (?, ?, ?)",
                    (pid, ws_id, color),
                )
        self.conn.commit()

    def delete_photos(self, photo_ids, include_companions=False):
        """Delete photos and all associated data.

        Returns dict with 'deleted' count and 'files' list of
        {photo_id, folder_path, filename, companion_path} for file cleanup.
        """
        if not photo_ids:
            return {"deleted": 0, "files": []}

        # Resolve to actual existing photos
        placeholders = ",".join("?" for _ in photo_ids)
        rows = self.conn.execute(
            f"SELECT p.id, p.filename, p.companion_path, p.folder_id, f.path AS folder_path "
            f"FROM photos p JOIN folders f ON p.folder_id = f.id "
            f"WHERE p.id IN ({placeholders})",
            list(photo_ids),
        ).fetchall()

        if not rows:
            return {"deleted": 0, "files": []}

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
                comp_ph = ",".join("?" for _ in companion_ids)
                comp_rows = self.conn.execute(
                    f"SELECT p.id, p.filename, p.companion_path, p.folder_id, f.path AS folder_path "
                    f"FROM photos p JOIN folders f ON p.folder_id = f.id "
                    f"WHERE p.id IN ({comp_ph})",
                    companion_ids,
                ).fetchall()
                rows = list(rows) + list(comp_rows)

        all_ids = list({row["id"] for row in rows})
        ph = ",".join("?" for _ in all_ids)

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

        try:
            # Delete associated data (non-cascading FKs)
            self.conn.execute(f"DELETE FROM photo_keywords WHERE photo_id IN ({ph})", all_ids)
            self.conn.execute(f"DELETE FROM pending_changes WHERE photo_id IN ({ph})", all_ids)
            # Deleting detections cascades to predictions via ON DELETE CASCADE
            self.conn.execute(f"DELETE FROM detections WHERE photo_id IN ({ph})", all_ids)

            # Clean collection rules
            import json as _json
            collections = self.conn.execute(
                "SELECT id, rules FROM collections WHERE workspace_id = ?",
                (self._ws_id(),),
            ).fetchall()
            deleted_set = set(all_ids)
            for coll in collections:
                rules = _json.loads(coll["rules"])
                changed = False
                for rule in rules:
                    if rule.get("field") == "photo_ids" and "value" in rule:
                        original_len = len(rule["value"])
                        rule["value"] = [v for v in rule["value"] if v not in deleted_set]
                        if len(rule["value"]) != original_len:
                            changed = True
                if changed:
                    self.conn.execute(
                        "UPDATE collections SET rules = ? WHERE id = ?",
                        (_json.dumps(rules), coll["id"]),
                    )

            # Delete photos (cascades to edit_history_items, inat_submissions)
            self.conn.execute(f"DELETE FROM photos WHERE id IN ({ph})", all_ids)

            # Update folder counts
            for fid, count in folder_counts.items():
                self.conn.execute(
                    "UPDATE folders SET photo_count = photo_count - ? WHERE id = ?",
                    (count, fid),
                )

            self.conn.commit()
        except Exception:
            self.conn.rollback()
            raise
        finally:
            # Always invalidate — even on rollback we may have partially dirtied
            # state, and on success the removed rows mean untracked on-disk
            # files should re-surface as "new" on the next read.
            if affected_folder_ids:
                self.invalidate_new_images_cache_for_folders(affected_folder_ids)
        return {"deleted": len(all_ids), "files": files}

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

    def update_photo_mask(self, photo_id, mask_path):
        """Store the mask file path for a photo."""
        self.conn.execute(
            "UPDATE photos SET mask_path=? WHERE id=?",
            (mask_path, photo_id),
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
                    ORDER BY p.id, d.detector_confidence DESC""",
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
                   ORDER BY p.id, d.detector_confidence DESC""",
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
          * has not already been processed — eye_tenengrad IS NULL keeps
            the stage idempotent across reruns
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
        ws_id = self._ws_id()
        min_conf = self.get_effective_config(cfg.load()).get(
            "detector_confidence", 0.2
        )
        if photo_ids is not None:
            photo_ids = list(photo_ids)
            if not photo_ids:
                return []
            placeholders = ",".join("?" for _ in photo_ids)
            extra_where = f" AND p.id IN ({placeholders})"
            params = (ws_id, min_conf, *photo_ids)
        else:
            extra_where = ""
            params = (ws_id, min_conf)
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
                 AND p.eye_tenengrad IS NULL{extra_where}
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
                return words[0].capitalize() + " " + " ".join(w.lower() for w in words[1:])
            return name.capitalize()
        elif convention == "title":
            # Title Case: "Black Phoebe"
            return name.title()
        return name

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
                    f"SELECT id FROM keywords WHERE name = ? COLLATE NOCASE "
                    f"AND parent_id IS NULL "
                    f"ORDER BY {type_priority_case}, id ASC LIMIT 1",
                    (name,),
                ).fetchone()
            else:
                existing = self.conn.execute(
                    "SELECT id FROM keywords WHERE name = ? COLLATE NOCASE "
                    "AND parent_id IS NULL AND type IN (?, 'general') "
                    "ORDER BY (type = ?) DESC, id ASC LIMIT 1",
                    (name, kw_type, kw_type),
                ).fetchone()
        else:
            if kw_type is None:
                existing = self.conn.execute(
                    f"SELECT id FROM keywords WHERE name = ? COLLATE NOCASE "
                    f"AND parent_id = ? "
                    f"ORDER BY {type_priority_case}, id ASC LIMIT 1",
                    (name, parent_id),
                ).fetchone()
            else:
                existing = self.conn.execute(
                    "SELECT id FROM keywords WHERE name = ? COLLATE NOCASE "
                    "AND parent_id = ? AND type IN (?, 'general') "
                    "ORDER BY (type = ?) DESC, id ASC LIMIT 1",
                    (name, parent_id, kw_type, kw_type),
                ).fetchone()
        if existing:
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

        taxon_id = None
        if kw_type is None:
            # Auto-detect taxonomy type from taxa table
            kw_type = 'general'
            if is_species:
                kw_type = 'taxonomy'
            else:
                # Check if name matches a known taxon (common name or scientific name)
                taxon = self.conn.execute(
                    """SELECT t.id FROM taxa t
                       WHERE t.common_name = ? COLLATE NOCASE
                          OR t.name = ? COLLATE NOCASE
                       LIMIT 1""",
                    (name, name),
                ).fetchone()
                if not taxon:
                    taxon = self.conn.execute(
                        """SELECT t.taxon_id AS id FROM taxa_common_names t
                           WHERE t.name = ? COLLATE NOCASE
                           LIMIT 1""",
                        (name,),
                    ).fetchone()
                if taxon:
                    kw_type = 'taxonomy'
                    taxon_id = taxon["id"]

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

    def _upsert_location_parent_chain(self, components):
        """Upsert a chain of parent location keywords from ``address_components``.

        Walks broadest → narrowest, returning the list of visited keyword ids
        in broadest → narrowest order. Returns an empty list if ``components``
        is empty / all entries lack a name. The deepest (narrowest) parent is
        ``chain[-1]`` if non-empty. Caller is responsible for the surrounding
        transaction.
        """
        chain: list[int] = []
        parent_id = None
        # Reverse Google's narrowest-first list to walk broadest → narrowest.
        for comp in reversed(components or []):
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
        ``lng``, ``address_components`` (Google's narrowest-first order).

        Each ``address_component`` becomes a parent ``type='location'``
        keyword chained via ``parent_id``. Per Task 4's finding, Google's
        standard responses do NOT carry a per-component ``place_id``, so
        parents dedupe on ``(name, parent_id)`` and only the leaf carries
        ``place_id``/coords.

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
            chain = self._upsert_location_parent_chain(components)
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
            chain = self._upsert_location_parent_chain(components)
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
        """Find and merge case-insensitive duplicate keywords in active workspace.

        Only merges keywords that are used by photos in the active workspace.
        Keeps the lowest ID (earliest created), moves all photo associations,
        and deletes the duplicates. Returns count of merges performed.
        """
        ws = self._ws_id()
        dupes = self.conn.execute(
            """SELECT LOWER(k.name) as lname, MIN(k.id) as keep_id,
                      GROUP_CONCAT(DISTINCT k.id) as all_ids
               FROM keywords k
               JOIN photo_keywords pk ON pk.keyword_id = k.id
               JOIN photos p ON p.id = pk.photo_id
               JOIN workspace_folders wf ON wf.folder_id = p.folder_id
               WHERE wf.workspace_id = ?
               GROUP BY LOWER(k.name) HAVING COUNT(DISTINCT k.id) > 1""",
            (ws,),
        ).fetchall()

        merged = 0
        for d in dupes:
            keep_id = d["keep_id"]
            all_ids = [int(x) for x in d["all_ids"].split(",")]
            remove_ids = [x for x in all_ids if x != keep_id]

            for rid in remove_ids:
                # Move photo associations (ignore if already exists for keep_id)
                self.conn.execute(
                    "UPDATE OR IGNORE photo_keywords SET keyword_id = ? WHERE keyword_id = ?",
                    (keep_id, rid),
                )
                # Delete orphaned associations
                self.conn.execute(
                    "DELETE FROM photo_keywords WHERE keyword_id = ?", (rid,)
                )
                # Delete the duplicate keyword
                self.conn.execute("DELETE FROM keywords WHERE id = ?", (rid,))
                merged += 1

        if merged:
            self.conn.commit()
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

    def get_species_keywords_for_photos(self, photo_ids):
        """Return species (taxonomy) keyword names for a batch of photos.

        Returns a dict mapping photo_id -> list of species name strings.
        """
        if not photo_ids:
            return {}
        placeholders = ",".join("?" for _ in photo_ids)
        rows = self.conn.execute(
            f"""SELECT pk.photo_id, k.name
                FROM photo_keywords pk
                JOIN keywords k ON k.id = pk.keyword_id
                WHERE pk.photo_id IN ({placeholders})
                  AND k.is_species = 1
                ORDER BY k.name""",
            list(photo_ids),
        ).fetchall()
        result = {}
        for r in rows:
            result.setdefault(r["photo_id"], []).append(r["name"])
        return result

    def get_highlights_candidates(self, folder_id, min_quality=0.0):
        """Return photos eligible for highlights selection.

        When ``folder_id`` is an int, returns photos in that folder and its
        descendant folders. When ``folder_id`` is ``None``, returns photos
        across every folder visible in the active workspace — useful when a
        photoshoot spans multiple dated folders (Vireo auto-organizes imports
        by EXIF capture date into ``YYYY/YYYY-MM-DD/`` subfolders).

        In both cases, only photos with ``quality_score >= min_quality`` that
        are not user-rejected are returned. Includes the photo's species
        keyword (or NULL) and DINO embeddings for MMR diversity.

        Species is derived from photo_keywords joined to keywords where
        is_species = 1, which covers both accepted predictions (accept_prediction
        tags the photo) and manual identification via the confirm-species flow.

        Ordered by quality_score DESC.
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
        rows = self.conn.execute(
            f"""SELECT p.id, p.folder_id, p.filename, p.extension,
                      p.timestamp, p.width, p.height, p.rating, p.flag,
                      p.thumb_path, p.quality_score, p.subject_sharpness,
                      p.subject_size, p.sharpness, p.phash_crop,
                      p.dino_subject_embedding, p.dino_global_embedding,
                      bp.species
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
                       WHERE k.is_species = 1
                   ) WHERE rn = 1
               ) bp ON bp.photo_id = p.id
               WHERE wf.workspace_id = ?
                 {folder_filter}
                 AND p.quality_score IS NOT NULL
                 AND p.quality_score >= ?
                 AND p.flag != 'rejected'
               ORDER BY p.quality_score DESC""",
            (ws, *folder_params, min_quality),
        ).fetchall()
        return rows

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
        kwargs always win over auto-detection.
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
            return

        # Auto-retype on rename: same logic as add_keyword. Only fires
        # on an actual name change so idempotent PUT-style updates
        # (client re-sending the existing name) don't unexpectedly
        # reclassify a 'general' keyword once the taxa table is
        # populated.
        if 'name' in updates:
            new_name = updates['name']
            current = self.conn.execute(
                "SELECT name, type, taxon_id FROM keywords WHERE id = ?",
                (keyword_id,),
            ).fetchone()
            if current is not None and new_name != current["name"]:
                cur_type = current["type"]
                taxon = self.conn.execute(
                    """SELECT t.id FROM taxa t
                       WHERE t.common_name = ? COLLATE NOCASE
                          OR t.name = ? COLLATE NOCASE
                       LIMIT 1""",
                    (new_name, new_name),
                ).fetchone()
                if not taxon:
                    taxon = self.conn.execute(
                        """SELECT t.taxon_id AS id FROM taxa_common_names t
                           WHERE t.name = ? COLLATE NOCASE
                           LIMIT 1""",
                        (new_name,),
                    ).fetchone()

                if cur_type == 'general':
                    # Only promote to taxonomy if a match exists; otherwise
                    # leave type/taxon_id alone.
                    if taxon:
                        updates.setdefault('type', 'taxonomy')
                        # Gate taxon_id on the EFFECTIVE type so an
                        # explicit non-taxonomy type kwarg (e.g.
                        # type='location') doesn't end up with a
                        # taxonomy link. Mirror add_keyword's invariant
                        # for the auto-promoted case: type='taxonomy'
                        # backed by a matched taxon implies is_species=1.
                        if updates.get('type') == 'taxonomy':
                            updates.setdefault('taxon_id', taxon["id"])
                            updates['is_species'] = 1
                elif (cur_type == 'taxonomy' and taxon
                      and updates.get('type', 'taxonomy') == 'taxonomy'):
                    # Already taxonomy: refresh taxon_id only if the new
                    # name matches a (possibly different) taxon AND the
                    # effective type stays 'taxonomy' (caller may demote
                    # to 'location' etc.). If no match, leave the existing
                    # link in place.
                    updates.setdefault('taxon_id', taxon["id"])
                # Other manual types ('location', 'people', etc.) are
                # preserved — user intent wins.

        set_clause = ", ".join(f"{k} = ?" for k in updates)
        values = list(updates.values()) + [keyword_id]
        self.conn.execute(f"UPDATE keywords SET {set_clause} WHERE id = ?", values)
        self.conn.commit()

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
               )
               SELECT k.id, k.name, k.parent_id, k.type, k.taxon_id,
                      k.latitude, k.longitude, k.place_id,
                      t.name AS taxon_name, t.common_name AS taxon_common_name,
                      COUNT(ws_photo.photo_id) AS photo_count
               FROM keywords k
               JOIN ancestors a ON a.id = k.id
               LEFT JOIN taxa t ON t.id = k.taxon_id
               LEFT JOIN (
                   SELECT pk.keyword_id, pk.photo_id
                   FROM photo_keywords pk
                   JOIN photos p ON p.id = pk.photo_id
                   JOIN workspace_folders wf ON wf.folder_id = p.folder_id
                   WHERE wf.workspace_id = ?
               ) ws_photo ON ws_photo.keyword_id = k.id
               GROUP BY k.id
               ORDER BY k.name""",
            (ws, ws),
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

        if collection_photo_ids is not None:
            placeholders = ",".join("?" for _ in collection_photo_ids)
            extra_conds.append(f"d.photo_id IN ({placeholders})")
            extra_params.extend(collection_photo_ids)

        where_clause = (" WHERE " + " AND ".join(extra_conds)) if extra_conds else ""
        self.conn.execute(
            f"""DELETE FROM predictions WHERE id IN (
                SELECT pr.id FROM predictions pr
                JOIN detections d ON d.id = pr.detection_id
                JOIN photos ph ON ph.id = d.photo_id
                JOIN workspace_folders wf
                  ON wf.folder_id = ph.folder_id AND wf.workspace_id = ?
                {where_clause}
            )""",
            [ws, *extra_params],
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
        run_conds = []
        run_params = []
        if model is not None:
            run_conds.append("cr.classifier_model = ?")
            run_params.append(model)
        if labels_fingerprint is not None:
            run_conds.append("cr.labels_fingerprint = ?")
            run_params.append(labels_fingerprint)
        if collection_photo_ids is not None:
            placeholders = ",".join("?" for _ in collection_photo_ids)
            run_conds.append(f"d.photo_id IN ({placeholders})")
            run_params.extend(collection_photo_ids)
        run_where = (" WHERE " + " AND ".join(run_conds)) if run_conds else ""
        # Single set-based DELETE via a rowid subquery — the previous
        # SELECT + per-row DELETE loop issued one statement per matching
        # run, which on a reclassify of a multi-thousand-detection
        # workspace dominates wall time on the startup-blocking thread.
        # Match semantics are identical: the subquery shape is the same
        # (JOIN through detections/photos/workspace_folders, same
        # optional filters), and rowid uniquely identifies each
        # classifier_runs row under the implicit-rowid default.
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
        conditions = ["wf.workspace_id = ?"]
        params = [ws, ws]  # first ? = pr_rev.workspace_id, second = wf.workspace_id
        if photo_ids is not None:
            placeholders = ",".join("?" for _ in photo_ids)
            conditions.append(f"d.photo_id IN ({placeholders})")
            params.extend(photo_ids)
        if model:
            conditions.append("pr.classifier_model = ?")
            params.append(model)
        if status:
            conditions.append("COALESCE(pr_rev.status, 'pending') = ?")
            params.append(status)
        # Latest-fingerprint-per-(detection, classifier_model) filter — same
        # pattern used by /api/species/summary and get_top_prediction_for_photo.
        conditions.append(
            "pr.labels_fingerprint = ("
            "SELECT pr2.labels_fingerprint FROM predictions pr2 "
            "WHERE pr2.detection_id = pr.detection_id "
            "AND pr2.classifier_model = pr.classifier_model "
            "ORDER BY pr2.created_at DESC, pr2.id DESC LIMIT 1)"
        )
        where = "WHERE " + " AND ".join(conditions)
        return self.conn.execute(
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
        ).fetchall()

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
        """Return True if the keyword is marked as a species."""
        row = self.conn.execute(
            "SELECT is_species FROM keywords WHERE id = ?", (keyword_id,),
        ).fetchone()
        return bool(row["is_species"]) if row else False

    def accept_prediction(self, prediction_id):
        """Accept a prediction: mark as accepted and add species keyword.

        If the prediction belongs to a group, derives the consensus species
        from the individual votes and applies that to all photos.

        All database changes are performed atomically in a single transaction.
        On failure, all changes are rolled back.
        """
        ws = self._ws_id()
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
            affected = []  # list of {"photo_id": int, "prediction_id": int}

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
                    self.update_prediction_status(gp["id"], "accepted", _commit=False)
                    self.tag_photo(gp["photo_id"], kid, _commit=False)
                    self.queue_change(gp["photo_id"], "keyword_add", species, _commit=False)
                    affected.append({"photo_id": gp["photo_id"], "prediction_id": gp["id"]})
            else:
                self.update_prediction_status(prediction_id, "accepted", _commit=False)
                self.tag_photo(pred["photo_id"], kid, _commit=False)
                self.queue_change(pred["photo_id"], "keyword_add", species, _commit=False)
                affected.append({"photo_id": pred["photo_id"], "prediction_id": prediction_id})

            self.conn.commit()
            return {"species": species, "keyword_id": kid, "affected": affected}
        except Exception:
            self.conn.rollback()
            raise

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
        """Count distinct photos in `photo_ids` that have at least one
        non-full-image detection above the active workspace's
        ``detector_confidence`` threshold with a classifier_runs row
        matching the given (classifier_model, labels_fingerprint).

        Used by the streaming pipeline's classify stage to pre-flight how
        many photos will hit the cache vs. require fresh inference.

        Mirrors the runtime gate's photo selection (see pipeline_job.py,
        the ``primary_det = photo_dets[0]`` block in the classify loop):
        full-image rows are excluded and below-threshold detections are
        ignored, so prior full-image classifier_runs and low-confidence
        secondary boxes don't inflate the cached_estimate. May still
        overcount for photos with multiple above-threshold non-full-image
        detections where a non-primary one happens to carry the matching
        run key — exact mirroring would require a window function over
        every photo's detection set.
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
            rows = self.conn.execute(
                f"SELECT DISTINCT d.photo_id "
                f"FROM detections d "
                f"JOIN classifier_runs cr ON cr.detection_id = d.id "
                f"WHERE cr.classifier_model = ? "
                f"  AND cr.labels_fingerprint = ? "
                f"  AND d.detector_model != 'full-image' "
                f"  AND d.detector_confidence >= ? "
                f"  AND d.photo_id IN ({placeholders})",
                [classifier_model, labels_fingerprint, min_conf, *chunk],
            ).fetchall()
            for r in rows:
                matched.add(r["photo_id"])
        return len(matched)

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

        Args:
            photo_id: the photo
            detections: list of dicts {box: {x,y,w,h}, confidence, category}
            detector_model: required, e.g. "megadetector-v6"
        Returns:
            list of new detection IDs (empty if detections was empty).
        """
        if detector_model is None:
            raise ValueError("detector_model is required")
        self.conn.execute(
            "DELETE FROM detections WHERE photo_id = ? AND detector_model = ?",
            (photo_id, detector_model),
        )
        ids = []
        for det in detections:
            box = det["box"]
            cur = self.conn.execute(
                """INSERT INTO detections
                     (photo_id, detector_model, box_x, box_y, box_w, box_h,
                      detector_confidence, category)
                   VALUES (?, ?, ?, ?, ?, ?, ?, ?)""",
                (photo_id, detector_model, box["x"], box["y"], box["w"], box["h"],
                 det["confidence"], det.get("category", "animal")),
            )
            ids.append(cur.lastrowid)
        commit_with_retry(self.conn)
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
            self.conn.execute(
                "DELETE FROM detections WHERE photo_id = ? AND detector_model = ?",
                (photo_id, detector_model),
            )
            ids = []
            for det in detections:
                box = det["box"]
                cur = self.conn.execute(
                    """INSERT INTO detections
                         (photo_id, detector_model, box_x, box_y, box_w, box_h,
                          detector_confidence, category)
                       VALUES (?, ?, ?, ?, ?, ?, ?, ?)""",
                    (photo_id, detector_model, box["x"], box["y"], box["w"], box["h"],
                     det["confidence"], det.get("category", "animal")),
                )
                ids.append(cur.lastrowid)
            self.conn.execute(
                """INSERT INTO detector_runs (photo_id, detector_model, box_count)
                   VALUES (?, ?, ?)
                   ON CONFLICT(photo_id, detector_model)
                   DO UPDATE SET box_count = excluded.box_count,
                                 run_at = datetime('now')""",
                (photo_id, detector_model, len(detections)),
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
        q += " ORDER BY detector_confidence DESC"
        return self.conn.execute(q, params).fetchall()

    def get_detections_for_photos(self, photo_ids, min_conf=None,
                                  detector_model=None):
        """Return {photo_id: [det_dict, ...]} for a batch of photos.

        Each det_dict has keys: x, y, w, h, confidence, category. Lists are
        ordered by confidence DESC. The detections table is global — threshold
        filtering happens at read time. Photos with no detections above
        ``min_conf`` are omitted from the result.

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
        placeholders = ",".join("?" for _ in photo_ids)
        q = (
            f"SELECT photo_id, box_x, box_y, box_w, box_h, "
            f"       detector_confidence, category "
            f"FROM detections "
            f"WHERE photo_id IN ({placeholders}) "
            f"  AND detector_confidence >= ?"
        )
        params = [*photo_ids, min_conf]
        if detector_model is not None:
            q += " AND detector_model = ?"
            params.append(detector_model)
        q += " ORDER BY photo_id, detector_confidence DESC"
        rows = self.conn.execute(q, params).fetchall()
        result = {}
        for r in rows:
            result.setdefault(r["photo_id"], []).append({
                "x": r["box_x"],
                "y": r["box_y"],
                "w": r["box_w"],
                "h": r["box_h"],
                "confidence": r["detector_confidence"],
                "category": r["category"],
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

    def list_misses(self, category=None, since=None):
        """Return photos flagged as misses in the active workspace.

        category: None | "no_subject" | "clipped" | "oof"
        since: optional ISO timestamp; if set, restricts to photos whose
            miss_computed_at >= since. Used by the pipeline-review step to
            scope results to the current run.

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

        rows = self.conn.execute(
            f"SELECT p.id, p.folder_id, p.filename, p.timestamp, p.burst_id, "
            f"       p.subject_size, p.crop_complete, "
            f"       p.subject_tenengrad, p.bg_tenengrad, "
            f"       p.miss_no_subject, p.miss_clipped, p.miss_oof, "
            f"       p.miss_computed_at, p.flag "
            f"FROM photos p "
            f"JOIN workspace_folders wf ON wf.folder_id = p.folder_id "
            f"WHERE wf.workspace_id = ? "
            f"  AND ({where}) "
            f"  AND (p.flag IS NULL OR p.flag != 'rejected') "
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

    def bulk_reject_miss_category(self, category, since=None):
        """Set flag='rejected' on every photo flagged with that miss category
        in the active workspace and not already rejected.

        ``since`` mirrors the filter on ``list_misses``: when set, only
        photos whose ``miss_computed_at >= since`` are rejected. This
        keeps bulk reject scoped to the /misses view the user is looking
        at (e.g. the current pipeline run), so older misses not shown on
        screen aren't silently rejected.

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
        rows = self.conn.execute(
            f"SELECT p.id, p.flag FROM photos p "
            f"JOIN workspace_folders wf ON wf.folder_id = p.folder_id "
            f"WHERE wf.workspace_id = ? "
            f"  AND p.{col}=1 "
            f"  AND (p.flag IS NULL OR p.flag != 'rejected') "
            f"{since_clause}",
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
            elif entry['action_type'] == 'color_label':
                if old_val:
                    self.set_color_label(pid, old_val)
                else:
                    self.remove_color_label(pid)
            elif entry['action_type'] in ('keyword_add', 'prediction_accept'):
                self.untag_photo(pid, int(entry['new_value']))
                kw = self.conn.execute("SELECT name FROM keywords WHERE id = ?",
                                       (int(entry['new_value']),)).fetchone()
                if kw:
                    self.remove_pending_changes(pid, 'keyword_add', kw['name'])
                if entry['action_type'] == 'prediction_accept' and old_val:
                    pred_id = int(old_val)
                    ws = self._ws_id()
                    # Restore predictions to pre-accept state. Scope by
                    # labels_fingerprint too — without it, undoing an accept
                    # in one label set would flip statuses of predictions
                    # produced under a different fingerprint and could
                    # promote the wrong fingerprint's top-confidence row
                    # back to 'pending'.
                    pred_row = self.conn.execute(
                        """SELECT detection_id, classifier_model AS model,
                                  labels_fingerprint
                           FROM predictions WHERE id = ?""",
                        (pred_id,),
                    ).fetchone()
                    if pred_row:
                        # Identify every sibling prediction for
                        # (detection, classifier_model, labels_fingerprint).
                        siblings = self.conn.execute(
                            """SELECT id, confidence FROM predictions
                               WHERE detection_id = ?
                                 AND classifier_model = ?
                                 AND labels_fingerprint = ?
                               ORDER BY confidence DESC""",
                            (pred_row["detection_id"], pred_row["model"],
                             pred_row["labels_fingerprint"]),
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
                            (ws, pred_row["detection_id"], pred_row["model"],
                             pred_row["labels_fingerprint"]),
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
                        self.conn.commit()
            elif entry['action_type'] == 'keyword_remove':
                self.tag_photo(pid, int(entry['new_value']))
                kw = self.conn.execute("SELECT name FROM keywords WHERE id = ?",
                                       (int(entry['new_value']),)).fetchone()
                if kw:
                    self.remove_pending_changes(pid, 'keyword_remove', kw['name'])
            elif entry['action_type'] == 'species_replace':
                # Atomic swap: the edit replaced old_value's species with
                # new_value's. Undo untags the new species and retags the
                # old one, symmetrically reversing the pending-change queue.
                new_kid = int(item['new_value']) if item['new_value'] else None
                old_kid = int(old_val) if old_val else None
                if new_kid:
                    self.untag_photo(pid, new_kid)
                    new_kw = self.conn.execute(
                        "SELECT name FROM keywords WHERE id = ?", (new_kid,)
                    ).fetchone()
                    if new_kw:
                        cancelled = self.remove_pending_changes(
                            pid, 'keyword_add', new_kw['name']
                        )
                        if cancelled == 0:
                            self.queue_change(pid, 'keyword_remove', new_kw['name'])
                if old_kid:
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
                self.update_photo_flag(pid, entry['new_value'], verify_workspace=False)
            elif entry['action_type'] == 'color_label':
                if new_val:
                    self.set_color_label(pid, new_val)
                else:
                    self.remove_color_label(pid)
            elif entry['action_type'] in ('keyword_add', 'prediction_accept'):
                self.tag_photo(pid, int(entry['new_value']))
                kw = self.conn.execute("SELECT name FROM keywords WHERE id = ?",
                                       (int(entry['new_value']),)).fetchone()
                if kw:
                    self.queue_change(pid, 'keyword_add', kw['name'])
                if entry['action_type'] == 'prediction_accept' and item['old_value']:
                    pred_id = int(item['old_value'])
                    ws = self._ws_id()
                    self.update_prediction_status(pred_id, 'accepted')
                    # Re-reject siblings, scoped to the same labels_fingerprint
                    # so the redo matches the original accept's scope and
                    # doesn't touch predictions from other label sets.
                    pred_row = self.conn.execute(
                        """SELECT detection_id, classifier_model AS model,
                                  labels_fingerprint
                           FROM predictions WHERE id = ?""",
                        (pred_id,),
                    ).fetchone()
                    if pred_row:
                        sibs = self.conn.execute(
                            """SELECT pr.id FROM predictions pr
                               LEFT JOIN prediction_review pr_rev
                                 ON pr_rev.prediction_id = pr.id
                                AND pr_rev.workspace_id = ?
                               WHERE pr.detection_id = ?
                                 AND pr.classifier_model = ?
                                 AND pr.labels_fingerprint = ?
                                 AND pr.id != ?
                                 AND COALESCE(pr_rev.status, 'pending')
                                     IN ('pending', 'alternative')""",
                            (ws, pred_row["detection_id"], pred_row["model"],
                             pred_row["labels_fingerprint"], pred_id),
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
                        self.conn.commit()
            elif entry['action_type'] == 'keyword_remove':
                self.untag_photo(pid, int(entry['new_value']))
                kw = self.conn.execute("SELECT name FROM keywords WHERE id = ?",
                                       (int(entry['new_value']),)).fetchone()
                if kw:
                    self.queue_change(pid, 'keyword_remove', kw['name'])
            elif entry['action_type'] == 'species_replace':
                # Re-apply the swap: untag old, retag new, mirror pending queue.
                new_kid = int(new_val) if new_val else None
                old_kid = int(item['old_value']) if item['old_value'] else None
                if old_kid:
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
                        cancelled = self.remove_pending_changes(
                            pid, 'keyword_remove', new_kw['name']
                        )
                        if cancelled == 0:
                            self.queue_change(pid, 'keyword_add', new_kw['name'])

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
        cur = self.conn.execute(
            "INSERT INTO new_image_snapshots (workspace_id, created_at, file_count) "
            "VALUES (?, datetime('now'), ?)",
            (ws_id, len(file_paths)),
        )
        snap_id = cur.lastrowid
        if file_paths:
            # De-duplicate in case the caller passed repeats; PK would reject them
            # but sending a clean set keeps executemany cheap.
            unique_paths = sorted(set(file_paths))
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

    def _build_collection_query(self, collection_id):
        """Build SQL clauses from collection rules.

        Returns (folder_join, join_clause, where, params) or None if collection not found.
        """
        row = self.conn.execute(
            "SELECT rules FROM collections WHERE id = ? AND workspace_id = ?",
            (collection_id, self._ws_id()),
        ).fetchone()
        if not row:
            return None

        rules = json.loads(row["rules"])
        conditions = []
        params = []
        need_keyword_join = False
        need_prediction_join = False

        for rule in rules:
            field = rule["field"]
            op = rule.get("op", "")
            value = rule.get("value")

            if field == "all":
                # Sentinel for defaults like "All Photos" — adds no condition,
                # so the workspace-folder join alone determines matches.
                continue
            elif field == "photo_ids":
                # Static collection — explicit list of photo IDs
                ids = value if isinstance(value, list) else []
                if ids:
                    placeholders = ",".join("?" for _ in ids)
                    conditions.append(f"p.id IN ({placeholders})")
                    params.extend(ids)
                else:
                    conditions.append("0")  # empty collection
            elif field == "rating":
                if op == ">=":
                    conditions.append("p.rating >= ?")
                    params.append(value)
                elif op == "<=":
                    conditions.append("p.rating <= ?")
                    params.append(value)
                elif op in ("equals", "is"):
                    conditions.append("p.rating = ?")
                    params.append(value)
                elif op == "is not":
                    conditions.append("p.rating != ?")
                    params.append(value)
            elif field == "keyword":
                if op == "contains":
                    need_keyword_join = True
                    conditions.append("k.name LIKE ?")
                    params.append(f"%{value}%")
                elif op == "not_contains":
                    conditions.append(
                        """NOT EXISTS (
                        SELECT 1 FROM photo_keywords pk4
                        JOIN keywords k4 ON k4.id = pk4.keyword_id
                        WHERE pk4.photo_id = p.id AND k4.name LIKE ?)"""
                    )
                    params.append(f"%{value}%")
                elif op in ("equals", "is"):
                    need_keyword_join = True
                    conditions.append("k.name = ?")
                    params.append(value)
                elif op == "is not":
                    conditions.append(
                        """NOT EXISTS (
                        SELECT 1 FROM photo_keywords pk4
                        JOIN keywords k4 ON k4.id = pk4.keyword_id
                        WHERE pk4.photo_id = p.id AND k4.name = ?)"""
                    )
                    params.append(value)
            elif field == "folder":
                if op == "under":
                    conditions.append("f.path LIKE ?")
                    params.append(f"{value}%")
                elif op == "not_under":
                    conditions.append("(f.path IS NULL OR f.path NOT LIKE ?)")
                    params.append(f"{value}%")
            elif field == "flag":
                if op in ("equals", "is"):
                    conditions.append("p.flag = ?")
                    params.append(value)
                elif op == "is not":
                    conditions.append("p.flag != ?")
                    params.append(value)
            elif field == "color_label":
                if op in ("equals", "is"):
                    conditions.append(
                        """EXISTS (
                        SELECT 1 FROM photo_color_labels pcl
                        WHERE pcl.photo_id = p.id AND pcl.workspace_id = ? AND pcl.color = ?)"""
                    )
                    params.append(self._ws_id())
                    params.append(value)
                elif op == "is not":
                    conditions.append(
                        """NOT EXISTS (
                        SELECT 1 FROM photo_color_labels pcl
                        WHERE pcl.photo_id = p.id AND pcl.workspace_id = ? AND pcl.color = ?)"""
                    )
                    params.append(self._ws_id())
                    params.append(value)
            elif field == "has_species":
                if op == "equals" and value is False or value == 0:
                    conditions.append(
                        """NOT EXISTS (
                        SELECT 1 FROM photo_keywords pk3
                        JOIN keywords k3 ON k3.id = pk3.keyword_id
                        WHERE pk3.photo_id = p.id AND k3.is_species = 1)"""
                    )
                elif op == "equals" and value is True or value == 1:
                    conditions.append(
                        """EXISTS (
                        SELECT 1 FROM photo_keywords pk3
                        JOIN keywords k3 ON k3.id = pk3.keyword_id
                        WHERE pk3.photo_id = p.id AND k3.is_species = 1)"""
                    )
            elif field == "has_subject":
                # Match the has_species pattern, but resolve "subject" via
                # the workspace's configured subject_types (a set of keyword
                # types). Sorted for deterministic SQL parameter order.
                #
                # When 'taxonomy' is among the subject types, also count
                # legacy species rows (is_species=1 with a non-taxonomy
                # type). Upgraded databases retain those rows until the
                # background mark_species_keywords pass retypes them; the
                # type-only predicate would otherwise place already-
                # identified photos into 'Needs Identification'.
                subject_types = sorted(self.get_subject_types())
                if not subject_types:
                    if op == "equals" and (value is True or value == 1):
                        conditions.append("0")  # nothing matches
                    # value==0 with empty subject_types is a no-op (every
                    # photo is "not identified" by the empty set)
                    continue
                type_placeholders = ",".join("?" * len(subject_types))
                type_clause = f"k5.type IN ({type_placeholders})"
                if "taxonomy" in subject_types:
                    type_clause = f"({type_clause} OR k5.is_species = 1)"
                if op == "equals" and (value is False or value == 0):
                    conditions.append(
                        f"""NOT EXISTS (
                        SELECT 1 FROM photo_keywords pk5
                        JOIN keywords k5 ON k5.id = pk5.keyword_id
                        WHERE pk5.photo_id = p.id AND {type_clause})"""
                    )
                    params.extend(subject_types)
                elif op == "equals" and (value is True or value == 1):
                    conditions.append(
                        f"""EXISTS (
                        SELECT 1 FROM photo_keywords pk5
                        JOIN keywords k5 ON k5.id = pk5.keyword_id
                        WHERE pk5.photo_id = p.id AND {type_clause})"""
                    )
                    params.extend(subject_types)
            elif field == "keyword_count":
                if op == "equals":
                    conditions.append(
                        """(SELECT COUNT(*) FROM photo_keywords pk2
                                         WHERE pk2.photo_id = p.id) = ?"""
                    )
                    params.append(value)
                elif op == ">=":
                    conditions.append(
                        """(SELECT COUNT(*) FROM photo_keywords pk2
                                         WHERE pk2.photo_id = p.id) >= ?"""
                    )
                    params.append(value)
            elif field == "timestamp":
                if op == "between" and isinstance(value, list) and len(value) == 2:
                    conditions.append("p.timestamp >= ? AND p.timestamp <= ?")
                    params.append(value[0])
                    params.append(_inclusive_date_to(value[1]))
                elif op == "recent_days":
                    conditions.append("p.timestamp >= datetime('now', ?)")
                    params.append(f"-{value} days")
            elif field == "extension":
                if op in ("equals", "is"):
                    conditions.append("p.extension = ?")
                    params.append(value)
                elif op == "is not":
                    conditions.append("p.extension != ?")
                    params.append(value)
            elif field in (
                "taxonomy_kingdom",
                "taxonomy_phylum",
                "taxonomy_class",
                "taxonomy_order",
                "taxonomy_family",
                "taxonomy_genus",
            ):
                need_prediction_join = True
                col = f"pred.{field}"
                if op in ("equals", "is"):
                    conditions.append(f"{col} = ?")
                    params.append(value)
                elif op == "is not":
                    conditions.append(f"({col} IS NULL OR {col} != ?)")
                    params.append(value)
                elif op == "contains":
                    conditions.append(f"{col} LIKE ?")
                    params.append(f"%{value}%")

        join_clause = ""
        if need_keyword_join:
            join_clause += " JOIN photo_keywords pk ON pk.photo_id = p.id"
            join_clause += " JOIN keywords k ON k.id = pk.keyword_id"
        if need_prediction_join:
            # Detections are global (no workspace_id); workspace scoping is
            # enforced by the folder_join on workspace_folders below.
            join_clause += (
                " JOIN detections det ON det.photo_id = p.id"
                " JOIN predictions pred ON pred.detection_id = det.id"
            )

        # Always join folders for folder-under rules, scoped to workspace
        folder_join = " JOIN folders f ON f.id = p.folder_id AND f.status IN ('ok', 'partial')"
        folder_join += " JOIN workspace_folders wf ON wf.folder_id = f.id AND wf.workspace_id = ?"

        # folder_join comes before join_clause in the query, so its param goes first
        params.insert(0, self._ws_id())

        where = ""
        if conditions:
            where = "WHERE " + " AND ".join(conditions)

        return folder_join, join_clause, where, params

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
            ORDER BY p.timestamp ASC, p.filename ASC, p.id ASC
            LIMIT ? OFFSET ?
        """
        return self.conn.execute(query, params).fetchall()

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

        Retypes any keyword whose name matches a taxon lookup: sets
        is_species=1, type='taxonomy', and (if the local taxa table is
        populated) links taxon_id to the matching taxa row by inat_id.

        Uses the local taxonomy only (no network requests).

        Args:
            taxonomy: a Taxonomy instance with a lookup() method
        """
        # Also include already-typed taxonomy keywords whose taxon_id is
        # still NULL — those were created before the local taxa table was
        # populated (e.g. via add_keyword(..., is_species=True) from the
        # classifier), and still need their hierarchy link filled in.
        keywords = self.conn.execute(
            "SELECT id, name, type, taxon_id, is_species FROM keywords "
            "WHERE is_species = 0 OR type IS NULL OR type != 'taxonomy' "
            "   OR taxon_id IS NULL"
        ).fetchall()
        updated = 0
        for kw in keywords:
            taxon = taxonomy.lookup(kw["name"])
            if not taxon:
                continue
            local_taxon_id = kw["taxon_id"]
            if local_taxon_id is None:
                inat_id = taxon.get("taxon_id")
                if inat_id is not None:
                    row = self.conn.execute(
                        "SELECT id FROM taxa WHERE inat_id = ?", (inat_id,)
                    ).fetchone()
                    if row:
                        local_taxon_id = row["id"]
            # Skip no-op updates so the "updated" count reflects real
            # changes. A matched row is fully consistent when type is
            # 'taxonomy', is_species is 1, and (taxon_id is already set
            # OR we have no local id to link it to).
            is_type_change = kw["type"] != "taxonomy"
            is_species_fix = kw["is_species"] != 1
            is_taxon_link = kw["taxon_id"] is None and local_taxon_id is not None
            if not (is_type_change or is_species_fix or is_taxon_link):
                continue
            self.conn.execute(
                "UPDATE keywords SET is_species = 1, type = 'taxonomy', "
                "taxon_id = COALESCE(taxon_id, ?) WHERE id = ?",
                (local_taxon_id, kw["id"]),
            )
            updated += 1
        if updated:
            self.conn.commit()
        return updated

    def create_default_collections(self):
        """Create default smart collections, skipping any that already exist by name."""
        existing_names = {c["name"] for c in self.get_collections()}

        defaults = [
            ("All Photos", [{"field": "all"}]),
            (
                "Needs Identification",
                [{"field": "has_subject", "op": "equals", "value": 0}],
            ),
            ("Untagged", [{"field": "keyword_count", "op": "equals", "value": 0}]),
            ("Flagged", [{"field": "flag", "op": "equals", "value": "flagged"}]),
            (
                "Recent Import",
                [{"field": "timestamp", "op": "recent_days", "value": 30}],
            ),
        ]
        for name, rules in defaults:
            if name not in existing_names:
                self.add_collection(name, json.dumps(rules))

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
                    json.dumps(
                        [{"field": "has_subject", "op": "equals", "value": 0}]
                    ),
                    row["id"],
                ),
            )
        self.conn.commit()

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
        placeholders = ",".join("?" * len(photo_ids))
        rows = self.conn.execute(
            f"SELECT photo_id, observation_id, observation_url, submitted_at FROM inat_submissions WHERE photo_id IN ({placeholders}) ORDER BY submitted_at DESC",
            photo_ids,
        ).fetchall()
        return {r["photo_id"]: dict(r) for r in rows}
