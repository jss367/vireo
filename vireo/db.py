"""SQLite database for Vireo photo browser metadata cache."""

import json
import logging
import os
import sqlite3
import uuid

from new_images import get_shared_cache

log = logging.getLogger(__name__)

_UNSET = object()  # sentinel for "not provided" vs explicit None


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
        self.conn = sqlite3.connect(db_path, check_same_thread=False)
        self.conn.row_factory = sqlite3.Row
        self.conn.execute("PRAGMA journal_mode=WAL")
        self.conn.execute("PRAGMA foreign_keys=ON")
        self.conn.execute("PRAGMA synchronous=NORMAL")
        self.conn.execute("PRAGMA cache_size=-10000")  # 10 MB
        self.conn.execute("PRAGMA temp_store=MEMORY")
        self.conn.execute("PRAGMA mmap_size=30000000")  # 30 MB
        self._active_workspace_id = None
        self._new_images_cache = get_shared_cache()
        self._create_tables()
        self.ensure_default_workspace()
        # Restore last-used workspace, or fall back to Default
        last = self.conn.execute(
            "SELECT id FROM workspaces ORDER BY CASE WHEN last_opened_at IS NULL THEN 0 ELSE 1 END DESC, last_opened_at DESC, id ASC LIMIT 1"
        ).fetchone()
        self.set_active_workspace(last[0])

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
                id          INTEGER PRIMARY KEY,
                folder_id   INTEGER REFERENCES folders(id),
                filename    TEXT,
                extension   TEXT,
                file_size   INTEGER,
                file_mtime  REAL,
                xmp_mtime   REAL,
                timestamp   TEXT,
                width       INTEGER,
                height      INTEGER,
                rating      INTEGER DEFAULT 0,
                flag        TEXT DEFAULT 'none',
                thumb_path  TEXT,
                sharpness   REAL,
                detection_box TEXT,
                detection_conf REAL,
                subject_sharpness REAL,
                subject_size REAL,
                quality_score REAL,
                embedding BLOB,
                embedding_model TEXT,
                latitude REAL,
                longitude REAL,
                phash TEXT,
                UNIQUE(folder_id, filename)
            );

            CREATE TABLE IF NOT EXISTS keywords (
                id          INTEGER PRIMARY KEY,
                name        TEXT,
                parent_id   INTEGER REFERENCES keywords(id),
                is_species  INTEGER DEFAULT 0,
                UNIQUE(name, parent_id)
            );

            CREATE TABLE IF NOT EXISTS photo_keywords (
                photo_id    INTEGER REFERENCES photos(id),
                keyword_id  INTEGER REFERENCES keywords(id),
                PRIMARY KEY (photo_id, keyword_id)
            );

            CREATE TABLE IF NOT EXISTS workspaces (
                id              INTEGER PRIMARY KEY,
                name            TEXT NOT NULL UNIQUE,
                config_overrides TEXT,
                ui_state        TEXT,
                created_at      TEXT DEFAULT (datetime('now')),
                last_opened_at  TEXT
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
                id                INTEGER PRIMARY KEY,
                photo_id          INTEGER REFERENCES photos(id) ON DELETE CASCADE,
                workspace_id      INTEGER REFERENCES workspaces(id) ON DELETE CASCADE,
                box_x             REAL,
                box_y             REAL,
                box_w             REAL,
                box_h             REAL,
                detector_confidence REAL,
                category          TEXT DEFAULT 'animal',
                detector_model    TEXT,
                created_at        TEXT DEFAULT (datetime('now'))
            );

            CREATE TABLE IF NOT EXISTS predictions (
                id              INTEGER PRIMARY KEY,
                detection_id    INTEGER REFERENCES detections(id) ON DELETE CASCADE,
                species         TEXT,
                confidence      REAL,
                model           TEXT,
                category        TEXT,
                status          TEXT DEFAULT 'pending',
                group_id        TEXT,
                vote_count      INTEGER,
                total_votes     INTEGER,
                individual      TEXT,
                taxonomy_kingdom TEXT,
                taxonomy_phylum TEXT,
                taxonomy_class  TEXT,
                taxonomy_order  TEXT,
                taxonomy_family TEXT,
                taxonomy_genus  TEXT,
                scientific_name TEXT,
                created_at      TEXT DEFAULT (datetime('now')),
                reviewed_at     TEXT,
                UNIQUE(detection_id, model, species)
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

            CREATE TABLE IF NOT EXISTS taxa (
                id          INTEGER PRIMARY KEY,
                inat_id     INTEGER UNIQUE,
                name        TEXT NOT NULL,
                common_name TEXT,
                rank        TEXT NOT NULL,
                parent_id   INTEGER REFERENCES taxa(id),
                kingdom     TEXT
            );

            CREATE INDEX IF NOT EXISTS idx_taxa_parent ON taxa(parent_id);
            CREATE INDEX IF NOT EXISTS idx_taxa_rank ON taxa(rank);
            CREATE INDEX IF NOT EXISTS idx_taxa_name ON taxa(name);
            CREATE INDEX IF NOT EXISTS idx_taxa_common ON taxa(common_name);

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

            CREATE INDEX IF NOT EXISTS idx_photos_timestamp ON photos(timestamp);
            CREATE INDEX IF NOT EXISTS idx_photos_folder ON photos(folder_id);
            CREATE INDEX IF NOT EXISTS idx_photos_rating ON photos(rating);
            CREATE INDEX IF NOT EXISTS idx_keywords_name ON keywords(name);
            CREATE INDEX IF NOT EXISTS idx_photo_keywords_photo ON photo_keywords(photo_id);
            CREATE INDEX IF NOT EXISTS idx_photo_keywords_keyword ON photo_keywords(keyword_id);

            CREATE INDEX IF NOT EXISTS idx_photo_color_labels_ws
            ON photo_color_labels(workspace_id);

            CREATE INDEX IF NOT EXISTS preview_cache_last_access
            ON preview_cache(last_access_at);

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

            CREATE INDEX IF NOT EXISTS idx_new_image_snapshots_ws
              ON new_image_snapshots(workspace_id);
        """
        )
        # Migrations for existing databases
        try:
            self.conn.execute("SELECT is_species FROM keywords LIMIT 0")
        except sqlite3.OperationalError:
            self.conn.execute(
                "ALTER TABLE keywords ADD COLUMN is_species INTEGER DEFAULT 0"
            )
        try:
            self.conn.execute("SELECT group_id FROM predictions LIMIT 0")
        except sqlite3.OperationalError:
            self.conn.execute("ALTER TABLE predictions ADD COLUMN group_id TEXT")
            self.conn.execute("ALTER TABLE predictions ADD COLUMN vote_count INTEGER")
            self.conn.execute("ALTER TABLE predictions ADD COLUMN total_votes INTEGER")
            self.conn.execute("ALTER TABLE predictions ADD COLUMN individual TEXT")
        try:
            self.conn.execute("SELECT sharpness FROM photos LIMIT 0")
        except sqlite3.OperationalError:
            self.conn.execute("ALTER TABLE photos ADD COLUMN sharpness REAL")
        try:
            self.conn.execute("SELECT quality_score FROM photos LIMIT 0")
        except sqlite3.OperationalError:
            self.conn.execute("ALTER TABLE photos ADD COLUMN detection_box TEXT")
            self.conn.execute("ALTER TABLE photos ADD COLUMN detection_conf REAL")
            self.conn.execute("ALTER TABLE photos ADD COLUMN subject_sharpness REAL")
            self.conn.execute("ALTER TABLE photos ADD COLUMN subject_size REAL")
            self.conn.execute("ALTER TABLE photos ADD COLUMN quality_score REAL")
        try:
            self.conn.execute("SELECT embedding FROM photos LIMIT 0")
        except sqlite3.OperationalError:
            self.conn.execute("ALTER TABLE photos ADD COLUMN embedding BLOB")
        try:
            self.conn.execute("SELECT embedding_model FROM photos LIMIT 0")
        except sqlite3.OperationalError:
            self.conn.execute("ALTER TABLE photos ADD COLUMN embedding_model TEXT")
        try:
            self.conn.execute("SELECT latitude FROM photos LIMIT 0")
        except sqlite3.OperationalError:
            self.conn.execute("ALTER TABLE photos ADD COLUMN latitude REAL")
            self.conn.execute("ALTER TABLE photos ADD COLUMN longitude REAL")
        try:
            self.conn.execute("SELECT phash FROM photos LIMIT 0")
        except sqlite3.OperationalError:
            self.conn.execute("ALTER TABLE photos ADD COLUMN phash TEXT")
        try:
            self.conn.execute("SELECT taxonomy_kingdom FROM predictions LIMIT 0")
        except sqlite3.OperationalError:
            self.conn.execute(
                "ALTER TABLE predictions ADD COLUMN taxonomy_kingdom TEXT"
            )
            self.conn.execute("ALTER TABLE predictions ADD COLUMN taxonomy_phylum TEXT")
            self.conn.execute("ALTER TABLE predictions ADD COLUMN taxonomy_class TEXT")
            self.conn.execute("ALTER TABLE predictions ADD COLUMN taxonomy_order TEXT")
            self.conn.execute("ALTER TABLE predictions ADD COLUMN taxonomy_family TEXT")
            self.conn.execute("ALTER TABLE predictions ADD COLUMN taxonomy_genus TEXT")
            self.conn.execute("ALTER TABLE predictions ADD COLUMN scientific_name TEXT")
        # Pipeline feature columns (SAM2 masking + quality features)
        try:
            self.conn.execute("SELECT mask_path FROM photos LIMIT 0")
        except sqlite3.OperationalError:
            self.conn.execute("ALTER TABLE photos ADD COLUMN mask_path TEXT")
            self.conn.execute(
                "ALTER TABLE photos ADD COLUMN dino_subject_embedding BLOB"
            )
            self.conn.execute(
                "ALTER TABLE photos ADD COLUMN dino_global_embedding BLOB"
            )
            self.conn.execute("ALTER TABLE photos ADD COLUMN subject_tenengrad REAL")
            self.conn.execute("ALTER TABLE photos ADD COLUMN bg_tenengrad REAL")
            self.conn.execute("ALTER TABLE photos ADD COLUMN crop_complete REAL")
            self.conn.execute("ALTER TABLE photos ADD COLUMN bg_separation REAL")
            self.conn.execute("ALTER TABLE photos ADD COLUMN subject_clip_high REAL")
            self.conn.execute("ALTER TABLE photos ADD COLUMN subject_clip_low REAL")
            self.conn.execute("ALTER TABLE photos ADD COLUMN subject_y_median REAL")
            self.conn.execute("ALTER TABLE photos ADD COLUMN phash_crop TEXT")
        # Noise estimate column
        try:
            self.conn.execute("SELECT noise_estimate FROM photos LIMIT 0")
        except sqlite3.OperationalError:
            self.conn.execute("ALTER TABLE photos ADD COLUMN noise_estimate REAL")
        # DINOv2 variant that produced the embeddings. NULL for legacy rows
        # written before this column existed; pipeline treats those as "unknown"
        # and drops them when the dim doesn't match the configured variant.
        try:
            self.conn.execute("SELECT dino_embedding_variant FROM photos LIMIT 0")
        except sqlite3.OperationalError:
            self.conn.execute(
                "ALTER TABLE photos ADD COLUMN dino_embedding_variant TEXT"
            )
        # Enhanced EXIF metadata columns
        try:
            self.conn.execute("SELECT focal_length FROM photos LIMIT 0")
        except sqlite3.OperationalError:
            self.conn.execute("ALTER TABLE photos ADD COLUMN focal_length REAL")
            self.conn.execute("ALTER TABLE photos ADD COLUMN burst_id TEXT")
        # Ingest: file hash for duplicate detection + companion for raw/JPEG pairing
        try:
            self.conn.execute("SELECT file_hash FROM photos LIMIT 0")
        except sqlite3.OperationalError:
            self.conn.execute("ALTER TABLE photos ADD COLUMN file_hash TEXT")
            self.conn.execute("ALTER TABLE photos ADD COLUMN companion_path TEXT")
        self.conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_photos_file_hash ON photos(file_hash)"
        )

        # Full EXIF metadata JSON blob
        try:
            self.conn.execute("SELECT exif_data FROM photos LIMIT 0")
        except sqlite3.OperationalError:
            self.conn.execute("ALTER TABLE photos ADD COLUMN exif_data TEXT")

        # Working copy path for JPG-centric pipeline
        try:
            self.conn.execute("SELECT working_copy_path FROM photos LIMIT 0")
        except Exception:
            self.conn.execute("ALTER TABLE photos ADD COLUMN working_copy_path TEXT")

        # Eye-focus detection columns (keypoint + windowed tenengrad)
        try:
            self.conn.execute("SELECT eye_x FROM photos LIMIT 0")
        except sqlite3.OperationalError:
            self.conn.execute("ALTER TABLE photos ADD COLUMN eye_x REAL")
            self.conn.execute("ALTER TABLE photos ADD COLUMN eye_y REAL")
            self.conn.execute("ALTER TABLE photos ADD COLUMN eye_conf REAL")
            self.conn.execute("ALTER TABLE photos ADD COLUMN eye_tenengrad REAL")

        # Edit history tables migration
        try:
            self.conn.execute("SELECT id FROM edit_history LIMIT 0")
        except sqlite3.OperationalError:
            self.conn.executescript("""
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
            """)

        # Add undone column to edit_history if missing (migration for existing databases)
        try:
            self.conn.execute("SELECT undone FROM edit_history LIMIT 0")
        except sqlite3.OperationalError:
            self.conn.execute("ALTER TABLE edit_history ADD COLUMN undone INTEGER DEFAULT 0")

        # Workspace migration for existing databases
        # Only triggers for legacy DBs that have predictions with photo_id
        # but no workspace_id. New schema uses detection_id instead of photo_id,
        # so this migration is skipped for fresh databases.
        needs_workspace_migration = False
        pred_schema = self.conn.execute(
            "SELECT sql FROM sqlite_master WHERE type='table' AND name='predictions'"
        ).fetchone()
        if pred_schema and "photo_id" in pred_schema[0].lower() and "workspace_id" not in pred_schema[0].lower():
            needs_workspace_migration = True

        if needs_workspace_migration:
            # Create default workspace
            self.conn.execute(
                "INSERT OR IGNORE INTO workspaces (name) VALUES (?)", ("Default",)
            )
            default_id = self.conn.execute(
                "SELECT id FROM workspaces WHERE name = 'Default'"
            ).fetchone()[0]

            # Link all existing folders to default workspace
            self.conn.execute(
                "INSERT OR IGNORE INTO workspace_folders (workspace_id, folder_id) "
                "SELECT ?, id FROM folders", (default_id,)
            )

            # Recreate predictions table to change UNIQUE(photo_id, model)
            # to UNIQUE(photo_id, model, workspace_id)
            try:
                self.conn.execute("SELECT workspace_id FROM predictions LIMIT 0")
            except sqlite3.OperationalError:
                self.conn.execute(
                    """CREATE TABLE predictions_new (
                        id          INTEGER PRIMARY KEY,
                        photo_id    INTEGER REFERENCES photos(id),
                        species     TEXT,
                        confidence  REAL,
                        model       TEXT,
                        category    TEXT,
                        status      TEXT DEFAULT 'pending',
                        group_id    TEXT,
                        vote_count  INTEGER,
                        total_votes INTEGER,
                        individual  TEXT,
                        taxonomy_kingdom TEXT,
                        taxonomy_phylum TEXT,
                        taxonomy_class TEXT,
                        taxonomy_order TEXT,
                        taxonomy_family TEXT,
                        taxonomy_genus TEXT,
                        scientific_name TEXT,
                        created_at  TEXT DEFAULT (datetime('now')),
                        workspace_id INTEGER REFERENCES workspaces(id) ON DELETE CASCADE,
                        UNIQUE(photo_id, model, workspace_id)
                    )"""
                )
                self.conn.execute(
                    """INSERT INTO predictions_new
                       (id, photo_id, species, confidence, model, category, status,
                        group_id, vote_count, total_votes, individual,
                        taxonomy_kingdom, taxonomy_phylum, taxonomy_class,
                        taxonomy_order, taxonomy_family, taxonomy_genus,
                        scientific_name, created_at, workspace_id)
                       SELECT id, photo_id, species, confidence, model, category, status,
                              group_id, vote_count, total_votes, individual,
                              taxonomy_kingdom, taxonomy_phylum, taxonomy_class,
                              taxonomy_order, taxonomy_family, taxonomy_genus,
                              scientific_name, created_at, ?
                       FROM predictions""",
                    (default_id,)
                )
                self.conn.execute("DROP TABLE predictions")
                self.conn.execute("ALTER TABLE predictions_new RENAME TO predictions")

            # Add workspace_id to collections and pending_changes via ALTER TABLE
            for table in ("collections", "pending_changes"):
                try:
                    self.conn.execute(f"SELECT workspace_id FROM {table} LIMIT 0")
                except sqlite3.OperationalError:
                    self.conn.execute(
                        f"ALTER TABLE {table} ADD COLUMN workspace_id INTEGER "
                        f"REFERENCES workspaces(id) ON DELETE CASCADE"
                    )
                    self.conn.execute(
                        f"UPDATE {table} SET workspace_id = ?", (default_id,)
                    )

            self.conn.commit()

        # Ensure change_token column exists (added after workspace migration)
        try:
            self.conn.execute("SELECT change_token FROM pending_changes LIMIT 0")
        except sqlite3.OperationalError:
            self.conn.execute("ALTER TABLE pending_changes ADD COLUMN change_token TEXT")
            self.conn.commit()

        # Keyword type/location/taxon columns (taxonomy support)
        try:
            self.conn.execute("SELECT type FROM keywords LIMIT 0")
        except sqlite3.OperationalError:
            self.conn.execute(
                "ALTER TABLE keywords ADD COLUMN type TEXT NOT NULL DEFAULT 'general'"
            )
            # Migrate existing is_species=1 keywords to type='taxonomy'
            self.conn.execute(
                "UPDATE keywords SET type = 'taxonomy' WHERE is_species = 1"
            )
        try:
            self.conn.execute("SELECT latitude FROM keywords LIMIT 0")
        except sqlite3.OperationalError:
            self.conn.execute("ALTER TABLE keywords ADD COLUMN latitude REAL")
            self.conn.execute("ALTER TABLE keywords ADD COLUMN longitude REAL")
        try:
            self.conn.execute("SELECT taxon_id FROM keywords LIMIT 0")
        except sqlite3.OperationalError:
            self.conn.execute(
                "ALTER TABLE keywords ADD COLUMN taxon_id INTEGER REFERENCES taxa(id)"
            )

        # Multi-animal migration: restructure predictions to use detection_id.
        # Check the predictions schema directly — the detections table is always
        # created by the executescript above, so checking for its existence
        # doesn't tell us whether predictions needs migration.
        pred_schema_row = self.conn.execute(
            "SELECT sql FROM sqlite_master WHERE type='table' AND name='predictions'"
        ).fetchone()
        needs_pred_migration = (
            pred_schema_row
            and "photo_id" in pred_schema_row[0].lower()
            and "detection_id" not in pred_schema_row[0].lower()
        )

        if needs_pred_migration:
            # Drop old predictions — accepted keywords are in photo_keywords
            # and survive. Pending/rejected predictions are lost.
            self.conn.execute("DROP TABLE IF EXISTS predictions")
            self.conn.execute("""
                CREATE TABLE predictions (
                    id              INTEGER PRIMARY KEY,
                    detection_id    INTEGER REFERENCES detections(id) ON DELETE CASCADE,
                    species         TEXT,
                    confidence      REAL,
                    model           TEXT,
                    category        TEXT,
                    status          TEXT DEFAULT 'pending',
                    group_id        TEXT,
                    vote_count      INTEGER,
                    total_votes     INTEGER,
                    individual      TEXT,
                    taxonomy_kingdom TEXT,
                    taxonomy_phylum TEXT,
                    taxonomy_class  TEXT,
                    taxonomy_order  TEXT,
                    taxonomy_family TEXT,
                    taxonomy_genus  TEXT,
                    scientific_name TEXT,
                    created_at      TEXT DEFAULT (datetime('now')),
                    reviewed_at     TEXT,
                    UNIQUE(detection_id, model, species)
                )
            """)
            self.conn.commit()

        # Migrate: UNIQUE(detection_id, model) → UNIQUE(detection_id, model, species)
        # Check if the unique index on predictions includes 'species'.
        needs_topn_migration = False
        for idx in self.conn.execute("PRAGMA index_list(predictions)").fetchall():
            if idx["unique"]:
                cols = [
                    r["name"]
                    for r in self.conn.execute(
                        f"PRAGMA index_info({idx['name']})"
                    ).fetchall()
                ]
                if "detection_id" in cols and "model" in cols and "species" not in cols:
                    needs_topn_migration = True
                    break

        if needs_topn_migration:
            log.info("Migrating predictions table: UNIQUE(detection_id, model) -> UNIQUE(detection_id, model, species)")
            self.conn.executescript("""
                CREATE TABLE predictions_topn (
                    id              INTEGER PRIMARY KEY,
                    detection_id    INTEGER REFERENCES detections(id) ON DELETE CASCADE,
                    species         TEXT,
                    confidence      REAL,
                    model           TEXT,
                    category        TEXT,
                    status          TEXT DEFAULT 'pending',
                    group_id        TEXT,
                    vote_count      INTEGER,
                    total_votes     INTEGER,
                    individual      TEXT,
                    taxonomy_kingdom TEXT,
                    taxonomy_phylum TEXT,
                    taxonomy_class  TEXT,
                    taxonomy_order  TEXT,
                    taxonomy_family TEXT,
                    taxonomy_genus  TEXT,
                    scientific_name TEXT,
                    created_at      TEXT DEFAULT (datetime('now')),
                    reviewed_at     TEXT,
                    UNIQUE(detection_id, model, species)
                );
                INSERT INTO predictions_topn SELECT * FROM predictions;
                DROP TABLE predictions;
                ALTER TABLE predictions_topn RENAME TO predictions;
            """)
            self.conn.commit()

        # Purge orphaned predictions with NULL detection_id.  These are
        # invisible to every workspace-scoped query (which JOINs through
        # detections) and accumulated on some installs before the
        # add_prediction NOT-NULL guard existed.  Regenerable via reclassify.
        orphan_cnt = self.conn.execute(
            "SELECT COUNT(*) FROM predictions WHERE detection_id IS NULL"
        ).fetchone()[0]
        if orphan_cnt:
            log.info("Purging %d orphaned predictions (NULL detection_id)", orphan_cnt)
            self.conn.execute("DELETE FROM predictions WHERE detection_id IS NULL")
            self.conn.commit()

        # Folder health status
        try:
            self.conn.execute("SELECT status FROM folders LIMIT 0")
        except sqlite3.OperationalError:
            self.conn.execute(
                "ALTER TABLE folders ADD COLUMN status TEXT NOT NULL DEFAULT 'ok'"
            )

        # Migrate pending_changes: add ON DELETE CASCADE to photo_id FK.
        # SQLite doesn't support ALTER FOREIGN KEY, so recreate the table.
        pc_schema = self.conn.execute(
            "SELECT sql FROM sqlite_master WHERE type='table' AND name='pending_changes'"
        ).fetchone()
        if pc_schema and "ON DELETE CASCADE" not in pc_schema[0].upper().split("PHOTO_ID", 1)[-1].split(",")[0]:
            self.conn.executescript("""
                CREATE TABLE pending_changes_new (
                    id          INTEGER PRIMARY KEY,
                    photo_id    INTEGER REFERENCES photos(id) ON DELETE CASCADE,
                    change_type TEXT,
                    value       TEXT,
                    change_token TEXT,
                    created_at  TEXT DEFAULT (datetime('now')),
                    workspace_id INTEGER REFERENCES workspaces(id) ON DELETE CASCADE
                );
                INSERT INTO pending_changes_new
                    SELECT id, photo_id, change_type, value, change_token, created_at, workspace_id
                    FROM pending_changes;
                DROP TABLE pending_changes;
                ALTER TABLE pending_changes_new RENAME TO pending_changes;
            """)
            self.conn.commit()

        # Ensure indexes exist (for fresh DBs that skip migration, and for
        # legacy DBs where DROP TABLE predictions destroys earlier indexes)
        self.conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_detections_photo "
            "ON detections(photo_id)"
        )
        self.conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_detections_workspace "
            "ON detections(workspace_id)"
        )
        self.conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_predictions_detection "
            "ON predictions(detection_id)"
        )
        self.conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_predictions_status "
            "ON predictions(status)"
        )
        self.conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_collections_workspace "
            "ON collections(workspace_id)"
        )
        self.conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_pending_workspace "
            "ON pending_changes(workspace_id)"
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
            """INSERT INTO workspaces (name, config_overrides, ui_state)
               VALUES (?, ?, ?)""",
            (name,
             json.dumps(config_overrides) if config_overrides else None,
             json.dumps(ui_state) if ui_state else None),
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
            dict with workspace overrides merged on top of global config
        """
        ws = self.get_workspace(self._active_workspace_id)
        if not ws or not ws["config_overrides"]:
            return global_config
        try:
            overrides = json.loads(ws["config_overrides"]) if isinstance(ws["config_overrides"], str) else ws["config_overrides"]
            result = dict(global_config)
            result.update(overrides)
            return result
        except (json.JSONDecodeError, TypeError):
            return global_config

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

        Moves: workspace_folders rows, detections (with child predictions),
        and pending_changes. Collections and edit_history stay behind.

        Returns:
            dict with keys: folders_moved, detections_moved, pending_changes_moved
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
            return {"folders_moved": 0, "detections_moved": 0, "pending_changes_moved": 0}

        placeholders = ",".join("?" for _ in folder_ids)

        try:
            # Move detections (predictions follow via detection_id FK)
            cur = self.conn.execute(
                f"""UPDATE detections SET workspace_id = ?
                    WHERE workspace_id = ?
                    AND photo_id IN (SELECT id FROM photos WHERE folder_id IN ({placeholders}))""",
                [target_ws_id, source_ws_id] + list(folder_ids),
            )
            detections_moved = cur.rowcount

            # Move pending_changes
            cur = self.conn.execute(
                f"""UPDATE pending_changes SET workspace_id = ?
                    WHERE workspace_id = ?
                    AND photo_id IN (SELECT id FROM photos WHERE folder_id IN ({placeholders}))""",
                [target_ws_id, source_ws_id] + list(folder_ids),
            )
            pending_changes_moved = cur.rowcount

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
            "detections_moved": detections_moved,
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

    # -- Folders --

    def add_folder(self, path, name=None, parent_id=None):
        """Insert a folder. Automatically links it to the active workspace.

        Returns the folder id.
        """
        cur = self.conn.execute(
            "INSERT OR IGNORE INTO folders (path, name, parent_id) VALUES (?, ?, ?)",
            (path, name, parent_id),
        )
        self.conn.commit()
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

        ``parent_id`` is rewritten to the nearest ancestor that is also linked
        to the active workspace AND has ``status='ok'``. If no such ancestor
        exists, ``parent_id`` is NULL. This keeps the returned set a
        well-formed tree: callers that group by ``parent_id`` (notably the
        browse-page folder sidebar) never leave a linked folder dangling under
        an ancestor that was filtered out of the result.
        """
        ws = self._ws_id()
        return self.conn.execute(
            """WITH RECURSIVE
               visible(id) AS (
                   SELECT f.id FROM folders f
                   JOIN workspace_folders wf ON wf.folder_id = f.id
                   WHERE wf.workspace_id = ? AND f.status = 'ok'
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
                      f.photo_count
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

        Returns the number of folders whose status changed.
        """
        rows = self.conn.execute("SELECT id, path, status FROM folders").fetchall()
        changed = 0
        for row in rows:
            exists = os.path.exists(row["path"])
            new_status = "ok" if exists else "missing"
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
                 "JOIN folders f ON f.id = p.folder_id AND f.status = 'ok'"]

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
            if criteria["has_predictions"]:
                conditions.append(
                    "EXISTS (SELECT 1 FROM predictions pr WHERE pr.photo_id = p.id AND pr.workspace_id = ?)"
                )
                params.append(self._ws_id())
            else:
                conditions.append(
                    "NOT EXISTS (SELECT 1 FROM predictions pr WHERE pr.photo_id = p.id AND pr.workspace_id = ?)"
                )
                params.append(self._ws_id())
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
        cur = self.conn.execute(
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
        self.conn.commit()
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

    def find_duplicate_groups(self):
        """Return [{file_hash, photo_ids: [...]}] for every hash with 2+ non-rejected rows.

        Used by the duplicate-scan job to preview groups without applying.
        """
        rows = self.conn.execute(
            """
            SELECT file_hash, GROUP_CONCAT(id) AS ids
            FROM photos
            WHERE file_hash IS NOT NULL AND flag != 'rejected'
            GROUP BY file_hash
            HAVING COUNT(*) > 1
            """
        ).fetchall()
        return [
            {
                "file_hash": r["file_hash"],
                "photo_ids": [int(x) for x in r["ids"].split(",")],
            }
            for r in rows
        ]

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
        from duplicates import (
            DupCandidate,
            PhotoMetadata,
            merge_metadata,
            resolve_duplicates,
        )

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

        candidates = [
            DupCandidate(
                id=r["id"],
                path=os.path.join(r["folder_path"] or "", r["filename"] or ""),
                mtime=r["file_mtime"] or 0.0,
            )
            for r in rows
        ]
        winner_id, losers_with_reasons = resolve_duplicates(candidates)
        loser_ids = [lid for lid, _reason in losers_with_reasons]

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
        return {
            "winner_id": winner_id,
            "loser_ids": list(loser_ids),
            "rejected": len(loser_ids),
        }

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
               JOIN folders f ON f.id = p.folder_id AND f.status = 'ok'
               WHERE wf.workspace_id = ?""",
            (self._ws_id(),),
        ).fetchone()[0]

    def count_folders(self):
        """Return folder count for the active workspace."""
        return self.conn.execute(
            """SELECT COUNT(*) FROM folders f
               JOIN workspace_folders wf ON wf.folder_id = f.id
               WHERE wf.workspace_id = ? AND f.status = 'ok'""",
            (self._ws_id(),),
        ).fetchone()[0]

    def count_keywords(self):
        """Return count of keywords used by photos in the active workspace."""
        return self.conn.execute(
            """SELECT COUNT(DISTINCT pk.keyword_id)
               FROM photo_keywords pk
               JOIN photos p ON p.id = pk.photo_id
               JOIN workspace_folders wf ON wf.folder_id = p.folder_id
               JOIN folders f ON f.id = p.folder_id AND f.status = 'ok'
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
        ("label_embedding", "p.embedding IS NOT NULL"),
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

        ``total`` is the number of photos in active (status='ok') folders of
        the workspace. Each other key is the count of those photos for which
        the named pipeline stage has produced output. ``detected`` and
        ``classified`` are joined from the detections/predictions tables;
        everything else is a simple NOT NULL check on ``photos``.
        """
        ws = self._ws_id()
        photo_row = self.conn.execute(
            f"""SELECT
                COUNT(*) AS total,
                {self._coverage_select_fragment()}
            FROM photos p
            JOIN workspace_folders wf ON wf.folder_id = p.folder_id
            JOIN folders f ON f.id = p.folder_id AND f.status = 'ok'
            WHERE wf.workspace_id = ?""",
            (ws,),
        ).fetchone()
        detected = self.conn.execute(
            """SELECT COUNT(DISTINCT d.photo_id)
               FROM detections d
               JOIN photos p ON p.id = d.photo_id
               JOIN workspace_folders wf ON wf.folder_id = p.folder_id
               JOIN folders f ON f.id = p.folder_id AND f.status = 'ok'
               WHERE d.workspace_id = ? AND wf.workspace_id = ?""",
            (ws, ws),
        ).fetchone()[0] or 0
        classified = self.conn.execute(
            """SELECT COUNT(DISTINCT d.photo_id)
               FROM predictions pr
               JOIN detections d ON d.id = pr.detection_id
               JOIN photos p ON p.id = d.photo_id
               JOIN workspace_folders wf ON wf.folder_id = p.folder_id
               JOIN folders f ON f.id = p.folder_id AND f.status = 'ok'
               WHERE d.workspace_id = ? AND wf.workspace_id = ?""",
            (ws, ws),
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
        ``status='ok'``. Each row carries ``folder_id``, ``path``, ``name``,
        ``total`` (photos in that folder only — descendants are NOT rolled
        in), and the same coverage keys as :meth:`get_coverage_stats`.
        Folders with zero photos are included so the dashboard can still
        show them as 0 / 0 if it chooses.
        """
        ws = self._ws_id()
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
            WHERE wf.workspace_id = ? AND f.status = 'ok'
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
               JOIN folders f ON f.id = p.folder_id AND f.status = 'ok'
               WHERE d.workspace_id = ? AND wf.workspace_id = ?
               GROUP BY p.folder_id""",
            (ws, ws),
        ).fetchall()
        cls_rows = self.conn.execute(
            """SELECT p.folder_id AS folder_id,
                      COUNT(DISTINCT d.photo_id) AS classified
               FROM predictions pr
               JOIN detections d ON d.id = pr.detection_id
               JOIN photos p ON p.id = d.photo_id
               JOIN workspace_folders wf ON wf.folder_id = p.folder_id
               JOIN folders f ON f.id = p.folder_id AND f.status = 'ok'
               WHERE d.workspace_id = ? AND wf.workspace_id = ?
               GROUP BY p.folder_id""",
            (ws, ws),
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
        """Return counts of photos with masks, detections, and sharpness data."""
        ws = self._ws_id()
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
               WHERE d.workspace_id = ? AND wf.workspace_id = ?""",
            (ws, ws),
        ).fetchone()[0]
        return {
            "masks": row["masks"] or 0,
            "detections": det_count or 0,
            "sharpness": row["sharpness"] or 0,
        }

    def get_dashboard_stats(self):
        """Return aggregate statistics for the dashboard."""
        ws = self._ws_id()

        top_keywords = self.conn.execute(
            """SELECT k.name, k.is_species, COUNT(pk.photo_id) as photo_count
               FROM keywords k
               JOIN photo_keywords pk ON pk.keyword_id = k.id
               JOIN photos p ON p.id = pk.photo_id
               JOIN workspace_folders wf ON wf.folder_id = p.folder_id
               JOIN folders f ON f.id = p.folder_id AND f.status = 'ok'
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
            JOIN folders f ON f.id = p.folder_id AND f.status = 'ok'
            WHERE p.timestamp IS NOT NULL AND wf.workspace_id = ?
            GROUP BY month
            ORDER BY month""",
            (ws,),
        ).fetchall()

        rating_dist = self.conn.execute(
            """SELECT p.rating, COUNT(*) as count
            FROM photos p
            JOIN workspace_folders wf ON wf.folder_id = p.folder_id
            JOIN folders f ON f.id = p.folder_id AND f.status = 'ok'
            WHERE wf.workspace_id = ?
            GROUP BY p.rating
            ORDER BY p.rating""",
            (ws,),
        ).fetchall()

        flag_dist = self.conn.execute(
            """SELECT p.flag, COUNT(*) as count
            FROM photos p
            JOIN workspace_folders wf ON wf.folder_id = p.folder_id
            JOIN folders f ON f.id = p.folder_id AND f.status = 'ok'
            WHERE wf.workspace_id = ?
            GROUP BY p.flag""",
            (ws,),
        ).fetchall()

        prediction_status = self.conn.execute(
            """SELECT pr.status, COUNT(*) as count
            FROM predictions pr
            JOIN detections d ON d.id = pr.detection_id
            WHERE d.workspace_id = ?
            GROUP BY pr.status""",
            (ws,),
        ).fetchall()

        classified_count = self.conn.execute(
            """SELECT COUNT(DISTINCT d.photo_id)
               FROM predictions pr
               JOIN detections d ON d.id = pr.detection_id
               WHERE d.workspace_id = ?""",
            (ws,),
        ).fetchone()[0]

        photos_by_hour = self.conn.execute(
            """SELECT CAST(substr(p.timestamp, 12, 2) AS INTEGER) as hour, COUNT(*) as count
            FROM photos p
            JOIN workspace_folders wf ON wf.folder_id = p.folder_id
            JOIN folders f ON f.id = p.folder_id AND f.status = 'ok'
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
            JOIN folders f ON f.id = p.folder_id AND f.status = 'ok'
            WHERE wf.workspace_id = ?
            GROUP BY bucket
            ORDER BY bucket""",
            (ws,),
        ).fetchall()

        detected_count = self.conn.execute(
            """SELECT COUNT(DISTINCT d.photo_id)
               FROM detections d
               JOIN photos p ON p.id = d.photo_id
               JOIN workspace_folders wf ON wf.folder_id = p.folder_id
               JOIN folders f ON f.id = p.folder_id AND f.status = 'ok'
               WHERE d.workspace_id = ? AND wf.workspace_id = ?""",
            (ws, ws),
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
                       "\nJOIN folders f ON f.id = p.folder_id AND f.status = 'ok'")

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
            JOIN folders f ON f.id = p.folder_id AND f.status = 'ok'
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
                       "\nJOIN folders f ON f.id = p.folder_id AND f.status = 'ok'")
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
                       "\nJOIN folders f ON f.id = p.folder_id AND f.status = 'ok'")
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
                       "\nJOIN folders f ON f.id = p.folder_id AND f.status = 'ok'")
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
               JOIN folders f ON f.id = p.folder_id AND f.status = 'ok'
               WHERE wf.workspace_id = ?""",
            (ws,),
        ).fetchone()[0]

        # Filtered count
        filtered_total = self.conn.execute(
            f"SELECT COUNT(DISTINCT p.id) FROM photos p {join_clause} {where}",
            params,
        ).fetchone()[0]

        # Classified vs unclassified (within filter)
        classified = self.conn.execute(
            f"""SELECT COUNT(DISTINCT p.id) FROM photos p
                {join_clause}
                JOIN detections det ON det.photo_id = p.id AND det.workspace_id = ?
                JOIN predictions pred ON pred.detection_id = det.id
                {where}""",
            [ws] + params,
        ).fetchone()[0]

        # Top species (within filter)
        # Use a CTE to select the single best prediction per photo (highest confidence,
        # non-rejected) to avoid inflating species counts when multiple models have
        # predicted different species for the same photo.
        top_species = self.conn.execute(
            f"""WITH best_pred AS (
                    SELECT det.photo_id, pred.species,
                           ROW_NUMBER() OVER (
                               PARTITION BY det.photo_id
                               ORDER BY pred.confidence DESC
                           ) AS rn
                    FROM predictions pred
                    JOIN detections det ON det.id = pred.detection_id
                    WHERE det.workspace_id = ? AND pred.status != 'rejected'
                )
                SELECT bp.species, COUNT(DISTINCT p.id) as count
                FROM photos p
                {join_clause}
                JOIN best_pred bp ON bp.photo_id = p.id AND bp.rn = 1
                {where}
                GROUP BY bp.species
                ORDER BY count DESC
                LIMIT 5""",
            [ws] + params,
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

        Returns photos that have non-null latitude and longitude. No pagination —
        returns all matching photos for map rendering. Includes the photo's
        species keyword (or NULL if none), derived from photo_keywords joined to
        keywords where is_species = 1.
        """
        conditions = ["wf.workspace_id = ?",
                      "p.latitude IS NOT NULL",
                      "p.longitude IS NOT NULL"]
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

        join_clause = ("JOIN workspace_folders wf ON wf.folder_id = p.folder_id"
                       "\nJOIN folders f ON f.id = p.folder_id AND f.status = 'ok'")
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
            SELECT p.id, p.latitude, p.longitude, p.thumb_path, p.filename,
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

        Uses the same derivation as get_geolocated_photos: species keywords
        (is_species = 1) tagged on the photo.  Only considers photos that have
        GPS coordinates, so every returned species can actually produce a map marker.
        """
        ws = self._ws_id()
        return [
            row[0]
            for row in self.conn.execute(
                """
                SELECT DISTINCT k.name
                FROM photos p
                JOIN workspace_folders wf ON wf.folder_id = p.folder_id
                JOIN folders f ON f.id = p.folder_id AND f.status = 'ok'
                JOIN photo_keywords pk ON pk.photo_id = p.id
                JOIN keywords k ON k.id = pk.keyword_id AND k.is_species = 1
                WHERE wf.workspace_id = ?
                  AND p.latitude IS NOT NULL
                  AND p.longitude IS NOT NULL
                ORDER BY k.name ASC
                """,
                (ws,),
            ).fetchall()
        ]

    def count_photos_without_gps(self):
        """Count photos in active workspace that lack GPS coordinates."""
        row = self.conn.execute(
            """
            SELECT COUNT(*) FROM photos p
            JOIN workspace_folders wf ON wf.folder_id = p.folder_id
            JOIN folders f ON f.id = p.folder_id AND f.status = 'ok'
            WHERE wf.workspace_id = ?
              AND (p.latitude IS NULL OR p.longitude IS NULL)
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
        self.conn.commit()

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
        self.conn.commit()

    def update_photo_mask(self, photo_id, mask_path):
        """Store the mask file path for a photo."""
        self.conn.execute(
            "UPDATE photos SET mask_path=? WHERE id=?",
            (mask_path, photo_id),
        )
        self.conn.commit()

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
        self.conn.commit()

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
        ws_id = self._ws_id()
        if folder_ids:
            placeholders = ",".join("?" * len(folder_ids))
            rows = self.conn.execute(
                f"""SELECT p.id, p.folder_id, p.filename,
                           d.box_x, d.box_y, d.box_w, d.box_h,
                           d.detector_confidence
                    FROM photos p
                    JOIN detections d ON d.photo_id = p.id AND d.workspace_id = ?
                    WHERE p.folder_id IN ({placeholders})
                      AND p.mask_path IS NULL
                    ORDER BY p.id, d.detector_confidence DESC""",
                [ws_id, *folder_ids],
            ).fetchall()
        else:
            rows = self.conn.execute(
                """SELECT p.id, p.folder_id, p.filename,
                          d.box_x, d.box_y, d.box_w, d.box_h,
                          d.detector_confidence
                   FROM photos p
                   JOIN workspace_folders wf ON wf.folder_id = p.folder_id
                   JOIN detections d ON d.photo_id = p.id AND d.workspace_id = ?
                   WHERE wf.workspace_id = ?
                     AND p.mask_path IS NULL
                   ORDER BY p.id, d.detector_confidence DESC""",
                (ws_id, ws_id),
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
        ws_id = self._ws_id()
        if photo_ids is not None:
            photo_ids = list(photo_ids)
            if not photo_ids:
                return []
            placeholders = ",".join("?" for _ in photo_ids)
            extra_where = f" AND p.id IN ({placeholders})"
            params = (ws_id, ws_id, *photo_ids)
        else:
            extra_where = ""
            params = (ws_id, ws_id)
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
                AND d.workspace_id = ?
                AND d.detector_model != 'full-image'
               JOIN predictions pr ON pr.detection_id = d.id
               WHERE p.mask_path IS NOT NULL
                 AND p.eye_tenengrad IS NULL{extra_where}
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
        self.conn.commit()

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

    def add_keyword(self, name, parent_id=None, is_species=False, _commit=True):
        """Insert a keyword. Returns existing id if duplicate (case-insensitive).

        If a keyword with the same name but different casing exists, reuses
        the existing one rather than creating a duplicate.

        For new species keywords, auto-detects the user's casing convention
        from existing keywords and applies it (unless overridden by config).

        Args:
            _commit: If False, skip the internal commit (caller is responsible
                     for committing the transaction).
        """
        # Case-insensitive lookup
        if parent_id is None:
            existing = self.conn.execute(
                "SELECT id FROM keywords WHERE name = ? COLLATE NOCASE AND parent_id IS NULL",
                (name,),
            ).fetchone()
        else:
            existing = self.conn.execute(
                "SELECT id FROM keywords WHERE name = ? COLLATE NOCASE AND parent_id = ?",
                (name, parent_id),
            ).fetchone()
        if existing:
            # Update is_species and type if it wasn't set before
            if is_species:
                self.conn.execute(
                    "UPDATE keywords SET is_species = 1, type = 'taxonomy' WHERE id = ? AND is_species = 0",
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

        # Auto-detect taxonomy type from taxa table
        kw_type = 'general'
        taxon_id = None
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
        self.conn.execute(
            "INSERT OR IGNORE INTO photo_keywords (photo_id, keyword_id) VALUES (?, ?)",
            (photo_id, keyword_id),
        )
        if _commit:
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
               JOIN folders f ON f.id = p.folder_id AND f.status = 'ok'
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
                   JOIN folders f0 ON f0.id = p.folder_id AND f0.status = 'ok'
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
                 AND f.status = 'ok'
               GROUP BY f.id
               ORDER BY latest_photo DESC""",
            (ws, ws, ws),
        ).fetchall()

    VALID_KEYWORD_TYPES = ('general', 'taxonomy', 'location', 'descriptive', 'people', 'event')

    def update_keyword(self, keyword_id, **kwargs):
        """Update keyword fields. Supports: type, taxon_id, latitude, longitude, name."""
        if 'type' in kwargs and kwargs['type'] not in self.VALID_KEYWORD_TYPES:
            raise ValueError(f"Invalid keyword type: {kwargs['type']}")
        allowed = {'type', 'taxon_id', 'latitude', 'longitude', 'name'}
        updates = {k: v for k, v in kwargs.items() if k in allowed}
        if not updates:
            return
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
                      k.latitude, k.longitude,
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
    ):
        """Store a classification prediction for a detection.

        Uses INSERT OR IGNORE so re-running classification doesn't destroy
        existing predictions that the user may have already reviewed.
        Use clear_predictions() first if you want a fresh start.

        Args:
            detection_id: the detection ID (from detections table)
            taxonomy: optional dict with keys kingdom, phylum, class, order,
                      family, genus, scientific_name from taxonomy lookup
        """
        if detection_id is None:
            raise ValueError(
                "add_prediction requires a non-null detection_id; "
                "predictions without a detection row are orphaned and "
                "invisible to workspace-scoped queries"
            )
        tax = taxonomy or {}
        self.conn.execute(
            """INSERT OR IGNORE INTO predictions
               (detection_id, species, confidence, model, category, status,
                group_id, vote_count, total_votes, individual,
                taxonomy_kingdom, taxonomy_phylum, taxonomy_class,
                taxonomy_order, taxonomy_family, taxonomy_genus, scientific_name)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            (
                detection_id,
                species,
                confidence,
                model,
                category,
                status,
                group_id,
                vote_count,
                total_votes,
                individual,
                tax.get("kingdom"),
                tax.get("phylum"),
                tax.get("class"),
                tax.get("order"),
                tax.get("family"),
                tax.get("genus"),
                tax.get("scientific_name"),
            ),
        )
        self.conn.commit()

    def clear_predictions(self, model=None, collection_photo_ids=None):
        """Clear predictions, optionally filtered by model and/or photo set."""
        if collection_photo_ids is not None:
            placeholders = ",".join("?" for _ in collection_photo_ids)
            if model:
                self.conn.execute(
                    f"""DELETE FROM predictions WHERE id IN (
                        SELECT pr.id FROM predictions pr
                        JOIN detections d ON d.id = pr.detection_id
                        WHERE d.workspace_id = ? AND pr.model = ?
                        AND d.photo_id IN ({placeholders})
                    )""",
                    [self._ws_id(), model, *collection_photo_ids],
                )
            else:
                self.conn.execute(
                    f"""DELETE FROM predictions WHERE id IN (
                        SELECT pr.id FROM predictions pr
                        JOIN detections d ON d.id = pr.detection_id
                        WHERE d.workspace_id = ? AND d.photo_id IN ({placeholders})
                    )""",
                    [self._ws_id(), *collection_photo_ids],
                )
        else:
            conditions = ["d.workspace_id = ?"]
            params = [self._ws_id()]
            if model:
                conditions.append("pr.model = ?")
                params.append(model)
            where = " AND ".join(conditions)
            self.conn.execute(
                f"""DELETE FROM predictions WHERE id IN (
                    SELECT pr.id FROM predictions pr
                    JOIN detections d ON d.id = pr.detection_id
                    WHERE {where}
                )""",
                params,
            )
        self.conn.commit()

    def get_predictions(self, photo_ids=None, model=None, status=None):
        """Get predictions with photo and detection info, optionally filtered."""
        conditions = ["d.workspace_id = ?"]
        params = [self._ws_id()]
        if photo_ids is not None:
            placeholders = ",".join("?" for _ in photo_ids)
            conditions.append(f"d.photo_id IN ({placeholders})")
            params.extend(photo_ids)
        if model:
            conditions.append("pr.model = ?")
            params.append(model)
        if status:
            conditions.append("pr.status = ?")
            params.append(status)
        where = "WHERE " + " AND ".join(conditions)
        return self.conn.execute(
            f"""SELECT pr.*, d.photo_id, d.box_x, d.box_y, d.box_w, d.box_h,
                       d.detector_confidence, d.detector_model,
                       p.filename, p.timestamp
                FROM predictions pr
                JOIN detections d ON d.id = pr.detection_id
                JOIN photos p ON p.id = d.photo_id
                {where} ORDER BY pr.confidence DESC""",
            params,
        ).fetchall()

    def update_prediction_status(self, prediction_id, status, _commit=True):
        """Update prediction status ('pending', 'accepted', 'rejected').

        Args:
            _commit: If False, skip the internal commit (caller is responsible
                     for committing the transaction).
        """
        self.conn.execute(
            "UPDATE predictions SET status = ? WHERE id = ?", (status, prediction_id)
        )
        if _commit:
            self.conn.commit()

    def get_group_predictions(self, group_id):
        """Get all predictions and photo data for a burst group.

        Each returned row is a dict with an ``alternatives`` list containing
        the per-detection alternative species predictions (status='alternative'),
        sorted by confidence descending.
        """
        primaries = self.conn.execute(
            """SELECT pr.*, d.photo_id, d.box_x, d.box_y, d.box_w, d.box_h,
                      d.detector_confidence, p.filename, p.timestamp, p.sharpness,
                      p.quality_score, p.subject_sharpness, p.subject_size,
                      p.rating, p.flag, p.width, p.height
               FROM predictions pr
               JOIN detections d ON d.id = pr.detection_id
               JOIN photos p ON p.id = d.photo_id
               WHERE pr.group_id = ? AND d.workspace_id = ?
               ORDER BY p.quality_score DESC""",
            (group_id, self._ws_id()),
        ).fetchall()
        rows = [dict(r) for r in primaries]
        if not rows:
            return rows
        # Alternatives are correlated by (detection_id, model): a detection may
        # have been classified by multiple models (and those may even share a
        # group), so we must not merge alternatives across models.
        #
        # Fetch all alternatives for the distinct detection_ids in one IN(...)
        # query, then filter by model in Python. This keeps the SQL expression
        # depth bounded even for very large burst groups (SQLite's default
        # expression-depth limit breaks with one OR branch per row).
        det_model_pairs = {
            (r['detection_id'], r.get('model'))
            for r in rows if r.get('detection_id') is not None
        }
        alts_by_key = {pair: [] for pair in det_model_pairs}
        det_ids = list({did for did, _ in det_model_pairs})
        if det_ids:
            placeholders = ','.join('?' * len(det_ids))
            alt_rows = self.conn.execute(
                f"""SELECT detection_id, model, species, confidence
                    FROM predictions
                    WHERE status = 'alternative' AND detection_id IN ({placeholders})
                    ORDER BY confidence DESC""",
                det_ids,
            ).fetchall()
            for a in alt_rows:
                key = (a['detection_id'], a['model'])
                if key in alts_by_key:
                    alts_by_key[key].append(
                        {'species': a['species'], 'confidence': a['confidence']}
                    )
        for r in rows:
            r['alternatives'] = alts_by_key.get((r.get('detection_id'), r.get('model')), [])
        return rows

    def update_predictions_status_by_photo(self, photo_id, status):
        """Update status for all predictions of a photo in the active workspace."""
        self.conn.execute(
            """UPDATE predictions SET status = ?
               WHERE detection_id IN (
                   SELECT id FROM detections
                   WHERE photo_id = ? AND workspace_id = ?
               )""",
            (status, photo_id, self._ws_id()),
        )
        self.conn.commit()

    def ungroup_prediction(self, prediction_id):
        """Remove a prediction from its group."""
        self.conn.execute(
            """UPDATE predictions SET group_id = NULL
               WHERE id = ? AND detection_id IN (
                   SELECT id FROM detections WHERE workspace_id = ?
               )""",
            (prediction_id, self._ws_id()),
        )
        self.conn.commit()

    def get_existing_prediction_photo_ids(self, model):
        """Return set of photo_ids that have predictions for a model."""
        rows = self.conn.execute(
            """SELECT DISTINCT d.photo_id FROM predictions pr
               JOIN detections d ON d.id = pr.detection_id
               WHERE pr.model = ? AND d.workspace_id = ?""",
            (model, self._ws_id()),
        ).fetchall()
        return {r["photo_id"] for r in rows}

    def get_prediction_for_photo(self, photo_id, model):
        """Return species, confidence, and detection_id for a photo's prediction by model, or None."""
        return self.conn.execute(
            """SELECT pr.species, pr.confidence, pr.detection_id FROM predictions pr
               JOIN detections d ON d.id = pr.detection_id
               WHERE d.photo_id = ? AND pr.model = ? AND d.workspace_id = ?""",
            (photo_id, model, self._ws_id()),
        ).fetchone()

    def get_photo_embedding(self, photo_id):
        """Return the embedding blob for a photo, or None."""
        row = self.conn.execute(
            "SELECT embedding FROM photos WHERE id = ?", (photo_id,),
        ).fetchone()
        return row["embedding"] if row else None

    def store_photo_embedding(self, photo_id, embedding_bytes, model=None,
                              verify_workspace=False):
        """Store an embedding blob for a photo, optionally with model name.

        Args:
            verify_workspace: when True, raises ValueError if the photo is
                not in the active workspace.  Defaults to False because this
                method is typically called from background classify jobs that
                already iterate only over workspace-scoped photos.
        """
        if verify_workspace:
            self._verify_photo_in_workspace(photo_id)
        self.conn.execute(
            "UPDATE photos SET embedding = ?, embedding_model = ? WHERE id = ?",
            (embedding_bytes, model, photo_id),
        )
        self.conn.commit()

    def get_embeddings_by_model(self, model_name):
        """Return (photo_id, embedding_blob) pairs for photos with given model.

        Only returns photos in folders visible to the active workspace.
        """
        rows = self.conn.execute(
            """SELECT p.id, p.embedding FROM photos p
               JOIN workspace_folders wf ON wf.folder_id = p.folder_id
               WHERE p.embedding IS NOT NULL
                 AND p.embedding_model = ?
                 AND wf.workspace_id = ?""",
            (model_name, self._ws_id()),
        ).fetchall()
        return [(row["id"], row["embedding"]) for row in rows]

    def update_prediction_group_info(self, detection_id, model, group_id, vote_count, total_votes, individual):
        """Update group info on an existing prediction.

        Only updates the primary (non-alternative) prediction row so that
        alternative rows for the same detection+model are not assigned group
        metadata they do not belong to.
        """
        self.conn.execute(
            """UPDATE predictions
               SET group_id=?, vote_count=?, total_votes=?, individual=?
               WHERE detection_id=? AND model=? AND status != 'alternative'""",
            (group_id, vote_count, total_votes, individual, detection_id, model),
        )
        self.conn.commit()

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
        pred = self.conn.execute(
            """SELECT pr.*, d.photo_id
               FROM predictions pr
               JOIN detections d ON d.id = pr.detection_id
               WHERE pr.id = ?""",
            (prediction_id,),
        ).fetchone()
        if not pred:
            return None

        try:
            # Reject sibling predictions for same detection+model
            # (covers both accepting an alternative and accepting the top-1)
            self.conn.execute(
                """UPDATE predictions SET status = 'rejected'
                   WHERE detection_id = ? AND model = ? AND id != ? AND status IN ('pending', 'alternative')""",
                (pred["detection_id"], pred["model"], prediction_id),
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

            # If grouped, accept all predictions in the group
            if pred["group_id"]:
                group_preds = self.conn.execute(
                    """SELECT pr.*, d.photo_id
                       FROM predictions pr
                       JOIN detections d ON d.id = pr.detection_id
                       WHERE pr.group_id = ? AND pr.model = ? AND d.workspace_id = ?""",
                    (pred["group_id"], pred["model"], self._ws_id()),
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

    def save_detections(self, photo_id, detections, detector_model=None):
        """Store detection bounding boxes for a photo.

        Args:
            photo_id: the photo ID
            detections: list of dicts with keys: box (dict with x,y,w,h),
                        confidence (float), category (str)
            detector_model: name of the detector model

        Returns:
            list of detection IDs
        """
        ws_id = self._ws_id()
        ids = []
        for det in detections:
            box = det["box"]
            cur = self.conn.execute(
                """INSERT INTO detections
                   (photo_id, workspace_id, box_x, box_y, box_w, box_h,
                    detector_confidence, category, detector_model)
                   VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                (photo_id, ws_id, box["x"], box["y"], box["w"], box["h"],
                 det["confidence"], det.get("category", "animal"), detector_model),
            )
            ids.append(cur.lastrowid)
        self.conn.commit()
        return ids

    def get_detections(self, photo_id):
        """Get all detections for a photo in the active workspace."""
        return self.conn.execute(
            """SELECT * FROM detections
               WHERE photo_id = ? AND workspace_id = ?
               ORDER BY detector_confidence DESC""",
            (photo_id, self._ws_id()),
        ).fetchall()

    def get_detections_for_photos(self, photo_ids):
        """Return {photo_id: [det_dict, ...]} for a batch of photos.

        Each det_dict has keys: x, y, w, h, confidence, category.
        Lists are ordered by confidence DESC. Scoped to the active workspace.
        Photos with no detections are omitted from the result.
        """
        if not photo_ids:
            return {}
        placeholders = ",".join("?" for _ in photo_ids)
        rows = self.conn.execute(
            f"""SELECT photo_id, box_x, box_y, box_w, box_h,
                       detector_confidence, category
                FROM detections
                WHERE workspace_id = ?
                  AND photo_id IN ({placeholders})
                ORDER BY photo_id, detector_confidence DESC""",
            [self._ws_id(), *photo_ids],
        ).fetchall()
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

    def clear_detections(self, photo_id):
        """Remove all detections (and cascaded predictions) for a photo."""
        self.conn.execute(
            "DELETE FROM detections WHERE photo_id = ? AND workspace_id = ?",
            (photo_id, self._ws_id()),
        )
        self.conn.commit()

    def get_existing_detection_photo_ids(self):
        """Return set of photo_ids that already have detections in this workspace."""
        rows = self.conn.execute(
            "SELECT DISTINCT photo_id FROM detections WHERE workspace_id = ?",
            (self._ws_id(),),
        ).fetchall()
        return {r["photo_id"] for r in rows}

    def get_detection_ids_for_photos(self, photo_ids):
        """Return {photo_id: set(detection_id, ...)} for the given photo IDs.

        Only returns rows from the active workspace.  Used to snapshot
        pre-run detection IDs so that a reclassify pass can delete only
        the *stale* rows after fresh ones have been inserted, avoiding the
        cascade-delete that would destroy other-model predictions.

        IDs are queried in chunks of at most 900 to stay safely under
        SQLite's default bound-parameter limit (SQLITE_LIMIT_VARIABLE_NUMBER,
        typically 999 in production builds).
        """
        if not photo_ids:
            return {}
        ws_id = self._ws_id()
        result: dict = {}
        ids = list(photo_ids)
        _CHUNK = 900
        for i in range(0, len(ids), _CHUNK):
            chunk = ids[i : i + _CHUNK]
            placeholders = ",".join("?" * len(chunk))
            rows = self.conn.execute(
                f"SELECT id, photo_id FROM detections "
                f"WHERE photo_id IN ({placeholders}) AND workspace_id = ?",
                (*chunk, ws_id),
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
    _NON_UNDOABLE = ('prediction_reject', 'discard')

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
                    # Restore all predictions for this detection to pre-accept state
                    pred_row = self.conn.execute(
                        "SELECT detection_id, model FROM predictions WHERE id = ?",
                        (pred_id,),
                    ).fetchone()
                    if pred_row:
                        # Set all to 'alternative' first
                        self.conn.execute(
                            """UPDATE predictions SET status = 'alternative'
                               WHERE detection_id = ? AND model = ?
                               AND status IN ('accepted', 'rejected')""",
                            (pred_row["detection_id"], pred_row["model"]),
                        )
                        # Promote highest-confidence to 'pending'
                        top = self.conn.execute(
                            """SELECT id FROM predictions
                               WHERE detection_id = ? AND model = ?
                               ORDER BY confidence DESC LIMIT 1""",
                            (pred_row["detection_id"], pred_row["model"]),
                        ).fetchone()
                        if top:
                            self.conn.execute(
                                "UPDATE predictions SET status = 'pending' WHERE id = ?",
                                (top["id"],),
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
                    self.update_prediction_status(pred_id, 'accepted')
                    # Re-reject siblings (mirrors accept_prediction behavior)
                    pred_row = self.conn.execute(
                        "SELECT detection_id, model FROM predictions WHERE id = ?",
                        (pred_id,),
                    ).fetchone()
                    if pred_row:
                        self.conn.execute(
                            """UPDATE predictions SET status = 'rejected'
                               WHERE detection_id = ? AND model = ? AND id != ?
                               AND status IN ('pending', 'alternative')""",
                            (pred_row["detection_id"], pred_row["model"], pred_id),
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
            join_clause += (
                " JOIN detections det ON det.photo_id = p.id"
                " AND det.workspace_id = ?"
                " JOIN predictions pred ON pred.detection_id = det.id"
            )
            # Insert workspace param before the existing condition params
            params.insert(0, self._ws_id())

        # Always join folders for folder-under rules, scoped to workspace
        folder_join = " JOIN folders f ON f.id = p.folder_id AND f.status = 'ok'"
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

        Uses the local taxonomy only (no network requests).

        Args:
            taxonomy: a Taxonomy instance with an is_taxon() method
        """
        keywords = self.conn.execute(
            "SELECT id, name FROM keywords WHERE is_species = 0"
        ).fetchall()
        updated = 0
        for kw in keywords:
            if taxonomy.is_taxon(kw["name"]):
                self.conn.execute(
                    "UPDATE keywords SET is_species = 1 WHERE id = ?", (kw["id"],)
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
                "Needs Classification",
                [{"field": "has_species", "op": "equals", "value": 0}],
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
