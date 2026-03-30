"""SQLite database for Vireo photo browser metadata cache."""

import json
import os
import sqlite3
import uuid

_UNSET = object()  # sentinel for "not provided" vs explicit None


class Database:
    """Local SQLite database that caches photo metadata from XMP sidecars.

    Args:
        db_path: path to the SQLite database file (created if missing)
    """

    def __init__(self, db_path):
        db_dir = os.path.dirname(db_path)
        if db_path != ":memory:" and db_dir:
            os.makedirs(db_dir, exist_ok=True)
        self.conn = sqlite3.connect(db_path, check_same_thread=False)
        self.conn.row_factory = sqlite3.Row
        self.conn.execute("PRAGMA journal_mode=WAL")
        self.conn.execute("PRAGMA foreign_keys=ON")
        self.conn.execute("PRAGMA synchronous=NORMAL")
        self.conn.execute("PRAGMA cache_size=-10000")  # 10 MB
        self.conn.execute("PRAGMA temp_store=MEMORY")
        self.conn.execute("PRAGMA mmap_size=30000000")  # 30 MB
        self._active_workspace_id = None
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
                photo_count INTEGER DEFAULT 0
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
                photo_id    INTEGER REFERENCES photos(id),
                change_type TEXT,
                value       TEXT,
                change_token TEXT,
                created_at  TEXT DEFAULT (datetime('now')),
                workspace_id INTEGER REFERENCES workspaces(id) ON DELETE CASCADE
            );

            CREATE TABLE IF NOT EXISTS predictions (
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

            CREATE INDEX IF NOT EXISTS idx_photos_timestamp ON photos(timestamp);
            CREATE INDEX IF NOT EXISTS idx_photos_folder ON photos(folder_id);
            CREATE INDEX IF NOT EXISTS idx_photos_rating ON photos(rating);
            CREATE INDEX IF NOT EXISTS idx_keywords_name ON keywords(name);
            CREATE INDEX IF NOT EXISTS idx_photo_keywords_photo ON photo_keywords(photo_id);
            CREATE INDEX IF NOT EXISTS idx_photo_keywords_keyword ON photo_keywords(keyword_id);
            CREATE INDEX IF NOT EXISTS idx_predictions_photo ON predictions(photo_id);
            CREATE INDEX IF NOT EXISTS idx_predictions_status ON predictions(status);
        """
        )
        # Migrations for existing databases
        try:
            self.conn.execute("SELECT is_species FROM keywords LIMIT 0")
        except Exception:
            self.conn.execute(
                "ALTER TABLE keywords ADD COLUMN is_species INTEGER DEFAULT 0"
            )
        try:
            self.conn.execute("SELECT group_id FROM predictions LIMIT 0")
        except Exception:
            self.conn.execute("ALTER TABLE predictions ADD COLUMN group_id TEXT")
            self.conn.execute("ALTER TABLE predictions ADD COLUMN vote_count INTEGER")
            self.conn.execute("ALTER TABLE predictions ADD COLUMN total_votes INTEGER")
            self.conn.execute("ALTER TABLE predictions ADD COLUMN individual TEXT")
        try:
            self.conn.execute("SELECT sharpness FROM photos LIMIT 0")
        except Exception:
            self.conn.execute("ALTER TABLE photos ADD COLUMN sharpness REAL")
        try:
            self.conn.execute("SELECT quality_score FROM photos LIMIT 0")
        except Exception:
            self.conn.execute("ALTER TABLE photos ADD COLUMN detection_box TEXT")
            self.conn.execute("ALTER TABLE photos ADD COLUMN detection_conf REAL")
            self.conn.execute("ALTER TABLE photos ADD COLUMN subject_sharpness REAL")
            self.conn.execute("ALTER TABLE photos ADD COLUMN subject_size REAL")
            self.conn.execute("ALTER TABLE photos ADD COLUMN quality_score REAL")
        try:
            self.conn.execute("SELECT embedding FROM photos LIMIT 0")
        except Exception:
            self.conn.execute("ALTER TABLE photos ADD COLUMN embedding BLOB")
        try:
            self.conn.execute("SELECT embedding_model FROM photos LIMIT 0")
        except Exception:
            self.conn.execute("ALTER TABLE photos ADD COLUMN embedding_model TEXT")
        try:
            self.conn.execute("SELECT latitude FROM photos LIMIT 0")
        except Exception:
            self.conn.execute("ALTER TABLE photos ADD COLUMN latitude REAL")
            self.conn.execute("ALTER TABLE photos ADD COLUMN longitude REAL")
        try:
            self.conn.execute("SELECT phash FROM photos LIMIT 0")
        except Exception:
            self.conn.execute("ALTER TABLE photos ADD COLUMN phash TEXT")
        try:
            self.conn.execute("SELECT taxonomy_kingdom FROM predictions LIMIT 0")
        except Exception:
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
        except Exception:
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
        except Exception:
            self.conn.execute("ALTER TABLE photos ADD COLUMN noise_estimate REAL")
        # Enhanced EXIF metadata columns
        try:
            self.conn.execute("SELECT focal_length FROM photos LIMIT 0")
        except Exception:
            self.conn.execute("ALTER TABLE photos ADD COLUMN focal_length REAL")
            self.conn.execute("ALTER TABLE photos ADD COLUMN burst_id TEXT")
        # Ingest: file hash for duplicate detection + companion for raw/JPEG pairing
        try:
            self.conn.execute("SELECT file_hash FROM photos LIMIT 0")
        except Exception:
            self.conn.execute("ALTER TABLE photos ADD COLUMN file_hash TEXT")
            self.conn.execute("ALTER TABLE photos ADD COLUMN companion_path TEXT")

        # Full EXIF metadata JSON blob
        try:
            self.conn.execute("SELECT exif_data FROM photos LIMIT 0")
        except Exception:
            self.conn.execute("ALTER TABLE photos ADD COLUMN exif_data TEXT")

        # Edit history tables migration
        try:
            self.conn.execute("SELECT id FROM edit_history LIMIT 0")
        except Exception:
            self.conn.executescript("""
                CREATE TABLE IF NOT EXISTS edit_history (
                    id           INTEGER PRIMARY KEY,
                    workspace_id INTEGER REFERENCES workspaces(id) ON DELETE CASCADE,
                    action_type  TEXT NOT NULL,
                    description  TEXT NOT NULL,
                    new_value    TEXT,
                    is_batch     INTEGER DEFAULT 0,
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

        # Workspace migration for existing databases
        # Check if predictions has workspace_id (detects legacy DBs even though
        # the workspaces table was already created by executescript above)
        needs_workspace_migration = False
        try:
            self.conn.execute("SELECT workspace_id FROM predictions LIMIT 0")
        except Exception:
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
            except Exception:
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
                except Exception:
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
        except Exception:
            self.conn.execute("ALTER TABLE pending_changes ADD COLUMN change_token TEXT")
            self.conn.commit()

        # Keyword type/location/taxon columns (taxonomy support)
        try:
            self.conn.execute("SELECT type FROM keywords LIMIT 0")
        except Exception:
            self.conn.execute(
                "ALTER TABLE keywords ADD COLUMN type TEXT NOT NULL DEFAULT 'general'"
            )
            # Migrate existing is_species=1 keywords to type='taxonomy'
            self.conn.execute(
                "UPDATE keywords SET type = 'taxonomy' WHERE is_species = 1"
            )
        try:
            self.conn.execute("SELECT latitude FROM keywords LIMIT 0")
        except Exception:
            self.conn.execute("ALTER TABLE keywords ADD COLUMN latitude REAL")
            self.conn.execute("ALTER TABLE keywords ADD COLUMN longitude REAL")
        try:
            self.conn.execute("SELECT taxon_id FROM keywords LIMIT 0")
        except Exception:
            self.conn.execute(
                "ALTER TABLE keywords ADD COLUMN taxon_id INTEGER REFERENCES taxa(id)"
            )

        # Ensure indexes exist (for fresh DBs that skip migration, and for
        # legacy DBs where DROP TABLE predictions destroys earlier indexes)
        self.conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_predictions_workspace "
            "ON predictions(workspace_id)"
        )
        self.conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_predictions_photo "
            "ON predictions(photo_id)"
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
        return cur.lastrowid

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

    def add_workspace_folder(self, workspace_id, folder_id):
        """Link a folder to a workspace."""
        self.conn.execute(
            "INSERT OR IGNORE INTO workspace_folders (workspace_id, folder_id) VALUES (?, ?)",
            (workspace_id, folder_id),
        )
        self.conn.commit()

    def remove_workspace_folder(self, workspace_id, folder_id):
        """Unlink a folder from a workspace."""
        self.conn.execute(
            "DELETE FROM workspace_folders WHERE workspace_id = ? AND folder_id = ?",
            (workspace_id, folder_id),
        )
        self.conn.commit()

    def get_workspace_folders(self, workspace_id):
        """Return all folders linked to a workspace."""
        return self.conn.execute(
            """SELECT f.* FROM folders f
               JOIN workspace_folders wf ON wf.folder_id = f.id
               WHERE wf.workspace_id = ?
               ORDER BY f.path""",
            (workspace_id,),
        ).fetchall()

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
        if cur.lastrowid:
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
        """Return folders for the active workspace."""
        return self.conn.execute(
            """SELECT f.id, f.path, f.name, f.parent_id, f.photo_count
               FROM folders f
               JOIN workspace_folders wf ON wf.folder_id = f.id
               WHERE wf.workspace_id = ?
               ORDER BY f.path""",
            (self._ws_id(),),
        ).fetchall()

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
    ):
        """Insert a photo. Returns the photo id."""
        cur = self.conn.execute(
            """INSERT OR IGNORE INTO photos
               (folder_id, filename, extension, file_size, file_mtime, xmp_mtime,
                timestamp, width, height)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)""",
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
            ),
        )
        self.conn.commit()
        if cur.lastrowid:
            return cur.lastrowid
        row = self.conn.execute(
            "SELECT id FROM photos WHERE folder_id = ? AND filename = ?",
            (folder_id, filename),
        ).fetchone()
        return row["id"]

    # Columns to return in photo list queries (excludes large fields)
    PHOTO_COLS = """id, folder_id, filename, extension, file_size, file_mtime, xmp_mtime,
                    timestamp, width, height, rating, flag, thumb_path, sharpness,
                    detection_box, detection_conf, subject_sharpness, subject_size, quality_score,
                    latitude, longitude, companion_path"""

    # Columns for single-photo detail queries (includes exif_data JSON)
    PHOTO_DETAIL_COLS = PHOTO_COLS + ", exif_data"

    def get_photo(self, photo_id):
        """Return a single photo by id, including full metadata."""
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
               WHERE wf.workspace_id = ?""",
            (self._ws_id(),),
        ).fetchone()[0]

    def count_folders(self):
        """Return folder count for the active workspace."""
        return self.conn.execute(
            """SELECT COUNT(*) FROM folders f
               JOIN workspace_folders wf ON wf.folder_id = f.id
               WHERE wf.workspace_id = ?""",
            (self._ws_id(),),
        ).fetchone()[0]

    def count_keywords(self):
        """Return count of keywords used by photos in the active workspace."""
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

    def get_pipeline_feature_counts(self):
        """Return counts of photos with masks, detections, and sharpness data."""
        ws = self._ws_id()
        row = self.conn.execute(
            """SELECT
                SUM(CASE WHEN p.mask_path IS NOT NULL THEN 1 ELSE 0 END) as masks,
                SUM(CASE WHEN p.detection_box IS NOT NULL THEN 1 ELSE 0 END) as detections,
                SUM(CASE WHEN p.subject_tenengrad IS NOT NULL THEN 1 ELSE 0 END) as sharpness
            FROM photos p
            JOIN workspace_folders wf ON wf.folder_id = p.folder_id
            WHERE wf.workspace_id = ?""",
            (ws,),
        ).fetchone()
        return {
            "masks": row["masks"] or 0,
            "detections": row["detections"] or 0,
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
            WHERE p.timestamp IS NOT NULL AND wf.workspace_id = ?
            GROUP BY month
            ORDER BY month""",
            (ws,),
        ).fetchall()

        rating_dist = self.conn.execute(
            """SELECT p.rating, COUNT(*) as count
            FROM photos p
            JOIN workspace_folders wf ON wf.folder_id = p.folder_id
            WHERE wf.workspace_id = ?
            GROUP BY p.rating
            ORDER BY p.rating""",
            (ws,),
        ).fetchall()

        flag_dist = self.conn.execute(
            """SELECT p.flag, COUNT(*) as count
            FROM photos p
            JOIN workspace_folders wf ON wf.folder_id = p.folder_id
            WHERE wf.workspace_id = ?
            GROUP BY p.flag""",
            (ws,),
        ).fetchall()

        prediction_status = self.conn.execute(
            """SELECT status, COUNT(*) as count
            FROM predictions WHERE workspace_id = ?
            GROUP BY status""",
            (ws,),
        ).fetchall()

        classified_count = self.conn.execute(
            "SELECT COUNT(DISTINCT photo_id) FROM predictions WHERE workspace_id = ?",
            (ws,),
        ).fetchone()[0]

        photos_by_hour = self.conn.execute(
            """SELECT CAST(substr(p.timestamp, 12, 2) AS INTEGER) as hour, COUNT(*) as count
            FROM photos p
            JOIN workspace_folders wf ON wf.folder_id = p.folder_id
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
            WHERE wf.workspace_id = ?
            GROUP BY bucket
            ORDER BY bucket""",
            (ws,),
        ).fetchall()

        detected_count = self.conn.execute(
            """SELECT COUNT(*) FROM photos p
            JOIN workspace_folders wf ON wf.folder_id = p.folder_id
            WHERE p.detection_conf IS NOT NULL AND p.detection_conf > 0
            AND wf.workspace_id = ?""",
            (ws,),
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

    def get_calendar_data(self, year, folder_id=None, rating_min=None, keyword=None):
        """Return daily photo counts for a given year, scoped to active workspace."""
        ws = self._ws_id()
        conditions = ["wf.workspace_id = ?", "p.timestamp IS NOT NULL",
                      "substr(p.timestamp, 1, 4) = ?"]
        params = [ws, str(year)]

        join_clause = "JOIN workspace_folders wf ON wf.folder_id = p.folder_id"

        if folder_id is not None:
            conditions.append("p.folder_id = ?")
            params.append(folder_id)
        if rating_min is not None:
            conditions.append("p.rating >= ?")
            params.append(rating_min)
        if keyword is not None:
            join_clause += """
                LEFT JOIN photo_keywords pk ON pk.photo_id = p.id
                LEFT JOIN keywords k ON k.id = pk.keyword_id
            """
            conditions.append("(k.name LIKE ? OR p.filename LIKE ?)")
            params.append(f"%{keyword}%")
            params.append(f"%{keyword}%")

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
    ):
        """Return paginated, filtered photo list scoped to active workspace."""
        conditions = ["wf.workspace_id = ?"]
        params = [self._ws_id()]

        if folder_id is not None:
            conditions.append("p.folder_id = ?")
            params.append(folder_id)
        if rating_min is not None:
            conditions.append("p.rating >= ?")
            params.append(rating_min)
        if date_from is not None:
            conditions.append("p.timestamp >= ?")
            params.append(date_from)
        if date_to is not None:
            conditions.append("p.timestamp <= ?")
            params.append(date_to)

        join_clause = "JOIN workspace_folders wf ON wf.folder_id = p.folder_id"
        if keyword is not None:
            join_clause += """
                LEFT JOIN photo_keywords pk ON pk.photo_id = p.id
                LEFT JOIN keywords k ON k.id = pk.keyword_id
            """
            conditions.append("(k.name LIKE ? OR p.filename LIKE ?)")
            params.append(f"%{keyword}%")
            params.append(f"%{keyword}%")

        where = "WHERE " + " AND ".join(conditions)

        sort_map = {
            "date": "p.timestamp ASC",
            "date_desc": "p.timestamp DESC",
            "name": "p.filename ASC",
            "name_desc": "p.filename DESC",
            "rating": "p.rating DESC",
            "sharpness": "p.sharpness DESC",
            "sharpness_asc": "p.sharpness ASC",
            "quality": "p.quality_score DESC",
        }
        order = sort_map.get(sort, "p.timestamp ASC")

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
    ):
        """Return count of photos matching the given filters, scoped to active workspace."""
        conditions = ["wf.workspace_id = ?"]
        params = [self._ws_id()]

        if folder_id is not None:
            conditions.append("p.folder_id = ?")
            params.append(folder_id)
        if rating_min is not None:
            conditions.append("p.rating >= ?")
            params.append(rating_min)
        if date_from is not None:
            conditions.append("p.timestamp >= ?")
            params.append(date_from)
        if date_to is not None:
            conditions.append("p.timestamp <= ?")
            params.append(date_to)

        join_clause = "JOIN workspace_folders wf ON wf.folder_id = p.folder_id"
        if keyword is not None:
            join_clause += """
                LEFT JOIN photo_keywords pk ON pk.photo_id = p.id
                LEFT JOIN keywords k ON k.id = pk.keyword_id
            """
            conditions.append("(k.name LIKE ? OR p.filename LIKE ?)")
            params.append(f"%{keyword}%")
            params.append(f"%{keyword}%")

        where = "WHERE " + " AND ".join(conditions)

        query = f"""
            SELECT COUNT(DISTINCT p.id) FROM photos p
            {join_clause}
            {where}
        """
        return self.conn.execute(query, params).fetchone()[0]

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
        returns all matching photos for map rendering. Includes the highest-confidence
        accepted prediction species name (or NULL if none).
        """
        conditions = ["wf.workspace_id = ?",
                      "p.latitude IS NOT NULL",
                      "p.longitude IS NOT NULL"]
        params = [self._ws_id()]

        if folder_id is not None:
            conditions.append("p.folder_id = ?")
            params.append(folder_id)
        if rating_min is not None:
            conditions.append("p.rating >= ?")
            params.append(rating_min)
        if date_from is not None:
            conditions.append("p.timestamp >= ?")
            params.append(date_from)
        if date_to is not None:
            conditions.append("p.timestamp <= ?")
            params.append(date_to)

        join_clause = "JOIN workspace_folders wf ON wf.folder_id = p.folder_id"
        if keyword is not None:
            join_clause += """
                LEFT JOIN photo_keywords pk ON pk.photo_id = p.id
                LEFT JOIN keywords k ON k.id = pk.keyword_id
            """
            conditions.append("(k.name LIKE ? OR p.filename LIKE ?)")
            params.append(f"%{keyword}%")
            params.append(f"%{keyword}%")

        where = "WHERE " + " AND ".join(conditions)

        having_clause = ""
        having_params = []
        if species is not None:
            having_clause = "HAVING species = ?"
            having_params.append(species)

        query = f"""
            SELECT p.id, p.latitude, p.longitude, p.thumb_path, p.filename,
                   p.timestamp, p.rating, p.folder_id,
                   (SELECT pr.species FROM predictions pr
                    WHERE pr.photo_id = p.id
                      AND pr.workspace_id = ?
                      AND pr.status = 'accepted'
                    ORDER BY pr.confidence DESC LIMIT 1) AS species
            FROM photos p
            {join_clause}
            {where}
            GROUP BY p.id
            {having_clause}
            ORDER BY p.timestamp ASC
        """
        params.insert(0, self._ws_id())  # for the subquery
        params.extend(having_params)
        return self.conn.execute(query, params).fetchall()

    def get_accepted_species(self):
        """Return distinct marker species from geolocated photos in the active workspace.

        Uses the same derivation as get_geolocated_photos: the highest-confidence
        accepted prediction per photo.  Only considers photos that have GPS
        coordinates, so every returned species can actually produce a map marker.
        """
        ws = self._ws_id()
        return [
            row[0]
            for row in self.conn.execute(
                """
                SELECT DISTINCT top_species FROM (
                    SELECT (SELECT pr.species FROM predictions pr
                            WHERE pr.photo_id = p.id
                              AND pr.workspace_id = ?
                              AND pr.status = 'accepted'
                            ORDER BY pr.confidence DESC LIMIT 1) AS top_species
                    FROM photos p
                    JOIN workspace_folders wf ON wf.folder_id = p.folder_id
                    WHERE wf.workspace_id = ?
                      AND p.latitude IS NOT NULL
                      AND p.longitude IS NOT NULL
                )
                WHERE top_species IS NOT NULL
                ORDER BY top_species ASC
                """,
                (ws, ws),
            ).fetchall()
        ]

    def count_photos_without_gps(self):
        """Count photos in active workspace that lack GPS coordinates."""
        row = self.conn.execute(
            """
            SELECT COUNT(*) FROM photos p
            JOIN workspace_folders wf ON wf.folder_id = p.folder_id
            WHERE wf.workspace_id = ?
              AND (p.latitude IS NULL OR p.longitude IS NULL)
            """,
            (self._ws_id(),),
        ).fetchone()
        return row[0]

    def update_photo_rating(self, photo_id, rating):
        """Set photo rating (0-5)."""
        self.conn.execute(
            "UPDATE photos SET rating = ? WHERE id = ?", (rating, photo_id)
        )
        self.conn.commit()

    def batch_update_photo_rating(self, photo_ids, rating):
        """Set rating for multiple photos in a single transaction."""
        if not photo_ids:
            return
        placeholders = ",".join("?" for _ in photo_ids)
        self.conn.execute(
            f"UPDATE photos SET rating = ? WHERE id IN ({placeholders})",
            [rating] + list(photo_ids),
        )
        self.conn.commit()

    def update_photo_flag(self, photo_id, flag):
        """Set photo flag ('none', 'flagged', 'rejected')."""
        self.conn.execute("UPDATE photos SET flag = ? WHERE id = ?", (flag, photo_id))
        self.conn.commit()

    def batch_update_photo_flag(self, photo_ids, flag):
        """Set flag for multiple photos in a single transaction."""
        if not photo_ids:
            return
        placeholders = ",".join("?" for _ in photo_ids)
        self.conn.execute(
            f"UPDATE photos SET flag = ? WHERE id IN ({placeholders})",
            [flag] + list(photo_ids),
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

        # Delete associated data (non-cascading FKs)
        self.conn.execute(f"DELETE FROM photo_keywords WHERE photo_id IN ({ph})", all_ids)
        self.conn.execute(f"DELETE FROM pending_changes WHERE photo_id IN ({ph})", all_ids)
        self.conn.execute(f"DELETE FROM predictions WHERE photo_id IN ({ph})", all_ids)

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
        return {"deleted": len(all_ids), "files": files}

    def update_photo_sharpness(self, photo_id, sharpness):
        """Set photo sharpness score."""
        self.conn.execute(
            "UPDATE photos SET sharpness = ? WHERE id = ?", (sharpness, photo_id)
        )
        self.conn.commit()

    def update_photo_quality(
        self,
        photo_id,
        detection_box=None,
        detection_conf=None,
        subject_sharpness=None,
        subject_size=None,
        quality_score=None,
        sharpness=None,
    ):
        """Update all quality-related scores for a photo."""
        import json as _json

        self.conn.execute(
            """UPDATE photos SET detection_box=?, detection_conf=?,
               subject_sharpness=?, subject_size=?, quality_score=?, sharpness=?
               WHERE id=?""",
            (
                _json.dumps(detection_box) if detection_box else None,
                detection_conf,
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
        """Get photos that don't have masks yet, optionally filtered by folders.

        Args:
            folder_ids: optional list of folder IDs to filter by.
                        If None, returns all workspace photos without masks.
        Returns:
            list of photo rows (id, folder_id, filename, detection_box, detection_conf)
        """
        if folder_ids:
            placeholders = ",".join("?" * len(folder_ids))
            return self.conn.execute(
                f"""SELECT id, folder_id, filename, detection_box, detection_conf
                    FROM photos
                    WHERE folder_id IN ({placeholders})
                      AND mask_path IS NULL
                      AND detection_box IS NOT NULL""",
                folder_ids,
            ).fetchall()
        # All workspace photos
        return self.conn.execute(
            """SELECT p.id, p.folder_id, p.filename, p.detection_box, p.detection_conf
               FROM photos p
               JOIN workspace_folders wf ON wf.folder_id = p.folder_id
               WHERE wf.workspace_id = ?
                 AND p.mask_path IS NULL
                 AND p.detection_box IS NOT NULL""",
            (self._ws_id(),),
        ).fetchall()

    def update_photo_embeddings(
        self, photo_id, dino_subject_embedding=None, dino_global_embedding=None
    ):
        """Store DINOv2 embedding BLOBs for a photo.

        Args:
            photo_id: photo ID
            dino_subject_embedding: bytes (float32 numpy array .tobytes())
            dino_global_embedding: bytes (float32 numpy array .tobytes())
        """
        self.conn.execute(
            "UPDATE photos SET dino_subject_embedding=?, dino_global_embedding=? WHERE id=?",
            (dino_subject_embedding, dino_global_embedding, photo_id),
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

    def add_keyword(self, name, parent_id=None, is_species=False):
        """Insert a keyword. Returns existing id if duplicate (case-insensitive).

        If a keyword with the same name but different casing exists, reuses
        the existing one rather than creating a duplicate.

        For new species keywords, auto-detects the user's casing convention
        from existing keywords and applies it (unless overridden by config).
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

    def tag_photo(self, photo_id, keyword_id):
        """Associate a keyword with a photo."""
        self.conn.execute(
            "INSERT OR IGNORE INTO photo_keywords (photo_id, keyword_id) VALUES (?, ?)",
            (photo_id, keyword_id),
        )
        self.conn.commit()

    def untag_photo(self, photo_id, keyword_id):
        """Remove a keyword association from a photo."""
        self.conn.execute(
            "DELETE FROM photo_keywords WHERE photo_id = ? AND keyword_id = ?",
            (photo_id, keyword_id),
        )
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
        """Return keywords used in the active workspace with photo counts, type, and taxon info."""
        ws = self._ws_id()
        return self.conn.execute(
            """SELECT k.id, k.name, k.parent_id, k.type, k.taxon_id,
                      k.latitude, k.longitude,
                      t.name AS taxon_name, t.common_name AS taxon_common_name,
                      COUNT(pk.photo_id) AS photo_count
               FROM keywords k
               JOIN photo_keywords pk ON pk.keyword_id = k.id
               JOIN photos p ON p.id = pk.photo_id
               JOIN workspace_folders wf ON wf.folder_id = p.folder_id
               LEFT JOIN taxa t ON t.id = k.taxon_id
               WHERE wf.workspace_id = ?
               GROUP BY k.id
               ORDER BY k.name""",
            (ws,),
        ).fetchall()

    # -- Predictions --

    def add_prediction(
        self,
        photo_id,
        species,
        confidence,
        model,
        category="new",
        group_id=None,
        vote_count=None,
        total_votes=None,
        individual=None,
        taxonomy=None,
    ):
        """Store a classification prediction for a photo.

        Uses INSERT OR IGNORE so re-running classification doesn't destroy
        existing predictions that the user may have already reviewed.
        Use clear_predictions() first if you want a fresh start.

        Args:
            taxonomy: optional dict with keys kingdom, phylum, class, order,
                      family, genus, scientific_name from taxonomy lookup
        """
        tax = taxonomy or {}
        self.conn.execute(
            """INSERT OR IGNORE INTO predictions
               (photo_id, species, confidence, model, category, status,
                group_id, vote_count, total_votes, individual,
                taxonomy_kingdom, taxonomy_phylum, taxonomy_class,
                taxonomy_order, taxonomy_family, taxonomy_genus, scientific_name,
                workspace_id)
               VALUES (?, ?, ?, ?, ?, 'pending', ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            (
                photo_id,
                species,
                confidence,
                model,
                category,
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
                self._ws_id(),
            ),
        )
        self.conn.commit()

    def clear_predictions(self, model=None, collection_photo_ids=None):
        """Clear predictions, optionally filtered by model and/or photo set."""
        conditions = ["workspace_id = ?"]
        params = [self._ws_id()]
        if model:
            conditions.append("model = ?")
            params.append(model)
        if collection_photo_ids is not None:
            placeholders = ",".join("?" for _ in collection_photo_ids)
            conditions.append(f"photo_id IN ({placeholders})")
            params.extend(collection_photo_ids)
        where = "WHERE " + " AND ".join(conditions)
        self.conn.execute(f"DELETE FROM predictions {where}", params)
        self.conn.commit()

    def get_predictions(self, photo_ids=None, model=None, status=None):
        """Get predictions with photo filename, optionally filtered."""
        conditions = ["pr.workspace_id = ?"]
        params = [self._ws_id()]
        if photo_ids is not None:
            placeholders = ",".join("?" for _ in photo_ids)
            conditions.append(f"pr.photo_id IN ({placeholders})")
            params.extend(photo_ids)
        if model:
            conditions.append("pr.model = ?")
            params.append(model)
        if status:
            conditions.append("pr.status = ?")
            params.append(status)
        where = "WHERE " + " AND ".join(conditions)
        return self.conn.execute(
            f"""SELECT pr.*, p.filename, p.timestamp FROM predictions pr
                JOIN photos p ON p.id = pr.photo_id
                {where} ORDER BY pr.confidence DESC""",
            params,
        ).fetchall()

    def update_prediction_status(self, prediction_id, status):
        """Update prediction status ('pending', 'accepted', 'rejected')."""
        self.conn.execute(
            "UPDATE predictions SET status = ? WHERE id = ?", (status, prediction_id)
        )
        self.conn.commit()

    def get_group_predictions(self, group_id):
        """Get all predictions and photo data for a burst group, ordered by quality."""
        return self.conn.execute(
            """SELECT pr.*, p.filename, p.timestamp, p.sharpness,
                      p.quality_score, p.subject_sharpness, p.subject_size,
                      p.detection_conf, p.rating, p.flag
               FROM predictions pr
               JOIN photos p ON p.id = pr.photo_id
               WHERE pr.group_id = ? AND pr.workspace_id = ?
               ORDER BY p.quality_score DESC""",
            (group_id, self._ws_id()),
        ).fetchall()

    def update_predictions_status_by_photo(self, photo_id, status):
        """Update status for all predictions of a photo in the active workspace."""
        self.conn.execute(
            "UPDATE predictions SET status = ? WHERE photo_id = ? AND workspace_id = ?",
            (status, photo_id, self._ws_id()),
        )
        self.conn.commit()

    def ungroup_prediction(self, prediction_id):
        """Remove a prediction from its group."""
        self.conn.execute(
            "UPDATE predictions SET group_id = NULL WHERE id = ? AND workspace_id = ?",
            (prediction_id, self._ws_id()),
        )
        self.conn.commit()

    def get_existing_prediction_photo_ids(self, model):
        """Return set of photo_ids that have predictions for a model."""
        rows = self.conn.execute(
            "SELECT DISTINCT photo_id FROM predictions WHERE model = ? AND workspace_id = ?",
            (model, self._ws_id()),
        ).fetchall()
        return {r["photo_id"] for r in rows}

    def get_prediction_for_photo(self, photo_id, model):
        """Return species and confidence for a photo's prediction by model, or None."""
        return self.conn.execute(
            "SELECT species, confidence FROM predictions WHERE photo_id = ? AND model = ? AND workspace_id = ?",
            (photo_id, model, self._ws_id()),
        ).fetchone()

    def get_photo_embedding(self, photo_id):
        """Return the embedding blob for a photo, or None."""
        row = self.conn.execute(
            "SELECT embedding FROM photos WHERE id = ?", (photo_id,),
        ).fetchone()
        return row["embedding"] if row else None

    def store_photo_embedding(self, photo_id, embedding_bytes, model=None):
        """Store an embedding blob for a photo, optionally with model name."""
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

    def update_prediction_group_info(self, photo_id, model, group_id, vote_count, total_votes, individual):
        """Update group info on an existing prediction."""
        self.conn.execute(
            """UPDATE predictions
               SET group_id=?, vote_count=?, total_votes=?, individual=?
               WHERE photo_id=? AND model=? AND workspace_id=?""",
            (group_id, vote_count, total_votes, individual, photo_id, model, self._ws_id()),
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
        """
        pred = self.conn.execute(
            "SELECT * FROM predictions WHERE id = ?", (prediction_id,)
        ).fetchone()
        if not pred:
            return None

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

        kid = self.add_keyword(species, is_species=True)
        affected = []  # list of {"photo_id": int, "prediction_id": int}

        # If grouped, accept all predictions in the group
        if pred["group_id"]:
            group_preds = self.conn.execute(
                "SELECT * FROM predictions WHERE group_id = ? AND model = ? AND workspace_id = ?",
                (pred["group_id"], pred["model"], self._ws_id()),
            ).fetchall()
            for gp in group_preds:
                self.update_prediction_status(gp["id"], "accepted")
                self.tag_photo(gp["photo_id"], kid)
                self.queue_change(gp["photo_id"], "keyword_add", species)
                affected.append({"photo_id": gp["photo_id"], "prediction_id": gp["id"]})
        else:
            self.update_prediction_status(prediction_id, "accepted")
            self.tag_photo(pred["photo_id"], kid)
            self.queue_change(pred["photo_id"], "keyword_add", species)
            affected.append({"photo_id": pred["photo_id"], "prediction_id": prediction_id})

        return {"species": species, "keyword_id": kid, "affected": affected}

    # -- Pending Changes --

    def queue_change(self, photo_id, change_type, value, workspace_id=None):
        """Add a change to the sync queue (skips if already queued).

        Returns the inserted pending change token, or None if an identical row already exists.
        If workspace_id is not provided, uses the active workspace.
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
        self.conn.commit()
        return change_token

    def get_pending_changes(self):
        """Return all pending changes ordered by creation time."""
        return self.conn.execute(
            "SELECT * FROM pending_changes WHERE workspace_id = ? ORDER BY created_at",
            (self._ws_id(),),
        ).fetchall()

    def remove_pending_changes(self, photo_id, change_type=None, value=None, workspace_id=None):
        """Delete matching pending changes. Returns rows removed."""
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

    def record_edit(self, action_type, description, new_value, items, is_batch=False):
        """Record an edit action with per-photo before/after values."""
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
        self.conn.commit()
        self._prune_edit_history()
        return edit_id

    def get_edit_history(self, limit=50, offset=0):
        """Return recent edit history entries (most recent first) with item counts."""
        rows = self.conn.execute(
            """SELECT eh.*, COUNT(ehi.id) as item_count
               FROM edit_history eh
               LEFT JOIN edit_history_items ehi ON ehi.edit_id = eh.id
               WHERE eh.workspace_id = ?
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
        """
        placeholders = ",".join("?" for _ in self._NON_UNDOABLE)
        entry = self.conn.execute(
            f"SELECT * FROM edit_history WHERE workspace_id = ? AND action_type NOT IN ({placeholders}) "
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

        for item in items:
            old_val = item['old_value']
            pid = item['photo_id']
            if entry['action_type'] == 'rating':
                self.update_photo_rating(pid, int(old_val))
                if old_val != entry['new_value']:
                    self.remove_pending_changes(pid, 'rating', entry['new_value'])
                    self.queue_change(pid, 'rating', old_val)
            elif entry['action_type'] == 'flag':
                self.update_photo_flag(pid, old_val)
            elif entry['action_type'] in ('keyword_add', 'prediction_accept'):
                self.untag_photo(pid, int(entry['new_value']))
                kw = self.conn.execute("SELECT name FROM keywords WHERE id = ?",
                                       (int(entry['new_value']),)).fetchone()
                if kw:
                    self.remove_pending_changes(pid, 'keyword_add', kw['name'])
                # Restore prediction status if this was a prediction accept
                if entry['action_type'] == 'prediction_accept' and old_val:
                    self.update_prediction_status(int(old_val), 'pending')
            elif entry['action_type'] == 'keyword_remove':
                self.tag_photo(pid, int(entry['new_value']))
                kw = self.conn.execute("SELECT name FROM keywords WHERE id = ?",
                                       (int(entry['new_value']),)).fetchone()
                if kw:
                    self.remove_pending_changes(pid, 'keyword_remove', kw['name'])

        self.conn.execute("DELETE FROM edit_history WHERE id = ?", (entry['id'],))
        self.conn.commit()
        return entry

    def _prune_edit_history(self):
        """Delete oldest entries beyond the configured max."""
        import config as cfg
        max_entries = cfg.get('max_edit_history') or 1000
        self.conn.execute(
            """DELETE FROM edit_history WHERE workspace_id = ? AND id NOT IN (
                 SELECT id FROM edit_history WHERE workspace_id = ?
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

    def _build_collection_query(self, collection_id):
        """Build SQL clauses from collection rules.

        Returns (folder_join, join_clause, where, params) or None if collection not found.
        """
        row = self.conn.execute(
            "SELECT rules FROM collections WHERE id = ?", (collection_id,)
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

            if field == "photo_ids":
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
                    params.extend(value)
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
                " JOIN predictions pred ON pred.photo_id = p.id"
                " AND pred.workspace_id = ?"
            )
            # Insert workspace param before the existing condition params
            params.insert(0, self._ws_id())

        # Always join folders for folder-under rules, scoped to workspace
        folder_join = " JOIN folders f ON f.id = p.folder_id"
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
        offset = (page - 1) * per_page
        params.extend([per_page, offset])

        pcols = ", ".join(f"p.{c.strip()}" for c in self.PHOTO_COLS.split(","))
        query = f"""
            SELECT DISTINCT {pcols} FROM photos p
            {folder_join}
            {join_clause}
            {where}
            ORDER BY p.timestamp ASC
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
