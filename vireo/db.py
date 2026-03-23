"""SQLite database for Vireo photo browser metadata cache."""

import json
import os
import sqlite3


class Database:
    """Local SQLite database that caches photo metadata from XMP sidecars.

    Args:
        db_path: path to the SQLite database file (created if missing)
    """

    def __init__(self, db_path):
        os.makedirs(os.path.dirname(db_path), exist_ok=True)
        self.conn = sqlite3.connect(db_path, check_same_thread=False)
        self.conn.row_factory = sqlite3.Row
        self.conn.execute("PRAGMA journal_mode=WAL")
        self.conn.execute("PRAGMA foreign_keys=ON")
        self._create_tables()

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

            CREATE INDEX IF NOT EXISTS idx_photos_timestamp ON photos(timestamp);
            CREATE INDEX IF NOT EXISTS idx_photos_folder ON photos(folder_id);
            CREATE INDEX IF NOT EXISTS idx_photos_rating ON photos(rating);
            CREATE INDEX IF NOT EXISTS idx_keywords_name ON keywords(name);
            CREATE INDEX IF NOT EXISTS idx_predictions_workspace ON predictions(workspace_id);
            CREATE INDEX IF NOT EXISTS idx_collections_workspace ON collections(workspace_id);
            CREATE INDEX IF NOT EXISTS idx_pending_workspace ON pending_changes(workspace_id);
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

        # Workspace migration for existing databases
        try:
            self.conn.execute("SELECT id FROM workspaces LIMIT 0")
        except Exception:
            # Create workspace tables
            self.conn.execute(
                """CREATE TABLE IF NOT EXISTS workspaces (
                    id              INTEGER PRIMARY KEY,
                    name            TEXT NOT NULL UNIQUE,
                    config_overrides TEXT,
                    ui_state        TEXT,
                    created_at      TEXT DEFAULT (datetime('now')),
                    last_opened_at  TEXT
                )"""
            )
            self.conn.execute(
                """CREATE TABLE IF NOT EXISTS workspace_folders (
                    workspace_id    INTEGER REFERENCES workspaces(id) ON DELETE CASCADE,
                    folder_id       INTEGER REFERENCES folders(id),
                    PRIMARY KEY (workspace_id, folder_id)
                )"""
            )

            # Create default workspace
            self.conn.execute(
                "INSERT INTO workspaces (name) VALUES (?)", ("Default",)
            )
            default_id = self.conn.execute(
                "SELECT id FROM workspaces WHERE name = 'Default'"
            ).fetchone()[0]

            # Link all existing folders to default workspace
            self.conn.execute(
                "INSERT INTO workspace_folders (workspace_id, folder_id) "
                "SELECT ?, id FROM folders", (default_id,)
            )

            # Add workspace_id to scoped tables and backfill
            for table in ("predictions", "collections", "pending_changes"):
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

            # Recreate predictions unique index for new constraint
            # (photo_id, model) -> (photo_id, model, workspace_id)
            self.conn.execute("DROP INDEX IF EXISTS sqlite_autoindex_predictions_1")
            self.conn.execute(
                "CREATE UNIQUE INDEX IF NOT EXISTS idx_predictions_unique "
                "ON predictions(photo_id, model, workspace_id)"
            )

            self.conn.commit()

        # Ensure workspace indexes exist (for fresh DBs that skip migration)
        self.conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_predictions_workspace "
            "ON predictions(workspace_id)"
        )
        self.conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_collections_workspace "
            "ON collections(workspace_id)"
        )
        self.conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_pending_workspace "
            "ON pending_changes(workspace_id)"
        )

    # -- Workspaces --

    _active_workspace_id = None

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

    def update_workspace(self, workspace_id, name=None, config_overrides=None,
                         ui_state=None, last_opened_at=None):
        """Update workspace fields. Only non-None args are updated."""
        updates = []
        params = []
        if name is not None:
            updates.append("name = ?")
            params.append(name)
        if config_overrides is not None:
            updates.append("config_overrides = ?")
            params.append(json.dumps(config_overrides))
        if ui_state is not None:
            updates.append("ui_state = ?")
            params.append(json.dumps(ui_state))
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
        """Insert a folder. Returns the folder id."""
        cur = self.conn.execute(
            "INSERT OR IGNORE INTO folders (path, name, parent_id) VALUES (?, ?, ?)",
            (path, name, parent_id),
        )
        self.conn.commit()
        if cur.lastrowid:
            return cur.lastrowid
        row = self.conn.execute(
            "SELECT id FROM folders WHERE path = ?", (path,)
        ).fetchone()
        return row["id"]

    def get_folder_tree(self):
        """Return all folders as a list of Row objects."""
        return self.conn.execute(
            "SELECT id, path, name, parent_id, photo_count FROM folders ORDER BY path"
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

    # Columns to return in photo queries (excludes large binary fields like embedding)
    PHOTO_COLS = """id, folder_id, filename, extension, file_size, file_mtime, xmp_mtime,
                    timestamp, width, height, rating, flag, thumb_path, sharpness,
                    detection_box, detection_conf, subject_sharpness, subject_size, quality_score,
                    latitude, longitude"""

    def get_photo(self, photo_id):
        """Return a single photo by id."""
        return self.conn.execute(
            f"SELECT {self.PHOTO_COLS} FROM photos WHERE id = ?", (photo_id,)
        ).fetchone()

    def count_photos(self):
        """Return total photo count."""
        return self.conn.execute("SELECT COUNT(*) FROM photos").fetchone()[0]

    def count_folders(self):
        """Return total folder count."""
        return self.conn.execute("SELECT COUNT(*) FROM folders").fetchone()[0]

    def count_keywords(self):
        """Return total keyword count."""
        return self.conn.execute("SELECT COUNT(*) FROM keywords").fetchone()[0]

    def count_pending_changes(self):
        """Return pending changes count."""
        return self.conn.execute(
            "SELECT COUNT(*) FROM pending_changes WHERE workspace_id = ?",
            (self._ws_id(),),
        ).fetchone()[0]

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
        """Return paginated, filtered photo list."""
        conditions = []
        params = []

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

        join_clause = ""
        if keyword is not None:
            join_clause = """
                LEFT JOIN photo_keywords pk ON pk.photo_id = p.id
                LEFT JOIN keywords k ON k.id = pk.keyword_id
            """
            conditions.append("(k.name LIKE ? OR p.filename LIKE ?)")
            params.append(f"%{keyword}%")
            params.append(f"%{keyword}%")

        where = ""
        if conditions:
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
        query = f"""
            SELECT {pcols} FROM photos p
            {join_clause}
            {where}
            ORDER BY {order}
            LIMIT ? OFFSET ?
        """
        return self.conn.execute(query, params).fetchall()

    def update_photo_rating(self, photo_id, rating):
        """Set photo rating (0-5)."""
        self.conn.execute(
            "UPDATE photos SET rating = ? WHERE id = ?", (rating, photo_id)
        )
        self.conn.commit()

    def update_photo_flag(self, photo_id, flag):
        """Set photo flag ('none', 'flagged', 'rejected')."""
        self.conn.execute("UPDATE photos SET flag = ? WHERE id = ?", (flag, photo_id))
        self.conn.commit()

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
            # Update is_species if it wasn't set before
            if is_species:
                self.conn.execute(
                    "UPDATE keywords SET is_species = 1 WHERE id = ? AND is_species = 0",
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

        cur = self.conn.execute(
            "INSERT INTO keywords (name, parent_id, is_species) VALUES (?, ?, ?)",
            (name, parent_id, 1 if is_species else 0),
        )
        self.conn.commit()
        return cur.lastrowid

    def merge_duplicate_keywords(self):
        """Find and merge case-insensitive duplicate keywords.

        Keeps the lowest ID (earliest created), moves all photo associations,
        and deletes the duplicates. Returns count of merges performed.
        """
        dupes = self.conn.execute(
            """SELECT LOWER(name) as lname, MIN(id) as keep_id, GROUP_CONCAT(id) as all_ids
               FROM keywords GROUP BY LOWER(name) HAVING COUNT(*) > 1"""
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
        """Return all keywords as a list of Row objects."""
        return self.conn.execute(
            "SELECT id, name, parent_id FROM keywords ORDER BY name"
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
            """SELECT k.id, k.name, k.parent_id
               FROM keywords k
               JOIN photo_keywords pk ON pk.keyword_id = k.id
               WHERE pk.photo_id = ?
               ORDER BY k.name""",
            (photo_id,),
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

    def accept_prediction(self, prediction_id):
        """Accept a prediction: mark as accepted and add species keyword.

        If the prediction belongs to a group, derives the consensus species
        from the individual votes and applies that to all photos.
        """
        pred = self.conn.execute(
            "SELECT * FROM predictions WHERE id = ?", (prediction_id,)
        ).fetchone()
        if not pred:
            return

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
        else:
            self.update_prediction_status(prediction_id, "accepted")
            self.tag_photo(pred["photo_id"], kid)
            self.queue_change(pred["photo_id"], "keyword_add", species)

    # -- Pending Changes --

    def queue_change(self, photo_id, change_type, value):
        """Add a change to the sync queue (skips if already queued)."""
        existing = self.conn.execute(
            "SELECT id FROM pending_changes WHERE photo_id = ? AND change_type = ? AND value = ? AND workspace_id = ?",
            (photo_id, change_type, value, self._ws_id()),
        ).fetchone()
        if existing:
            return
        self.conn.execute(
            "INSERT INTO pending_changes (photo_id, change_type, value, workspace_id) VALUES (?, ?, ?, ?)",
            (photo_id, change_type, value, self._ws_id()),
        )
        self.conn.commit()

    def get_pending_changes(self):
        """Return all pending changes ordered by creation time."""
        return self.conn.execute(
            "SELECT * FROM pending_changes WHERE workspace_id = ? ORDER BY created_at",
            (self._ws_id(),),
        ).fetchall()

    def clear_pending(self, change_ids):
        """Delete pending changes by id."""
        if not change_ids:
            return
        placeholders = ",".join("?" for _ in change_ids)
        self.conn.execute(
            f"DELETE FROM pending_changes WHERE id IN ({placeholders})",
            change_ids,
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
        self.conn.execute("DELETE FROM collections WHERE id = ?", (collection_id,))
        self.conn.commit()

    def get_collection_photos(self, collection_id, page=1, per_page=50):
        """Build SQL from collection rules and return matching photos."""
        row = self.conn.execute(
            "SELECT rules FROM collections WHERE id = ?", (collection_id,)
        ).fetchone()
        if not row:
            return []

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

        # Always join folders for folder-under rules
        folder_join = " LEFT JOIN folders f ON f.id = p.folder_id"

        where = ""
        if conditions:
            where = "WHERE " + " AND ".join(conditions)

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

        Tries local taxonomy first, then falls back to the iNaturalist API
        for alternate/regional names (e.g. "Grey Plover" -> "Black-bellied Plover").

        Args:
            taxonomy: a Taxonomy instance with is_taxon() and api_lookup() methods
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
            elif hasattr(taxonomy, "api_lookup") and taxonomy.api_lookup(kw["name"]):
                self.conn.execute(
                    "UPDATE keywords SET is_species = 1 WHERE id = ?", (kw["id"],)
                )
                updated += 1
        if updated:
            self.conn.commit()
        # Persist any newly discovered alternate names
        if hasattr(taxonomy, "save"):
            taxonomy.save()
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
