"""SQLite database for Spotter photo browser metadata cache."""

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
        self.conn.executescript("""
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
                UNIQUE(folder_id, filename)
            );

            CREATE TABLE IF NOT EXISTS keywords (
                id          INTEGER PRIMARY KEY,
                name        TEXT,
                parent_id   INTEGER REFERENCES keywords(id),
                UNIQUE(name, parent_id)
            );

            CREATE TABLE IF NOT EXISTS photo_keywords (
                photo_id    INTEGER REFERENCES photos(id),
                keyword_id  INTEGER REFERENCES keywords(id),
                PRIMARY KEY (photo_id, keyword_id)
            );

            CREATE TABLE IF NOT EXISTS collections (
                id          INTEGER PRIMARY KEY,
                name        TEXT,
                rules       TEXT
            );

            CREATE TABLE IF NOT EXISTS pending_changes (
                id          INTEGER PRIMARY KEY,
                photo_id    INTEGER REFERENCES photos(id),
                change_type TEXT,
                value       TEXT,
                created_at  TEXT DEFAULT (datetime('now'))
            );

            CREATE INDEX IF NOT EXISTS idx_photos_timestamp ON photos(timestamp);
            CREATE INDEX IF NOT EXISTS idx_photos_folder ON photos(folder_id);
            CREATE INDEX IF NOT EXISTS idx_photos_rating ON photos(rating);
            CREATE INDEX IF NOT EXISTS idx_keywords_name ON keywords(name);
        """)

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
        row = self.conn.execute("SELECT id FROM folders WHERE path = ?", (path,)).fetchone()
        return row['id']

    def get_folder_tree(self):
        """Return all folders as a list of Row objects."""
        return self.conn.execute(
            "SELECT id, path, name, parent_id, photo_count FROM folders ORDER BY path"
        ).fetchall()

    # -- Photos --

    def add_photo(self, folder_id, filename, extension, file_size, file_mtime,
                  timestamp=None, width=None, height=None, xmp_mtime=None):
        """Insert a photo. Returns the photo id."""
        cur = self.conn.execute(
            """INSERT OR IGNORE INTO photos
               (folder_id, filename, extension, file_size, file_mtime, xmp_mtime,
                timestamp, width, height)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            (folder_id, filename, extension, file_size, file_mtime, xmp_mtime,
             timestamp, width, height),
        )
        self.conn.commit()
        if cur.lastrowid:
            return cur.lastrowid
        row = self.conn.execute(
            "SELECT id FROM photos WHERE folder_id = ? AND filename = ?",
            (folder_id, filename),
        ).fetchone()
        return row['id']

    def get_photo(self, photo_id):
        """Return a single photo by id."""
        return self.conn.execute(
            "SELECT * FROM photos WHERE id = ?", (photo_id,)
        ).fetchone()

    def get_photos(self, folder_id=None, page=1, per_page=50, sort='date',
                   rating_min=None, date_from=None, date_to=None, keyword=None):
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
                JOIN photo_keywords pk ON pk.photo_id = p.id
                JOIN keywords k ON k.id = pk.keyword_id
            """
            conditions.append("k.name = ?")
            params.append(keyword)

        where = ""
        if conditions:
            where = "WHERE " + " AND ".join(conditions)

        sort_map = {
            'date': 'p.timestamp ASC',
            'date_desc': 'p.timestamp DESC',
            'name': 'p.filename ASC',
            'name_desc': 'p.filename DESC',
            'rating': 'p.rating DESC',
        }
        order = sort_map.get(sort, 'p.timestamp ASC')

        offset = (page - 1) * per_page
        params.extend([per_page, offset])

        query = f"""
            SELECT p.* FROM photos p
            {join_clause}
            {where}
            ORDER BY {order}
            LIMIT ? OFFSET ?
        """
        return self.conn.execute(query, params).fetchall()

    def update_photo_rating(self, photo_id, rating):
        """Set photo rating (0-5)."""
        self.conn.execute("UPDATE photos SET rating = ? WHERE id = ?", (rating, photo_id))
        self.conn.commit()

    def update_photo_flag(self, photo_id, flag):
        """Set photo flag ('none', 'flagged', 'rejected')."""
        self.conn.execute("UPDATE photos SET flag = ? WHERE id = ?", (flag, photo_id))
        self.conn.commit()

    # -- Keywords --

    def add_keyword(self, name, parent_id=None):
        """Insert a keyword. Returns existing id if duplicate. Returns the keyword id."""
        # Check for null parent_id separately since UNIQUE(name, parent_id) treats NULLs as distinct
        if parent_id is None:
            existing = self.conn.execute(
                "SELECT id FROM keywords WHERE name = ? AND parent_id IS NULL", (name,)
            ).fetchone()
        else:
            existing = self.conn.execute(
                "SELECT id FROM keywords WHERE name = ? AND parent_id = ?", (name, parent_id)
            ).fetchone()
        if existing:
            return existing['id']

        cur = self.conn.execute(
            "INSERT INTO keywords (name, parent_id) VALUES (?, ?)",
            (name, parent_id),
        )
        self.conn.commit()
        return cur.lastrowid

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

    # -- Pending Changes --

    def queue_change(self, photo_id, change_type, value):
        """Add a change to the sync queue."""
        self.conn.execute(
            "INSERT INTO pending_changes (photo_id, change_type, value) VALUES (?, ?, ?)",
            (photo_id, change_type, value),
        )
        self.conn.commit()

    def get_pending_changes(self):
        """Return all pending changes ordered by creation time."""
        return self.conn.execute(
            "SELECT * FROM pending_changes ORDER BY created_at"
        ).fetchall()

    def clear_pending(self, change_ids):
        """Delete pending changes by id."""
        if not change_ids:
            return
        placeholders = ','.join('?' for _ in change_ids)
        self.conn.execute(
            f"DELETE FROM pending_changes WHERE id IN ({placeholders})",
            change_ids,
        )
        self.conn.commit()

    # -- Collections --

    def add_collection(self, name, rules_json):
        """Insert a smart collection. Returns the collection id."""
        cur = self.conn.execute(
            "INSERT INTO collections (name, rules) VALUES (?, ?)",
            (name, rules_json),
        )
        self.conn.commit()
        return cur.lastrowid

    def get_collections(self):
        """Return all collections."""
        return self.conn.execute(
            "SELECT id, name, rules FROM collections ORDER BY name"
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

        rules = json.loads(row['rules'])
        conditions = []
        params = []
        need_keyword_join = False

        for rule in rules:
            field = rule['field']
            op = rule['op']
            value = rule['value']

            if field == 'rating':
                if op == '>=':
                    conditions.append("p.rating >= ?")
                    params.append(value)
                elif op == '<=':
                    conditions.append("p.rating <= ?")
                    params.append(value)
                elif op == 'equals':
                    conditions.append("p.rating = ?")
                    params.append(value)
            elif field == 'keyword':
                need_keyword_join = True
                if op == 'contains':
                    conditions.append("k.name LIKE ?")
                    params.append(f"%{value}%")
                elif op == 'equals':
                    conditions.append("k.name = ?")
                    params.append(value)
            elif field == 'folder':
                if op == 'under':
                    conditions.append("f.path LIKE ?")
                    params.append(f"{value}%")
            elif field == 'flag':
                if op == 'equals':
                    conditions.append("p.flag = ?")
                    params.append(value)
            elif field == 'keyword_count':
                if op == 'equals':
                    conditions.append("""(SELECT COUNT(*) FROM photo_keywords pk2
                                         WHERE pk2.photo_id = p.id) = ?""")
                    params.append(value)
                elif op == '>=':
                    conditions.append("""(SELECT COUNT(*) FROM photo_keywords pk2
                                         WHERE pk2.photo_id = p.id) >= ?""")
                    params.append(value)
            elif field == 'timestamp':
                if op == 'between' and isinstance(value, list) and len(value) == 2:
                    conditions.append("p.timestamp >= ? AND p.timestamp <= ?")
                    params.extend(value)
                elif op == 'recent_days':
                    conditions.append("p.timestamp >= datetime('now', ?)")
                    params.append(f"-{value} days")
            elif field == 'extension':
                if op == 'equals':
                    conditions.append("p.extension = ?")
                    params.append(value)

        join_clause = ""
        if need_keyword_join:
            join_clause += " JOIN photo_keywords pk ON pk.photo_id = p.id"
            join_clause += " JOIN keywords k ON k.id = pk.keyword_id"

        # Always join folders for folder-under rules
        folder_join = " LEFT JOIN folders f ON f.id = p.folder_id"

        where = ""
        if conditions:
            where = "WHERE " + " AND ".join(conditions)

        offset = (page - 1) * per_page
        params.extend([per_page, offset])

        query = f"""
            SELECT DISTINCT p.* FROM photos p
            {folder_join}
            {join_clause}
            {where}
            ORDER BY p.timestamp ASC
            LIMIT ? OFFSET ?
        """
        return self.conn.execute(query, params).fetchall()

    def update_folder_counts(self):
        """Recalculate photo_count for all folders."""
        self.conn.execute("""
            UPDATE folders SET photo_count = (
                SELECT COUNT(*) FROM photos WHERE photos.folder_id = folders.id
            )
        """)
        self.conn.commit()

    def create_default_collections(self):
        """Create default smart collections if none exist."""
        existing = self.get_collections()
        if existing:
            return

        defaults = [
            ('Untagged', [{"field": "keyword_count", "op": "equals", "value": 0}]),
            ('Flagged', [{"field": "flag", "op": "equals", "value": "flagged"}]),
            ('Recent Import', [{"field": "timestamp", "op": "recent_days", "value": 30}]),
        ]
        for name, rules in defaults:
            self.add_collection(name, json.dumps(rules))
