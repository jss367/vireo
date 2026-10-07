"""Persistence for core photo rows: insert, lookups, listing and deletion.

Photo rows are global; the workspace only decides which folders are
visible. Methods that read through ``workspace_folders`` use the
``workspace_id`` the façade resolved (``Database._photos_repository()``),
while the catalog-wide ones are built with ``workspace_id=None``.

``Database`` keeps the composition. The listing reads take the façade's
folder-subtree, collection, rules, location-filter and sort helpers as
callbacks, so patches of those ``Database`` methods still apply and the
calls happen at the same point as before. ``delete_photos`` keeps the
companion resolution, the new-images cache invalidation and the pipeline
cache prune on the façade; ``add_photo`` keeps the duplicate auto-resolve
hook. Ratings, flags, wildlife exclusion (``photo_review``) and color labels
(``photo_labels``) have their own repositories. The working-copy and
thumbnail column writes here are the ones the on-demand image routes make.
"""

import os

from repositories.photo_row_deletion import photo_row_deletion
from repositories.top_species import TOP_SPECIES_RANKING_SQL


class PhotoRepository:
    def __init__(
        self,
        conn,
        workspace_id=None,
        *,
        chunk_size=800,
        photo_cols,
        photo_detail_cols,
        execute_with_retry,
        commit_with_retry,
        inclusive_date_to,
        keyword_token_clause,
    ):
        self.conn = conn
        self.workspace_id = workspace_id
        self.chunk_size = chunk_size
        # ``Database.PHOTO_COLS`` / ``PHOTO_DETAIL_COLS``: the column lists
        # stay on the façade, where the collection reads use them too.
        self.photo_cols = photo_cols
        self.photo_detail_cols = photo_detail_cols
        # ``db`` module helpers, passed in so repositories import no ``db``
        # code and monkeypatches of the module functions still apply.
        self.execute_with_retry = execute_with_retry
        self.commit_with_retry = commit_with_retry
        self.inclusive_date_to = inclusive_date_to
        self.keyword_token_clause = keyword_token_clause

    def _chunks(self, values, size=None):
        """Yield ``values`` in lists of at most ``size`` (default chunk_size)."""
        values = list(values)
        if size is None:
            size = self.chunk_size
        for idx in range(0, len(values), size):
            yield values[idx:idx + size]

    # -- rows and lookups ----------------------------------------------------

    def filter_out_wildlife_excluded(self, photo_ids, chunk_size):
        """Return ``photo_ids`` minus the wildlife-excluded ones, in order."""
        if not photo_ids:
            return []
        photo_ids_list = list(photo_ids)
        excluded = set()
        for chunk in self._chunks(photo_ids_list, chunk_size):
            placeholders = ",".join("?" * len(chunk))
            rows = self.conn.execute(
                f"""SELECT id FROM photos
                    WHERE wildlife_excluded = 1
                      AND id IN ({placeholders})""",
                chunk,
            ).fetchall()
            excluded.update(r["id"] for r in rows)
        return [pid for pid in photo_ids_list if pid not in excluded]

    def add(
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
        """Insert a photo (or find the existing row), commit.

        Returns ``(photo_id, inserted)``: ``inserted`` is True only when this
        call actually created the row. On a race where a concurrent writer
        inserts the same (folder, filename) first, the ``INSERT OR IGNORE``
        is a no-op and the SELECT fallback returns the winner's id with
        ``inserted=False``; callers that gate recycled-id or inherited-
        membership cleanup must consult ``inserted`` rather than their own
        pre-check, since the pre-check cannot see a concurrent insert.
        """
        cur = self.execute_with_retry(
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
        self.commit_with_retry(self.conn)
        if cur.rowcount > 0:
            return cur.lastrowid, True
        row = self.conn.execute(
            "SELECT id FROM photos WHERE folder_id = ? AND filename = ?",
            (folder_id, filename),
        ).fetchone()
        return row["id"], False

    def get(self, photo_id, verify_workspace=False):
        """Return one photo row (detail columns), or None.

        With ``verify_workspace`` the row must sit in a folder of
        ``workspace_id``; the repository must then be workspace-scoped.
        """
        if verify_workspace:
            return self.conn.execute(
                f"""SELECT {self.photo_detail_cols} FROM photos
                    WHERE id = ? AND id IN (
                        SELECT photo_id FROM photo_workspace_visibility
                        WHERE workspace_id = ?)""",
                (photo_id, self.workspace_id),
            ).fetchone()
        return self.conn.execute(
            f"SELECT {self.photo_detail_cols} FROM photos WHERE id = ?", (photo_id,)
        ).fetchone()

    def get_filenames(self, photo_ids):
        """Return {photo_id: (folder_id, filename)} for the ids that exist."""
        if not photo_ids:
            return {}
        result = {}
        for chunk in self._chunks(photo_ids):
            placeholders = ",".join("?" for _ in chunk)
            rows = self.conn.execute(
                f"SELECT id, folder_id, filename FROM photos "
                f"WHERE id IN ({placeholders})",
                list(chunk),
            ).fetchall()
            for row in rows:
                result[row["id"]] = (row["folder_id"], row["filename"])
        return result

    def existing_ids(self, photo_ids):
        """Which of ``photo_ids`` have a ``photos`` row, in any workspace."""
        found = set()
        for chunk in self._chunks(photo_ids):
            placeholders = ",".join("?" for _ in chunk)
            found.update(
                r["id"] for r in self.conn.execute(
                    f"SELECT id FROM photos WHERE id IN ({placeholders})",
                    chunk,
                ).fetchall()
            )
        return found

    def get_by_ids(self, photo_ids, *, include_exif=False):
        """Return {photo_id: Row} (list columns, optionally exif_data)."""
        if not photo_ids:
            return {}
        result = {}
        for chunk in self._chunks(photo_ids):
            placeholders = ",".join("?" for _ in chunk)
            rows = self.conn.execute(
                f"SELECT {self.photo_cols}{', exif_data' if include_exif else ''} "
                f"FROM photos WHERE id IN ({placeholders})",
                list(chunk),
            ).fetchall()
            for row in rows:
                result[row["id"]] = row
        return result

    def get_folder_statuses(self, photo_ids):
        """Return ``{photo_id: folder_status}`` for the requested photos."""
        if not photo_ids:
            return {}
        result = {}
        for chunk in self._chunks(list(dict.fromkeys(photo_ids))):
            placeholders = ",".join("?" for _ in chunk)
            rows = self.conn.execute(
                f"""SELECT p.id, f.status
                    FROM photos p
                    JOIN folders f ON f.id = p.folder_id
                    WHERE p.id IN ({placeholders})""",
                list(chunk),
            ).fetchall()
            result.update({row["id"]: row["status"] for row in rows})
        return result

    def get_with_folder_path(self, photo_id):
        """One photo's whole row plus its folder's ``folder_path``, or None.

        ``SELECT p.*``, so the row carries the embedding BLOBs too.
        """
        return self.conn.execute(
            """SELECT p.*, f.path as folder_path FROM photos p
               JOIN folders f ON f.id = p.folder_id WHERE p.id = ?""",
            (photo_id,),
        ).fetchone()

    def file_in_workspace(self, photo_id, workspace_id):
        """``filename`` and ``folder_path`` of one photo ``workspace_id`` can see, or None.

        ``workspace_id`` is explicit, so ``None`` matches no row rather than
        raising.
        """
        return self.conn.execute(
            """SELECT p.filename, f.path AS folder_path
               FROM photos p
               JOIN folders f ON p.folder_id = f.id
               JOIN photo_workspace_visibility wf ON wf.photo_id = p.id
               WHERE p.id = ? AND wf.workspace_id = ?""",
            (photo_id, workspace_id),
        ).fetchone()

    def best_batch_rows_by_ids(self, photo_ids):
        """Best Batch rows for the named photos the workspace can see in online folders.

        Rows carry ``id``, ``folder_id``, ``filename``, ``extension``,
        ``timestamp``, ``flag``, ``rating``, ``quality_score`` and
        ``sharpness``, in no particular order. Photos in folders that are not
        ``'ok'``/``'partial'`` are absent. One statement, unchunked: the
        caller bounds the list (``POST /api/photos/best-batch`` takes at
        most 500 ids).
        """
        placeholders = ",".join("?" for _ in photo_ids)
        return self.conn.execute(
            f"""SELECT p.id, p.folder_id, p.filename, p.extension, p.timestamp,
                      p.flag, p.rating, p.quality_score, p.sharpness
               FROM photos p
               JOIN photo_workspace_visibility wf ON wf.photo_id = p.id
               JOIN folders f ON f.id = p.folder_id AND f.status IN ('ok', 'partial')
               WHERE wf.workspace_id = ? AND p.id IN ({placeholders})""",
            (self.workspace_id, *photo_ids),
        ).fetchall()

    def flags_ratings_and_eyes(self, photo_ids):
        """``{photo_id: Row}`` with ``id``, ``flag``, ``rating`` and the eye fields.

        The eye fields are ``eye_x``, ``eye_y``, ``eye_conf`` and
        ``eye_tenengrad``. Ids with no photo row are absent.
        """
        result = {}
        for chunk in self._chunks(photo_ids):
            placeholders = ",".join("?" for _ in chunk)
            rows = self.conn.execute(
                f"""SELECT id, flag, rating,
                           eye_x, eye_y, eye_conf, eye_tenengrad
                      FROM photos WHERE id IN ({placeholders})""",
                chunk,
            ).fetchall()
            result.update({r["id"]: r for r in rows})
        return result

    def in_workspace_folders(self, folder_ids, workspace_id):
        """Rows (``id``, ``folder_id``, ``filename``) of the workspace's photos in ``folder_ids``.

        Visibility is ``photo_workspace_visibility`` (folder links plus
        photo-only grants). Rows come one ``IN`` chunk at a time, in the
        order of ``folder_ids``' chunks; no ORDER BY within a chunk.
        """
        photos = []
        for chunk in self._chunks(folder_ids):
            marks = ",".join("?" for _ in chunk)
            photos.extend(self.conn.execute(
                f"SELECT p.id, p.folder_id, p.filename FROM photos p "
                f"JOIN photo_workspace_visibility pv ON pv.photo_id = p.id "
                f"WHERE p.folder_id IN ({marks}) AND pv.workspace_id = ?",
                [*chunk, workspace_id],
            ))
        return photos

    def count(self):
        """Return the workspace's photo count, skipping missing folders."""
        return self.conn.execute(
            """SELECT COUNT(*) FROM photos p
               JOIN photo_workspace_visibility wf ON wf.photo_id = p.id
               JOIN folders f ON f.id = p.folder_id AND f.status IN ('ok', 'partial')
               WHERE wf.workspace_id = ?""",
            (self.workspace_id,),
        ).fetchone()[0]

    def count_in_workspace(self):
        """Return the workspace's photo count, including missing folders."""
        return self.conn.execute(
            """SELECT COUNT(*) FROM photos p
               JOIN photo_workspace_visibility wf ON wf.photo_id = p.id
               WHERE wf.workspace_id = ?""",
            (self.workspace_id,),
        ).fetchone()[0]

    def by_paths(self, paths):
        """Return {abs_path: photo_id} for any of ``paths`` already in DB."""
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

    def ids_at_paths(self, paths):
        """Ids of the photos stored at ``paths``, in one join.

        The (directory, filename) pairs go through a temp table rather than
        bound parameters, so any number of paths is one statement. A path
        listed twice yields its id twice. The temp-table insert opens a
        transaction on the connection that this method does not end.
        """
        self.conn.execute(
            "CREATE TEMP TABLE IF NOT EXISTS _imported_paths (dirpath TEXT, fname TEXT)"
        )
        self.conn.execute("DELETE FROM _imported_paths")
        self.conn.executemany(
            "INSERT INTO _imported_paths (dirpath, fname) VALUES (?, ?)",
            [(os.path.dirname(p), os.path.basename(p)) for p in paths],
        )
        rows = self.conn.execute(
            """SELECT p.id FROM photos p
               JOIN folders f ON p.folder_id = f.id
               JOIN _imported_paths ip ON f.path = ip.dirpath
                                       AND p.filename = ip.fname"""
        ).fetchall()
        photo_ids = [r["id"] for r in rows]
        self.conn.execute("DROP TABLE IF EXISTS _imported_paths")
        return photo_ids

    def ids_in_folders(self, folder_ids):
        """Ids of every photo whose folder is one of ``folder_ids``."""
        ids = []
        for chunk in self._chunks(folder_ids):
            placeholders = ",".join("?" for _ in chunk)
            ids.extend(row["id"] for row in self.conn.execute(
                f"SELECT id FROM photos WHERE folder_id IN ({placeholders})",
                list(chunk),
            ))
        return ids

    # -- workspace-scoped listing -------------------------------------------

    def get_calendar_data(
        self,
        year,
        folder_id=None,
        collection_id=None,
        rules=None,
        *,
        get_folder_subtree_ids,
        build_collection_query,
        build_query_from_rules,
    ):
        """Return daily photo counts for ``year`` plus the workspace's year bounds."""
        ws = self.workspace_id
        conditions = ["wf.workspace_id = ?", "p.timestamp IS NOT NULL",
                      "substr(p.timestamp, 1, 4) = ?"]
        join_params = []
        where_params = [ws, str(year)]
        if rules is not None:
            r_folder_join, r_join_clause, r_where, r_params = (
                build_query_from_rules(rules)
            )
            conditions.append(
                "p.id IN (SELECT DISTINCT p.id FROM photos p "
                f"{r_folder_join} {r_join_clause} {r_where})"
            )
            where_params.extend(r_params)

        # Dashboard-scoped collection Browse composes the collection with the
        # active rules/folder — the calendar must match the grid, so restrict
        # counts to photos in the collection (same subquery shape as
        # get_browse_summary). Match get_photos / _append_collection_restriction:
        # raise on a missing/invalid collection instead of silently returning
        # unfiltered workspace data (which would mislead users into thinking
        # the collection contains those photos).
        if collection_id is not None:
            parts = build_collection_query(collection_id)
            if parts is None:
                raise ValueError("collection not found in active workspace")
            coll_folder_join, coll_join_clause, coll_where, coll_params = parts
            coll_subquery = (
                f"SELECT DISTINCT p.id FROM photos p "
                f"{coll_folder_join} {coll_join_clause} {coll_where}"
            )
            conditions.append(f"p.id IN ({coll_subquery})")
            where_params.extend(coll_params)

        join_clause = ("JOIN photo_workspace_visibility wf ON wf.photo_id = p.id"
                       "\nJOIN folders f ON f.id = p.folder_id AND f.status IN ('ok', 'partial')")

        if folder_id is not None:
            subtree = get_folder_subtree_ids(folder_id)
            placeholders = ",".join("?" for _ in subtree)
            conditions.append(f"p.folder_id IN ({placeholders})")
            where_params.extend(subtree)

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
            JOIN photo_workspace_visibility wf ON wf.photo_id = p.id
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

    def list_page(
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
        *,
        get_folder_subtree_ids,
        build_collection_query,
        append_location_status_filter,
        photo_sort_clause,
    ):
        """Return one page of the workspace's filtered photo list."""
        conditions = ["wf.workspace_id = ?"]
        where_params = [self.workspace_id]
        join_params = []

        if folder_id is not None:
            subtree = get_folder_subtree_ids(folder_id)
            placeholders = ",".join("?" for _ in subtree)
            conditions.append(f"p.folder_id IN ({placeholders})")
            where_params.extend(subtree)
        if collection_id is not None:
            parts = build_collection_query(collection_id)
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
            where_params.append(self.inclusive_date_to(date_to))
        if flag is not None:
            conditions.append("COALESCE(p.flag, 'none') = ?")
            where_params.append(flag)
        append_location_status_filter(conditions, location_status)

        join_clause = ("JOIN photo_workspace_visibility wf ON wf.photo_id = p.id"
                       "\nJOIN folders f ON f.id = p.folder_id AND f.status IN ('ok', 'partial')")
        if keyword is not None:
            kw_clause, kw_params = self.keyword_token_clause(
                keyword,
                match_case=keyword_match_case,
                whole_word=keyword_whole_word,
            )
            if kw_clause:
                conditions.append(kw_clause)
                where_params.extend(kw_params)

        if color_label is not None:
            join_clause += "\nJOIN photo_color_labels pcl ON pcl.photo_id = p.id AND pcl.workspace_id = ?"
            join_params.append(self.workspace_id)
            conditions.append("pcl.color = ?")
            where_params.append(color_label)

        # join_params must precede where_params because JOIN placeholders appear
        # in the SQL before the WHERE placeholders.
        params = join_params + where_params

        where = "WHERE " + " AND ".join(conditions)

        order, order_params = photo_sort_clause(sort)

        page = max(1, page)
        offset = (page - 1) * per_page
        params.extend(order_params)
        params.extend([per_page, offset])

        pcols = ", ".join(f"p.{c.strip()}" for c in self.photo_cols.split(","))
        distinct = "DISTINCT " if keyword is not None else ""
        query = f"""
            SELECT {distinct}{pcols} FROM photos p
            {join_clause}
            {where}
            ORDER BY {order}
            LIMIT ? OFFSET ?
        """
        return self.conn.execute(query, params).fetchall()

    def get_ids(
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
        *,
        get_folder_subtree_ids,
        build_collection_query,
        append_location_status_filter,
        photo_sort_clause,
    ):
        """Return every filtered photo id in the workspace, in sort order."""
        conditions = ["wf.workspace_id = ?"]
        where_params = [self.workspace_id]
        join_params = []

        if folder_id is not None:
            subtree = get_folder_subtree_ids(folder_id)
            placeholders = ",".join("?" for _ in subtree)
            conditions.append(f"p.folder_id IN ({placeholders})")
            where_params.extend(subtree)
        if collection_id is not None:
            parts = build_collection_query(collection_id)
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
            where_params.append(self.inclusive_date_to(date_to))
        if flag is not None:
            conditions.append("COALESCE(p.flag, 'none') = ?")
            where_params.append(flag)
        append_location_status_filter(conditions, location_status)

        join_clause = ("JOIN photo_workspace_visibility wf ON wf.photo_id = p.id"
                       "\nJOIN folders f ON f.id = p.folder_id AND f.status IN ('ok', 'partial')")
        if keyword is not None:
            kw_clause, kw_params = self.keyword_token_clause(
                keyword,
                match_case=keyword_match_case,
                whole_word=keyword_whole_word,
            )
            if kw_clause:
                conditions.append(kw_clause)
                where_params.extend(kw_params)

        if color_label is not None:
            join_clause += "\nJOIN photo_color_labels pcl ON pcl.photo_id = p.id AND pcl.workspace_id = ?"
            join_params.append(self.workspace_id)
            conditions.append("pcl.color = ?")
            where_params.append(color_label)

        params = join_params + where_params
        where = "WHERE " + " AND ".join(conditions)

        order, order_params = photo_sort_clause(sort)
        params = params + order_params
        distinct = "DISTINCT " if keyword is not None else ""
        query = f"""
            SELECT {distinct}p.id FROM photos p
            {join_clause}
            {where}
            ORDER BY {order}
        """
        return [row["id"] for row in self.conn.execute(query, params).fetchall()]

    def get_position(
        self,
        photo_id,
        folder_id=None,
        collection_id=None,
        sort="date",
        *,
        get_folder_subtree_ids,
        build_collection_query,
        photo_sort_clause,
    ):
        """Return a photo's zero-based position, or None when not listed."""
        conditions = ["wf.workspace_id = ?"]
        params = [self.workspace_id]

        if folder_id is not None:
            subtree = get_folder_subtree_ids(folder_id)
            placeholders = ",".join("?" for _ in subtree)
            conditions.append(f"p.folder_id IN ({placeholders})")
            params.extend(subtree)
        if collection_id is not None:
            parts = build_collection_query(collection_id)
            if parts is None:
                raise ValueError("collection not found in active workspace")
            coll_folder_join, coll_join_clause, coll_where, coll_params = parts
            coll_subquery = (
                "SELECT DISTINCT p.id FROM photos p "
                f"{coll_folder_join} {coll_join_clause} {coll_where}"
            )
            conditions.append(f"p.id IN ({coll_subquery})")
            params.extend(coll_params)

        order, order_params = photo_sort_clause(sort)
        where = "WHERE " + " AND ".join(conditions)
        # The ORDER BY lives inside the select list here, so its parameters
        # bind *before* the WHERE's — unlike the paged reads above, where the
        # clause trails the WHERE.
        row = self.conn.execute(
            f"""
            SELECT position
            FROM (
                SELECT p.id,
                       ROW_NUMBER() OVER (ORDER BY {order}) - 1 AS position
                FROM photos p
                JOIN photo_workspace_visibility wf ON wf.photo_id = p.id
                JOIN folders f ON f.id = p.folder_id
                    AND f.status IN ('ok', 'partial')
                {where}
            ) ordered_photos
            WHERE id = ?
            """,
            order_params + params + [photo_id],
        ).fetchone()
        return int(row["position"]) if row is not None else None

    def count_filtered(
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
        *,
        get_folder_subtree_ids,
        build_collection_query,
        append_location_status_filter,
    ):
        """Return how many workspace photos match the filters."""
        conditions = ["wf.workspace_id = ?"]
        where_params = [self.workspace_id]
        join_params = []

        if folder_id is not None:
            subtree = get_folder_subtree_ids(folder_id)
            placeholders = ",".join("?" for _ in subtree)
            conditions.append(f"p.folder_id IN ({placeholders})")
            where_params.extend(subtree)
        if collection_id is not None:
            parts = build_collection_query(collection_id)
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
            where_params.append(self.inclusive_date_to(date_to))
        if flag is not None:
            conditions.append("COALESCE(p.flag, 'none') = ?")
            where_params.append(flag)
        append_location_status_filter(conditions, location_status)

        join_clause = ("JOIN photo_workspace_visibility wf ON wf.photo_id = p.id"
                       "\nJOIN folders f ON f.id = p.folder_id AND f.status IN ('ok', 'partial')")
        if keyword is not None:
            kw_clause, kw_params = self.keyword_token_clause(
                keyword,
                match_case=keyword_match_case,
                whole_word=keyword_whole_word,
            )
            if kw_clause:
                conditions.append(kw_clause)
                where_params.extend(kw_params)

        if color_label is not None:
            join_clause += "\nJOIN photo_color_labels pcl ON pcl.photo_id = p.id AND pcl.workspace_id = ?"
            join_params.append(self.workspace_id)
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
        collection_id=None,
        rules=None,
        *,
        get_folder_subtree_ids,
        build_collection_query,
        build_query_from_rules,
        detector_confidence,
    ):
        """Return the browse panel's totals, classified count, top species
        and folder breakdown.

        ``detector_confidence()`` returns the workspace's detector floor; it
        is called after the two counts, where the façade used to read it.
        """
        ws = self.workspace_id

        # Build shared filter conditions. Metadata filtering arrives
        # exclusively as a universal-filter ``rules`` tree (legacy per-field
        # params removed in Phase 5).
        conditions = ["wf.workspace_id = ?"]
        join_params = []
        where_params = [ws]
        if folder_id is not None:
            subtree = get_folder_subtree_ids(folder_id)
            placeholders = ",".join("?" for _ in subtree)
            conditions.append(f"p.folder_id IN ({placeholders})")
            where_params.extend(subtree)

        # When browsing a collection, restrict photos to those matching the
        # collection's rules by using a subquery from _build_collection_query.
        # Match get_photos / _append_collection_restriction: raise on a
        # missing/invalid collection instead of silently returning unfiltered
        # workspace numbers (which would mislead users into thinking the
        # collection contains those photos).
        if collection_id is not None:
            parts = build_collection_query(collection_id)
            if parts is None:
                raise ValueError("collection not found in active workspace")
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

        if rules is not None:
            r_folder_join, r_join_clause, r_where, r_params = (
                build_query_from_rules(rules)
            )
            rules_subquery = (
                f"SELECT DISTINCT p.id FROM photos p "
                f"{r_folder_join} {r_join_clause} {r_where}"
            )
            conditions.append(f"p.id IN ({rules_subquery})")
            where_params.extend(r_params)

        join_clause = ("JOIN photo_workspace_visibility wf ON wf.photo_id = p.id"
                       "\nJOIN folders f ON f.id = p.folder_id AND f.status IN ('ok', 'partial')")
        # join_params must precede where_params because JOIN placeholders appear
        # in the SQL before the WHERE placeholders.
        params = join_params + where_params

        where = "WHERE " + " AND ".join(conditions)

        # Total (unfiltered) count
        total = self.conn.execute(
            """SELECT COUNT(*) FROM photos p
               JOIN photo_workspace_visibility wf ON wf.photo_id = p.id
               JOIN folders f ON f.id = p.folder_id AND f.status IN ('ok', 'partial')
               WHERE wf.workspace_id = ?""",
            (ws,),
        ).fetchone()[0]

        # Every aggregate below describes the same filtered set, and a
        # metadata search is by far the costliest part of computing it, so
        # the matching ids are materialized once. The TEMP table lives on
        # this request's connection and is dropped before returning; the
        # SAVEPOINT reads all four aggregates from one snapshot without
        # committing a caller's open transaction (as ``stage_scope_ids``).
        self.conn.execute("SAVEPOINT browse_summary")
        try:
            self.conn.execute("DROP TABLE IF EXISTS temp._browse_summary_ids")
            self.conn.execute(
                "CREATE TEMP TABLE _browse_summary_ids (id INTEGER PRIMARY KEY)"
            )
            self.conn.execute(
                "INSERT INTO _browse_summary_ids (id) "
                f"SELECT DISTINCT p.id FROM photos p {join_clause} {where}",
                params,
            )
            summary = self._browse_summary_aggregates(ws, detector_confidence)
        except BaseException:
            # An interrupted INSERT (a superseded search) makes SQLite roll
            # back the whole transaction itself, savepoint included.
            if self.conn.in_transaction:
                self.conn.execute("ROLLBACK TO browse_summary")
            raise
        finally:
            self.conn.execute("DROP TABLE IF EXISTS temp._browse_summary_ids")
            if self.conn.in_transaction:
                self.conn.execute("RELEASE browse_summary")
        return {"total": total, **summary}

    def _browse_summary_aggregates(self, ws, detector_confidence):
        """The Browse summary's aggregates over ``temp._browse_summary_ids``."""
        filtered_total = self.conn.execute(
            "SELECT COUNT(*) FROM _browse_summary_ids"
        ).fetchone()[0]

        # Classified vs unclassified (within filter).  Detections and
        # predictions are global; workspace scoping comes from the filtered
        # ids and the detector_confidence read-time threshold.
        min_conf = detector_confidence()
        classified = self.conn.execute(
            """SELECT COUNT(DISTINCT s.id) FROM _browse_summary_ids s
                JOIN detections det ON det.photo_id = s.id
                JOIN predictions pred ON pred.detection_id = det.id
                WHERE det.detector_confidence >= ?""",
            (min_conf,),
        ).fetchone()[0]

        # Top species (within filter).  Review status is workspace-scoped via
        # prediction_review; absent rows are treated as 'pending' (which is
        # included — we only want to exclude 'rejected' reviews).
        # Pin to the most recent labels_fingerprint per
        # (detection, classifier_model) so a workspace that rotated label
        # sets doesn't have stale higher-confidence rows from an old
        # fingerprint dominating the top-species ranking.
        top_species = self.conn.execute(
            f"""WITH best_pred AS ({TOP_SPECIES_RANKING_SQL})
                SELECT bp.species, COUNT(DISTINCT s.id) as count
                FROM _browse_summary_ids s
                JOIN best_pred bp ON bp.photo_id = s.id AND bp.rn = 1
                GROUP BY bp.species
                ORDER BY count DESC
                LIMIT 5""",
            (ws, min_conf),
        ).fetchall()

        # Folder breakdown (within filter)
        folder_counts = self.conn.execute(
            """SELECT f.id as folder_id, f.name, COUNT(*) as count
                FROM _browse_summary_ids s
                JOIN photos p ON p.id = s.id
                JOIN folders f ON f.id = p.folder_id
                GROUP BY f.id
                ORDER BY count DESC"""
        ).fetchall()

        return {
            "filtered_total": filtered_total,
            "classified": classified,
            "unclassified": filtered_total - classified,
            "top_species": [{"species": r["species"], "count": r["count"]} for r in top_species],
            "folder_counts": [{"folder_id": r["folder_id"], "name": r["name"], "count": r["count"]} for r in folder_counts],
        }

    # -- companions and deletion ---------------------------------------------

    def count_with_companions(self, photo_ids):
        """How many of ``photo_ids`` in the workspace carry a companion file."""
        total = 0
        ws_id = self.workspace_id
        for chunk in self._chunks(list(dict.fromkeys(photo_ids or []))):
            placeholders = ",".join("?" for _ in chunk)
            row = self.conn.execute(
                f"SELECT COUNT(*) AS n FROM photos p "
                f"JOIN photo_workspace_visibility wf ON wf.photo_id = p.id "
                f"WHERE p.id IN ({placeholders}) "
                f"AND wf.workspace_id = ? "
                f"AND NULLIF(p.companion_path, '') IS NOT NULL",
                list(chunk) + [ws_id],
            ).fetchone()
            total += int(row["n"] or 0)
        return total

    def resolve_for_delete(self, photo_ids, include_companions=False):
        """Read-only: the rows, ids and file records a delete would remove."""
        if not photo_ids:
            return {"ids": [], "files": [], "_rows": []}

        # Resolve to actual existing photos. Chunked — callers like
        # /api/audit/remove-missing pass arbitrarily large id lists straight
        # from the request body.
        rows = []
        for chunk in self._chunks(list(dict.fromkeys(photo_ids))):
            placeholders = ",".join("?" for _ in chunk)
            rows.extend(self.conn.execute(
                f"SELECT p.id, p.filename, p.companion_path, p.folder_id, f.path AS folder_path "
                f"FROM photos p JOIN folders f ON p.folder_id = f.id "
                f"WHERE p.id IN ({placeholders})",
                list(chunk),
            ).fetchall())

        if not rows:
            return {"ids": [], "files": [], "_rows": []}

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
                for chunk in self._chunks(dict.fromkeys(companion_ids)):
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
                "folder_id": row["folder_id"],
                "folder_path": row["folder_path"],
                "filename": row["filename"],
                "companion_path": row["companion_path"],
            }
            for row in rows
        ]

        return {"ids": all_ids, "files": files, "_rows": rows}

    def delete(
        self,
        all_ids,
        folder_counts,
        deleted_stems_by_folder,
        folder_paths,
        *,
        commit,
        workspace_id_fn,
    ):
        """Delete the resolved photos, their dependents and stale provenance.

        With ``commit`` the work commits here and rolls back on error;
        without it, the caller's transaction owns both. ``workspace_id_fn``
        resolves the active workspace mid-transaction, where the façade used
        to call ``_ws_id()``, so a missing workspace still rolls back.
        """
        # Chunk the all_ids IN-clauses. ``include_companions=True`` can double
        # the id count from the caller's input chunk (companions get merged in
        # above), so a 900-id outer chunk can reach ~1800 here — past the 999
        # SQLITE_MAX_VARIABLE_NUMBER on legacy builds. All chunked statements
        # share the same transaction, so partial-failure rollback still works.
        id_chunks = list(self._chunks(all_ids))

        try:
            # Delete associated data (non-cascading FKs)
            for chunk in id_chunks:
                ph = ",".join("?" for _ in chunk)
                self.conn.execute(f"DELETE FROM photo_keywords WHERE photo_id IN ({ph})", chunk)
                self.conn.execute(
                    f"DELETE FROM photo_embedded_keyword_offered WHERE photo_id IN ({ph})",
                    chunk,
                )
                self.conn.execute(f"DELETE FROM pending_changes WHERE photo_id IN ({ph})", chunk)
                # Deleting detections cascades to predictions via ON DELETE CASCADE
                self.conn.execute(f"DELETE FROM detections WHERE photo_id IN ({ph})", chunk)

            # The active workspace is still resolved here so a delete
            # without one rolls back.
            workspace_id_fn()

            # Delete photos (cascades to edit_history_items, inat_submissions)
            # and take them out of every workspace's static collections.
            with photo_row_deletion(self.conn) as photo_rows:
                photo_rows.delete(dict.fromkeys(all_ids))

            # A moved RAW/JPEG sibling stores the source folder path as
            # provenance so another same-stem sibling can follow it to the
            # destination without tripping the developed-render collision
            # guard. Once delete_photos removes the last such sibling from
            # the source, that proof is no longer valid: a later unrelated
            # photo imported at the same path must not inherit the old
            # render. Find source stems drained by this delete and expire
            # their provenance in the same transaction.
            drained_stems_by_path = {}
            for folder_id, deleted_stems in deleted_stems_by_folder.items():
                remaining_stems = {
                    os.path.splitext(row["filename"])[0]
                    for row in self.conn.execute(
                        "SELECT filename FROM photos WHERE folder_id = ?",
                        (folder_id,),
                    )
                }
                drained = deleted_stems - remaining_stems
                if drained:
                    drained_stems_by_path.setdefault(
                        folder_paths[folder_id], set()
                    ).update(drained)

            stale_provenance_ids = []
            provenance_paths = list(drained_stems_by_path)
            for path_chunk in self._chunks(provenance_paths):
                path_ph = ",".join("?" for _ in path_chunk)
                for row in self.conn.execute(
                    f"SELECT id, filename, last_move_source_folder_path "
                    f"FROM photos WHERE last_move_source_folder_path "
                    f"IN ({path_ph})",
                    path_chunk,
                ):
                    stem = os.path.splitext(row["filename"])[0]
                    if stem in drained_stems_by_path[
                        row["last_move_source_folder_path"]
                    ]:
                        stale_provenance_ids.append(row["id"])
            for stale_chunk in self._chunks(stale_provenance_ids):
                stale_ph = ",".join("?" for _ in stale_chunk)
                self.conn.execute(
                    f"UPDATE photos SET "
                    f"last_move_source_folder_path = NULL "
                    f"WHERE id IN ({stale_ph})",
                    stale_chunk,
                )

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

    # -- quality scores ------------------------------------------------------

    def update_sharpness(self, photo_id, sharpness):
        """Set photo sharpness score."""
        self.conn.execute(
            "UPDATE photos SET sharpness = ? WHERE id = ?", (sharpness, photo_id)
        )
        self.commit_with_retry(self.conn)

    def update_quality(
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
        self.commit_with_retry(self.conn)

    # -- working copy and thumbnail columns ----------------------------------

    def working_copy_path(self, photo_id):
        """The photo's ``working_copy_path``, or None (unset or unknown id)."""
        row = self.conn.execute(
            "SELECT working_copy_path FROM photos WHERE id=?",
            (photo_id,),
        ).fetchone()
        return row["working_copy_path"] if row else None

    def record_generated_original(self, photo_id, working_copy_path, *,
                                  tracked, dimensions=None):
        """Record an on-demand full-resolution render on the photo row and commit.

        ``tracked`` stores ``working_copy_path`` and clears the eviction
        marker; untracked clears the path and marks it evicted at the source
        mtime (``-1`` without one), ignoring ``working_copy_path``.
        ``dimensions`` (``(width, height)``) also sets the photo's size.
        """
        if tracked:
            updates = [
                "working_copy_path=?",
                "working_copy_evicted_mtime=NULL",
            ]
            params = [working_copy_path]
        else:
            updates = [
                "working_copy_path=NULL",
                "working_copy_evicted_mtime=COALESCE(file_mtime, -1)",
            ]
            params = []
        if dimensions is not None:
            updates.extend(["width=?", "height=?"])
            params.extend(dimensions)
        params.append(photo_id)
        self.conn.execute(
            f"UPDATE photos SET {', '.join(updates)} WHERE id=?",
            params,
        )
        self.conn.commit()

    def set_thumb_path(self, photo_id, thumb_path):
        """Store the photo's thumbnail filename and commit."""
        self.conn.execute(
            "UPDATE photos SET thumb_path=? WHERE id=?",
            (thumb_path, photo_id),
        )
        self.conn.commit()
