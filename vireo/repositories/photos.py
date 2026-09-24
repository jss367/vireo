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
(``photo_labels``) have their own repositories.
"""

import os


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
        """Insert a photo (or find the existing row), commit, return its id."""
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
            photo_id = cur.lastrowid
        else:
            row = self.conn.execute(
                "SELECT id FROM photos WHERE folder_id = ? AND filename = ?",
                (folder_id, filename),
            ).fetchone()
            photo_id = row["id"]
        return photo_id

    def get(self, photo_id, verify_workspace=False):
        """Return one photo row (detail columns), or None.

        With ``verify_workspace`` the row must sit in a folder of
        ``workspace_id``; the repository must then be workspace-scoped.
        """
        if verify_workspace:
            return self.conn.execute(
                f"""SELECT {self.photo_detail_cols} FROM photos
                    WHERE id = ? AND folder_id IN (
                        SELECT folder_id FROM workspace_folders
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

    def count(self):
        """Return the workspace's photo count, skipping missing folders."""
        return self.conn.execute(
            """SELECT COUNT(*) FROM photos p
               JOIN workspace_folders wf ON wf.folder_id = p.folder_id
               JOIN folders f ON f.id = p.folder_id AND f.status IN ('ok', 'partial')
               WHERE wf.workspace_id = ?""",
            (self.workspace_id,),
        ).fetchone()[0]

    def count_in_workspace(self):
        """Return the workspace's photo count, including missing folders."""
        return self.conn.execute(
            """SELECT COUNT(*) FROM photos p
               JOIN workspace_folders wf ON wf.folder_id = p.folder_id
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

        join_clause = ("JOIN workspace_folders wf ON wf.folder_id = p.folder_id"
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

        join_clause = ("JOIN workspace_folders wf ON wf.folder_id = p.folder_id"
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

        join_clause = ("JOIN workspace_folders wf ON wf.folder_id = p.folder_id"
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
                JOIN workspace_folders wf ON wf.folder_id = p.folder_id
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

        join_clause = ("JOIN workspace_folders wf ON wf.folder_id = p.folder_id"
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

        join_clause = ("JOIN workspace_folders wf ON wf.folder_id = p.folder_id"
                       "\nJOIN folders f ON f.id = p.folder_id AND f.status IN ('ok', 'partial')")
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
        min_conf = detector_confidence()
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

    # -- companions and deletion ---------------------------------------------

    def count_with_companions(self, photo_ids):
        """How many of ``photo_ids`` in the workspace carry a companion file."""
        total = 0
        ws_id = self.workspace_id
        for chunk in self._chunks(list(dict.fromkeys(photo_ids or []))):
            placeholders = ",".join("?" for _ in chunk)
            row = self.conn.execute(
                f"SELECT COUNT(*) AS n FROM photos p "
                f"JOIN workspace_folders wf ON wf.folder_id = p.folder_id "
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
                self.conn.execute(f"DELETE FROM pending_changes WHERE photo_id IN ({ph})", chunk)
                # Deleting detections cascades to predictions via ON DELETE CASCADE
                self.conn.execute(f"DELETE FROM detections WHERE photo_id IN ({ph})", chunk)

            # Clean collection rules
            import json as _json
            collections = self.conn.execute(
                "SELECT id, rules FROM collections WHERE workspace_id = ?",
                (workspace_id_fn(),),
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
