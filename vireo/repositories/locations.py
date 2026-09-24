"""Persistence for place/location keywords, photo locations and geo queries.

``Database`` keeps the composition: keyword upserts (``_upsert_one_keyword``,
``_upsert_location_parent_chain``), ``tag_photo`` with its provenance fold,
``link_keyword_to_place`` (a pinned ``photo_keywords`` writer), the sync
queue's ``queue_change``, folder subtrees, the rules query builder and
keyword merges all stay on the façade. Where a moved method needs one of
them mid-query it receives the bound façade method as a callback, so
patches of ``Database`` methods still apply.

Photos and keywords are catalog-wide, so most methods take no workspace.
The ones that are scoped resolve the active workspace through
``workspace_id_fn`` at the same point the original ``Database`` code called
``self._ws_id()``; building the repository never resolves it.
"""

import time


class LocationRepository:
    def __init__(self, conn, workspace_id_fn=None, *, chunk_size=800,
                 photo_date_asc_order=None):
        self.conn = conn
        self.workspace_id_fn = workspace_id_fn
        self.chunk_size = chunk_size
        self.photo_date_asc_order = photo_date_asc_order

    def transaction(self):
        """The connection as a context manager: commit on success, roll back on error."""
        return self.conn

    def _chunks(self, values):
        values = list(values)
        for idx in range(0, len(values), self.chunk_size):
            yield values[idx:idx + self.chunk_size]

    # -- location status and map queries -------------------------------------

    def get_photo_statuses(self, photo_ids):
        """Return ``{photo_id: exif|assigned|none}`` for the requested photos."""
        if not photo_ids:
            return {}
        result = {}
        for chunk in self._chunks(list(dict.fromkeys(photo_ids))):
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

    def get_geolocated_photos(
        self,
        folder_id=None,
        rules=None,
        *,
        folder_subtree_ids,
        build_query_from_rules,
    ):
        """Return all geolocated photos; see ``Database.get_geolocated_photos``.

        ``folder_subtree_ids`` and ``build_query_from_rules`` are the façade's
        ``get_folder_subtree_ids`` and ``_build_query_from_rules``.
        """
        # Paired fallback: either BOTH EXIF axes win, or BOTH keyword axes win.
        # Per-axis COALESCE would let a photo with partial EXIF (only one axis
        # populated) emit a mixed pair, producing wrong markers.
        conditions = [
            "wf.workspace_id = ?",
            "((p.latitude IS NOT NULL AND p.longitude IS NOT NULL) "
            " OR (kl.latitude IS NOT NULL AND kl.longitude IS NOT NULL))",
        ]
        params = [self.workspace_id_fn()]

        if folder_id is not None:
            subtree = folder_subtree_ids(folder_id)
            placeholders = ",".join("?" for _ in subtree)
            conditions.append(f"p.folder_id IN ({placeholders})")
            params.extend(subtree)
        if rules is not None:
            r_folder_join, r_join_clause, r_where, r_params = (
                build_query_from_rules(rules)
            )
            conditions.append(
                "p.id IN (SELECT DISTINCT p.id FROM photos p "
                f"{r_folder_join} {r_join_clause} {r_where})"
            )
            params.extend(r_params)

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
        where = "WHERE " + " AND ".join(conditions)

        # Surface the most-recently-tagged species keyword (highest rowid),
        # which reflects the user's latest confirmed identification when
        # multiple tags exist.
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
            ORDER BY {self.photo_date_asc_order}
        """
        return self.conn.execute(query, species_col_params + params).fetchall()

    def get_assigned(self, photo_id):
        """Return linked location-keyword coordinates for one photo, or None.

        The caller has already applied the workspace membership check.
        """
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

    def get_photo_leaves(self, photo_ids):
        """Choose the effective exported location row for each photo."""
        if not photo_ids:
            return {}

        leaves = {}
        for chunk in self._chunks(list(dict.fromkeys(photo_ids))):
            placeholders = ",".join("?" for _ in chunk)
            rows = self.conn.execute(
                f"""SELECT photo_id, id, name, parent_id FROM (
                        SELECT pk.photo_id, k.id, k.name, k.parent_id,
                               ROW_NUMBER() OVER (
                                 PARTITION BY pk.photo_id
                                 ORDER BY (k.latitude IS NULL OR k.longitude IS NULL) ASC,
                                          (k.parent_id IS NULL) ASC,
                                          k.id DESC
                               ) AS rn
                        FROM photo_keywords pk
                        JOIN keywords k ON k.id = pk.keyword_id
                        WHERE pk.photo_id IN ({placeholders})
                          AND k.type = 'location'
                    ) WHERE rn = 1""",
                list(chunk),
            ).fetchall()
            for row in rows:
                leaves[row["photo_id"]] = row

        return leaves

    def get_photo_paths(self, leaves):
        """Return ``{photo_id: [broadest, ..., leaf]}`` for chosen leaf rows.

        ``leaves`` is ``Database._get_photo_location_leaves``'s result; see
        ``Database.get_photo_location_paths``.
        """
        # One cache for the whole batch: a shoot shares a place, so thousands
        # of photos resolve the same handful of chains.
        chains = {}

        def chain_for(keyword_id, name, parent_id):
            if keyword_id in chains:
                return chains[keyword_id]
            parts = [name]
            seen = {keyword_id}
            current = parent_id
            while current is not None and current not in seen:
                seen.add(current)
                parent = self.conn.execute(
                    "SELECT name, parent_id, type FROM keywords WHERE id = ?",
                    (current,),
                ).fetchone()
                # Stop at the first non-location ancestor rather than walking
                # into an unrelated tree: a location chain re-parented under a
                # general keyword would otherwise write that keyword into
                # every sidecar as the root of the place hierarchy.
                if parent is None or parent["type"] != "location":
                    break
                parts.append(parent["name"])
                current = parent["parent_id"]
            chains[keyword_id] = list(reversed(parts))
            return chains[keyword_id]

        return {
            photo_id: chain_for(row["id"], row["name"], row["parent_id"])
            for photo_id, row in leaves.items()
        }

    def has_pending_change(self, photo_id):
        """Return whether a ``location`` change is queued for ``photo_id`` in any workspace."""
        return self.conn.execute(
            "SELECT 1 FROM pending_changes WHERE photo_id = ? "
            "AND change_type = 'location' LIMIT 1",
            (photo_id,),
        ).fetchone() is not None

    def count_photos_with_location(self):
        """Count photos in the active workspace carrying a location keyword."""
        return self.conn.execute(
            """SELECT COUNT(DISTINCT p.id)
               FROM photos p
               JOIN workspace_folders wf ON wf.folder_id = p.folder_id
               JOIN photo_keywords pk ON pk.photo_id = p.id
               JOIN keywords k ON k.id = pk.keyword_id
               WHERE wf.workspace_id = ? AND k.type = 'location'""",
            (self.workspace_id_fn(),),
        ).fetchone()[0]

    def queue_changes_for_tagged_photos(self, *, queue_change):
        """Queue a ``location`` change for every located photo in the workspace.

        ``queue_change`` is the façade's ``Database.queue_change``; each call
        runs uncommitted and one commit covers the batch. Returns
        ``{"photos": n, "queued": k, "already_queued": n - k}``.
        """
        ws_id = self.workspace_id_fn()
        photo_ids = [
            row[0] for row in self.conn.execute(
                """SELECT DISTINCT p.id
                   FROM photos p
                   JOIN workspace_folders wf ON wf.folder_id = p.folder_id
                   JOIN photo_keywords pk ON pk.photo_id = p.id
                   JOIN keywords k ON k.id = pk.keyword_id
                   WHERE wf.workspace_id = ? AND k.type = 'location'
                   ORDER BY p.id""",
                (ws_id,),
            ).fetchall()
        ]
        queued = 0
        for photo_id in photo_ids:
            if queue_change(
                photo_id, "location", "effective",
                workspace_id=ws_id, _commit=False,
            ):
                queued += 1
        self.conn.commit()
        return {
            "photos": len(photo_ids),
            "queued": queued,
            "already_queued": len(photo_ids) - queued,
        }

    def get_effective(self, photo_ids, verify_workspace=True):
        """Return effective locations for many photos keyed by photo ID.

        See ``Database.get_effective_photo_locations``.
        """
        if not photo_ids:
            return {}

        photo_ids = list(dict.fromkeys(photo_ids))
        result = {}
        seen_ids = set()
        for chunk in self._chunks(photo_ids):
            selected_values = ",".join("(?)" for _ in chunk)
            workspace_join = ""
            params = list(chunk)
            if verify_workspace:
                workspace_join = """
                    JOIN workspace_folders wf
                      ON wf.folder_id = p.folder_id AND wf.workspace_id = ?
                """
                params.append(self.workspace_id_fn())
            rows = self.conn.execute(
                f"""
                WITH selected(id) AS (VALUES {selected_values}),
                ranked_locations AS (
                    SELECT pk_loc.photo_id, k_loc.name, k_loc.place_id,
                           k_loc.latitude, k_loc.longitude,
                           ROW_NUMBER() OVER (
                             PARTITION BY pk_loc.photo_id
                             ORDER BY (k_loc.parent_id IS NULL) ASC, k_loc.id DESC
                           ) AS rn
                    FROM photo_keywords pk_loc
                    JOIN selected s ON s.id = pk_loc.photo_id
                    JOIN keywords k_loc ON k_loc.id = pk_loc.keyword_id
                    WHERE k_loc.type = 'location'
                      AND k_loc.latitude IS NOT NULL
                      AND k_loc.longitude IS NOT NULL
                )
                SELECT p.id,
                       p.latitude AS photo_latitude,
                       p.longitude AS photo_longitude,
                       kl.latitude AS keyword_latitude,
                       kl.longitude AS keyword_longitude,
                       kl.name AS keyword_location_name,
                       kl.place_id AS place_id
                FROM selected s
                JOIN photos p ON p.id = s.id
                {workspace_join}
                LEFT JOIN ranked_locations kl
                  ON kl.photo_id = p.id AND kl.rn = 1
                """,
                params,
            ).fetchall()
            seen_ids.update(row["id"] for row in rows)
            for row in rows:
                if (
                    row["photo_latitude"] is not None
                    and row["photo_longitude"] is not None
                ):
                    result[row["id"]] = {
                        "photo_id": row["id"],
                        "latitude": row["photo_latitude"],
                        "longitude": row["photo_longitude"],
                        "source": "exif",
                        "keyword_location_name": None,
                        "place_id": None,
                    }
                elif (
                    row["keyword_latitude"] is not None
                    and row["keyword_longitude"] is not None
                ):
                    result[row["id"]] = {
                        "photo_id": row["id"],
                        "latitude": row["keyword_latitude"],
                        "longitude": row["keyword_longitude"],
                        "source": "keyword",
                        "keyword_location_name": row["keyword_location_name"],
                        "place_id": row["place_id"],
                    }

        if verify_workspace:
            missing_id = next(
                (pid for pid in photo_ids if pid not in seen_ids), None
            )
            if missing_id is not None:
                raise ValueError(
                    f"Photo {missing_id} does not belong to the active workspace"
                )
        return result

    def count_photos_without_coordinates(self):
        """Count photos in the active workspace that the map can't plot."""
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
            (self.workspace_id_fn(),),
        ).fetchone()
        return row[0]

    def get_plottable_photo_ids(self, folder_id=None, *, folder_subtree_ids):
        """Return ids of every photo the Map endpoint could render.

        ``folder_subtree_ids`` is the façade's ``get_folder_subtree_ids``.
        """
        params = [self.workspace_id_fn()]
        folder_clause = ""
        if folder_id is not None:
            subtree = folder_subtree_ids(folder_id)
            placeholders = ",".join("?" for _ in subtree)
            folder_clause = f"AND p.folder_id IN ({placeholders})"
            params.extend(subtree)
        rows = self.conn.execute(
            f"""
            SELECT p.id FROM photos p
            JOIN workspace_folders wf ON wf.folder_id = p.folder_id
            JOIN folders f ON f.id = p.folder_id AND f.status IN ('ok', 'partial')
            WHERE wf.workspace_id = ?
              {folder_clause}
              AND (
                (p.latitude IS NOT NULL AND p.longitude IS NOT NULL)
                OR EXISTS (
                  SELECT 1 FROM photo_keywords pk
                  JOIN keywords k ON k.id = pk.keyword_id
                  WHERE pk.photo_id = p.id
                    AND k.type = 'location'
                    AND k.latitude IS NOT NULL
                    AND k.longitude IS NOT NULL
                )
              )
            """,
            params,
        ).fetchall()
        return [row[0] for row in rows]

    # -- legacy taxonomy repair ----------------------------------------------

    def restore_misclassified_ancestor(self, keyword_id, allow_leaf=False):
        """Retype one legacy taxonomy row that is structurally a location.

        See ``Database._restore_misclassified_location_ancestor``. Does not
        commit.
        """
        row = self.conn.execute(
            "WITH RECURSIVE descendants(id, type) AS ("
            "  SELECT child.id, child.type FROM keywords child "
            "  WHERE child.parent_id = ? "
            "  UNION "
            "  SELECT child.id, child.type FROM keywords child "
            "  JOIN descendants parent ON child.parent_id = parent.id "
            "  WHERE parent.type = 'taxonomy'"
            ") "
            "SELECT k.id FROM keywords k "
            "LEFT JOIN keywords parent ON parent.id = k.parent_id "
            "WHERE k.id = ? AND k.type = 'taxonomy' "
            "  AND (k.parent_id IS NULL OR parent.type = 'location') "
            "  AND ("
            "    k.place_id IS NOT NULL "
            "    OR (parent.type = 'location' AND ("
            "      ? OR EXISTS ("
            "        SELECT 1 FROM descendants WHERE type = 'location'"
            "      )"
            "    ))"
            "    OR (? AND EXISTS ("
            "      SELECT 1 FROM descendants WHERE type = 'location'"
            "    ))"
            "  )",
            (keyword_id, keyword_id, allow_leaf, allow_leaf),
        ).fetchone()
        if row is None:
            return False
        self.conn.execute(
            "UPDATE keywords SET type = 'location', is_species = 0, "
            "taxon_id = NULL WHERE id = ?",
            (keyword_id,),
        )
        return True

    def repair_misclassified_ancestors(self, *, restore_ancestor, merge_duplicate_roots):
        """Run the restore passes and the root merge; commit if anything changed.

        ``restore_ancestor`` and ``merge_duplicate_roots`` are the façade's
        ``_restore_misclassified_location_ancestor`` and
        ``_merge_duplicate_location_roots``. Returns ``(repaired, merged_roots)``.
        """
        repaired = 0
        while True:
            rows = self.conn.execute(
                "SELECT k.id FROM keywords k "
                "LEFT JOIN keywords parent ON parent.id = k.parent_id "
                "WHERE k.type = 'taxonomy' "
                "  AND (k.parent_id IS NULL OR parent.type = 'location')"
            ).fetchall()
            repaired_this_pass = 0
            for row in rows:
                if restore_ancestor(row["id"]):
                    repaired += 1
                    repaired_this_pass += 1
            if not repaired_this_pass:
                break
        merged_roots = merge_duplicate_roots()
        if repaired or merged_roots:
            self.conn.commit()
        return repaired, merged_roots

    def merge_duplicate_roots(self, *, merge_keyword_into):
        """Collapse same-name coordless ``type='location'`` roots.

        ``merge_keyword_into`` is the façade's ``_merge_keyword_into``. See
        ``Database._merge_duplicate_location_roots``.
        """
        duplicates = self.conn.execute(
            "SELECT name FROM keywords "
            "WHERE type = 'location' AND parent_id IS NULL "
            "GROUP BY name HAVING COUNT(*) > 1"
        ).fetchall()
        merged = 0
        for dup in duplicates:
            candidates = self.conn.execute(
                "SELECT id, place_id, latitude, longitude FROM keywords "
                "WHERE name = ? AND parent_id IS NULL AND type = 'location' "
                "ORDER BY id",
                (dup["name"],),
            ).fetchall()
            if len(candidates) < 2:
                continue
            place_bearing = [c for c in candidates if c["place_id"] is not None]
            coordless = [c for c in candidates if c["place_id"] is None]
            if place_bearing:
                # Any place-bearing root at this name means coordless
                # anchors cannot be safely merged into it: address
                # components have no place_id, so coordless-anchor
                # descendants may refer to a different Google place with
                # the same name. Preserve place-bearing roots alongside
                # coordless anchors; only collapse coordless duplicates
                # into a single shared anchor.
                if len(coordless) < 2:
                    continue
                survivor_id = coordless[0]["id"]
                for cand in coordless[1:]:
                    merged += merge_keyword_into(
                        cand["id"], survivor_id,
                    )
                continue
            # No place-bearing roots — every candidate is coordless.
            # Legacy duplicate anchors describe the same neutral parent
            # and can safely merge into the oldest survivor.
            survivor_id = candidates[0]["id"]
            for cand in candidates[1:]:
                merged += merge_keyword_into(cand["id"], survivor_id)
        return merged

    # -- place chains and photo links ----------------------------------------

    def find_place_keyword(self, place_id):
        """Return the keyword row already carrying ``place_id``, or None."""
        return self.conn.execute(
            "SELECT id FROM keywords WHERE place_id = ?",
            (place_id,),
        ).fetchone()

    def require_location_keyword(self, leaf_keyword_id):
        """Raise ``ValueError`` unless ``leaf_keyword_id`` is a location keyword."""
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

    def delete_photo_links(self, photo_id):
        """Delete ``photo_id``'s location-keyword links. Caller owns the transaction."""
        self.conn.execute(
            "DELETE FROM photo_keywords WHERE photo_id = ? "
            "AND keyword_id IN (SELECT id FROM keywords WHERE type='location')",
            (photo_id,),
        )

    def clear_photo(self, photo_id):
        """Remove any ``type='location'`` keyword links for ``photo_id`` and commit."""
        with self.conn:
            self.conn.execute(
                "DELETE FROM photo_keywords WHERE photo_id = ? "
                "AND keyword_id IN (SELECT id FROM keywords WHERE type='location')",
                (photo_id,),
            )

    # -- reverse-geocode cache -----------------------------------------------

    def reverse_geocode_cache_get(self, lat_grid, lng_grid):
        """Return ``{"place_id", "response"}`` cached at a grid cell, or None."""
        row = self.conn.execute(
            "SELECT place_id, response FROM place_reverse_geocode_cache "
            "WHERE lat_grid = ? AND lng_grid = ?",
            (lat_grid, lng_grid),
        ).fetchone()
        if row is None:
            return None
        return {"place_id": row["place_id"], "response": row["response"]}

    def reverse_geocode_cache_put(self, lat_grid, lng_grid, place_id, response_json):
        """Upsert a reverse-geocode result at a grid cell and commit."""
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
