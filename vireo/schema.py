"""One-time database schema initialization and ordered migrations.

The legacy canonical-schema code remains in ``Database`` while it is split
into discrete historical migrations.  This module is the startup boundary:
web requests open an initialized database and never perform schema work.
"""

from __future__ import annotations

import json
import sqlite3
import threading
from collections.abc import Callable
from dataclasses import dataclass

from db import Database

_SCHEMA_LOCK = threading.Lock()


@dataclass(frozen=True)
class Migration:
    version: int
    name: str
    apply: Callable[[sqlite3.Connection], None]
    validate: Callable[[sqlite3.Connection], None] | None = None


def _establish_startup_boundary(conn):
    """Version marker for the first migration managed by this registry."""
    conn.execute(
        "INSERT OR REPLACE INTO db_meta(key, value) VALUES (?, ?)",
        ("schema_manager", "registry-v1"),
    )


def _validate_startup_boundary(conn):
    row = conn.execute(
        "SELECT value FROM db_meta WHERE key='schema_manager'"
    ).fetchone()
    if row is None or row[0] != "registry-v1":
        raise RuntimeError("schema migration validation failed: registry marker missing")


_LEGACY_DEFAULT_TABS = [
    "import", "browse", "pipeline", "pipeline_review",
    "review", "cull", "jobs", "highlights", "misses", "storage", "settings",
]
_PRIMARY_WORKFLOW_TABS = ["import", "pipeline", "review", "browse"]


def _consolidate_default_navigation(conn):
    """Simplify only the untouched legacy default; preserve custom tab sets."""
    import json

    rows = conn.execute("SELECT id, tabs FROM workspaces").fetchall()
    changed_ids = []
    for workspace_id, raw_tabs in rows:
        try:
            tabs = json.loads(raw_tabs) if raw_tabs else None
        except (TypeError, ValueError):
            continue
        if tabs == _LEGACY_DEFAULT_TABS:
            conn.execute(
                "UPDATE workspaces SET tabs=? WHERE id=?",
                (json.dumps(_PRIMARY_WORKFLOW_TABS), workspace_id),
            )
            changed_ids.append(workspace_id)
    if changed_ids:
        conn.execute(
            "INSERT OR REPLACE INTO db_meta(key, value) VALUES (?, ?)",
            ("navigation_consolidated", "1"),
        )
        # Record exactly which workspaces this migration rewrote so the
        # reversal in migration 7 can restore only those rows and leave
        # any workspace the user later customized to the same tab set alone.
        conn.execute(
            "INSERT OR REPLACE INTO db_meta(key, value) VALUES (?, ?)",
            ("navigation_consolidated_ids", json.dumps(changed_ids)),
        )


def _restore_direct_default_navigation(conn):
    """Restore the direct tabs changed by migration 6; preserve custom sets."""
    import json

    marker = conn.execute(
        "SELECT value FROM db_meta WHERE key='navigation_consolidated'"
    ).fetchone()
    if marker is None or marker[0] != "1":
        return

    tracked_row = conn.execute(
        "SELECT value FROM db_meta WHERE key='navigation_consolidated_ids'"
    ).fetchone()
    tracked_ids = None
    if tracked_row is not None:
        try:
            tracked_ids = {int(x) for x in json.loads(tracked_row[0])}
        except (TypeError, ValueError):
            tracked_ids = None

    rows = conn.execute("SELECT id, tabs FROM workspaces").fetchall()
    for workspace_id, raw_tabs in rows:
        # When the consolidation migration recorded which rows it changed,
        # only revert those specific workspaces. Workspaces the user later
        # customized to the compact tab set (or that already matched it
        # before v6 ran) were never touched by v6 and must be preserved.
        if tracked_ids is not None and workspace_id not in tracked_ids:
            continue
        try:
            tabs = json.loads(raw_tabs) if raw_tabs else None
        except (TypeError, ValueError):
            continue
        if tabs == _PRIMARY_WORKFLOW_TABS:
            conn.execute(
                "UPDATE workspaces SET tabs=? WHERE id=?",
                (json.dumps(_LEGACY_DEFAULT_TABS), workspace_id),
            )
    conn.execute(
        "INSERT OR REPLACE INTO db_meta(key, value) VALUES (?, ?)",
        ("navigation_consolidated", "0"),
    )
    conn.execute(
        "DELETE FROM db_meta WHERE key='navigation_consolidated_ids'"
    )


_LEGACY_DETECTOR_MODEL = "MegaDetector"
_CANONICAL_DETECTOR_MODEL = "megadetector-v6"


def _merge_review_winning_column_sql(column):
    """Return the CASE expression picking ``column`` from the winning review row.

    Two prediction_review rows collide when both the legacy and canonical
    prediction rows carry a review in the same workspace. The winner is:
      1. The non-pending row when the other is pending; otherwise
      2. The row with the later ``reviewed_at`` (NULLs treated as oldest);
      3. Ties keep the pre-existing row.
    Applying the same expression to every merged column keeps status,
    reviewed_at, individual, group_id, and the vote counts sourced from the
    same row — otherwise a rejected status from one row could be stored
    alongside the accepted-group metadata from the other, and grouped
    accepts later use group_id/individual to retag other photos.
    """
    return f"""
        CASE
          WHEN prediction_review.status = 'pending'
               AND excluded.status <> 'pending'
            THEN excluded.{column}
          WHEN prediction_review.status <> 'pending'
               AND excluded.status = 'pending'
            THEN prediction_review.{column}
          WHEN COALESCE(excluded.reviewed_at, '')
               > COALESCE(prediction_review.reviewed_at, '')
            THEN excluded.{column}
          ELSE prediction_review.{column}
        END
    """


def _merge_null_species_predictions(conn):
    """Explicit merge path for classifier predictions with ``species IS NULL``.

    The main INSERT relies on the ``UNIQUE(detection_id, classifier_model,
    labels_fingerprint, species)`` constraint to drive its ON CONFLICT UPSERT,
    but SQLite treats NULLs as distinct in UNIQUE constraints — so two rows
    with matching non-null columns and ``species = NULL`` do not collide.
    Without this pass every legacy NULL-species prediction would be inserted
    as a fresh row on the survivor detection, leaving a duplicate that
    ``legacy_prediction_merge`` never maps to; that duplicate would then
    surface as an extra cached prediction (with a stale higher confidence
    than the merged winner).

    For each legacy NULL-species prediction: if the survivor already has a
    matching (classifier_model, labels_fingerprint, species=NULL) row, merge
    into it using the same rules as the main UPSERT (winning confidence,
    COALESCEd taxonomy, earliest created_at). Otherwise insert onto the
    survivor. Subsequent losers targeting the same survivor row fall back
    into the merge branch on the freshly inserted row.
    """
    null_losers = conn.execute(
        """
        SELECT m.survivor_id, p.classifier_model, p.labels_fingerprint,
               p.confidence, p.category, p.scientific_name,
               p.taxonomy_kingdom, p.taxonomy_phylum, p.taxonomy_class,
               p.taxonomy_order, p.taxonomy_family, p.taxonomy_genus,
               p.created_at
        FROM predictions p
        JOIN legacy_detection_merge m ON m.old_id = p.detection_id
        WHERE p.species IS NULL
        """
    ).fetchall()

    def _pick_higher_confidence(cur, new):
        if cur is None:
            return new
        if new is None:
            return cur
        return max(cur, new)

    def _pick_earlier(cur, new):
        if cur is None:
            return new
        if new is None:
            return cur
        return min(cur, new)

    for row in null_losers:
        (survivor_id, classifier_model, labels_fingerprint, loser_confidence,
         loser_category, loser_sci, loser_king, loser_phyl, loser_cls,
         loser_ord, loser_fam, loser_gen, loser_created) = row

        existing = conn.execute(
            """
            SELECT id, confidence, created_at
            FROM predictions
            WHERE detection_id = ?
              AND classifier_model = ?
              AND labels_fingerprint = ?
              AND species IS NULL
            """,
            (survivor_id, classifier_model, labels_fingerprint),
        ).fetchone()

        if existing is None:
            conn.execute(
                """
                INSERT INTO predictions (
                  detection_id, classifier_model, labels_fingerprint, species,
                  confidence, category, scientific_name, taxonomy_kingdom,
                  taxonomy_phylum, taxonomy_class, taxonomy_order,
                  taxonomy_family, taxonomy_genus, created_at
                ) VALUES (?, ?, ?, NULL, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (survivor_id, classifier_model, labels_fingerprint,
                 loser_confidence, loser_category, loser_sci,
                 loser_king, loser_phyl, loser_cls, loser_ord, loser_fam,
                 loser_gen, loser_created),
            )
            continue

        existing_id, existing_confidence, existing_created = existing
        conn.execute(
            """
            UPDATE predictions
            SET confidence = ?,
                category = COALESCE(category, ?),
                scientific_name = COALESCE(scientific_name, ?),
                taxonomy_kingdom = COALESCE(taxonomy_kingdom, ?),
                taxonomy_phylum = COALESCE(taxonomy_phylum, ?),
                taxonomy_class = COALESCE(taxonomy_class, ?),
                taxonomy_order = COALESCE(taxonomy_order, ?),
                taxonomy_family = COALESCE(taxonomy_family, ?),
                taxonomy_genus = COALESCE(taxonomy_genus, ?),
                created_at = ?
            WHERE id = ?
            """,
            (
                _pick_higher_confidence(existing_confidence, loser_confidence),
                loser_category, loser_sci,
                loser_king, loser_phyl, loser_cls,
                loser_ord, loser_fam, loser_gen,
                _pick_earlier(existing_created, loser_created),
                existing_id,
            ),
        )


def _merge_legacy_detector_alias(conn):
    """Consolidate pre-versioned MegaDetector cache rows without losing review data.

    Older builds stored MegaDetector V6 rows under ``MegaDetector``. A short-lived
    startup migration renamed that key to ``megadetector-v6``, but catalogs that
    skipped that build could later accumulate a second, canonical copy of every
    box. Multi-subject Compare then rendered the two cache rows as two subjects.

    Collapse boxes on the detector cache's rounded natural key and make the normal
    canonical content-addressed id their survivor. Predictions, classifier-run
    markers, review state, and edit-history prediction references are moved to the
    survivor before duplicate detections are deleted.
    """
    # Masks persist the detector key alongside the prompt geometry. Keep that
    # denormalized key aligned with the detection rows so a valid mask does not
    # become stale merely because this migration canonicalized its model name.
    conn.execute(
        "UPDATE photo_masks SET detector_model = ? WHERE detector_model = ?",
        (_CANONICAL_DETECTOR_MODEL, _LEGACY_DETECTOR_MODEL),
    )

    # Load every detection row that carries either alias in one pass so we can
    # detect not just literal legacy rows but also canonical rows kept from the
    # short-lived unversioned rename: those already carry the current model name
    # yet keep their old rowid instead of the content-addressed one. Skipping
    # them here would let the next detector rerun UPSERT under the correct id
    # and delete the pre-existing canonical row, cascading its predictions and
    # reviews.
    from detection_id import detection_id as _detection_id

    rows = conn.execute(
        """
        SELECT id, photo_id, detector_model, box_x, box_y, box_w, box_h,
               detector_confidence, category, created_at
        FROM detections
        WHERE detector_model IN (?, ?)
        """,
        (_LEGACY_DETECTOR_MODEL, _CANONICAL_DETECTOR_MODEL),
    ).fetchall()

    def _expected_content_id(row):
        """Return the content-addressed id ``row`` should have, or ``None``.

        Historical fixtures with a NULL category or coordinate cannot be
        content-addressed, so the caller falls back to preserving the raw
        rowid for those rows.
        """
        coords = (row["box_x"], row["box_y"], row["box_w"], row["box_h"])
        if row["category"] is None or any(value is None for value in coords):
            return None
        return _detection_id(
            row["photo_id"], _CANONICAL_DETECTOR_MODEL, coords, row["category"],
        )

    def _is_stale_canonical(row):
        """True if ``row`` is a canonical alias whose id needs to be re-keyed."""
        if row["detector_model"] != _CANONICAL_DETECTOR_MODEL:
            return False
        expected = _expected_content_id(row)
        return expected is not None and expected != row["id"]

    has_legacy = any(
        row["detector_model"] == _LEGACY_DETECTOR_MODEL for row in rows
    )
    has_stale_canonical = any(_is_stale_canonical(row) for row in rows)

    if not has_legacy and not has_stale_canonical:
        # Zero-box detector runs can carry the old key even when no detection row
        # exists, so normalize them on the warm/empty path too.
        conn.execute(
            """
            INSERT INTO detector_runs
                (photo_id, detector_model, run_at, box_count)
            SELECT photo_id, ?, run_at, box_count
            FROM detector_runs
            WHERE detector_model = ?
            ON CONFLICT(photo_id, detector_model) DO UPDATE SET
              run_at = CASE
                WHEN detector_runs.run_at IS NULL THEN excluded.run_at
                WHEN excluded.run_at IS NULL THEN detector_runs.run_at
                WHEN excluded.run_at > detector_runs.run_at THEN excluded.run_at
                ELSE detector_runs.run_at
              END,
              box_count = MAX(detector_runs.box_count, excluded.box_count)
            """,
            (_CANONICAL_DETECTOR_MODEL, _LEGACY_DETECTOR_MODEL),
        )
        conn.execute(
            "DELETE FROM detector_runs WHERE detector_model = ?",
            (_LEGACY_DETECTOR_MODEL,),
        )
        return

    legacy_count = sum(
        1 for row in rows if row["detector_model"] == _LEGACY_DETECTOR_MODEL
    )

    conn.execute("DROP TABLE IF EXISTS temp.legacy_detector_groups")
    conn.execute("DROP TABLE IF EXISTS temp.legacy_detection_merge")
    conn.execute("DROP TABLE IF EXISTS temp.legacy_prediction_merge")

    # Build groups with the same four-decimal box identity used by normal detector
    # writes. The canonical content-addressed id is the survivor even when a box
    # exists only under the legacy key, so a later detector rerun will UPSERT that
    # row instead of deleting it (and cascading its predictions/reviews).

    groups = {}
    for row in rows:
        coords = (row["box_x"], row["box_y"], row["box_w"], row["box_h"])
        if row["category"] is None or any(value is None for value in coords):
            # Defensive support for malformed historical fixtures. Real detector
            # boxes always have a category and four coordinates; keep NULL-bearing
            # rows distinct because they cannot have a content-addressed id.
            key = ("raw", row["photo_id"], *coords, row["category"])
        else:
            qbox = tuple(f"{round(value, 4):.4f}" for value in coords)
            key = ("quantized", row["photo_id"], *qbox, row["category"])
        groups.setdefault(key, []).append(row)

    conn.execute(
        """
        CREATE TEMP TABLE legacy_detector_groups (
          survivor_id INTEGER PRIMARY KEY,
          photo_id INTEGER NOT NULL,
          max_confidence REAL,
          first_created TEXT
        )
        """
    )
    conn.execute(
        """
        CREATE TEMP TABLE legacy_detection_merge (
          old_id INTEGER PRIMARY KEY,
          survivor_id INTEGER NOT NULL
        )
        """
    )

    # find_stale_masks and count_extract_stale require exact equality between
    # photo_masks.prompt_xywh and a detection row's box_xywh. When two aliased
    # detections differ by sub-quantization drift (e.g. 0.10001 vs 0.10002),
    # a mask created from the loser box would suddenly fail that equality
    # against the surviving canonical row and be flagged stale. Track the
    # (photo, loser_coords) -> survivor_coords remaps so we can realign the
    # mask prompt in the same transaction.
    mask_prompt_remaps: list[tuple[int, tuple, tuple]] = []

    for group_rows in groups.values():
        group_has_legacy = any(
            row["detector_model"] == _LEGACY_DETECTOR_MODEL
            for row in group_rows
        )
        group_has_stale_canonical = any(
            _is_stale_canonical(row) for row in group_rows
        )
        # Legacy-touched groups always run through the merge to canonicalize the
        # model name. Canonical-only groups only need attention when at least one
        # row keeps a non-content-addressed id from a pre-versioned rename —
        # otherwise the group is already in its final shape and processing it
        # would be a no-op (survivor_id == source.id, no losers to merge).
        if not group_has_legacy and not group_has_stale_canonical:
            continue
        canonical_rows = [
            row for row in group_rows
            if row["detector_model"] == _CANONICAL_DETECTOR_MODEL
        ]
        source = min(canonical_rows or group_rows, key=lambda row: row["id"])
        coords = (
            source["box_x"], source["box_y"],
            source["box_w"], source["box_h"],
        )
        if source["category"] is None or any(value is None for value in coords):
            survivor_id = source["id"]
        else:
            survivor_id = _detection_id(
                source["photo_id"], _CANONICAL_DETECTOR_MODEL,
                coords, source["category"],
            )

        group_ids = {row["id"] for row in group_rows}
        occupant = conn.execute(
            "SELECT id FROM detections WHERE id = ?", (survivor_id,),
        ).fetchone()
        if occupant is not None and occupant["id"] not in group_ids:
            raise RuntimeError(
                f"canonical detection id collision while migrating {survivor_id}"
            )

        # When an existing content-addressed row already occupies survivor_id, it
        # is the row that will remain in the table after the merge — the INSERT
        # below is skipped, so its raw box coordinates are what masks must be
        # realigned to. Otherwise the retained row is the freshly inserted one
        # using `source`'s coordinates.
        if occupant is not None:
            survivor_row = next(
                row for row in group_rows if row["id"] == occupant["id"]
            )
        else:
            survivor_row = source
        survivor_coords = (
            survivor_row["box_x"], survivor_row["box_y"],
            survivor_row["box_w"], survivor_row["box_h"],
        )

        confidences = [
            row["detector_confidence"] for row in group_rows
            if row["detector_confidence"] is not None
        ]
        created_values = [
            row["created_at"] for row in group_rows
            if row["created_at"] is not None
        ]
        max_confidence = max(confidences) if confidences else None
        first_created = min(created_values) if created_values else None
        if occupant is None:
            conn.execute(
                """
                INSERT INTO detections (
                  id, photo_id, detector_model, box_x, box_y, box_w, box_h,
                  detector_confidence, category, created_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    survivor_id, source["photo_id"], _CANONICAL_DETECTOR_MODEL,
                    *coords, max_confidence, source["category"], first_created,
                ),
            )
        conn.execute(
            "INSERT INTO legacy_detector_groups VALUES (?, ?, ?, ?)",
            (survivor_id, source["photo_id"], max_confidence, first_created),
        )
        conn.executemany(
            "INSERT INTO legacy_detection_merge VALUES (?, ?)",
            [
                (row["id"], survivor_id) for row in group_rows
                if row["id"] != survivor_id
            ],
        )

        # Plan mask prompt remaps for every loser box whose exact coordinates
        # differ from the survivor's. Skip rows with any NULL coordinate — a
        # mask cannot exactly-match a NULL prompt column, so there is nothing
        # to realign.
        if any(value is None for value in survivor_coords):
            continue
        for row in group_rows:
            loser_coords = (
                row["box_x"], row["box_y"], row["box_w"], row["box_h"],
            )
            if any(value is None for value in loser_coords):
                continue
            if loser_coords == survivor_coords:
                continue
            mask_prompt_remaps.append(
                (source["photo_id"], loser_coords, survivor_coords),
            )

    # Keep the strongest cached confidence and oldest creation timestamp on the
    # survivor. In the normal duplicate case these values are already identical.
    conn.execute(
        """
        UPDATE detections
        SET detector_confidence = (
              SELECT max_confidence FROM legacy_detector_groups
              WHERE survivor_id = detections.id
            ),
            created_at = COALESCE(created_at, (
              SELECT first_created FROM legacy_detector_groups
              WHERE survivor_id = detections.id
            ))
        WHERE id IN (SELECT survivor_id FROM legacy_detector_groups)
        """
    )

    # Realign mask prompts stored under a loser box to the survivor's exact
    # coordinates so find_stale_masks and count_extract_stale keep matching
    # after the alias merge. Constrain to MegaDetector-family masks (already
    # canonicalized by the rename above): another detector's mask can
    # coincidentally share the loser's prompt coordinates, and rewriting it
    # would break equality against its own detection row.
    #
    # find_stale_masks picks a single "primary" detection per photo — the
    # highest-confidence non-full-image row, tie-broken by smallest id — and
    # only that row's coordinates keep the mask fresh. Skip the remap only
    # when that primary IS a retained megadetector-v6 row still sitting at
    # the loser's exact coordinates (for example the same box under a
    # different category, outside this alias group and higher-confidence than
    # the merged survivor). In every other case — including a retained row at
    # the loser coords that is lower-confidence than the survivor — the
    # primary sits elsewhere; leaving the mask at the loser coords would
    # immediately flag it stale, so remap to the survivor coords instead.
    for photo_id, loser_coords, survivor_coords in mask_prompt_remaps:
        primary = conn.execute(
            """
            SELECT detector_model, box_x, box_y, box_w, box_h
            FROM detections
            WHERE photo_id = ?
              AND detector_model != 'full-image'
              AND id NOT IN (SELECT old_id FROM legacy_detection_merge)
            ORDER BY detector_confidence DESC, id ASC
            LIMIT 1
            """,
            (photo_id,),
        ).fetchone()
        primary_matches_loser = (
            primary is not None
            and primary["detector_model"] == _CANONICAL_DETECTOR_MODEL
            and (
                primary["box_x"], primary["box_y"],
                primary["box_w"], primary["box_h"],
            ) == loser_coords
        )
        if primary_matches_loser:
            continue
        conn.execute(
            """
            UPDATE photo_masks
               SET prompt_x = ?, prompt_y = ?, prompt_w = ?, prompt_h = ?
             WHERE photo_id = ?
               AND detector_model = ?
               AND prompt_x = ? AND prompt_y = ?
               AND prompt_w = ? AND prompt_h = ?
            """,
            (*survivor_coords, photo_id, _CANONICAL_DETECTOR_MODEL, *loser_coords),
        )

    # Copy loser predictions onto their survivor detection. The identity UNIQUE
    # makes this both an insert for legacy-only information and a merge for output
    # already regenerated under the canonical detection id.
    #
    # NULL species is handled separately below: SQLite's UNIQUE constraint treats
    # NULLs as distinct, so ON CONFLICT would never fire for species=NULL rows and
    # a stray duplicate would be inserted on the survivor. That duplicate would
    # not receive a `legacy_prediction_merge` entry pointing at it, so it would
    # linger as an unmapped extra row and could surface as the top cached
    # prediction (with a stale/higher confidence than the merged winner).
    conn.execute(
        """
        INSERT INTO predictions (
          detection_id, classifier_model, labels_fingerprint, species,
          confidence, category, scientific_name, taxonomy_kingdom,
          taxonomy_phylum, taxonomy_class, taxonomy_order, taxonomy_family,
          taxonomy_genus, created_at
        )
        SELECT m.survivor_id, p.classifier_model, p.labels_fingerprint,
               p.species, p.confidence, p.category, p.scientific_name,
               p.taxonomy_kingdom, p.taxonomy_phylum, p.taxonomy_class,
               p.taxonomy_order, p.taxonomy_family, p.taxonomy_genus,
               p.created_at
        FROM predictions p
        JOIN legacy_detection_merge m ON m.old_id = p.detection_id
        WHERE p.species IS NOT NULL
        ON CONFLICT(detection_id, classifier_model, labels_fingerprint, species)
        DO UPDATE SET
          confidence = CASE
            WHEN predictions.confidence IS NULL THEN excluded.confidence
            WHEN excluded.confidence IS NULL THEN predictions.confidence
            WHEN excluded.confidence > predictions.confidence THEN excluded.confidence
            ELSE predictions.confidence
          END,
          category = COALESCE(predictions.category, excluded.category),
          scientific_name = COALESCE(predictions.scientific_name, excluded.scientific_name),
          taxonomy_kingdom = COALESCE(predictions.taxonomy_kingdom, excluded.taxonomy_kingdom),
          taxonomy_phylum = COALESCE(predictions.taxonomy_phylum, excluded.taxonomy_phylum),
          taxonomy_class = COALESCE(predictions.taxonomy_class, excluded.taxonomy_class),
          taxonomy_order = COALESCE(predictions.taxonomy_order, excluded.taxonomy_order),
          taxonomy_family = COALESCE(predictions.taxonomy_family, excluded.taxonomy_family),
          taxonomy_genus = COALESCE(predictions.taxonomy_genus, excluded.taxonomy_genus),
          created_at = CASE
            WHEN predictions.created_at IS NULL THEN excluded.created_at
            WHEN excluded.created_at IS NULL THEN predictions.created_at
            WHEN excluded.created_at < predictions.created_at THEN excluded.created_at
            ELSE predictions.created_at
          END
        """
    )

    _merge_null_species_predictions(conn)

    # Map every soon-to-be-deleted prediction id to the prediction that now owns
    # its identity on the survivor. This drives both review-state and undo-history
    # repair before the loser rows cascade away.
    conn.execute(
        """
        CREATE TEMP TABLE legacy_prediction_merge AS
        SELECT old_p.id AS old_id, MIN(new_p.id) AS survivor_id
        FROM predictions old_p
        JOIN legacy_detection_merge dm ON dm.old_id = old_p.detection_id
        JOIN predictions new_p
          ON new_p.detection_id = dm.survivor_id
         AND new_p.classifier_model = old_p.classifier_model
         AND new_p.labels_fingerprint = old_p.labels_fingerprint
         AND new_p.species IS old_p.species
        GROUP BY old_p.id
        """
    )
    conn.execute("CREATE UNIQUE INDEX temp.idx_legacy_prediction_merge_old ON legacy_prediction_merge(old_id)")

    conn.execute(
        f"""
        INSERT INTO prediction_review (
          prediction_id, workspace_id, status, reviewed_at,
          individual, group_id, vote_count, total_votes
        )
        SELECT pm.survivor_id, r.workspace_id, r.status, r.reviewed_at,
               r.individual, r.group_id, r.vote_count, r.total_votes
        FROM prediction_review r
        JOIN legacy_prediction_merge pm ON pm.old_id = r.prediction_id
        WHERE 1
        ON CONFLICT(prediction_id, workspace_id) DO UPDATE SET
          status = {_merge_review_winning_column_sql('status')},
          reviewed_at = {_merge_review_winning_column_sql('reviewed_at')},
          individual = {_merge_review_winning_column_sql('individual')},
          group_id = {_merge_review_winning_column_sql('group_id')},
          vote_count = {_merge_review_winning_column_sql('vote_count')},
          total_votes = {_merge_review_winning_column_sql('total_votes')}
        """
    )

    # prediction_accept history stores a bare prediction id in old_value. Newer
    # history payloads can store it in JSON; update both forms so undo/redo keeps
    # addressing the surviving prediction after duplicate rows are removed.
    conn.execute(
        """
        UPDATE edit_history_items
        SET old_value = (
          SELECT CAST(pm.survivor_id AS TEXT)
          FROM legacy_prediction_merge pm
          WHERE CAST(pm.old_id AS TEXT) = edit_history_items.old_value
        )
        WHERE edit_id IN (
          SELECT id FROM edit_history WHERE action_type = 'prediction_accept'
        )
          AND old_value IN (
            SELECT CAST(old_id AS TEXT) FROM legacy_prediction_merge
          )
        """
    )
    prediction_id_map = dict(conn.execute("SELECT old_id, survivor_id FROM legacy_prediction_merge").fetchall())
    json_history = conn.execute(
        "SELECT id, old_value FROM edit_history_items WHERE ltrim(old_value) LIKE '{%'"
    ).fetchall()
    for item_id, raw_value in json_history:
        try:
            payload = json.loads(raw_value)
        except (TypeError, json.JSONDecodeError):
            continue
        if not isinstance(payload, dict):
            continue

        changed = False
        try:
            old_prediction_id = int(payload.get("prediction_id"))
        except (TypeError, ValueError):
            pass
        else:
            survivor_id = prediction_id_map.get(old_prediction_id)
            if survivor_id is not None:
                payload["prediction_id"] = survivor_id
                changed = True

        prediction_ids = payload.get("prediction_ids")
        if isinstance(prediction_ids, list):
            remapped_ids = []
            for value in prediction_ids:
                try:
                    prediction_id = int(value)
                except (TypeError, ValueError):
                    remapped_ids.append(value)
                    continue
                survivor_id = prediction_id_map.get(prediction_id)
                remapped_ids.append(survivor_id if survivor_id is not None else value)
                changed = changed or survivor_id is not None
            payload["prediction_ids"] = remapped_ids

        if not changed:
            continue
        conn.execute(
            "UPDATE edit_history_items SET old_value = ? WHERE id = ?",
            (json.dumps(payload, separators=(",", ":")), item_id),
        )

    # Preserve classifier completion markers and prefer the later run time / larger
    # recorded output count when both aliases already have the same run identity.
    conn.execute(
        """
        INSERT INTO classifier_runs (
          detection_id, classifier_model, labels_fingerprint,
          run_at, prediction_count
        )
        SELECT dm.survivor_id, cr.classifier_model, cr.labels_fingerprint,
               cr.run_at, cr.prediction_count
        FROM classifier_runs cr
        JOIN legacy_detection_merge dm ON dm.old_id = cr.detection_id
        WHERE 1
        ON CONFLICT(detection_id, classifier_model, labels_fingerprint)
        DO UPDATE SET
          run_at = CASE
            WHEN classifier_runs.run_at IS NULL THEN excluded.run_at
            WHEN excluded.run_at IS NULL THEN classifier_runs.run_at
            WHEN excluded.run_at > classifier_runs.run_at THEN excluded.run_at
            ELSE classifier_runs.run_at
          END,
          prediction_count = MAX(
            classifier_runs.prediction_count, excluded.prediction_count
          )
        """
    )

    merged_detection_count = conn.execute("SELECT COUNT(*) FROM legacy_detection_merge").fetchone()[0]
    merged_prediction_count = conn.execute("SELECT COUNT(*) FROM legacy_prediction_merge").fetchone()[0]

    # Deleting loser detections now safely cascades only records already copied to
    # their survivor. Normalize every retained legacy-only survivor afterward.
    conn.execute("DELETE FROM detections WHERE id IN (SELECT old_id FROM legacy_detection_merge)")
    conn.execute(
        "UPDATE detections SET detector_model = ? WHERE detector_model = ?",
        (_CANONICAL_DETECTOR_MODEL, _LEGACY_DETECTOR_MODEL),
    )

    # Merge zero-box and populated detector-run aliases, then make box_count agree
    # with the repaired cache for every photo touched by legacy detections.
    conn.execute(
        """
        INSERT INTO detector_runs (photo_id, detector_model, run_at, box_count)
        SELECT photo_id, ?, run_at, box_count
        FROM detector_runs
        WHERE detector_model = ?
        ON CONFLICT(photo_id, detector_model) DO UPDATE SET
          run_at = CASE
            WHEN detector_runs.run_at IS NULL THEN excluded.run_at
            WHEN excluded.run_at IS NULL THEN detector_runs.run_at
            WHEN excluded.run_at > detector_runs.run_at THEN excluded.run_at
            ELSE detector_runs.run_at
          END,
          box_count = MAX(detector_runs.box_count, excluded.box_count)
        """,
        (_CANONICAL_DETECTOR_MODEL, _LEGACY_DETECTOR_MODEL),
    )
    conn.execute(
        "DELETE FROM detector_runs WHERE detector_model = ?",
        (_LEGACY_DETECTOR_MODEL,),
    )
    conn.execute(
        """
        INSERT INTO detector_runs (photo_id, detector_model, run_at, box_count)
        SELECT g.photo_id, 'megadetector-v6', MIN(d.created_at), COUNT(d.id)
        FROM (SELECT DISTINCT photo_id FROM legacy_detector_groups) g
        JOIN detections d
          ON d.photo_id = g.photo_id
         AND d.detector_model = 'megadetector-v6'
        GROUP BY g.photo_id
        ON CONFLICT(photo_id, detector_model) DO UPDATE SET
          box_count = excluded.box_count,
          run_at = COALESCE(detector_runs.run_at, excluded.run_at)
        """
    )

    # Recompute counts after prediction identities have been folded together.
    conn.execute(
        """
        UPDATE classifier_runs
        SET prediction_count = (
          SELECT COUNT(*) FROM predictions p
          WHERE p.detection_id = classifier_runs.detection_id
            AND p.classifier_model = classifier_runs.classifier_model
            AND p.labels_fingerprint = classifier_runs.labels_fingerprint
        )
        WHERE detection_id IN (
          SELECT survivor_id FROM legacy_detector_groups
        )
        """
    )

    conn.execute(
        "INSERT OR REPLACE INTO db_meta(key, value) VALUES (?, ?)",
        (
            "legacy_megadetector_alias_repair",
            json.dumps(
                {
                    "legacy_detections": legacy_count,
                    "merged_detections": merged_detection_count,
                    "remapped_predictions": merged_prediction_count,
                },
                sort_keys=True,
            ),
        ),
    )


def _validate_legacy_detector_alias_merge(conn):
    legacy_detections = conn.execute(
        "SELECT COUNT(*) FROM detections WHERE detector_model = ?",
        (_LEGACY_DETECTOR_MODEL,),
    ).fetchone()[0]
    legacy_runs = conn.execute(
        "SELECT COUNT(*) FROM detector_runs WHERE detector_model = ?",
        (_LEGACY_DETECTOR_MODEL,),
    ).fetchone()[0]
    if legacy_detections or legacy_runs:
        raise RuntimeError("schema migration validation failed: legacy MegaDetector aliases remain")
    fk_errors = conn.execute("PRAGMA foreign_key_check").fetchall()
    if fk_errors:
        raise RuntimeError("schema migration validation failed: detector repair broke foreign keys")


MIGRATIONS = (
    Migration(
        version=5,
        name="establish-versioned-schema-manager",
        apply=_establish_startup_boundary,
        validate=_validate_startup_boundary,
    ),
    Migration(
        version=6,
        name="consolidate-untouched-default-navigation",
        apply=_consolidate_default_navigation,
    ),
    Migration(
        version=7,
        name="restore-direct-default-navigation",
        apply=_restore_direct_default_navigation,
    ),
    Migration(
        version=8,
        name="merge-legacy-megadetector-alias",
        apply=_merge_legacy_detector_alias,
        validate=_validate_legacy_detector_alias_merge,
    ),
)


def _apply_pending(conn):
    current = conn.execute("PRAGMA user_version").fetchone()[0]
    latest = MIGRATIONS[-1].version if MIGRATIONS else current
    if current > latest:
        raise RuntimeError(f"database schema version {current} is newer than supported {latest}")

    for migration in MIGRATIONS:
        if migration.version <= current:
            continue
        conn.execute("BEGIN IMMEDIATE")
        try:
            migration.apply(conn)
            if migration.validate is not None:
                migration.validate(conn)
            conn.execute(f"PRAGMA user_version = {migration.version}")
        except BaseException:
            conn.rollback()
            raise
        else:
            conn.commit()
        current = migration.version


def ensure_schema(db_path):
    """Initialize and migrate ``db_path`` once before request handling."""
    with _SCHEMA_LOCK, Database(db_path) as db:
        _apply_pending(db.conn)
