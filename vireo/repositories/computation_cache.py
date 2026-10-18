"""Catalog SQL behind the portable computation cache (``computation_cache.py``).

``computation_cache`` turns fully identified detector and classifier runs into
content-addressed artifacts (export, and the per-run publish after a fresh
classification) and materializes trusted artifacts back onto matching catalog
rows (import). This repository holds the catalog reads and writes those two
directions run, with their SQL unchanged from the module:

- export: the portable detector and classifier runs, a run's detection boxes,
  classified detections and stored candidates, the label-set metadata, the
  run-level match summary and the Settings-page counts;
- publish: a detection's photo identity and the portable-identity stamp on a
  fresh run's ``classifier_runs`` and ``predictions`` rows;
- materialize: the rows an artifact's original-only identity matches, the
  runtime a catalog already has installed, the label-set collision check, the
  manual-review pin, and the replacement of a subject's predictions, match
  summary and ``classifier_runs`` marker.

The artifact logic (validation, digests, which candidate wins, what counts as
recognized) stays in ``computation_cache``. Nothing here commits: every write
runs in the caller's transaction, which commits or rolls back through
``db.commit()`` / ``db.rollback()`` exactly where the module always did.
Every table involved is catalog-wide, so nothing is scoped to a workspace.

Callers reach it as ``db.computation_cache`` (a fresh repository per access,
see ``Database.computation_cache``); there are no forwarding wrappers on
``Database``.
"""

import sqlite3
from collections.abc import Mapping
from typing import Any


class ComputationCacheRepository:
    def __init__(self, conn: sqlite3.Connection) -> None:
        self.conn = conn

    # -- export and publish reads ---------------------------------------------

    def detection_runtime(self, detection_id: int) -> sqlite3.Row | None:
        """The detection's ``runtime_fingerprint`` row, or None without a detection."""
        return self.conn.execute(
            "SELECT runtime_fingerprint FROM detections WHERE id = ?",
            (detection_id,),
        ).fetchone()

    def match_summary(
        self, detection_id: int, classifier_model: str, labels_fingerprint: str,
    ) -> sqlite3.Row | None:
        """The ``classifier_match_scores`` row of one run, or None."""
        return self.conn.execute(
            """SELECT max_match_score, match_margin, top_species,
                  label_count, score_kind
           FROM classifier_match_scores
           WHERE detection_id = ? AND classifier_model = ?
             AND labels_fingerprint = ?""",
            (detection_id, classifier_model, labels_fingerprint),
        ).fetchone()

    def promotable_detection(self, detection_id: int) -> sqlite3.Row | None:
        """A detection with the photo columns its portable identity derives from."""
        return self.conn.execute(
            """SELECT d.photo_id, d.detector_model, d.runtime_fingerprint,
                  d.box_x, d.box_y, d.box_w, d.box_h, d.category,
                  p.file_hash, p.companion_path, p.working_copy_path
           FROM detections d
           JOIN photos p ON p.id = d.photo_id
           WHERE d.id = ?""",
            (detection_id,),
        ).fetchone()

    def run_predictions(
        self, detection_id: int, classifier_model: str, labels_fingerprint: str,
    ) -> list[sqlite3.Row]:
        """One run's stored candidates, best first, with their taxonomy columns."""
        return self.conn.execute(
            """SELECT species, confidence, scientific_name, source_taxon_id,
                  taxonomy_kingdom, taxonomy_phylum, taxonomy_class,
                  taxonomy_order, taxonomy_family, taxonomy_genus,
                  match_score
           FROM predictions
           WHERE detection_id = ? AND classifier_model = ?
             AND labels_fingerprint = ?
           ORDER BY confidence DESC, species COLLATE NOCASE""",
            (detection_id, classifier_model, labels_fingerprint),
        ).fetchall()

    def labels_metadata(self, labels_fingerprint: str) -> sqlite3.Row | None:
        """``display_name`` and ``label_count`` of a label set, or None."""
        return self.conn.execute(
            """SELECT display_name, label_count FROM labels_fingerprints
           WHERE fingerprint = ?""",
            (labels_fingerprint,),
        ).fetchone()

    def portable_detector_runs(self) -> list[sqlite3.Row]:
        """Every detector run with its photo's hash, in photo then model order."""
        return self.conn.execute(
            """SELECT p.id AS photo_id, p.file_hash, dr.detector_model,
                  dr.runtime_fingerprint, dr.input_fingerprint
           FROM detector_runs dr
           JOIN photos p ON p.id = dr.photo_id
           ORDER BY p.id, dr.detector_model"""
        ).fetchall()

    def run_detection_boxes(
        self, photo_id: int, detector_model: str, runtime_fingerprint: str,
    ) -> list[sqlite3.Row]:
        """The boxes one detector run produced, most confident first."""
        return self.conn.execute(
            """SELECT box_x, box_y, box_w, box_h,
                      detector_confidence, category
               FROM detections
               WHERE photo_id = ? AND detector_model = ?
                 AND runtime_fingerprint = ?
               ORDER BY detector_confidence DESC, id ASC""",
            (photo_id, detector_model, runtime_fingerprint),
        ).fetchall()

    def portable_classifier_runs(self) -> list[sqlite3.Row]:
        """Every classifier run with its detection and photo identity columns."""
        return self.conn.execute(
            """SELECT DISTINCT
                  p.id AS photo_id, p.file_hash,
                  d.detector_model, d.runtime_fingerprint AS detector_runtime,
                  cr.classifier_model, cr.labels_fingerprint,
                  cr.labels_fingerprint_full, cr.runtime_fingerprint,
                  cr.input_fingerprint, cr.input_recipe
           FROM classifier_runs cr
           JOIN detections d ON d.id = cr.detection_id
           JOIN photos p ON p.id = d.photo_id
           ORDER BY p.id, cr.classifier_model, cr.labels_fingerprint"""
        ).fetchall()

    def classified_detections(self, run: Mapping[str, Any]) -> list[sqlite3.Row]:
        """The detections one ``portable_classifier_runs`` row classified, best first."""
        return self.conn.execute(
            """SELECT d.id, d.box_x, d.box_y, d.box_w, d.box_h,
                  d.detector_model, d.category
           FROM detections d
           JOIN classifier_runs cr ON cr.detection_id = d.id
           WHERE d.photo_id = ? AND d.detector_model = ?
             AND d.runtime_fingerprint = ?
             AND cr.classifier_model = ?
             AND cr.labels_fingerprint = ?
             AND cr.labels_fingerprint_full = ?
             AND cr.runtime_fingerprint = ?
             AND cr.input_fingerprint = ?
           ORDER BY d.detector_confidence DESC, d.id ASC""",
            (
                run["photo_id"], run["detector_model"],
                run["detector_runtime"], run["classifier_model"],
                run["labels_fingerprint"],
                run["labels_fingerprint_full"],
                run["runtime_fingerprint"], run["input_fingerprint"],
            ),
        ).fetchall()

    def exportable_detector_counts(self) -> sqlite3.Row:
        """Portable, missing-hash and legacy detector-run counts (SUMs, may be NULL)."""
        return self.conn.execute(
            """SELECT
             SUM(CASE WHEN length(p.file_hash) = 64
                           AND length(dr.runtime_fingerprint) = 64
                           AND length(dr.input_fingerprint) = 64
                      THEN 1 ELSE 0 END) AS portable,
             SUM(CASE WHEN p.file_hash IS NULL OR length(p.file_hash) != 64
                      THEN 1 ELSE 0 END) AS missing_hash,
             SUM(CASE WHEN dr.runtime_fingerprint = 'legacy'
                      THEN 1 ELSE 0 END) AS legacy
           FROM detector_runs dr JOIN photos p ON p.id = dr.photo_id"""
        ).fetchone()

    def exportable_classifier_counts(self) -> sqlite3.Row:
        """Portable, missing-hash and legacy classifier-run counts (SUMs, may be NULL)."""
        return self.conn.execute(
            """SELECT
             SUM(CASE WHEN length(p.file_hash) = 64
                           AND length(cr.runtime_fingerprint) = 64
                           AND length(cr.input_fingerprint) = 64
                           AND length(cr.labels_fingerprint_full) = 64
                           AND length(d.runtime_fingerprint) = 64
                      THEN 1 ELSE 0 END) AS portable,
             SUM(CASE WHEN p.file_hash IS NULL OR length(p.file_hash) != 64
                      THEN 1 ELSE 0 END) AS missing_hash,
             SUM(CASE WHEN cr.runtime_fingerprint = 'legacy'
                           OR cr.labels_fingerprint_full IS NULL
                      THEN 1 ELSE 0 END) AS legacy
           FROM classifier_runs cr
           JOIN detections d ON d.id = cr.detection_id
           JOIN photos p ON p.id = d.photo_id"""
        ).fetchone()

    # -- publish write ----------------------------------------------------------

    def stamp_portable_run_identity(
        self,
        detection_id: int,
        classifier_model: str,
        labels_fingerprint: str,
        labels_fingerprint_full: str,
        runtime_fingerprint: str,
        input_fingerprint: str,
    ) -> None:
        """Write a fresh run's portable identity on its run and prediction rows.

        Does not commit; the caller commits both updates together.
        """
        self.conn.execute(
            """UPDATE classifier_runs
               SET labels_fingerprint_full = ?, runtime_fingerprint = ?,
                   input_fingerprint = ?
               WHERE detection_id = ? AND classifier_model = ?
                 AND labels_fingerprint = ?""",
            (
                labels_fingerprint_full, runtime_fingerprint, input_fingerprint,
                detection_id, classifier_model, labels_fingerprint,
            ),
        )
        self.conn.execute(
            """UPDATE predictions SET labels_fingerprint_full = ?
               WHERE detection_id = ? AND classifier_model = ?
                 AND labels_fingerprint = ?""",
            (
                labels_fingerprint_full, detection_id,
                classifier_model, labels_fingerprint,
            ),
        )

    # -- materialize reads --------------------------------------------------------

    def installed_detector_runtime(
        self, photo_sha256: str, detector_model: str,
    ) -> sqlite3.Row | None:
        """A detector runtime already installed on a photo the artifact matches."""
        return self.conn.execute(
            """SELECT dr.runtime_fingerprint
                   FROM detector_runs dr
                   JOIN photos p ON p.id = dr.photo_id
                   WHERE p.file_hash = ? AND dr.detector_model = ?
                     AND p.companion_path IS NULL
                     AND p.working_copy_path IS NULL
                     AND (p.flag IS NULL OR p.flag != 'rejected')
                   LIMIT 1""",
            (photo_sha256, detector_model),
        ).fetchone()

    def installed_classifier_runtime(
        self,
        photo_sha256: str,
        classifier_model: str,
        labels_short_fingerprint: str,
        input_fingerprint: str,
        detector_model: str,
    ) -> sqlite3.Row | None:
        """A classifier runtime already installed on a photo the artifact matches."""
        return self.conn.execute(
            """SELECT cr.runtime_fingerprint
               FROM classifier_runs cr
               JOIN detections d ON d.id = cr.detection_id
               JOIN photos p ON p.id = d.photo_id
               WHERE p.file_hash = ? AND cr.classifier_model = ?
                 AND cr.labels_fingerprint = ?
                 AND cr.input_fingerprint = ?
                 AND d.detector_model IN (?, 'full-image')
                 AND p.companion_path IS NULL
                 AND p.working_copy_path IS NULL
                 AND (p.flag IS NULL OR p.flag != 'rejected')
               LIMIT 1""",
            (
                photo_sha256, classifier_model, labels_short_fingerprint,
                input_fingerprint, detector_model,
            ),
        ).fetchone()

    def original_backed_photos(self, photo_sha256: str) -> list[sqlite3.Row]:
        """Non-rejected photos with this hash and no companion or working copy."""
        return self.conn.execute(
            """SELECT id FROM photos
               WHERE file_hash = ? AND companion_path IS NULL
                 AND working_copy_path IS NULL
                 AND (flag IS NULL OR flag != 'rejected')
               ORDER BY id""",
            (photo_sha256,),
        ).fetchall()

    def detector_run_identity(
        self, photo_id: int, detector_model: str,
    ) -> sqlite3.Row | None:
        """The runtime and input fingerprints of one detector run, or None."""
        return self.conn.execute(
            """SELECT runtime_fingerprint, input_fingerprint
                   FROM detector_runs
                   WHERE photo_id = ? AND detector_model = ?""",
            (photo_id, detector_model),
        ).fetchone()

    def labels_fingerprint_collision(
        self, short_fingerprint: str, full_fingerprint: str,
    ) -> bool:
        """True when the short fingerprint already names a different full digest."""
        return self.conn.execute(
            """SELECT 1 FROM labels_fingerprints
               WHERE fingerprint = ? AND full_fingerprint IS NOT NULL
                 AND full_fingerprint != ?""",
            (short_fingerprint, full_fingerprint),
        ).fetchone() is not None

    def classifier_run_identity(
        self, detection_id: int, classifier_model: str, labels_short_fingerprint: str,
    ) -> sqlite3.Row | None:
        """The runtime and input fingerprints of one classifier run, or None."""
        return self.conn.execute(
            """SELECT runtime_fingerprint, input_fingerprint
               FROM classifier_runs
               WHERE detection_id = ? AND classifier_model = ?
                 AND labels_fingerprint = ?""",
            (detection_id, classifier_model, labels_short_fingerprint),
        ).fetchone()

    def manual_review_exists(
        self, detection_id: int, classifier_model: str, labels_short_fingerprint: str,
    ) -> bool:
        """True when a person accepted or rejected one of the run's predictions.

        Auto-created taxonomy-match reviews (``__vireo_auto_match__``) are
        reproducible cache state and do not count.
        """
        row = self.conn.execute(
            """SELECT 1
           FROM predictions p
           JOIN prediction_review pr ON pr.prediction_id = p.id
           WHERE p.detection_id = ? AND p.classifier_model = ?
             AND p.labels_fingerprint = ?
             AND pr.status IN ('accepted', 'rejected')
             AND COALESCE(pr.individual, '') != '__vireo_auto_match__'
           LIMIT 1""",
            (detection_id, classifier_model, labels_short_fingerprint),
        ).fetchone()
        return row is not None

    def full_image_detection(self, photo_id: int) -> sqlite3.Row | None:
        """The photo's lowest-id ``full-image`` detection, or None."""
        return self.conn.execute(
            """SELECT id FROM detections
                   WHERE photo_id = ? AND detector_model = 'full-image'
                   ORDER BY id LIMIT 1""",
            (photo_id,),
        ).fetchone()

    def detection_exists(self, detection_id: int) -> bool:
        """True when a detection row has this id."""
        return self.conn.execute(
            "SELECT 1 FROM detections WHERE id = ?",
            (detection_id,),
        ).fetchone() is not None

    # -- materialize writes (no commit: the caller owns the transaction) --------

    def upsert_imported_labels_fingerprint(
        self,
        short_fingerprint: str,
        full_fingerprint: str,
        display_name: str | None,
        label_count: int | None,
    ) -> None:
        """Record an artifact's label set, keeping known values over missing ones."""
        self.conn.execute(
            """INSERT INTO labels_fingerprints
                         (fingerprint, full_fingerprint, display_name,
                          sources_json, label_count)
                       VALUES (?, ?, ?, '[]', ?)
                       ON CONFLICT(fingerprint) DO UPDATE SET
                         full_fingerprint = COALESCE(
                             excluded.full_fingerprint,
                             labels_fingerprints.full_fingerprint
                         ),
                         display_name = COALESCE(excluded.display_name, display_name),
                         label_count = COALESCE(excluded.label_count, label_count)""",
            (short_fingerprint, full_fingerprint, display_name, label_count),
        )

    def fill_missing_match_score(
        self,
        match_score: float,
        detection_id: int,
        classifier_model: str,
        labels_short_fingerprint: str,
        species: str,
    ) -> int:
        """Set ``match_score`` on a candidate that has none; the number of rows filled."""
        cursor = self.conn.execute(
            """UPDATE predictions
                 SET match_score = ?
               WHERE detection_id = ? AND classifier_model = ?
                 AND labels_fingerprint = ? AND species = ?
                 AND match_score IS NULL""",
            (
                match_score, detection_id, classifier_model,
                labels_short_fingerprint, species,
            ),
        )
        return cursor.rowcount

    def insert_match_summary(
        self,
        detection_id: int,
        classifier_model: str,
        labels_short_fingerprint: str,
        match: Mapping[str, Any],
    ) -> None:
        """Insert an artifact's run-level match summary (``match`` block)."""
        self.conn.execute(
            """INSERT INTO classifier_match_scores
                     (detection_id, classifier_model, labels_fingerprint,
                      max_match_score, match_margin, top_species,
                      label_count, score_kind)
                   VALUES (?, ?, ?, ?, ?, ?, ?, ?)""",
            (
                detection_id, classifier_model, labels_short_fingerprint,
                match["max_match_score"], match.get("match_margin"),
                match.get("top_species"), match.get("label_count"),
                match.get("score_kind"),
            ),
        )

    def delete_match_summary(
        self, detection_id: int, classifier_model: str, labels_short_fingerprint: str,
    ) -> None:
        """Delete one run's ``classifier_match_scores`` row."""
        self.conn.execute(
            """DELETE FROM classifier_match_scores
               WHERE detection_id = ? AND classifier_model = ?
                 AND labels_fingerprint = ?""",
            (detection_id, classifier_model, labels_short_fingerprint),
        )

    def delete_run_predictions(
        self, detection_id: int, classifier_model: str, labels_short_fingerprint: str,
    ) -> None:
        """Delete every candidate of one run (reviews cascade with them)."""
        self.conn.execute(
            """DELETE FROM predictions
               WHERE detection_id = ? AND classifier_model = ?
                 AND labels_fingerprint = ?""",
            (detection_id, classifier_model, labels_short_fingerprint),
        )

    def insert_imported_prediction(
        self,
        detection_id: int,
        classifier_model: str,
        labels_short_fingerprint: str,
        labels_full_fingerprint: str,
        species: str,
        candidate: Mapping[str, Any],
    ) -> None:
        """Insert one artifact candidate as a ``new`` prediction (ignored if present)."""
        taxonomy = candidate.get("taxonomy") or {}
        self.conn.execute(
            """INSERT OR IGNORE INTO predictions
                     (detection_id, classifier_model,
                      labels_fingerprint, labels_fingerprint_full,
                      species, confidence, category, scientific_name,
                      taxonomy_kingdom, taxonomy_phylum,
                      taxonomy_class, taxonomy_order,
                      taxonomy_family, taxonomy_genus, source_taxon_id,
                      match_score)
                   VALUES (?, ?, ?, ?, ?, ?, 'new', ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            (
                detection_id, classifier_model,
                labels_short_fingerprint, labels_full_fingerprint,
                species, candidate["confidence"],
                taxonomy.get("scientific_name"),
                taxonomy.get("kingdom"), taxonomy.get("phylum"),
                taxonomy.get("class"), taxonomy.get("order"),
                taxonomy.get("family"), taxonomy.get("genus"),
                taxonomy.get("taxon_id"),
                # Absent on artifacts published before the field existed;
                # NULL there means "not recorded", never "matched badly".
                candidate.get("match_score"),
            ),
        )

    def upsert_imported_classifier_run(
        self,
        detection_id: int,
        classifier_model: str,
        labels_short_fingerprint: str,
        labels_full_fingerprint: str,
        runtime_fingerprint: str,
        input_fingerprint: str,
        prediction_count: int,
        input_recipe: str | None,
    ) -> None:
        """Write (or replace) the ``classifier_runs`` marker for a materialized run."""
        self.conn.execute(
            """INSERT INTO classifier_runs
                 (detection_id, classifier_model, labels_fingerprint,
                  labels_fingerprint_full, runtime_fingerprint,
                  input_fingerprint, prediction_count, input_recipe)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?)
               ON CONFLICT(detection_id, classifier_model,
                           labels_fingerprint)
               DO UPDATE SET
                 labels_fingerprint_full = excluded.labels_fingerprint_full,
                 runtime_fingerprint = excluded.runtime_fingerprint,
                 input_fingerprint = excluded.input_fingerprint,
                 prediction_count = excluded.prediction_count,
                 input_recipe = excluded.input_recipe,
                 run_at = datetime('now')""",
            (
                detection_id, classifier_model,
                labels_short_fingerprint, labels_full_fingerprint,
                runtime_fingerprint, input_fingerprint,
                prediction_count, input_recipe,
            ),
        )
