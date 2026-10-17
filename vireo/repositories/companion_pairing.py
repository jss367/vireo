"""Persistence for RAW+JPEG companion pairing.

A RAW and the camera JPEG of the same exposure are one photo: the RAW row
carries the JPEG's filename in ``companion_path`` and the JPEG's import
identity in ``companion_identities``. The scanner pairs them two ways, and
both read and write through this repository:

- the end-of-scan pass (``scanner._pair_raw_jpeg_companions``) folds a JPEG
  that already has a ``photos`` row of its own into its RAW: it carries the
  JPEG's metadata, detections, review state, pending changes, iNaturalist
  submissions and edit recipe over, then deletes the JPEG's row;
- a scan that meets a JPEG beside its cataloged RAW records it as the RAW's
  companion without ever giving it a row (``_ScanRun._attach_companion``),
  and an unchanged known companion is left in place
  (``_ScanRun._keep_known_companion``).

The ``photo_keywords`` copy a merge makes lives in
``repositories/keyword_provenance.py`` (``carry_companion_keywords``), with
every other association writer.

Callers reach this module as ``db.companion_pairing`` (a fresh repository per
access, see ``Database.companion_pairing``); there are no forwarding wrappers
on ``Database``. Pairing is catalog-wide, so the repository takes no
workspace id. Nothing here commits: every write joins the caller's
transaction, and the scanner owns the commit.
"""

import sqlite3

from metadata import EXIF_SUMMARY_COLUMNS
from repositories.photo_row_deletion import photo_row_deletion

# The columns a merge may carry from companion to primary.
#
# Transfer metadata from companion to primary if primary lacks it.
# Includes the promoted EXIF summary columns
# (``EXIF_SUMMARY_COLUMNS``) so a RAW row that got merged with its
# JPEG companion doesn't lose ``camera_make``/``camera_model``/
# ``lens``/``aperture``/``shutter_speed``/``iso`` when the JPEG row
# is deleted — otherwise the new universal filters miss the paired
# photo even though ExifTool extracted those values.
# ``focal_length`` is part of ``EXIF_SUMMARY_COLUMNS`` — do not name it
# again here or the gap-fill loop (``scanner._fill_primary_metadata_gaps``)
# would append a second ``focal_length=?`` assignment and SQLite would
# reject the UPDATE for duplicate columns.
_TRANSFER_COLUMNS = (
    "timestamp, rating, flag, latitude, longitude, exif_data, "
    "width, height, "
    + ", ".join(EXIF_SUMMARY_COLUMNS)
)


class CompanionPairingRepository:
    def __init__(self, conn: sqlite3.Connection) -> None:
        self.conn = conn

    # -- finding pairs -------------------------------------------------------

    def rows_for_stem(self, folder_id: int, stem: str) -> list[sqlite3.Row]:
        """Pairing candidates in one folder whose filename sorts under ``stem``.

        Every name whose stem is ``stem`` sorts in ``[stem + ".", stem + "/")``;
        the caller drops the names in that range with a different stem.
        """
        return self.conn.execute(
            "SELECT id, folder_id, filename, extension, timestamp,"
            " camera_make, camera_model, companion_path FROM photos"
            " WHERE folder_id = ? AND filename >= ? AND filename < ?"
            " AND (companion_path IS NULL OR extension IN"
            " ('.nef','.cr2','.cr3','.arw','.raf','.dng','.rw2','.orf'))",
            (folder_id, stem + ".", stem + "/"),
        ).fetchall()

    def pairing_candidate_rows(self) -> list[sqlite3.Row]:
        """Every photo not yet paired, plus RAW rows that already carry a companion."""
        return self.conn.execute(
            "SELECT id, folder_id, filename, extension, timestamp,"
            " camera_make, camera_model, companion_path FROM photos"
            " WHERE companion_path IS NULL"
            " OR (companion_path IS NOT NULL AND extension IN"
            " ('.nef','.cr2','.cr3','.arw','.raf','.dng','.rw2','.orf'))"
            " ORDER BY folder_id, filename"
        ).fetchall()

    # -- merging a companion row into its RAW --------------------------------

    def metadata_transfer_row(self, photo_id: int) -> sqlite3.Row | None:
        """The columns a merge may carry from companion to primary, for one row."""
        return self.conn.execute(
            f"SELECT {_TRANSFER_COLUMNS} FROM photos WHERE id = ?",
            (photo_id,),
        ).fetchone()

    def keep_import_identity(self, primary_id: int, companion_id: int) -> None:
        """Copy the companion row's import identity onto the primary."""
        self.conn.execute(
            "INSERT OR REPLACE INTO companion_identities "
            "(photo_id, filename, file_size, timestamp, file_hash, file_mtime, needs_sync) "
            "SELECT ?, filename, file_size, timestamp, file_hash, file_mtime, 0"
            " FROM photos WHERE id=?",
            (primary_id, companion_id),
        )

    def set_companion_path(self, primary_id: int, filename: str) -> None:
        self.conn.execute(
            "UPDATE photos SET companion_path = ? WHERE id = ?",
            (filename, primary_id),
        )

    def detections_of(self, photo_id: int) -> list[sqlite3.Row]:
        """The photo's detections, with every column a re-key needs."""
        return self.conn.execute(
            "SELECT id, detector_model, box_x, box_y, box_w, box_h,"
            " detector_confidence, category"
            " FROM detections WHERE photo_id = ?",
            (photo_id,),
        ).fetchall()

    def move_detection(self, detection_id: str, photo_id: int) -> None:
        self.conn.execute(
            "UPDATE detections SET photo_id = ? WHERE id = ?",
            (photo_id, detection_id),
        )

    def insert_rekeyed_detection(
        self, new_id: str, photo_id: int, det: sqlite3.Row,
    ) -> None:
        """Insert ``det`` under ``new_id`` for ``photo_id``; a no-op if that id exists."""
        self.conn.execute(
            "INSERT INTO detections"
            " (id, photo_id, detector_model, box_x, box_y, box_w, box_h,"
            "  detector_confidence, category)"
            " VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)"
            " ON CONFLICT(id) DO NOTHING",
            (new_id, photo_id, det["detector_model"],
             det["box_x"], det["box_y"], det["box_w"], det["box_h"],
             det["detector_confidence"], det["category"]),
        )

    def redirect_predictions(self, old_id: str, new_id: str) -> None:
        self.conn.execute(
            "UPDATE OR IGNORE predictions SET detection_id = ? WHERE detection_id = ?",
            (new_id, old_id),
        )

    def carry_prediction_reviews_to_survivors(self, old_id: str, new_id: str) -> None:
        """Move review state off ``old_id``'s predictions onto ``new_id``'s twins."""
        self.conn.execute(
            """INSERT INTO prediction_review
                 (prediction_id, workspace_id, status, reviewed_at,
                  individual, group_id, vote_count, total_votes)
               SELECT survivor.id, pr.workspace_id, pr.status,
                      pr.reviewed_at, pr.individual, pr.group_id,
                      pr.vote_count, pr.total_votes
                 FROM predictions duplicate
                 JOIN predictions survivor
                   ON survivor.detection_id = ?
                  AND survivor.classifier_model = duplicate.classifier_model
                  AND survivor.labels_fingerprint = duplicate.labels_fingerprint
                  AND survivor.species IS duplicate.species
                 JOIN prediction_review pr
                   ON pr.prediction_id = duplicate.id
                WHERE duplicate.detection_id = ?
               ON CONFLICT(prediction_id, workspace_id)
               DO UPDATE SET status = excluded.status,
                             reviewed_at = excluded.reviewed_at,
                             individual = COALESCE(
                                 excluded.individual,
                                 prediction_review.individual
                             ),
                             group_id = COALESCE(
                                 excluded.group_id,
                                 prediction_review.group_id
                             ),
                             vote_count = COALESCE(
                                 excluded.vote_count,
                                 prediction_review.vote_count
                             ),
                             total_votes = COALESCE(
                                 excluded.total_votes,
                                 prediction_review.total_votes
                             )
                WHERE prediction_review.status = 'pending'
                  AND excluded.status <> 'pending'""",
            (new_id, old_id),
        )

    def redirect_classifier_runs(self, old_id: str, new_id: str) -> None:
        self.conn.execute(
            "UPDATE OR IGNORE classifier_runs SET detection_id = ? WHERE detection_id = ?",
            (new_id, old_id),
        )

    def delete_detection(self, detection_id: str) -> None:
        self.conn.execute(
            "DELETE FROM detections WHERE id = ?",
            (detection_id,),
        )

    def transfer_pending_changes(self, primary_id: int, companion_id: int) -> None:
        self.conn.execute(
            "UPDATE pending_changes SET photo_id = ? WHERE photo_id = ?",
            (primary_id, companion_id),
        )

    def transfer_inat_submissions(self, primary_id: int, companion_id: int) -> None:
        """Move the companion's submissions to the primary, dropping ones it already has."""
        self.conn.execute(
            """DELETE FROM inat_submissions
               WHERE photo_id = ? AND observation_id IN (
                   SELECT observation_id FROM inat_submissions WHERE photo_id = ?
               )""",
            (companion_id, primary_id),
        )
        self.conn.execute(
            "UPDATE inat_submissions SET photo_id = ? WHERE photo_id = ?",
            (primary_id, companion_id),
        )

    def copy_edit_recipe(self, primary_id: int, companion_id: int) -> bool:
        """Give the primary the companion's recipe unless it has one; True if copied."""
        return bool(self.conn.execute(
            """INSERT OR IGNORE INTO photo_edit_recipes
                   (photo_id, recipe_json, updated_at)
               SELECT ?, recipe_json, updated_at
               FROM photo_edit_recipes
               WHERE photo_id = ?""",
            (primary_id, companion_id),
        ).rowcount)

    def transfer_recipe_history(self, primary_id: int, companion_id: int) -> None:
        """Move the companion's edit-recipe history items to the primary."""
        self.conn.execute(
            """UPDATE edit_history_items
                   SET photo_id = ?
                   WHERE photo_id = ?
                     AND edit_id IN (
                         SELECT id FROM edit_history
                         WHERE action_type = 'edit_recipe'
                     )""",
            (primary_id, companion_id),
        )

    def delete_keyword_associations(self, photo_id: int) -> None:
        self.conn.execute("DELETE FROM photo_keywords WHERE photo_id = ?", (photo_id,))

    def row_deletion(self):
        """``photo_row_deletion`` on this connection (see that module)."""
        return photo_row_deletion(self.conn)

    # -- attaching a companion without a row ---------------------------------

    def claim_companion(
        self, owner_id: int, owner_folder_id: int, owner_filename: str, name: str,
    ) -> bool:
        """Set ``name`` as the owner's companion if the owner row is unchanged.

        The guarded write takes the writer lock; False when the owner row is
        gone, renamed, or already pairs with a different file.
        """
        return self.conn.execute(
            "UPDATE photos SET companion_path = ? WHERE id = ?"
            " AND folder_id IS ? AND filename = ?"
            " AND (companion_path IS NULL OR companion_path = ?)",
            (name, owner_id, owner_folder_id, owner_filename, name),
        ).rowcount == 1

    def photo_exists(self, folder_id: int, filename: str) -> bool:
        return self.conn.execute(
            "SELECT 1 FROM photos WHERE folder_id = ? AND filename = ?",
            (folder_id, filename),
        ).fetchone() is not None

    def capture_columns(self, photo_id: int) -> sqlite3.Row | None:
        """The timestamp and camera columns pairing compares, or None."""
        return self.conn.execute(
            "SELECT timestamp, camera_make, camera_model FROM photos"
            " WHERE id = ?",
            (photo_id,),
        ).fetchone()

    def record_identity(
        self, owner_id: int, name: str, file_size: int, timestamp: str | None,
        file_hash: str | None, file_mtime: float,
    ) -> None:
        """Store the companion file's import identity on its owner."""
        self.conn.execute(
            "INSERT OR REPLACE INTO companion_identities"
            " (photo_id, filename, file_size, timestamp, file_hash,"
            " file_mtime, needs_sync) VALUES (?, ?, ?, ?, ?, ?, 0)",
            (owner_id, name, file_size, timestamp,
             file_hash, file_mtime),
        )

    def lock_owner(
        self, owner_id: int, folder_id: int, filename: str, companion: str,
    ) -> bool:
        """A guarded no-op write on the owner row; True if the row still matches."""
        return self.conn.execute(
            "UPDATE photos SET id=id WHERE id=? AND folder_id IS ?"
            " AND filename=? AND companion_path=?",
            (owner_id, folder_id, filename, companion),
        ).rowcount == 1

    def set_identity_mtime(self, owner_id: int, filename: str, file_mtime: float) -> None:
        self.conn.execute(
            "UPDATE companion_identities SET file_mtime = ?"
            " WHERE photo_id = ? AND filename = ?",
            (file_mtime, owner_id, filename),
        )

    def clear_identity_mtime(self, owner_id: int) -> None:
        """Null the owner's stored companion mtime so the next scan re-reads it."""
        self.conn.execute(
            "UPDATE companion_identities SET file_mtime = NULL "
            "WHERE photo_id = ?",
            (owner_id,),
        )

    # -- known companions ----------------------------------------------------

    _OWNER_SQL = (
        "SELECT p.id AS owner_id, p.filename AS owner_filename,"
        " p.folder_id AS folder_id,"
        " c.file_size, c.file_mtime, c.file_hash, c.needs_sync"
        " FROM photos p LEFT JOIN companion_identities c"
        " ON c.photo_id = p.id AND c.filename = p.companion_path"
        " WHERE p.folder_id = ? AND p.companion_path = ?"
        # A RAW with a stored identity vouches for the file; legacy pairs
        # that two RAWs both claim prefer the one that can.
        " ORDER BY c.photo_id IS NULL, p.id LIMIT 1"
    )

    def companion_names(self, folder_id: int) -> set[str]:
        """Every ``companion_path`` recorded in one folder."""
        return {
            row[0] for row in self.conn.execute(
                "SELECT companion_path FROM photos"
                " WHERE folder_id = ? AND companion_path IS NOT NULL",
                (folder_id,),
            )
        }

    def owner_of(self, folder_id: int, filename: str) -> sqlite3.Row | None:
        """The RAW holding ``filename`` as its companion, with the stored identity."""
        return self.conn.execute(
            self._OWNER_SQL, (folder_id, filename),
        ).fetchone()

    def owner_row(self, photo_id: int) -> sqlite3.Row | None:
        """``(folder_id, filename, folder path)`` of a surviving RAW, or None."""
        return self.conn.execute(
            "SELECT p.folder_id, p.filename, f.path FROM photos p"
            " JOIN folders f ON f.id = p.folder_id WHERE p.id = ?",
            (photo_id,),
        ).fetchone()
