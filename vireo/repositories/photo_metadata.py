"""Catalog-wide reads of photos' stored camera metadata, for export.

Owns the two bulk reads ``export.py`` makes for an export selection (also
used by site export, site publishing and panoramas): each photo's raw
``exif_data`` JSON and its promoted camera and exposure columns. Photo rows
are global and callers pass ids they have already resolved, so nothing here
is workspace-scoped and building the repository never needs an active
workspace (unlike ``db.photos``). The reads keep export's own chunks of 999
ids, so the statements are the ones export always ran. EXIF writes and the
search-text index live elsewhere (``scanning``, ``exif_search``).

Callers reach it as ``db.photo_metadata`` (a fresh repository per access, see
``Database.photo_metadata``); there are no forwarding wrappers on
``Database``.
"""

import sqlite3
from collections.abc import Sequence

_EXPORT_CHUNK = 999


class PhotoMetadataRepository:
    def __init__(self, conn: sqlite3.Connection) -> None:
        self.conn = conn

    def exif_data_by_ids(self, photo_ids: Sequence[int]) -> dict[int, str | None]:
        """``{photo_id: exif_data}`` (the raw JSON text) for ``photo_ids``.

        Ids with no photo row are absent.
        """
        out = {}
        for i in range(0, len(photo_ids), _EXPORT_CHUNK):
            chunk = photo_ids[i:i + _EXPORT_CHUNK]
            placeholders = ",".join("?" for _ in chunk)
            rows = self.conn.execute(
                f"SELECT id, exif_data FROM photos WHERE id IN ({placeholders})",
                list(chunk),
            ).fetchall()
            for row in rows:
                out[row["id"]] = row["exif_data"]
        return out

    def camera_data_by_ids(self, photo_ids: Sequence[int]) -> dict[int, dict]:
        """``{photo_id: dict}`` of the promoted camera and exposure columns.

        Each dict carries ``id``, ``camera_make``, ``camera_model``, ``lens``,
        ``focal_length``, ``aperture``, ``shutter_speed`` and ``iso``. Ids
        with no photo row are absent.
        """
        out = {}
        for i in range(0, len(photo_ids), _EXPORT_CHUNK):
            chunk = photo_ids[i:i + _EXPORT_CHUNK]
            placeholders = ",".join("?" for _ in chunk)
            rows = self.conn.execute(
                f"""SELECT id, camera_make, camera_model, lens, focal_length,
                           aperture, shutter_speed, iso
                    FROM photos WHERE id IN ({placeholders})""",
                list(chunk),
            ).fetchall()
            for row in rows:
                out[row["id"]] = dict(row)
        return out
