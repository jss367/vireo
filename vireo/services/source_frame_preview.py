"""Cache upright, unedited inspection frames alongside the shared preview LRU.

Unlike ordinary previews, these frames ignore saved crop/rotation recipes and
use the same JPEG-first original/working-copy loader as native focus crops.
"""
from __future__ import annotations

import hashlib
import io
import json
import logging
import os
import sqlite3
from collections.abc import Callable
from dataclasses import dataclass
from typing import TYPE_CHECKING

from artifact_flight import atomic_write_bytes, preview_artifact_flights
from PIL import Image
from preview_cache import evict_if_over_quota, paired_preview_ready

if TYPE_CHECKING:
    from db import Database
    from services.service_types import JsonDict

log = logging.getLogger(__name__)
type SourceLoader = Callable[[JsonDict, JsonDict | None], tuple[Image.Image | None, str | None]]


class SourceFrameUnavailable(ValueError):
    """No stable, available source raster can be served for inspection."""


def _file_state(path: str | None) -> tuple[str, int, int, int] | None:
    if not path:
        return None
    try:
        stat = os.stat(path)
        return path, stat.st_size, stat.st_mtime_ns, stat.st_ino
    except OSError:
        return None


@dataclass
class _SourceFrame:
    """One source-state-keyed render, shared by equal-key request flights."""

    db: Database
    photo_id: int
    size: int
    root: str
    quality: int
    loader: SourceLoader

    def state(self) -> tuple[JsonDict, JsonDict | None, str]:
        photo_row = self.db.photos.get(self.photo_id, verify_workspace=True)
        if photo_row is None:
            raise SourceFrameUnavailable('Photo is unavailable')
        photo = dict(photo_row)
        folder_row = self.db.folders.get(photo['folder_id'])
        folder = dict(folder_row) if folder_row else None
        original = os.path.join(folder['path'], photo['filename']) if folder else None
        working = os.path.join(self.root, photo['working_copy_path']) if photo.get('working_copy_path') else None
        identity = {key: photo.get(key) for key in (
            'id', 'folder_id', 'filename', 'file_size', 'file_mtime', 'file_hash',
            'working_copy_path', 'width', 'height',
        )}
        key = hashlib.sha256(json.dumps(
            [1, identity, original, _file_state(original), working, _file_state(working), self.size, self.quality],
            sort_keys=True,
        ).encode()).hexdigest()
        return photo, folder, key

    def cached(self, path: str) -> bytes | None:
        if not paired_preview_ready(self.db, path):
            return None
        try:
            with open(path, 'rb') as handle:
                data = handle.read()
            self.db.caches.paired_preview_touch(os.path.basename(path))
            return data
        except (OSError, sqlite3.Error):
            return None

    def publish(self, path: str, key: str, data: bytes) -> None:
        # Revalidation is mandatory even when durable cache writes are unavailable.
        if self.state()[2] != key:
            raise SourceFrameUnavailable('Photo source changed; refresh and try again')

        def write() -> None:
            if self.state()[2] != key:
                raise SourceFrameUnavailable('Photo source changed; refresh and try again')
            atomic_write_bytes(data, path)
            self.db.caches.paired_preview_insert(self.photo_id, os.path.basename(path), len(data))

        try:
            if self.db.in_transaction:
                write()
            else:
                with self.db.transaction(immediate=True):
                    write()
            evict_if_over_quota(self.db, self.root)
        except (OSError, sqlite3.Error):
            # A read-only/full cache must not prevent inspecting available pixels.
            log.warning('Could not cache source frame %s', path, exc_info=True)

    def render(self, photo: JsonDict, folder: JsonDict | None, key: str, path: str) -> bytes:
        image, _source = self.loader(photo, folder)
        if image is None:
            raise SourceFrameUnavailable('Full frame unavailable')
        try:
            rgb = image.convert('RGB')
            try:
                rgb.thumbnail((self.size, self.size), Image.Resampling.LANCZOS)
                buffer = io.BytesIO()
                rgb.save(buffer, format='JPEG', quality=self.quality)
                data = buffer.getvalue()
            finally:
                rgb.close()
        finally:
            image.close()
        self.publish(path, key, data)
        return data

    def read(self) -> bytes:
        photo, folder, key = self.state()
        path = os.path.join(self.root, 'previews', 'paired', f'{self.photo_id}_{self.size}_source_{key}.jpg')
        cached = self.cached(path)
        if cached is not None:
            return cached
        result = preview_artifact_flights.run(
            path, lambda: self.render(photo, folder, key, path), self.read,
        )
        assert result.value is not None
        return result.value


def source_frame_bytes(
    db: Database, photo_id: int, size: int, root: str, quality: int, loader: SourceLoader,
) -> bytes:
    """Read/reuse a source-keyed JPEG; edits never change its pixel geometry."""
    return _SourceFrame(db, photo_id, size, root, quality, loader).read()
