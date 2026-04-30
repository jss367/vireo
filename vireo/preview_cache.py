"""Shared helpers for the preview_cache LRU.

The eviction pass is called from three places: the Flask request path
(``_serve_preview``), the startup migration in ``create_app``, and the
pipeline job's preview stage. Keeping the logic here avoids having the
pipeline import from ``app`` (which would be circular) or duplicating
the loop in two modules.
"""

import logging
import os

log = logging.getLogger(__name__)


def evict_if_over_quota(db, vireo_dir):
    """Evict oldest preview_cache entries until under preview_cache_max_mb.

    Walks rows in ascending ``last_access_at`` order, removes files and
    rows, and stops as soon as total <= quota. Self-healing: if a file
    is already missing, the ghost row is still deleted. If ``unlink``
    fails for any other OS reason the row is *left in place* so the
    bytes stay accounted for and a future pass can retry; otherwise the
    accounting under-reports and eviction stops targeting the leaked
    bytes.

    Deletes are batched into one transaction to avoid hundreds of
    fsyncs when the quota is shrunk dramatically.
    """
    import config as cfg

    quota_mb = cfg.load().get("preview_cache_max_mb", 20480)
    max_bytes = int(quota_mb) * 1024 * 1024
    total = db.preview_cache_total_bytes()
    if total <= max_bytes:
        return

    preview_dir = os.path.join(vireo_dir, "previews")
    to_delete = []
    freed_bytes = 0
    for row in db.preview_cache_oldest_first():
        if total <= max_bytes:
            break
        path = os.path.join(
            preview_dir, f"{row['photo_id']}_{row['size']}.jpg"
        )
        removed = True
        try:
            os.remove(path)
        except FileNotFoundError:
            pass
        except OSError as e:
            log.warning("Failed to remove preview cache file %s: %s", path, e)
            removed = False
        if removed:
            to_delete.append((row["photo_id"], row["size"]))
            total -= row["bytes"]
            freed_bytes += row["bytes"]

    if to_delete:
        db.conn.executemany(
            "DELETE FROM preview_cache WHERE photo_id=? AND size=?",
            to_delete,
        )
        db.conn.commit()
        log.info(
            "Preview cache eviction: removed %d entries, freed %.1f MB",
            len(to_delete), freed_bytes / 1024 / 1024,
        )


def reconcile_preview_cache(db, vireo_dir):
    """Drop preview_cache rows whose on-disk file is missing.

    Counterpart to ``evict_if_over_quota``'s self-heal: that path only
    cleans up ghost rows when the cache is *over* quota. If the cache
    accounting drifts while *under* quota — e.g. files deleted by an
    external process, or a previous eviction pass that removed files
    after the row's ``last_access_at`` was recently touched — the
    table keeps reporting ``total_bytes`` for files that no longer
    exist, and eviction stays asleep. That's invisible to the user
    until the next pipeline run regenerates everything from RAW
    because none of the cache files actually exist.

    Run at startup so a stale table can't poison the rest of the
    session. Returns the number of rows dropped.
    """
    preview_dir = os.path.join(vireo_dir, "previews")
    to_delete = []
    for row in db.preview_cache_oldest_first():
        path = os.path.join(
            preview_dir, f"{row['photo_id']}_{row['size']}.jpg"
        )
        if not os.path.exists(path):
            to_delete.append((row["photo_id"], row["size"]))

    if to_delete:
        db.conn.executemany(
            "DELETE FROM preview_cache WHERE photo_id=? AND size=?",
            to_delete,
        )
        db.conn.commit()
        log.info(
            "Preview cache reconcile: dropped %d ghost rows (files missing on disk)",
            len(to_delete),
        )
    return len(to_delete)
