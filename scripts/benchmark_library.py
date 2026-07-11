#!/usr/bin/env python3
"""Measure interactive request budgets against a synthetic large library."""

import argparse
import json
import os
import sys
import tempfile
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "vireo"))


BUDGETS_100K = {
    "startup_seconds": 5.0,
    "browse_init_seconds": 2.0,
    "folder_tree_seconds": 1.0,
    "job_poll_seconds": 0.5,
}
BUDGETS_1M = {
    "startup_seconds": 15.0,
    "browse_init_seconds": 5.0,
    "folder_tree_seconds": 2.0,
    "job_poll_seconds": 1.0,
}


def _seed(db_path, photo_count):
    from db import Database

    with Database(db_path) as db:
        folder_id = db.add_folder("/synthetic/library", name="library")
        rows = (
            (
                folder_id,
                f"photo-{index:09d}.jpg",
                ".jpg",
                1_000_000,
                float(index),
                f"2024-01-{(index % 28) + 1:02d}T12:00:00",
                3,
                "none",
            )
            for index in range(photo_count)
        )
        db.conn.executemany(
            """INSERT INTO photos
               (folder_id, filename, extension, file_size, file_mtime,
                timestamp, rating, flag)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?)""",
            rows,
        )
        db.conn.execute(
            "UPDATE folders SET photo_count=? WHERE id=?",
            (photo_count, folder_id),
        )
        db.conn.commit()


def _timed(call):
    started = time.perf_counter()
    result = call()
    return time.perf_counter() - started, result


def benchmark(photo_count):
    os.environ["VIREO_DISABLE_BROWSER_AUTH"] = "1"
    os.environ["VIREO_DISABLE_STARTUP_BACKFILL_TIMERS"] = "1"
    with tempfile.TemporaryDirectory(prefix="vireo-benchmark-") as tmp:
        os.environ["HOME"] = tmp
        db_path = os.path.join(tmp, "library.db")
        thumb_dir = os.path.join(tmp, "thumbnails")
        os.makedirs(thumb_dir)
        _seed(db_path, photo_count)

        from app import create_app

        startup, app = _timed(
            lambda: create_app(db_path=db_path, thumb_cache_dir=thumb_dir)
        )
        client = app.test_client()

        def request(path):
            response = client.get(path)
            if response.status_code != 200:
                raise RuntimeError(f"{path} returned {response.status_code}")
            return response

        browse, _ = _timed(lambda: request("/api/browse/init?per_page=100"))
        folders, _ = _timed(lambda: request("/api/folders"))
        jobs, _ = _timed(lambda: request("/api/jobs"))
        return {
            "photos": photo_count,
            "startup_seconds": round(startup, 4),
            "browse_init_seconds": round(browse, 4),
            "folder_tree_seconds": round(folders, 4),
            "job_poll_seconds": round(jobs, 4),
        }


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--photos", type=int, default=100_000)
    parser.add_argument("--enforce", action="store_true")
    args = parser.parse_args()

    results = benchmark(args.photos)
    print(json.dumps(results, indent=2, sort_keys=True))
    if not args.enforce:
        return

    budgets = BUDGETS_1M if args.photos >= 1_000_000 else BUDGETS_100K
    failures = [
        f"{name}: {results[name]:.4f}s > {limit:.4f}s"
        for name, limit in budgets.items()
        if results[name] > limit
    ]
    if failures:
        raise SystemExit("Performance budget exceeded:\n" + "\n".join(failures))


if __name__ == "__main__":
    main()
