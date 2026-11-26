#!/usr/bin/env python3
"""Save a small catalog made by each Vireo release, for the upgrade test.

``vireo/tests/test_release_catalog_upgrades.py`` opens every saved catalog
with current code and requires its photos, flags, ratings, keywords, masks
and workspace membership to come through. Each catalog is written by that
release's own code (``release_catalog_writer.py`` run against the release's
``vireo/`` directory), so its schema, defaults and side tables are what a
user of that release really has, not a hand-built imitation.

Each release gets two files in ``vireo/tests/fixtures/release_catalogs/``:
``v<version>.db.gz``, the catalog, and ``v<version>.json``, what the release
itself read back from it (the snapshot the test compares against).

    python scripts/release_catalogs.py make --current   # the version in pyproject.toml
    python scripts/release_catalogs.py make v0.76.0     # a tagged release
    python scripts/release_catalogs.py backfill         # every tag since FIRST_RELEASE

``release.sh`` runs ``make --current`` after bumping the version, so each
release commit carries its own catalog. Releases before ``FIRST_RELEASE``
stamp a schema older than ``schema.OLDEST_UPGRADABLE_VERSION``, which current
code refuses to open; their refusal is tested in ``test_schema.py``.
"""

from __future__ import annotations

import argparse
import gzip
import json
import os
import re
import shutil
import sqlite3
import subprocess
import sys
import tarfile
import tempfile
import tomllib
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent
FIXTURE_DIR = REPO_ROOT / "vireo" / "tests" / "fixtures" / "release_catalogs"
WRITER = Path(__file__).resolve().parent / "release_catalog_writer.py"
FIRST_RELEASE = (0, 56, 0)
_TAG_RE = re.compile(r"^v(\d+)\.(\d+)\.(\d+)$")


def current_version():
    with open(REPO_ROOT / "pyproject.toml", "rb") as f:
        return "v" + tomllib.load(f)["project"]["version"]


def fixture_paths(release):
    return FIXTURE_DIR / f"{release}.db.gz", FIXTURE_DIR / f"{release}.json"


def release_tags():
    """Release tags at or after ``FIRST_RELEASE``, oldest first."""
    out = subprocess.run(
        ["git", "-C", str(REPO_ROOT), "tag", "--list", "v*"],
        check=True, capture_output=True, text=True,
    ).stdout.split()
    tags = []
    for tag in out:
        match = _TAG_RE.match(tag)
        if match and tuple(map(int, match.groups())) >= FIRST_RELEASE:
            tags.append((tuple(map(int, match.groups())), tag))
    return [tag for _, tag in sorted(tags)]


def _export_tag(tag, dest):
    """Extract the tag's ``vireo/`` directory under ``dest``; return its path."""
    archive = subprocess.run(
        ["git", "-C", str(REPO_ROOT), "archive", "--format=tar", tag, "vireo"],
        check=True, capture_output=True,
    ).stdout
    tar_path = Path(dest) / "release.tar"
    tar_path.write_bytes(archive)
    with tarfile.open(tar_path) as tar:
        tar.extractall(dest, filter="data")
    return Path(dest) / "vireo"


def _write_with(release_dir, work):
    """Run the writer on the release's code in a child with its own HOME.

    The child's HOME is a scratch folder: a release reads and may write its
    config and data folders under HOME, and must never touch the user's.
    """
    db_path = Path(work) / "vireo.db"
    snapshot_path = Path(work) / "snapshot.json"
    home = Path(work) / "home"
    home.mkdir()
    env = {k: v for k, v in os.environ.items() if not k.startswith("VIREO_")}
    env.update(HOME=str(home), USERPROFILE=str(home), XDG_DATA_HOME=str(home / "data"),
               XDG_CACHE_HOME=str(home / "cache"), XDG_CONFIG_HOME=str(home / "config"))
    result = subprocess.run(
        [sys.executable, "-I", str(WRITER), str(release_dir), str(db_path), str(snapshot_path)],
        env=env, capture_output=True, text=True,
    )
    if result.returncode != 0:
        raise RuntimeError(f"the writer failed:\n{result.stdout}{result.stderr}")
    return db_path, json.loads(snapshot_path.read_text())


def _save(release, db_path, snapshot):
    """Compact the catalog into one file and store it gzipped beside its snapshot."""
    catalog_path, snapshot_path = fixture_paths(release)
    FIXTURE_DIR.mkdir(parents=True, exist_ok=True)
    compact = db_path.with_name("compact.db")
    with sqlite3.connect(db_path) as conn:
        conn.execute("VACUUM INTO ?", (str(compact),))
    # mtime=0 and no stored name keep the archive independent of when and
    # where it was made.
    with (open(compact, "rb") as src, open(catalog_path, "wb") as raw,
          gzip.GzipFile(filename="", mode="wb", fileobj=raw, mtime=0) as dst):
        shutil.copyfileobj(src, dst)
    snapshot = {"release": release, **snapshot}
    snapshot_path.write_text(json.dumps(snapshot, indent=2, sort_keys=True) + "\n")
    return catalog_path


def make(release, release_dir=None):
    """Write ``release``'s catalog with its code; ``release_dir`` defaults to the tag."""
    with tempfile.TemporaryDirectory(prefix="vireo-release-catalog-") as work:
        if release_dir is None:
            release_dir = _export_tag(release, work)
        db_path, snapshot = _write_with(release_dir, work)
        path = _save(release, db_path, snapshot)
    print(f"{release}: schema {snapshot['schema_version']}, {path.relative_to(REPO_ROOT)}")
    return path


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    sub = parser.add_subparsers(dest="command", required=True)
    make_cmd = sub.add_parser("make", help="write the catalog for the given releases")
    make_cmd.add_argument("tags", nargs="*", help="release tags, such as v0.76.0")
    make_cmd.add_argument("--current", action="store_true",
                          help="the version in pyproject.toml, written by this checkout's code")
    sub.add_parser("backfill", help="write the catalog of every release tag that has none")
    args = parser.parse_args(argv)

    if args.command == "backfill":
        missing = [tag for tag in release_tags() if not fixture_paths(tag)[0].exists()]
        for tag in missing:
            make(tag)
        if not missing:
            print("Every release since v%d.%d.%d has a catalog." % FIRST_RELEASE)
        return 0
    if not args.current and not args.tags:
        parser.error("name release tags or pass --current")
    if args.current:
        make(current_version(), release_dir=REPO_ROOT / "vireo")
    for tag in args.tags:
        if not _TAG_RE.match(tag):
            parser.error(f"{tag!r} is not a release tag (vX.Y.Z)")
        make(tag)
    return 0


if __name__ == "__main__":
    sys.exit(main())
