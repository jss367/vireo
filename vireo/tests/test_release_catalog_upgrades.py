"""Catalogs made by real releases come through an upgrade intact.

``fixtures/release_catalogs/`` holds a small catalog written by each release
since ``FIRST_RELEASE`` with that release's own code, beside the snapshot the
release read back from it (``scripts/release_catalogs.py`` makes both). Each
test here starts the current app on one of them, the way a user who skipped
every release in between would, and requires the same photos, flags,
ratings, keywords, masks and workspace membership afterwards.

The synthetic upgrade tests in ``test_schema.py`` build an old catalog by
hand from what a migration expects to find. These catch what a hand-built
catalog leaves out: a default, trigger or side table the release really
wrote, and the interaction of every migration and startup repair between
that release and today.
"""

import gzip
import importlib.util
import json
import re
import shutil
import sqlite3
import sys
import tomllib
from pathlib import Path

import pytest
from canonical_schema import SCHEMA_VERSION

REPO_ROOT = Path(__file__).resolve().parents[2]
FIXTURE_DIR = Path(__file__).resolve().parent / "fixtures" / "release_catalogs"


def _load_script(name):
    spec = importlib.util.spec_from_file_location(name, REPO_ROOT / "scripts" / f"{name}.py")
    module = importlib.util.module_from_spec(spec)
    sys.modules.setdefault(name, module)
    spec.loader.exec_module(module)
    return module


writer = _load_script("release_catalog_writer")


def _release_key(path):
    return tuple(int(n) for n in re.findall(r"\d+", path.name.split(".db.gz")[0]))


RELEASES = [p.name.removesuffix(".db.gz")
            for p in sorted(FIXTURE_DIR.glob("v*.db.gz"), key=_release_key)]

# Data a migration deliberately removes, keyed by the migration's version: a
# catalog stamped below it loses exactly this on upgrade. Anything else that
# changes fails the test.
#
# 54 (``_retire_unbound_detector_caches``): a MegaDetector mask from before
# decoded frames were recorded cannot prove it was made in the frame Vireo
# decodes today, so the mask row goes and the photo has no active mask until
# Process runs again or ``scripts/restore_subject_analysis.py`` verifies and
# restores it.
RETIRED_MASK_DETECTORS = {54: {"megadetector-v6"}}


def expected_after_upgrade(before):
    """The release's snapshot with every documented retirement applied."""
    retired = set()
    for version, detectors in RETIRED_MASK_DETECTORS.items():
        if before["schema_version"] < version:
            retired |= detectors
    photos = {}
    for filename, photo in before["photos"].items():
        kept = [m for m in photo["masks"] if m["detector_model"] not in retired]
        active = photo["active_mask"]
        if active is not None and active not in {m["path"] for m in kept}:
            active = None
        photos[filename] = {**photo, "masks": kept, "active_mask": active}
    return {
        "schema_version": SCHEMA_VERSION,
        "folder_workspaces": before["folder_workspaces"],
        "photos": photos,
    }


@pytest.fixture
def upgraded_app(tmp_path, monkeypatch):
    """Start the current app on a copy of a release's catalog."""
    import config as cfg
    import models

    monkeypatch.setenv("HOME", str(tmp_path))
    monkeypatch.setattr(cfg, "CONFIG_PATH", str(tmp_path / "config.json"))
    monkeypatch.setattr(models, "DEFAULT_MODELS_DIR", str(tmp_path / "vireo-models"))
    monkeypatch.setattr(models, "CONFIG_PATH", str(tmp_path / "models.json"))
    started = []

    def start(release):
        from app import create_app

        vireo_dir = tmp_path / "vireo"
        (vireo_dir / "thumbnails").mkdir(parents=True)
        db_path = vireo_dir / "vireo.db"
        with gzip.open(FIXTURE_DIR / f"{release}.db.gz", "rb") as src, open(db_path, "wb") as dst:
            shutil.copyfileobj(src, dst)
        app = create_app(
            db_path=str(db_path), thumb_cache_dir=str(vireo_dir / "thumbnails"),
            api_token="test-token-123",
        )
        started.append(app)
        return app, db_path

    yield start
    for app in started:
        if not app._cleanup_app_resources(job_timeout=30.0):
            pytest.fail("app background jobs did not stop during fixture teardown")


def _snapshot(db_path):
    with sqlite3.connect(db_path) as conn:
        return writer.snapshot(conn)


@pytest.mark.parametrize("release", RELEASES)
def test_release_catalog_survives_upgrade(release, upgraded_app):
    before = json.loads((FIXTURE_DIR / f"{release}.json").read_text())
    app, db_path = upgraded_app(release)

    assert _snapshot(db_path) == expected_after_upgrade(before)

    # The app itself reads them back, not just the catalog's rows.
    response = app.test_client().get("/api/photos?per_page=100")
    assert response.status_code == 200
    listed = {p["filename"]: (p["flag"] or "none", p["rating"] or 0)
              for p in response.get_json()["photos"]}
    assert listed == {name: (p["flag"], p["rating"]) for name, p in before["photos"].items()}


def test_release_catalog_written_by_its_release_holds_the_whole_dataset():
    """Every snapshot shows the full dataset, so the upgrade test checks all of it."""
    assert RELEASES, "no release catalogs found"
    for release in RELEASES:
        snapshot = json.loads((FIXTURE_DIR / f"{release}.json").read_text())
        assert snapshot["release"] == release
        assert sorted(snapshot["photos"]) == sorted(p["filename"] for p in writer.PHOTOS)
        assert any(p["masks"] for p in snapshot["photos"].values()), release
        assert any(p["keywords"] for p in snapshot["photos"].values()), release
        assert {p["flag"] for p in snapshot["photos"].values()} >= {"flagged", "rejected"}, release


def test_the_current_version_has_a_release_catalog():
    """``scripts/release.sh`` saves each release's catalog in the release commit.

    A missing one means a release was cut without it; write it from the
    release tag with ``python scripts/release_catalogs.py make v<version>``.
    """
    with open(REPO_ROOT / "pyproject.toml", "rb") as f:
        version = "v" + tomllib.load(f)["project"]["version"]
    assert version in RELEASES
