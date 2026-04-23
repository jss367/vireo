"""Integration tests for Tier-1 user-first scenarios.

Each test starts Vireo in a subprocess with seeded data, runs a scenario
via Playwright, and asserts the report contains no BUG findings.

Requirements:
  - ``playwright`` with Chromium installed (``playwright install chromium``)
  - ``Pillow`` for thumbnail generation
"""
from pathlib import Path

import pytest

try:
    from playwright.sync_api import sync_playwright as _sync_playwright  # noqa: F401

    _HAS_PLAYWRIGHT = True
except ImportError:
    _HAS_PLAYWRIGHT = False


def _chromium_installed():
    if not _HAS_PLAYWRIGHT:
        return False
    cache = Path.home() / "Library" / "Caches" / "ms-playwright"
    if not cache.exists():
        cache = Path.home() / ".cache" / "ms-playwright"
    if not cache.exists():
        return False
    return any(p.name.startswith("chromium") for p in cache.iterdir())


pytestmark = pytest.mark.skipif(
    not _chromium_installed(),
    reason="playwright is not installed or chromium is not available",
)


@pytest.fixture()
def userfirst_env(tmp_path, monkeypatch):
    """Set up env vars pointing at a temp profile + fake photos root."""
    profile_dir = tmp_path / "profile"
    profile_dir.mkdir()
    photos_root = tmp_path / "photos"
    photos_root.mkdir()
    monkeypatch.setenv("VIREO_PROFILE", str(profile_dir))
    monkeypatch.setenv("VIREO_TEST_PHOTOS", str(photos_root))
    return {"profile": profile_dir, "photos_root": photos_root}


# ---------------------------------------------------------------------------
# Scenarios
# ---------------------------------------------------------------------------

def test_browse_scenario(userfirst_env):
    from vireo.testing.userfirst.harness import vireo_session
    from vireo.testing.userfirst.scenarios import browse
    from vireo.testing.userfirst.seeds import browse_seed

    with vireo_session(name="browse", seed=browse_seed) as session:
        browse.run(session)

    report = session.report
    if report.has_bugs():
        msg = "\n".join(
            f"  [{f.kind}] {f.message} {f.context}" for f in report.findings
        )
        pytest.fail(f"browse scenario reported bugs:\n{msg}")


def test_cull_scenario(userfirst_env):
    from vireo.testing.userfirst.harness import vireo_session
    from vireo.testing.userfirst.scenarios import cull
    from vireo.testing.userfirst.seeds import browse_seed

    with vireo_session(name="cull", seed=browse_seed) as session:
        cull.run(session)

    report = session.report
    if report.has_bugs():
        msg = "\n".join(
            f"  [{f.kind}] {f.message} {f.context}" for f in report.findings
        )
        pytest.fail(f"cull scenario reported bugs:\n{msg}")


def test_rate_flag_scenario(userfirst_env):
    from vireo.testing.userfirst.harness import vireo_session
    from vireo.testing.userfirst.scenarios import rate_flag
    from vireo.testing.userfirst.seeds import browse_seed

    with vireo_session(name="rate_flag", seed=browse_seed) as session:
        rate_flag.run(session)

    report = session.report
    if report.has_bugs():
        msg = "\n".join(
            f"  [{f.kind}] {f.message} {f.context}" for f in report.findings
        )
        pytest.fail(f"rate_flag scenario reported bugs:\n{msg}")


def test_scan_scenario(userfirst_env):
    from vireo.testing.userfirst.harness import vireo_session
    from vireo.testing.userfirst.scenarios import scan
    from vireo.testing.userfirst.seeds import browse_seed

    with vireo_session(name="scan", seed=browse_seed) as session:
        scan.run(session)

    report = session.report
    if report.has_bugs():
        msg = "\n".join(
            f"  [{f.kind}] {f.message} {f.context}" for f in report.findings
        )
        pytest.fail(f"scan scenario reported bugs:\n{msg}")


def test_pipeline_review_scenario(userfirst_env):
    from vireo.testing.userfirst.harness import vireo_session
    from vireo.testing.userfirst.scenarios import pipeline_review
    from vireo.testing.userfirst.seeds import browse_seed

    with vireo_session(name="pipeline_review", seed=browse_seed) as session:
        pipeline_review.run(session)

    report = session.report
    if report.has_bugs():
        msg = "\n".join(
            f"  [{f.kind}] {f.message} {f.context}" for f in report.findings
        )
        pytest.fail(f"pipeline_review scenario reported bugs:\n{msg}")


def test_keywords_scenario(userfirst_env):
    from vireo.testing.userfirst.harness import vireo_session
    from vireo.testing.userfirst.scenarios import keywords
    from vireo.testing.userfirst.seeds import browse_seed

    with vireo_session(name="keywords", seed=browse_seed) as session:
        keywords.run(session)

    report = session.report
    if report.has_bugs():
        msg = "\n".join(
            f"  [{f.kind}] {f.message} {f.context}" for f in report.findings
        )
        pytest.fail(f"keywords scenario reported bugs:\n{msg}")


def test_workspaces_scenario(userfirst_env):
    from vireo.testing.userfirst.harness import vireo_session
    from vireo.testing.userfirst.scenarios import workspaces
    from vireo.testing.userfirst.seeds import browse_seed

    with vireo_session(name="workspaces", seed=browse_seed) as session:
        workspaces.run(session)

    report = session.report
    if report.has_bugs():
        msg = "\n".join(
            f"  [{f.kind}] {f.message} {f.context}" for f in report.findings
        )
        pytest.fail(f"workspaces scenario reported bugs:\n{msg}")


def test_duplicates_scenario(userfirst_env):
    from vireo.testing.userfirst.harness import vireo_session
    from vireo.testing.userfirst.scenarios import duplicates
    from vireo.testing.userfirst.seeds import browse_seed

    with vireo_session(name="duplicates", seed=browse_seed) as session:
        duplicates.run(session)

    report = session.report
    if report.has_bugs():
        msg = "\n".join(
            f"  [{f.kind}] {f.message} {f.context}" for f in report.findings
        )
        pytest.fail(f"duplicates scenario reported bugs:\n{msg}")


def test_misses_scenario(userfirst_env):
    from vireo.testing.userfirst.harness import vireo_session
    from vireo.testing.userfirst.scenarios import misses
    from vireo.testing.userfirst.seeds import misses_seed

    with vireo_session(name="misses", seed=misses_seed) as session:
        misses.run(session)

    report = session.report
    if report.has_bugs():
        msg = "\n".join(
            f"  [{f.kind}] {f.message} {f.context}" for f in report.findings
        )
        pytest.fail(f"misses scenario reported bugs:\n{msg}")


def test_map_geo_scenario(userfirst_env):
    from vireo.testing.userfirst.harness import vireo_session
    from vireo.testing.userfirst.scenarios import map_geo
    from vireo.testing.userfirst.seeds import browse_seed

    with vireo_session(name="map_geo", seed=browse_seed) as session:
        map_geo.run(session)

    report = session.report
    if report.has_bugs():
        msg = "\n".join(
            f"  [{f.kind}] {f.message} {f.context}" for f in report.findings
        )
        pytest.fail(f"map_geo scenario reported bugs:\n{msg}")


# ---------------------------------------------------------------------------
# Regression scenarios — each one guards against a specific past bug.
# ---------------------------------------------------------------------------

def test_browse_lightbox_arrows_regression(userfirst_env):
    """Regression guard for #598: lightbox arrows navigate photos from /browse."""
    from vireo.testing.userfirst.harness import vireo_session
    from vireo.testing.userfirst.scenarios import browse_lightbox
    from vireo.testing.userfirst.seeds import browse_seed

    with vireo_session(name="browse_lightbox", seed=browse_seed) as session:
        browse_lightbox.run(session)

    report = session.report
    if report.has_bugs():
        msg = "\n".join(
            f"  [{f.kind}] {f.message} {f.context}" for f in report.findings
        )
        pytest.fail(f"browse_lightbox scenario reported bugs:\n{msg}")


def test_browse_folders_orphan_parent_regression(userfirst_env):
    """Regression guard for #597: orphan-parent folders stay visible on /browse."""
    from vireo.testing.userfirst.harness import vireo_session
    from vireo.testing.userfirst.scenarios import browse_folders
    from vireo.testing.userfirst.seeds import orphan_folder_seed

    with vireo_session(name="browse_folders", seed=orphan_folder_seed) as session:
        browse_folders.run(session)

    report = session.report
    if report.has_bugs():
        msg = "\n".join(
            f"  [{f.kind}] {f.message} {f.context}" for f in report.findings
        )
        pytest.fail(f"browse_folders scenario reported bugs:\n{msg}")


def test_browse_multiselect_shortcut_regression(userfirst_env):
    """Regression guard for #601: keyboard shortcuts act on full multi-selection."""
    from vireo.testing.userfirst.harness import vireo_session
    from vireo.testing.userfirst.scenarios import browse_multiselect
    from vireo.testing.userfirst.seeds import browse_seed

    with vireo_session(name="browse_multiselect", seed=browse_seed) as session:
        browse_multiselect.run(session)

    report = session.report
    if report.has_bugs():
        msg = "\n".join(
            f"  [{f.kind}] {f.message} {f.context}" for f in report.findings
        )
        pytest.fail(f"browse_multiselect scenario reported bugs:\n{msg}")
