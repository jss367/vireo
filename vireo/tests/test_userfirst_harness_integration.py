"""Integration tests — actually start Flask + Playwright + sweep.

Slow (several seconds each). Skipped if playwright or chromium aren't available.
"""
import json
from pathlib import Path

import pytest

playwright = pytest.importorskip("playwright.sync_api")


@pytest.fixture
def test_profile(tmp_path, monkeypatch):
    """Set up an isolated test profile + photos root.

    The harness passes HOME=<profile>/fake_home to the Flask subprocess, so
    the subprocess's ~/.vireo/* reads/writes are isolated. The parent
    (pytest) process keeps its real HOME so Playwright finds Chromium.
    """
    profile = tmp_path / "vireo-test-profile"
    photos = tmp_path / "vireo-test-photos"
    profile.mkdir()
    photos.mkdir()
    monkeypatch.setenv("VIREO_PROFILE", str(profile))
    monkeypatch.setenv("VIREO_TEST_PHOTOS", str(photos))
    return profile, photos


def _chromium_installed():
    cache = Path.home() / "Library" / "Caches" / "ms-playwright"
    if not cache.exists():
        cache = Path.home() / ".cache" / "ms-playwright"
    return cache.exists() and any(p.name.startswith("chromium") for p in cache.iterdir())


pytestmark = pytest.mark.skipif(
    not _chromium_installed(), reason="Playwright Chromium not installed"
)


def test_harness_starts_and_stops_cleanly(test_profile):
    profile, _ = test_profile
    from testing.userfirst.harness import vireo_session

    with vireo_session(name="smoke") as session:
        resp = session.goto("/api/health")
        assert resp is not None
        assert resp.status == 200

    runs = list((profile / "runs").iterdir())
    assert len(runs) == 1
    findings_json = runs[0] / "findings.json"
    assert findings_json.exists()
    data = json.loads(findings_json.read_text())
    assert data["name"] == "smoke"


def test_sweep_runs_without_crash(test_profile):
    profile, _ = test_profile
    from testing.userfirst.harness import vireo_session
    from testing.userfirst.sweep import run_sweep

    with vireo_session(name="sweep-test") as session:
        run_sweep(session, pages=["/welcome", "/settings"])

    runs = list((profile / "runs").iterdir())
    assert len(runs) == 1
    screens = list((runs[0] / "screens").iterdir())
    assert len(screens) >= 2
    report_md = (runs[0] / "report.md").read_text()
    assert "sweep-test" in report_md


def test_sweep_flags_missing_static_asset(test_profile, monkeypatch):
    """Inject a bogus <script src="/static/does-not-exist.js"> into a template,
    run sweep, verify the harness surfaces the 404 as a BUG finding. This is
    the regression guard for the bug that motivated this entire harness.
    """
    profile, _ = test_profile
    from testing.userfirst.harness import vireo_session
    from testing.userfirst.sweep import run_sweep

    # Find and patch the welcome template
    template = Path(__file__).parent.parent / "templates" / "welcome.html"
    original = template.read_text()
    injected = original.replace(
        "</body>",
        '<script src="/static/definitely-missing-12345.js"></script></body>',
        1,
    )
    template.write_text(injected)
    try:
        with vireo_session(name="missing-asset") as session:
            run_sweep(session, pages=["/welcome"])

        runs = sorted((profile / "runs").iterdir())
        findings = json.loads((runs[-1] / "findings.json").read_text())
        bug_urls = [
            f["context"].get("url")
            for f in findings["findings"]
            if f["kind"] == "BUG"
        ]
        assert any("definitely-missing-12345" in (u or "") for u in bug_urls), (
            f"expected missing-asset BUG in findings, got: {findings['findings']}"
        )
    finally:
        template.write_text(original)


def test_report_is_written_when_session_body_raises(test_profile):
    """Even if the `with` body crashes, findings.json + report.md must be on
    disk — otherwise the exact failures we most want to diagnose vanish.
    """
    profile, _ = test_profile
    from testing.userfirst.harness import vireo_session
    from testing.userfirst.report import Finding

    with pytest.raises(RuntimeError, match="boom"):
        with vireo_session(name="crash-test") as session:
            session.report.add(Finding.bug("recorded before the crash"))
            raise RuntimeError("boom")

    runs = list((profile / "runs").iterdir())
    assert len(runs) == 1
    findings = json.loads((runs[0] / "findings.json").read_text())
    assert findings["name"] == "crash-test"
    messages = [f["message"] for f in findings["findings"]]
    assert any("recorded before the crash" in m for m in messages)
    assert (runs[0] / "report.md").exists()


def test_prune_runs_enforces_cap(test_profile):
    profile, _ = test_profile
    from testing.userfirst.harness import vireo_session

    for _ in range(3):
        with vireo_session(name="prune-test", keep_runs=2):
            pass

    runs = list((profile / "runs").iterdir())
    assert len(runs) == 2
