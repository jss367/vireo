"""Tests for scripts/report_release_downloads.py."""

import importlib.util
from pathlib import Path

SCRIPT = Path(__file__).resolve().parent.parent / "scripts" / "report_release_downloads.py"
SPEC = importlib.util.spec_from_file_location("report_release_downloads", SCRIPT)
assert SPEC and SPEC.loader
reporter = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(reporter)


def _release(tag, published_at, assets, *, draft=False):
    return {
        "tag_name": tag,
        "published_at": published_at,
        "draft": draft,
        "assets": [
            {"name": name, "download_count": count} for name, count in assets
        ],
    }


def test_summarize_release_counts_only_public_installer_formats():
    release = _release(
        "v1.2.3",
        "2026-07-12T00:00:00Z",
        [
            ("Vireo_1.2.3_aarch64.dmg", 4),
            ("Vireo_1.2.3_x64.dmg", 2),
            ("Vireo_1.2.3_x64-setup.exe", 3),
            ("Vireo_1.2.3_amd64.deb", 5),
            ("Vireo_1.2.3_amd64.AppImage", 20),
            ("Vireo_aarch64.app.tar.gz", 30),
            ("latest.json", 40),
        ],
    )

    summary = reporter.summarize_release(release)

    assert summary["downloads"] == {"macos": 6, "windows": 3, "linux": 5}
    assert summary["total"] == 14
    assert len(summary["assets"]) == 4


def test_build_report_skips_drafts_and_unpublished_releases_and_applies_limit():
    releases = [
        _release("v3", "2026-07-12T00:00:00Z", [("Vireo_3_aarch64.dmg", 3)]),
        _release("draft", None, [("Vireo_draft_aarch64.dmg", 99)], draft=True),
        _release("v2", "2026-07-11T00:00:00Z", [("Vireo_2_amd64.deb", 2)]),
    ]

    report = reporter.build_report(releases, "owner/repo", limit=1)

    assert report["release_count"] == 1
    assert report["downloads"] == {"macos": 3, "windows": 0, "linux": 0}
    assert report["releases"][0]["version"] == "v3"


def test_markdown_explains_windows_update_overlap():
    report = reporter.build_report(
        [
            _release(
                "v1",
                "2026-07-12T00:00:00Z",
                [("Vireo_1_x64-setup.exe", 7)],
            )
        ],
        "owner/repo",
        limit=20,
    )

    rendered = reporter.render_markdown(report)

    assert "| v1 | 2026-07-12 | 0 | 7 | 0 | 7 |" in rendered
    assert "automatic updates" in rendered
