#!/usr/bin/env python3
"""Report public Vireo installer download counts from GitHub Releases."""

from __future__ import annotations

import argparse
import json
import os
import sys
import urllib.error
import urllib.request
from datetime import UTC, datetime
from typing import Any

DEFAULT_REPOSITORY = "jss367/vireo"
PLATFORM_SUFFIXES = {
    "macos": ".dmg",
    "windows": "-setup.exe",
    "linux": ".deb",
}


def installer_platform(name: str) -> str | None:
    """Return the website installer platform for a release asset name."""
    for platform, suffix in PLATFORM_SUFFIXES.items():
        if name.endswith(suffix):
            return platform
    return None


def fetch_releases(repository: str, token: str | None = None) -> list[dict[str, Any]]:
    """Fetch every release visible to the supplied GitHub credentials."""
    releases: list[dict[str, Any]] = []
    page = 1
    while True:
        url = f"https://api.github.com/repos/{repository}/releases?per_page=100&page={page}"
        headers = {
            "Accept": "application/vnd.github+json",
            "User-Agent": "vireo-download-report",
            "X-GitHub-Api-Version": "2022-11-28",
        }
        if token:
            headers["Authorization"] = f"Bearer {token}"

        request = urllib.request.Request(url, headers=headers)
        with urllib.request.urlopen(request, timeout=30) as response:
            batch = json.load(response)
        if not isinstance(batch, list):
            raise ValueError("GitHub returned an unexpected response")
        releases.extend(batch)
        if len(batch) < 100:
            break
        page += 1
    return releases


def summarize_release(release: dict[str, Any]) -> dict[str, Any]:
    """Reduce a GitHub release to installer counts and contributing assets."""
    counts = {platform: 0 for platform in PLATFORM_SUFFIXES}
    assets = []
    for asset in release.get("assets", []):
        platform = installer_platform(asset.get("name", ""))
        if platform is None:
            continue
        count = int(asset.get("download_count", 0))
        counts[platform] += count
        assets.append(
            {
                "name": asset["name"],
                "platform": platform,
                "downloads": count,
            }
        )

    return {
        "version": release["tag_name"],
        "published_at": release["published_at"],
        "downloads": counts,
        "total": sum(counts.values()),
        "assets": assets,
    }


def build_report(
    releases: list[dict[str, Any]], repository: str, limit: int
) -> dict[str, Any]:
    """Build a stable, serializable report from published GitHub releases."""
    published = [
        release
        for release in releases
        if release.get("published_at") and not release.get("draft", False)
    ]
    if limit > 0:
        published = published[:limit]
    summaries = [summarize_release(release) for release in published]
    totals = {
        platform: sum(item["downloads"][platform] for item in summaries)
        for platform in PLATFORM_SUFFIXES
    }
    return {
        "repository": repository,
        "collected_at": datetime.now(UTC).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "release_count": len(summaries),
        "downloads": totals,
        "total": sum(totals.values()),
        "releases": summaries,
        "notes": [
            "Counts are asset downloads, not unique people or confirmed installations.",
            "Windows -setup.exe counts include both manual downloads and automatic updates.",
        ],
    }


def render_markdown(report: dict[str, Any]) -> str:
    """Render a compact human-readable report."""
    lines = [
        f"# Installer downloads for {report['repository']}",
        "",
        f"Collected: {report['collected_at']}",
        "",
        "| Release | Published | macOS | Windows* | Linux | Total |",
        "| --- | --- | ---: | ---: | ---: | ---: |",
    ]
    for release in report["releases"]:
        counts = release["downloads"]
        lines.append(
            f"| {release['version']} | {release['published_at'][:10]} | "
            f"{counts['macos']} | {counts['windows']} | {counts['linux']} | "
            f"{release['total']} |"
        )
    totals = report["downloads"]
    lines.extend(
        [
            f"| **Reported total** |  | **{totals['macos']}** | "
            f"**{totals['windows']}** | **{totals['linux']}** | "
            f"**{report['total']}** |",
            "",
            "*Windows uses the same `-setup.exe` asset for manual installs and automatic updates.",
            "",
            "Counts are cumulative GitHub asset downloads, not unique people or confirmed installations.",
        ]
    )
    return "\n".join(lines)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--repository", default=DEFAULT_REPOSITORY, metavar="OWNER/REPO")
    parser.add_argument(
        "--limit",
        type=int,
        default=20,
        help="number of published releases to show; 0 shows all (default: 20)",
    )
    parser.add_argument("--json", action="store_true", help="emit structured JSON")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    if args.limit < 0:
        print("error: --limit must be zero or greater", file=sys.stderr)
        return 2
    try:
        releases = fetch_releases(args.repository, os.environ.get("GITHUB_TOKEN"))
        report = build_report(releases, args.repository, args.limit)
    except (urllib.error.URLError, TimeoutError, ValueError, KeyError) as error:
        print(f"error: could not read GitHub releases: {error}", file=sys.stderr)
        return 1

    if args.json:
        print(json.dumps(report, indent=2))
    else:
        print(render_markdown(report))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
