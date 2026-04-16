"""Generic route-walker for user-first testing.

Visits every registered page route, captures console/network errors and a
screenshot per page. Finds the "missing static asset" / "broken wire"
class of bug that code review can't see.
"""

# Ordered page routes — only user-facing pages, no /api/* or /<int:id>.
DEFAULT_PAGES = [
    "/",
    "/welcome",
    "/dashboard",
    "/browse",
    "/review",
    "/lightroom",
    "/audit",
    "/cull",
    "/pipeline",
    "/pipeline/review",
    "/variants",
    "/workspace",
    "/compare",
    "/settings",
    "/shortcuts",
    "/keywords",
    "/jobs",
    "/duplicates",
    "/move",
    "/highlights",
    "/map",
    "/logs",
]


def run_sweep(session, pages=None):
    """Visit each page in `pages`, capture findings on every one.

    Returns the session's report (same one that's mutated during the run).

    Uses `wait_until="load"` instead of the default `networkidle`: some pages
    (e.g. `/logs`) open a long-lived `EventSource` on load, which means the
    network never goes idle and `networkidle` would time out and record a
    false BUG finding for an otherwise-healthy page.
    """
    if pages is None:
        pages = DEFAULT_PAGES
    for path in pages:
        label = path.strip("/").replace("/", "_") or "root"
        session.goto(path, wait_until="load")
        session.screenshot(f"sweep-{label}")
    return session.report
