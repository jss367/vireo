"""What one Browse search keystroke costs, in SQLite work rather than seconds.

A keystroke re-issues the grid query, the calendar and the summary. Their
cost must scale with the photos a term can match, not with how much EXIF
the catalog stores: a real catalog holds ~10 KB of EXIF per photo, and
walking all of it for every keystroke is what made searches take minutes
(v0.57.0). The fixture catalogs here are tiny, so wall-clock time never
shows the problem; SQLite's virtual-machine step count does, identically
on every machine.
"""

import json
from urllib.parse import quote

import pytest

PHOTOS = 150
# SQLite reports progress every this many VM steps; the count is steps / this.
STEP_GRAIN = 100
# How much more a keystroke may cost when every photo carries a full EXIF
# blob than when it carries a one-tag stub. Reading a longer raw string is
# a single C call, so a keystroke that never parses non-matching EXIF stays
# near 1x; walking the JSON costs one row per tag, which is far past this.
MAX_EXIF_COST_RATIO = 1.5


def realistic_exif(index):
    """~10 KB and ~300 values, shaped like exiftool's grouped JSON output.

    Values stay clear of every search term below, so no photo carrying
    this EXIF matches: all the work a term does on these rows is overhead.
    """
    groups = {}
    for group in ("EXIF", "MakerNotes", "Composite", "XMP", "File", "IPTC"):
        groups[group] = {
            f"{group}Tag{tag:03d}": (
                f"Setting value {group.lower()} {tag} for frame {index}"
                if tag % 3 else tag * 7 + 0.25
            )
            for tag in range(50)
        }
    return json.dumps(groups)


@pytest.fixture
def search_catalog(app_and_db, monkeypatch):
    """The app with PHOTOS extra photos, and a meter on every request DB."""
    import db as db_module
    from web import app_hooks

    app, db = app_and_db
    folder = db.add_folder("/photos/Survey", name="Survey")
    db.conn.executemany(
        "INSERT INTO photos (folder_id, filename, extension, file_size, file_mtime, "
        "timestamp) VALUES (?, ?, '.jpg', 1000, 1.0, '2019-05-01T08:00:00')",
        [(folder, f"frame{i:04d}.jpg") for i in range(PHOTOS)],
    )
    db.conn.commit()

    ticks = [0]

    class MeteredDatabase(db_module.Database):
        def __init__(self, *args, **kwargs):
            super().__init__(*args, **kwargs)

            def tick():
                ticks[0] += 1
                return 0

            self.conn.set_progress_handler(tick, STEP_GRAIN)

    monkeypatch.setattr(app_hooks, "Database", MeteredDatabase)
    client = app.test_client()

    def set_exif(make_exif):
        db.conn.executemany(
            "UPDATE photos SET exif_data=? WHERE folder_id=? AND filename=?",
            [(make_exif(i), folder, f"frame{i:04d}.jpg") for i in range(PHOTOS)],
        )
        db.conn.commit()

    def keystroke(term):
        """Run the three reads Browse issues for ``term``; return their cost."""
        rules = [{"field": "metadata", "op": "contains", "value": term}]
        encoded = quote(json.dumps(rules))
        ticks[0] = 0
        grid = client.post("/api/photos/query",
                           json={"rules": rules, "page": 1, "per_page": 50})
        calendar = client.get(f"/api/photos/calendar?year=2019&rules={encoded}")
        summary = client.get(f"/api/browse/summary?rules={encoded}")
        for response in (grid, calendar, summary):
            assert response.status_code == 200, response.get_json()
        # None of the Survey photos match: the grid only holds fixture photos.
        assert all(not p["filename"].startswith("frame")
                   for p in grid.get_json()["photos"])
        return ticks[0]

    return set_exif, keystroke


# "Tag" is in every tag name and no value: a raw-text check on the EXIF JSON
# lets it through for every photo, as "long" was for every GPSLongitude.
# Number-only terms can't use a raw-text check at all, since SQLite
# re-renders stored numbers. The stored tag values serve both.
@pytest.mark.parametrize("term", ["zzqxv", "hawk", "Canon EOS", "Tag", "2024", "1.4"])
def test_keystroke_cost_does_not_grow_with_catalog_exif(search_catalog, term):
    set_exif, keystroke = search_catalog
    set_exif(lambda i: json.dumps({"EXIF": {"Make": "Nikon"}}))
    stub_cost = keystroke(term)
    set_exif(realistic_exif)
    full_cost = keystroke(term)
    assert full_cost <= stub_cost * MAX_EXIF_COST_RATIO, (
        f"searching {term!r} cost {full_cost / stub_cost:.1f}x more SQLite work "
        f"once each photo carried realistic EXIF ({stub_cost} -> {full_cost} "
        f"x{STEP_GRAIN} steps): non-matching EXIF is being parsed per keystroke"
    )
