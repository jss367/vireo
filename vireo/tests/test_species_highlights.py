"""One list of highlights per species, ordered by recorded comparisons.

A highlight is catalog-wide, and its number in a species row is its place
among the highlights that row can show. Adding, moving and removing one each
record a comparison, scoped to what the page showed, so a trip's order never
claims anything about another trip.
"""

import json
import sqlite3

import pytest
from canonical_schema import create_species_choices
from highlights_payload import HighlightsQuery, build_highlights_payload
from schema import _merge_species_highlights
from services import keyword_tagging


def _folder(db, path):
    fid = db.conn.execute(
        "INSERT INTO folders (path, name, status) VALUES (?, ?, 'ok')",
        (path, path.strip("/")),
    ).lastrowid
    db.conn.execute(
        "INSERT INTO workspace_folders (workspace_id, folder_id) VALUES (?, ?)",
        (db._ws_id(), fid),
    )
    return fid


def _photo(db, fid, name, timestamp, quality):
    pid = db.conn.execute(
        "INSERT INTO photos (folder_id, filename, timestamp, quality_score, flag) "
        "VALUES (?, ?, ?, ?, 'none')",
        (fid, name, timestamp, quality),
    ).lastrowid
    keyword_tagging.tag_photo(db, pid, db.keywords.add("Bald Eagle", is_species=True))
    return pid


@pytest.fixture
def trips(app_and_db):
    """Two trips with eagles; Vireo's score prefers the later photo of each."""
    app, db = app_and_db
    first = _folder(db, "/trip-2024")
    second = _folder(db, "/trip-2025")
    photos = {
        "a1": _photo(db, first, "a1.jpg", "2024-05-01T08:00:00", 0.4),
        "a2": _photo(db, first, "a2.jpg", "2024-05-01T08:01:00", 0.6),
        "a3": _photo(db, first, "a3.jpg", "2024-05-01T08:02:00", 0.9),
        "b1": _photo(db, second, "b1.jpg", "2025-05-01T08:00:00", 0.3),
        "b2": _photo(db, second, "b2.jpg", "2025-05-01T08:01:00", 0.8),
    }
    db.commit()
    return app, db, {"first": first, "second": second}, photos


def _add(client, photo_id, **scope):
    return client.post(
        "/api/species-highlights",
        json={"species": "Bald Eagle", "photo_id": photo_id, **scope},
    )


def _move(client, photo_id, direction, **extra):
    return client.patch(
        "/api/species-highlights/order",
        json={"species": "Bald Eagle", "photo_id": photo_id, "direction": direction, **extra},
    )


def _row(db, folder_id=None):
    """The eagle row as the Highlights page gets it for a folder or everything."""
    payload = build_highlights_payload(db, HighlightsQuery(
        scope="workspace" if folder_id is None else "folder",
        folder_id=folder_id, min_quality=0.0,
    ))
    return next(b for b in payload["buckets"] if b["species"] == "Bald Eagle")


def _highlights(bucket):
    return [(p["id"], p["highlight_rank"]) for p in bucket["photos"] if p["is_highlighted"]]


def test_a_new_highlight_takes_the_next_number_in_the_folder_it_was_added_in(trips):
    app, db, folders, photos = trips
    client = app.test_client()

    first = _add(client, photos["a1"], folder_id=folders["first"]).get_json()
    second = _add(client, photos["a2"], folder_id=folders["first"]).get_json()

    assert first["added"] is True and first["after"] == []
    assert second["added"] is True and second["after"] == [photos["a1"]]
    bucket = _row(db, folders["first"])
    assert _highlights(bucket) == [(photos["a1"], 1), (photos["a2"], 2)]
    # Highlights lead the row even though Vireo scores a3 highest.
    assert [p["id"] for p in bucket["photos"]][:3] == [
        photos["a1"], photos["a2"], photos["a3"],
    ]
    assert bucket["highlight_count"] == 2
    assert bucket["best_decision"]["kind"] == "highlight"
    assert bucket["best_decision"]["photo_id"] == photos["a1"]


def test_adding_a_highlight_twice_changes_nothing(trips):
    app, db, folders, photos = trips
    client = app.test_client()
    _add(client, photos["a1"], folder_id=folders["first"])
    _add(client, photos["a2"], folder_id=folders["first"])

    again = _add(client, photos["a1"], folder_id=folders["first"]).get_json()

    assert again["added"] is False
    assert _highlights(_row(db, folders["first"])) == [
        (photos["a1"], 1), (photos["a2"], 2),
    ]


def test_highlights_of_two_trips_are_numbered_per_trip_and_tie_across_them(trips):
    app, db, folders, photos = trips
    client = app.test_client()
    _add(client, photos["a1"], folder_id=folders["first"])
    _add(client, photos["a2"], folder_id=folders["first"])

    added = _add(client, photos["b1"], folder_id=folders["second"]).get_json()

    # Added in the second trip, it claims nothing about the first trip's.
    assert added["after"] == []
    assert _highlights(_row(db, folders["second"])) == [(photos["b1"], 1)]
    assert _row(db, folders["second"])["best_decision"]["kind"] == "highlight"
    everything = _row(db)
    decision = everything["best_decision"]
    assert decision["kind"] == "tie"
    assert set(decision["tied_photo_ids"]) == {photos["a1"], photos["b1"]}
    # The newest highlight leads meanwhile; a1 stays ahead of a2.
    assert _highlights(everything) == [
        (photos["b1"], 1), (photos["a1"], 2), (photos["a2"], 3),
    ]


def test_up_on_the_first_highlight_settles_a_tie_in_its_favor(trips):
    app, db, folders, photos = trips
    client = app.test_client()
    _add(client, photos["a1"], folder_id=folders["first"])
    _add(client, photos["b1"], folder_id=folders["second"])
    assert _row(db)["best_decision"]["kind"] == "tie"

    moved = _move(client, photos["b1"], "up").get_json()

    assert moved["moved"] is True and moved["over"] == [photos["a1"]]
    everything = _row(db)
    assert everything["best_decision"]["kind"] == "highlight"
    assert _highlights(everything) == [(photos["b1"], 1), (photos["a1"], 2)]


def test_up_swaps_a_highlight_with_the_neighbor_the_page_showed(trips):
    app, db, folders, photos = trips
    client = app.test_client()
    for key in ("a1", "a2", "a3"):
        _add(client, photos[key], folder_id=folders["first"])

    moved = _move(
        client, photos["a3"], "up",
        neighbor_id=photos["a2"], folder_id=folders["first"],
    ).get_json()

    assert moved["moved"] is True and moved["over"] == [photos["a2"]]
    # The older "a2 ahead of a3" is the comparison the move replaced.
    assert moved["superseded"] == [
        {"better_photo_id": photos["a2"], "worse_photo_id": photos["a3"]},
    ]
    assert _row(db, folders["first"])["best_decision"]["kind"] == "highlight"
    assert _highlights(_row(db, folders["first"])) == [
        (photos["a1"], 1), (photos["a3"], 2), (photos["a2"], 3),
    ]


def test_down_moves_a_highlight_behind_the_next_one(trips):
    app, db, folders, photos = trips
    client = app.test_client()
    for key in ("a1", "a2", "a3"):
        _add(client, photos[key], folder_id=folders["first"])

    moved = _move(client, photos["a1"], "down", folder_id=folders["first"]).get_json()

    assert moved["moved"] is True and moved["after"] == [photos["a2"]]
    assert _highlights(_row(db, folders["first"])) == [
        (photos["a2"], 1), (photos["a1"], 2), (photos["a3"], 3),
    ]


def test_down_on_the_last_highlight_changes_nothing(trips):
    app, db, folders, photos = trips
    client = app.test_client()
    _add(client, photos["a1"], folder_id=folders["first"])
    _add(client, photos["a2"], folder_id=folders["first"])

    moved = _move(client, photos["a2"], "down", folder_id=folders["first"]).get_json()

    assert moved["moved"] is False
    assert db.conn.execute("SELECT COUNT(*) FROM species_choices").fetchone()[0] == 2


def test_moving_a_photo_that_is_not_a_highlight_is_refused(trips):
    app, _db, folders, photos = trips
    client = app.test_client()
    _add(client, photos["a1"], folder_id=folders["first"])

    response = _move(client, photos["a2"], "up", folder_id=folders["first"])

    assert response.status_code == 404


def test_a_neighbor_that_is_not_a_highlight_is_refused(trips):
    app, _db, folders, photos = trips
    client = app.test_client()
    _add(client, photos["a1"], folder_id=folders["first"])

    response = _move(
        client, photos["a1"], "down",
        neighbor_id=photos["a2"], folder_id=folders["first"],
    )

    assert response.status_code == 400


@pytest.fixture(params=["folder", "collection", "rules"])
def scoped_highlights(trips, request):
    app, db, folders, photos = trips
    client = app.test_client()
    for key in ("a1", "a2", "b1"):
        folder = folders["second" if key == "b1" else "first"]
        assert _add(client, photos[key], folder_id=folder).status_code == 200
    rules = [{"field": "photo_ids", "value": [photos["a1"], photos["a2"]]}]
    if request.param == "folder":
        scope = {"folder_id": folders["first"]}
    elif request.param == "collection":
        collection = db.collections.add("Synthetic trip", json.dumps(rules))
        db.commit()
        scope = {"collection_id": collection}
    else:
        scope = {"rules": rules}
    return client, db, folders, photos, scope


@pytest.mark.parametrize("direction", ["up", "down"])
def test_neighbor_outside_requested_scope_is_refused_without_changes(scoped_highlights, direction):
    client, db, folders, photos, scope = scoped_highlights
    before = db.species_choices.comparisons()
    before_orders = [_highlights(_row(db, folder)) for folder in folders.values()]

    response = _move(
        client, photos["a1"], direction, neighbor_id=photos["b1"], **scope,
    )

    assert response.status_code == 400
    assert db.species_choices.comparisons() == before
    assert [_highlights(_row(db, folder)) for folder in folders.values()] == before_orders


@pytest.mark.parametrize("direction", ["up", "down"])
def test_neighbor_inside_requested_scope_can_change_order(scoped_highlights, direction):
    client, db, folders, photos, scope = scoped_highlights
    moving, neighbor = (photos["a2"], photos["a1"])
    if direction == "down":
        moving, neighbor = neighbor, moving

    response = _move(client, moving, direction, neighbor_id=neighbor, **scope)

    assert response.status_code == 200
    assert response.get_json()["moved"] is True
    assert _highlights(_row(db, folders["first"])) == [
        (photos["a2"], 1), (photos["a1"], 2),
    ]
    assert _highlights(_row(db, folders["second"])) == [(photos["b1"], 1)]


def test_removing_the_middle_highlight_keeps_the_others_in_order(trips):
    app, db, folders, photos = trips
    client = app.test_client()
    for key in ("a1", "a2", "a3"):
        _add(client, photos[key], folder_id=folders["first"])

    removed = client.delete(
        "/api/species-highlights",
        json={"species": "Bald Eagle", "photo_id": photos["a2"]},
    ).get_json()

    assert removed["removed"] == 1
    bucket = _row(db, folders["first"])
    assert _highlights(bucket) == [(photos["a1"], 1), (photos["a3"], 2)]
    # Not a tie: a1 stayed ahead of a3 through the removed photo.
    assert bucket["best_decision"]["kind"] == "highlight"


def test_a_removed_highlight_returns_at_the_end_not_at_its_old_place(trips):
    app, db, folders, photos = trips
    client = app.test_client()
    for key in ("a1", "a2"):
        _add(client, photos[key], folder_id=folders["first"])
    client.delete(
        "/api/species-highlights",
        json={"species": "Bald Eagle", "photo_id": photos["a1"]},
    )

    _add(client, photos["a1"], folder_id=folders["first"])

    assert _highlights(_row(db, folders["first"])) == [
        (photos["a2"], 1), (photos["a1"], 2),
    ]


def test_make_top_highlight_puts_a_photo_first_among_the_folders_highlights(trips):
    app, db, folders, photos = trips
    client = app.test_client()
    _add(client, photos["a1"], folder_id=folders["first"])
    _add(client, photos["a2"], folder_id=folders["first"])
    _add(client, photos["b1"], folder_id=folders["second"])

    response = client.post("/api/photo-preferences", json={
        "purpose": "species_representative", "species": "Bald Eagle",
        "photo_id": photos["a3"], "folder_id": folders["first"],
    })

    assert response.status_code == 200
    assert set(response.get_json()["over"]) == {photos["a1"], photos["a2"]}
    assert _highlights(_row(db, folders["first"])) == [
        (photos["a3"], 1), (photos["a1"], 2), (photos["a2"], 3),
    ]
    # It was never compared with the other trip's highlight.
    assert _row(db)["best_decision"]["kind"] == "tie"


def test_a_hand_reorder_keeps_the_reason_an_earlier_choice_gave(trips):
    app, db, folders, photos = trips
    client = app.test_client()
    client.post("/api/species-best/choose", json={
        "species": "Bald Eagle", "photo_id": photos["a1"],
        "folder_id": folders["first"], "source": "claude", "reason": "eye is sharp",
    })

    _add(client, photos["a2"], folder_id=folders["first"])
    _move(client, photos["a1"], "up", folder_id=folders["first"])

    decision = _row(db, folders["first"])["best_decision"]
    assert decision["kind"] == "highlight"
    assert decision["source"] == "claude"
    assert decision["reason"] == "eye is sharp"


def test_an_added_highlight_records_who_added_it_and_why(trips):
    app, db, folders, photos = trips
    client = app.test_client()

    client.post("/api/species-highlights", json={
        "species": "Bald Eagle", "photo_id": photos["a1"],
        "folder_id": folders["first"], "source": "claude", "reason": "shows the wings",
    })

    decision = _row(db, folders["first"])["best_decision"]
    assert decision["source"] == "claude"
    assert decision["reason"] == "shows the wings"


def test_the_highlight_filter_keeps_species_with_or_without_one(trips):
    app, db, folders, photos = trips
    client = app.test_client()
    _add(client, photos["a1"], folder_id=folders["first"])

    def species(selection):
        payload = build_highlights_payload(db, HighlightsQuery(
            scope="workspace", min_quality=0.0, highlight_filter=selection,
        ))
        return [b["species"] for b in payload["buckets"]]

    assert species("yes") == ["Bald Eagle"]
    assert species("no") == []


@pytest.mark.parametrize("extra, status", [
    ({"folder_id": "everywhere"}, 400),
    ({"source": "x" * 200}, 400),
    ({"photo_id": 999999}, 404),
])
def test_a_highlight_write_with_a_bad_request_is_refused_and_writes_nothing(
    trips, extra, status,
):
    app, db, folders, photos = trips
    client = app.test_client()
    body = {"species": "Bald Eagle", "photo_id": photos["a1"], **extra}

    responses = [
        client.post("/api/species-highlights", json=body),
        client.patch("/api/species-highlights/order", json={**body, "direction": "up"}),
        client.post("/api/photo-preferences", json={**body, "purpose": "species_representative"}),
        client.post("/api/photo-preferences", json={**body, "purpose": "highlights"}),
    ]

    assert [r.status_code for r in responses] == [status] * 4
    assert db.species_curation.get_highlights() == {}
    assert db.conn.execute("SELECT COUNT(*) FROM species_choices").fetchone()[0] == 0


def _legacy_catalog():
    """A scratch catalog shaped like one before the merge."""
    conn = sqlite3.connect(":memory:")
    conn.executescript(
        """CREATE TABLE photos (id INTEGER PRIMARY KEY);
           CREATE TABLE species_representatives (
               id INTEGER PRIMARY KEY AUTOINCREMENT, species TEXT NOT NULL,
               photo_id INTEGER NOT NULL, selected_order INTEGER NOT NULL,
               created_at TEXT DEFAULT (datetime('now')),
               updated_at TEXT DEFAULT (datetime('now')),
               UNIQUE(species, photo_id));
           CREATE TABLE species_highlights (
               workspace_id INTEGER NOT NULL, species TEXT NOT NULL,
               photo_id INTEGER NOT NULL, rank INTEGER NOT NULL,
               created_at TEXT DEFAULT (datetime('now')),
               updated_at TEXT DEFAULT (datetime('now')),
               PRIMARY KEY (workspace_id, species, photo_id));"""
    )
    create_species_choices(conn)
    return conn


def _migrated_order(conn, species):
    """The species' highlights after the merge, best first."""
    from species_preference import Comparison, rank_contenders

    newest_first = [row[0] for row in conn.execute(
        "SELECT photo_id FROM species_representatives WHERE species = ? "
        "ORDER BY selected_order DESC, id DESC", (species,),
    )]
    comparisons = [Comparison(*row) for row in conn.execute(
        "SELECT c.id, c.photo_id, l.photo_id FROM species_choices c "
        "JOIN species_choice_losers l ON l.choice_id = c.id"
    )]
    return rank_contenders(newest_first, comparisons, newest_first)


def test_the_merge_turns_a_ranked_list_into_highlights_in_the_same_order():
    conn = _legacy_catalog()
    conn.executemany(
        "INSERT INTO species_highlights (workspace_id, species, photo_id, rank) "
        "VALUES (1, 'Verdin', ?, ?)",
        [(10, 1), (11, 2), (12, 3)],
    )

    _merge_species_highlights(conn)

    ranking = _migrated_order(conn, "Verdin")
    assert ranking.order == (10, 11, 12)
    assert not ranking.tied
    assert conn.execute(
        "SELECT 1 FROM sqlite_master WHERE name = 'species_highlights'"
    ).fetchone() is None


def test_the_merge_keeps_a_representative_ahead_of_highlights_that_were_not():
    conn = _legacy_catalog()
    conn.execute(
        "INSERT INTO species_representatives (species, photo_id, selected_order) "
        "VALUES ('Verdin', 12, 1)"
    )
    conn.executemany(
        "INSERT INTO species_highlights (workspace_id, species, photo_id, rank) "
        "VALUES (1, 'Verdin', ?, ?)",
        [(10, 1), (11, 2), (12, 3)],
    )

    _merge_species_highlights(conn)

    # The page led with the representative, then the ranked highlights.
    ranking = _migrated_order(conn, "Verdin")
    assert ranking.order == (12, 10, 11)
    assert not ranking.tied


def test_the_merge_leaves_representatives_nobody_ranked_tied():
    conn = _legacy_catalog()
    conn.executemany(
        "INSERT INTO species_representatives (species, photo_id, selected_order) "
        "VALUES ('Verdin', ?, ?)",
        [(10, 1), (11, 2), (12, 3)],
    )
    conn.executemany(
        "INSERT INTO species_highlights (workspace_id, species, photo_id, rank) "
        "VALUES (1, 'Verdin', ?, ?)",
        [(10, 1), (12, 2)],
    )

    _merge_species_highlights(conn)

    ranking = _migrated_order(conn, "Verdin")
    # 10 was ranked ahead of 12; 11 was never compared with either.
    assert ranking.order == (11, 10, 12)
    assert set(ranking.leaders) == {10, 11}


def test_the_merge_is_a_no_op_on_a_catalog_without_the_old_table():
    conn = _legacy_catalog()
    conn.execute("DROP TABLE species_highlights")

    _merge_species_highlights(conn)

    assert conn.execute("SELECT COUNT(*) FROM species_choices").fetchone()[0] == 0

