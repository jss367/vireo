"""The best photo of each species within a scope, and the choices behind it.

One eagle is photographed on two trips. Choosing the best eagle of each trip
must not claim more than the trip: across both trips the two winners are tied
until someone compares them, and every surface (the best-of-each-species
list, Highlights, the Life List, the representative badge) shows the same
photo for the same set.
"""

import json

import pytest
from highlights_payload import HighlightsQuery, build_highlights_payload, build_life_list_payload
from services import catalog_deletion, keyword_tagging
from services.species_choices import PhotoScope, choose_best, contenders_in_scope


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


def _photo(db, fid, name, timestamp, quality=0.5, species="Bald Eagle"):
    pid = db.conn.execute(
        "INSERT INTO photos (folder_id, filename, timestamp, quality_score, flag) "
        "VALUES (?, ?, ?, ?, 'none')",
        (fid, name, timestamp, quality),
    ).lastrowid
    if species:
        keyword_tagging.tag_photo(db, pid, db.keywords.add(species, is_species=True))
    return pid


@pytest.fixture
def trips(app_and_db):
    """Two trips with two eagles each; the second of each scores higher."""
    app, db = app_and_db
    first = _folder(db, "/trip-2024")
    second = _folder(db, "/trip-2025")
    photos = {
        "a1": _photo(db, first, "a1.jpg", "2024-05-01T08:00:00", 0.4),
        "a2": _photo(db, first, "a2.jpg", "2024-05-01T08:01:00", 0.9),
        "b1": _photo(db, second, "b1.jpg", "2025-05-01T08:00:00", 0.3),
        "b2": _photo(db, second, "b2.jpg", "2025-05-01T08:01:00", 0.8),
    }
    db.commit()
    return app, db, {"first": first, "second": second}, photos


def _best(client, **params):
    data = client.get("/api/species-best", query_string=params).get_json()
    return data, {entry["species"]: entry for entry in data["species"]}


def _choose(client, **body):
    return client.post("/api/species-best/choose", json=body)


def test_unchosen_species_lead_with_vireos_score(trips):
    app, _db, _folders, photos = trips

    data, species = _best(app.test_client())

    eagle = species["Bald Eagle"]
    assert [p["id"] for p in eagle["photos"]] == [photos["a2"]]
    assert eagle["decision"]["kind"] == "score"
    assert eagle["decision"]["contender_ids"] == []
    assert eagle["photo_count"] == 4
    assert data["meta"]["guessed"] == 1
    assert data["meta"]["chosen"] == 0
    assert data["meta"]["needs_comparison"] == 0


def test_a_choice_within_a_trip_leads_that_trip_and_records_who_and_why(trips):
    app, _db, folders, photos = trips
    client = app.test_client()

    response = _choose(
        client, species="Bald Eagle", photo_id=photos["a1"],
        folder_id=folders["first"], source="claude", reason="eye is sharp",
    )

    assert response.status_code == 200
    assert response.get_json()["over"] == []
    _data, species = _best(client, folder_id=folders["first"])
    decision = species["Bald Eagle"]["decision"]
    assert decision["kind"] == "chosen"
    assert decision["photo_id"] == photos["a1"]
    assert decision["source"] == "claude"
    assert decision["reason"] == "eye is sharp"
    assert species["Bald Eagle"]["photos"][0]["is_chosen"] is True
    # The other trip has no chosen photo, so Vireo's score still leads it.
    _data, species = _best(client, folder_id=folders["second"])
    assert species["Bald Eagle"]["decision"]["kind"] == "score"
    assert species["Bald Eagle"]["photos"][0]["id"] == photos["b2"]


def test_winners_of_two_trips_tie_until_compared(trips):
    app, _db, folders, photos = trips
    client = app.test_client()
    _choose(client, species="Bald Eagle", photo_id=photos["a1"], folder_id=folders["first"])
    _choose(client, species="Bald Eagle", photo_id=photos["b1"], folder_id=folders["second"])

    data, species = _best(client)

    decision = species["Bald Eagle"]["decision"]
    assert decision["kind"] == "tie"
    # The newest choice is shown while the tie stands.
    assert decision["photo_id"] == photos["b1"]
    assert decision["tied_photo_ids"] == [photos["b1"], photos["a1"]]
    assert data["meta"]["needs_comparison"] == 1
    # Each trip is settled on its own.
    for folder, winner in (("first", "a1"), ("second", "b1")):
        _data, scoped = _best(client, folder_id=folders[folder])
        assert scoped["Bald Eagle"]["decision"]["kind"] == "chosen"
        assert scoped["Bald Eagle"]["decision"]["photo_id"] == photos[winner]

    response = _choose(
        client, species="Bald Eagle", photo_id=photos["a1"], over=[photos["b1"]],
        reason="cleaner background",
    )

    assert response.get_json()["over"] == [photos["b1"]]
    data, species = _best(client)
    decision = species["Bald Eagle"]["decision"]
    assert decision["kind"] == "chosen"
    assert decision["photo_id"] == photos["a1"]
    assert decision["reason"] == "cleaner background"
    assert decision["contender_ids"] == [photos["a1"], photos["b1"]]
    assert data["meta"]["needs_comparison"] == 0
    # The second trip still leads with its own winner.
    _data, scoped = _best(client, folder_id=folders["second"])
    assert scoped["Bald Eagle"]["decision"]["photo_id"] == photos["b1"]


def test_a_scoped_choice_beats_only_the_chosen_photos_in_the_scope(trips):
    app, db, folders, photos = trips
    client = app.test_client()
    _choose(client, species="Bald Eagle", photo_id=photos["a1"], folder_id=folders["first"])
    _choose(client, species="Bald Eagle", photo_id=photos["b1"], folder_id=folders["second"])

    response = _choose(
        client, species="Bald Eagle", photo_id=photos["a2"], folder_id=folders["first"],
    )

    assert response.get_json()["over"] == [photos["a1"]]
    assert contenders_in_scope(db, "Bald Eagle", PhotoScope(folder_id=folders["first"])) == [
        photos["a2"], photos["a1"],
    ]
    _data, species = _best(client)
    assert species["Bald Eagle"]["decision"]["kind"] == "tie"
    assert species["Bald Eagle"]["decision"]["tied_photo_ids"] == [photos["a2"], photos["b1"]]


def test_a_scoped_choice_refuses_a_photo_outside_the_scope(trips):
    app, db, folders, photos = trips
    client = app.test_client()
    _choose(client, species="Bald Eagle", photo_id=photos["a1"], folder_id=folders["first"])

    response = _choose(
        client, species="Bald Eagle", photo_id=photos["b1"], folder_id=folders["first"],
    )

    assert response.status_code == 400
    assert "not in the supplied scope" in response.get_json()["error"]
    # The first trip's choice stands; b1 recorded no out-of-scope comparison.
    comparisons = [(c.better, c.worse) for c in db.species_choices.comparisons()]
    assert comparisons == []
    assert db.get_species_representatives(eligible_only=True)["Bald Eagle"] == photos["a1"]


def test_a_newer_choice_supersedes_the_comparison_it_contradicts(trips):
    app, db, _folders, photos = trips
    client = app.test_client()
    _choose(client, species="Bald Eagle", photo_id=photos["a1"], over=[photos["b1"]])

    response = _choose(client, species="Bald Eagle", photo_id=photos["b1"], over=[photos["a1"]])

    assert response.get_json()["superseded"] == [
        {"better_photo_id": photos["a1"], "worse_photo_id": photos["b1"]},
    ]
    comparisons = db.species_choices.comparisons()
    assert [(c.better, c.worse) for c in comparisons] == [(photos["b1"], photos["a1"])]
    _data, species = _best(client)
    assert species["Bald Eagle"]["decision"]["photo_id"] == photos["b1"]
    assert species["Bald Eagle"]["decision"]["kind"] == "chosen"


def test_a_rule_tree_scopes_the_list_to_a_year(trips):
    app, _db, _folders, photos = trips
    client = app.test_client()
    year = json.dumps([
        {"field": "timestamp", "op": "between", "value": ["2025-01-01", "2025-12-31"]},
    ])

    data, species = _best(client, rules=year, per_species=5)

    assert [p["id"] for p in species["Bald Eagle"]["photos"]] == [photos["b2"], photos["b1"]]
    assert data["meta"]["photos_considered"] == 2

    _choose(client, species="Bald Eagle", photo_id=photos["b1"], rules=json.loads(year))
    _data, species = _best(client, rules=year, per_species=5)
    assert [p["id"] for p in species["Bald Eagle"]["photos"]] == [photos["b1"], photos["b2"]]
    assert [p["is_chosen"] for p in species["Bald Eagle"]["photos"]] == [True, False]


def test_a_collection_scopes_the_list(trips):
    app, db, _folders, photos = trips
    collection_id = db.collections.add(
        "Keepers", json.dumps([{"field": "photo_ids", "value": [photos["a1"], photos["b1"]]}]),
    )
    db.commit()

    _data, species = _best(app.test_client(), collection_id=collection_id)

    assert [p["id"] for p in species["Bald Eagle"]["photos"]] == [photos["a1"]]
    assert species["Bald Eagle"]["photo_count"] == 2


def test_one_species_can_be_asked_for_by_name(trips):
    app, db, folders, _photos = trips
    _photo(db, folders["first"], "o.jpg", "2024-05-02T08:00:00", 0.7, species="Osprey")
    db.commit()

    data, species = _best(app.test_client(), species="osprey")

    assert list(species) == ["Osprey"]
    assert data["meta"]["species_count"] == 1


def test_unconfirmed_species_are_left_out_and_counted(trips):
    app, db, folders, _photos = trips
    pid = _photo(db, folders["first"], "m.jpg", "2024-05-03T08:00:00", 0.7, species=None)
    det = db.detections.save(
        pid, [{"box": {"x": 0.1, "y": 0.1, "w": 0.3, "h": 0.3},
               "confidence": 0.9, "category": "animal"}],
        detector_model="MDV6",
    )[0]
    db.predictions.add(det, "Merlin", 0.95, "m")
    db.commit()
    client = app.test_client()

    data, species = _best(client)

    assert "Merlin" not in species
    assert data["meta"]["unconfirmed_species_omitted"] == 1

    data, species = _best(client, include_unconfirmed="1")

    assert species["Merlin"]["is_confirmed"] is False
    assert species["Merlin"]["photos"][0]["is_confirmed"] is False
    assert data["meta"]["unconfirmed_species_omitted"] == 0


def test_a_rejected_chosen_photo_stops_leading(trips):
    app, db, _folders, photos = trips
    client = app.test_client()
    _choose(client, species="Bald Eagle", photo_id=photos["a1"])
    db.conn.execute("UPDATE photos SET flag = 'rejected' WHERE id = ?", (photos["a1"],))
    db.commit()

    _data, species = _best(client)

    assert species["Bald Eagle"]["decision"]["kind"] == "score"
    assert species["Bald Eagle"]["photos"][0]["id"] == photos["a2"]


def test_set_representative_is_a_choice_over_every_other_chosen_photo(trips):
    app, db, folders, photos = trips
    client = app.test_client()
    _choose(client, species="Bald Eagle", photo_id=photos["a1"], folder_id=folders["first"])
    _choose(client, species="Bald Eagle", photo_id=photos["b1"], folder_id=folders["second"])

    response = client.post("/api/photo-preferences", json={
        "purpose": "species_representative", "species": "Bald Eagle",
        "photo_id": photos["a1"],
    })

    assert response.status_code == 200
    _data, species = _best(client)
    assert species["Bald Eagle"]["decision"]["kind"] == "chosen"
    assert species["Bald Eagle"]["decision"]["photo_id"] == photos["a1"]
    assert db.get_species_representatives(eligible_only=True)["Bald Eagle"] == photos["a1"]


def test_every_surface_leads_with_the_same_photo(trips):
    app, db, folders, photos = trips
    client = app.test_client()
    _choose(client, species="Bald Eagle", photo_id=photos["b1"], folder_id=folders["second"])
    _choose(client, species="Bald Eagle", photo_id=photos["a1"], folder_id=folders["first"])
    # a1 was chosen last, so it leads the tie; the comparison overrides that.
    _choose(client, species="Bald Eagle", photo_id=photos["b1"], over=[photos["a1"]],
            source="claude", reason="closer")

    _data, species = _best(client)
    assert species["Bald Eagle"]["decision"]["photo_id"] == photos["b1"]

    highlights = build_highlights_payload(db, HighlightsQuery(scope="workspace"))
    bucket = next(b for b in highlights["buckets"] if b["species"] == "Bald Eagle")
    assert bucket["photos"][0]["id"] == photos["b1"]
    assert bucket["preferred_photo_id"] == photos["b1"]
    assert bucket["best_decision"]["kind"] == "chosen"

    life_list = build_life_list_payload(db)
    entry = next(e for e in life_list["species"] if e["species"] == "Bald Eagle")
    assert entry["best"]["id"] == photos["b1"]
    assert entry["preferred_photo_id"] == photos["b1"]
    assert entry["best_source"] == "representative"
    assert entry["best_decision"]["kind"] == "chosen"
    assert entry["best_decision"]["source"] == "claude"

    assert db.get_species_representatives(eligible_only=True)["Bald Eagle"] == photos["b1"]
    detail = client.get(f"/api/photos/{photos['b1']}").get_json()
    assert detail["species_representatives"][0]["is_species_representative"] is True
    detail = client.get(f"/api/photos/{photos['a1']}").get_json()
    assert detail["species_representatives"][0]["is_species_representative"] is False


def test_life_list_reports_an_uncompared_pair_as_a_tie(trips):
    _app, db, folders, photos = trips
    with db.transaction():
        choose_best(db, "Bald Eagle", photos["a1"], scope=PhotoScope(folder_id=folders["first"]))
        choose_best(db, "Bald Eagle", photos["b1"], scope=PhotoScope(folder_id=folders["second"]))

    entry = next(
        e for e in build_life_list_payload(db)["species"] if e["species"] == "Bald Eagle"
    )

    assert entry["best_decision"]["kind"] == "tie"
    assert entry["best_decision"]["tied_photo_ids"] == [photos["b1"], photos["a1"]]
    assert entry["best"]["id"] == photos["b1"]


def test_rank_traverses_through_representatives_visible_in_other_workspaces(trips):
    # The first workspace sees ``b1`` and ``b2`` only; a second workspace
    # sees ``a2``. Comparisons ``b1 > a2`` and ``a2 > b2`` must still settle
    # ``b1 > b2`` for the first workspace's species-best, since ``a2`` is
    # chosen for Bald Eagle catalog-wide even when this workspace cannot
    # see it. Without the globally-eligible traversal set, the chain's hop
    # through ``a2`` would be inert and the two visible representatives
    # would be reported as tied.
    app, db, folders, photos = trips
    first_ws = db._ws_id()
    other = db.create_workspace("Other")
    db.conn.execute(
        "INSERT INTO workspace_folders (workspace_id, folder_id) VALUES (?, ?)",
        (other, folders["first"]),
    )
    db.conn.execute(
        "DELETE FROM workspace_folders "
        "WHERE workspace_id = ? AND folder_id = ?",
        (first_ws, folders["first"]),
    )
    db.commit()
    db.set_active_workspace(other)
    with db.transaction():
        choose_best(db, "Bald Eagle", photos["a2"], over=[photos["b2"]])
    db.set_active_workspace(first_ws)
    with db.transaction():
        choose_best(db, "Bald Eagle", photos["b2"])
        choose_best(db, "Bald Eagle", photos["b1"], over=[photos["a2"]])

    _data, species = _best(app.test_client())

    decision = species["Bald Eagle"]["decision"]
    assert decision["kind"] == "chosen"
    assert decision["photo_id"] == photos["b1"]


def test_life_list_rank_traverses_through_other_workspace_representatives(trips):
    # Same chain as above, verified through the Life List entry: the first
    # workspace only lists the second trip, but ``b1 > a2 > b2`` still
    # settles ``b1 > b2`` even though ``a2`` is not on this Life List.
    _app, db, folders, photos = trips
    first_ws = db._ws_id()
    other = db.create_workspace("Other")
    db.conn.execute(
        "INSERT INTO workspace_folders (workspace_id, folder_id) VALUES (?, ?)",
        (other, folders["first"]),
    )
    db.conn.execute(
        "DELETE FROM workspace_folders "
        "WHERE workspace_id = ? AND folder_id = ?",
        (first_ws, folders["first"]),
    )
    db.commit()
    db.set_active_workspace(other)
    with db.transaction():
        choose_best(db, "Bald Eagle", photos["a2"], over=[photos["b2"]])
    db.set_active_workspace(first_ws)
    with db.transaction():
        choose_best(db, "Bald Eagle", photos["b2"])
        choose_best(db, "Bald Eagle", photos["b1"], over=[photos["a2"]])

    entry = next(
        e for e in build_life_list_payload(db)["species"] if e["species"] == "Bald Eagle"
    )

    assert entry["best_decision"]["kind"] == "chosen"
    assert entry["best"]["id"] == photos["b1"]


def test_life_list_rank_ignores_representatives_no_longer_tagged(trips):
    # b1 > a2 and a2 > a1, but a2 was relabeled off Bald Eagle. The
    # species_representatives row for a2 is kept for undo, but the Life
    # List ranks only the chosen photos it can show, and a chain hopping
    # through a2 must not survive the relabel. b1 and a1 therefore stay
    # tied until someone compares them directly.
    _app, db, _folders, photos = trips
    eagle_kw = db.keywords.add("Bald Eagle", is_species=True)
    with db.transaction():
        choose_best(db, "Bald Eagle", photos["a1"])
        choose_best(db, "Bald Eagle", photos["a2"], over=[photos["a1"]])
        choose_best(db, "Bald Eagle", photos["b1"], over=[photos["a2"]])
        keyword_tagging.untag_photo(db, photos["a2"], eagle_kw)

    entry = next(
        e for e in build_life_list_payload(db)["species"] if e["species"] == "Bald Eagle"
    )

    assert entry["best_decision"]["kind"] == "tie"
    assert set(entry["best_decision"]["tied_photo_ids"]) == {photos["a1"], photos["b1"]}


def test_clearing_a_species_representatives_forgets_its_choices(trips):
    app, db, _folders, photos = trips
    client = app.test_client()
    _choose(client, species="Bald Eagle", photo_id=photos["a1"], over=[photos["b1"]])

    response = client.delete("/api/photo-preferences", json={
        "purpose": "species_representative", "species": "Bald Eagle",
    })

    assert response.status_code == 200
    assert db.species_choices.comparisons() == []
    assert db.species_choices.notes() == {}


def test_deleting_a_photo_removes_its_comparisons(trips):
    app, db, _folders, photos = trips
    _choose(app.test_client(), species="Bald Eagle", photo_id=photos["a1"], over=[photos["b1"]])

    catalog_deletion.delete_photos(db, [photos["b1"]])

    assert db.species_choices.comparisons() == []
    assert list(db.species_choices.notes()) == [photos["a1"]]


@pytest.mark.parametrize(("body", "status", "message"), [
    ({"photo_id": 1}, 400, "species required"),
    ({"species": "Bald Eagle", "photo_id": "1"}, 400, "photo_id must be an integer"),
    ({"species": "Bald Eagle", "photo_id": True}, 400, "photo_id must be an integer"),
    ({"species": "Bald Eagle", "photo_id": 999999}, 404, "Unknown photo ids"),
    ({"species": "Bald Eagle", "over": "all"}, 400, "over must be a list"),
    ({"species": "Bald Eagle", "over": [999999]}, 404, "Unknown photo ids"),
    ({"species": "Bald Eagle", "source": "x" * 61}, 400, "source must be at most"),
    ({"species": "Bald Eagle", "folder_id": "trip"}, 400, "folder_id must be an integer"),
    ({"species": "Bald Eagle", "rules": {"nope": 1}}, 400, "rules must be"),
    ({"species": "Bald Eagle", "collection_id": 424242}, 400, "collection not found"),
    ({"species": "Bald Eagle", "over": [], "folder_id": 1}, 400, "not both"),
    ({"species": "Osprey"}, 400, "confirm the species"),
])
def test_choose_rejects_what_it_cannot_record(trips, body, status, message):
    app, db, _folders, photos = trips
    body = {"photo_id": photos["a1"], **body}

    response = _choose(app.test_client(), **body)

    assert response.status_code == status
    assert message in response.get_json()["error"]
    assert db.species_choices.notes() == {}
    assert db.species_curation.get_representative_lists() == {}


def test_list_rejects_a_malformed_scope(trips):
    app, _db, _folders, _photos = trips
    client = app.test_client()

    assert client.get("/api/species-best?rules=%7Bnot-json").status_code == 400
    assert client.get("/api/species-best?folder_id=x").status_code == 400
    assert client.get("/api/species-best?collection_id=424242").status_code == 400


def test_the_routes_are_aliased_for_headless_reviewers(trips):
    app, _db, _folders, photos = trips
    client = app.test_client()
    headers = {"X-Vireo-Token": "test-token-123"}

    chosen = client.post(
        "/api/v1/species-best/choose", headers=headers,
        json={"species": "Bald Eagle", "photo_id": photos["a1"], "source": "claude"},
    )
    listed = client.get("/api/v1/species-best", headers=headers)

    assert chosen.status_code == 200
    assert listed.get_json()["species"][0]["decision"]["source"] == "claude"


def test_a_comparison_survives_renaming_the_species(trips):
    app, db, _folders, photos = trips
    client = app.test_client()
    _choose(client, species="Bald Eagle", photo_id=photos["b1"])
    _choose(client, species="Bald Eagle", photo_id=photos["a1"], over=[photos["b1"]],
            source="claude", reason="closer")
    # Choosing b1 again in a scope without a1 makes it the newest choice, so
    # only the comparison keeps a1 ahead.
    _choose(client, species="Bald Eagle", photo_id=photos["b1"], over=[])
    keyword_id = db.keywords.add("Bald Eagle", is_species=True)
    db.commit()

    response = client.put(f"/api/keywords/{keyword_id}", json={"name": "American Bald Eagle"})

    assert response.status_code == 200, response.get_json()
    _data, species = _best(client)
    decision = species["American Bald Eagle"]["decision"]
    assert decision["kind"] == "chosen"
    assert decision["photo_id"] == photos["a1"]
    assert decision["contender_ids"] == [photos["a1"], photos["b1"]]
    assert (decision["source"], decision["reason"]) == ("claude", "closer")


def test_clearing_one_species_keeps_another_species_choices(trips):
    app, db, folders, photos = trips
    osprey = _photo(db, folders["first"], "o.jpg", "2024-05-02T08:00:00", 0.7, species="Osprey")
    db.commit()
    client = app.test_client()
    _choose(client, species="Osprey", photo_id=osprey, reason="only one")
    _choose(client, species="Bald Eagle", photo_id=photos["a1"], over=[photos["b1"]])

    client.delete("/api/photo-preferences", json={
        "purpose": "species_representative", "species": "Bald Eagle",
    })

    assert db.species_choices.comparisons() == []
    assert list(db.species_choices.notes()) == [osprey]
