"""Reviewed GPS participates in map presence without changing raw GPS rules."""

import pytest
from services import workspace_folders
from test_db_locations import _kw, _link, _photo


@pytest.fixture
def locations(app_and_db, tmp_path):
    app, db = app_and_db
    baseline_missing = db.locations.count_photos_without_coordinates()
    workspace = db.require_workspace_id()
    root = workspace_folders.add_folder(db, str(tmp_path / "visible"))
    child = workspace_folders.add_folder(db, str(tmp_path / "visible" / "child"), parent_id=root)
    offline = workspace_folders.add_folder(db, str(tmp_path / "offline"))
    hidden = workspace_folders.add_folder(db, str(tmp_path / "hidden"), link_to_workspace=False)
    elsewhere = db.create_workspace("Synthetic other workspace")
    db.set_active_workspace(workspace)
    ids = {name: _photo(db, root, name + ".jpg") for name in (
        "reviewed", "raw", "assigned", "none", "partial_raw", "text_only", "reviewed_raw", "reviewed_assigned",
    )}
    ids["reviewed_child"] = _photo(db, child, "reviewed-child.jpg")
    ids["offline"] = _photo(db, offline, "offline.jpg")
    ids["hidden"] = _photo(db, hidden, "hidden.jpg")
    db.photo_visibility.grant(elsewhere, [ids["hidden"]])
    db.conn.execute("UPDATE photos SET latitude = 0, longitude = 0 WHERE id = ?", (ids["raw"],))
    db.conn.execute("UPDATE photos SET latitude = 3, longitude = 4 WHERE id = ?", (ids["reviewed_raw"],))
    db.conn.execute("UPDATE photos SET latitude = 1 WHERE id IN (?, ?)", (ids["partial_raw"], ids["reviewed_child"]))
    placed = _kw(db, "Synthetic park", lat=5, lng=6)
    _link(db, ids["assigned"], placed)
    _link(db, ids["reviewed_assigned"], placed)
    _link(db, ids["text_only"], _kw(db, "Name without coordinates"))
    for key in ("reviewed", "reviewed_raw", "reviewed_assigned", "reviewed_child", "offline", "hidden"):
        db.audit.apply_gps_review([ids[key]], "Synthetic correction", .1, .2)
    db.conn.execute("UPDATE folders SET status = 'missing' WHERE id = ?", (offline,))
    db.commit()
    return app, db, ids, root, child, elsewhere, baseline_missing


def test_marker_status_count_and_plottable_ids_agree_for_reviewed_gps(locations):
    _app, db, ids, root, child, _elsewhere, baseline_missing = locations
    expected = {ids[name] for name in ("reviewed", "raw", "assigned", "reviewed_raw", "reviewed_assigned", "reviewed_child")}
    markers = db.locations.get_geolocated_photos(folder_id=root)
    assert {row["id"] for row in markers} == expected
    assert set(db.locations.get_plottable_photo_ids(folder_id=root)) == expected
    assert db.locations.get_plottable_photo_ids(folder_id=child) == [ids["reviewed_child"]]
    assert db.locations.count_photos_without_coordinates() == baseline_missing + 3
    statuses = db.locations.get_photo_statuses(list(ids.values()))
    for name in ("reviewed", "reviewed_raw", "reviewed_assigned", "reviewed_child", "offline", "hidden"):
        assert statuses[ids[name]] == "reviewed_gps"
    assert statuses[ids["raw"]] == "exif"
    assert statuses[ids["assigned"]] == "assigned"
    assert all(statuses[ids[name]] == "none" for name in ("none", "partial_raw", "text_only"))
    for row in markers:
        if statuses[row["id"]] == "reviewed_gps":
            assert (row["latitude"], row["longitude"], row["coord_source"]) == (.1, .2, "reviewed_gps")
    assert not set(db.locations.get_plottable_photo_ids()) & {ids["offline"], ids["hidden"]}


def test_reviewed_locations_stay_scoped_to_the_active_workspace(locations):
    _app, db, ids, _root, _child, elsewhere, _baseline = locations
    db.set_active_workspace(elsewhere)
    assert db.locations.get_plottable_photo_ids() == [ids["hidden"]]
    assert [row["id"] for row in db.locations.get_geolocated_photos()] == [ids["hidden"]]
    assert db.locations.count_photos_without_coordinates() == 0


@pytest.mark.parametrize("name,expected", [("reviewed", "none"), ("reviewed_raw", "exif"),
                                          ("reviewed_assigned", "assigned"), ("reviewed_child", "none")])
def test_removing_a_review_restores_the_existing_coordinate_source(locations, name, expected):
    _app, db, ids, root, _child, _elsewhere, baseline = locations
    db.audit.remove_gps_review([ids[name]], "Synthetic correction")
    db.commit()
    assert db.locations.get_photo_statuses([ids[name]]) == {ids[name]: expected}
    assert (ids[name] in db.locations.get_plottable_photo_ids(folder_id=root)) == (expected != "none")
    assert db.locations.count_photos_without_coordinates() == baseline + 3 + (expected == "none")


def test_map_selection_summary_partitions_reviewed_only_photos(locations):
    app, _db, ids, _root, _child, _elsewhere, _baseline = locations
    response = app.test_client().post("/api/photos/geo", json={
        "photo_ids": [ids[name] for name in ("reviewed", "none", "offline", "hidden")],
    })
    assert response.status_code == 200
    payload = response.get_json()
    assert [row["id"] for row in payload["photos"]] == [ids["reviewed"]]
    assert payload["selection"] == {"selected": 4, "shown": 1, "hidden_by_filters": 0,
                                     "without_location": 1, "folder_unavailable": 1, "not_in_workspace": 1}


@pytest.mark.parametrize("status,names", [
    ("reviewed_gps", ["reviewed", "reviewed_raw", "reviewed_assigned", "reviewed_child"]),
    ("exif", ["raw"]), ("assigned", ["assigned"]), ("none", ["none", "partial_raw", "text_only"]),
])
def test_browse_status_and_id_filters_use_the_reported_coordinate_source(locations, status, names):
    app, _db, ids, root, _child, _elsewhere, _baseline = locations
    client = app.test_client()
    response = client.get(f"/api/photos?folder_id={root}&location_status={status}")
    assert response.status_code == 200
    assert {row["id"] for row in response.get_json()["photos"]} == {ids[name] for name in names}
    assert {row["location_status"] for row in response.get_json()["photos"]} == {status}
    response = client.get(f"/api/photos/ids?folder_id={root}&location_status={status}")
    assert response.status_code == 200
    assert set(response.get_json()["photo_ids"]) == {ids[name] for name in names}


def test_numeric_gps_rules_still_read_raw_coordinates(locations):
    app, _db, ids, root, _child, _elsewhere, _baseline = locations
    response = app.test_client().post(f"/api/photos/geo?folder_id={root}", json={
        "photo_ids": [ids["reviewed"], ids["reviewed_raw"]],
        "rules": [{"field": "gps_lat", "op": ">", "value": 2}],
    })
    assert response.status_code == 200
    [marker] = response.get_json()["photos"]
    assert marker["id"] == ids["reviewed_raw"]
    assert marker["latitude"] == .1


def test_visual_map_candidates_include_reviewed_only_locations(locations, monkeypatch):
    import json

    from services.visual_scope import VisualScope

    app, _db, ids, root, _child, _elsewhere, _baseline = locations
    seen = []

    def visual(_self, _db, rules, _visual, **kwargs):
        seen.extend(kwargs["candidate_photo_ids"])
        return rules, {"status": "ok", "matched": len(seen)}

    monkeypatch.setattr(VisualScope, "apply_to_rules", visual)
    response = app.test_client().get("/api/photos/geo", query_string={
        "folder_id": root, "visual": json.dumps({"prompt": "synthetic", "strength": "balanced"}),
    })
    assert response.status_code == 200
    assert ids["reviewed"] in seen
    assert not set(seen) & {ids[name] for name in ("none", "partial_raw", "text_only", "offline", "hidden")}
    assert ids["reviewed"] in {row["id"] for row in response.get_json()["photos"]}


def test_merged_loser_only_review_is_located_on_raw_empty_survivor(locations):
    from repositories.photo_row_deletion import photo_row_deletion

    _app, db, ids, root, _child, _elsewhere, baseline = locations
    losing = _photo(db, root, "loser-with-correction.jpg")
    db.audit.apply_gps_review([losing], "Synthetic losing correction", .3, .4)
    db.commit()
    with db.transaction(), photo_row_deletion(db.conn) as deletion:
        deletion.delete({losing: ids["none"]})
    assert db.locations.get_photo_statuses([ids["none"]]) == {ids["none"]: "reviewed_gps"}
    assert ids["none"] in db.locations.get_plottable_photo_ids(folder_id=root)
    marker = next(row for row in db.locations.get_geolocated_photos(folder_id=root) if row["id"] == ids["none"])
    assert (marker["latitude"], marker["longitude"]) == (.3, .4)
    assert db.locations.count_photos_without_coordinates() == baseline + 2
