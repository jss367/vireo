"""``/api/predictions/compare``: one page of rows, and honest counts.

The endpoint derives the whole comparison once, keeps it as a snapshot and
hands the browser a token. These tests cover what the page depends on: that
a page is a page, that the counts describe the collection rather than the
page, and that a decision moves both together.
"""

import json
import os
import sys

import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))


@pytest.fixture
def compare_collection(app_and_db):
    """An app whose collection holds three photos with predictions."""
    app, db = app_and_db
    photo_ids = [
        row["id"] for row in
        db.conn.execute("SELECT id FROM photos ORDER BY id").fetchall()
    ]
    cardinal = db.add_keyword("Cardinal", is_species=True)
    db.tag_photo(photo_ids[0], cardinal)
    for index, photo_id in enumerate(photo_ids):
        det_ids = db.save_detections(photo_id, [{
            "box": {"x": 0.1, "y": 0.1, "w": 0.3, "h": 0.3},
            "confidence": 0.9,
            "category": "animal",
        }], detector_model="MDV6")
        db.add_prediction(det_ids[0], "Cardinal" if index == 0 else "Blue Jay",
                          0.9 - index / 10, "model-a")
        db.add_prediction(det_ids[0], "Cardinal" if index == 0 else "Sparrow",
                          0.8 - index / 10, "model-b")
    cid = db.add_collection(
        "Everything", json.dumps([{"field": "photo_ids", "value": photo_ids}]),
    )
    return app, db, cid, photo_ids


def _get(app, cid, **params):
    query = "&".join(
        f"{key}={value}" for key, value in [("collection_id", cid)] + list(params.items())
    )
    response = app.test_client().get(f"/api/predictions/compare?{query}")
    assert response.status_code == 200, response.get_json()
    return response.get_json()


def test_compare_returns_one_page_and_collection_wide_counts(compare_collection):
    app, _db, cid, photo_ids = compare_collection

    payload = _get(app, cid, filter="all", per_page=2)

    assert len(payload["photos"]) == 2
    assert payload["total"] == len(photo_ids)
    assert payload["summary"]["photos"] == len(photo_ids)
    assert payload["page"] == 1
    assert payload["per_page"] == 2
    assert payload["token"]
    assert set(payload["models"]) == {"model-a", "model-b"}


def test_compare_pages_do_not_repeat_or_drop_rows(compare_collection):
    app, _db, cid, photo_ids = compare_collection

    first = _get(app, cid, filter="all", sort="filename", per_page=2)
    second = _get(
        app, cid, filter="all", sort="filename", per_page=2, page=2,
        token=first["token"],
    )

    seen = [photo["photo_id"] for photo in first["photos"] + second["photos"]]
    assert sorted(seen) == sorted(photo_ids)


def test_compare_reuses_the_snapshot_behind_its_token(compare_collection):
    app, _db, cid, _photo_ids = compare_collection

    first = _get(app, cid, filter="all")
    second = _get(app, cid, filter="conflict", token=first["token"])

    assert second["token"] == first["token"]


def test_changing_the_threshold_rederives_the_comparison(compare_collection):
    """The conflict threshold changes what every row means, so the snapshot
    it was derived under cannot answer for the new one."""
    app, _db, cid, _photo_ids = compare_collection

    first = _get(app, cid, filter="all")
    second = _get(app, cid, filter="all", token=first["token"], min_confidence=0.9)

    assert second["token"] != first["token"]


def test_narrowing_to_one_model_rederives_the_comparison(compare_collection):
    app, _db, cid, _photo_ids = compare_collection

    first = _get(app, cid, filter="all")
    second = _get(app, cid, filter="all", token=first["token"], model="model-a")

    assert second["token"] != first["token"]
    assert second["visible_models"] == ["model-a"]
    assert set(second["models"]) == {"model-a", "model-b"}


def test_filter_counts_cover_every_filter_the_page_offers(compare_collection):
    app, _db, cid, photo_ids = compare_collection

    payload = _get(app, cid, filter="all")

    assert {item["id"] for item in payload["filters"]} == set(payload["filter_counts"])
    assert payload["filter_counts"]["all"] == len(photo_ids)
    assert {item["id"] for item in payload["excludes"]} == set(payload["exclusion_counts"])


def test_rows_carry_the_status_and_signal_the_page_renders(compare_collection):
    app, _db, cid, _photo_ids = compare_collection

    payload = _get(app, cid, filter="all", per_page=200)
    row = next(
        photo for photo in payload["photos"]
        if photo["signal"]["model_disagreement_score"] > 0
    )

    assert row["status"]["category"] == "models_disagree"
    assert len(row["subject_statuses"]) == len(row["subjects"])
    assert row["subjects"][0]["status"]["category"] == "models_disagree"
    assert "vs" in row["signal"]["model_disagreement_label"]


def test_a_decision_updates_the_counts_through_refresh_photo_id(compare_collection):
    """A refreshed photo is rebuilt inside the snapshot, so the chip counts
    follow the work instead of reporting the state before the click."""
    app, db, cid, photo_ids = compare_collection
    client = app.test_client()

    first = _get(app, cid, filter="needs_review")
    assert first["summary"]["needs_review"] == len(photo_ids)

    for row in db.get_predictions(photo_ids=[photo_ids[0]]):
        db.update_prediction_status(row["id"], "reviewed", _commit=False)
    db.conn.commit()

    refreshed = client.get(
        f"/api/predictions/compare?collection_id={cid}&filter=needs_review"
        f"&token={first['token']}&refresh_photo_id={photo_ids[0]}"
    ).get_json()

    assert refreshed["token"] == first["token"]
    assert refreshed["summary"]["needs_review"] == len(photo_ids) - 1
    assert photo_ids[0] not in [
        photo["photo_id"] for photo in refreshed["photos"]
    ]


def test_refresh_photo_id_picks_up_a_sibling_that_just_joined(app_and_db):
    """A grouped decision can hand ``refresh_photo_id`` a sibling that was
    outside the snapshot but now satisfies a keyword-based collection rule.
    The snapshot must rebuild the sibling and append it to the records — it
    cannot silently discard IDs it did not already index."""
    app, db = app_and_db
    photo_ids = [
        row["id"] for row in
        db.conn.execute("SELECT id FROM photos ORDER BY id").fetchall()
    ]
    bird = db.add_keyword("Bird", is_species=True)
    db.tag_photo(photo_ids[0], bird)
    db.tag_photo(photo_ids[1], bird)
    # Only photos with the "Bird" species keyword satisfy the rule, so the
    # third photo starts outside the collection until it is tagged too.
    for photo_id in photo_ids:
        det_ids = db.save_detections(photo_id, [{
            "box": {"x": 0.1, "y": 0.1, "w": 0.3, "h": 0.3},
            "confidence": 0.9,
            "category": "animal",
        }], detector_model="MDV6")
        db.add_prediction(det_ids[0], "Bird", 0.9, "model-a")
    cid = db.add_collection(
        "Birds",
        json.dumps([{"field": "keyword", "op": "equals", "value": "Bird"}]),
    )

    first = _get(app, cid, filter="all")
    assert first["total"] == 2

    db.tag_photo(photo_ids[2], bird)

    refresh_url = (
        f"/api/predictions/compare?collection_id={cid}&filter=all"
        f"&token={first['token']}"
        f"&refresh_photo_id={photo_ids[0]}"
        f"&refresh_photo_id={photo_ids[1]}"
        f"&refresh_photo_id={photo_ids[2]}"
    )
    refreshed = app.test_client().get(refresh_url).get_json()

    assert refreshed["token"] == first["token"]
    assert refreshed["total"] == 3
    assert set(photo["photo_id"] for photo in refreshed["photos"]) == set(photo_ids)


def test_targeted_photo_ids_still_return_full_rows(compare_collection):
    """The decision path asks for named photos and gets the same row shape,
    assessment included."""
    app, _db, cid, photo_ids = compare_collection

    response = app.test_client().get(
        f"/api/predictions/compare?collection_id={cid}&photo_id={photo_ids[1]}"
    )

    payload = response.get_json()
    assert [photo["photo_id"] for photo in payload["photos"]] == [photo_ids[1]]
    assert payload["photos"][0]["status"]["category"]
    assert "token" not in payload


def test_per_page_is_capped(compare_collection):
    app, _db, cid, _photo_ids = compare_collection

    payload = _get(app, cid, filter="all", per_page=100000)

    assert payload["per_page"] == 200


def test_page_past_the_end_is_clamped_to_the_last_page(compare_collection):
    """A decision that shrinks the queue can leave the caller on a page that
    no longer exists. The endpoint clamps the page rather than echoing an
    impossible one like "page 3 of 2" with an empty row list."""
    app, _db, cid, photo_ids = compare_collection

    payload = _get(app, cid, filter="all", per_page=2, page=99)

    expected_last = (len(photo_ids) + 1) // 2
    assert payload["page"] == expected_last
    assert payload["total"] == len(photo_ids)
    assert payload["photos"]


def test_search_narrows_the_listed_rows(compare_collection):
    app, db, cid, photo_ids = compare_collection

    payload = _get(app, cid, filter="all", q="bird2")

    assert [photo["photo_id"] for photo in payload["photos"]] == [photo_ids[1]]
    assert payload["total"] == 1


def test_excludes_are_applied_and_counted(compare_collection):
    app, db, cid, photo_ids = compare_collection
    db.update_photo_flag(photo_ids[0], "rejected")

    payload = _get(app, cid, filter="all", exclude="rejected")

    assert payload["total"] == len(photo_ids) - 1
    assert payload["exclusion_counts"]["rejected"] == 1


def test_unknown_filter_and_sort_fall_back_instead_of_failing(compare_collection):
    app, _db, cid, photo_ids = compare_collection

    payload = _get(app, cid, filter="not-a-filter", sort="not-a-sort")

    assert payload["total"] == len(photo_ids)


def test_a_joining_sibling_that_carries_a_new_model_reassesses(app_and_db):
    """A sibling pulled into the collection by a grouped decision can carry a
    model no photo in the snapshot had. Its column, its agreement and its
    filter counts were all derived without it, so the refresh that pulls the
    sibling in has to derive the comparison again and answer from that —
    advertising the model while serving counts that ignored it would put a
    column on the page whose numbers are simply wrong."""
    app, db = app_and_db
    photo_ids = [
        row["id"] for row in
        db.conn.execute("SELECT id FROM photos ORDER BY id").fetchall()
    ]
    bird = db.add_keyword("Bird", is_species=True)
    db.tag_photo(photo_ids[0], bird)
    db.tag_photo(photo_ids[1], bird)
    for photo_id in photo_ids:
        det_ids = db.save_detections(photo_id, [{
            "box": {"x": 0.1, "y": 0.1, "w": 0.3, "h": 0.3},
            "confidence": 0.9,
            "category": "animal",
        }], detector_model="MDV6")
        db.add_prediction(det_ids[0], "Bird", 0.9, "model-a")
        # Only the photo still outside the collection was run through this
        # model, so nothing in the first snapshot knows it exists.
        if photo_id == photo_ids[2]:
            db.add_prediction(det_ids[0], "Sparrow", 0.7, "model-z")
    cid = db.add_collection(
        "Birds",
        json.dumps([{"field": "keyword", "op": "equals", "value": "Bird"}]),
    )

    first = _get(app, cid, filter="all")
    assert first["models"] == ["model-a"]

    db.tag_photo(photo_ids[2], bird)
    refreshed = app.test_client().get(
        f"/api/predictions/compare?collection_id={cid}&filter=all"
        f"&token={first['token']}"
        f"&refresh_photo_id={photo_ids[2]}"
    ).get_json()
    # The refresh itself carries the new derivation: a fresh token, the wider
    # inventory, and every model in that inventory actually shown.
    assert refreshed["token"] != first["token"]
    assert refreshed["models"] == ["model-a", "model-z"]
    assert refreshed["visible_models"] == ["model-a", "model-z"]

    # And the counts beside the rows were derived under that inventory: the
    # two photos that only ever ran through model-a are now missing one of
    # the shown models, which the pre-widening assessment could not see.
    assert refreshed["filter_counts"]["missing_visible_model"] == 2
    by_id = {photo["photo_id"]: photo for photo in refreshed["photos"]}
    assert by_id[photo_ids[0]]["signal"]["missing_visible_model_count"] == 1
    assert by_id[photo_ids[2]]["signal"]["missing_visible_model_count"] == 0

    # The token the client was handed is the one it can come back with.
    again = _get(app, cid, filter="all", token=refreshed["token"])
    assert again["token"] == refreshed["token"]
    assert again["models"] == ["model-a", "model-z"]


def test_a_widening_refresh_shows_every_model_it_advertises(app_and_db):
    """The invariant behind the rebuild: whatever set of models a response
    names, that is the set its rows and its counts were assessed under. A
    response that lists a model the page will draw a column for, but whose
    assessment never looked at it, is lying about every number on screen."""
    app, db = app_and_db
    photo_ids = [
        row["id"] for row in
        db.conn.execute("SELECT id FROM photos ORDER BY id").fetchall()
    ]
    bird = db.add_keyword("Bird", is_species=True)
    db.tag_photo(photo_ids[0], bird)
    for photo_id in photo_ids:
        det_ids = db.save_detections(photo_id, [{
            "box": {"x": 0.1, "y": 0.1, "w": 0.3, "h": 0.3},
            "confidence": 0.9,
            "category": "animal",
        }], detector_model="MDV6")
        db.add_prediction(det_ids[0], "Bird", 0.9, "model-a")
        if photo_id == photo_ids[1]:
            db.add_prediction(det_ids[0], "Sparrow", 0.7, "model-z")
    cid = db.add_collection(
        "Birds",
        json.dumps([{"field": "keyword", "op": "equals", "value": "Bird"}]),
    )

    first = _get(app, cid, filter="all")
    db.tag_photo(photo_ids[1], bird)
    refreshed = app.test_client().get(
        f"/api/predictions/compare?collection_id={cid}&filter=all"
        f"&token={first['token']}&refresh_photo_id={photo_ids[1]}"
    ).get_json()

    # An all-models page draws a column per entry in ``models``, so that list
    # and the list the rows were assessed under have to be the same list.
    assert refreshed["visible_models"] == refreshed["models"]
    for photo in refreshed["photos"]:
        shown = set(refreshed["models"])
        ran = set(photo.get("predictions") or {})
        assert photo["signal"]["missing_visible_model_count"] == len(shown - ran)


def test_a_pinned_model_page_is_rebuilt_but_stays_pinned(app_and_db):
    """The page pinned to one model was assessed correctly either way, but it
    still gets the fresh derivation: its model selector has to offer the model
    that just appeared, and one code path is easier to trust than two."""
    app, db = app_and_db
    photo_ids = [
        row["id"] for row in
        db.conn.execute("SELECT id FROM photos ORDER BY id").fetchall()
    ]
    bird = db.add_keyword("Bird", is_species=True)
    db.tag_photo(photo_ids[0], bird)
    for photo_id in photo_ids:
        det_ids = db.save_detections(photo_id, [{
            "box": {"x": 0.1, "y": 0.1, "w": 0.3, "h": 0.3},
            "confidence": 0.9,
            "category": "animal",
        }], detector_model="MDV6")
        db.add_prediction(det_ids[0], "Bird", 0.9, "model-a")
        if photo_id == photo_ids[1]:
            db.add_prediction(det_ids[0], "Sparrow", 0.7, "model-z")
    cid = db.add_collection(
        "Birds",
        json.dumps([{"field": "keyword", "op": "equals", "value": "Bird"}]),
    )

    first = _get(app, cid, filter="all", model="model-a")
    assert first["models"] == ["model-a"]

    db.tag_photo(photo_ids[1], bird)
    refreshed = app.test_client().get(
        f"/api/predictions/compare?collection_id={cid}&filter=all"
        f"&model=model-a&token={first['token']}"
        f"&refresh_photo_id={photo_ids[1]}"
    ).get_json()

    # Rebuilt, so the selector can offer model-z — but still pinned, so the
    # rows and counts are the ones the user asked to see.
    assert refreshed["token"] != first["token"]
    assert refreshed["models"] == ["model-a", "model-z"]
    assert refreshed["visible_models"] == ["model-a"]

    still = _get(app, cid, filter="all", model="model-a",
                 token=refreshed["token"])
    assert still["token"] == refreshed["token"]
    assert still["visible_models"] == ["model-a"]


def test_compare_rows_carry_the_render_key_their_thumbnails_need(compare_collection):
    """Rows must ship the edit fingerprint, on both payload paths.

    ID Conflicts builds its thumbnail URLs with the shared
    ``vireoThumbnailUrl``, which appends ``?er=<render_key>``. Thumbnails are
    served ``Cache-Control: public, max-age=86400``, so a row without the key
    falls back to the bare URL — and a browser holding a copy cached before
    the edit keeps showing pre-edit pixels for a day.
    """
    app, db, cid, photo_ids = compare_collection
    edited = photo_ids[0]
    db.set_photo_edit_recipe(edited, {"adjustments": {"exposure": 1.5}})

    # Snapshot path (a normal page) and refresh path (after a decision) are
    # built separately; both feed the same renderer.
    snapshot_rows = _get(app, cid, filter="all", per_page=10)["photos"]
    refresh_rows = _get(app, cid, filter="all", refresh_photo_id=edited)["photos"]

    for label, rows in (("snapshot", snapshot_rows), ("refresh", refresh_rows)):
        by_id = {row["photo_id"]: row for row in rows}
        assert edited in by_id, f"{label} payload dropped the edited photo"
        assert by_id[edited].get("render_key"), (
            f"{label} row for the edited photo has no render_key; its "
            "thumbnail URL cannot bust the browser cache"
        )
        for photo_id in photo_ids[1:]:
            if photo_id in by_id:
                assert by_id[photo_id]["render_key"] is None, (
                    f"{label} row for an unedited photo invented a render "
                    "key, costing a needless refetch"
                )
