"""Batch edits preserve per-photo choices and restore them through history."""

import pytest
from edit_batch import compose_recipe, decode_preset, encode_preset
from image_edits import RecipeError


def test_selective_white_balance_preserves_exposure_crop_and_other_white_balance():
    original = {"crop": {"x": 0.1, "y": 0.1, "w": 0.8, "h": 0.7},
                "adjustments": {"exposure": 1.2, "white_balance": {"temperature": 20, "tint": -7}}}
    result = compose_recipe(original, {"adjustments": {"white_balance": {"temperature": 0}}},
                            ["adjustments.white_balance.temperature"], "merge")
    assert result["crop"] == original["crop"]
    assert result["adjustments"] == {"exposure": 1.2, "white_balance": {"tint": -7}}
    assert original["adjustments"]["white_balance"]["temperature"] == 20


def test_relative_offsets_clamp_and_support_negative_detail():
    result = compose_recipe({"adjustments": {"exposure": 4.9, "sharpen": 20}},
                            {"adjustments": {"exposure": 0.3, "sharpen": -30}},
                            ["adjustments.exposure", "adjustments.sharpen"], "relative")
    assert result["adjustments"] == {"exposure": 5}


def test_radius_patch_uses_existing_sharpening():
    result = compose_recipe({"adjustments": {"sharpen": 20}}, {"adjustments": {"sharpen_radius": 2}},
                            ["adjustments.sharpen_radius"], "merge")
    assert result["adjustments"] == {"sharpen": 20, "sharpen_radius": 2}


@pytest.mark.parametrize("fields, mode, recipe", [
    ([], "merge", {}), (["nope"], "merge", {}), ([{}], "merge", {}),
    (None, "relative", {}), (["crop"], "relative", {}),
    (["adjustments.exposure"], "relative", {"adjustments": {"exposure": True}}),
    (["adjustments.exposure"], "relative", {"adjustments": {"exposure": float("nan")}}),
    (["adjustments.sharpen_radius"], "merge", {"adjustments": {"sharpen_radius": 99}}),
])
def test_bad_operations_rejected(fields, mode, recipe):
    with pytest.raises(RecipeError):
        compose_recipe({}, recipe, fields, mode)


def test_partial_preset_keeps_explicit_neutral_settings():
    raw = encode_preset({"rotation": 90, "adjustments": {"exposure": 2}},
                        ["rotation", "adjustments.white_balance.temperature"])
    recipe, fields = decode_preset(raw)
    assert recipe == {"version": 1, "rotation": 90}
    target = compose_recipe({"adjustments": {"exposure": -1, "white_balance": {"temperature": 35}}},
                            recipe, fields, "merge")
    assert target == {"version": 1, "rotation": 90, "adjustments": {"exposure": -1}}


def test_relative_api_preserves_individual_edits_and_undo_redo(app_and_db):
    app, db = app_and_db
    client = app.test_client()
    ids = [photo["id"] for photo in db.get_photos()]
    originals = {}
    for index, pid in enumerate(ids):
        originals[pid] = db.set_photo_edit_recipe(pid, {
            "rotation": 90 * index, "adjustments": {"exposure": index - 1, "contrast": 10 * index},
        })
    response = client.post('/api/photos/edit-recipe/apply', json={
        "photo_ids": ids + [ids[0], 999999], "recipe": {"adjustments": {"exposure": .3}},
        "fields": ["adjustments.exposure"], "mode": "relative",
    })
    assert response.status_code == 200
    assert response.json["skipped"] == [999999]
    assert response.json["count"] == len(ids)
    for index, pid in enumerate(ids):
        after = db.get_photo_edit_recipe(pid)
        assert after["adjustments"]["exposure"] == pytest.approx(index - .7)
        assert after.get("rotation") == (originals[pid] or {}).get("rotation")
        assert after["adjustments"].get("contrast", 0) == 10 * index
    applied = {pid: db.get_photo_edit_recipe(pid) for pid in ids}
    assert client.post('/api/undo').status_code == 200
    assert {pid: db.get_photo_edit_recipe(pid) for pid in ids} == originals
    assert client.post('/api/redo').status_code == 200
    assert {pid: db.get_photo_edit_recipe(pid) for pid in ids} == applied
    assert len([h for h in db.get_edit_history() if h["action_type"] == "edit_recipe"]) == 1


def test_partial_paste_can_reset_without_overwriting_other_settings(app_and_db):
    app, db = app_and_db
    client = app.test_client()
    ids = [photo["id"] for photo in db.get_photos()]
    for pid in ids:
        db.set_photo_edit_recipe(pid, {"rotation": 90, "adjustments": {"exposure": 1, "saturation": 10}})
    response = client.post('/api/photos/edit-recipe/apply', json={
        "photo_ids": ids, "recipe": {}, "fields": ["adjustments.exposure"],
    })
    assert response.status_code == 200
    for pid in ids:
        assert db.get_photo_edit_recipe(pid) == {"version": 1, "rotation": 90, "adjustments": {"saturation": 10}}


def test_invalid_batch_leaves_every_photo_unchanged(app_and_db):
    app, db = app_and_db
    client = app.test_client()
    ids = [photo["id"] for photo in db.get_photos()]
    response = client.post('/api/photos/edit-recipe/apply', json={
        "photo_ids": ids, "recipe": {"adjustments": {"sharpen_radius": 99}},
        "fields": ["adjustments.sharpen_radius"], "mode": "merge",
    })
    assert response.status_code == 400
    assert all(db.get_photo_edit_recipe(pid) is None for pid in ids)


def test_summary_reports_mixed_neutral_and_common_values(app_and_db):
    app, db = app_and_db
    ids = [photo["id"] for photo in db.get_photos()]
    db.set_photo_edit_recipe(ids[0], {"adjustments": {"exposure": 1}})
    response = app.test_client().post('/api/photos/edit-recipe/summary', json={"photo_ids": ids})
    assert response.status_code == 200
    assert response.json["values"]["adjustments.exposure"] is None
    assert response.json["values"]["adjustments.sharpen_radius"] == 1
    assert response.json["values"]["adjustments.white_balance.temperature"] == 0


def test_rich_presets_roundtrip_and_compose_without_saving(app_and_db):
    app, db = app_and_db
    client = app.test_client()
    pid = db.get_photos()[0]["id"]
    fields = ["rotation", "adjustments.white_balance.temperature"]
    response = client.post('/api/edit-presets', json={
        "name": "Portrait orientation", "recipe": {"rotation": 90}, "fields": fields,
    })
    assert response.status_code == 200
    assert client.get('/api/edit-presets').json["presets"] == [response.json["preset"]]
    response = client.post(f'/api/photos/{pid}/edit-recipe/compose', json={
        "current": {"adjustments": {"exposure": 1, "white_balance": {"temperature": 30, "tint": 7}}},
        "recipe": response.json["preset"]["recipe"], "fields": fields,
    })
    assert response.status_code == 200
    assert response.json["recipe"] == {
        "version": 1, "rotation": 90, "adjustments": {"exposure": 1, "white_balance": {"tint": 7}},
    }
    assert db.get_photo_edit_recipe(pid) is None


def test_radius_only_preset_preserves_destination_sharpen_strength():
    raw = encode_preset({"adjustments": {"sharpen": 30, "sharpen_radius": 2}},
                        ["adjustments.sharpen_radius"])
    recipe, fields = decode_preset(raw)
    result = compose_recipe({"adjustments": {"sharpen": 60}}, recipe, fields, "merge")
    assert result["adjustments"] == {"sharpen": 60, "sharpen_radius": 2}


def test_denoise_method_survives_selective_presets_and_can_reset():
    source = {"adjustments": {"denoise_mode": "camera", "noise_reduction": 55}}
    recipe, fields = decode_preset(encode_preset(source, ["adjustments.denoise_mode"]))
    result = compose_recipe({"adjustments": {"noise_reduction": 70}}, recipe, fields, "merge")
    assert result["adjustments"] == {"denoise_mode": "camera", "noise_reduction": 70}
    reset = compose_recipe(result, {}, fields, "merge")
    assert reset["adjustments"] == {"noise_reduction": 70}
