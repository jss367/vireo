"""Exact-value picker predicates and model choices use library semantics."""
import config as cfg
import pytest


@pytest.fixture(autouse=True)
def isolated_config(tmp_path, monkeypatch):
    monkeypatch.setattr(cfg, "CONFIG_PATH", str(tmp_path / "config.json"))


def _photo(db, folder, name, **cols):
    pid = db.add_photo(folder_id=folder, filename=name, extension=".jpg", file_size=1, file_mtime=1)
    if cols:
        db.conn.execute("UPDATE photos SET " + ", ".join(f"{key}=?" for key in cols) + " WHERE id=?",
                        [*cols.values(), pid])
        db.conn.commit()
    return pid


@pytest.mark.parametrize("field", ["camera_make", "camera_model", "lens"])
def test_equipment_choices_include_any_and_exclude_all(db, tmp_path, field):
    folder = db.add_folder(str(tmp_path / "photos"))
    a = _photo(db, folder, "a.jpg", **{field: "Alpha"})
    b = _photo(db, folder, "b.jpg", **{field: "Beta"})
    c = _photo(db, folder, "c.jpg")
    def ids(op, value):
        return set(db.query_photo_ids([{"field": field, "op": op, "value": value}]))
    assert ids("in", ["ALPHA", "beta"]) == {a, b}
    assert ids("not_in", ["ALPHA", "beta"]) == {c}
    assert ids("in", []) == set()
    assert ids("not_in", []) == {a, b, c}
    assert ids("contains", "pha") == {a}


@pytest.mark.parametrize("field", ["keyword", "species"])
def test_keyword_choices_match_any_and_exclude_all(db, tmp_path, field):
    folder = db.add_folder(str(tmp_path / "photos"))
    a = _photo(db, folder, "a.jpg")
    b = _photo(db, folder, "b.jpg")
    c = _photo(db, folder, "c.jpg")
    hawk = db.add_keyword("Hawk", is_species=True)
    robin = db.add_keyword("Robin", is_species=True)
    db.tag_photo(a, hawk)
    db.tag_photo(a, robin)
    db.tag_photo(b, robin)
    assert set(db.query_photo_ids([{"field": field, "op": "in", "value": ["Hawk", "Robin"]}])) == {a, b}
    assert set(db.query_photo_ids([{"field": field, "op": "not_in", "value": ["Hawk", "Robin"]}])) == {c}
    nested = {"mode": "any", "rules": [
        {"field": field, "op": "in", "value": ["Hawk"]},
        {"field": "filename", "op": "is", "value": "c.jpg"},
    ]}
    assert set(db.query_photo_ids(nested)) == {a, c}


def test_model_choices_respect_workspace_and_visible_predictions(db, tmp_path):
    folder = db.add_folder(str(tmp_path / "photos"))
    a = _photo(db, folder, "a.jpg")
    def predict(pid, model, confidence=0.95, status="pending"):
        det = db.save_detections(pid, [{"box": {"x": 0, "y": 0, "w": 1, "h": 1}, "confidence": confidence}], detector_model=model)[0]
        db.add_prediction(det, species="Hawk", confidence=0.9, model=model, status=status)
    predict(a, "Visible model")
    predict(a, "Hidden detector", confidence=0.01)
    predict(a, "Alternative only", status="alternative")
    ws = db._active_workspace_id
    other = db.create_workspace("Other")
    db.set_active_workspace(other)
    folder2 = db.add_folder(str(tmp_path / "elsewhere"))
    predict(_photo(db, folder2, "elsewhere.jpg"), "Other workspace model")
    db.set_active_workspace(ws)
    assert db.get_filter_field_values("classifier_model") == [{"value": "Visible model", "count": 1}]
    assert db.get_filter_field_values("classifier_model", q="Visible") == [{"value": "Visible model", "count": 1}]
    assert db.get_filter_field_values("classifier_model", q="%") == []
    assert db.get_filter_field_values("classifier_model", rules=[{"field": "filename", "op": "is", "value": "missing"}]) == []
