"""Tests for the Life List page and /api/life-list."""
import csv
import io
import json
import os

import pytest
from PIL import Image


@pytest.fixture
def life_app(tmp_path, monkeypatch):
    """App seeded with two confirmed species, a taxon link, a location
    keyword, a rejected photo, and an untagged photo."""
    monkeypatch.setenv("HOME", str(tmp_path))
    import config as cfg
    import models
    from app import create_app
    from db import Database

    monkeypatch.setattr(cfg, "CONFIG_PATH", str(tmp_path / "config.json"))
    monkeypatch.setattr(
        models, "DEFAULT_MODELS_DIR", str(tmp_path / "vireo-models"),
    )
    monkeypatch.setattr(models, "CONFIG_PATH", str(tmp_path / "models.json"))

    db_path = str(tmp_path / "test.db")
    thumb_dir = str(tmp_path / "thumbs")
    os.makedirs(thumb_dir)

    db = Database(db_path)
    ws_id = db.ensure_default_workspace()
    db.set_active_workspace(ws_id)
    fid = db.add_folder("/photos/2024", name="2024")

    # Cardinal: two keepers (p2 outscores p1) plus one rejected photo
    # that must not count.
    p1 = db.add_photo(folder_id=fid, filename="card1.jpg", extension=".jpg",
                      file_size=1000, file_mtime=1.0,
                      timestamp="2024-01-15T10:00:00")
    p2 = db.add_photo(folder_id=fid, filename="card2.jpg", extension=".jpg",
                      file_size=1000, file_mtime=2.0,
                      timestamp="2024-03-10T09:00:00")
    p_rej = db.add_photo(folder_id=fid, filename="card3.jpg", extension=".jpg",
                         file_size=1000, file_mtime=3.0,
                         timestamp="2024-04-01T08:00:00")
    db.conn.execute(
        "UPDATE photos SET quality_score = 0.5 WHERE id = ?", (p1,))
    db.conn.execute(
        "UPDATE photos SET quality_score = 0.9 WHERE id = ?", (p2,))
    db.conn.execute(
        "UPDATE photos SET flag = 'rejected' WHERE id = ?", (p_rej,))

    # Sparrow: earlier capture, keyword linked to a taxon. No
    # quality_score — unscored photos must still make the life list.
    p3 = db.add_photo(folder_id=fid, filename="sparrow1.jpg", extension=".jpg",
                      file_size=1000, file_mtime=4.0,
                      timestamp="2023-06-01T07:00:00")

    # Untagged photo: must not appear anywhere.
    db.add_photo(folder_id=fid, filename="empty.jpg", extension=".jpg",
                 file_size=1000, file_mtime=5.0,
                 timestamp="2024-05-01T12:00:00")

    k_card = db.add_keyword("Northern Cardinal", is_species=True)
    cur = db.conn.execute(
        "INSERT INTO taxa (name, common_name, rank) VALUES (?, ?, ?)",
        ("Passer domesticus", "House Sparrow", "species"))
    taxon_id = cur.lastrowid
    k_sparrow = db.add_keyword("House Sparrow", kw_type="taxonomy")
    db.conn.execute(
        "UPDATE keywords SET taxon_id = ? WHERE id = ?", (taxon_id, k_sparrow))
    k_loc = db.add_keyword("Backyard", kw_type="location")
    db.conn.commit()

    db.tag_photo(p1, k_card)
    db.tag_photo(p2, k_card)
    db.tag_photo(p_rej, k_card)
    db.tag_photo(p3, k_sparrow)
    db.tag_photo(p2, k_loc)

    for pid in (p1, p2, p3):
        Image.new("RGB", (100, 100)).save(os.path.join(thumb_dir, f"{pid}.jpg"))

    app = create_app(db_path=db_path, thumb_cache_dir=thumb_dir)
    yield app, db, {"p1": p1, "p2": p2, "p3": p3, "folder": fid}
    db.close()


def _get_life_list(app):
    client = app.test_client()
    resp = client.get("/api/life-list")
    assert resp.status_code == 200
    return resp.get_json()


def _entry(data, species):
    matches = [e for e in data["species"] if e["species"] == species]
    assert len(matches) == 1, f"expected one entry for {species}"
    return matches[0]


def test_page_renders(life_app):
    app, _, _ = life_app
    resp = app.test_client().get("/life-list")
    assert resp.status_code == 200
    assert b"Life List" in resp.data
    assert b"Export Life List" in resp.data


def test_groups_by_species_and_counts(life_app):
    app, _, _ = life_app
    data = _get_life_list(app)
    assert data["meta"]["species_count"] == 2
    # p1, p2, p3 — the rejected and untagged photos don't count.
    assert data["meta"]["photo_count"] == 3
    cardinal = _entry(data, "Northern Cardinal")
    assert cardinal["photo_count"] == 2
    sparrow = _entry(data, "House Sparrow")
    assert sparrow["photo_count"] == 1


def test_best_photo_is_highest_scored(life_app):
    app, _, ids = life_app
    data = _get_life_list(app)
    cardinal = _entry(data, "Northern Cardinal")
    assert cardinal["best"]["id"] == ids["p2"]
    # Photos are returned best-first for the lightbox set.
    assert [p["id"] for p in cardinal["photos"]] == [ids["p2"], ids["p1"]]


def test_life_list_best_ignores_pick_when_no_preference(life_app):
    """Life List ranking must stay score-driven — the Highlights-only
    picked-first ordering must not leak into `_build_life_list_payload`."""
    app, db, ids = life_app
    # p1's quality_score (0.5) is lower than p2's (0.9). Flagging p1
    # must NOT promote it above p2 on the Life List.
    db.update_photo_flag(ids["p1"], "flagged")

    data = _get_life_list(app)
    cardinal = _entry(data, "Northern Cardinal")
    assert cardinal["best"]["id"] == ids["p2"]
    assert [p["id"] for p in cardinal["photos"]] == [ids["p2"], ids["p1"]]


def test_life_list_photo_preference_overrides_best_photo(life_app):
    app, _, ids = life_app
    client = app.test_client()
    resp = client.post("/api/photo-preferences", json={
        "purpose": "life_list",
        "species": "Northern Cardinal",
        "photo_id": ids["p1"],
    })
    assert resp.status_code == 200

    data = _get_life_list(app)
    cardinal = _entry(data, "Northern Cardinal")
    assert cardinal["best"]["id"] == ids["p1"]
    assert cardinal["best"]["is_life_list_photo"] is True
    assert cardinal["has_preferred_photo"] is True
    assert [p["id"] for p in cardinal["photos"][:2]] == [ids["p1"], ids["p2"]]


def test_life_list_photo_preference_must_match_species(life_app):
    app, _, ids = life_app
    resp = app.test_client().post("/api/photo-preferences", json={
        "purpose": "life_list",
        "species": "Northern Cardinal",
        "photo_id": ids["p3"],
    })
    assert resp.status_code == 400


def test_highlights_photo_preference_overrides_best_photo(life_app):
    app, _, ids = life_app
    client = app.test_client()
    resp = client.post("/api/photo-preferences", json={
        "purpose": "highlights",
        "species": "Northern Cardinal",
        "photo_id": ids["p1"],
    })
    assert resp.status_code == 200

    data = client.get("/api/highlights?scope=workspace").get_json()
    cardinal = next(
        b for b in data["buckets"] if b["species"] == "Northern Cardinal"
    )
    assert cardinal["photos"][0]["id"] == ids["p1"]
    assert cardinal["photos"][0]["is_highlights_photo"] is True
    assert cardinal["has_preferred_photo"] is True


def test_highlights_picks_sort_before_higher_scored_unflagged(life_app):
    app, db, ids = life_app
    db.update_photo_flag(ids["p1"], "flagged")

    data = app.test_client().get("/api/highlights?scope=workspace").get_json()
    cardinal = next(
        b for b in data["buckets"] if b["species"] == "Northern Cardinal"
    )
    assert [p["id"] for p in cardinal["photos"][:2]] == [ids["p1"], ids["p2"]]
    assert cardinal["photos"][0]["flag"] == "flagged"
    assert cardinal["photos"][1]["flag"] == "none"


def test_highlights_bucket_best_score_ignores_pick_promotion(life_app):
    """Picking a lower-scored photo must not demote the whole species bucket.

    The Highlights UI ranks buckets by ``best_score`` for the default
    "Best photo first" sort. A pick moves the flagged photo to
    ``photos[0]`` for display, but ``best_score`` must still reflect the
    highest-scored photo in the bucket so the species isn't pushed down
    below buckets whose actual best photo is worse.
    """
    app, db, ids = life_app
    db.update_photo_flag(ids["p1"], "flagged")

    data = app.test_client().get("/api/highlights?scope=workspace").get_json()
    cardinal = next(
        b for b in data["buckets"] if b["species"] == "Northern Cardinal"
    )
    picked = next(p for p in cardinal["photos"] if p["id"] == ids["p1"])
    unpicked = next(p for p in cardinal["photos"] if p["id"] == ids["p2"])
    # Sanity: p1 (picked) really is the lower-scored photo, so photos[0]'s
    # score would demote the bucket if best_score anchored to it.
    assert picked["highlight_score"] < unpicked["highlight_score"]
    assert cardinal["best_score"] == unpicked["highlight_score"]


def test_life_order_numbering_and_dates(life_app):
    app, _, _ = life_app
    data = _get_life_list(app)
    sparrow = _entry(data, "House Sparrow")
    cardinal = _entry(data, "Northern Cardinal")
    # Sparrow was photographed first (2023) so it's lifer #1.
    assert sparrow["number"] == 1
    assert cardinal["number"] == 2
    assert cardinal["first_seen"] == "2024-01-15T10:00:00"
    assert cardinal["last_seen"] == "2024-03-10T09:00:00"
    # Entries arrive in life order.
    assert [e["number"] for e in data["species"]] == [1, 2]


def test_taxon_names_attached(life_app):
    app, _, _ = life_app
    data = _get_life_list(app)
    sparrow = _entry(data, "House Sparrow")
    assert sparrow["scientific_name"] == "Passer domesticus"
    assert sparrow["common_name"] == "House Sparrow"
    cardinal = _entry(data, "Northern Cardinal")
    assert cardinal["scientific_name"] is None


def test_locations_from_location_keywords(life_app):
    app, _, _ = life_app
    data = _get_life_list(app)
    cardinal = _entry(data, "Northern Cardinal")
    assert cardinal["locations"] == ["Backyard"]
    sparrow = _entry(data, "House Sparrow")
    assert sparrow["locations"] == []


def test_unscored_photo_still_listed(life_app):
    app, _, ids = life_app
    data = _get_life_list(app)
    sparrow = _entry(data, "House Sparrow")
    assert sparrow["best"]["id"] == ids["p3"]
    assert sparrow["best"]["quality_score"] is None


def test_null_flag_photo_included(life_app):
    """Older or manually edited databases can have NULL flags; those photos
    are kept (treated like 'none'), not filtered out as rejected."""
    app, db, ids = life_app
    db.conn.execute(
        "UPDATE photos SET flag = NULL WHERE id IN (?, ?)",
        (ids["p2"], ids["p3"]))
    db.conn.commit()
    data = _get_life_list(app)
    assert data["meta"]["species_count"] == 2
    assert data["meta"]["photo_count"] == 3
    cardinal = _entry(data, "Northern Cardinal")
    assert cardinal["photo_count"] == 2
    # p2 (now NULL-flagged) carries the location tag — the locations
    # query must include it too.
    assert cardinal["locations"] == ["Backyard"]


def test_workspace_scoping(life_app):
    app, db, ids = life_app
    # Hide the folder from the active workspace: the life list empties.
    db.conn.execute(
        "DELETE FROM workspace_folders WHERE folder_id = ?", (ids["folder"],))
    db.conn.commit()
    data = _get_life_list(app)
    assert data["species"] == []
    assert data["meta"]["species_count"] == 0


def test_same_named_species_keywords_dedupe_photos(life_app):
    """A photo tagged with two species keywords sharing a display name
    (different parents — allowed by UNIQUE(name, parent_id)) must count
    once: photo_count and the lightbox set both dedupe by photo id."""
    app, db, ids = life_app
    # Two parent rows so the children can share a name.
    parent_a = db.add_keyword("Cardinalidae", kw_type="taxonomy")
    parent_b = db.add_keyword("Cardinalis", kw_type="taxonomy")
    twin_a = db.add_keyword(
        "Northern Cardinal", parent_id=parent_a, is_species=True)
    twin_b = db.add_keyword(
        "Northern Cardinal", parent_id=parent_b, is_species=True)
    assert twin_a != twin_b
    db.tag_photo(ids["p2"], twin_a)
    db.tag_photo(ids["p2"], twin_b)
    db.conn.commit()

    data = _get_life_list(app)
    cardinal = _entry(data, "Northern Cardinal")
    # Still two distinct photos (p1, p2) — not four.
    assert cardinal["photo_count"] == 2
    photo_ids = [p["id"] for p in cardinal["photos"]]
    assert sorted(photo_ids) == sorted([ids["p1"], ids["p2"]])
    assert len(photo_ids) == len(set(photo_ids))
    # Workspace-wide distinct-photo count is unaffected.
    assert data["meta"]["photo_count"] == 3


def test_life_list_export_json_attachment(life_app):
    app, _, _ = life_app
    resp = app.test_client().get("/api/life-list/export?format=json")
    assert resp.status_code == 200
    assert resp.headers["Content-Type"] == "application/json"
    assert "attachment" in resp.headers.get("Content-Disposition", "")
    assert "vireo-life-list-" in resp.headers.get("Content-Disposition", "")

    data = json.loads(resp.get_data(as_text=True))
    assert data["meta"]["species_count"] == 2
    cardinal = _entry(data, "Northern Cardinal")
    assert cardinal["locations"] == []


def test_life_list_export_species_csv_with_locations(life_app):
    app, _, _ = life_app
    resp = app.test_client().get(
        "/api/life-list/export?format=csv&include_locations=1"
    )
    assert resp.status_code == 200
    assert resp.headers["Content-Type"].startswith("text/csv")

    rows = list(csv.DictReader(io.StringIO(resp.get_data(as_text=True))))
    assert [r["species"] for r in rows] == ["House Sparrow", "Northern Cardinal"]
    cardinal = next(r for r in rows if r["species"] == "Northern Cardinal")
    assert cardinal["locations"] == "Backyard"
    assert cardinal["best_filename"] == "card2.jpg"


def test_life_list_export_csv_escapes_formula_leading_cells(life_app):
    app, db, ids = life_app
    p_formula = db.add_photo(
        folder_id=ids["folder"],
        filename="@formula.jpg",
        extension=".jpg",
        file_size=1000,
        file_mtime=10.0,
        timestamp="2024-06-01T08:00:00",
    )
    k_species = db.add_keyword("=2+2", is_species=True)
    k_location = db.add_keyword("+Backyard", kw_type="location")
    db.tag_photo(p_formula, k_species)
    db.tag_photo(p_formula, k_location)

    resp = app.test_client().get(
        "/api/life-list/export?format=csv&include_locations=1"
    )
    rows = list(csv.DictReader(io.StringIO(resp.get_data(as_text=True))))
    formula_row = next(r for r in rows if r["species"] == "'=2+2")
    assert formula_row["locations"] == "'+Backyard"
    assert formula_row["best_filename"] == "'@formula.jpg"

    photo_resp = app.test_client().get(
        "/api/life-list/export?format=csv&detail=photos&photos=all"
        "&include_locations=1"
    )
    photo_rows = list(csv.DictReader(io.StringIO(photo_resp.get_data(as_text=True))))
    photo_row = next(r for r in photo_rows if r["species"] == "'=2+2")
    assert photo_row["filename"] == "'@formula.jpg"
    assert photo_row["locations"] == "'+Backyard"


def test_life_list_export_photo_csv_all_photos(life_app):
    app, _, ids = life_app
    resp = app.test_client().get(
        "/api/life-list/export?format=csv&detail=photos&photos=all"
    )
    assert resp.status_code == 200

    rows = list(csv.DictReader(io.StringIO(resp.get_data(as_text=True))))
    assert [int(r["photo_id"]) for r in rows] == [ids["p3"], ids["p2"], ids["p1"]]
    assert all(r["locations"] == "" for r in rows)


def test_life_list_export_all_photos_is_not_limited_to_page_count(life_app):
    app, db, ids = life_app
    k_card = db.add_keyword("Northern Cardinal", is_species=True)
    extra_ids = []
    for i in range(13):
        pid = db.add_photo(
            folder_id=ids["folder"],
            filename=f"card-extra-{i}.jpg",
            extension=".jpg",
            file_size=1000,
            file_mtime=20.0 + i,
            timestamp=f"2024-05-{i + 1:02d}T08:00:00",
        )
        db.tag_photo(pid, k_card)
        extra_ids.append(pid)

    page_payload = _get_life_list(app)
    cardinal = _entry(page_payload, "Northern Cardinal")
    assert cardinal["photo_count"] == 15
    assert len(cardinal["photos"]) == 12

    resp = app.test_client().get(
        "/api/life-list/export?format=csv&detail=photos&photos=all"
    )
    rows = list(csv.DictReader(io.StringIO(resp.get_data(as_text=True))))
    cardinal_rows = [r for r in rows if r["species"] == "Northern Cardinal"]
    assert len(cardinal_rows) == 15
    exported_ids = {int(r["photo_id"]) for r in cardinal_rows}
    assert set(extra_ids).issubset(exported_ids)


def test_life_list_summary_exports_ignore_hidden_all_photo_scope(life_app):
    app, db, ids = life_app
    k_card = db.add_keyword("Northern Cardinal", is_species=True)
    for i in range(13):
        pid = db.add_photo(
            folder_id=ids["folder"],
            filename=f"card-hidden-scope-{i}.jpg",
            extension=".jpg",
            file_size=1000,
            file_mtime=40.0 + i,
            timestamp=f"2024-07-{i + 1:02d}T08:00:00",
        )
        db.tag_photo(pid, k_card)

    resp = app.test_client().get("/api/life-list/export?format=json&photos=all")
    assert resp.status_code == 200
    data = json.loads(resp.get_data(as_text=True))
    cardinal = _entry(data, "Northern Cardinal")
    assert cardinal["photo_count"] == 15
    assert len(cardinal["photos"]) == 12
    assert data["meta"]["photos_per_species"] == 12

    csv_resp = app.test_client().get(
        "/api/life-list/export?format=csv&detail=species&photos=all"
    )
    rows = list(csv.DictReader(io.StringIO(csv_resp.get_data(as_text=True))))
    cardinal_row = next(r for r in rows if r["species"] == "Northern Cardinal")
    assert cardinal_row["photo_count"] == "15"


def test_life_list_file_export_keeps_duplicate_filenames_by_photo_id(life_app):
    app, db, ids = life_app
    k_card = db.add_keyword("Northern Cardinal", is_species=True)
    other_folder = db.add_folder("/photos/2025", name="2025")
    for folder_id in (ids["folder"], other_folder):
        pid = db.add_photo(
            folder_id=folder_id,
            filename="DSC_0001.JPG",
            extension=".JPG",
            file_size=1000,
            file_mtime=30.0 + folder_id,
            timestamp=f"2024-06-{folder_id:02d}T08:00:00",
        )
        db.tag_photo(pid, k_card)

    resp = app.test_client().get("/api/life-list/export?format=file&photos=all")
    assert resp.status_code == 200
    assert resp.get_data(as_text=True).splitlines().count("DSC_0001.JPG") == 2


def test_life_list_export_text_and_file_lists(life_app):
    app, _, _ = life_app
    client = app.test_client()

    text_resp = client.get("/api/life-list/export?format=txt&include_locations=1")
    assert text_resp.status_code == 200
    text = text_resp.get_data(as_text=True)
    assert "#1 House Sparrow (Passer domesticus)" in text
    assert "locations: Backyard" in text

    files_resp = client.get("/api/life-list/export?format=file&photos=all")
    assert files_resp.status_code == 200
    assert files_resp.get_data(as_text=True).splitlines() == [
        "sparrow1.jpg",
        "card2.jpg",
        "card1.jpg",
    ]


def test_life_list_export_rejects_unknown_format(life_app):
    app, _, _ = life_app
    resp = app.test_client().get("/api/life-list/export?format=pdf")
    assert resp.status_code == 400
