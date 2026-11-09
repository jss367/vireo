"""Fetched hybrid crosses survive storage for manual tagging, not classification."""

def test_hybrid_crosses_are_counted_and_still_offered_for_hand_tagging(
    app_and_db, tmp_path, monkeypatch,
):
    """The classifier is never asked for a cross, so Settings says how many
    a list holds and autocomplete still offers them: tagging by hand is the
    only way to record one."""
    import labels as labels_mod

    labels_dir = tmp_path / "labels"
    labels_dir.mkdir(exist_ok=True)
    monkeypatch.setattr(labels_mod, "LABELS_DIR", str(labels_dir))
    fetched = labels_mod.SpeciesLabels(
        ["Mallard", "Mexican Duck"], hybrid_crosses=["Mallard × Mexican Duck"],
    )
    label_path = labels_mod.save_labels("Ducks", 14, "California", ["birds"], fetched)
    meta = labels_mod.get_saved_labels()[0]
    monkeypatch.setattr("labels.get_active_labels", lambda: [meta])

    app, _ = app_and_db
    with app.test_client() as c:
        entry = next(
            l for l in c.get("/api/labels").get_json()["labels"]
            if l["labels_file"] == label_path
        )
        offered = c.get("/api/species/search?q=Mexican").get_json()

    assert entry["usable_count"] == 2
    assert entry["hybrid_cross_count"] == 1
    assert offered == ["Mexican Duck", "Mallard × Mexican Duck"]


def test_fetched_cross_and_species_with_shared_name_keep_both_identities(tmp_path, monkeypatch):
    import json
    from unittest.mock import patch

    import labels

    monkeypatch.setattr(labels, "LABELS_DIR", str(tmp_path))
    payload = {"total_results": 2, "results": [
        {"taxon": {"id": 1, "name": "Anas platyrhynchos", "preferred_common_name": "Shared Duck", "rank": "species"}},
        {"taxon": {"id": 2, "name": "Anas platyrhynchos × Mareca strepera",
                   "preferred_common_name": "Shared Duck", "rank": "hybrid"}},
    ]}
    with patch("labels.urllib.request.urlopen") as request:
        request.return_value.__enter__.return_value.read.return_value = json.dumps(payload).encode()
        fetched = labels.fetch_species_list(14, ["birds"], drop_hybrid_crosses=True)
    species_name = "Shared Duck (Anas platyrhynchos)"
    cross_name = "Shared Duck (Anas platyrhynchos × Mareca strepera)"
    assert fetched == [species_name]
    assert fetched.hybrid_crosses == [cross_name]
    path = labels.save_labels("Shared ducks", 14, "California", ["birds"], fetched)
    raw = labels.read_label_file(path)
    assert set(raw) == {species_name, cross_name}
    assert raw.identities[species_name]["taxon_id"] == 1
    assert raw.identities[cross_name]["taxon_id"] == 2
    assert labels.load_merged_labels([{"labels_file": path}]) == [species_name]
    assert labels.label_set_summary({"labels_file": path}) == (1, 0, 1)
