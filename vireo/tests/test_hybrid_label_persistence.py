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
