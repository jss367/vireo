import json
import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from labels import get_active_labels, set_active_labels, load_merged_labels, get_saved_labels, LABELS_DIR


def test_get_active_labels_empty(tmp_path, monkeypatch):
    """Returns empty list when no active labels configured."""
    monkeypatch.setattr("labels.os.path.expanduser", lambda p: str(tmp_path / p.lstrip("~/")))
    result = get_active_labels()
    assert result == []


def test_set_and_get_active_labels(tmp_path, monkeypatch):
    """set_active_labels stores list, get_active_labels returns it."""
    config_path = str(tmp_path / "labels_active.json")
    labels_dir = str(tmp_path / "labels")
    os.makedirs(labels_dir)

    # Create two label files and their metadata
    for name, slug, species in [
        ("CA Birds", "ca-birds", ["Robin", "Jay"]),
        ("CA Reptiles", "ca-reptiles", ["Lizard", "Snake"]),
    ]:
        txt_path = os.path.join(labels_dir, f"{slug}.txt")
        with open(txt_path, "w") as f:
            for sp in species:
                f.write(sp + "\n")
        meta_path = os.path.join(labels_dir, f"{slug}.json")
        with open(meta_path, "w") as f:
            json.dump({"name": name, "labels_file": txt_path, "species_count": len(species)}, f)

    monkeypatch.setattr("labels.LABELS_DIR", labels_dir)

    def fake_expanduser(p):
        if "labels_active" in p:
            return config_path
        return str(tmp_path / p.lstrip("~/"))

    monkeypatch.setattr("labels.os.path.expanduser", fake_expanduser)

    paths = [
        os.path.join(labels_dir, "ca-birds.txt"),
        os.path.join(labels_dir, "ca-reptiles.txt"),
    ]
    set_active_labels(paths)

    result = get_active_labels()
    assert len(result) == 2
    names = {r["name"] for r in result}
    assert names == {"CA Birds", "CA Reptiles"}


def test_get_active_labels_old_format(tmp_path, monkeypatch):
    """Old single-object format is migrated to a one-element list."""
    config_path = str(tmp_path / "labels_active.json")
    labels_dir = str(tmp_path / "labels")
    os.makedirs(labels_dir)

    txt_path = os.path.join(labels_dir, "ca-birds.txt")
    with open(txt_path, "w") as f:
        f.write("Robin\nJay\n")
    with open(os.path.join(labels_dir, "ca-birds.json"), "w") as f:
        json.dump({"name": "CA Birds", "labels_file": txt_path, "species_count": 2}, f)

    # Write old format: single object with labels_file key
    with open(config_path, "w") as f:
        json.dump({"name": "CA Birds", "labels_file": txt_path, "species_count": 2}, f)

    monkeypatch.setattr("labels.LABELS_DIR", labels_dir)

    def fake_expanduser(p):
        if "labels_active" in p:
            return config_path
        return str(tmp_path / p.lstrip("~/"))

    monkeypatch.setattr("labels.os.path.expanduser", fake_expanduser)

    result = get_active_labels()
    assert len(result) == 1
    assert result[0]["name"] == "CA Birds"


def test_get_active_labels_skips_missing_files(tmp_path, monkeypatch):
    """Label sets whose .txt file is missing are silently skipped."""
    config_path = str(tmp_path / "labels_active.json")

    with open(config_path, "w") as f:
        json.dump({"active_labels": ["/nonexistent/path.txt"]}, f)

    def fake_expanduser(p):
        if "labels_active" in p:
            return config_path
        return str(tmp_path / p.lstrip("~/"))

    monkeypatch.setattr("labels.os.path.expanduser", fake_expanduser)

    result = get_active_labels()
    assert result == []


def test_load_merged_labels_deduplicates(tmp_path):
    """Merging label sets deduplicates and sorts species."""
    dir_ = str(tmp_path / "labels")
    os.makedirs(dir_)

    txt1 = os.path.join(dir_, "birds.txt")
    with open(txt1, "w") as f:
        f.write("Robin\nJay\nSparrow\n")

    txt2 = os.path.join(dir_, "reptiles.txt")
    with open(txt2, "w") as f:
        f.write("Lizard\nSnake\nRobin\n")  # Robin is a duplicate

    label_sets = [
        {"labels_file": txt1, "name": "Birds"},
        {"labels_file": txt2, "name": "Reptiles"},
    ]
    result = load_merged_labels(label_sets)
    assert result == ["Jay", "Lizard", "Robin", "Snake", "Sparrow"]


def test_load_merged_labels_empty():
    """Empty input returns empty list."""
    assert load_merged_labels([]) == []


def test_load_merged_labels_skips_missing(tmp_path):
    """Missing files are skipped, valid ones still load."""
    dir_ = str(tmp_path / "labels")
    os.makedirs(dir_)

    txt1 = os.path.join(dir_, "birds.txt")
    with open(txt1, "w") as f:
        f.write("Robin\nJay\n")

    label_sets = [
        {"labels_file": txt1},
        {"labels_file": "/nonexistent/gone.txt"},
    ]
    result = load_merged_labels(label_sets)
    assert result == ["Jay", "Robin"]
