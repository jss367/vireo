import builtins
import json
import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from labels import (
    get_active_labels,
    load_merged_labels,
    read_label_file,
    set_active_labels,
)


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


def test_load_merged_labels_with_metas_returns_consumed_only(tmp_path):
    """``load_merged_labels_with_metas`` must return only the metadata of the
    sets it actually opened and read — otherwise a caller naming the label
    source would claim a file that was deleted between its own existence
    check and the loader's read. Doing this in the same pass closes that
    race (see ``classify_job._load_labels``).
    """
    from labels import load_merged_labels_with_metas

    dir_ = str(tmp_path / "labels")
    os.makedirs(dir_)

    txt = os.path.join(dir_, "birds.txt")
    with open(txt, "w") as f:
        f.write("Robin\n")

    label_sets = [
        {"labels_file": txt, "name": "Birds"},
        {"labels_file": "/nonexistent/gone.txt", "name": "Deleted"},
    ]
    labels, consumed = load_merged_labels_with_metas(label_sets)
    assert list(labels) == ["Robin"]
    # Only the entry whose file was actually read survives.
    assert consumed == [{"labels_file": txt, "name": "Birds"}]


def test_load_merged_labels_dedupes_apostrophe_variants(tmp_path):
    """Two label files that spell the same species with a curly vs plain
    apostrophe must merge into a single canonical entry — otherwise the
    classifier can return one as primary and the other as an alternative,
    and after ``add_prediction`` folds both onto the same UNIQUE row the
    alternative's INSERT-OR-IGNORE upserts ``prediction_review.status =
    'alternative'``, hiding the only top-1 prediction from the pending
    review queue.
    """
    dir_ = str(tmp_path / "labels")
    os.makedirs(dir_)

    txt1 = os.path.join(dir_, "birds_ascii.txt")
    with open(txt1, "w") as f:
        f.write("Say's phoebe\nJay\n")

    txt2 = os.path.join(dir_, "birds_curly.txt")
    with open(txt2, "w", encoding="utf-8") as f:
        f.write("Say’s phoebe\nRobin\n")

    label_sets = [
        {"labels_file": txt1, "name": "Birds ASCII"},
        {"labels_file": txt2, "name": "Birds curly"},
    ]
    result = load_merged_labels(label_sets)
    assert result == ["Jay", "Robin", "Say's phoebe"], (
        "curly and ASCII apostrophe spellings of the same species must "
        f"merge to a single canonical entry — got {result}"
    )


def test_load_merged_labels_dedupes_case_only_variants(tmp_path):
    """Two label files that spell the same species with different case must
    merge into a single entry — otherwise the classifier's softmax gets fed
    both prompts as separate classes, splitting probability between
    near-duplicates. Each result is thresholded independently in
    ``_build_custom_results``, so a valid prediction can fall below the
    configured threshold or lose an alternative slot. SQLite's ``COLLATE
    NOCASE`` and ``add_prediction`` already treat these as one species at
    the storage layer; label merging must match that semantics.
    """
    dir_ = str(tmp_path / "labels")
    os.makedirs(dir_)

    txt1 = os.path.join(dir_, "birds_titlecase.txt")
    with open(txt1, "w") as f:
        f.write("Say's Phoebe\nJay\n")

    txt2 = os.path.join(dir_, "birds_lowercase.txt")
    with open(txt2, "w") as f:
        f.write("say's phoebe\nRobin\n")

    label_sets = [
        {"labels_file": txt1, "name": "Birds Title Case"},
        {"labels_file": txt2, "name": "Birds lowercase"},
    ]
    result = load_merged_labels(label_sets)
    # Exactly one Say's phoebe survives — either capitalization is
    # DB-equivalent under COLLATE NOCASE, so the test asserts the count
    # rather than the specific case.
    say_variants = [n for n in result if n.lower() == "say's phoebe"]
    assert len(say_variants) == 1, (
        "case-only apostrophe variants must merge to a single entry — "
        f"got {result}"
    )
    assert "Jay" in result and "Robin" in result


def test_load_merged_labels_dedupes_case_plus_apostrophe_variants(tmp_path):
    """Case + apostrophe collision (``Say's Phoebe`` ASCII vs ``Say's
    phoebe`` curly) collapses to a single spelling. The prior fold used
    ``normalize_keyword_display`` for the group key, which preserved case
    and left these in separate buckets; the ASCII-NOCASE key matches the
    downstream storage/consensus/alternative dedupe.
    """
    dir_ = str(tmp_path / "labels")
    os.makedirs(dir_)

    txt = os.path.join(dir_, "birds.txt")
    with open(txt, "w", encoding="utf-8") as f:
        f.write("Say's Phoebe\nSay’s phoebe\nJay\n")

    result = load_merged_labels([{"labels_file": txt}])
    say_variants = [n for n in result if n.lower().replace("’", "'")
                    == "say's phoebe"]
    assert len(say_variants) == 1, (
        "case + apostrophe collision must merge to a single entry — "
        f"got {result}"
    )
    # The survivor should be in add_prediction's storage form (ASCII
    # apostrophe), so the classifier's label and the persisted species
    # agree byte-for-byte.
    assert "’" not in say_variants[0], (
        "collision survivor should be in the ASCII apostrophe form so it "
        f"matches what add_prediction stores — got {say_variants[0]!r}"
    )


def test_load_merged_labels_preserves_spelling_without_collision(tmp_path):
    """A curly-apostrophe label with no ASCII twin keeps its source spelling.

    ``compute_fingerprint(labels)`` hashes this exact list and
    ``classifier_runs`` is keyed on the result, so rewriting a label that
    has nothing to dedupe against changes the fingerprint of the label set
    and strands every cached run — re-running inference over the whole
    catalog for no benefit. Five of the six shipped label sets contain such
    lone variants (`Bosc’s Fringe-toed lizard`, `Geoffroy’s Tamarin`,
    `'Anianiau`), which together account for ~70k cached runs.
    """
    from labels_fingerprint import compute_fingerprint

    dir_ = str(tmp_path / "labels")
    os.makedirs(dir_)

    txt = os.path.join(dir_, "reptiles.txt")
    with open(txt, "w", encoding="utf-8") as f:
        f.write("Bosc’s Fringe-toed lizard\nGeyr’s Spiny-tailed Lizard\n"
                "'Anianiau\nSkink\n")

    raw = ["'Anianiau", "Bosc’s Fringe-toed lizard",
           "Geyr’s Spiny-tailed Lizard", "Skink"]
    result = load_merged_labels([{"labels_file": txt}])

    assert result == sorted(raw), (
        "labels with no folded-key collision must keep their source "
        f"spelling so the fingerprint stays stable — got {result}"
    )
    assert compute_fingerprint(result) == compute_fingerprint(sorted(raw))


def test_load_merged_labels_reads_utf8_regardless_of_locale(tmp_path,
                                                            monkeypatch):
    """Label files must be read as UTF-8, not the platform default.

    Python picks the locale codepage for text files opened without an
    explicit ``encoding``, which is cp1252 on Windows. Label files hold
    non-ASCII species names, so the platform default silently mojibakes
    ``Hawaiʻi ʻamakihi`` into ``HawaiÊ»i Ê»amakihi`` (U+02BB cannot be
    represented in cp1252 at all) and can raise on byte sequences cp1252
    leaves undefined. This reproduced as three Windows-only CI failures.
    """
    dir_ = str(tmp_path / "labels")
    os.makedirs(dir_)
    txt = os.path.join(dir_, "mixed.txt")
    with open(txt, "w", encoding="utf-8") as f:
        f.write("Hawaiʻi ʻamakihi\nKöhler’s Vine Snake\nSkink\n")

    # Simulate a Windows interpreter: default text encoding = cp1252.
    real_open = builtins.open

    def locale_default_open(*args, **kwargs):
        mode = args[1] if len(args) > 1 else kwargs.get("mode", "r")
        if "b" not in str(mode) and "encoding" not in kwargs:
            kwargs["encoding"] = "cp1252"
        return real_open(*args, **kwargs)

    monkeypatch.setattr(builtins, "open", locale_default_open)
    result = load_merged_labels([{"labels_file": txt}])

    assert result == ["Hawaiʻi ʻamakihi", "Köhler’s Vine Snake", "Skink"], (
        f"label names were decoded with the locale codepage — got {result}"
    )


def test_load_merged_labels_falls_back_to_cp1252_for_legacy_windows_files(
    tmp_path,
):
    """Files saved by _atomic_write_text before it took an explicit UTF-8
    encoding used the locale default -- cp1252 on Windows. Reading those
    legacy files strictly as UTF-8 raises UnicodeDecodeError on the first
    non-ASCII species (0x92 for a curly apostrophe, 0xF6 for `ö`), which
    would silently drop the active set from classification. The reader
    falls back to cp1252 so those files keep working.
    """
    dir_ = str(tmp_path / "labels")
    os.makedirs(dir_)
    txt = os.path.join(dir_, "legacy.txt")
    # Write cp1252 bytes for `Köhler's Vine Snake` (with a curly apostrophe)
    # and `Say's phoebe` (ASCII apostrophe) -- 0xF6 is `ö` and 0x92 is the
    # curly right single quote in cp1252, both invalid as standalone UTF-8.
    with open(txt, "wb") as f:
        f.write("Köhler’s Vine Snake\nSay's phoebe\n".encode("cp1252"))

    result = load_merged_labels([{"labels_file": txt}])

    assert result == ["Köhler’s Vine Snake", "Say's phoebe"], (
        "legacy cp1252 label file did not round-trip through the fallback: "
        f"got {result}"
    )


def test_read_label_file_falls_back_to_cp1252_for_legacy_windows_files(
    tmp_path,
):
    """``read_label_file`` is the shared primitive used by every direct
    label-file reader (``load_merged_labels`` and every ``labels_file``
    branch in ``app.py``/``classify_job.py``). A legacy Windows install
    saved label sets in cp1252 before the writer became explicit UTF-8, so
    reading such a file strictly as UTF-8 raises ``UnicodeDecodeError`` on
    the first non-ASCII byte -- 0xF6 for ``ö``, 0x92 for the curly right
    single quote -- and hides the entire label set from classification.
    The helper's fallback must keep those legacy files loadable through
    every code path that touches label bytes, not just the merge path.
    """
    dir_ = str(tmp_path / "labels")
    os.makedirs(dir_)
    txt = os.path.join(dir_, "legacy.txt")
    with open(txt, "wb") as f:
        f.write("Köhler’s Vine Snake\nSay's phoebe\n".encode("cp1252"))

    result = read_label_file(txt)
    assert result == ["Köhler’s Vine Snake", "Say's phoebe"], (
        "legacy cp1252 label file did not round-trip through the fallback: "
        f"got {result}"
    )


def test_load_merged_labels_preserves_okina_letters(tmp_path):
    """The apostrophe fold in ``normalize_keyword_display`` deliberately
    excludes the Hawaiian okina (U+02BB, category Lm — a letter, not
    punctuation). Species names like ``ʻApapane`` and ``Hawaiʻi ʻamakihi``
    must round-trip through the label loader unchanged."""
    dir_ = str(tmp_path / "labels")
    os.makedirs(dir_)

    txt = os.path.join(dir_, "hawaii.txt")
    with open(txt, "w", encoding="utf-8") as f:
        f.write("ʻApapane\nHawaiʻi ʻamakihi\n")

    result = load_merged_labels([{"labels_file": txt}])
    assert result == ["Hawaiʻi ʻamakihi", "ʻApapane"]
