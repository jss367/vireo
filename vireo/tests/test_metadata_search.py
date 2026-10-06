import json
import shutil
import subprocess
from pathlib import Path

import pytest
from db import Database


def parse_queries(queries, options=None):
    node = shutil.which("node")
    if not node:
        pytest.skip("Node is required to exercise the browser search parser")
    parser = Path(__file__).parents[1] / "static" / "vireo-search.js"
    script = """
      const {parse} = require(process.argv[1]);
      const {queries, options} = JSON.parse(require('fs').readFileSync(0, 'utf8'));
      process.stdout.write(JSON.stringify(queries.map(q => {
        try { return {rules: [parse(q, options)]}; }
        catch (e) { return {error: e.message}; }
      })));
    """
    result = subprocess.run(
        [node, "-e", script, str(parser)], input=json.dumps({"queries": queries, "options": options}),
        capture_output=True, text=True, check=True, timeout=15,
    )
    return json.loads(result.stdout)


@pytest.fixture
def catalog(tmp_path, monkeypatch):
    import config
    monkeypatch.setattr(config, "load", lambda: {"detector_confidence": 0.2})
    db = Database(str(tmp_path / "search.db"))
    db.set_active_workspace(db.ensure_default_workspace())
    folder = db.add_folder("/photos/Wetlands", name="Lake visit")
    ids = {}
    for name in ("hawk", "owl", "robin", "empty"):
        ids[name] = db.add_photo(folder_id=folder, filename=f"{name}.jpg", extension=".jpg",
                                 file_size=100, file_mtime=1.0)
    db.conn.execute(
        "UPDATE photos SET camera_make='Canon', camera_model='EOS R5', lens='600mm', "
        "iso=54321, latitude=37.76543, timestamp='2025-11-23T08:30:00', exif_data=? WHERE id=?",
        (json.dumps({
            "EXIF": {"Artist": "Zoë García", "Copyright": "100%_mine", "SerialNumber": "SN-987654"},
            "IPTC": {"Caption-Abstract": "A quiet morning", "City": "Monterey"},
            "File": {"FileName": "previous-filename.jpg", "Directory": "/previous-directory"},
            "XMP": {"Description": {"en": "Lake mist", "fr": "Brume matinale"},
                    "Creator": ["Alice", "Bob"], "CustomBoolean": True,
                    "Subject": ["removed-keyword"], "Rating": 99999},
        }), ids["hawk"]),
    )
    db.tag_photo(ids["hawk"], db.add_keyword("Perched"))
    db.tag_photo(ids["owl"], db.add_keyword("Perched"))
    db.tag_photo(ids["owl"], db.add_keyword("Barn Owl", is_species=True))
    taxon_id = db.conn.execute(
        "INSERT INTO taxa (name, common_name, rank) VALUES ('Tyto alba', 'Western Barn Owl', 'species')"
    ).lastrowid
    db.conn.execute("UPDATE keywords SET taxon_id=? WHERE name='Barn Owl'", (taxon_id,))
    db.conn.execute("UPDATE photos SET exif_data='broken json' WHERE id=?", (ids["empty"],))
    db.set_color_label(ids["robin"], "purple")
    db.conn.commit()
    yield db, ids
    db.close()


def test_boolean_search_matches_catalog(catalog):
    db, ids = catalog
    cases = [
        ("hawk OR owl", {"hawk", "owl"}),
        ("hawk OR owl AND Perched", {"hawk", "owl"}),
        ("hawk OR robin AND Perched", {"hawk"}),
        ("(hawk OR robin) AND Perched", {"hawk"}),
        ("NOT (hawk OR owl)", {"robin", "empty"}),
        ("Perched NOT hawk", {"owl"}),
        ("NOT NOT hawk", {"hawk"}),
        ("hawk Canon", {"hawk"}),
        ('"A quiet morning"', {"hawk"}),
        ('"morning A"', set()),
        ('"Alice Bob"', set()),  # phrases never cross metadata values
        ("purple OR (owl NOT Perched)", {"robin"}),
        ("hawk or owl", set()),  # lowercase words remain literal
        ('"OR"', {"hawk"}),  # literal substring of "morning"
        ("", {"hawk", "owl", "robin", "empty"}),
    ]
    for (query, expected), parsed in zip(cases, parse_queries([q for q, _ in cases]), strict=True):
        assert "error" not in parsed, (query, parsed)
        assert set(db.query_photo_ids(parsed["rules"])) == {ids[name] for name in expected}, query


@pytest.mark.parametrize("term, names", [
    ("haw", {"hawk"}), ("wetlands", {"hawk", "owl", "robin", "empty"}),
    ("Lake visit", {"hawk", "owl", "robin", "empty"}),
    ("Canon", {"hawk"}), ("EOS R5", {"hawk"}), ("600mm", {"hawk"}),
    ("54321", {"hawk"}), ("37.76543", {"hawk"}), ("2025-11-23", {"hawk"}),
    ("SN-987654", {"hawk"}), ("quiet morning", {"hawk"}), ("Monterey", {"hawk"}),
    ("Lake mist", {"hawk"}), ("Brume matinale", {"hawk"}), ("Zoë García", {"hawk"}),
    ("Alice", {"hawk"}), ("Bob", {"hawk"}), ("Perched", {"hawk", "owl"}),
    ("Barn Owl", {"owl"}), ("purple", {"robin"}), ("100%_mine", {"hawk"}),
    ("%", {"hawk"}), ("_", {"hawk"}), ("' OR 1=1 --", set()),
    ("removed-keyword", set()), ("99999", set()), ("SerialNumber", set()),
    ("previous-filename", set()), ("previous-directory", set()), ("true", {"hawk"}),
    ("Tyto alba", {"owl"}), ("Western Barn Owl", {"owl"}),
])
def test_metadata_values_and_negative_complement(catalog, term, names):
    db, ids = catalog
    rule = {"field": "metadata", "op": "contains", "value": term}
    assert set(db.query_photo_ids([rule])) == {ids[name] for name in names}
    rule["op"] = "not_contains"
    assert set(db.query_photo_ids([rule])) == {pid for name, pid in ids.items() if name not in names}


def test_invalid_search_syntax():
    queries = [
        "hawk OR", "AND hawk", "NOT", "hawk AND OR owl", "()", "(hawk", "hawk)",
        '"hawk', '""', "(" * 18 + "hawk" + ")" * 18, "NOT " * 18 + "hawk", "x" * 4097,
    ]
    assert all(result.get("error") for result in parse_queries(queries))


def test_quoted_escapes_and_literal_paths():
    queries = [r'"say \"hello\""', r'C:\Photos\bird.jpg', r'"C:\Photos\bird.jpg"']
    values = [result["rules"][0]["value"] for result in parse_queries(queries)]
    assert values == ['say "hello"', r'C:\Photos\bird.jpg', r'C:\Photos\bird.jpg']


def test_keyword_scope_matches_keyword_names_only(catalog):
    db, ids = catalog
    [metadata] = parse_queries(['"Lake visit" OR Perched'])
    [keyword] = parse_queries(['"Lake visit" OR Perched'], {"field": "keyword"})
    assert {r["field"] for r in keyword["rules"][0]["rules"]} == {"keyword"}
    # The folder name matches every photo as metadata, never as a keyword.
    assert set(db.query_photo_ids(metadata["rules"])) == set(ids.values())
    assert set(db.query_photo_ids(keyword["rules"])) == {ids["hawk"], ids["owl"]}


def test_number_search_matches_where_a_number_starts(catalog):
    """``7688`` finds file number 7688, not every value with 7688 buried in
    its digits: hashes, scores, byte counts, coefficients, shutter counts."""
    db, ids = catalog
    folder = db.add_folder("/photos/20240712", name="20240712")
    filename = db.add_photo(folder_id=folder, filename="_D857688.NEF", extension=".nef",
                            file_size=1, file_mtime=1.0)
    file_number = db.add_photo(folder_id=folder, filename="a.nef", extension=".nef",
                               file_size=1, file_mtime=1.0)
    noise = db.add_photo(folder_id=folder, filename="b.nef", extension=".nef",
                         file_size=31837688, file_mtime=1.0)
    db.conn.execute("UPDATE photos SET exif_data=? WHERE id=?", (json.dumps({
        "MakerNotes": {"FileNumber": "100-7688"},
        "Composite": {"LensSpec": "180 600 5.6 6.3", "GPSPosition": "32.853585 -117.006978"},
    }), file_number))
    db.conn.execute(
        "UPDATE photos SET file_hash='a104b76885', quality_score=0.7688, "
        "subject_size=0.1768838, burst_id='B7688', exif_data=? WHERE id=?", (json.dumps({
            "Composite": {"FOV": "2.85582458251892 0.176888033540145"},
            "MakerNotes": {"ShutterCount": 157688, "VignetteCoefficient1": 0.002911567688},
            "EXIF": {"ThumbnailLength": 7688, "StripOffsets": "7688 9000",
                     "JpgFromRaw": "(Binary data 7688 bytes, use -b option to extract)"},
        }), noise))
    db.conn.commit()

    def search(term):
        return set(db.query_photo_ids([{"field": "metadata", "op": "contains", "value": term}]))

    assert search("7688") == {filename, file_number}
    assert search("0712") == {filename, file_number, noise}  # folder names are text
    for term in ("5.6", "6.3", "180 600", "117", "-117.0", "32.853585 -117"):
        assert search(term) == {file_number}, term
    assert search("157688") == {noise}
    assert search("0.0029") == {noise}
    assert search("Binary") == set()


def test_metadata_search_excludes_exiftool_file_size(catalog):
    """Raw and formatted ExifTool byte counts never answer metadata searches."""
    db, ids = catalog
    db.conn.execute("UPDATE photos SET exif_data=? WHERE id=?", (json.dumps({
        "System": {"FileSize": 31837688}, "File": {"FileSize": "31 MB"},
    }), ids["empty"]))
    for term in ("3183", "31837688", "31 MB"):
        rule = {"field": "metadata", "op": "contains", "value": term}
        assert ids["empty"] not in db.query_photo_ids([rule])


@pytest.mark.parametrize("number,term,expected", [
    (1e-10, "10", False), (-1e-10, "10", False),
    (1e-10, "1", True), (-1e-10, "1", True),
    (-117.006978, "117", True), (-117.006978, "-117", True),
])
@pytest.mark.parametrize("source", ["photo", "exif"])
def test_scalar_numbers_do_not_match_inside_exponents(catalog, number, term, expected, source):
    """Scalar prefixes include an optional sign, never an exponent's digits."""
    db, ids = catalog
    if source == "photo":
        db.conn.execute("UPDATE photos SET latitude=? WHERE id=?", (number, ids["empty"]))
    else:
        db.conn.execute("UPDATE photos SET exif_data=? WHERE id=?",
                        (json.dumps({"EXIF": {"Number": number}}), ids["empty"]))
    rule = {"field": "metadata", "op": "contains", "value": term}
    assert (ids["empty"] in db.query_photo_ids([rule])) == expected


@pytest.mark.parametrize("op,value", [("is", "hawk"), ("contains", ""), ("contains", None),
                                    ("contains", 123), pytest.param("contains", "x" * 4097, id="too-long")])
def test_invalid_metadata_rules_are_rejected(catalog, op, value):
    db, _ = catalog
    with pytest.raises(ValueError):
        db.query_photo_ids([{"field": "metadata", "op": op, "value": value}])


def test_metadata_search_sees_edits_and_scopes(catalog):
    db, ids = catalog
    rules = [{"field": "metadata", "op": "contains", "value": "Perched"}]
    keyword_id = db.conn.execute("SELECT id FROM keywords WHERE name='Perched'").fetchone()[0]
    db.conn.execute("UPDATE keywords SET name='Resting' WHERE id=?", (keyword_id,))
    assert db.query_photo_ids(rules) == []
    rules[0]["value"] = "Resting"
    assert set(db.query_photo_ids(rules)) == {ids["hawk"], ids["owl"]}
    db.untag_photo(ids["hawk"], keyword_id)
    assert db.query_photo_ids(rules) == [ids["owl"]]
    collection = db.add_collection("Resting birds", json.dumps(rules))
    assert db.count_photos_for_rules([], collection_id=collection) == 1
    other_ws = db.create_workspace("Other workspace")
    db.set_active_workspace(other_ws)
    assert db.query_photo_ids(rules) == []


def test_metadata_search_uses_current_visible_predictions(catalog):
    db, ids = catalog
    det = db.save_detections(ids["empty"], [
        {"box": {"x": 0, "y": 0, "w": 1, "h": 1}, "confidence": 0.9, "category": "animal"},
    ], detector_model="test")[0]
    for fingerprint, species, timestamp in [("old", "Old species", "2025-01-01"),
                                            ("new", "New species", "2026-01-01")]:
        db.conn.execute(
            "INSERT INTO predictions (detection_id, classifier_model, labels_fingerprint, species, "
            "scientific_name, source_taxon_id, taxonomy_family, confidence, created_at) VALUES (?, 'test', "
            "?, ?, 'Strix nebulosa', 19893, 'Strigidae', 0.9, ?)", (det, fingerprint, species, timestamp),
        )
    db.conn.commit()
    def search(value):
        return db.query_photo_ids([{"field": "metadata", "op": "contains", "value": value}])
    assert search("Old species") == []
    assert search("New species") == [ids["empty"]]
    assert search("Strix nebulosa") == [ids["empty"]]
    assert search("Strigidae") == [ids["empty"]]
    current_id = db.conn.execute("SELECT MAX(id) FROM predictions").fetchone()[0]
    db.update_prediction_status(current_id, "alternative")
    assert search("New species") == []
    db.update_prediction_status(current_id, "reviewed")
    assert search("reviewed") == [ids["empty"]]
    db.conn.execute("UPDATE detections SET detector_confidence=0.01 WHERE id=?", (det,))
    assert search("New species") == []


def test_metadata_search_ignores_guessed_binomials_on_custom_labels(catalog):
    """A custom-label row without a source taxon stored a binomial guessed from
    text, sometimes another species' (legacy burst enrichment). Its label and
    ranks answer a search; the guessed binomial does not. A fixed-head row's
    binomial is the model's own output and does."""
    db, ids = catalog
    det = db.save_detections(ids["empty"], [
        {"box": {"x": 0, "y": 0, "w": 1, "h": 1}, "confidence": 0.9, "category": "animal"},
    ], detector_model="test")[0]
    db.add_prediction(det, "Lilac-crowned Amazon", 0.9, "BioCLIP-2.5", labels_fingerprint="custom",
                      taxonomy={"scientific_name": "Amazona rhodocorytha", "genus": "Amazona"})
    db.add_prediction(det, "Red-crowned Parrot", 0.8, "iNat21 (EVA-02 Large)", labels_fingerprint="tol",
                      taxonomy={"scientific_name": "Amazona viridigenalis"})

    def search(value):
        return db.query_photo_ids([{"field": "metadata", "op": "contains", "value": value}])
    assert search("rhodocorytha") == []
    assert search("Lilac-crowned") == [ids["empty"]]
    assert search("Amazona") == [ids["empty"]]
    assert search("viridigenalis") == [ids["empty"]]


def test_metadata_search_ignores_predictions_on_identified_photos(catalog):
    """A photo the user identified answers search by its species, not by a
    classifier guess on some other detection (a Wood duck found by "least"
    through a stray "Least Grebe" box)."""
    db, ids = catalog
    genus = db.add_keyword("Tyto", is_species=True)
    genus_taxon = db.conn.execute(
        "INSERT INTO taxa (name, common_name, rank) VALUES ('Tyto', 'Masked owls', 'genus')"
    ).lastrowid
    db.conn.execute("UPDATE keywords SET taxon_id=? WHERE id=?", (genus_taxon, genus))
    db.tag_photo(ids["robin"], genus)
    for name in ("owl", "hawk", "robin", "empty"):
        det = db.save_detections(ids[name], [
            {"box": {"x": 0, "y": 0, "w": 1, "h": 1}, "confidence": 0.9, "category": "animal"},
        ], detector_model="test")[0]
        db.conn.execute(
            "INSERT INTO predictions (detection_id, classifier_model, labels_fingerprint, species, "
            "scientific_name, confidence) VALUES (?, 'test', 'tol', 'Least Grebe', "
            "'Tachybaptus dominicus', 0.9)", (det,),
        )
    db.conn.commit()

    def search(value, op="contains"):
        return sorted(db.query_photo_ids([{"field": "metadata", "op": op, "value": value}]))

    # owl is identified as Barn Owl; hawk only has a non-species keyword and
    # robin only a genus, so their predictions still answer search.
    unidentified = sorted([ids["hawk"], ids["robin"], ids["empty"]])
    assert search("Least Grebe") == unidentified
    assert search("Tachybaptus") == unidentified
    assert ids["owl"] in search("Least Grebe", op="not_contains")
    assert search("Barn Owl") == [ids["owl"]]


def test_color_search_is_workspace_scoped_for_shared_photos(catalog):
    db, ids = catalog
    original_ws = db._ws_id()
    other_ws = db.create_workspace("Shared photos")
    folder = db.conn.execute("SELECT folder_id FROM photos WHERE id=?", (ids["robin"],)).fetchone()[0]
    db.add_workspace_folder(other_ws, folder)
    rule = [{"field": "metadata", "op": "contains", "value": "purple"}]
    assert db.query_photo_ids(rule) == [ids["robin"]]
    db.set_active_workspace(other_ws)
    assert db.query_photo_ids(rule) == []
    db.set_active_workspace(original_ws)
    assert db.query_photo_ids(rule) == [ids["robin"]]


@pytest.mark.parametrize("raw_exif, term", [
    ('{"EXIF": {"Artist": "\\u0068awk-watcher"}}', "hawk"),  # escape hides ASCII
    ('{"EXIF": {"Note": "line\\nbreak"}}', "line\nbreak"),
    ('{"EXIF": {"Note": "say \\"cheese\\""}}', 'say "cheese"'),
    ('{"EXIF": {"ExposureTime": 1e-05}}', "1.0e-05"),
    ('{"EXIF": {"Offset": 0.7999999999999999}}', "0.8"),
    ('{"EXIF": {"Offset": 5.0e2}}', "500.0"),
    ('{"EXIF": {"Serial": 12345678901234567890}}', "1.23456789012346e+19"),
    ('{"EXIF": {"Huge": 1e999}}', "Inf"),
    ('{"EXIF": {"Huge": -1e999}}', "-inf"),
    ('{"EXIF": {"Model": "Plain text"}}', "plain TEXT"),
    ('{"EXIF": {"Flash": false}}', "fals"),
])
def test_raw_text_shortcut_never_drops_a_rendered_match(catalog, raw_exif, term):
    """Values whose rendering differs from the stored JSON still match."""
    db, ids = catalog
    db.conn.execute("UPDATE photos SET exif_data=? WHERE id=?", (raw_exif, ids["robin"]))
    rule = {"field": "metadata", "op": "contains", "value": term}
    assert ids["robin"] in db.query_photo_ids([rule])
    rule["op"] = "not_contains"
    assert ids["robin"] not in db.query_photo_ids([rule])


@pytest.mark.parametrize("term, shortcut", [
    ("hawk", True), ("f/2.8", True), ("Info", True), ("_", True),
    ("2024", False), ("1.4", False), ("1E-05", False), ("inf", False), ("-In", False),
])
def test_raw_text_shortcut_only_for_terms_no_number_renders_as(term, shortcut):
    from metadata_search import raw_text_rules_out
    assert raw_text_rules_out(term) is shortcut


def test_browse_summary_runs_the_metadata_filter_once(catalog):
    db, ids = catalog
    db.conn.execute("UPDATE photos SET rating=5 WHERE id=?", (ids["owl"],))  # caller's open write
    statements = []
    db.conn.set_trace_callback(statements.append)
    try:
        summary = db.get_browse_summary(
            rules=[{"field": "metadata", "op": "contains", "value": "Perched"}])
    finally:
        db.conn.set_trace_callback(None)
    assert summary["filtered_total"] == 2
    assert summary["folder_counts"][0]["count"] == 2
    assert sum("json_tree" in sql for sql in statements) == 1
    assert db.conn.execute(
        "SELECT 1 FROM temp.sqlite_master WHERE name='_browse_summary_ids'").fetchone() is None
    assert db.conn.in_transaction
    db.conn.rollback()
    assert db.conn.execute("SELECT rating FROM photos WHERE id=?", (ids["owl"],)).fetchone()[0] != 5


def test_browse_summary_interrupted_mid_materialization_cleans_up(catalog):
    """A superseded search interrupts the id INSERT, which makes SQLite roll
    back the whole transaction on its own. The interrupt must surface as-is
    (not as a missing-savepoint error) and leave the connection usable."""
    import sqlite3

    db, _ = catalog
    db.conn.commit()
    materializing = []
    db.conn.set_trace_callback(
        lambda sql: materializing.append(sql.startswith("INSERT INTO _browse_summary_ids")))
    db.conn.set_progress_handler(lambda: 1 if materializing and materializing[-1] else 0, 1)
    try:
        with pytest.raises(sqlite3.OperationalError, match="interrupted"):
            db.get_browse_summary(
                rules=[{"field": "metadata", "op": "contains", "value": "Perched"}])
    finally:
        db.conn.set_progress_handler(None, 0)
        db.conn.set_trace_callback(None)
    assert not db.conn.in_transaction
    assert db.conn.execute(
        "SELECT 1 FROM temp.sqlite_master WHERE name='_browse_summary_ids'").fetchone() is None
    assert db.get_browse_summary(
        rules=[{"field": "metadata", "op": "contains", "value": "Perched"}])["filtered_total"] == 2
