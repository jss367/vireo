from pathlib import Path

import pytest

from encounter_eval.common import configure_repo

REPO = configure_repo(Path(__file__).resolve().parents[3])


@pytest.fixture
def library(tmp_path):
    """A real small SQLite library; no application initialization or ML models."""
    import sqlite3

    from repositories.pipeline_features import PIPELINE_PHOTO_COLS

    path = tmp_path / "library.db"
    conn = sqlite3.connect(path)
    cols = [part.strip().removeprefix("p.") for part in PIPELINE_PHOTO_COLS.split(",")]
    conn.execute("CREATE TABLE photos (" + ",".join("id INTEGER PRIMARY KEY" if c == "id" else c for c in cols)
                 + ",file_hash,thumb_path)")
    conn.executescript("""
    CREATE TABLE workspaces(id INTEGER PRIMARY KEY, name TEXT);
    CREATE TABLE db_meta(key TEXT PRIMARY KEY, value TEXT);
    CREATE TABLE workspace_folders(workspace_id INTEGER, folder_id INTEGER);
    CREATE TABLE folders(id INTEGER PRIMARY KEY, path TEXT);
    CREATE TABLE taxa(id INTEGER PRIMARY KEY, inat_id INTEGER, name TEXT, common_name TEXT, rank TEXT);
    CREATE TABLE keywords(id INTEGER PRIMARY KEY, name TEXT, taxon_id INTEGER, is_species INTEGER, type TEXT);
    CREATE TABLE photo_keywords(photo_id INTEGER, keyword_id INTEGER, source TEXT);
    CREATE TABLE detections(id INTEGER PRIMARY KEY, photo_id INTEGER, detector_model TEXT, runtime_fingerprint TEXT,
        box_x REAL, box_y REAL, box_w REAL, box_h REAL, detector_confidence REAL, category TEXT);
    CREATE TABLE detector_runs(photo_id INTEGER, detector_model TEXT, box_count INTEGER);
    CREATE TABLE predictions(id INTEGER PRIMARY KEY, detection_id INTEGER, species TEXT, scientific_name TEXT,
        confidence REAL, classifier_model TEXT, labels_fingerprint TEXT, created_at TEXT);
    CREATE TABLE classifier_runs(detection_id INTEGER, classifier_model TEXT, labels_fingerprint TEXT,
        runtime_fingerprint TEXT, input_fingerprint TEXT);
    INSERT INTO workspaces VALUES(1, 'Example library');
    INSERT INTO folders VALUES(1, '/photos/example');
    INSERT INTO workspace_folders VALUES(1,1);
    INSERT INTO taxa VALUES(1,101,'Tringa erythropus','Spotted Redshank','species');
    INSERT INTO taxa VALUES(2,102,'Plegadis falcinellus','Glossy Ibis','species');
    INSERT INTO keywords VALUES(1,'Spotted Redshank',1,1,'taxonomy');
    INSERT INTO keywords VALUES(2,'Glossy Ibis',2,1,'taxonomy');
    """)
    for i in range(1, 13):
        species = "Spotted Redshank" if i < 7 else "Glossy Ibis"
        conn.execute("INSERT INTO photos(id,folder_id,filename,timestamp) VALUES(?,1,?,?)",
                     (i, f"photo-{i:03}.jpg", f"2026-01-01T12:00:{i:02}"))
        conn.execute("INSERT INTO photo_keywords VALUES(?,?,?)", (i, 1 if i < 7 else 2, "manual"))
        if i != 6:
            conn.execute("INSERT INTO detections VALUES(?,?, 'megadetector-v6','runtime',.1,.1,.4,.4,.9,'animal')", (i, i))
            conn.execute("INSERT INTO predictions VALUES(?,?,?,?,?,?,?,?)", (i, i, species, None, .95, "model-a", "labels-1", "2026-01-02"))
        conn.execute("INSERT INTO detector_runs VALUES(?,'megadetector-v6',?)", (i, int(i != 6)))
    conn.commit()
    conn.close()
    return path
