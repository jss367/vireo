import sqlite3
import sys
from pathlib import Path

import pytest
from PIL import Image

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / 'src'))
from keeper_eval.curate import curate


@pytest.fixture
def evidence(tmp_path):
    catalog_dir = tmp_path / 'catalog'
    catalog_dir.mkdir()
    sources = tmp_path / 'originals'
    sources.mkdir()
    catalog = catalog_dir / 'photos.db'
    conn = sqlite3.connect(catalog)
    conn.executescript('''
        CREATE TABLE folders(id INTEGER PRIMARY KEY,path TEXT);
        CREATE TABLE photos(id INTEGER PRIMARY KEY,folder_id INTEGER,filename TEXT,width INTEGER,height INTEGER,camera_model TEXT);
        CREATE TABLE detections(id INTEGER PRIMARY KEY,photo_id INTEGER,box_x REAL,box_y REAL,box_w REAL,box_h REAL,
            detector_model TEXT,runtime_fingerprint TEXT);
    ''')
    conn.execute('INSERT INTO folders VALUES(1,?)', (str(sources),))
    requests = []
    for i in range(10):
        name = f'original-{i}.png'
        exif = Image.Exif()
        exif[272] = 'test-camera'
        Image.new('RGB', (80, 60), (i * 20, 80, 100)).save(sources / name, exif=exif)
        conn.execute('INSERT INTO photos VALUES(?,1,?,80,60,?)', (i + 1, name, 'test-camera'))
        conn.execute('INSERT INTO detections VALUES(?,?,.25,.25,.5,.5,?,?)', (i + 1, i + 1, 'test-detector', 'test-runtime'))
        requests.append({'photo_id': i + 1, 'detection_id': i + 1, 'sample_id': f'{i + 1:012x}',
                         'group_id': f'encounter-{i}', 'session_id': 'test-session',
                         'partition': 'development' if i < 2 else 'diagnostic' if i < 4 else 'holdout'})
    conn.commit()
    conn.close()
    spec = {'label_definition': 'Synthetic test judgments, never calibration data', 'samples': requests}
    bundle = tmp_path / 'bundle'
    manifest = curate(catalog, spec, bundle)
    return {'bundle': bundle, 'manifest': manifest, 'db': catalog, 'spec': spec, 'sources': sources}
