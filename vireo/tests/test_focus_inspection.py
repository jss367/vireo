"""Focus evidence fails closed on stale associations; manual inspection is independent."""
import io

import pytest
from focus_crop import focus_point, valid_subject_box, validate_raster_geometry
from PIL import Image
from services import workspace_folders
from test_burst_evidence import _working_copy


def test_eye_must_belong_to_selected_subject():
    boxes = [dict(category='animal', detector_model='test', box_x=.1, box_y=.1, box_w=.2, box_h=.2),
             dict(category='animal', detector_model='test', box_x=.7, box_y=.7, box_w=.2, box_h=.2)]
    photo = dict(eye_x=.8, eye_y=.8, eye_tenengrad=999)
    assert focus_point(photo, boxes, 'auto', eye_enabled=True) == ((.2, .2), 'subject')
    with pytest.raises(ValueError, match='No eye'):
        focus_point(photo, boxes, 'eye', eye_enabled=True)
    assert focus_point(photo, boxes[::-1], 'eye', eye_enabled=True) == ((.8, .8), 'eye')
    assert focus_point(photo, boxes, 'auto', eye_enabled=True, point=(.8, .8)) == ((.8, .8), 'point')


@pytest.mark.parametrize('values', [(0, 0, 0, .2), (-.1, .1, .2, .2), (.9, 0, .2, .2), (0, 0, float('nan'), .2), (0, 0, .2, float('inf'))])
def test_invalid_boxes_are_not_clamped_into_evidence(values):
    assert not valid_subject_box(dict(zip(('box_x', 'box_y', 'box_w', 'box_h'), values, strict=True)))


def test_upright_geometry_allows_exif_rotation_but_not_a_cropped_raster():
    validate_raster_geometry((2000, 3000), {'width': 6000, 'height': 4000})
    with pytest.raises(ValueError, match='geometry'):
        validate_raster_geometry((3000, 1000), {'width': 6000, 'height': 4000})


def test_stale_mask_and_invalid_primary_require_explicit_inspection(app_and_db):
    app, db = app_and_db
    pid = db.photos.list_page()[0]['id']
    _working_copy(app, db, pid)
    client = app.test_client()
    db.conn.execute('UPDATE photos SET mask_path=?, eye_x=.7, eye_y=.5, eye_tenengrad=50 WHERE id=?', ('missing-mask.png', pid))
    db.commit()
    assert client.get(f'/photos/{pid}/focus-crop').status_code == 404
    assert client.get(f'/photos/{pid}/focus-crop?basis=eye').status_code == 404
    manual = client.get(f'/photos/{pid}/focus-crop?center=.75,.25&size=128&mask=1')
    assert manual.status_code == 200
    assert manual.headers['X-Vireo-Focus-Basis'] == 'point'
    assert manual.headers['X-Vireo-Focus-Mask'] == '0'
    assert Image.open(io.BytesIO(manual.data)).size == (128, 128)
    db.conn.execute('UPDATE photos SET mask_path=NULL WHERE id=?', (pid,))
    db.detections.save(pid, [{'box': {'x': .1, 'y': .2, 'w': -.1, 'h': .2}, 'confidence': .9, 'category': 'animal'}], detector_model='test')
    db.commit()
    assert client.get(f'/photos/{pid}/focus-crop').status_code == 404
    assert client.get(f'/photos/{pid}/focus-crop?center=.3,.2').status_code == 200


def test_crop_raster_rejects_subject_coordinates_and_reports_native_scale(app_and_db):
    app, db = app_and_db
    pid = db.photos.list_page()[0]['id']
    _working_copy(app, db, pid, size=(1200, 400))
    db.conn.execute('UPDATE photos SET width=6000,height=4000 WHERE id=?', (pid,))
    db.detections.save(pid, [{'box': {'x': .1, 'y': .2, 'w': .2, 'h': .2}, 'confidence': .9, 'category': 'animal'}], detector_model='test')
    db.commit()
    client = app.test_client()
    assert client.get(f'/photos/{pid}/focus-crop?source=working').status_code == 404
    manual = client.get(f'/photos/{pid}/focus-crop?source=working&center=.3,.2')
    assert manual.status_code == 200
    assert manual.headers['X-Vireo-Focus-Original-Scale'] == 'unknown'
    assert manual.headers['X-Vireo-Focus-Raster-Size'] == '1200,400'
    assert manual.headers['X-Vireo-Focus-Source'] == 'working_copy'


def test_exif_upright_subject_and_manual_center_hit_same_pixels(app_and_db, tmp_path):
    from services import photo_ingest
    app, db = app_and_db
    folder = workspace_folders.add_folder(db, str(tmp_path), name='EXIF focus')
    path = tmp_path / 'portrait.jpg'
    image = Image.new('RGB', (600, 400), 'blue')
    image.paste('red', (100, 50, 200, 150))
    exif = image.getexif()
    exif[274] = 6
    image.save(path, exif=exif)
    pid = photo_ingest.add_photo(db, folder_id=folder, filename=path.name, extension='.jpg', file_size=path.stat().st_size, file_mtime=path.stat().st_mtime)
    db.conn.execute('UPDATE photos SET width=600,height=400 WHERE id=?', (pid,))
    db.detections.save(pid, [{'box': {'x': .6, 'y': .15, 'w': .3, 'h': .2}, 'confidence': .9, 'category': 'animal'}], detector_model='test')
    db.commit()
    for query in ('basis=subject', 'center=.75,.25'):
        response = app.test_client().get(f'/photos/{pid}/focus-crop?source=original&size=128&{query}')
        assert response.status_code == 200
        assert response.headers['X-Vireo-Focus-Raster-Size'] == '400,600'
        crop = Image.open(io.BytesIO(response.data))
        red, green, blue = crop.getpixel((64, 64))
        assert red > 200 and blue < 40


def test_overlapping_boxes_do_not_authorize_unbound_legacy_eye(app_and_db):
    app, db = app_and_db
    pid = db.photos.list_page()[0]['id']
    _working_copy(app, db, pid)
    db.detections.save(pid, [
        {'box': {'x': .1, 'y': .1, 'w': .6, 'h': .6}, 'confidence': .9, 'category': 'animal'},
        {'box': {'x': .2, 'y': .2, 'w': .6, 'h': .6}, 'confidence': .8, 'category': 'animal'},
    ], detector_model='test')
    db.conn.execute('UPDATE photos SET eye_x=.5,eye_y=.5,eye_tenengrad=999 WHERE id=?', (pid,))
    db.commit()
    client = app.test_client()
    assert client.get(f'/photos/{pid}/focus-crop?basis=eye').status_code == 404
    response = client.get(f'/photos/{pid}/focus-crop')
    assert response.status_code == 200
    assert response.headers['X-Vireo-Focus-Basis'] == 'subject'


def test_primary_changed_while_decoding_does_not_serve_old_association(app_and_db, monkeypatch):
    import web.media as media
    app, db = app_and_db
    pid = db.photos.list_page()[0]['id']
    _working_copy(app, db, pid)
    db.detections.save(pid, [{'box': {'x': .1, 'y': .2, 'w': .2, 'h': .2}, 'confidence': .9, 'category': 'animal'}], detector_model='test')
    db.commit()
    original = media._focus_source_image
    def changed(*args):
        result = original(*args)
        db.conn.execute('UPDATE detections SET box_x=.6 WHERE photo_id=?', (pid,))
        db.commit()
        return result
    monkeypatch.setattr(media, '_focus_source_image', changed)
    response = app.test_client().get(f'/photos/{pid}/focus-crop')
    assert response.status_code == 404
    assert 'changed while decoding' in response.get_json()['error']


@pytest.mark.parametrize('basis', ['frame', 'Frame', ' frame ', ' FRAME '])
@pytest.mark.parametrize('stale_mask', [False, True])
def test_frame_basis_normalization_never_revalidates_unused_subjects(app_and_db, basis, stale_mask):
    app, db = app_and_db
    pid = db.photos.list_page()[0]['id']
    _working_copy(app, db, pid)
    db.detections.save(pid, [{'box': {'x': .1, 'y': .2, 'w': .2, 'h': .2},
                              'confidence': .9, 'category': 'animal'}], detector_model='test')
    if stale_mask:
        db.conn.execute('UPDATE photos SET mask_path=? WHERE id=?', ('stale-mask.png', pid))
    db.commit()
    client = app.test_client()
    response = client.get(f'/photos/{pid}/focus-crop', query_string={'basis': basis, 'size': 128})
    assert response.status_code == 200
    assert response.headers['X-Vireo-Focus-Basis'] == 'frame'
    assert response.headers['X-Vireo-Focus-Center'] == '0.5000,0.5000'
    subject = client.get(f'/photos/{pid}/focus-crop', query_string={'basis': ' Subject '})
    assert subject.status_code == (404 if stale_mask else 200)
