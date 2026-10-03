"""Show Mask draws the subject mask before any local slider moves."""

import io

import numpy as np
import pytest
from PIL import Image
from playwright.sync_api import expect


@pytest.fixture
def masked_photo(live_server, tmp_path):
    folder = tmp_path / 'masked-photos'
    folder.mkdir()
    path = folder / 'heron.png'
    Image.new('RGB', (256, 128), (120, 140, 160)).save(path)
    db = live_server['db']
    folder_id = db.add_folder(str(folder))
    photo_id = db.add_photo(
        folder_id=folder_id, filename=path.name, extension='.png',
        file_size=path.stat().st_size, file_mtime=path.stat().st_mtime,
        width=256, height=128,
    )
    arr = np.zeros((128, 256), dtype=np.uint8)
    arr[:, :128] = 255
    mask_path = folder / f'{photo_id}.sam2-small.png'
    Image.fromarray(arr, 'L').save(mask_path)
    db.upsert_photo_mask(
        photo_id, 'sam2-small', str(mask_path), 'megadetector-v6',
        0.0, 0.0, 0.5, 1.0,
    )
    db.set_active_mask_variant(photo_id, 'sam2-small')
    return photo_id


def test_show_mask_before_any_local_adjustment(live_server, page, masked_photo):
    page.goto(f"{live_server['url']}/edit/{masked_photo}")
    expect(page.locator('#editorFilename')).to_have_text('heron.png')
    page.wait_for_function('!editorState.loading')
    expect(page.locator('#localBand')).to_be_visible()

    with page.expect_response('**/edit-mask-preview?*') as resp:
        page.locator('#maskOverlayBtn').click()
    assert resp.value.ok
    overlay = page.locator('#maskOverlayImg')
    expect(overlay).to_be_visible()
    alpha = np.asarray(Image.open(io.BytesIO(resp.value.body())))[..., 3]
    width = alpha.shape[1]
    assert alpha[:, : width * 2 // 5].mean() > 60
    assert alpha[:, width * 3 // 5:].mean() < 5

    # Moving only Feather re-requests the overlay at the new feather.
    with page.expect_response(
        lambda r: '/edit-mask-preview?' in r.url and 'feather=60' in r.url
    ) as feathered:
        page.locator('#featherRange').evaluate(
            """el => {
                el.value = '60';
                el.dispatchEvent(new Event('input', {bubbles: true}));
            }"""
        )
    assert feathered.value.ok
    expect(overlay).to_be_visible()

    page.locator('#maskOverlayBtn').click()
    expect(overlay).to_be_hidden()
