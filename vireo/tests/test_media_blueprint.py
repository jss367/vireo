"""Media's public endpoint identities and per-app dependencies survive extraction."""

from types import SimpleNamespace

from flask import Flask
from web.media import create_media_blueprint
from web.responses import json_error


def _app(variant):
    photo = {"id": 7}
    db = SimpleNamespace(
        photos=SimpleNamespace(get=lambda pid, **kwargs: photo if pid == 7 else None),
        masks_features=SimpleNamespace(
            list_masks_for_photo=lambda pid: [{"variant": variant, "created_at": "test"}],
            active_variant=lambda pid: variant,
        ),
    )

    def unexpected(*args, **kwargs):
        raise AssertionError("mask listing must not launch rendering or jobs")

    app = Flask(__name__)
    app.config["TESTING"] = True
    app.register_blueprint(create_media_blueprint(
        lambda: db, json_error, "/catalog/catalog.db", {"THUMB_CACHE_DIR": "/cache/thumbs"},
        get_runner=unexpected, invalid_preview_cache_paths=set(), clear_preview_cache_invalid=unexpected,
        render_edit_preview=unexpected, cancel_edit_preview=unexpected,
    ))
    return app


def test_media_endpoint_names_and_methods_are_stable():
    app = _app("sam2-small")
    expected = {
        "serve_thumbnail": ("/thumbnails/<filename>", {"GET"}),
        "serve_duplicate_thumbnail": ("/thumbnails/duplicate/<filename>", {"GET"}),
        "serve_mask": ("/masks/<filename>", {"GET"}),
        "api_serve_mask": ("/api/masks/<int:pid>/<variant>.png", {"GET"}),
        "api_photo_masks": ("/api/photos/<int:pid>/masks", {"GET"}),
        "serve_crop_preview": ("/photos/<int:photo_id>/crop", {"GET"}),
        "serve_focus_crop": ("/photos/<int:photo_id>/focus-crop", {"GET"}),
        "serve_full_photo": ("/photos/<int:photo_id>/full", {"GET"}),
        "serve_photo_preview": ("/photos/<int:photo_id>/preview", {"GET"}),
        "serve_edit_mask_preview": ("/photos/<int:photo_id>/edit-mask-preview", {"GET"}),
        "api_cancel_edit_preview": ("/api/edit-preview/cancel", {"POST"}),
        "serve_photo_edit_preview": ("/photos/<int:photo_id>/edit-preview", {"GET"}),
        "api_auto_tone": ("/api/photos/<int:photo_id>/auto-tone", {"GET"}),
        "serve_original_photo": ("/photos/<int:photo_id>/original", {"GET"}),
    }
    actual = {
        rule.endpoint.removeprefix("media."): (rule.rule, set(rule.methods) - {"HEAD", "OPTIONS"})
        for rule in app.url_map.iter_rules() if rule.endpoint.startswith("media.")
    }
    assert actual == expected


def test_media_views_keep_their_own_app_dependencies():
    first, second = _app("sam2-small"), _app("sam3-large")
    for app, variant in [(second, "sam3-large"), (first, "sam2-small")]:
        response = app.test_client().get("/api/photos/7/masks")
        assert response.status_code == 200
        assert response.json == {
            "photo_id": 7, "active": variant,
            "variants": [{"variant": variant, "url": f"/api/masks/7/{variant}.png", "created_at": "test"}],
        }
        assert app.test_client().get("/api/photos/8/masks").status_code == 404
