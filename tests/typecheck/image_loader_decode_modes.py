"""Static-only load_image contract fixture; do not execute image loading.

Check with: basedpyright tests/typecheck/image_loader_decode_modes.py
"""
from typing import assert_type

from float_image import FloatImage
from image_loader import (
    RAW_DECODE_CAMERA_RENDERED,
    RAW_DECODE_JPEG_FIRST,
    RAW_DECODE_LINEAR,
    RAW_DECODE_PRESERVE_HIGHLIGHTS,
    load_image,
)
from PIL import Image


def check_literal_modes() -> None:
    path = "/invented/never-read.raw"
    assert_type(load_image(path), Image.Image | None)
    assert_type(load_image(path, None), Image.Image | None)
    assert_type(load_image(path, None, RAW_DECODE_JPEG_FIRST), Image.Image | None)
    assert_type(load_image(path, None, RAW_DECODE_CAMERA_RENDERED), Image.Image | None)
    assert_type(load_image(path, None, RAW_DECODE_PRESERVE_HIGHLIGHTS), Image.Image | None)
    assert_type(load_image(path, None, RAW_DECODE_LINEAR), Image.Image | FloatImage | None)
    assert_type(load_image(path, raw_decode=RAW_DECODE_JPEG_FIRST), Image.Image | None)
    assert_type(load_image(path, raw_decode=RAW_DECODE_CAMERA_RENDERED), Image.Image | None)
    assert_type(load_image(path, raw_decode=RAW_DECODE_PRESERVE_HIGHLIGHTS), Image.Image | None)
    assert_type(load_image(path, raw_decode=RAW_DECODE_LINEAR), Image.Image | FloatImage | None)


def check_dynamic_mode(raw_decode: str) -> None:
    path = "/invented/never-read.raw"
    assert_type(load_image(path, None, raw_decode), Image.Image | FloatImage | None)
    assert_type(load_image(path, raw_decode=raw_decode), Image.Image | FloatImage | None)
