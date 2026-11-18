"""Original-frame proofs for reviewer confirmation, independent of model identity."""
from __future__ import annotations

import os
import secrets
from typing import TYPE_CHECKING

from detection_id import REVIEWER_FRAME_PREFIX
from itsdangerous import BadSignature, URLSafeTimedSerializer

if TYPE_CHECKING:
    from db import Database
    from PIL import Image

_TOKENS = URLSafeTimedSerializer(secrets.token_bytes(32), salt="reviewer-original-frame-v2")


def original_subject_path(db: Database, photo_id: int) -> str:
    """Never let a JPEG alias hide the RAW/original coordinate source."""
    photo = db.photos.get(photo_id, verify_workspace=True)
    folder = db.folders.get(photo["folder_id"]) if photo is not None else None
    if photo is None or folder is None:
        raise ValueError("Original subject frame unavailable; reconnect the original and retry")
    return os.path.join(folder["path"], photo["filename"])


def render_subject_frame(db: Database, photo_id: int, size: int) -> tuple[Image.Image, str]:
    from image_loader import RAW_DECODE_ALIGNED, load_image

    image = load_image(original_subject_path(db, photo_id), max_size=size, raw_decode=RAW_DECODE_ALIGNED)
    if image is None:
        raise ValueError("Original subject frame unavailable; reconnect the original and retry")
    identity = image.info.get("vireo_subject_frame_id")
    if not isinstance(identity, str) or len(identity) != 64:
        image.close()
        raise ValueError("Original frame could not be verified; reload it before drawing")
    return image, identity


def issue_frame_token(db: Database, photo_id: int, identity: str) -> str:
    return _TOKENS.dumps({"photo": photo_id, "workspace": db.require_workspace_id(), "frame": identity})


def verified_reviewer_runtime(db: Database, photo_id: int, token: object) -> str:
    """Validate the served frame again under the selection's publication lock."""
    if not isinstance(token, str) or len(token) > 4096:
        raise ValueError("Reload the original frame before confirming the reviewer box")
    try:
        proof = _TOKENS.loads(token, max_age=1800)
    except BadSignature as error:
        raise ValueError("Original-frame confirmation expired or is invalid; reload the original") from error
    if (not isinstance(proof, dict) or proof.get("photo") != photo_id
            or proof.get("workspace") != db.require_workspace_id()):
        raise ValueError("Frame confirmation belongs to another photo or workspace; reload the original")
    image, identity = render_subject_frame(db, photo_id, 1920)
    image.close()
    if proof.get("frame") != identity:
        raise ValueError("Original frame changed while drawing; reload it and confirm the box again")
    return REVIEWER_FRAME_PREFIX + identity
