"""Exercise current-frame reviewer selections in synthetic domain tests."""


def confirmed_reviewer_box(db, photo_id, box, *, edit=None):
    from services.subject_frames import issue_frame_token, render_subject_frame, verified_reviewer_runtime
    from subjects import select_reviewer_box

    image, identity = render_subject_frame(db, photo_id, 1920)
    image.close()
    token = issue_frame_token(db, photo_id, identity)
    if edit is not None:
        from services.subject_selections import _save_box
        from subjects import reviewer_revision

        with db.transaction(immediate=True):
            _save_box(db, photo_id, {"operation": "edit", "detection_id": edit,
                "revision": reviewer_revision(db.detections.row(edit)), "box": box, "frame_token": token})
        return edit
    runtime = verified_reviewer_runtime(db, photo_id, token)
    return select_reviewer_box(db, photo_id, box, frame_runtime=runtime)


def original_frame_detections(path, detections):
    """Stand in for detection on a generated original, retaining actual frame proof."""
    from image_loader import RAW_DECODE_ALIGNED, load_image

    with load_image(path, max_size=1280, raw_decode=RAW_DECODE_ALIGNED) as image:
        identity = image.info["vireo_subject_frame_id"]
    return [{**detection, "frame_id": identity} for detection in detections]
