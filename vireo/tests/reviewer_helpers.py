"""Exercise current-frame reviewer selections in synthetic domain tests."""


def confirmed_reviewer_box(db, photo_id, box):
    from services.subject_frames import issue_frame_token, render_subject_frame, verified_reviewer_runtime
    from subjects import select_reviewer_box

    image, identity = render_subject_frame(db, photo_id, 1920)
    image.close()
    token = issue_frame_token(db, photo_id, identity)
    runtime = verified_reviewer_runtime(db, photo_id, token)
    return select_reviewer_box(db, photo_id, box, frame_runtime=runtime)
