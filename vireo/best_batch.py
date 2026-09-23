"""Best Batch: find the burst around one photo and rank it.

``best_batch_scope`` locates a likely camera burst around a seed photo
(same-folder filename sequence, bounded by capture time, with a
capture-time fallback) and ``build_best_batch_response`` scores it with the
pipeline's selected-batch review and shapes the ``/api/photos/<id>/best-batch``
payload.
"""

import os
import re
from datetime import datetime


def _filename_sequence_key(filename):
    """Return (prefix, number, width, ext) for names like DSC_3069.NEF."""
    stem, ext = os.path.splitext(filename or "")
    match = re.match(r"^(.*?)(\d+)$", stem)
    if not match:
        return None
    digits = match.group(2)
    return (match.group(1), int(digits), len(digits), ext.lower())


def _parse_capture_timestamp(value):
    if value is None or isinstance(value, datetime):
        return value
    try:
        return datetime.fromisoformat(str(value))
    except (TypeError, ValueError):
        return None


def _time_gap_seconds(a, b):
    ta = _parse_capture_timestamp(a)
    tb = _parse_capture_timestamp(b)
    if ta is None or tb is None:
        return None
    try:
        return abs((ta - tb).total_seconds())
    except TypeError:
        return None


def best_batch_scope(db, seed_photo_id, max_gap_seconds=8.0, max_sequence_gap=2, max_photos=120):
    """Find a likely burst/batch around one seed photo.

    The first pass follows same-folder filename sequence numbers, which matches
    camera batches such as DSC_3069...DSC_3133. Capture time is used as a
    boundary when available. If a filename has no trailing number, fall back to
    neighboring capture-time rows in the folder.
    """
    seed = db.get_photo(seed_photo_id, verify_workspace=True)
    if not seed:
        return None, "Photo not found"

    folder_id = seed["folder_id"]
    seed_key = _filename_sequence_key(seed["filename"])
    seed_ext = (seed["extension"] or os.path.splitext(seed["filename"])[1]).lower()
    ws = db._ws_id()

    def _limited(rows):
        if len(rows) <= max_photos:
            return rows
        seed_idx = next(
            (i for i, row in enumerate(rows) if row["id"] == seed_photo_id),
            len(rows) // 2,
        )
        half = max_photos // 2
        start = max(0, seed_idx - half)
        end = min(len(rows), start + max_photos)
        start = max(0, end - max_photos)
        return rows[start:end]

    def _time_ok(left, right, require_timestamps=False):
        gap = _time_gap_seconds(left["timestamp"], right["timestamp"])
        if gap is None:
            return not require_timestamps
        return gap <= max_gap_seconds

    if seed_key:
        prefix, _, width, _ = seed_key
        rows = db.conn.execute(
            """SELECT p.id, p.folder_id, p.filename, p.extension, p.timestamp,
                      p.flag, p.rating, p.quality_score, p.sharpness
               FROM photos p
               JOIN workspace_folders wf ON wf.folder_id = p.folder_id
               JOIN folders f ON f.id = p.folder_id AND f.status IN ('ok', 'partial')
               WHERE wf.workspace_id = ? AND p.folder_id = ? AND p.filename LIKE ?
               ORDER BY p.filename ASC, p.id ASC""",
            (ws, folder_id, f"{prefix}%"),
        ).fetchall()
        seq_rows = []
        for row in rows:
            key = _filename_sequence_key(row["filename"])
            row_ext = (row["extension"] or os.path.splitext(row["filename"])[1]).lower()
            if (
                key
                and key[0] == prefix
                and key[2] == width
                and row_ext == seed_ext
            ):
                seq_rows.append((key[1], row))
        seq_rows.sort(key=lambda item: (item[0], item[1]["filename"], item[1]["id"]))
        seed_idx = next(
            (i for i, (_, row) in enumerate(seq_rows) if row["id"] == seed_photo_id),
            None,
        )
        if seed_idx is not None:
            left = seed_idx
            while left > 0:
                prev_num, prev_row = seq_rows[left - 1]
                curr_num, curr_row = seq_rows[left]
                if curr_num - prev_num > max_sequence_gap or not _time_ok(prev_row, curr_row):
                    break
                left -= 1
            right = seed_idx
            while right < len(seq_rows) - 1:
                curr_num, curr_row = seq_rows[right]
                next_num, next_row = seq_rows[right + 1]
                if next_num - curr_num > max_sequence_gap or not _time_ok(curr_row, next_row):
                    break
                right += 1
            batch = [row for _, row in seq_rows[left:right + 1]]
            if len(batch) >= 2:
                return _limited(batch), "filename_sequence"

    rows = db.conn.execute(
        """SELECT p.id, p.folder_id, p.filename, p.extension, p.timestamp,
                  p.flag, p.rating, p.quality_score, p.sharpness
           FROM photos p
           JOIN workspace_folders wf ON wf.folder_id = p.folder_id
           JOIN folders f ON f.id = p.folder_id AND f.status IN ('ok', 'partial')
           WHERE wf.workspace_id = ? AND p.folder_id = ?
           ORDER BY p.timestamp IS NULL, p.timestamp ASC, p.filename ASC, p.id ASC""",
        (ws, folder_id),
    ).fetchall()
    seed_idx = next((i for i, row in enumerate(rows) if row["id"] == seed_photo_id), None)
    if seed_idx is None:
        return None, "Photo not found"
    left = seed_idx
    while (
        left > 0
        and _time_ok(rows[left - 1], rows[left], require_timestamps=True)
    ):
        left -= 1
    right = seed_idx
    while (
        right < len(rows) - 1
        and _time_ok(rows[right], rows[right + 1], require_timestamps=True)
    ):
        right += 1
    batch = rows[left:right + 1]
    if len(batch) < 2:
        return None, "No neighboring batch photos found"
    return _limited(batch), "capture_time"


def _quality_rank(photos, key, photo_id):
    values = [
        (p["id"], p.get(key))
        for p in photos
        if p.get(key) is not None
    ]
    if not values:
        return None
    values.sort(key=lambda item: item[1], reverse=True)
    for idx, (pid, _) in enumerate(values, start=1):
        if pid == photo_id:
            return idx
    return None


def _best_batch_reasons(photo, photos, is_best=False):
    reasons = []
    reject_reasons = photo.get("reject_reasons") or []
    if reject_reasons:
        return [str(r).replace("_", " ") for r in reject_reasons[:3]]

    q_rank = _quality_rank(photos, "quality_composite", photo["id"])
    focus_key = "eye_focus_score" if photo.get("eye_focus_score") is not None else "focus_score"
    focus_rank = _quality_rank(photos, focus_key, photo["id"])
    exposure_rank = _quality_rank(photos, "exposure_score", photo["id"])

    if q_rank == 1:
        reasons.append("highest overall quality")
    elif q_rank is not None and q_rank <= 3:
        reasons.append(f"quality rank #{q_rank}")
    if focus_rank == 1:
        reasons.append("sharpest eye" if focus_key == "eye_focus_score" else "sharpest subject")
    elif focus_rank is not None and focus_rank <= 3:
        reasons.append(f"focus rank #{focus_rank}")
    if exposure_rank == 1 and photo.get("exposure_score", 0) >= 0.7:
        reasons.append("cleanest exposure")
    if photo.get("crop_complete") is not None and photo.get("crop_complete") >= 0.9:
        reasons.append("full subject in frame")
    if is_best and not reasons:
        reasons.append("best available score in this batch")
    if not reasons:
        reasons.append("usable alternate")
    return reasons[:4]


def build_best_batch_response(db, seed_photo_id, rows):
    import config as cfg
    from pipeline import (
        load_photo_features,
        run_selected_batch_review,
        serialize_results,
    )

    effective_cfg = db.get_effective_config(cfg.load())
    photo_ids = [row["id"] for row in rows]
    loaded = load_photo_features(db, config=effective_cfg, photo_ids=photo_ids)
    by_id = {p["id"]: p for p in loaded}
    photos = [by_id[pid] for pid in photo_ids if pid in by_id]
    if len(photos) < 2:
        return None, "At least two batch photos with pipeline features are required"
    if not any(
        p.get("mask_path")
        or p.get("subject_tenengrad") is not None
        or p.get("eye_tenengrad") is not None
        for p in photos
    ):
        return None, "Run the pipeline on these photos before using Best Batch"

    results = serialize_results(run_selected_batch_review(photos, config=effective_cfg))
    result_photos = results.get("photos", [])
    if len(result_photos) < 2:
        return None, "Could not score this batch"

    ranked = sorted(
        result_photos,
        key=lambda p: (
            p.get("label") != "REJECT",
            p.get("quality_composite") if p.get("quality_composite") is not None else -1,
            p.get("focus_score") if p.get("focus_score") is not None else -1,
        ),
        reverse=True,
    )
    best = ranked[0]
    alternate_ids = [p["id"] for p in ranked[1:5] if p.get("label") != "REJECT"]
    reject_ids = [p["id"] for p in result_photos if p["id"] != best["id"]]
    sequence_keys = [_filename_sequence_key(p.get("filename")) for p in result_photos]
    sequence_nums = [key[1] for key in sequence_keys if key]

    cards = []
    for idx, photo in enumerate(ranked, start=1):
        if photo["id"] == best["id"]:
            role = "best"
        elif photo["id"] in alternate_ids:
            role = "alternate"
        else:
            role = "reject"
        cards.append({
            "id": photo["id"],
            "filename": photo.get("filename"),
            "width": photo.get("width"),
            "height": photo.get("height"),
            "rank": idx,
            "role": role,
            "label": photo.get("label"),
            "quality": photo.get("quality_composite"),
            "quality_pct": (
                round(photo["quality_composite"] * 100)
                if photo.get("quality_composite") is not None else None
            ),
            "focus": photo.get("eye_focus_score", photo.get("focus_score")),
            "focus_basis": "eye" if photo.get("eye_focus_score") is not None else "subject",
            "sharpness": photo.get("subject_tenengrad"),
            "eye_x": photo.get("eye_x"),
            "eye_y": photo.get("eye_y"),
            "eye_conf": photo.get("eye_conf"),
            "eye_tenengrad": photo.get("eye_tenengrad"),
            "exposure": photo.get("exposure_score"),
            "flag": photo.get("flag") or "none",
            "rating": photo.get("rating"),
            "reasons": _best_batch_reasons(photo, result_photos, is_best=photo["id"] == best["id"]),
        })

    best_reasons = _best_batch_reasons(best, result_photos, is_best=True)
    return {
        "seed_photo_id": seed_photo_id,
        "photo_ids": photo_ids,
        "count": len(photo_ids),
        "sequence_range": (
            [min(sequence_nums), max(sequence_nums)] if sequence_nums else None
        ),
        "best_photo_id": best["id"],
        "best_filename": best.get("filename"),
        "best_reasons": best_reasons,
        "summary": results.get("summary", {}),
        "cards": cards,
        "alternate_ids": alternate_ids,
        "suggested_reject_ids": reject_ids,
    }, None
