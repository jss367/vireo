"""Highlights and Life List payload builders.

Pure functions (a ``Database`` in, JSON-ready dicts out) behind the
Highlights page, the Life List page and the site export/publish, which
renders both payloads. The highlight-bucket helpers (scoring, curation
ordering, search and curation-state filters) are shared by both builders
and by the ``web.highlights`` routes; ``photo_highlight_entries`` backs the
photo-detail highlight badges in ``web.photos``.
"""

import math
import re

from db import text_search_match
from photo_payload import attach_edit_recipes


def _rank01(value, values):
    valid = [v for v in values if v is not None]
    if value is None:
        return 0.0
    if len(valid) <= 1:
        return 0.5 if valid else 0.0
    below = sum(1 for v in valid if v < value)
    equal = sum(1 for v in valid if v == value)
    return (below + 0.5 * equal) / len(valid)


def _highlight_exposure_score(photo):
    clip_high = photo.get("subject_clip_high") or 0.0
    clip_low = photo.get("subject_clip_low") or 0.0
    y_median = photo.get("subject_y_median")
    if y_median is None:
        return 0.5
    clip_penalty = math.exp(-6.0 * clip_high - 3.0 * clip_low)
    lum_penalty = math.exp(-abs((y_median / 255.0) - 0.45) / 0.30)
    return max(0.0, min(1.0, clip_penalty * lum_penalty))


def _highlight_area_score(subject_size):
    if subject_size is None or subject_size <= 0:
        return 0.0
    return min(1.0, math.sqrt(subject_size) * 2.0)


def _highlight_score_bucket(photos, picked_first=False):
    """Attach highlight scores and compact reason labels to one species bucket.

    picked_first promotes flagged photos above unflagged ones regardless of
    score — desired on the Highlights page. It is off by default so callers
    such as Life List can calculate the ranked baseline before applying their
    own Representative / Pick curation tiers.
    """
    subject_values = [p.get("subject_tenengrad") for p in photos]
    eye_values = [p.get("eye_tenengrad") for p in photos if p.get("eye_tenengrad") is not None]
    bg_values = [p.get("bg_tenengrad") for p in photos]
    bg_sep_values = [p.get("bg_separation") for p in photos if p.get("bg_separation") is not None]
    noise_values = [p.get("noise_estimate") for p in photos if p.get("noise_estimate") is not None]
    max_bg_sep = max(bg_sep_values) if bg_sep_values else None

    for p in photos:
        if p.get("eye_tenengrad") is not None and eye_values:
            focus = _rank01(p.get("eye_tenengrad"), eye_values)
            focus_label = "eye focus"
        else:
            subject_t = p.get("subject_tenengrad")
            rank = _rank01(subject_t, subject_values)
            if subject_t is not None and p.get("bg_tenengrad") is not None:
                ratio = math.log((subject_t + 1e-8) / (p.get("bg_tenengrad") + 1e-8))
                bg_term = 1.0 / (1.0 + math.exp(-ratio))
                focus = 0.70 * rank + 0.30 * bg_term
            elif subject_t is not None:
                focus = rank
            else:
                focus = p.get("quality_score") if p.get("quality_score") is not None else 0.0
            focus_label = "focus"

        exposure = _highlight_exposure_score(p)
        crop = p.get("crop_complete")
        crop_score = crop if crop is not None else 0.5
        if max_bg_sep and p.get("bg_separation") is not None:
            bg_sep = 1.0 - min(1.0, p.get("bg_separation") / max_bg_sep)
        else:
            bg_sep = 0.5
        composition = 0.55 * crop_score + 0.45 * bg_sep
        area = _highlight_area_score(p.get("subject_size"))
        if p.get("noise_estimate") is not None and noise_values:
            noise = 1.0 - _rank01(p.get("noise_estimate"), noise_values)
        else:
            noise = 1.0 - _rank01(p.get("bg_tenengrad"), bg_values) if bg_values else 0.5

        rich_available = any(
            p.get(k) is not None
            for k in (
                "subject_tenengrad", "eye_tenengrad", "crop_complete",
                "subject_clip_high", "subject_y_median", "noise_estimate",
            )
        )
        if rich_available:
            score = (
                0.42 * focus
                + 0.18 * exposure
                + 0.16 * composition
                + 0.10 * area
                + 0.08 * noise
                + 0.06 * (p.get("quality_score") or 0.0)
            )
        else:
            score = p.get("quality_score") if p.get("quality_score") is not None else 0.0

        rating = p.get("rating") or 0
        if rating >= 4:
            score += 0.04 + 0.02 * (rating - 4)
        elif rating == 3:
            score += 0.015
        # Baseline BEFORE the pick bonus, so the client can recompute
        # highlight_score on a lightbox pick/unpick via clamp(base + bonus)
        # instead of subtracting the bonus from the already-clamped value —
        # which loses precision when the raw score exceeded 1.0 pre-clamp
        # (e.g. base 0.97 → cached 1.0 → subtract 0.08 → 0.92, but the
        # correct unpicked value is 0.97).
        base_score_pre_pick = score
        if p.get("flag") == "flagged":
            score += 0.08

        reasons = []
        if p.get("flag") == "flagged":
            reasons.append("picked")
        if rating:
            reasons.append(f"{rating} star")
        if focus >= 0.72:
            reasons.append(f"strong {focus_label}")
        elif focus < 0.35 and rich_available:
            reasons.append("soft subject")
        if exposure >= 0.70:
            reasons.append("good exposure")
        elif p.get("subject_clip_high") is not None and p.get("subject_clip_high") > 0.30:
            reasons.append("highlight clipping")
        if crop is not None:
            if crop >= 0.90:
                reasons.append("clean crop")
            elif crop < 0.60:
                reasons.append("clipped subject")
        if area >= 0.50:
            reasons.append("large subject")
        if not reasons:
            reasons.append("legacy quality")

        p["highlight_score"] = round(max(0.0, min(1.0, score)), 4)
        p["highlight_base_score"] = round(
            max(0.0, min(1.0, base_score_pre_pick)), 4
        )
        p["score_parts"] = {
            "focus": round(focus, 3),
            "exposure": round(exposure, 3),
            "composition": round(composition, 3),
            "subject": round(area, 3),
            "noise": round(noise, 3),
        }
        p["reasons"] = reasons[:3]

    if picked_first:
        # Highlights page ordering: three contiguous regions —
        #   1. picks (flagged): analyzed first (by score desc), then
        #      unanalyzed picks in capture order;
        #   2. analyzed non-picks: by highlight_score desc;
        #   3. unanalyzed non-picks: capture order (earliest first).
        # The analyzed-before-unanalyzed tier separates the groups so the
        # score term only reorders analyzed rows and the timestamp term only
        # reorders unanalyzed rows — they never compete. Timestamps are ISO
        # text, so lexicographic order is chronological; missing timestamps
        # sort to the end of their group. This is an ascending sort (no
        # reverse), negating the score to rank analyzed rows high-first.
        def _bucket_sort_key(p):
            analyzed = p.get("quality_score") is not None
            ts = p.get("timestamp")
            return (
                0 if p.get("flag") == "flagged" else 1,
                0 if analyzed else 1,
                -(p.get("highlight_score") or 0) if analyzed else 0.0,
                (ts is None, ts),
                p.get("id", 0),
            )

        photos.sort(key=_bucket_sort_key)
    else:
        photos.sort(
            key=lambda p: (
                p.get("highlight_score") or 0,
                p.get("predicted_confidence") or 0,
                p.get("quality_score") or 0,
                p.get("rating") or 0,
                -p.get("id", 0),
            ),
            reverse=True,
        )


def _sort_photos_with_representatives_first(photos, representative_order):
    """Promote representative photos while preserving ranked order otherwise."""
    ranked_position = {photo["id"]: idx for idx, photo in enumerate(photos)}
    photos.sort(key=lambda photo: (
        0 if photo["id"] in representative_order else 1,
        representative_order.get(photo["id"], 0),
        ranked_position.get(photo["id"], 0),
    ))


def bucket_best_score(photos):
    """Return the highest highlight_score in a bucket, or None if empty.

    Used to rank buckets on the Highlights page. Reads the max across all
    photos so a pick-first (or user-preferred) reorder at photos[0] can't
    demote a species by anchoring the bucket score to a lower-scored photo.
    """
    scores = [p.get("highlight_score") for p in photos if p.get("highlight_score") is not None]
    return max(scores) if scores else None


def _bucket_unanalyzed_count(photos):
    """Length of the trailing "Not yet analyzed" tail of a bucket.

    Counts only the contiguous run of unscored non-pick photos at the END of
    ``photos``. This is what the divider labels, so callers can trust that a
    non-zero value marks a real tail below the divider.

    Only the trailing run is counted (not every unscored non-pick in the
    bucket) so that curated ordering — where
    :func:`apply_ordered_highlights` or :func:`apply_highlight_preferences`
    can promote an unscored, non-flagged photo to the front as a species
    highlight or representative — doesn't inflate the count with photos that
    now live above the divider. Unscored picks are excluded because they
    stay grouped with the other picks at the front regardless.
    """
    if not photos:
        return 0
    tail = 0
    for p in reversed(photos):
        if p.get("quality_score") is None and p.get("flag") != "flagged":
            tail += 1
        else:
            break
    return tail


def apply_highlight_preferences(db, buckets):
    preferences = db.get_species_representative_lists(eligible_only=True)
    for bucket in buckets:
        representative_ids = preferences.get(bucket["species"]) or []
        representative_set = set(representative_ids)
        representative_order = {
            photo_id: idx for idx, photo_id in enumerate(representative_ids)
        }
        preferred_id = representative_ids[0] if representative_ids else None
        applied = False
        for photo in bucket.get("photos") or []:
            is_rep = photo.get("id") in representative_set
            photo["is_species_representative"] = is_rep
            applied = applied or is_rep
        # Promote representatives to the front so cross-workspace reps
        # (visible via species_representatives but without a workspace-scoped
        # species_highlights rank) become the bucket's primary photo.
        # apply_ordered_highlights runs before this and only sorts when a
        # visible photo has a workspace highlight rank, so without this pass
        # a rep chosen in another workspace stays buried in score order.
        # Matches the Life List behavior which also promotes reps over
        # workspace highlights.
        if applied:
            _sort_photos_with_representatives_first(
                bucket["photos"], representative_order
            )
        top = bucket["photos"][0] if bucket["photos"] else {}
        bucket["preferred_photo_id"] = preferred_id
        # Species-level state stays true even when the representative photo is
        # outside the current folder or search result.
        bucket["has_species_representative"] = preferred_id is not None
        bucket["has_preferred_photo"] = applied
        bucket["best_quality"] = top.get("quality_score")
        # Rank by the highest-scored photo in the bucket, not photos[0].
        # A picked (or manually preferred) photo may sit at photos[0] with a
        # lower highlight_score; using its score for bucket ranking would
        # demote the whole species below buckets with worse actual best
        # photos.
        bucket["best_score"] = bucket_best_score(bucket["photos"])
        bucket["best_timestamp"] = top.get("timestamp")
        # Run last so curated promotion (an unscored rep pushed to the front)
        # doesn't leave a stale tail count that anchors the divider above
        # analyzed content.
        bucket["unanalyzed_count"] = _bucket_unanalyzed_count(bucket.get("photos"))


def apply_ordered_highlights(db, buckets):
    highlights = db.get_species_highlights(eligible_only=True)
    for bucket in buckets:
        ranks = highlights.get(bucket["species"], {})
        # Species-level state stays true even when every selected highlight is
        # outside the current folder or search result.
        bucket["has_highlight_selection"] = bool(ranks)
        photos = bucket.get("photos") or []
        matched = False
        for p in photos:
            rank = ranks.get(p.get("id"))
            p["is_highlighted"] = rank is not None
            p["highlight_rank"] = rank
            if rank is not None:
                matched = True
        # Only re-sort when at least one visible photo actually maps to a
        # stored highlight rank. `ranks` is workspace-scoped, so a folder
        # or search view of the same species can be truthy here while none
        # of its photos are highlights — re-sorting then discards the
        # picked-first order that _highlight_score_bucket already applied.
        if matched:
            bucket["photos"].sort(
                key=lambda p: (
                    0 if p.get("is_highlighted") else 1,
                    p.get("highlight_rank") if p.get("highlight_rank") is not None else 10**9,
                    -(p.get("highlight_score") or 0),
                    -(p.get("predicted_confidence") or 0),
                    -(p.get("quality_score") or 0),
                    -(p.get("rating") or 0),
                    p.get("id", 0),
                )
            )
        best = bucket["photos"][0] if bucket.get("photos") else {}
        bucket["highlight_count"] = sum(
            1 for p in bucket.get("photos") or [] if p.get("is_highlighted")
        )
        # unanalyzed_count is assigned in apply_highlight_preferences instead:
        # that runs after this pass and can still reorder photos, so anchoring
        # the tail count here would be stale under curated ordering.
        bucket["best_quality"] = best.get("quality_score")
        bucket["best_score"] = best.get("highlight_score")
        bucket["best_timestamp"] = best.get("timestamp")


def photo_highlight_entries(db, photo_id):
    """Return highlight-eligible species entries for one visible photo.

    Restricts both the candidate scan and the species-highlights lookup to
    the requested photo so the photo-detail endpoint doesn't rebuild every
    workspace bucket for each call (browse detail, lightbox, batch actions).
    """
    candidates = db.get_highlights_candidates(
        None, min_quality=0.0, photo_id=photo_id
    )
    buckets, _unidentified = collect_highlight_buckets(
        candidates, confidence_threshold=0.0,
        canonicalize_species=species_canonicalizer(db),
    )
    entries = []
    for bucket in buckets:
        species = bucket.get("species")
        if not species:
            continue
        ranks = db.get_species_highlights(species).get(species, {})
        for photo in bucket.get("photos") or []:
            if photo.get("id") != photo_id:
                continue
            rank = ranks.get(photo_id)
            entries.append({
                "species": species,
                "is_highlighted": rank is not None,
                "highlight_rank": rank,
                "is_confirmed": bool(photo.get("has_accepted_species")),
            })
            break
    return entries


def _highlight_confidence_label(confidence, is_accepted):
    if is_accepted:
        return "confirmed"
    if confidence is None:
        return "unknown"
    if confidence >= 0.85:
        return "likely"
    return "candidate"


def normalize_highlight_confirmation_filter(value):
    value = (value or "all").strip().lower()
    if value not in {"all", "confirmed", "unconfirmed"}:
        return "all"
    return value


def _normalize_highlight_presence_filter(value):
    value = (value or "all").strip().lower()
    if value not in {"all", "yes", "no"}:
        return "all"
    return value


def species_canonicalizer(db):
    """Memoized wrapper around db.resolve_species_display_name.

    Bucket collection resolves the same handful of species strings for
    thousands of candidate rows; caching avoids a keyword lookup per row.
    """
    cache = {}

    def canonicalize(name):
        if name not in cache:
            cache[name] = db.resolve_species_display_name(name)
        return cache[name]

    return canonicalize


def collect_highlight_buckets(
    candidates,
    confidence_threshold,
    confirmation_filter="all",
    canonicalize_species=None,
):
    confirmation_filter = normalize_highlight_confirmation_filter(
        confirmation_filter
    )
    bucket_map = {}
    unidentified_photos = []

    for row in candidates:
        r = dict(row)
        accepted = r.get("species")
        if confirmation_filter == "confirmed" and accepted is None:
            continue
        if confirmation_filter == "unconfirmed" and accepted is not None:
            continue
        predicted_conf = r.get("predicted_confidence")
        if accepted:
            species = accepted
            if canonicalize_species is not None:
                species = canonicalize_species(species) or species
            is_accepted = True
        elif (
            r.get("predicted_species")
            and predicted_conf is not None
            and predicted_conf >= confidence_threshold
        ):
            # Prediction labels are external vocabulary (classifier label
            # files) with their own casing. Bucket by the spelling
            # add_keyword would store so a predicted `Common Waxbill` and
            # photos already accepted as `Common waxbill` land in ONE
            # bucket, and so curation written from this bucket's label keys
            # on the string the keyword row and eligibility queries use.
            species = r["predicted_species"]
            if canonicalize_species is not None:
                species = canonicalize_species(species) or species
            is_accepted = False
        else:
            species = None
            is_accepted = False

        photo = {
            "id": r["id"],
            "filename": r["filename"],
            "timestamp": r.get("timestamp"),
            "folder_name": r.get("folder_name"),
            "folder_path": r.get("folder_path"),
            "keyword_names": r.get("keyword_names") or "",
            "rating": r.get("rating") or 0,
            "flag": r.get("flag") or "none",
            "quality_score": r.get("quality_score"),
            "is_analyzed": r.get("quality_score") is not None,
            "subject_sharpness": r.get("subject_sharpness"),
            "subject_size": r.get("subject_size"),
            "sharpness": r.get("sharpness"),
            "mask_path": r.get("mask_path"),
            "subject_tenengrad": r.get("subject_tenengrad"),
            "bg_tenengrad": r.get("bg_tenengrad"),
            "crop_complete": r.get("crop_complete"),
            "bg_separation": r.get("bg_separation"),
            "subject_clip_high": r.get("subject_clip_high"),
            "subject_clip_low": r.get("subject_clip_low"),
            "subject_y_median": r.get("subject_y_median"),
            "noise_estimate": r.get("noise_estimate"),
            "eye_tenengrad": r.get("eye_tenengrad"),
            "species": accepted,
            "prediction_id": r.get("prediction_id"),
            "predicted_species": r.get("predicted_species"),
            "predicted_confidence": predicted_conf,
            "has_accepted_species": accepted is not None,
            "is_unidentified": species is None,
            "is_confirmable_prediction": (
                accepted is None
                and species is not None
                and r.get("prediction_id") is not None
            ),
        }

        if species is None:
            unidentified_photos.append(photo)
        else:
            entry = bucket_map.setdefault(
                species, {"is_accepted": True, "photos": []}
            )
            entry["is_accepted"] = entry["is_accepted"] and is_accepted
            entry["photos"].append(photo)

    buckets = []
    for species, entry in bucket_map.items():
        photos = entry["photos"]
        _highlight_score_bucket(photos, picked_first=True)
        confidences = [
            p["predicted_confidence"]
            for p in photos
            if p.get("predicted_confidence") is not None
        ]
        avg_confidence = (
            round(sum(confidences) / len(confidences), 4)
            if confidences else None
        )
        top = photos[0] if photos else {}
        buckets.append({
            "species": species,
            "is_accepted": entry["is_accepted"],
            "certainty": _highlight_confidence_label(
                avg_confidence, entry["is_accepted"]
            ),
            "avg_confidence": avg_confidence,
            "photo_count": len(photos),
            "best_quality": top.get("quality_score"),
            "best_score": bucket_best_score(photos),
            "best_timestamp": top.get("timestamp"),
            "photos": photos,
        })

    _highlight_score_bucket(unidentified_photos, picked_first=True)
    buckets.sort(
        key=lambda b: (
            b.get("best_score") or 0,
            b.get("avg_confidence") or 0,
            b.get("photo_count") or 0,
        ),
        reverse=True,
    )
    return buckets, unidentified_photos


def _highlight_search_fields(photo):
    return [
        photo.get("filename") or "",
        photo.get("folder_name") or "",
        photo.get("folder_path") or "",
        photo.get("keyword_names") or "",
        photo.get("species") or "",
        photo.get("predicted_species") or "",
        "unidentified" if photo.get("is_unidentified") else "",
    ]


def _highlight_photo_matches_query(
    photo,
    query,
    match_case=False,
    whole_word=False,
):
    tokens = str(query or "").split()
    if not tokens:
        return True
    fields = _highlight_search_fields(photo)
    return all(
        any(
            text_search_match(field, token, match_case, whole_word)
            for field in fields
        )
        for token in tokens
    )


def _refresh_highlight_bucket_metadata(bucket):
    photos = bucket.get("photos") or []
    confidences = [
        p.get("predicted_confidence")
        for p in photos
        if p.get("predicted_confidence") is not None
    ]
    avg_confidence = (
        round(sum(confidences) / len(confidences), 4)
        if confidences else None
    )
    top = photos[0] if photos else {}
    # Filtering can drop the unconfirmed photos from a mixed bucket, leaving
    # only confirmed ones — recompute so the "candidate" badge and the
    # `confirmed` sort match what the row now actually contains.
    bucket["is_accepted"] = bool(photos) and all(
        p.get("has_accepted_species") for p in photos
    )
    bucket["avg_confidence"] = avg_confidence
    bucket["certainty"] = _highlight_confidence_label(
        avg_confidence, bucket.get("is_accepted")
    )
    bucket["photo_count"] = len(photos)
    bucket["best_quality"] = top.get("quality_score")
    bucket["best_score"] = bucket_best_score(photos)
    bucket["best_timestamp"] = top.get("timestamp")
    return bucket


def filter_highlight_sections(
    buckets,
    unidentified_photos,
    query,
    match_case=False,
    whole_word=False,
):
    query = (query or "").strip()
    if not query:
        for bucket in buckets:
            _refresh_highlight_bucket_metadata(bucket)
        return buckets, unidentified_photos

    filtered_buckets = []
    for bucket in buckets:
        photos = [
            p for p in (bucket.get("photos") or [])
            if _highlight_photo_matches_query(p, query, match_case, whole_word)
        ]
        if photos:
            updated = {**bucket, "photos": photos}
            filtered_buckets.append(_refresh_highlight_bucket_metadata(updated))

    unidentified = [
        p for p in unidentified_photos
        if _highlight_photo_matches_query(p, query, match_case, whole_word)
    ]
    return filtered_buckets, unidentified


def filter_highlight_curation_state(
    buckets,
    unidentified_photos,
    highlight_filter="all",
    representative_filter="all",
):
    highlight_filter = _normalize_highlight_presence_filter(highlight_filter)
    representative_filter = _normalize_highlight_presence_filter(
        representative_filter
    )

    def matches(bucket, filter_value, field):
        if filter_value == "all":
            return True
        return bool(bucket.get(field)) == (filter_value == "yes")

    filtered = [
        bucket for bucket in buckets
        if matches(bucket, highlight_filter, "has_highlight_selection")
        and matches(
            bucket,
            representative_filter,
            "has_species_representative",
        )
    ]
    # These two states only apply to named species. Unidentified photos have
    # neither a species highlight list nor a species representative assignment.
    if highlight_filter != "all" or representative_filter != "all":
        unidentified_photos = []
    return filtered, unidentified_photos


_LIFE_LIST_LEADING_APOSTROPHE_RE = re.compile(
    r"^['`´ʹʻʼ‘’‛]+"
)


def _life_list_alphabetical_key(name):
    """Sort key that ignores a leading Hawaiian ʻokina or apostrophe variant.

    Kept in sync with ``lifeListAlphabeticalName`` in ``life_list.html`` so
    server-side lifer numbering ties break under the same letter the client
    displays the species under.
    """
    return _LIFE_LIST_LEADING_APOSTROPHE_RE.sub("", str(name or "")).lower()


def build_highlights_payload(
    db,
    scope="folder",
    folder_id=None,
    min_quality=0.0,
    confidence_threshold=0.70,
    limit_per_bucket=20,
    species_filter="",
    species_match_case=False,
    species_whole_word=False,
    search_query="",
    search_match_case=False,
    search_whole_word=False,
    confirmation_filter="all",
    highlight_filter="all",
    representative_filter="all",
):
    folders = db.get_folders_with_quality_data()
    if scope == "workspace":
        folder_id = None
    elif folder_id is None and folders:
        folder_id = folders[0]["id"]  # Most recent
    limit_per_bucket = max(1, min(int(limit_per_bucket), 100))
    species_filter = (species_filter or "").strip()
    confirmation_filter = normalize_highlight_confirmation_filter(
        confirmation_filter
    )
    highlight_filter = _normalize_highlight_presence_filter(highlight_filter)
    representative_filter = _normalize_highlight_presence_filter(
        representative_filter
    )

    candidates = db.get_highlights_candidates(folder_id, min_quality=min_quality)
    total_in_scope = db.count_filtered_photos(folder_id=folder_id)

    buckets, unidentified_photos = collect_highlight_buckets(
        candidates, confidence_threshold, confirmation_filter,
        canonicalize_species=species_canonicalizer(db),
    )
    eligible_count = sum(b["photo_count"] for b in buckets) + len(
        unidentified_photos
    )
    if species_filter:
        buckets = [
            b for b in buckets
            if text_search_match(
                b["species"],
                species_filter,
                species_match_case,
                species_whole_word,
            )
        ]
        if not text_search_match(
            "unidentified",
            species_filter,
            species_match_case,
            species_whole_word,
        ):
            unidentified_photos = []

    buckets, unidentified_photos = filter_highlight_sections(
        buckets,
        unidentified_photos,
        search_query,
        search_match_case,
        search_whole_word,
    )
    apply_ordered_highlights(db, buckets)
    apply_highlight_preferences(db, buckets)
    buckets, unidentified_photos = filter_highlight_curation_state(
        buckets,
        unidentified_photos,
        highlight_filter,
        representative_filter,
    )

    def limited_bucket(bucket):
        photos = bucket["photos"]
        limited = photos[:limit_per_bucket]
        return {
            **bucket,
            "photos": limited,
            "loaded_count": len(limited),
            "has_more": len(photos) > len(limited),
        }

    limited_buckets = [limited_bucket(b) for b in buckets]
    unidentified_limited = unidentified_photos[:limit_per_bucket]
    visible_photos = []
    for bucket in limited_buckets:
        visible_photos.extend(bucket["photos"])
    visible_photos.extend(unidentified_limited)
    attach_edit_recipes(db, visible_photos)

    return {
        "buckets": limited_buckets,
        "unidentified": {
            "photo_count": len(unidentified_photos),
            "photos": unidentified_limited,
            "loaded_count": len(unidentified_limited),
            "has_more": len(unidentified_photos) > len(unidentified_limited),
            "unanalyzed_count": _bucket_unanalyzed_count(unidentified_photos),
        },
        "folders": [
            {"id": f["id"], "name": f["name"], "photo_count": f["photo_count"]}
            for f in folders
        ],
        "meta": {
            "total_in_scope": total_in_scope,
            "eligible": eligible_count,
            "limit_per_bucket": limit_per_bucket,
            "confirmation": confirmation_filter,
            "highlight_selection": highlight_filter,
            "species_representative": representative_filter,
            "search": (search_query or "").strip(),
        },
        "scope": "workspace" if folder_id is None else "folder",
    }


def build_life_list_payload(
    db,
    photos_per_species=12,
    photo_offset=0,
    species_filter=None,
):
    rows = db.get_life_list_candidates(species=species_filter)
    locations_by_species = db.get_life_list_locations(species=species_filter)
    class_by_taxon = db.get_class_ancestors_for_taxa({
        row["taxon_id"] for row in rows if row["taxon_id"] is not None
    })
    photo_offset = max(0, int(photo_offset))
    if photos_per_species is None:
        photo_limit = None
    else:
        photo_limit = max(
            1, min(int(photos_per_species), 100)
        )

    # Canonicalize each row's species to the root keyword's stored
    # spelling. After repair_duplicate_photo_species detaches a
    # redundant root row, a photo may only carry the hierarchy leaf
    # (``verdin``) whose spelling differs from the curation-keyed
    # root (``Verdin``); without canonicalization, that photo would
    # sit in its own ``verdin`` bucket, its curation on
    # ``Verdin`` would never apply, and single-species API loads
    # for ``Verdin`` would return an entry named ``verdin``.
    canonicalize = species_canonicalizer(db)
    merged_locations = {}
    for raw_species, locations in locations_by_species.items():
        canonical = canonicalize(raw_species) or raw_species
        existing = merged_locations.setdefault(canonical, [])
        for loc in locations:
            if loc not in existing:
                existing.append(loc)
    locations_by_species = merged_locations

    buckets = {}
    for row in rows:
        r = dict(row)
        r["species"] = canonicalize(r["species"]) or r["species"]
        entry = buckets.setdefault(r["species"], {
            "scientific_name": None,
            "common_name": None,
            "taxon_id": None,
            "taxon_rank": None,
            "taxonomic_class": None,
            "photos": [],
            "seen_ids": set(),
        })
        # Two keyword rows can share a name (different parents); take
        # the first linked taxon's names for the species entry.
        if entry["scientific_name"] is None:
            entry["scientific_name"] = r.get("scientific_name")
        if entry["common_name"] is None:
            entry["common_name"] = r.get("common_name")
        if entry["taxon_id"] is None and r.get("taxon_id") is not None:
            entry["taxon_id"] = r["taxon_id"]
            entry["taxon_rank"] = r.get("taxon_rank")
            entry["taxonomic_class"] = class_by_taxon.get(r["taxon_id"])
        # A photo tagged with two same-name species keywords would
        # otherwise be appended once per row, inflating photo_count
        # and duplicating cards in the lightbox.
        if r["id"] in entry["seen_ids"]:
            continue
        entry["seen_ids"].add(r["id"])
        entry["photos"].append({
            "id": r["id"],
            "filename": r["filename"],
            "timestamp": r.get("timestamp"),
            "rating": r.get("rating") or 0,
            "flag": r.get("flag") or "none",
            "quality_score": r.get("quality_score"),
            "subject_sharpness": r.get("subject_sharpness"),
            "subject_size": r.get("subject_size"),
            "sharpness": r.get("sharpness"),
            "subject_tenengrad": r.get("subject_tenengrad"),
            "bg_tenengrad": r.get("bg_tenengrad"),
            "crop_complete": r.get("crop_complete"),
            "bg_separation": r.get("bg_separation"),
            "subject_clip_high": r.get("subject_clip_high"),
            "subject_clip_low": r.get("subject_clip_low"),
            "subject_y_median": r.get("subject_y_median"),
            "noise_estimate": r.get("noise_estimate"),
            "eye_tenengrad": r.get("eye_tenengrad"),
        })

    def compact(photo):
        return {
            "id": photo["id"],
            "filename": photo["filename"],
            "timestamp": photo.get("timestamp"),
            "flag": photo.get("flag") or "none",
            "quality_score": photo.get("quality_score"),
            "highlight_score": photo.get("highlight_score"),
            "reasons": photo.get("reasons") or [],
            "is_species_representative": bool(
                photo.get("is_species_representative")
            ),
            "is_life_list_photo": bool(photo.get("is_species_representative")),
            "is_highlighted": bool(photo.get("is_highlighted")),
            "highlight_rank": photo.get("highlight_rank"),
        }

    species_entries = []
    distinct_photo_ids = set()
    representatives = db.get_species_representative_lists(species=species_filter)
    highlights_by_species = db.get_species_highlights(species=species_filter)
    for species, entry in buckets.items():
        photos = entry["photos"]
        distinct_photo_ids.update(p["id"] for p in photos)
        timestamps = [p["timestamp"] for p in photos if p.get("timestamp")]
        _highlight_score_bucket(photos)
        representative_ids = representatives.get(species) or []
        representative_order = {
            photo_id: idx for idx, photo_id in enumerate(representative_ids)
        }
        preferred_id = representative_ids[0] if representative_ids else None
        highlight_ranks = highlights_by_species.get(species, {})
        for photo in photos:
            rank = highlight_ranks.get(photo["id"])
            photo["is_highlighted"] = rank is not None
            photo["highlight_rank"] = rank
            photo["is_species_representative"] = (
                photo["id"] in representative_order
            )
        preferred_applied = any(
            photo.get("is_species_representative") for photo in photos
        )
        if not preferred_applied and highlight_ranks:
            valid_highlights = [
                (highlight_ranks[p["id"]], p["id"])
                for p in photos
                if p["id"] in highlight_ranks
            ]
            if valid_highlights:
                _, highlight_id = min(valid_highlights)
                for idx, photo in enumerate(photos):
                    if photo["id"] == highlight_id:
                        if idx:
                            photos.insert(0, photos.pop(idx))
                        break
        # Life List curation order is explicit representative selection,
        # then Picks (the P flag), then the existing highlight/algorithmic
        # ranking. Keep ranked positions as the final tie-breaker so
        # multiple Picks retain their quality order and non-Picks behave
        # exactly as before.
        ranked_position = {
            photo["id"]: idx for idx, photo in enumerate(photos)
        }
        photos.sort(key=lambda photo: (
            0 if photo["id"] in representative_order else (
                1 if photo.get("flag") == "flagged" else 2
            ),
            representative_order.get(photo["id"], 0),
            ranked_position.get(photo["id"], 0),
        ))
        best_photo = photos[0] if photos else {}
        if best_photo.get("is_species_representative"):
            best_source = "representative"
        elif best_photo.get("flag") == "flagged":
            best_source = "pick"
        elif best_photo.get("is_highlighted"):
            best_source = "highlight"
        else:
            best_source = "algorithm"
        if photo_limit is None:
            top = photos[photo_offset:]
        else:
            top = photos[photo_offset:photo_offset + photo_limit]
        species_entries.append({
            "species": species,
            "scientific_name": entry["scientific_name"],
            "common_name": entry["common_name"],
            "taxon_id": entry["taxon_id"],
            "taxon_rank": entry["taxon_rank"],
            "taxonomic_class": entry["taxonomic_class"],
            "photo_count": len(photos),
            "first_seen": min(timestamps) if timestamps else None,
            "last_seen": max(timestamps) if timestamps else None,
            "locations": locations_by_species.get(species, []),
            "preferred_photo_id": preferred_id,
            "has_preferred_photo": preferred_applied,
            "best_source": best_source,
            "best": compact(photos[0]) if photos else None,
            "photos": [compact(p) for p in top],
            "loaded_count": min(len(photos), photo_offset + len(top)),
            "has_more": photo_offset + len(top) < len(photos),
        })

    # Life-list numbering: chronological by first photographed date,
    # the way birders count lifers. Species with no capture time go
    # last, alphabetically, so they still get a stable number.
    species_entries.sort(key=lambda e: (
        e["first_seen"] is None,
        e["first_seen"] or "",
        _life_list_alphabetical_key(e["species"]),
        e["species"].lower(),
    ))
    for i, e in enumerate(species_entries, start=1):
        e["number"] = i
    life_list_photos = []
    for entry in species_entries:
        if entry.get("best"):
            life_list_photos.append(entry["best"])
        life_list_photos.extend(entry.get("photos") or [])
    attach_edit_recipes(db, life_list_photos)

    return {
        "species": species_entries,
        "meta": {
            "species_count": len(species_entries),
            "photo_count": len(distinct_photo_ids),
            "photos_per_species": photo_limit,
        },
    }
