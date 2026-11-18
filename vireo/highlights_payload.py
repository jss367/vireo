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
from bisect import bisect_left, bisect_right
from dataclasses import dataclass, field

from db import text_search_match
from photo_payload import attach_edit_recipes
from species_memberships import (
    HighlightMembership,
    candidate_subjects,
    photo_species_memberships,
    subject_is_background_like,
    subject_is_whole_frame,
)
from species_preference import Preferences, choice_note
from species_presence import IN_FRAME, SUBJECT, Presence, species_presence


class _Ranks:
    """One metric's values across a bucket, for ranking each photo in it.

    Sorted once, so ranking every photo of a bucket costs ``n log n``; a
    species bucket at workspace scope can hold thousands of photos.
    """

    def __init__(self, values):
        self._sorted = sorted(v for v in values if v is not None)

    def __bool__(self):
        return bool(self._sorted)

    def rank01(self, value):
        """The share of the bucket's values below ``value`` (ties count
        half): 0.0 for a missing value, 0.5 when the bucket has one."""
        valid = self._sorted
        if value is None:
            return 0.0
        if len(valid) <= 1:
            return 0.5 if valid else 0.0
        below = bisect_left(valid, value)
        equal = bisect_right(valid, value) - below
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


def _species_is_subject(photo):
    """False for an entry in the bucket of a species the photo is not a
    picture of (``species_presence``): another bird is its subject, or the
    crop leaves this species out. Entries built without a presence (the Life
    List's, the unidentified section's) are their photo's subject."""
    return photo.get("species_presence", SUBJECT) == SUBJECT


def _pick_counts(photo):
    """Whether the photo's pick flag is a pick of this entry's species. The
    flag belongs to the photo, so it speaks for the bird the photo is of and
    not for every species in the frame."""
    return photo.get("flag") == "flagged" and _species_is_subject(photo)


def _highlight_score_bucket(photos, picked_first=False):
    """Attach highlight scores and compact reason labels to one species bucket.

    picked_first promotes flagged photos above unflagged ones regardless of
    score — desired on the Highlights page. It is off by default so callers
    such as Life List can calculate the ranked baseline before applying their
    own Representative / Pick curation tiers.
    """
    # Ranked against the photos this species is the subject of: the others'
    # measurements describe a different bird.
    ranked = [p for p in photos if _species_is_subject(p)]
    subject_values = _Ranks(p.get("subject_tenengrad") for p in ranked)
    eye_values = _Ranks(p.get("eye_tenengrad") for p in ranked)
    bg_values = _Ranks(p.get("bg_tenengrad") for p in ranked)
    bg_sep_values = [p.get("bg_separation") for p in ranked if p.get("bg_separation") is not None]
    noise_values = _Ranks(p.get("noise_estimate") for p in ranked)
    max_bg_sep = max(bg_sep_values) if bg_sep_values else None

    for p in photos:
        focus, focus_label = _highlight_focus(p, eye_values, subject_values)

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
            noise = 1.0 - noise_values.rank01(p.get("noise_estimate"))
        elif p.get("bg_tenengrad") is not None and bg_values:
            noise = 1.0 - bg_values.rank01(p.get("bg_tenengrad"))
        else:
            noise = 0.5

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
        if _pick_counts(p):
            score += 0.08

        reasons = _highlight_reasons(
            p, rating, focus, focus_label, rich_available, exposure, crop, area,
        )

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

    _sort_highlight_bucket(photos, picked_first)


def _highlight_focus(p, eye_values, subject_values):
    """``(focus score, reason label)`` for one photo in a bucket."""
    if p.get("eye_tenengrad") is not None and eye_values:
        focus = eye_values.rank01(p.get("eye_tenengrad"))
        focus_label = "eye focus"
    else:
        subject_t = p.get("subject_tenengrad")
        rank = subject_values.rank01(subject_t)
        if subject_t is not None and p.get("bg_tenengrad") is not None:
            ratio = math.log((subject_t + 1e-8) / (p.get("bg_tenengrad") + 1e-8))
            bg_term = 1.0 / (1.0 + math.exp(-ratio))
            focus = 0.70 * rank + 0.30 * bg_term
        elif subject_t is not None:
            focus = rank
        else:
            focus = p.get("quality_score") if p.get("quality_score") is not None else 0.0
        focus_label = "focus"

    return focus, focus_label


def _highlight_reasons(p, rating, focus, focus_label, rich_available,
                       exposure, crop, area):
    """The compact reason labels shown for one scored photo."""
    if not _species_is_subject(p):
        # The measurements below describe the photo's own subject, which is
        # a different bird from this entry's species.
        return ["scored on another bird"]
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
    return reasons


def _sort_highlight_bucket(photos, picked_first):
    """Order a scored bucket for the Highlights page or the ranked baseline."""
    if picked_first:
        # Highlights page ordering: four contiguous regions —
        #   1. picks (flagged): analyzed first (by score desc), then
        #      unanalyzed picks in capture order;
        #   2. analyzed non-picks: by highlight_score desc;
        #   3. unanalyzed non-picks: capture order (earliest first);
        #   4. photos this species is not the subject of
        #      (``species_presence``): the ones with it in frame first, the
        #      bird that fills most of the frame leading, then the ones
        #      whose crop leaves it out; capture order within a tie. A pick
        #      or a score there describes another bird, so neither orders
        #      this region.
        # The analyzed-before-unanalyzed tier separates the groups so the
        # score term only reorders analyzed rows and the timestamp term only
        # reorders unanalyzed rows — they never compete. Timestamps are ISO
        # text, so lexicographic order is chronological; missing timestamps
        # sort to the end of their group. This is an ascending sort (no
        # reverse), negating the score to rank analyzed rows high-first.
        def _bucket_sort_key(p):
            analyzed = p.get("quality_score") is not None
            ts = p.get("timestamp")
            if not _species_is_subject(p):
                return (
                    1,
                    0 if p.get("species_presence") == IN_FRAME else 1,
                    0,
                    -(p.get("species_frame_share") or 0.0),
                    (ts is None, ts),
                    p.get("id", 0),
                )
            return (
                0,
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


def _sort_highlights_first(photos, representative_order):
    """Put the highlights first, in their order, keeping the rest as ranked."""
    ranked_position = {photo["id"]: idx for idx, photo in enumerate(photos)}
    photos.sort(key=lambda photo: (
        0 if photo["id"] in representative_order else 1,
        representative_order.get(photo["id"], 0),
        ranked_position.get(photo["id"], 0),
    ))


def bucket_best_score(photos):
    """Return the highest highlight_score in a bucket, or None if empty.

    Ranks buckets on the Highlights page. Reads the max across all
    photos so a pick-first (or user-preferred) reorder at photos[0] can't
    demote a species by anchoring the bucket score to a lower-scored photo.
    Only photos the species is the subject of count: another photo's score
    measures a different bird.
    """
    scores = [
        p.get("highlight_score") for p in photos
        if p.get("highlight_score") is not None and _species_is_subject(p)
    ]
    return max(scores) if scores else None


def _bucket_unanalyzed_count(photos):
    """Length of the trailing "Not yet analyzed" tail of a bucket.

    Counts only the contiguous run of unscored non-pick photos at the END of
    ``photos``. This is what the divider labels, so callers can trust that a
    non-zero value marks a real tail below the divider.

    Only the trailing run is counted (not every unscored non-pick in the
    bucket) so that curated ordering — where
    :func:`apply_highlight_preferences`
    can promote an unscored, non-flagged photo to the front as a species
    highlight — doesn't inflate the count with photos that
    now live above the divider. Unscored picks are excluded because they
    stay grouped with the other picks at the front regardless. The photos
    the species is not the subject of trail the bucket under their own
    divider, so the run is counted above them.
    """
    if not photos:
        return 0
    tail = 0
    for p in reversed(photos):
        if not _species_is_subject(p):
            if tail:
                break
            continue
        if p.get("quality_score") is None and p.get("flag") != "flagged":
            tail += 1
        else:
            break
    return tail


def best_decision(top, ranking, note, fallback_kind=None):
    """Why ``top`` is the photo shown first for its species in this set.

    ``highlight``: it is the one highlight here that no other highlight
    beats. ``tie``: several highlights here have never been compared
    (``tied_photo_ids``) and the newest one is shown meanwhile. Otherwise
    nothing is highlighted here, and ``fallback_kind`` (or, when None, the
    Highlights page's own order) names what put it first: ``pick`` (a flagged
    pick, by Vireo's score among picks), ``score`` (Vireo's score) or
    ``unanalyzed`` (no score yet, so nothing but capture order). When no
    photo has the species as its subject, ``in_frame`` or ``outside_crop``
    names its presence instead of borrowing another species' pick or score.

    ``note`` is the choice that explains the photo's place
    (``species_preference.choice_note``), None when none was recorded.
    """
    if not top:
        return None
    decision = {
        "photo_id": top["id"],
        "contender_ids": list(ranking.order),
        "tied_photo_ids": [],
        "source": None,
        "reason": None,
        "chosen_at": None,
    }
    if ranking.order and ranking.order[0] == top["id"]:
        decision["kind"] = "tie" if ranking.tied else "highlight"
        if ranking.tied:
            decision["tied_photo_ids"] = list(ranking.leaders)
        for key in ("source", "reason", "chosen_at"):
            decision[key] = (note or {}).get(key)
    elif fallback_kind is not None:
        decision["kind"] = fallback_kind
    elif not _species_is_subject(top):
        decision["kind"] = top["species_presence"]
    elif top.get("flag") == "flagged":
        decision["kind"] = "pick"
    elif top.get("quality_score") is None:
        decision["kind"] = "unanalyzed"
    else:
        decision["kind"] = "score"
    return decision


def apply_highlight_preferences(db, buckets):
    """Mark each bucket's highlights, number them and put them first.

    A species' highlights are ranked within the bucket's own photos, so a
    folder or filtered view leads with the best highlight it can show and
    numbers the rest from there (``highlight_rank``, 1 first). The
    species-level state (``preferred_photo_id``, ``has_highlight_selection``)
    covers every highlight the workspace can see, so it stays true when the
    highlight is outside the current folder or search result.
    """
    highlights = db.species_curation.get_highlights(eligible_only=True)
    any_chosen = any(highlights.get(bucket["species"]) for bucket in buckets)
    choices = Preferences(db.species_choices.comparisons() if any_chosen else ())
    notes = db.species_choices.notes() if any_chosen else {}
    # A highlight visible only in another workspace is still chosen for the
    # species, so comparisons chained through it settle a pair shown here.
    globally_chosen = (
        db.species_curation.get_globally_eligible_representatives()
        if any_chosen else {}
    )
    for bucket in buckets:
        species = bucket["species"]
        chosen_ids = highlights.get(species) or []
        through = {*chosen_ids, *(globally_chosen.get(species) or ())}
        photos = bucket.get("photos") or []
        visible_ids = {photo.get("id") for photo in photos}
        ranking = choices.rank(
            [pid for pid in chosen_ids if pid in visible_ids], through,
        )
        position = {photo_id: idx for idx, photo_id in enumerate(ranking.order)}
        for photo in photos:
            place = position.get(photo.get("id"))
            photo["is_highlighted"] = place is not None
            photo["highlight_rank"] = None if place is None else place + 1
            photo["is_species_representative"] = place is not None
        # Highlights lead the bucket; the picked-first and score order of
        # _highlight_score_bucket stays for everything after them.
        if position:
            _sort_highlights_first(photos, position)
        workspace_order = choices.rank(chosen_ids, through).order
        top = photos[0] if photos else {}
        bucket["preferred_photo_id"] = workspace_order[0] if workspace_order else None
        bucket["has_highlight_selection"] = bool(chosen_ids)
        bucket["has_species_representative"] = bool(chosen_ids)
        bucket["has_preferred_photo"] = bool(position)
        bucket["highlight_count"] = len(position)
        bucket["best_quality"] = top.get("quality_score")
        # Rank by the highest-scored photo in the bucket, not photos[0]: a
        # highlight or pick may sit first with a lower highlight_score, and
        # using its score would demote the whole species below buckets whose
        # best photo is actually worse.
        bucket["best_score"] = bucket_best_score(photos)
        bucket["best_timestamp"] = top.get("timestamp")
        # Run last so a highlight without a score, pushed to the front,
        # does not leave a stale tail count anchoring the divider above
        # analyzed content.
        bucket["unanalyzed_count"] = _bucket_unanalyzed_count(photos)
        bucket["best_decision"] = best_decision(
            top, ranking, choice_note(notes, species, top.get("id")),
        )


def _workspace_highlight_rank(db, species, photo_id):
    """``photo_id``'s place (1 first) among the species' highlights the
    workspace can see, or None when it is not one of them."""
    photo_ids = db.species_curation.get_highlights(species).get(species, [])
    if photo_id not in photo_ids:
        return None
    if len(photo_ids) == 1:
        return 1
    order = Preferences(db.species_choices.comparisons()).rank(
        photo_ids, photo_ids,
    ).order
    return order.index(photo_id) + 1


def photo_highlight_entries(db, photo_id):
    """Return highlight-eligible species entries for one visible photo.

    Restricts both the candidate scan and the species-highlights lookup to
    the requested photo so the photo-detail endpoint doesn't rebuild every
    workspace bucket for each call (browse detail, lightbox, batch actions).
    """
    candidates = db.species_curation.get_highlights_candidates(
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
        for photo in bucket.get("photos") or []:
            if photo.get("id") != photo_id:
                continue
            rank = _workspace_highlight_rank(db, species, photo_id)
            entries.append({
                "species": species,
                "is_highlighted": rank is not None,
                "highlight_rank": rank,
                "is_confirmed": bool(photo.get("has_accepted_species")),
            })
            break
    return entries


def _highlight_confidence_label(confidence, is_accepted, whole_frame_only=False):
    """The bucket badge. ``whole_frame_only`` means every unconfirmed photo in
    it was classified from its whole frame with no detected animal, so the
    bucket is a guess about scenery and never "likely", whatever the
    softmax says."""
    if is_accepted:
        return "confirmed"
    if whole_frame_only:
        return "whole_frame"
    if confidence is None:
        return "unknown"
    if confidence >= 0.85:
        return "likely"
    return "candidate"


def _bucket_confidence(photos):
    """``(avg_confidence, whole_frame_count)`` over a bucket's photos.

    The average covers only predictions from a detected region: a
    whole-frame guess has no animal behind its softmax, so it neither raises
    nor lowers how sure the bucket looks.
    """
    localized = [p for p in photos if not p.get("whole_frame")]
    confidences = [
        p["predicted_confidence"]
        for p in localized
        if p.get("predicted_confidence") is not None
    ]
    avg_confidence = (
        round(sum(confidences) / len(confidences), 4)
        if confidences else None
    )
    return avg_confidence, len(photos) - len(localized)


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
    """Memoized wrapper around db.keyword_taxa.resolve_species_display.

    Bucket collection resolves the same handful of species strings for
    thousands of candidate rows; caching avoids a keyword lookup per row.
    """
    cache = {}

    def canonicalize(name):
        if name not in cache:
            cache[name] = db.keyword_taxa.resolve_species_display(name)
        return cache[name]

    return canonicalize


def collect_highlight_buckets(
    candidates,
    confidence_threshold,
    confirmation_filter="all",
    canonicalize_species=None,
    seen_species=None,
):
    """Group candidate rows into species buckets plus the unidentified photos.

    A photo with two species appears in both buckets, as a separate entry
    each, so each bucket's scores, ranks and confirm state are its own.
    ``seen_species``, when given a set, receives every species any candidate
    is a member of, whatever ``confirmation_filter`` then keeps, so a caller
    can say how many species its filter left out.
    """
    confirmation_filter = normalize_highlight_confirmation_filter(
        confirmation_filter
    )
    bucket_map = {}
    unidentified_photos = []

    for row in candidates:
        r = dict(row)
        memberships = photo_species_memberships(
            r, canonicalize_species, confidence_threshold,
        )
        presence = species_presence(r, memberships, canonicalize_species)
        confirmed = [m.species for m in memberships if m.accepted_name]
        all_buckets = [m.species for m in memberships]
        if seen_species is not None:
            seen_species.update(all_buckets)
        if not memberships:
            if confirmation_filter != "confirmed":
                subjects = candidate_subjects(r)
                # A whole-frame prediction below the confidence threshold
                # lands in Unidentified; the chip must still say "Whole
                # frame" instead of a bare "ID N%" (the exact case
                # ``whole_frame`` distinguishes), so read it off the
                # fallback subject here as well as on the bucket path.
                fallback = subjects[0] if subjects else None
                unidentified_photos.append(_highlight_candidate_photo(
                    r, HighlightMembership(
                        None, None, fallback,
                        whole_frame=bool(fallback) and subject_is_whole_frame(fallback),
                    ),
                    confirmed, all_buckets,
                ))
            continue
        for membership in memberships:
            is_accepted = membership.accepted_name is not None
            if confirmation_filter == "confirmed" and not is_accepted:
                continue
            if confirmation_filter == "unconfirmed" and is_accepted:
                continue
            entry = bucket_map.setdefault(
                membership.species, {"is_accepted": True, "photos": []}
            )
            entry["is_accepted"] = entry["is_accepted"] and is_accepted
            entry["photos"].append(_highlight_candidate_photo(
                r, membership, confirmed, all_buckets, presence,
            ))

    buckets = []
    for species, entry in bucket_map.items():
        buckets.append(_highlight_bucket_entry(species, entry))

    _highlight_score_bucket(unidentified_photos, picked_first=True)
    # Buckets made only of whole-frame guesses sort after every bucket with
    # a detected animal behind it, whatever their scores.
    buckets.sort(
        key=lambda b: (
            b.get("certainty") != "whole_frame",
            b.get("best_score") or 0,
            b.get("avg_confidence") or 0,
            b.get("photo_count") or 0,
        ),
        reverse=True,
    )
    return buckets, unidentified_photos


def _highlight_candidate_photo(
    r, membership, confirmed_species, bucket_species, presence=None,
):
    """The Highlights photo entry for one candidate row in one bucket.

    ``presence`` is the photo's ``species_presence`` map. The entry carries
    this bucket's part of it (``species_presence``, ``species_frame_share``)
    and ``subject_species_list``, the species the photo is a picture of, so
    a card in another species' bucket can name them.

    ``confirmed_species`` lists every species the photo carries as a
    keyword, so an entry in one species' bucket can say what else the
    photo is already tagged with. ``bucket_species`` lists every bucket the
    photo is in, confirmed or not, so the page can refresh each of them
    after a change even where the photo is past the loaded window.
    """
    accepted = membership.accepted_name
    species = membership.species
    prediction = membership.prediction or {}
    presence = presence or {}
    here = presence.get(species) or Presence(SUBJECT)
    return {
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
        "canonical_species": species if accepted is not None else None,
        "bucket_species": species,
        "confirmed_species_list": list(confirmed_species),
        "bucket_species_list": list(bucket_species),
        "species_presence": here.kind,
        "species_frame_share": here.frame_share,
        "subject_species_list": [
            name for name, value in presence.items() if value.kind == SUBJECT
        ],
        "prediction_id": prediction.get("prediction_id"),
        "predicted_species": prediction.get("species"),
        "predicted_confidence": prediction.get("confidence"),
        "predicted_detector_confidence": prediction.get("detector_confidence"),
        # The prediction came from the whole frame (the detector found no
        # animal) or from a region the classifier scored like background;
        # either way the page says so instead of showing a bare "ID 96%".
        "whole_frame": membership.whole_frame,
        "background_like": bool(prediction) and subject_is_background_like(prediction),
        "has_accepted_species": accepted is not None,
        "is_unidentified": species is None,
        "is_confirmable_prediction": (
            accepted is None
            and species is not None
            and prediction.get("prediction_id") is not None
        ),
    }


def _highlight_bucket_entry(species, entry):
    """Score one species bucket and summarize it for the page."""
    photos = entry["photos"]
    _highlight_score_bucket(photos, picked_first=True)
    avg_confidence, whole_frame_count = _bucket_confidence(photos)
    top = photos[0] if photos else {}
    return {
        "species": species,
        "is_accepted": entry["is_accepted"],
        "certainty": _highlight_confidence_label(
            avg_confidence, entry["is_accepted"],
            whole_frame_only=bool(photos) and whole_frame_count == len(photos),
        ),
        "avg_confidence": avg_confidence,
        "whole_frame_count": whole_frame_count,
        "photo_count": len(photos),
        "best_quality": top.get("quality_score"),
        "best_score": bucket_best_score(photos),
        "best_timestamp": top.get("timestamp"),
        "photos": photos,
    }


def _highlight_search_fields(photo):
    # A photo with a species is searched by that species, not by a
    # classifier guess the user never took (same rule as Browse search).
    predicted = "" if photo.get("species") else photo.get("predicted_species")
    return [
        photo.get("filename") or "",
        photo.get("folder_name") or "",
        photo.get("folder_path") or "",
        photo.get("keyword_names") or "",
        photo.get("species") or "",
        photo.get("canonical_species") or "",
        predicted or "",
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
    avg_confidence, whole_frame_count = _bucket_confidence(photos)
    top = photos[0] if photos else {}
    # Filtering can drop the unconfirmed photos from a mixed bucket, leaving
    # only confirmed ones — recompute so the "candidate" badge and the
    # `confirmed` sort match what the row now actually contains.
    bucket["is_accepted"] = bool(photos) and all(
        p.get("has_accepted_species") for p in photos
    )
    bucket["avg_confidence"] = avg_confidence
    bucket["whole_frame_count"] = whole_frame_count
    bucket["certainty"] = _highlight_confidence_label(
        avg_confidence, bucket.get("is_accepted"),
        whole_frame_only=bool(photos) and whole_frame_count == len(photos),
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
):
    """Keep the species that do (``yes``) or do not (``no``) have a highlight."""
    highlight_filter = _normalize_highlight_presence_filter(highlight_filter)
    if highlight_filter == "all":
        return buckets, unidentified_photos
    wanted = highlight_filter == "yes"
    filtered = [
        bucket for bucket in buckets
        if bool(bucket.get("has_highlight_selection")) == wanted
    ]
    # Only a named species has a highlight list, so unidentified photos match
    # neither state.
    return filtered, []


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


def _filter_highlights_by_species(
    buckets, unidentified_photos, species_filter, species_match_case,
    species_whole_word,
):
    """Keep the buckets (and the Unidentified section) the species filter matches."""
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
    return buckets, unidentified_photos


def _limit_highlight_buckets(db, buckets, unidentified_photos, limit_per_bucket):
    """Trim each section to its first page and attach the shown photos' edits."""
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
    return limited_buckets, unidentified_limited


@dataclass(frozen=True)
class TextMatch:
    """A text filter as the filter bar sends it: the text and its toggles."""

    text: str = ""
    match_case: bool = False
    whole_word: bool = False


@dataclass(frozen=True)
class HighlightsQuery:
    """What one Highlights payload covers and how it is filtered and paged.

    The defaults are the Highlights page's: the most recent folder with
    quality data, every species and section, twenty photos per section.
    """

    scope: str = "folder"
    """``"workspace"`` for every folder, else one folder (``folder_id``, or
    the most recent folder with quality data when that is None)."""
    folder_id: int | None = None
    min_quality: float = 0.0
    confidence_threshold: float = 0.70
    limit_per_bucket: int = 20
    """Photos per section on the first page, clamped to 1..100."""
    species: TextMatch = field(default_factory=TextMatch)
    """Keeps only the species sections whose name matches."""
    search: TextMatch = field(default_factory=TextMatch)
    """The free-text search over sections and photos."""
    confirmation_filter: str = "all"
    highlight_filter: str = "all"
    collection_id: int | None = None
    rules: object = None
    """Visual membership resolved for this collection, when applicable."""

    def resolved_folder(self, folders):
        """Use the most recent folder only when no broader scope was requested."""
        if self.collection_id is not None or self.scope == "workspace":
            return None
        if self.folder_id is not None or not folders:
            return self.folder_id
        return folders[0]["id"]

    def candidates(self, db, folder_id):
        return db.species_curation.get_highlights_candidates(
            folder_id, min_quality=self.min_quality,
            collection_id=self.collection_id, rules=self.rules,
        )


def build_highlights_payload(db, query=None):
    """The Highlights payload for ``query`` (a ``HighlightsQuery``; None
    for the page defaults)."""
    if query is None:
        query = HighlightsQuery()
    folders = db.folders.with_quality_data()
    folder_id = query.resolved_folder(folders)
    limit_per_bucket = max(1, min(int(query.limit_per_bucket), 100))
    species_filter = (query.species.text or "").strip()
    confirmation_filter = normalize_highlight_confirmation_filter(
        query.confirmation_filter
    )
    highlight_filter = _normalize_highlight_presence_filter(query.highlight_filter)

    candidates = query.candidates(db, folder_id)
    if query.collection_id is None:
        scope_name = "workspace" if folder_id is None else "folder"
        total_in_scope = db.photos.count_filtered(folder_id=folder_id)
    else:
        scope_name = "collection"
        total_in_scope = db.collections.count_photos_for_rules(
            query.rules or [], collection_id=query.collection_id,
        )

    buckets, unidentified_photos = collect_highlight_buckets(
        candidates, query.confidence_threshold, confirmation_filter,
        canonicalize_species=species_canonicalizer(db),
    )
    # Distinct photos: a two-species photo sits in two buckets.
    eligible_count = len(
        {p["id"] for b in buckets for p in b["photos"]}
        | {p["id"] for p in unidentified_photos}
    )
    buckets, unidentified_photos = _filter_highlights_by_species(
        buckets, unidentified_photos, species_filter, query.species.match_case,
        query.species.whole_word,
    )

    buckets, unidentified_photos = filter_highlight_sections(
        buckets,
        unidentified_photos,
        query.search.text,
        query.search.match_case,
        query.search.whole_word,
    )
    apply_highlight_preferences(db, buckets)
    buckets, unidentified_photos = filter_highlight_curation_state(
        buckets,
        unidentified_photos,
        highlight_filter,
    )

    limited_buckets, unidentified_limited = _limit_highlight_buckets(
        db, buckets, unidentified_photos, limit_per_bucket,
    )

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
            "search": (query.search.text or "").strip(),
        },
        "scope": scope_name,
    }


class _LifeListIdentities:
    """Group linked names by taxon while leaving stored curation keys intact."""

    def __init__(self, db):
        self.canonicalize = species_canonicalizer(db)
        self.names = {}
        self.aliases = {}
        for row in db.species_curation.get_life_list_keywords():
            identity = self.key(row)
            name = (row["species"] if row["taxon_id"] is not None
                    else self.canonicalize(row["species"]))
            # The query orders roots before hierarchy leaves, then by id.
            self.names.setdefault(identity, name)
            self.aliases.setdefault(row["species"], set()).add(identity)

    def key(self, row):
        if row["taxon_id"] is not None:
            return ("taxon", row["taxon_id"])
        return ("name", self.canonicalize(row["species"]) or row["species"])


def _merge_life_list_locations(identities, rows):
    """Combine alias locations without sharing them across different taxa."""
    locations = {}
    for row in rows:
        existing = locations.setdefault(identities.key(row), [])
        if row["location"] not in existing:
            existing.append(row["location"])
    return locations


def _life_list_selection_maps(db, identities, buckets, scoped):
    """``{identity: [photo_id, ...]}``: the highlights saved under any alias
    of each listed species, newest selection first."""
    species_names = None
    if scoped:
        species_names = {
            name for name, keys in identities.aliases.items()
            if keys.intersection(buckets)
        }
    highlights = {}
    for row in db.species_curation.get_life_list_selections(species_names):
        keys = identities.aliases.get(row["species"], set())
        for key in keys.intersection(buckets):
            # Legacy curation keys contain only a name. A shared name
            # must not move a selection onto a different taxon's photo.
            if len(keys) > 1 and row["photo_id"] not in buckets[key]["seen_ids"]:
                continue
            ids = highlights.setdefault(key, [])
            if row["photo_id"] not in ids:
                ids.append(row["photo_id"])
    return highlights


def _life_list_buckets(rows, identities, class_by_taxon):
    """Group linked names by taxon and unlinked names by stored spelling."""
    buckets = {}
    for row in rows:
        r = dict(row)
        identity = identities.key(r)
        entry = buckets.setdefault(identity, {
            "species": identities.names.get(identity, r["species"]),
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
    return buckets


def _compact_life_list_photo(photo):
    """The trimmed photo dict a Life List entry carries."""
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


_LIFE_LIST_DECISION_KINDS = {"pick": "pick"}


def _rank_listed_representatives(
    preferences, representative_ids, photos, chosen_for_species=None,
):
    """``(ranking, ids)``: the chosen photos on the list ranked by the
    comparisons, then every chosen id with those first. A chosen photo the
    list cannot show (rejected, or no longer this species) keeps its
    selection order after them and never leads one it can; ``chosen_for_species``
    carries the catalog-wide eligible set so a chain through a representative
    visible only in another workspace still settles a scoped pair, while a
    stale row kept for undo (no longer tagged with the species) stays inert."""
    listed_ids = {photo["id"] for photo in photos}
    live_ids = [pid for pid in representative_ids if pid in listed_ids]
    through = chosen_for_species if chosen_for_species is not None else live_ids
    ranking = preferences.rank(live_ids, through)
    return ranking, [
        *ranking.order,
        *(pid for pid in representative_ids if pid not in listed_ids),
    ]


def _life_list_species_entry(
    species, entry, photos, representative_ids,
    locations, photo_offset, photo_limit, choices,
):
    """Rank one species' photos and build its Life List entry.

    ``choices`` is ``(preferences, notes, globally_chosen)``: the recorded
    comparisons, each chosen photo's choices, and the catalog-wide eligible
    representatives per species used as the ranking's traversal set.
    """
    preferences, notes, globally_chosen = choices
    timestamps = [p["timestamp"] for p in photos if p.get("timestamp")]
    _highlight_score_bucket(photos)
    ranking, representative_ids = _rank_listed_representatives(
        preferences, representative_ids, photos,
        chosen_for_species=globally_chosen.get(species),
    )
    representative_order = {
        photo_id: idx for idx, photo_id in enumerate(representative_ids)
    }
    preferred_id = representative_ids[0] if representative_ids else None
    listed_place = {photo_id: idx for idx, photo_id in enumerate(ranking.order)}
    for photo in photos:
        place = listed_place.get(photo["id"])
        photo["is_highlighted"] = place is not None
        photo["highlight_rank"] = None if place is None else place + 1
        photo["is_species_representative"] = place is not None
    preferred_applied = bool(listed_place)
    # Life List order is the highlights, in their order, then Picks (the P
    # flag), then the algorithmic ranking. Ranked positions are the final
    # tie-breaker, so several Picks keep their quality order.
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
    if best_photo.get("is_highlighted"):
        best_source = "highlight"
    elif best_photo.get("flag") == "flagged":
        best_source = "pick"
    else:
        best_source = "algorithm"
    if photo_limit is None:
        top = photos[photo_offset:]
    else:
        top = photos[photo_offset:photo_offset + photo_limit]
    return {
        "species": species,
        "scientific_name": entry["scientific_name"],
        "common_name": entry["common_name"],
        "taxon_id": entry["taxon_id"],
        "taxon_rank": entry["taxon_rank"],
        "taxonomic_class": entry["taxonomic_class"],
        "photo_count": len(photos),
        "first_seen": min(timestamps) if timestamps else None,
        "last_seen": max(timestamps) if timestamps else None,
        "locations": locations,
        "preferred_photo_id": preferred_id,
        "has_preferred_photo": preferred_applied,
        "best_source": best_source,
        "best_decision": best_decision(
            best_photo, ranking, choice_note(notes, species, best_photo.get("id")),
            fallback_kind=_LIFE_LIST_DECISION_KINDS.get(best_source),
        ),
        "best": _compact_life_list_photo(photos[0]) if photos else None,
        "photos": [_compact_life_list_photo(p) for p in top],
        "loaded_count": min(len(photos), photo_offset + len(top)),
        "has_more": photo_offset + len(top) < len(photos),
    }


def build_life_list_payload(
    db,
    photos_per_species: int | None = 12,
    photo_offset=0,
    species_filter=None,
    taxon_id=None,
    unlinked_only=False,
):
    rows = db.species_curation.get_life_list_candidates(species=species_filter)
    if unlinked_only:
        rows = [row for row in rows if row["taxon_id"] is None]
    elif taxon_id is not None:
        rows = [row for row in rows if row["taxon_id"] == taxon_id]
    location_rows = db.species_curation.get_life_list_location_rows(species=species_filter)
    class_by_taxon = db.species_curation.get_class_ancestors({
        row["taxon_id"] for row in rows if row["taxon_id"] is not None
    })
    photo_offset = max(0, int(photo_offset))
    if photos_per_species is None:
        photo_limit = None
    else:
        photo_limit = max(
            1, min(int(photos_per_species), 100)
        )

    # Taxon identity unifies all stored aliases, including two differently
    # spelled roots. Name resolution alone intentionally preserves each root
    # because keyword writes and saved curation still use those spellings.
    identities = _LifeListIdentities(db)
    locations = _merge_life_list_locations(identities, location_rows)
    buckets = _life_list_buckets(rows, identities, class_by_taxon)
    representatives = _life_list_selection_maps(
        db, identities, buckets, scoped=species_filter is not None,
    )
    choices = (Preferences(), {}, {})
    if representatives:
        choices = (
            Preferences(db.species_choices.comparisons()),
            db.species_choices.notes(),
            db.species_curation.get_globally_eligible_representatives(),
        )

    species_entries = []
    distinct_photo_ids = set()
    for identity, entry in buckets.items():
        photos = entry["photos"]
        distinct_photo_ids.update(p["id"] for p in photos)
        species_entries.append(_life_list_species_entry(
            entry["species"], entry, photos, representatives.get(identity, []),
            locations.get(identity, []), photo_offset, photo_limit, choices,
        ))

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
