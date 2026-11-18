"""The best photo of each species within a set of photos.

``build_species_best_payload`` answers "one photo per species for this trip,
this year, or everything" for any scope Browse can filter to. It is the
Highlights payload's own buckets and order, cut to the first photos of each
species, so the photo it names for a species is the photo the Highlights
page leads that species with over the same photos:

1. the species' highlights, ranked by the recorded comparisons, the newest
   one first among photos nobody has compared;
2. flagged picks, by Vireo's score;
3. everything else by Vireo's score, unanalyzed photos last.
4. photos where the species is only in frame or outside the crop
   (``species_presence``); another bird's pick or score does not promote them.

Each species carries a ``decision`` saying which of these put its first
photo there (``highlights_payload.best_decision``), so a reader can tell a
highlight from a photo Vireo guessed, and a settled highlight from two
nobody has compared.

A pure payload builder: a ``Database`` in, JSON-ready dicts out.
"""

from dataclasses import dataclass, field

from highlights_payload import (
    apply_highlight_preferences,
    collect_highlight_buckets,
    species_canonicalizer,
)
from keyword_normalization import keyword_match_key
from services.species_choices import PhotoScope

MAX_PHOTOS_PER_SPECIES = 50
#: Decision kinds where nobody chose the photo: Vireo ordered it.
GUESSED_KINDS = frozenset({"score", "unanalyzed", "in_frame", "outside_crop"})


@dataclass(frozen=True)
class SpeciesBestQuery:
    """What one best-of-each-species payload covers."""

    scope: PhotoScope = field(default_factory=PhotoScope)
    species: str | None = None
    """Only this species, by name; None for every species in the scope."""
    per_species: int = 1
    """Photos per species, best first, clamped to 1..MAX_PHOTOS_PER_SPECIES."""
    include_unconfirmed: bool = False
    """Also list species held only by an unconfirmed prediction."""
    confidence_threshold: float = 0.70
    """The prediction confidence an unconfirmed species needs to be listed."""


def _photo_entry(photo):
    """The trimmed photo dict a species entry carries."""
    return {
        "id": photo["id"],
        "filename": photo["filename"],
        "timestamp": photo.get("timestamp"),
        "folder_name": photo.get("folder_name"),
        "rating": photo.get("rating") or 0,
        "flag": photo.get("flag") or "none",
        "species_presence": photo.get("species_presence") or "subject",
        "subject_species": photo.get("subject_species_list") or [],
        "quality_score": photo.get("quality_score"),
        "is_analyzed": photo.get("quality_score") is not None,
        "highlight_score": photo.get("highlight_score"),
        "reasons": photo.get("reasons") or [],
        "is_highlighted": bool(photo.get("is_highlighted")),
        "highlight_rank": photo.get("highlight_rank"),
        "is_confirmed": bool(photo.get("has_accepted_species")),
        "predicted_confidence": photo.get("predicted_confidence"),
    }


def _species_entry(bucket, per_species):
    decision = bucket.get("best_decision")
    photos = bucket.get("photos") or []
    return {
        "species": bucket["species"],
        "is_confirmed": bool(bucket.get("is_accepted")),
        "certainty": bucket.get("certainty"),
        "photo_count": len(photos),
        "decision": decision,
        "photos": [_photo_entry(p) for p in photos[:per_species]],
    }


def _species_key(species, canonicalize):
    """The match key of the species asked for, None when every species is."""
    if species is None:
        return None
    return keyword_match_key(canonicalize(species) or species)


def _matching_buckets(buckets, species, canonicalize):
    wanted = _species_key(species, canonicalize)
    if wanted is None:
        return buckets
    return [b for b in buckets if keyword_match_key(b["species"]) == wanted]


def build_species_best_payload(db, query=None):
    """The best photos of each species in ``query.scope``, best first.

    Species are listed alphabetically. A species held only by an unconfirmed
    prediction is left out unless ``include_unconfirmed`` is set, and
    ``meta.unconfirmed_species_omitted`` counts what that left out, so a
    short list is never mistaken for a complete one. Raises ``ValueError``
    on a malformed rule tree or an unknown collection.
    """
    if query is None:
        query = SpeciesBestQuery()
    per_species = max(1, min(int(query.per_species), MAX_PHOTOS_PER_SPECIES))
    scope = query.scope
    canonicalize = species_canonicalizer(db)
    candidates = db.species_curation.get_highlights_candidates(
        scope.folder_id, min_quality=0.0,
        rules=scope.rules, collection_id=scope.collection_id,
    )
    seen_species = set()
    buckets, _unidentified = collect_highlight_buckets(
        candidates, query.confidence_threshold,
        "all" if query.include_unconfirmed else "confirmed",
        canonicalize_species=canonicalize, seen_species=seen_species,
    )
    buckets = _matching_buckets(buckets, query.species, canonicalize)
    # Species some candidate belongs to but the list does not show: the ones
    # held only by an unconfirmed prediction, when those are left out.
    wanted = _species_key(query.species, canonicalize)
    listed = {keyword_match_key(b["species"]) for b in buckets}
    unconfirmed_omitted = len({
        key for key in map(keyword_match_key, seen_species)
        if key not in listed and wanted in (None, key)
    })
    apply_highlight_preferences(db, buckets)
    entries = sorted(
        (_species_entry(bucket, per_species) for bucket in buckets),
        key=lambda e: (e["species"].lower(), e["species"]),
    )
    kinds = [e["decision"]["kind"] for e in entries if e["decision"]]
    return {
        "species": entries,
        "meta": {
            "scope": scope.as_json(),
            "species_count": len(entries),
            "photos_considered": len(candidates),
            "per_species": per_species,
            "include_unconfirmed": bool(query.include_unconfirmed),
            "unconfirmed_species_omitted": unconfirmed_omitted,
            "highlighted": kinds.count("highlight"),
            "needs_comparison": kinds.count("tie"),
            "guessed": sum(1 for kind in kinds if kind in GUESSED_KINDS),
        },
    }
