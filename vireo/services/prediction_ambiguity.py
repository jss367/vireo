"""Which pending predictions a bare Accept must not act on.

Browse's selection panel splits its payload into acceptable and ambiguous
prediction ids, and ``batch-accept`` re-derives the same verdict before it
writes. Both go through ``ambiguous_prediction_ids`` here so the rule has one
implementation. ``effective_category_resolver`` compares a prediction against
the photo's *current* species keywords (the stored ``predictions.category``
column is a classification-time snapshot), and ``prediction_is_ambiguous``
turns that comparison, or the stored snapshot when no fresh comparison is
possible, into the verdict.

Nothing here touches ``request`` or takes the prediction-decision lock; the
routes that decide predictions do that through ``services.prediction_decisions``.
"""

from __future__ import annotations


def effective_category_resolver(db, photo_ids):
    """Build ``(photo_id, species) -> category`` against *current* keywords.

    ``predictions.category`` is a snapshot of how the prediction compared
    to the photo's keywords at classification time, and nothing rewrites
    it when keywords change afterwards (the only writers are the classify
    path and duplicate merge). So a photo that gained a Robin keyword
    after a pending Sparrow prediction was stored as ``new`` still reads
    ``new`` — and Browse would offer a bare Accept that tags a species
    conflicting with what the photo already says. ``CORE_PHILOSOPHY.md``
    forbids exactly that: the button must mean what the user reads it as.

    Returns ``match``/``new``/``refinement``/``broader``/``conflict`` from
    ``compare_prediction_to_keywords`` — Compare's vocabulary, because
    this is Compare's computation, shared rather than reimplemented (see
    ``api_predictions_compare``). Callers treat
    ``refinement``/``broader``/``conflict`` as ambiguous, the same set
    Browse's ``predictionIsAmbiguous`` refuses to offer a bare Accept for.

    Two details are load-bearing and are the reason this goes through the
    same helpers Compare uses rather than a raw keyword query:

    * ``get_species_keywords_for_photos`` canonicalizes a hierarchy alias
      through its linked taxon's root, and ``resolve_species_display_name``
      does the same for the prediction label. Comparing raw
      ``keywords.name`` text would make a photo tagged with the leaf
      ``Desert Verdin`` read as *conflicting* with a ``Verdin``
      prediction whenever the taxonomy file is unavailable — inventing an
      ambiguity and sending a settled photo to Review.
    * the comparison runs on the species the accept path would actually
      apply (the burst consensus), not the row's own label.

    Returns None when no comparison is possible (compare or the photo set
    unavailable) so callers can fall back to the stored snapshot.
    """
    photo_ids = [pid for pid in dict.fromkeys(photo_ids) if pid is not None]
    if not photo_ids:
        return None
    try:
        from compare import compare_prediction_to_keywords
    except Exception:
        return None
    # Cached by mtime inside load_local_taxonomy, so this is a lookup on
    # the hot path rather than a re-parse per request. None degrades
    # compare_prediction_to_keywords to exact-text matching, which is
    # still current-state truth — better than a stale column either way,
    # and a missing or corrupt taxonomy file must never hard-fail the
    # endpoint.
    try:
        from taxonomy import load_local_taxonomy
        taxonomy = load_local_taxonomy()
    except Exception:
        taxonomy = None
    from species_identity import SpeciesResolver
    resolver = SpeciesResolver(db=db)
    species_by_photo = db.get_species_keywords_for_photos(photo_ids, include_identities=True)
    resolved = {}
    cache = {}

    def comparison_name(species, identity=None):
        identity = identity or resolver.display(species)
        if identity.scientific_name:
            return identity.scientific_name
        if species not in resolved:
            resolved[species] = db.resolve_species_display_name(identity.display_name)
        return resolved[species]

    keyword_names = {}
    for photo_id, entries in species_by_photo.items():
        names = []
        for entry in entries:
            source = {"taxon_id": int(entry["key"][6:])} if entry["key"].startswith("taxon:") else None
            identity = resolver.resolve(entry["name"], source=source) if source else resolver.display(entry["name"])
            names.append(comparison_name(entry["name"], identity))
        keyword_names[photo_id] = names

    def _category(photo_id, species, identity=None):
        if not species or photo_id is None:
            return None
        identity = identity or resolver.display(species)
        key = (photo_id, identity.key)
        if key not in cache:
            if any(entry["key"] == identity.key for entry in species_by_photo.get(photo_id, [])):
                cache[key] = "match"
            else:
                comparison = compare_prediction_to_keywords(
                    comparison_name(species, identity),
                    keyword_names.get(photo_id, []),
                    taxonomy,
                )
                cache[key] = (
                    comparison.get("category")
                    if isinstance(comparison, dict) else None
                )
        return cache[key]

    return _category


# Fresh-comparison categories a bare Accept must not act on.
EFFECTIVE_AMBIGUOUS_CATEGORIES = frozenset(
    {"refinement", "broader", "conflict"}
)
# Stored-snapshot categories that mean the same thing, used only when no
# fresh comparison is available.
STORED_AMBIGUOUS_CATEGORIES = frozenset({"disagreement", "refinement"})


def prediction_is_ambiguous(effective_category, stored_category):
    """Would a bare Accept here be dishonest?

    The fresh comparison wins outright when there is one. ORing it with
    the stored snapshot would make ambiguity a one-way ratchet: a photo
    whose conflicting keyword has since been removed would keep being
    routed to Review forever, naming a conflict that no longer exists —
    the same staleness bug in the other direction. The snapshot is the
    fallback for when no fresh comparison could be made at all.
    """
    if effective_category is not None:
        return effective_category in EFFECTIVE_AMBIGUOUS_CATEGORIES
    return stored_category in STORED_AMBIGUOUS_CATEGORIES


def ambiguous_prediction_ids(db, rows):
    """Which of ``rows`` a bare Accept must not act on.

    The one definition of "ambiguous" for the pair of endpoints that
    need it: the selection panel, which splits its payload into
    ``acceptable_prediction_ids`` and ``ambiguous_prediction_ids``, and
    ``batch-accept``, which re-derives the same verdict before writing.
    Two conditions, both of which mean a bare Accept would decide
    something the user has not been shown:

    * an ``alternative`` sibling on the row's ``(detection, model)`` —
      the classifier offered a runner-up, so accepting picks a winner on
      the user's behalf;
    * a disagreement/refinement against the photo's species keywords,
      judged by ``prediction_is_ambiguous`` on the *current* keywords
      (see ``effective_category_resolver`` for why the stored
      ``category`` column cannot be trusted for this).

    ``batch-accept`` recomputes rather than trusting the payload because
    the panel's split is a snapshot: a keyword added from Review, a
    second Browse tab, or an XMP sync between render and click makes a
    row ambiguous while it is still ``pending``, so the decided-status
    precondition alone cannot catch it. The panel's own refresh handles
    mutations inside one document; only the server sees the rest. This
    lives here — not once per endpoint — for the reason rounds 7 and 8
    established for the status precondition and the accept scope: a rule
    with two implementations is a rule that drifts.

    ``rows`` are prediction rows carrying ``id``, ``photo_id``,
    ``detection_id``, ``model``, ``category``, ``species``, ``group_id``
    and ``individual``. Returns the ambiguous subset of their ids.
    """
    rows = list(rows)
    if not rows:
        return set()
    photo_ids = list(dict.fromkeys(
        row["photo_id"] for row in rows if row["photo_id"] is not None
    ))
    # Keyed by (detection, model) exactly as /api/predictions nests
    # alternatives, so "has alternatives" means the same thing in Browse,
    # in this check, and in Review.
    alt_keys = {
        (row["detection_id"], row["model"])
        for row in db.get_predictions(
            photo_ids=photo_ids, status="alternative",
        )
    }
    effective_category_of = effective_category_resolver(db, photo_ids)
    from species_identity import SpeciesResolver
    resolver = SpeciesResolver(db=db)
    ambiguous = set()
    for row in rows:
        # Compared on the species the accept path would actually apply
        # (the burst consensus), not the row's own label.
        identity = resolver.consensus(row)
        species = identity.display_name
        effective_category = (
            effective_category_of(row["photo_id"], species, identity)
            if effective_category_of is not None and species else None
        )
        if (
            (row["detection_id"], row["model"]) in alt_keys
            or prediction_is_ambiguous(effective_category, row["category"])
        ):
            ambiguous.add(row["id"])
    return ambiguous
