"""Compare model predictions against existing keywords using taxonomy."""

import logging

log = logging.getLogger(__name__)

_CATEGORY_PRIORITY = {
    "conflict": 50,
    "refinement": 40,
    "broader": 30,
    "new": 20,
    "match": 10,
}


def categorize(prediction, existing_keywords, taxonomy):
    """Categorize a prediction relative to existing keywords using taxonomy.

    Args:
        prediction: the model's predicted species name
        existing_keywords: set of all dc:subject keywords from the XMP
        taxonomy: a Taxonomy instance for looking up taxa and relationships

    Returns:
        'match' — prediction matches an existing species keyword
        'new' — no existing species keywords found
        'refinement' — prediction is more specific than an existing keyword
        'disagreement' — prediction differs from existing species keyword
    """
    # Filter existing keywords to just those recognized as taxa
    existing_taxa = []
    for kw in existing_keywords:
        if taxonomy.is_taxon(kw):
            existing_taxa.append(kw)
        else:
            log.debug("Ignoring non-taxon keyword: %s", kw)

    # No recognized taxa in existing keywords — this is new info
    if not existing_taxa:
        return "new"

    # Check each existing taxon against the prediction
    for taxon_kw in existing_taxa:
        rel = taxonomy.relationship(taxon_kw, prediction)

        if rel is None:
            log.warning("Prediction '%s' not found in taxonomy", prediction)
            return "disagreement"
        elif rel == "same":
            return "match"
        elif rel == "ancestor":
            # Existing is broader, prediction is more specific → refinement
            return "refinement"
        elif rel == "descendant":
            # Existing is more specific, prediction is broader — unusual but treat as match
            return "match"
        # 'sibling' and 'unrelated' fall through to check remaining taxa

    # No existing taxon matched/contained the prediction
    return "disagreement"


def _taxon_rank_map(taxon):
    if not taxon:
        return {}
    ranks = taxon.get("lineage_ranks") or []
    names = taxon.get("lineage_names") or []
    return {rank: name for rank, name in zip(ranks, names, strict=False)}


def _shared_rank(pred_taxon, keyword_taxon):
    pred = _taxon_rank_map(pred_taxon)
    kw = _taxon_rank_map(keyword_taxon)
    for rank in ("genus", "family", "order", "class", "phylum", "kingdom"):
        if pred.get(rank) and pred.get(rank) == kw.get(rank):
            return rank
    return None


def compare_prediction_to_keywords(prediction, existing_species, taxonomy):
    """Return a UI-oriented comparison between one prediction and species keywords.

    The legacy ``categorize`` result is intentionally coarse because it is
    stored on prediction rows. The Compare page needs a sharper explanation:
    match/refinement/broader/new/conflict plus the keyword and taxonomic rank
    that made that decision.
    """
    existing_species = [kw for kw in existing_species if kw]
    if not existing_species:
        return {
            "category": "new",
            "label": "New species keyword",
            "detail": "No species keyword is currently attached to this photo.",
            "matched_keyword": None,
            "shared_rank": None,
        }

    if taxonomy is None:
        for kw in existing_species:
            if kw.lower() == prediction.lower():
                return {
                    "category": "match",
                    "label": "Match",
                    "detail": "Prediction text matches an existing species keyword.",
                    "matched_keyword": kw,
                    "shared_rank": None,
                }
        return {
            "category": "conflict",
            "label": "Conflict",
            "detail": "Taxonomy data is unavailable, so only exact keyword matches can be confirmed.",
            "matched_keyword": existing_species[0],
            "shared_rank": None,
        }

    pred_taxon = taxonomy.lookup(prediction)
    if pred_taxon is None:
        return {
            "category": "conflict",
            "label": "Unknown prediction",
            "detail": "The predicted species was not found in the local taxonomy.",
            "matched_keyword": existing_species[0],
            "shared_rank": None,
        }

    best = None
    for kw in existing_species:
        keyword_taxon = taxonomy.lookup(kw)
        rel = taxonomy.relationship(kw, prediction)
        shared = _shared_rank(pred_taxon, keyword_taxon)
        if rel == "same":
            result = {
                "category": "match",
                "label": "Match",
                "detail": "Prediction agrees with the existing species keyword.",
                "matched_keyword": kw,
                "shared_rank": shared,
            }
        elif rel == "ancestor":
            result = {
                "category": "refinement",
                "label": "Refinement",
                "detail": "Prediction is more specific than the existing species keyword.",
                "matched_keyword": kw,
                "shared_rank": shared,
            }
        elif rel == "descendant":
            result = {
                "category": "broader",
                "label": "Broader",
                "detail": "Prediction is broader than the existing species keyword.",
                "matched_keyword": kw,
                "shared_rank": shared,
            }
        else:
            if rel == "sibling":
                detail = "Prediction and keyword share the same immediate parent."
            elif shared:
                detail = f"Prediction and keyword share the same {shared}."
            elif rel is None:
                detail = "At least one keyword was not found in the local taxonomy."
            else:
                detail = "Prediction and species keyword are taxonomically distant."
            result = {
                "category": "conflict",
                "label": "Conflict",
                "detail": detail,
                "matched_keyword": kw,
                "shared_rank": shared,
            }

        if best is None or _CATEGORY_PRIORITY[result["category"]] < _CATEGORY_PRIORITY[best["category"]]:
            best = result
        if result["category"] == "match":
            return result

    return best or {
        "category": "conflict",
        "label": "Conflict",
        "detail": "Prediction and species keyword could not be matched.",
        "matched_keyword": existing_species[0],
        "shared_rank": None,
    }
