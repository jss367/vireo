"""ID Conflicts page: build, assess, filter, sort and page the comparison.

The page compares each photo's species keywords against every model
prediction. Building that comparison is expensive — it resolves every
prediction through the taxonomy — and a catalog-sized collection produces
tens of thousands of rows, so the whole derived layer lives here on the
server: the browser is sent one rendered page of rows plus the counts,
never the collection.
"""

import secrets
import threading
from collections import OrderedDict

from db import text_search_match


def build_comparison(db, collection_id, photo_ids=None):
    """Return the full comparison payload for one collection.

    ``photo_ids`` narrows the build to a subset while keeping the
    collection's rules applied, so a caller can rebuild a handful of rows
    without paying for the whole collection.

    Imported here rather than at module scope so tests can patch the
    taxonomy the comparison runs against.
    """
    from compare import compare_prediction_to_keywords
    from species_identity import SpeciesResolver
    from taxonomy import load_local_taxonomy

    photos = db.get_collection_photos(
        collection_id,
        per_page=999999,
        photo_ids=photo_ids,
    )
    row_ids = [p["id"] for p in photos]
    if not row_ids:
        return {
            "models": [],
            "photos": [],
            "summary": _comparison_summary(0),
            "taxonomy_available": False,
        }

    preds = db.get_predictions(photo_ids=row_ids)
    detections_by_photo = db.get_detections_for_photos(row_ids)
    keywords_by_photo = db.get_keywords_for_photos(row_ids)
    species_identities = db.get_species_keywords_for_photos(row_ids, include_identities=True)
    species_by_photo = {pid: [entry["name"] for entry in entries]
                        for pid, entries in species_identities.items()}
    edit_recipes_by_photo = db.get_photo_edit_recipes(row_ids)
    taxonomy = load_local_taxonomy()

    names = _SpeciesNames(
        db, taxonomy, SpeciesResolver(taxonomy=taxonomy, db=db),
    )

    # Collect distinct models and build per-photo lookup
    # With multi-detection, each photo may have multiple predictions per model
    build = _ComparisonBuild(
        {
            p["id"]: _summarize_photo(
                p, edit_recipes_by_photo, keywords_by_photo, species_by_photo,
            )
            for p in photos
        },
        species_by_photo,
        taxonomy,
        names,
        compare_prediction_to_keywords,
    )
    for pid, entries in species_identities.items():
        build.by_photo[pid]["species_identity_keys"] = [entry["key"] for entry in entries]
    build.attach_detected_subjects(detections_by_photo)
    build.attach_predictions(preds)
    summary = build.summarize(len(photos))

    model_list = sorted(build.models)
    summary["models"] = len(model_list)

    return {
        "models": model_list,
        "photos": list(build.by_photo.values()),
        "summary": summary,
        "taxonomy_available": taxonomy is not None,
    }


def _comparison_summary(photo_count):
    """The comparison summary counters, all zero but the photo count."""
    return {
        "photos": photo_count,
        "models": 0,
        "matches": 0,
        "refinements": 0,
        "broader": 0,
        "conflicts": 0,
        "new": 0,
        "missing_predictions": 0,
        "needs_review": 0,
    }


def _summarize_photo(row, edit_recipes_by_photo, keywords_by_photo,
                     species_by_photo):
    row_keys = row.keys()
    def row_bool(key):
        return bool(row[key]) if key in row_keys else False

    return {
        "photo_id": row["id"],
        "filename": row["filename"],
        "timestamp": row["timestamp"],
        "rating": row["rating"],
        "flag": row["flag"],
        "wildlife_excluded": row_bool("wildlife_excluded"),
        "miss_no_subject": row_bool("miss_no_subject"),
        "miss_clipped": row_bool("miss_clipped"),
        "miss_oof": row_bool("miss_oof"),
        "width": row["width"],
        "height": row["height"],
        "edit_recipe": edit_recipes_by_photo.get(row["id"]),
        "keywords": keywords_by_photo.get(row["id"], []),
        "species_keywords": species_by_photo.get(row["id"], []),
        "predictions": {},
        "subjects": [],
        "row_category": "missing_prediction",
        "row_label": "Missing prediction",
    }


class _SpeciesNames:
    """Species-name resolution shared by every prediction in one build."""

    def __init__(self, db, taxonomy, species_resolver):
        self.db = db
        self.taxonomy = taxonomy
        self.species_resolver = species_resolver
        self._resolved_name_cache = {}
        # Resolved once for the whole build: detecting it re-reads the config and
        # scans every species keyword, and a collection can hold thousands of
        # distinct model labels that each need it.
        self.case_convention = db.species_case_convention()

    def resolved_names(self, raw_species):
        """``(comparison_name, stored_name)`` for one raw model label.

        ``resolve_species_display_name``'s last resort scans every
        species keyword to detect the case convention, and a collection
        can carry tens of thousands of predictions spread over a handful
        of distinct labels — memoize per label so that scan happens once
        each instead of once per prediction.

        The first element is the default resolution the keyword
        comparison needs: it must agree with the spelling
        ``add_keyword`` would land on. The second skips the
        case-convention invention, so it is only ever a spelling some
        keyword row actually holds or the model's own label — the only
        two things safe to show a user as a name.
        """
        key = raw_species or ""
        cached = self._resolved_name_cache.get(key)
        if cached is None:
            cached = (
                self.db.resolve_species_display_name(
                    raw_species, case_convention=self.case_convention,
                ),
                self.db.resolve_species_display_name(
                    raw_species, apply_case_convention=False,
                ),
            )
            self._resolved_name_cache[key] = cached
        return cached

    def canonical_species_key(self, raw_species, db_species):
        """One identity key per taxon, however a model spelled it.

        Different models name the same taxon differently ("Western
        Cattle-Egret" vs the outdated binomial "Bubulcus ibis"), and
        the ID Conflicts page must not read that as a model
        disagreement. Resolve each candidate name through the taxonomy
        (synonym-aware) to a taxon id.

        When nothing resolves — no local taxonomy, or neither
        spelling indexed — fall back to normalized text, preferring
        ``db_species``. That is ``resolve_species_display_name``'s
        output, which collapses a hierarchy alias onto its root
        keyword ("Desert Verdin" -> "Verdin"); the keyword comparison
        right above already treats those as one species, so keying
        off the raw spelling here would report a disagreement the
        rest of the page contradicts. Casing is irrelevant to the
        key, so the resolver's case-convention last resort cannot
        leak through this path.
        """
        for candidate in (raw_species, db_species):
            if candidate and self.taxonomy is not None:
                identity = self.species_resolver.resolve(candidate)
                if identity.taxon_id is not None:
                    return identity.key
        for candidate in (db_species, raw_species):
            if candidate:
                return str(candidate).strip().lower()
        return ""

    def canonical_display_name(self, raw_species, db_species):
        """Taxonomy-preferred display name that travels with each prediction.

        The ID Conflicts page groups predictions by ``canonical_species``,
        but the group's *displayed* name would otherwise be whichever raw
        prediction the first (alphabetical) model happened to store — so
        a persisted outdated binomial like "Bubulcus ibis" leaks into the
        consensus and multi-subject summaries even though the server
        resolved the taxon. Return the taxonomy's preferred common name
        (falling back to the current scientific name) whenever any
        candidate resolves.

        When the taxonomy misses, the identity key above falls back to
        the DB-canonical name, so two models can land in one group while
        spelling the taxon differently ("Desert Verdin" and its root
        "Verdin"). Prefer that same name here so the group's label is
        the taxon the catalog agrees on rather than whichever model
        sorted first alphabetically.

        ``db_species`` must therefore be the
        ``apply_case_convention=False`` resolution, which is only ever a
        spelling some keyword row actually holds or else the caller's
        own name. The default resolution would instead invent one by
        applying the catalog's keyword-case convention to a name it has
        never seen ("Bubulcus ibis" -> "Bubulcus Ibis") — right for
        predicting where ``add_keyword`` lands, but a spelling neither
        the model nor the catalog ever said, so it must not reach a
        user-facing label. Taking the non-inventing resolution makes
        that path unreachable from here structurally, rather than
        leaving the display to detect the rewrite after the fact (which
        cannot tell an invented re-casing from a stored row that differs
        from the model's spelling only by case).
        """
        for candidate in (raw_species, db_species):
            if candidate and self.taxonomy is not None:
                identity = self.species_resolver.resolve(candidate)
                if identity.taxon_id is not None:
                    return identity.display_name
        for candidate in (db_species, raw_species):
            if candidate:
                return str(candidate).strip()
        return ""


_ROW_PRIORITY = {
    "conflict": 6,
    "refinement": 5,
    "broader": 4,
    "new": 3,
    "missing_prediction": 2,
    "match": 1,
}
_ROW_LABELS = {
    "conflict": "Conflict",
    "refinement": "Refinement",
    "broader": "Broader",
    "new": "No species keyword",
    "missing_prediction": "Missing prediction",
    "match": "Match",
}


class _ComparisonBuild:
    """Run-wide state for one ``build_comparison`` call."""

    def __init__(self, by_photo, species_by_photo, taxonomy, names,
                 compare_prediction_to_keywords):
        self.models = set()
        self.by_photo = by_photo
        self.subjects_by_detection = {}
        self.species_by_photo = species_by_photo
        self.taxonomy = taxonomy
        self.names = names
        self.compare_prediction_to_keywords = compare_prediction_to_keywords

    def attach_detected_subjects(self, detections_by_photo):
        for pid, detections in detections_by_photo.items():
            photo = self.by_photo.get(pid)
            if photo is None:
                continue
            for det in detections:
                if (
                    det.get("category") != "animal"
                    or det.get("detector_model") == "full-image"
                ):
                    continue
                subject = {
                    "detection_id": det["id"],
                    "kind": "detected",
                    "box": {
                        "x": det["x"],
                        "y": det["y"],
                        "w": det["w"],
                        "h": det["h"],
                    },
                    "detector_confidence": det["confidence"],
                    "predictions": {},
                }
                photo["subjects"].append(subject)
                self.subjects_by_detection[det["id"]] = subject

    def attach_predictions(self, preds):
        for pr in preds:
            d = dict(pr)
            if d.get("status") == "alternative":
                continue
            pid = d["photo_id"]
            model = d["model"]
            if pid not in self.by_photo:
                continue
            subject = self._subject_for_prediction(d, pid)
            if subject is None:
                continue
            self._attach_prediction(d, pid, model, subject)

    def _subject_for_prediction(self, d, pid):
        subject = self.subjects_by_detection.get(d.get("detection_id"))
        if subject is None:
            if d.get("detector_model") != "full-image":
                # Predictions backed by a detection below the current
                # workspace threshold are intentionally dormant until the
                # user lowers that threshold. Do not let them create
                # invisible Compare conflicts.
                return None
            subject = {
                "detection_id": d["detection_id"],
                "kind": "full_image",
                "box": {"x": 0, "y": 0, "w": 1, "h": 1},
                "detector_confidence": 0,
                "predictions": {},
            }
            self.by_photo[pid]["subjects"].append(subject)
            self.subjects_by_detection[d["detection_id"]] = subject
        return subject

    def _attach_prediction(self, d, pid, model, subject):
        self.models.add(model)
        if model not in self.by_photo[pid]["predictions"]:
            self.by_photo[pid]["predictions"][model] = []
        # get_species_keywords_for_photos now canonicalizes hierarchy
        # aliases through their linked taxon's root (e.g. an attached
        # ``Desert Verdin`` leaf is reported as ``Verdin`` when a root
        # ``Verdin`` row exists). If the raw prediction label is that
        # alias and it is not present in the on-disk taxonomy JSON,
        # ``compare_prediction_to_keywords`` would fall through to its
        # exact-text fallback and flag a needless conflict because
        # ``"Verdin" != "Desert Verdin"``. Route the prediction label
        # through the same DB-side resolver so it agrees with the
        # canonical species spellings we already returned.
        comparison_prediction, stored_prediction = self.names.resolved_names(
            d["species"]
        )
        native_identity = d.get("labels_fingerprint") == "tol" or model.startswith("iNat")
        source_identity = self.names.species_resolver.prediction(d) if d.get("source_taxon_id") or native_identity else None
        existing_species = self.species_by_photo.get(pid, [])
        comparison_name = self._comparison_name(
            comparison_prediction, source_identity, existing_species,
        )
        comparison = self.compare_prediction_to_keywords(
            comparison_name, existing_species, self.taxonomy,
        )
        prediction = {
            "id": d["id"],
            "detection_id": d["detection_id"],
            "species": d["species"],
            "canonical_species": source_identity.key if source_identity else self.names.canonical_species_key(
                d["species"], comparison_prediction,
            ),
            "canonical_display": source_identity.display_name if source_identity else self.names.canonical_display_name(
                d["species"], stored_prediction,
            ),
            "confidence": d["confidence"],
            "status": d["status"],
            "category": comparison["category"],
            "category_label": comparison["label"],
            "category_detail": comparison["detail"],
            "matched_keyword": comparison["matched_keyword"],
            "shared_rank": comparison["shared_rank"],
            "box_x": d.get("box_x"),
            "box_y": d.get("box_y"),
            "box_w": d.get("box_w"),
            "box_h": d.get("box_h"),
        }
        self.by_photo[pid]["predictions"][model].append(prediction)
        subject["predictions"].setdefault(model, []).append(prediction)

    def _comparison_name(self, comparison_prediction, source_identity,
                         existing_species):
        taxonomy = self.taxonomy
        comparison_name = comparison_prediction
        scientific_name = source_identity.scientific_name if source_identity else None
        if scientific_name and taxonomy is not None and taxonomy.lookup(scientific_name):
            # Use native identity when taxonomy can compare it, retaining
            # the exact-text fallback for an unindexed confirmed label.
            exact_unindexed = any(
                kw.lower() == comparison_prediction.lower() and taxonomy.lookup(kw) is None
                for kw in existing_species
            )
            if not exact_unindexed:
                comparison_name = scientific_name
        return comparison_name

    def summarize(self, photo_count):
        summary = _comparison_summary(photo_count)
        for photo in self.by_photo.values():
            self._summarize_row(photo, summary)
        return summary

    def _summarize_row(self, photo, summary):
        highest_category = None
        pending_category = None
        for model_preds in photo["predictions"].values():
            for pred in model_preds:
                cat = pred["category"]
                if cat == "match":
                    summary["matches"] += 1
                elif cat == "refinement":
                    summary["refinements"] += 1
                elif cat == "broader":
                    summary["broader"] += 1
                elif cat == "conflict":
                    summary["conflicts"] += 1
                elif cat == "new":
                    summary["new"] += 1
                if (
                    highest_category is None
                    or _ROW_PRIORITY[cat] > _ROW_PRIORITY[highest_category]
                ):
                    highest_category = cat
                if pred.get("status") == "pending" and (
                    pending_category is None
                    or _ROW_PRIORITY[cat] > _ROW_PRIORITY[pending_category]
                ):
                    pending_category = cat
        if not photo["predictions"]:
            summary["missing_predictions"] += 1
        row_category = pending_category or highest_category or "missing_prediction"
        photo["row_category"] = row_category
        photo["row_label"] = _ROW_LABELS[row_category]
        # Any pending prediction means the user hasn't decided yet, so
        # the photo still needs review — including pending "match"
        # predictions, which classify_job stores deliberately when a
        # multi-species sidecar makes a photo-level match ambiguous
        # (see _store_pending_detection_prediction / auto_accept=False).
        photo["needs_review"] = pending_category is not None
        if photo["needs_review"]:
            summary["needs_review"] += 1


# ---------------------------------------------------------------------------
# Assessment
#
# What a row *means* — its status pill, its signal pills, whether it needs
# review — is not a property of the stored predictions alone. It depends on
# which models the user is comparing and where they put the conflict
# threshold, so it has to be derived per view rather than stored. This is the
# only implementation of those rules: the page renders what it is told.
# ---------------------------------------------------------------------------

# Which category wins when one photo's subjects disagree about what the row
# is. ``unclassified`` marks a detected subject with no predictions at all and
# shares ``missing_prediction``'s priority so a photo whose other subject
# already matched still surfaces the subject that needs classifying.
CATEGORY_ORDER = {
    "models_disagree": 8,
    "conflict": 7,
    "additional": 6,
    "refinement": 5,
    "broader": 4,
    "new": 3,
    "missing_prediction": 2,
    "unclassified": 2,
    "match": 1,
    "low_conflict": 0,
}

CATEGORY_LABELS = {
    "models_disagree": "Models disagree about subject",
    "conflict": "Conflict",
    "additional": "Additional species suggested",
    "refinement": "Refinement",
    "broader": "Broader",
    "new": "No species keyword",
    "missing_prediction": "Missing prediction",
    "match": "Match",
    "low_conflict": "Below threshold",
    "unclassified": "Unclassified subject",
}

DEFAULT_MIN_CONFIDENCE = 0.4

# Keyword comparison outcomes that support the photo's existing species
# keyword rather than contradicting it.
_SUPPORTING = ("match", "refinement", "broader")


def _text_key(value):
    """Sort key for user-visible text, case-insensitive with a stable tail."""
    text = "" if value is None else str(value)
    return (text.casefold(), text)


def effective_category(pred, min_confidence):
    """The prediction's category once the conflict threshold is applied.

    A low-confidence conflict is not evidence of anything, so the threshold
    demotes it to ``low_conflict`` — visible, but not counted as a conflict
    and not driving the row's status.
    """
    category = pred.get("category") or "missing_prediction"
    if category == "conflict" and (pred.get("confidence") or 0) < min_confidence:
        return "low_conflict"
    return category


def _usable(pred):
    return pred.get("status") != "rejected"


def _identity(pred):
    """One key per taxon, however a model spelled it."""
    canonical = pred.get("canonical_species")
    if canonical:
        return canonical
    return str(pred.get("species") or "").strip().lower()


def _best_for_model(subject, model):
    preds = [p for p in (subject.get("predictions") or {}).get(model, []) if _usable(p)]
    if not preds:
        return None
    return sorted(
        preds,
        key=lambda p: (-(p.get("confidence") or 0), _text_key(p.get("species"))),
    )[0]


def subject_signal(subject, models, min_confidence):
    """Model agreement and keyword conflict evidence for one detected subject."""
    top_preds = []
    all_preds = []
    missing = 0
    keyword_conflict_score = 0.0
    keyword_conflict_count = 0
    max_keyword_conflict_confidence = 0.0
    keyword_support_score = 0.0
    keyword_support_count = 0
    saw_low_conflict = False
    max_confidence = 0.0
    min_conf_seen = None

    for model in models:
        preds = [
            p for p in (subject.get("predictions") or {}).get(model, [])
            if _usable(p)
        ]
        if not preds:
            missing += 1
            continue
        for pred in preds:
            all_preds.append(pred)
            confidence = pred.get("confidence") or 0
            category = effective_category(pred, min_confidence)
            if category == "low_conflict":
                saw_low_conflict = True
            elif category == "conflict":
                keyword_conflict_count += 1
                keyword_conflict_score += confidence
                max_keyword_conflict_confidence = max(
                    max_keyword_conflict_confidence, confidence,
                )
            elif pred.get("category") in _SUPPORTING:
                keyword_support_count += 1
                keyword_support_score += confidence
        best = _best_for_model(subject, model)
        if best is None:
            continue
        conf = best.get("confidence") or 0
        top_preds.append({
            "model": model,
            "species": best.get("species") or "",
            # Server-resolved taxonomy name, so a consensus group is labelled
            # by the taxon rather than by whichever model sorted first.
            "display": best.get("canonical_display") or best.get("species") or "",
            "identity": _identity(best),
            "confidence": conf,
        })
        max_confidence = max(max_confidence, conf)
        min_conf_seen = conf if min_conf_seen is None else min(min_conf_seen, conf)

    groups = {}
    order = []
    for item in top_preds:
        key = item["identity"]
        group = groups.get(key)
        if group is None:
            groups[key] = {"species": item["display"], "count": 0, "confidence": 0.0}
            order.append(key)
            group = groups[key]
        elif not group["species"] and item["display"]:
            group["species"] = item["display"]
        group["count"] += 1
        group["confidence"] += item["confidence"]
    ranked = sorted(
        (groups[key] for key in order),
        key=lambda g: (-g["count"], -g["confidence"], _text_key(g["species"])),
    )

    disagreement_pair = None
    disagreement_score = 0.0
    for i in range(len(top_preds)):
        for j in range(i + 1, len(top_preds)):
            if top_preds[i]["identity"] == top_preds[j]["identity"]:
                continue
            # A disagreement is only as strong as its least confident side.
            pair_score = min(
                top_preds[i]["confidence"] or 0, top_preds[j]["confidence"] or 0,
            ) * 100
            if disagreement_pair is None or pair_score > disagreement_score:
                disagreement_pair = [top_preds[i], top_preds[j]]
                disagreement_score = pair_score

    return {
        "all_predictions": all_preds,
        "top_predictions": top_preds,
        "top_prediction_count": len(top_preds),
        "unique_top_species_count": len(ranked),
        "missing_visible_model_count": missing,
        "model_disagreement_score": disagreement_score,
        "model_disagreement_pair": disagreement_pair,
        "keyword_conflict_count": keyword_conflict_count,
        "keyword_conflict_score": keyword_conflict_score * 100,
        "max_keyword_conflict_confidence": max_keyword_conflict_confidence,
        "keyword_support_count": keyword_support_count,
        "keyword_support_score": keyword_support_score * 100,
        "consensus_species": ranked[0]["species"] if ranked else "",
        "consensus_count": ranked[0]["count"] if ranked else 0,
        "max_confidence": max_confidence,
        "min_confidence": min_conf_seen,
        "saw_low_conflict": saw_low_conflict,
    }


def _subject_status(signal, index, supporting, detected_count, min_confidence):
    has_pending = any(
        pred.get("status") == "pending" for pred in signal["all_predictions"]
    )
    label = None
    if not signal["all_predictions"]:
        category = "unclassified"
    elif signal["model_disagreement_score"] > 0:
        category = "models_disagree"
    elif signal["keyword_support_count"] > 0:
        supported = sorted(
            (
                pred for pred in signal["all_predictions"]
                if pred.get("category") in _SUPPORTING
            ),
            key=lambda pred: -CATEGORY_ORDER.get(pred.get("category"), 0),
        )
        category = supported[0].get("category") if supported else "match"
    else:
        another_subject_accounts_for_keyword = any(
            value for other, value in enumerate(supporting) if other != index
        )
        if (
            detected_count > 1
            and another_subject_accounts_for_keyword
            and signal["keyword_conflict_count"] > 0
            and signal["consensus_species"]
        ):
            # The photo's species keyword is explained by a *different*
            # subject, so this one is a candidate second species rather than a
            # contradiction of the keyword.
            category = "additional"
            label = (
                "Additional species suggested"
                if signal["consensus_count"] >= 2
                else "Possible additional species"
            )
        elif signal["keyword_conflict_count"] > 0:
            category = "conflict"
        elif signal["saw_low_conflict"]:
            category = "low_conflict"
        else:
            category = None
            if signal["all_predictions"]:
                category = effective_category(
                    signal["all_predictions"][0], min_confidence,
                )
            category = category or "unclassified"
    return {
        "category": category,
        "label": label or CATEGORY_LABELS.get(category, category),
        "needs_review": has_pending,
        "has_prediction": bool(signal["all_predictions"]),
    }


def assess_photo(photo, models, min_confidence):
    """Derive one photo's per-subject statuses, row status and signal roll-up.

    Returns ``{"statuses": [...], "signals": [...], "status": {...},
    "signal": {...}}``. ``status`` is the row's headline category; ``signal``
    rolls the subject evidence up to the row so the page can rank and label
    it without re-deriving anything.
    """
    subjects = photo.get("subjects") or []
    signals = [subject_signal(subject, models, min_confidence) for subject in subjects]
    detected_count = sum(
        1 for subject in subjects if subject.get("kind") != "full_image"
    )
    supporting = [signal["keyword_support_count"] > 0 for signal in signals]
    statuses = [
        _subject_status(signal, index, supporting, detected_count, min_confidence)
        for index, signal in enumerate(signals)
    ]
    return {
        "subjects": subjects,
        "signals": signals,
        "statuses": statuses,
        "status": _row_status(statuses),
        "signal": _row_signal(subjects, signals, statuses),
    }


def _row_status(statuses):
    """The one category that describes the row.

    Unclassified subjects stay in the running alongside pending ones: a
    subject with no predictions at all has nothing to mark reviewed, but it
    still needs classifying, so a multi-subject photo must not render as the
    pending match that happens to sit next to it.
    """
    candidates = [
        status for status in statuses
        if status["needs_review"] or status["category"] == "unclassified"
    ]
    if not candidates:
        candidates = list(statuses)
    if not candidates:
        return {
            "category": "missing_prediction",
            "label": CATEGORY_LABELS["missing_prediction"],
            "needs_review": False,
            "has_prediction": False,
        }
    chosen = sorted(
        candidates, key=lambda s: -CATEGORY_ORDER.get(s["category"], 0),
    )[0]
    return {
        "category": chosen["category"],
        "label": chosen["label"],
        "needs_review": any(status["needs_review"] for status in statuses),
        "has_prediction": any(status["has_prediction"] for status in statuses),
    }


def _row_signal(subjects, signals, statuses):
    missing = 0
    keyword_conflict_score = 0.0
    keyword_conflict_count = 0
    max_keyword_conflict_confidence = 0.0
    keyword_support_score = 0.0
    max_confidence = 0.0
    min_confidence_seen = None
    disagreement_pair = None
    disagreement_subject = None
    model_disagreement_score = 0.0
    consensus_species = ""
    consensus_count = 0
    consensus_subject = None
    max_top_prediction_count = 0
    max_unique_species = 0
    additional_count = 0

    for index, signal in enumerate(signals):
        status = statuses[index]
        missing += signal["missing_visible_model_count"]
        keyword_support_score += signal["keyword_support_score"]
        max_confidence = max(max_confidence, signal["max_confidence"] or 0)
        if signal["min_confidence"] is not None:
            min_confidence_seen = (
                signal["min_confidence"] if min_confidence_seen is None
                else min(min_confidence_seen, signal["min_confidence"])
            )
        max_top_prediction_count = max(
            max_top_prediction_count, signal["top_prediction_count"],
        )
        max_unique_species = max(
            max_unique_species, signal["unique_top_species_count"],
        )
        if signal["consensus_count"] > consensus_count:
            consensus_count = signal["consensus_count"]
            consensus_species = signal["consensus_species"]
            consensus_subject = index
        if signal["model_disagreement_score"] > model_disagreement_score:
            model_disagreement_score = signal["model_disagreement_score"]
            disagreement_pair = signal["model_disagreement_pair"]
            disagreement_subject = index
        if status["category"] == "additional":
            additional_count += 1
        else:
            # An "additional species" subject is not disagreeing with the
            # keyword — another subject already accounts for it — so its
            # conflict evidence must not inflate the row's keyword conflict.
            keyword_conflict_count += signal["keyword_conflict_count"]
            keyword_conflict_score += signal["keyword_conflict_score"]
            max_keyword_conflict_confidence = max(
                max_keyword_conflict_confidence,
                signal["max_keyword_conflict_confidence"],
            )

    multi_subject = len(subjects) > 1
    # The strongest disagreement and the strongest consensus are often
    # different subjects of the same photo; an unlabelled "X vs Y ·
    # Consensus: Z" reads as a contradiction, so name the subject.
    disagreement_prefix = (
        f"Subject {disagreement_subject + 1}: "
        if multi_subject and disagreement_subject is not None else ""
    )
    if disagreement_pair:
        first = disagreement_pair[0]["display"] or disagreement_pair[0]["species"]
        second = disagreement_pair[1]["display"] or disagreement_pair[1]["species"]
        model_disagreement_label = f"{disagreement_prefix}{first} vs {second}"
    else:
        model_disagreement_label = ""
    if consensus_count >= 2 and consensus_species:
        consensus_prefix = (
            f"Subject {consensus_subject + 1}: "
            if multi_subject and consensus_subject is not None else ""
        )
        consensus_label = (
            f"{consensus_prefix}{consensus_count} models agree on {consensus_species}"
        )
    else:
        consensus_label = ""
    keyword_disagreement_label = (
        f"{keyword_conflict_count} model conflict"
        f"{'' if keyword_conflict_count == 1 else 's'}"
        if keyword_conflict_count else ""
    )

    return {
        "top_prediction_count": max_top_prediction_count,
        "unique_top_species_count": max_unique_species,
        "missing_visible_model_count": missing,
        "model_disagreement_score": model_disagreement_score,
        "model_disagreement_pair": disagreement_pair,
        "keyword_conflict_count": keyword_conflict_count,
        "keyword_conflict_score": keyword_conflict_score,
        "max_keyword_conflict_confidence": max_keyword_conflict_confidence,
        "keyword_support_score": keyword_support_score,
        "consensus_species": consensus_species,
        "consensus_count": consensus_count,
        "consensus_subject": consensus_subject,
        "additional_subject_count": additional_count,
        "max_confidence": max_confidence,
        "min_confidence": min_confidence_seen,
        "model_disagreement_label": model_disagreement_label,
        "consensus_label": consensus_label,
        "keyword_disagreement_label": keyword_disagreement_label,
    }


# ---------------------------------------------------------------------------
# Filters, sorts and exclusions
# ---------------------------------------------------------------------------

FILTERS = [
    ("model_disagreement", "Models disagree"),
    ("strong_model_disagreement", "Strong model disagreement"),
    ("keyword_model_conflict", "Keyword vs models"),
    ("strong_keyword_model_conflict", "Strong keyword conflict"),
    ("consensus_conflict", "Model consensus conflicts"),
    ("all_models_agree", "Shown models agree"),
    ("missing_visible_model", "Missing shown model"),
    ("two_models", "Two+ model predictions"),
    ("needs_review", "Needs review"),
    ("additional", "Additional species"),
    ("conflict", "Conflicts"),
    ("refinement", "Refinements"),
    ("broader", "Broader"),
    ("new", "No species keyword"),
    ("missing_prediction", "Missing predictions"),
    ("match", "Matches"),
    ("reviewed", "Reviewed"),
    ("all", "All"),
]

FILTER_IDS = [item[0] for item in FILTERS]
_FILTER_BIT = {name: 1 << index for index, name in enumerate(FILTER_IDS)}

SORTS = [
    ("review_priority", "Review priority"),
    ("model_disagreement", "Model disagreement"),
    ("keyword_disagreement", "Keyword disagreement"),
    ("consensus_conflict", "Consensus conflict"),
    ("top_confidence", "Top confidence"),
    ("low_confidence", "Low confidence"),
    ("filename", "Filename"),
    ("newest", "Newest"),
    ("oldest", "Oldest"),
]

SORT_IDS = [item[0] for item in SORTS]

EXCLUDES = [
    ("rejected", "Hide rejects"),
    ("picks", "Hide picks"),
    ("unflagged", "Hide unflagged"),
    ("not_wildlife", "Hide not wildlife"),
    ("misses", "Hide marked misses"),
    ("multi_species", "Hide multi-species"),
]

EXCLUDE_IDS = [item[0] for item in EXCLUDES]
_EXCLUDE_BIT = {name: 1 << index for index, name in enumerate(EXCLUDE_IDS)}

# Summary tiles are counted per subject status, so a two-subject photo that
# conflicts twice counts twice — the tiles describe the work, not the rows.
_TILE_CATEGORIES = ("match", "refinement", "broader", "conflict", "additional", "new")

# Subject-status category -> the summary tile it feeds.
_TILE_KEYS = {
    "match": "matches",
    "refinement": "refinements",
    "broader": "broader",
    "conflict": "conflicts",
    "additional": "additional",
    "new": "new",
}


class IndexRecord:
    """One photo, reduced to what filtering, sorting and counting need.

    Building the comparison for a catalog-sized collection is expensive, so
    the result is reduced to this and kept; the page's full rows are rebuilt
    on demand for the handful of photos actually on screen.
    """

    __slots__ = (
        "photo_id", "filename", "timestamp", "category", "needs_review",
        "has_unclassified", "exclude_mask", "filter_mask", "tile_counts",
        "model_disagreement_score", "keyword_conflict_count",
        "keyword_conflict_score", "max_keyword_conflict_confidence",
        "consensus_count", "top_prediction_count", "unique_top_species_count",
        "missing_visible_model_count", "additional_subject_count",
        "max_confidence", "min_confidence", "search_text",
    )


def _exclude_mask(photo):
    flag = photo.get("flag") or "none"
    mask = 0
    if flag == "rejected":
        mask |= _EXCLUDE_BIT["rejected"]
    if flag == "flagged":
        mask |= _EXCLUDE_BIT["picks"]
    if flag == "none":
        mask |= _EXCLUDE_BIT["unflagged"]
    if photo.get("wildlife_excluded"):
        mask |= _EXCLUDE_BIT["not_wildlife"]
    if photo.get("miss_no_subject") or photo.get("miss_clipped") or photo.get("miss_oof"):
        mask |= _EXCLUDE_BIT["misses"]
    return mask


def _is_multi_species(photo, assessment):
    """Whether the photo holds, or is suggested to hold, two or more species.

    Two distinct species keywords make it multi-species outright; so does a
    subject the shown models read as an additional species, because that
    photo is a second-species decision rather than a one-species one.
    """
    species = set(photo.get("species_identity_keys", photo.get("species_keywords") or []))
    return len(species) > 1 or assessment["signal"]["additional_subject_count"] > 0


def _filter_mask(record):
    mask = _FILTER_BIT["all"]
    if record.model_disagreement_score > 0:
        mask |= _FILTER_BIT["model_disagreement"]
    if record.model_disagreement_score >= 70:
        mask |= _FILTER_BIT["strong_model_disagreement"]
    if record.keyword_conflict_count > 0:
        mask |= _FILTER_BIT["keyword_model_conflict"]
    if (
        record.keyword_conflict_score >= 100
        or record.max_keyword_conflict_confidence >= 0.75
    ):
        mask |= _FILTER_BIT["strong_keyword_model_conflict"]
    if record.consensus_count >= 2 and record.keyword_conflict_count > 0:
        mask |= _FILTER_BIT["consensus_conflict"]
    if (
        record.top_prediction_count >= 2
        and record.unique_top_species_count == 1
        and record.missing_visible_model_count == 0
    ):
        mask |= _FILTER_BIT["all_models_agree"]
    if record.missing_visible_model_count > 0:
        mask |= _FILTER_BIT["missing_visible_model"]
    if record.top_prediction_count >= 2:
        mask |= _FILTER_BIT["two_models"]
    if record.needs_review:
        mask |= _FILTER_BIT["needs_review"]
    if record.additional_subject_count > 0:
        mask |= _FILTER_BIT["additional"]
    # Detected subjects with no predictions surface as ``unclassified``
    # statuses; photos with no compare subjects at all fall back to the
    # ``missing_prediction`` row category. Both need a prediction.
    if record.has_unclassified or record.category == "missing_prediction":
        mask |= _FILTER_BIT["missing_prediction"]
    for name in ("conflict", "refinement", "broader", "new", "match"):
        if record.category == name:
            mask |= _FILTER_BIT[name]
    return mask


def _search_text(photo, assessment):
    """Everything the page shows about a row, joined for substring search.

    Fields are joined with newlines, which are not word characters, so
    whole-word matching behaves exactly as it does against the separate
    fields the browser used to search.
    """
    status = assessment["status"]
    signal = assessment["signal"]
    parts = [
        photo.get("filename") or "",
        status["label"],
        signal["model_disagreement_label"],
        signal["keyword_disagreement_label"],
        signal["consensus_species"],
    ]
    for keyword in photo.get("keywords") or []:
        parts.append(keyword.get("name") or "")
        parts.append(keyword.get("type") or "")
    for model, preds in (photo.get("predictions") or {}).items():
        parts.append(model)
        for pred in preds:
            parts.append(pred.get("species") or "")
            parts.append(pred.get("category_label") or "")
            parts.append(pred.get("matched_keyword") or "")
            parts.append(pred.get("status") or "")
    return "\n".join(part for part in parts if part)


def index_record(photo, assessment):
    record = IndexRecord()
    status = assessment["status"]
    signal = assessment["signal"]
    record.photo_id = photo["photo_id"]
    record.filename = photo.get("filename") or ""
    record.timestamp = photo.get("timestamp") or ""
    record.category = status["category"]
    record.needs_review = bool(status["needs_review"])
    record.has_unclassified = any(
        item["category"] == "unclassified" for item in assessment["statuses"]
    )
    record.model_disagreement_score = signal["model_disagreement_score"]
    record.keyword_conflict_count = signal["keyword_conflict_count"]
    record.keyword_conflict_score = signal["keyword_conflict_score"]
    record.max_keyword_conflict_confidence = signal["max_keyword_conflict_confidence"]
    record.consensus_count = signal["consensus_count"]
    record.top_prediction_count = signal["top_prediction_count"]
    record.unique_top_species_count = signal["unique_top_species_count"]
    record.missing_visible_model_count = signal["missing_visible_model_count"]
    record.additional_subject_count = signal["additional_subject_count"]
    record.max_confidence = signal["max_confidence"]
    record.min_confidence = signal["min_confidence"]
    counts = dict.fromkeys(_TILE_CATEGORIES, 0)
    for item in assessment["statuses"]:
        if item["category"] in counts:
            counts[item["category"]] += 1
    record.tile_counts = tuple(counts[name] for name in _TILE_CATEGORIES)
    record.exclude_mask = _exclude_mask(photo)
    if _is_multi_species(photo, assessment):
        record.exclude_mask |= _EXCLUDE_BIT["multi_species"]
    # ``reviewed`` reads every stored prediction, not just the shown models:
    # the filter answers "has this photo been dealt with", which does not
    # change because the user narrowed the comparison to one model.
    reviewed = any(
        pred.get("status") == "reviewed"
        for preds in (photo.get("predictions") or {}).values()
        for pred in preds
    )
    record.filter_mask = _filter_mask(record)
    if reviewed:
        record.filter_mask |= _FILTER_BIT["reviewed"]
    record.search_text = _search_text(photo, assessment)
    return record


# ---------------------------------------------------------------------------
# Selection: filter, search, sort and page the index
# ---------------------------------------------------------------------------

def _sort_key(sort):
    if sort == "model_disagreement":
        return lambda r: (
            -r.model_disagreement_score, -r.top_prediction_count,
            -r.max_confidence, _text_key(r.filename),
        )
    if sort == "keyword_disagreement":
        return lambda r: (
            -r.keyword_conflict_score, -r.max_keyword_conflict_confidence,
            -CATEGORY_ORDER.get(r.category, 0), _text_key(r.filename),
        )
    if sort == "consensus_conflict":
        return lambda r: (
            0 if (r.consensus_count >= 2 and r.keyword_conflict_count > 0) else 1,
            -r.consensus_count, -r.keyword_conflict_score, _text_key(r.filename),
        )
    if sort == "top_confidence":
        return lambda r: (-r.max_confidence, _text_key(r.filename))
    if sort == "low_confidence":
        # A row with no top prediction has no confidence to be low, so it sorts
        # to the end. A stored confidence of exactly 0.0 is a real reading and
        # the least confident one there is, so it has to be told apart from
        # that absence rather than falsified by it.
        return lambda r: (
            999 if r.min_confidence is None else r.min_confidence,
            _text_key(r.filename),
        )
    if sort == "filename":
        return lambda r: _text_key(r.filename)
    return lambda r: (
        0 if r.needs_review else 1,
        -CATEGORY_ORDER.get(r.category, 0),
        -r.keyword_conflict_score,
        -r.model_disagreement_score,
        _text_key(r.filename),
    )


def sort_records(records, sort):
    """Order records the way the page's sort control describes.

    ``newest``/``oldest`` sort twice rather than negating a string key:
    Python's sort is stable, so the filename pass survives as the tiebreak
    of the timestamp pass.
    """
    if sort in ("newest", "oldest"):
        rows = sorted(records, key=lambda r: _text_key(r.filename))
        rows.sort(key=lambda r: _text_key(r.timestamp), reverse=(sort == "newest"))
        return rows
    return sorted(records, key=_sort_key(sort))


def _query_tokens(query):
    return [token for token in str(query or "").split() if token]


def _matches_query(record, tokens, match_case, whole_word):
    # Folded on demand rather than stored folded: a second copy of every
    # row's searchable text costs more memory than the folding costs time,
    # and the search box is debounced.
    return all(
        text_search_match(record.search_text, token, match_case, whole_word)
        for token in tokens
    )


class Selection:
    """One page of rows plus every count the page displays."""

    __slots__ = ("photo_ids", "total", "page", "summary", "filter_counts",
                 "exclusion_counts")


def select(
    records,
    models,
    filter_id="all",
    excludes=(),
    query="",
    match_case=False,
    whole_word=False,
    sort="review_priority",
    page=1,
    per_page=60,
):
    """Filter, sort and page the index, and count everything the page shows.

    The counts deliberately have different scopes, matching what each control
    claims: the summary tiles describe the whole collection, the exclusion
    chips count what each exclusion would hide, the filter chips count what
    each filter would show *after* exclusions, and ``total`` is the rows
    actually listed — exclusions, filter and search together.
    """
    exclude_mask = 0
    for name in excludes:
        exclude_mask |= _EXCLUDE_BIT.get(name, 0)
    wanted = _FILTER_BIT.get(filter_id, _FILTER_BIT["all"])
    tokens = _query_tokens(query)

    summary = {
        "photos": len(records),
        "models": len(models),
        "needs_review": 0,
        "missing_predictions": 0,
        "model_disagreements": 0,
        "keyword_model_conflicts": 0,
    }
    for name in _TILE_CATEGORIES:
        summary[_TILE_KEYS[name]] = 0
    tile_keys = [_TILE_KEYS[name] for name in _TILE_CATEGORIES]
    exclusion_counts = dict.fromkeys(EXCLUDE_IDS, 0)
    filter_counts = dict.fromkeys(FILTER_IDS, 0)
    matched = []

    for record in records:
        # Summary tiles and signal counters describe the collection, so they
        # ignore the exclusions and the search box — exactly as the page
        # counted them when it held every row.
        if record.needs_review:
            summary["needs_review"] += 1
        if record.has_unclassified or record.category == "missing_prediction":
            summary["missing_predictions"] += 1
        if record.model_disagreement_score > 0:
            summary["model_disagreements"] += 1
        if record.keyword_conflict_count > 0:
            summary["keyword_model_conflicts"] += 1
        for key, count in zip(tile_keys, record.tile_counts, strict=True):
            if count:
                summary[key] += count
        for name in EXCLUDE_IDS:
            if record.exclude_mask & _EXCLUDE_BIT[name]:
                exclusion_counts[name] += 1
        if record.exclude_mask & exclude_mask:
            continue
        mask = record.filter_mask
        for index, name in enumerate(FILTER_IDS):
            if mask & (1 << index):
                filter_counts[name] += 1
        if not (mask & wanted):
            continue
        if tokens and not _matches_query(record, tokens, match_case, whole_word):
            continue
        matched.append(record)

    ordered = sort_records(matched, sort)
    per_page = max(1, per_page)
    total = len(ordered)
    # Clamp the page to the last one that still holds a row. A decision that
    # removes the last matching row on the final page would otherwise leave
    # the caller past the end — the slice would come back empty and the
    # pager would read "page 3 of 2" until the user paged back manually.
    last_page = max(1, (total + per_page - 1) // per_page)
    page = min(last_page, max(1, page))
    start = (page - 1) * per_page
    result = Selection()
    result.photo_ids = [record.photo_id for record in ordered[start:start + per_page]]
    result.total = total
    result.page = page
    result.summary = summary
    result.filter_counts = filter_counts
    result.exclusion_counts = exclusion_counts
    return result


# ---------------------------------------------------------------------------
# Snapshots
#
# The page has always worked from a snapshot: the browser used to fetch every
# row once and then filter, sort and count that copy until the user reloaded.
# Keeping the snapshot here instead changes nothing about how current the
# numbers are — it just means the browser is no longer the thing holding a
# catalog in memory. Decisions made on the page patch the snapshot, so the
# counts follow the work being done.
# ---------------------------------------------------------------------------

# Each snapshot of a catalog-sized collection costs tens of megabytes, so
# only the current comparison and the one before it are kept.
_MAX_SNAPSHOTS = 2


class Snapshot:
    __slots__ = ("token", "collection_id", "workspace_id", "models",
                 "all_models", "min_confidence", "taxonomy_available",
                 "records", "by_id", "_lock")

    def __init__(self, token, collection_id, workspace_id, models, all_models,
                 min_confidence, taxonomy_available, records):
        self.token = token
        self.collection_id = collection_id
        self.workspace_id = workspace_id
        self.models = models
        self.all_models = all_models
        self.min_confidence = min_confidence
        self.taxonomy_available = taxonomy_available
        self.records = records
        self.by_id = {record.photo_id: index for index, record in enumerate(records)}
        self._lock = threading.Lock()

    def matches(self, collection_id, workspace_id, models, min_confidence):
        return (
            self.collection_id == collection_id
            and self.workspace_id == workspace_id
            and self.models == models
            and self.min_confidence == min_confidence
        )

    def patch(self, db, photo_ids):
        """Rebuild the given photos in place after a decision changed them.

        A photo whose keywords no longer satisfy the collection's rules is
        dropped, which is how a row leaves the list once it has been dealt
        with. A previously-unindexed sibling whose keywords now do satisfy
        them is appended, so a decision that pulls a grouped photo into
        the collection is reflected in the same request rather than waiting
        for a full reload.

        Returns True when one of those siblings carried a model the snapshot
        had never seen. Everything here was derived under the old inventory,
        so that answer is the one thing a patch cannot repair in place: the
        caller has to throw the snapshot away and derive a new one.
        """
        # Two decisions can land at once. Serialize the rebuilds so the
        # second reads the list the first produced rather than overwriting it
        # with a copy taken before it ran. Readers never take the lock: the
        # swap below is a single rebind, so a request either counts the whole
        # old list or the whole new one.
        with self._lock:
            wanted = list(dict.fromkeys(photo_ids))
            if not wanted:
                return False
            built = build_comparison(db, self.collection_id, photo_ids=wanted)
            rebuilt = {}
            for photo in built["photos"]:
                assessment = assess_photo(photo, self.models, self.min_confidence)
                rebuilt[photo["photo_id"]] = index_record(photo, assessment)
            wanted_set = set(wanted)
            previously_indexed = set(self.by_id)
            records = [
                rebuilt.get(record.photo_id, record)
                for record in self.records
                if record.photo_id not in wanted_set or record.photo_id in rebuilt
            ]
            # A sibling outside the snapshot can now satisfy a keyword-based
            # collection rule after a grouped decision. Append the ones the
            # build returned so the counts, filters and pager pick them up.
            for pid in wanted:
                if pid in previously_indexed:
                    continue
                record = rebuilt.get(pid)
                if record is not None:
                    records.append(record)
            self.records = records
            self.by_id = {
                record.photo_id: index for index, record in enumerate(records)
            }
            # A sibling can also carry a model no photo in the snapshot had.
            # Every status, every agreement reading and every filter count in
            # here was derived without that model, so the snapshot can no
            # longer answer for the collection it now describes. Say so and
            # let the caller replace it; quietly widening the inventory would
            # only let the page draw a column whose numbers came from an
            # assessment that never looked at it.
            return not set(built["models"]).issubset(self.all_models)


class SnapshotStore:
    """A tiny bounded cache of comparison snapshots, keyed by opaque token."""

    def __init__(self, limit=_MAX_SNAPSHOTS):
        self._limit = limit
        self._lock = threading.Lock()
        self._snapshots = OrderedDict()

    def get(self, token):
        if not token:
            return None
        with self._lock:
            snapshot = self._snapshots.get(token)
            if snapshot is not None:
                self._snapshots.move_to_end(token)
            return snapshot

    def put(self, snapshot):
        with self._lock:
            self._snapshots[snapshot.token] = snapshot
            self._snapshots.move_to_end(snapshot.token)
            while len(self._snapshots) > self._limit:
                self._snapshots.popitem(last=False)
        return snapshot

    def discard(self, token):
        with self._lock:
            self._snapshots.pop(token, None)

    def clear(self):
        with self._lock:
            self._snapshots.clear()


def resolve_models(all_models, requested):
    """The models the page is comparing, in the order it will show them.

    An explicit request is taken at face value rather than intersected with
    what the photos in hand happen to carry: "this model predicted nothing
    here" is exactly the signal the Missing shown model filter reports, and
    dropping the model from the comparison would erase it.
    """
    if not requested:
        return list(all_models)
    return list(dict.fromkeys(requested))


def build_snapshot(db, collection_id, workspace_id, models=None,
                   min_confidence=DEFAULT_MIN_CONFIDENCE):
    built = build_comparison(db, collection_id)
    visible = resolve_models(built["models"], models)
    records = []
    for photo in built["photos"]:
        assessment = assess_photo(photo, visible, min_confidence)
        records.append(index_record(photo, assessment))
    return Snapshot(
        token=secrets.token_urlsafe(16),
        collection_id=collection_id,
        workspace_id=workspace_id,
        models=visible,
        all_models=built["models"],
        min_confidence=min_confidence,
        taxonomy_available=built["taxonomy_available"],
        records=records,
    )


def page_rows(db, collection_id, photo_ids, models, min_confidence):
    """Full rows for the photos on screen, in the order asked for.

    Rebuilt from the database rather than served from the snapshot: it costs
    milliseconds for a page of rows, and it means the rows the user acts on
    are current even when the counts beside them came from the snapshot.
    """
    if not photo_ids:
        return []
    built = build_comparison(db, collection_id, photo_ids=list(photo_ids))
    by_id = {photo["photo_id"]: photo for photo in built["photos"]}
    rows = []
    for photo_id in photo_ids:
        photo = by_id.get(photo_id)
        if photo is None:
            continue
        rows.append(attach_assessment(photo, models, min_confidence))
    return rows


def attach_assessment(photo, models, min_confidence):
    """Annotate one row with everything the page renders it from."""
    assessment = assess_photo(photo, models, min_confidence)
    photo["status"] = assessment["status"]
    photo["signal"] = assessment["signal"]
    photo["subject_statuses"] = assessment["statuses"]
    for index, subject in enumerate(photo.get("subjects") or []):
        subject["status"] = assessment["statuses"][index]
        subject["signal_consensus_species"] = (
            assessment["signals"][index]["consensus_species"]
        )
    for preds in (photo.get("predictions") or {}).values():
        for pred in preds:
            pred["effective_category"] = effective_category(pred, min_confidence)
    photo["best_prediction_id"] = _best_prediction_id(
        photo, models, min_confidence, assessment,
    )
    return photo


def _best_prediction_id(photo, models, min_confidence, assessment):
    """The prediction a batch action would apply to this row, or None.

    Multi-subject photos have no single answer — the page disables their
    checkbox and asks the user to act per subject — so they return None
    rather than a guess.
    """
    if len(assessment["subjects"]) > 1:
        return None
    best = None
    best_score = None
    for model in models:
        for pred in (photo.get("predictions") or {}).get(model, []):
            if pred.get("status") and pred.get("status") != "pending":
                continue
            score = CATEGORY_ORDER.get(effective_category(pred, min_confidence), 0)
            if (
                best is None
                or score > best_score
                or (score == best_score
                    and (pred.get("confidence") or 0) > (best.get("confidence") or 0))
            ):
                best = pred
                best_score = score
    return best["id"] if best else None
