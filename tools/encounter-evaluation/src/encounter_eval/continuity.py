"""Adapt retained inference evidence to Vireo's shared continuity implementation."""

from .common import restore_features


def native_evidence(photos):
    """Translate frozen raw evidence without consulting reference labels."""
    evidence = {}
    identities = {}
    for photo in photos:
        detections = []
        for d in photo.get("evidence", []):
            entries = []
            for source in d["sources"]:
                for q in source["predictions"]:
                    key = q["taxon"]
                    identities[key] = q["taxon"]
                    entries.append((q["name"], q["score"], source["model"], key))
            detections.append(
                {
                    "id": d["id"],
                    "detector_model": d["detector_model"],
                    "category": d["category"],
                    "confidence": d["detector_confidence"],
                    **{k: d["box_" + k] for k in ("x", "y", "w", "h")},
                    "predictions": entries,
                }
            )
        evidence[photo["id"]] = detections
    return evidence, identities


def apply_continuity(photos, config, *, repair_isolated=True, historical_baseline=False):
    from encounter_continuity import apply_encounter_continuity, apply_previous_continuity

    evidence, identities = native_evidence(photos)
    restored = []
    for photo in photos:
        translated = dict(photo)
        keys = photo.get("species_keys", {})
        for field in ("species_top5", "grouping_species_top5"):
            if field in photo:
                translated[field] = [
                    (*entry[:3], keys.get(entry[3], entry[3]), *entry[4:]) if len(entry) > 3 else entry
                    for entry in photo[field]
                ]
        translated["species_keys"] = {**keys, **{value: value for value in keys.values()}}
        restored.append(translated)
    restored = restore_features(restored)
    transform = apply_previous_continuity if historical_baseline else apply_encounter_continuity
    after = transform(restored, evidence, config, repair_isolated=repair_isolated)
    return [
        {
            **p,
            "species_keys": {
                **p.get("species_keys", {}),
                **{
                    e[3]: identities[e[3]]
                    for field in ("species_top5", "grouping_species_top5")
                    for e in p.get(field, []) if len(e) > 3 and e[3] in identities
                },
            },
        }
        for p in after
    ]
