"""Adapt retained inference evidence to Vireo's shared continuity implementation."""

from .common import restore_features


def apply_continuity(photos, config, *, repair_isolated=True):
    from encounter_continuity import apply_encounter_continuity

    evidence = {}
    identities = {}
    for photo in photos:
        detections = []
        for d in photo.get("evidence", []):
            entries = []
            for source in d["sources"]:
                for q in source["predictions"]:
                    key = "taxon:" + q["taxon"][5:] if q["taxon"].startswith("inat:") else q["taxon"]
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
    restored = restore_features([dict(p) for p in photos])
    after = apply_encounter_continuity(restored, evidence, config, repair_isolated=repair_isolated)
    return [
        {
            **p,
            "species_keys": {
                **p.get("species_keys", {}),
                **{e[3]: identities[e[3]] for e in p["species_top5"] if e[3] in identities},
            },
        }
        for p in after
    ]
