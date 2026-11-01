"""Audited, idempotent repair of known legacy common-name enrichment errors."""

import json

from species_identity import COMMON_NAME_CORRECTIONS, resolution_identity


def plan_repairs(db):
    """Only repair custom BioCLIP labels whose identity has verified evidence.

    Raw labels, confidences, prediction IDs, keywords and all review state are
    preserved. Model-native scientific labels and hybrids are outside this
    repair: a similar common name is not evidence to rewrite their identity.
    """
    repairs = db.species_identity_repairs
    changes = []
    for name, evidence in COMMON_NAME_CORRECTIONS.items():
        rows = repairs.bioclip_correction_candidates(name, evidence["scientific_name"])
        for row in rows:
            changes.append({
                **dict(row), "new_scientific_name": evidence["scientific_name"],
                "new_source_taxon_id": evidence["taxon_id"],
                "reason": "verified-common-name-correction:" + name,
            })
    return changes


def apply_repairs(db, changes, retire_artifacts=True):
    """Apply the plan atomically, retaining an in-database before/after audit.

    Optimistic row checks reject a plan if predictions changed after preview.
    The caller owns the transaction; no partial commit can escape here.
    ``retire_artifacts`` keeps corrected runs out of portable exports; pass
    False only when a fresh run in this catalog would write the corrected
    values itself, so the cached output still matches its fingerprint.
    """
    repairs = db.species_identity_repairs
    count = 0
    for change in changes:
        after = {
            "scientific_name": change["new_scientific_name"],
            "source_taxon_id": change["new_source_taxon_id"],
        }
        updated = repairs.stamp_prediction_identity(
            change["id"], *after.values(),
            scientific_name=change["scientific_name"],
            source_taxon_id=change["source_taxon_id"],
            species=change["species"],
            classifier_model=change["classifier_model"],
            labels_fingerprint=change["labels_fingerprint"],
            detection_id=change["detection_id"],
        )
        if updated != 1:
            raise ValueError(f"Prediction {change['id']} changed after the repair was planned")
        repairs.record_repair(
            change["id"], resolution_identity(), json.dumps({
                "scientific_name": change["scientific_name"],
                "source_taxon_id": change["source_taxon_id"],
            }), json.dumps(after), change["reason"],
        )
        # Do not export corrected rows under an old artifact fingerprint.
        if retire_artifacts:
            repairs.retire_classifier_runs(
                change["detection_id"], change["classifier_model"], change["labels_fingerprint"],
            )
        count += 1
    if count:
        repairs.clear_group_fingerprints()
    return count


def repair_on_upgrade(db):
    marker = "species_identity_repair:" + resolution_identity()
    if db.meta.get(marker) == "1":
        return 0
    with db.transaction():
        count = apply_repairs(db, plan_repairs(db))
        db.meta.set(marker, "1")
    return count
