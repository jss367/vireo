"""Human-readable categories for storage outside the main cache counters."""

import os
import re


def describe_auxiliary_storage(path, db_path):
    """Return a category and explanation; the original path remains visible."""
    name = os.path.basename(path)
    db_name = os.path.basename(db_path)
    if name == "backups" or (
        name.startswith(db_name + ".bak")
        or re.fullmatch(re.escape(db_name) + r"\.pre-v\d+\.bak", name)
    ):
        return "Catalog backups", "Saved catalog snapshots from upgrades or repairs."
    if path in (f"{db_path}-wal", f"{db_path}-shm", f"{db_path}-journal"):
        return "Database support files", "Live database transaction and coordination files. Keep these with the catalog."
    descriptions = {
        "originals": ("Full-resolution renders", "Generated full-resolution images, including display and paired-photo renders."),
        "staging": ("Local import originals", "Photos kept locally for review or import recovery; these may be the only remaining copies."),
        "taxonomy": ("Species reference data", "Species names and classification reference data."),
        "taxonomy.json": ("Species reference data", "Species names and classification reference data."),
        "external-edits": ("External editor files", "Images prepared for or returned from an external editor."),
        "external-dng": ("External editor files", "Converted images used by external editors."),
        "edit-masks": ("Editing masks", "Masks used by photo edits."),
        "computation-cache": ("Saved computation results", "Stored analysis results used to reuse previous computations."),
        "card_cleanup": ("Memory card cleanup records", "Records used to track memory card cleanup."),
        "inat-uploads": ("iNaturalist transfer files", "Images prepared for iNaturalist uploads and exports."),
        "inat-exports": ("iNaturalist transfer files", "Images prepared for iNaturalist uploads and exports."),
        "logs": ("Application logs", "Diagnostic logs from Vireo."),
    }
    if name in descriptions:
        return descriptions[name]
    if re.search(r"\.log(?:\.\d+)?$", name):
        return "Application logs", "Diagnostic logs from Vireo."
    return "Additional files", "Files without a dedicated storage category. Review their paths before removing anything."


def group_auxiliary_storage(entries, db_path):
    """Group measured paths without losing their individual sizes or names."""
    categories = {}
    for entry in entries:
        name, description = describe_auxiliary_storage(entry["path"], db_path)
        category = categories.setdefault(name, {
            "name": name, "size": 0, "entries": [],
        })
        category["size"] += entry["size"]
        category["entries"].append({**entry, "description": description})
    for category in categories.values():
        category["entries"].sort(key=lambda entry: (-entry["size"], entry["path"]))
    return sorted(categories.values(), key=lambda category: (-category["size"], category["name"]))
