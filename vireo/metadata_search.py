"""SQL predicates for literal search across photo metadata values.

Keep values separate: a phrase must occur in one value, not across adjacent
fields or JSON syntax. Read the catalog directly so scans, renames and edits
are searchable in the same transaction, without a second index to rebuild.
"""

PHOTO_COLUMNS = (
    "filename", "extension", "file_size", "timestamp", "width", "height",
    "rating", "flag", "camera_make", "camera_model", "lens",
    "focal_length", "aperture", "shutter_speed", "iso", "latitude", "longitude",
    "sharpness", "subject_sharpness", "quality_score", "noise_estimate",
    "subject_size", "detection_conf", "burst_id", "file_hash", "companion_path",
)

PREDICTION_COLUMNS = (
    "species", "scientific_name", "classifier_model", "confidence", "category",
    "taxonomy_kingdom", "taxonomy_phylum", "taxonomy_class", "taxonomy_order",
    "taxonomy_family", "taxonomy_genus",
)


def values_contain(columns):
    """Columns are trusted SQL expressions, never user input. Bind one LIKE."""
    return (
        "EXISTS (SELECT 1 FROM json_each(json_array("
        + ", ".join(columns)
        + ")) search_value WHERE CAST(search_value.atom AS TEXT) LIKE ? ESCAPE '\\')"
    )


def photo_metadata_predicates():
    """Photo, folder, keyword/taxon and file-tag predicates, one bind each.

    Existing relation indexes keep correlated lookups scoped to each candidate
    photo. JSON is expanded in SQLite, never loaded into Python or the browser.
    """
    photo = values_contain([f"p.{column}" for column in PHOTO_COLUMNS])
    folder = (
        "EXISTS (SELECT 1 FROM folders search_folder WHERE search_folder.id = p.folder_id AND "
        + values_contain(["search_folder.path", "search_folder.name"])
        + ")"
    )
    keyword = (
        "EXISTS (SELECT 1 FROM photo_keywords search_pk "
        "JOIN keywords search_k ON search_k.id = search_pk.keyword_id "
        "LEFT JOIN taxa search_t ON search_t.id = search_k.taxon_id "
        "WHERE search_pk.photo_id = p.id AND "
        + values_contain([
            "search_k.name", "search_k.latitude", "search_k.longitude",
            "search_t.name", "search_t.common_name", "search_t.rank", "search_t.kingdom",
        ])
        + ")"
    )
    # These cached file tags have authoritative, editable catalog counterparts.
    # Searching their stale copies would resurrect removed keywords/ratings.
    managed_paths = (
        "$.XMP.Subject", "$.XMP.HierarchicalSubject", "$.IPTC.Keywords",
        "$.XMP.Rating", "$.XMP.Label", "$.XMP.ColorLabel",
        "$.File.FileName", "$.File.Directory", "$.System.FileName", "$.System.Directory",
    )
    paths_sql = ", ".join(f"'{path}'" for path in managed_paths)
    tags = (
        "EXISTS (SELECT 1 FROM json_tree(json_remove("
        "CASE WHEN json_valid(p.exif_data) THEN p.exif_data ELSE '{}' END, "
        + paths_sql
        + ")) search_tag WHERE (CASE WHEN search_tag.type IN ('true', 'false') "
        "THEN search_tag.type ELSE CAST(search_tag.atom AS TEXT) END) LIKE ? ESCAPE '\\')"
    )
    return [photo, folder, keyword, tags]
