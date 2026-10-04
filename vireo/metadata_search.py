"""SQL predicates for literal search across photo metadata values.

Keep values separate: a phrase must occur in one value, not across adjacent
fields or JSON syntax. Read the catalog directly so scans, renames and edits
are searchable in the same transaction, without a second index to rebuild.
"""

# Values a person reads and might type. Byte counts, the content hash, burst
# ids and computed scores are left out: nobody searches for them, and their
# long digit and hex runs answered short number searches by accident.
PHOTO_COLUMNS = (
    "filename", "extension", "timestamp", "width", "height",
    "rating", "flag", "camera_make", "camera_model", "lens",
    "focal_length", "aperture", "shutter_speed", "iso", "latitude", "longitude",
    "companion_path",
)

PREDICTION_COLUMNS = (
    "species", "scientific_name", "classifier_model", "category",
    "taxonomy_kingdom", "taxonomy_phylum", "taxonomy_class", "taxonomy_order",
    "taxonomy_family", "taxonomy_genus",
)


# File-layout tags: where the embedded previews and image data sit in the
# file and how many bytes they take. Exiftool also reports the previews
# themselves as "(Binary data N bytes, ...)" placeholders.
LAYOUT_TAGS = (
    "StripOffsets", "StripByteCounts", "TileOffsets", "TileByteCounts",
    "ThumbnailOffset", "ThumbnailLength", "JpgFromRawStart", "JpgFromRawLength",
    "OtherImageStart", "OtherImageLength", "PreviewImageStart", "PreviewImageLength",
    "MPImageStart", "MPImageLength",
)


def value_matches(value, value_type, number_text=False):
    """Whether one rendered value matches the term; binds ``term_binds``.

    Text matches anywhere, so ``7688`` finds ``_D857688.NEF`` and a folder
    named ``20240712`` answers ``0712``. A number matches only where it
    starts with the term: ``7688`` finds file number 7688, not shutter count
    157688 or a coefficient of 0.0029115676880. With ``number_text``, file
    tags written as text made only of numbers ("2.86 0.177", "180 600 5.6
    6.3") count as numbers too, matching where any of them starts.
    """
    number_like = f"{value_type} IN ('integer', 'real')"
    if number_text:
        number_like += f" OR ({value_type} = 'text' AND {value} NOT GLOB '*[^0-9 .,+-]*')"
    return (
        f"(CASE WHEN {number_like} "
        f"THEN lower({value}) GLOB ? OR lower({value}) GLOB ? "
        f"ELSE {value} LIKE ? ESCAPE '\\' END)"
    )


def term_binds(like, term):
    """The binds for one ``value_matches``: number-start GLOBs, then LIKE."""
    glob = "".join(f"[{ch}]" if ch in "*?[" else ch for ch in term.lower())
    return [f"{glob}*", f"*[^0-9.]{glob}*", like]


def values_contain(columns):
    """Columns are trusted SQL expressions, never user input. Binds ``term_binds``."""
    return (
        "EXISTS (SELECT 1 FROM json_each(json_array("
        + ", ".join(columns)
        + ")) search_value WHERE "
        + value_matches("CAST(search_value.atom AS TEXT)", "search_value.type")
        + ")"
    )


# Every character SQLite can produce when it reads a JSON number back as
# text ("1.0e-05", "1.23456789012346e+19", "-Inf"), case-folded like LIKE.
_NUMBER_TEXT_CHARS = frozenset("0123456789.+-e")


def raw_text_rules_out(term):
    """Whether a row's raw EXIF text lacking ``term`` proves no value matches.

    The per-value search compares SQLite's rendering of each value, which
    can differ from the stored text. Strings differ only through escapes
    (``"\\u0068awk"`` renders as ``hawk``); the SQL takes any row holding a
    backslash down the full walk. Numbers are re-rendered outright:
    ``0.7999999999999999`` reads back as ``0.8``, ``5.0e2`` as ``500.0`` and
    ``1e999`` as ``Inf``. So the shortcut is only sound for a term no number
    can render as.
    """
    folded = term.lower()
    return not (set(folded) <= _NUMBER_TEXT_CHARS or folded in "-inf")


def photo_metadata_predicates(like, term):
    """Photo, folder, keyword/taxon and file-tag predicates with their binds.

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
    layout_sql = ", ".join(f"'{tag}'" for tag in LAYOUT_TAGS)
    tag_value = (
        "(CASE WHEN search_tag.type IN ('true', 'false') "
        "THEN search_tag.type ELSE CAST(search_tag.atom AS TEXT) END)"
    )
    tags = (
        "EXISTS (SELECT 1 FROM json_tree(json_remove("
        "CASE WHEN json_valid(p.exif_data) THEN p.exif_data ELSE '{}' END, "
        + paths_sql
        + f")) search_tag WHERE search_tag.key NOT IN ({layout_sql}) "
        f"AND {tag_value} NOT LIKE '(Binary data %' AND "
        + value_matches(tag_value, "search_tag.type", number_text=True)
        + ")"
    )
    binds = term_binds(like, term)
    tag_params = list(binds)
    if raw_text_rules_out(term):
        # Parsing and walking every photo's EXIF is nearly all of a search's
        # cost; one pass over the raw text skips the photos that cannot match.
        tags = (
            "((typeof(p.exif_data) != 'text' OR p.exif_data LIKE ? ESCAPE '\\' "
            "OR instr(p.exif_data, char(92)) > 0) AND " + tags + ")"
        )
        tag_params = [like, *binds]
    return [photo, folder, keyword, tags], [*binds, *binds, *binds, *tag_params]
