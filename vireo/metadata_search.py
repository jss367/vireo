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


# File size and layout tags: where embedded previews and image data sit
# in the file and how many bytes they take. Exiftool also reports the previews
# themselves as "(Binary data N bytes, ...)" placeholders.
LAYOUT_TAGS = (
    "FileSize", "StripOffsets", "StripByteCounts", "TileOffsets", "TileByteCounts",
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
    6.3") count as numbers too, matching where any of them starts. Scalar
    numbers may omit their leading sign, but never match within an exponent.
    """
    scalar_number = f"{value_type} IN ('integer', 'real')"
    number_text_like = "0"
    if number_text:
        number_text_like = f"{value_type} = 'text' AND {value} NOT GLOB '*[^0-9 .,+-]*'"
    return (
        f"(CASE WHEN {scalar_number} OR ({number_text_like}) "
        f"THEN lower({value}) GLOB ? "
        f"OR ({scalar_number} AND lower(ltrim({value}, '+-')) GLOB ?) "
        f"OR ({number_text_like} AND lower({value}) GLOB ?) "
        f"ELSE {value} LIKE ? ESCAPE '\\' END)"
    )


def term_binds(like, term):
    """The binds for one ``value_matches``: number-start GLOBs, then LIKE."""
    glob = "".join(f"[{ch}]" if ch in "*?[" else ch for ch in term.lower())
    return [f"{glob}*", f"{glob}*", f"*[^0-9.]{glob}*", like]


def prediction_search_values(alias):
    """Searchable expressions for a ``predictions`` row aliased ``alias``.

    A custom-label row's stored scientific name may be another species' guess
    (legacy burst enrichment put Amazona rhodocorytha on "Lilac-crowned
    Amazon"), so it only answers a search when it is evidence; the label stays
    searchable either way. The rank columns stay: those guesses were nearly
    always a confusable neighbour, so their higher ranks are right, and SQL
    cannot resolve a label to correct them.
    """
    from species_identity import stored_taxonomy_evidence_sql

    return [
        f"CASE WHEN {stored_taxonomy_evidence_sql(alias)} THEN {alias}.scientific_name END"
        if column == "scientific_name" else f"{alias}.{column}"
        for column in PREDICTION_COLUMNS
    ]


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


# These cached file tags have authoritative, editable catalog counterparts.
# Searching their stale copies would resurrect removed keywords/ratings.
MANAGED_PATHS = (
    "$.XMP.Subject", "$.XMP.HierarchicalSubject", "$.IPTC.Keywords",
    "$.XMP.Rating", "$.XMP.Label", "$.XMP.ColorLabel",
    "$.File.FileName", "$.File.Directory", "$.System.FileName", "$.System.Directory",
)

# How a file tag value reads back: JSON booleans as words, everything else
# as SQLite renders the atom.
TAG_VALUE = (
    "(CASE WHEN search_tag.type IN ('true', 'false') "
    "THEN search_tag.type ELSE CAST(search_tag.atom AS TEXT) END)"
)

EXIF_SEARCH_TEXT_TABLE = "photo_exif_search_text"


def exif_tags(source):
    """``FROM`` body yielding ``source``'s searchable file tags as ``search_tag``.

    Containers and nulls drop out with the layout tags and binary
    placeholders, because their ``TAG_VALUE`` is NULL.
    """
    paths_sql = ", ".join(f"'{path}'" for path in MANAGED_PATHS)
    layout_sql = ", ".join(f"'{tag}'" for tag in LAYOUT_TAGS)
    return (
        f"json_tree(json_remove(CASE WHEN json_valid({source}) THEN {source} "
        f"ELSE '{{}}' END, {paths_sql})) search_tag "
        f"WHERE search_tag.key NOT IN ({layout_sql}) "
        f"AND {TAG_VALUE} NOT LIKE '(Binary data %'"
    )


def exif_search_text(source):
    """Every searchable tag value of ``source``, rendered and joined.

    Tag names stay out: ``GPSLongitude`` is in every GPS photo's EXIF, so a
    raw-text check let "long" through for all of them and the full walk then
    found nothing. A value that matches a term contains it, so text lacking
    the term proves no tag matches.
    """
    return (
        f"(SELECT COALESCE(group_concat({TAG_VALUE}, char(31)), '') "
        f"FROM {exif_tags(source)})"
    )


def exif_search_text_triggers():
    """``(name, sql)`` for the triggers that keep the search text current.

    Every writer of ``photos.exif_data`` goes through SQLite, so triggers
    cover scans, repairs and capture-time edits without each one knowing.
    """
    upsert = (
        f"INSERT OR REPLACE INTO {EXIF_SEARCH_TEXT_TABLE} (photo_id, value_text) "
        f"VALUES (NEW.id, {exif_search_text('NEW.exif_data')});"
    )
    return [
        (f"trg_{EXIF_SEARCH_TEXT_TABLE}_{suffix}",
         f"CREATE TRIGGER trg_{EXIF_SEARCH_TEXT_TABLE}_{suffix} AFTER {event} ON photos "
         f"BEGIN {upsert} END")
        for suffix, event in (("insert", "INSERT"), ("update", "UPDATE OF exif_data"))
    ]


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
    binds = term_binds(like, term)
    # Parsing and walking every photo's EXIF is nearly all of a search's
    # cost, so only photos whose stored tag values contain the term walk it.
    # A photo the startup backfill has not reached yet (NULL) falls back to
    # the raw-text check, or walks when no raw check is sound for the term.
    indexed = (
        f"(SELECT search_text.value_text LIKE ? ESCAPE '\\' "
        f"FROM {EXIF_SEARCH_TEXT_TABLE} search_text WHERE search_text.photo_id = p.id)"
    )
    unindexed, unindexed_params = "1", []
    if raw_text_rules_out(term):
        unindexed = (
            "(typeof(p.exif_data) != 'text' OR p.exif_data LIKE ? ESCAPE '\\' "
            "OR instr(p.exif_data, char(92)) > 0)"
        )
        unindexed_params = [like]
    tags = (
        f"(COALESCE({indexed}, {unindexed}) AND EXISTS (SELECT 1 FROM "
        + exif_tags("p.exif_data")
        + " AND "
        + value_matches(TAG_VALUE, "search_tag.type", number_text=True)
        + "))"
    )
    tag_params = [like, *unindexed_params, *binds]
    return [photo, folder, keyword, tags], [*binds, *binds, *binds, *tag_params]
