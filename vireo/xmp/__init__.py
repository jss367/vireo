"""Consolidated XMP sidecar operations.

Provides read/write/merge/remove for XMP keyword and rating metadata.

The package's modules, by responsibility:

- ``namespaces``: the namespace URIs and the prefixes ElementTree writes
  them with (the single source of truth).
- ``rdf``: element-level structure -- an item's or property's value in any
  serialization, qualifier detection, occurrence ranking.
- ``photo_scope``: which Descriptions describe the photo (``rdf:about``,
  ``xml:base``, the sidecar's document URI) and the property and keyword-bag
  lookups scoped to them.
- ``location_marker``: Vireo's record of the location keywords it wrote.
- ``readers``: the read-only functions (keywords, markers, the sync-review
  fields).
- ``publish``: the atomic publish -- temp file, owner/mode/ACL/xattr/flag
  preservation, ``replace_file``.
- ``editor``: ``SidecarEditor``, which parses a sidecar once, routes each
  edit to a collaborator and publishes once, plus the one-shot ``write_*``
  functions built on it. Its collaborators:
  - ``document``: ``_SidecarDocument``, the parse state, the dirty flag,
    the Descriptions new properties land on, and ``commit``.
  - ``properties``: ``_SimpleProperties``, simple-property set, delete and
    restore.
  - ``keywords``: ``_KeywordEdits``, keyword bags and keyword add, rename
    and removal.
  - ``fields``: ``_FieldEdits``, rating, pick flag and edit recipe.
  - ``gps``: ``_GpsEdits``, Vireo-assigned GPS and its backup.
  - ``location_keywords``: ``_LocationKeywordEdits``, the place written as
    keywords and the ownership record that lets Vireo remove it again.

This module re-exports what code outside the package imports. A module
inside the package looks a name up in its own globals, so a test that
patches one of those names patches the submodule that calls it (for example
``xmp.document._write_tree_atomic``), not this package.
"""

from xmp.editor import (
    SidecarEditor,
    remove_keywords,
    remove_vireo_gps_location,
    write_edit_recipe,
    write_gps_location,
    write_pick_flag,
    write_rating,
    write_sidecar,
)
from xmp.location_marker import (
    LOCATION_KEYWORDS_MARKER,
    LOCATION_KEYWORDS_OWNED,
    _parse_location_keywords_owned,
    location_keyword_entries,
)
from xmp.namespaces import (
    NS_DC,
    NS_EXIF,
    NS_LR,
    NS_RDF,
    NS_VIREO,
    NS_X,
    NS_XML,
    NS_XMP,
    NS_XMPDM,
)
from xmp.readers import (
    read_hierarchical_keywords,
    read_keywords,
    read_sync_preview_metadata,
    read_vireo_location_keywords,
    read_vireo_location_keywords_owned,
)

__all__ = [
    "LOCATION_KEYWORDS_MARKER",
    "LOCATION_KEYWORDS_OWNED",
    "NS_DC",
    "NS_EXIF",
    "NS_LR",
    "NS_RDF",
    "NS_VIREO",
    "NS_X",
    "NS_XML",
    "NS_XMP",
    "NS_XMPDM",
    "SidecarEditor",
    "_parse_location_keywords_owned",
    "location_keyword_entries",
    "read_hierarchical_keywords",
    "read_keywords",
    "read_sync_preview_metadata",
    "read_vireo_location_keywords",
    "read_vireo_location_keywords_owned",
    "remove_keywords",
    "remove_vireo_gps_location",
    "write_edit_recipe",
    "write_gps_location",
    "write_pick_flag",
    "write_rating",
    "write_sidecar",
]
