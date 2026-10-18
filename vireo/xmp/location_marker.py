"""Vireo's record of the location keywords it wrote into a sidecar.

``_LocationKeywordEdits`` writes the marker and its ownership companion;
keyword import and the sync review read them back to tell Vireo's entries
from the user's.
"""

from xmp.namespaces import NS_VIREO

# Attribute recording the location keyword path Vireo last wrote into this
# sidecar, e.g. ``United States|California|Kumeyaay Lake``. Location keywords
# are the one keyword kind Vireo owns end to end -- the user assigns a place
# in Vireo, never in the sidecar -- so a later change or an unset location has
# to remove them again. Without a record of what was written, "remove the old
# place" would have to guess, and any guess wide enough to catch a renamed
# place is also wide enough to delete a location keyword the user typed in
# Lightroom.
LOCATION_KEYWORDS_MARKER = f"{{{NS_VIREO}}}locationKeywords"
# Companion attribute recording which of the two entries the last write
# actually inserted into the sidecar. ``add_keywords`` skips an entry the
# sidecar already carries -- if the user typed "Kumeyaay Lake" in Lightroom
# themselves, or another Vireo keyword happens to share the leaf name, we
# have not authored it and must not remove it later. Values are a
# comma-separated combination of ``flat`` and ``hier``; an empty string
# means neither entry was ours. A missing attribute is a legacy write from
# before this record existed and is treated as "both" (the pre-fix
# assumption) so removal continues to work on sidecars already in the wild.
LOCATION_KEYWORDS_OWNED = f"{{{NS_VIREO}}}locationKeywordsOwned"


def _location_marker_parts(marker_value):
    """Split a stored location-keyword marker into its path segments."""
    if not marker_value:
        return []
    return [part for part in str(marker_value).split("|") if part.strip()]


def _parse_location_keywords_owned(value):
    """Return ``(owns_flat, owns_hier)`` for a stored owned-marker value.

    ``None`` (attribute missing) means the write pre-dates the marker and is
    treated as if Vireo owned both entries -- the pre-fix behaviour, kept so
    a removal against an older sidecar still cleans up what it wrote.
    """
    if value is None:
        return True, True
    tokens = {token.strip() for token in str(value).split(",")}
    tokens.discard("")
    return "flat" in tokens, "hier" in tokens


def _format_location_keywords_owned(owns_flat, owns_hier):
    """Serialize an ownership tuple for the companion marker."""
    parts = []
    if owns_flat:
        parts.append("flat")
    if owns_hier:
        parts.append("hier")
    return ",".join(parts)


def location_keyword_entries(marker_value):
    """Return ``(flat_leaf, hierarchical_path)`` for a location marker value.

    Both are ``(None, None)`` for an empty marker. These are exactly the two
    sidecar entries :meth:`SidecarEditor.set_location_keywords` writes, so an
    importer can skip them without re-deriving the convention.
    """
    parts = _location_marker_parts(marker_value)
    if not parts:
        return None, None
    return parts[-1], "|".join(parts)
