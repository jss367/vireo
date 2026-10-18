"""``SidecarEditor``, the coordinator for sidecar edits, and one-shot writers."""

from xmp.document import _SidecarDocument
from xmp.fields import _FieldEdits
from xmp.gps import _GpsEdits
from xmp.keywords import _KeywordEdits
from xmp.location_keywords import _LocationKeywordEdits
from xmp.properties import _SimpleProperties


class SidecarEditor:
    """Accumulate sidecar edits and publish them in a single atomic write.

    Every writer below used to parse the sidecar, mutate it, and republish it
    on its own. One publish is a temp-file create, an fsync, an ACL and
    extended-attribute copy, and a rename -- roughly 0.4s on an SMB-mounted
    NAS -- so a photo whose sync queued keywords, a flag and a rating paid
    that price four times over, re-reading the file before each one. The
    editor parses once, applies every mutation to the same tree, and writes
    once.

    It also tracks whether any mutation actually changed something, so
    re-syncing a sidecar that already carries the requested metadata costs a
    read instead of a full network publish.

    One editor drives one sidecar and is not thread-safe; callers that write
    several sidecars concurrently use one editor per file.

    The editor is a coordinator. ``document`` holds the parsed tree and the
    dirty flag every collaborator sets, and publishes; ``properties`` writes
    simple properties; ``keywords``, ``fields``, ``gps`` and ``locations``
    implement the public mutations below, which delegate to them unchanged.
    """

    def __init__(self, xmp_path):
        self.document = _SidecarDocument(xmp_path)
        self.properties = _SimpleProperties(self.document)
        self.keywords = _KeywordEdits(self.document)
        self.fields = _FieldEdits(self.document, self.properties)
        self.gps = _GpsEdits(self.document, self.properties)
        self.locations = _LocationKeywordEdits(
            self.document, self.properties, self.keywords,
        )

    @property
    def path(self):
        return self.document.path

    # ── Mutations ───────────────────────────────────────────────────────

    def add_keywords(self, flat_keywords=(), hierarchical_keywords=()):
        """See :meth:`_KeywordEdits.add_keywords`."""
        return self.keywords.add_keywords(flat_keywords, hierarchical_keywords)

    def replace_keyword_hierarchies(self, replacements):
        """See :meth:`_KeywordEdits.replace_keyword_hierarchies`."""
        return self.keywords.replace_keyword_hierarchies(replacements)

    def remove_keywords(self, keywords_to_remove, *, hierarchical=True,
                        keep_exact=False):
        """See :meth:`_KeywordEdits.remove_keywords`."""
        return self.keywords.remove_keywords(
            keywords_to_remove, hierarchical=hierarchical, keep_exact=keep_exact,
        )

    def set_rating(self, rating, create=False):
        """See :meth:`_FieldEdits.set_rating`."""
        return self.fields.set_rating(rating, create=create)

    def set_pick_flag(self, flag):
        """See :meth:`_FieldEdits.set_pick_flag`."""
        return self.fields.set_pick_flag(flag)

    def set_gps_location(self, latitude, longitude, source="assigned"):
        """See :meth:`_GpsEdits.set_gps_location`."""
        return self.gps.set_gps_location(latitude, longitude, source=source)

    def remove_vireo_gps_location(self):
        """See :meth:`_GpsEdits.remove_vireo_gps_location`."""
        return self.gps.remove_vireo_gps_location()

    def set_location_keywords(self, path_parts):
        """See :meth:`_LocationKeywordEdits.set_location_keywords`."""
        return self.locations.set_location_keywords(path_parts)

    def remove_vireo_location_keywords(self):
        """See :meth:`_LocationKeywordEdits.remove_vireo_location_keywords`."""
        return self.locations.remove_vireo_location_keywords()

    def release_location_flat_ownership_for(self, keywords):
        """See :meth:`_LocationKeywordEdits.release_location_flat_ownership_for`."""
        return self.locations.release_location_flat_ownership_for(keywords)

    def set_edit_recipe(self, recipe_json):
        """See :meth:`_FieldEdits.set_edit_recipe`."""
        return self.fields.set_edit_recipe(recipe_json)

    # ── Publication ─────────────────────────────────────────────────────

    def commit(self):
        """Publish the accumulated edits; a no-op when nothing changed.

        Returns True when the sidecar was written.
        """
        return self.document.commit()


def write_sidecar(xmp_path, flat_keywords, hierarchical_keywords):
    """Write or merge keywords into an XMP sidecar file.

    Args:
        xmp_path: Path to the .xmp file (created if missing, merged if exists)
        flat_keywords: set of keyword strings for dc:subject
        hierarchical_keywords: set of pipe-delimited hierarchy strings for lr:hierarchicalSubject
    """
    editor = SidecarEditor(xmp_path)
    editor.add_keywords(flat_keywords, hierarchical_keywords)
    editor.commit()


def write_rating(xmp_path, rating):
    """Write xmp:Rating attribute to an XMP sidecar.

    No-op if the file does not exist (we don't create an XMP just for a rating).
    """
    editor = SidecarEditor(xmp_path)
    editor.set_rating(rating)
    editor.commit()


def write_pick_flag(xmp_path, flag):
    """Write Lightroom-compatible pick/reject flag metadata.

    Vireo stores flags as ``flagged`` / ``none`` / ``rejected``. Lightroom
    Classic 13.2+ persists the equivalent pick state in ``xmpDM:pick`` using
    values ``1`` / ``0`` / ``-1``.
    """
    editor = SidecarEditor(xmp_path)
    editor.set_pick_flag(flag)
    editor.commit()


def write_gps_location(xmp_path, latitude, longitude, source="assigned"):
    """Write Lightroom-compatible GPS coordinates to an XMP sidecar.

    A small Vireo marker records that these GPS fields were written by Vireo,
    so ``remove_vireo_gps_location`` can clear stale assigned-location GPS
    without touching unrelated GPS metadata from another application.
    """
    editor = SidecarEditor(xmp_path)
    editor.set_gps_location(latitude, longitude, source=source)
    editor.commit()


def remove_vireo_gps_location(xmp_path):
    """Remove GPS fields only when Vireo previously wrote them."""
    editor = SidecarEditor(xmp_path)
    removed = editor.remove_vireo_gps_location()
    editor.commit()
    return removed


def write_edit_recipe(xmp_path, recipe_json):
    """Write or clear Vireo's non-destructive edit recipe marker."""
    editor = SidecarEditor(xmp_path)
    changed = editor.set_edit_recipe(recipe_json)
    editor.commit()
    return changed


def remove_keywords(xmp_path, keywords_to_remove, *, hierarchical=True):
    """Remove keywords from dc:subject and lr:hierarchicalSubject in an XMP file.

    Args:
        xmp_path: path to the .xmp sidecar
        keywords_to_remove: set of keyword strings to remove
        hierarchical: When True (default), a hierarchical entry is deleted
            if any of its pipe-delimited segments matches — used for real
            keyword removals so ``Animals|Birds|Hawk`` is dropped when the
            user removes ``Hawk``. When False, hierarchies are left
            untouched and only the flat ``dc:subject`` bag is pruned — used
            by the sync path to canonicalize add-equivalent flat variants
            (e.g. strip a legacy ``'apapane`` before writing the clean
            ``apapane``) without deleting an unrelated hierarchy whose
            leaf happens to match the added flat keyword.
    """
    editor = SidecarEditor(xmp_path)
    editor.remove_keywords(keywords_to_remove, hierarchical=hierarchical)
    editor.commit()
