"""Single-valued photo fields: rating, pick flag and Vireo's edit recipe."""

from xmp.document import _SidecarDocument
from xmp.namespaces import NS_VIREO, NS_XMP, NS_XMPDM
from xmp.properties import _SimpleProperties


class _FieldEdits:
    """Writes ``xmp:Rating``, ``xmpDM:pick`` and ``vireo:editRecipe``."""

    def __init__(self, document: _SidecarDocument, properties: _SimpleProperties):
        self.document = document
        self.properties = properties

    def set_rating(self, rating, create=False):
        """Set xmp:Rating on a sidecar that exists or is already being written.

        A rating alone never creates a sidecar. Within one editor an earlier
        keyword, flag, location or edit mutation may have created one, and the
        rating then belongs in it -- which is why callers apply the rating
        last.

        ``create`` opts out of that, for the one caller whose skip would be
        permanent: the sync that runs before a NAS transfer. That transfer
        deletes the local originals once verified, so a rating skipped here
        has nowhere left to land -- every later sync would find no sidecar
        and skip it again, while the queued change was already cleared.

        On a readable existing sidecar with ambiguous non-empty RDF subjects
        (several distinct ``rdf:about`` values, no empty subject), no
        Description currently belongs to the photo. ``document.description()``
        handles that by adding a fresh Description scoped to the photo, so the rating
        lands there instead of being silently dropped -- which would let the
        sync caller clear the queued rating with nothing written.
        """
        if create:
            return self.properties.set_properties(
                self.document.description(), {f"{{{NS_XMP}}}Rating": str(rating)},
            )
        if not self.document.dirty and not self.document.readable():
            return False
        return self.properties.set_properties(
            self.document.description(), {f"{{{NS_XMP}}}Rating": str(rating)},
        )

    def set_pick_flag(self, flag):
        """Set the Lightroom-compatible pick state, creating a sidecar if needed."""
        values = {
            "flagged": "1",
            "none": "0",
            "rejected": "-1",
        }
        if flag not in values:
            raise ValueError("flag must be 'none', 'flagged', or 'rejected'")
        desc = self.document.description()
        return self.properties.set_properties(desc, {f"{{{NS_XMPDM}}}pick": values[flag]})

    def set_edit_recipe(self, recipe_json):
        """Write or clear Vireo's non-destructive edit recipe marker."""
        recipe_json = recipe_json or ""
        recipe_attr = f"{{{NS_VIREO}}}editRecipe"
        version_attr = f"{{{NS_VIREO}}}editRecipeSchema"

        if recipe_json:
            desc = self.document.description()
            return self.properties.set_properties(
                desc, {recipe_attr: recipe_json, version_attr: "1"},
            )

        if not self.document.readable():
            return False
        desc = self.document.find_description()
        if desc is None:
            return False
        removed = False
        for attr in (recipe_attr, version_attr):
            removed |= self.properties.delete_property(attr)
        return removed
