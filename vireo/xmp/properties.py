"""Simple-property writes: set, delete and restore, honoring qualified copies."""

from xmp.document import _SidecarDocument
from xmp.namespaces import NS_RDF
from xmp.photo_scope import _plain_property_occurrences, _property_occurrences
from xmp.rdf import (
    _ancestor_carries_xml_qualifier,
    _build_parent_map,
    _is_plain_property_occurrence,
    _property_occurrence_score,
    _simple_prop_carries_qualifier,
    _update_simple_property_value,
)


def _update_kept_simple_property(keeper, name, value):
    """Write ``value`` into the kept occurrence; whether anything changed."""
    owner, child = keeper
    if child is None:
        if owner.get(name) != value:
            owner.set(name, value)
            return True
    elif f"{{{NS_RDF}}}value" in child.attrib or len(child):
        # Qualified property. Update whichever spelling
        # already carries the value -- attribute or element
        # form, on the property itself or inside a nested
        # ``rdf:Description`` -- in place. That preserves
        # qualifier attributes and sibling elements; the
        # helper only writes a fresh ``rdf:value`` if no
        # form carries one yet.
        return bool(_update_simple_property_value(child, value))
    elif (child.text or "").strip() != value:
        child.text = value
        return True
    return False


def _drop_duplicate_simple_properties(rest, name, parent_map):
    """Remove plain duplicate occurrences; whether anything changed."""
    changed = False
    for owner, child in rest:
        if child is None:
            if _ancestor_carries_xml_qualifier(owner, parent_map):
                # The attribute is a value-qualified RDF
                # statement in its own right (the owner or an
                # ancestor carries ``xml:lang'' / ``xml:base''),
                # so it carries data Vireo doesn't own and
                # can't back up per-occurrence. Leave both the
                # attribute AND its original value alone -- a
                # later restore of Vireo's own backup only
                # touches the keeper, so overwriting this
                # occurrence's value now would permanently
                # replace it with the keeper's on the next
                # clear. External readers may resolve the
                # conflicting copies differently from Vireo,
                # but that's an acceptable trade for data
                # preservation.
                pass
            else:
                del owner.attrib[name]
                changed = True
        elif (
            _simple_prop_carries_qualifier(child)
            or _ancestor_carries_xml_qualifier(child, parent_map)
        ):
            # Removing this duplicate would silently drop its
            # qualifier attributes or elements (a distinct
            # ``rdf:ID``, a ``foo:source`` attribute-form
            # qualifier, a sibling qualifier element alongside
            # ``rdf:value``, etc.) OR its effective inherited
            # ``xml:lang'' -- the child text inside a
            # language-qualified context is a language-tagged
            # statement in its own right, so dropping it
            # would silently lose the tag. Walk from the
            # CHILD, not the owner, so a nearer
            # ``xml:lang=""'' reset on the child correctly
            # cancels the owner's language and leaves the
            # effectively-unqualified duplicate open to
            # removal (its value would otherwise persist as
            # stale on a keeper-only rewrite). Leave every
            # genuinely-qualified duplicate entirely alone
            # (both the element and its original value):
            # Vireo can't back up this occurrence's value
            # independently, so overwriting would leave a
            # later restore with only the keeper's value to
            # write back and permanently replace this one.
            # External readers may resolve the conflicting
            # copies differently from Vireo, but that's an
            # acceptable trade for data preservation.
            continue
        else:
            owner.remove(child)
            changed = True
    return changed


class _SimpleProperties:
    """Writes to simple (single-valued) properties of the photo.

    A simple property can be stored as a Description attribute or as a child
    element, in plain or qualified form, and more than once. Writes update an
    existing occurrence in place and remove only copies whose removal loses
    no qualifier data; the ``plain`` variants leave qualified copies alone
    entirely.
    """

    def __init__(self, document: _SidecarDocument):
        self.document = document

    def set_properties(self, desc, values):
        """Set simple properties, marking the tree dirty only for real changes.

        A property the sidecar already carries is updated in place, as an
        attribute or as a child element, whichever form it uses; a
        Description free of inherited ``xml:*`` receives properties that
        are new. Any further copies of the property are removed so no
        reader can pick up a stale value.
        """
        changed = False
        new_desc = None
        parent_map = _build_parent_map(self.document.root)
        for name, value in values.items():
            found = _property_occurrences(self.document.root, name)
            if not found:
                # Adding under ``desc`` directly would let an ``xml:lang``
                # (or another ``xml:*`` attribute) on that Description --
                # or on an ancestor above it, like ``rdf:RDF`` or
                # ``x:xmpmeta`` -- attach to a numeric rating, a GPS
                # coordinate, or a Vireo marker: language-tagging a
                # value that carries no natural language is invalid
                # XMP semantics and ``_photo_scoped_bags`` already
                # avoids the same trap for keyword arrays. Fall back
                # to a Description free of inherited ``xml:*``.
                target = desc
                if _ancestor_carries_xml_qualifier(desc, parent_map):
                    if new_desc is None:
                        new_desc = self.document.unqualified_photo_description()
                        # ``unqualified_photo_description`` may have
                        # mutated the tree, so refresh the parent map.
                        parent_map = _build_parent_map(self.document.root)
                    target = new_desc
                target.set(name, value)
                changed = True
                continue
            best_idx = max(
                range(len(found)),
                key=lambda i: _property_occurrence_score(
                    found[i], parent_map,
                ),
            )
            keeper = found[best_idx]
            rest = [
                entry for i, entry in enumerate(found) if i != best_idx
            ]
            if _update_kept_simple_property(keeper, name, value):
                changed = True
            if _drop_duplicate_simple_properties(rest, name, parent_map):
                changed = True
        if changed:
            self.document.dirty = True
        return changed

    def delete_property(self, name):
        """Remove every copy of a simple property; True when one existed."""
        found = _property_occurrences(self.document.root, name)
        for owner, child in found:
            if child is None:
                owner.attrib.pop(name, None)
            else:
                owner.remove(child)
        if found:
            self.document.dirty = True
        return bool(found)

    def delete_plain_property_copies(self, name):
        """Remove only plain (unqualified) copies of a simple property.

        Callers that own the plain occurrence (Vireo's own writes) but
        cannot vouch for qualified duplicates -- another tool may have
        added a language-tagged or ``foo:source''-annotated occurrence
        after the initial write -- use this instead of
        :meth:`delete_property`. Retained qualified duplicates keep
        their metadata AND their original values, matching the
        preservation invariant that ``set_properties`` applies. True
        when at least one plain copy existed.
        """
        found = _property_occurrences(self.document.root, name)
        if not found:
            return False
        parent_map = _build_parent_map(self.document.root)
        removed = False
        for owner, child in found:
            if child is None:
                if _ancestor_carries_xml_qualifier(owner, parent_map):
                    continue
                if name in owner.attrib:
                    owner.attrib.pop(name, None)
                    removed = True
            elif _simple_prop_carries_qualifier(child) or (
                _ancestor_carries_xml_qualifier(child, parent_map)
            ):
                continue
            else:
                owner.remove(child)
                removed = True
        if removed:
            self.document.dirty = True
        return removed

    _is_plain_occurrence = staticmethod(_is_plain_property_occurrence)

    def set_plain_property_value(self, desc, name, value):
        """Set a simple property's value, landing only on plain occurrences.

        Callers use this instead of :meth:`set_properties` for
        properties whose qualified copies carry data Vireo shouldn't
        overwrite. A GPS coordinate stored with ``foo:source="camera"''
        is one such property: overwriting it would silently destroy
        the pre-existing user-authored value, and Vireo's backup
        (``vireo:previousGPS*'') only holds one value it could later
        restore. The write lands on the first plain occurrence and
        collapses any other plain duplicates; if none exists a fresh
        plain occurrence is created under an unqualified Description.
        Qualified occurrences are left entirely alone -- their
        metadata AND their values both survive. Returns True when
        the tree changed.
        """
        parent_map = _build_parent_map(self.document.root)
        plain = _plain_property_occurrences(self.document.root, name)
        changed = False
        if plain:
            owner, child = plain[0]
            if child is None:
                if owner.get(name) != value:
                    owner.set(name, value)
                    changed = True
            elif _update_simple_property_value(child, value):
                changed = True
            for owner, child in plain[1:]:
                if child is None:
                    if name in owner.attrib:
                        del owner.attrib[name]
                        changed = True
                else:
                    owner.remove(child)
                    changed = True
        else:
            target = desc
            if _ancestor_carries_xml_qualifier(desc, parent_map):
                target = self.document.unqualified_photo_description()
            target.set(name, value)
            changed = True
        if changed:
            self.document.dirty = True
        return changed

    def restore_plain_property_value(self, name, value):
        """Restore ``value'' onto the FIRST plain copy of a property.

        Vireo's own writes land on a single plain occurrence
        (``set_plain_property_value'' collapses plain duplicates
        at write time). So on removal only ONE plain occurrence is
        Vireo's to restore; anything a later external tool added
        alongside it carries data Vireo didn't overwrite and can't
        vouch for (Codex finding: looping the whole plain set
        overwrites a newly-added external ``30,0N'' back to the
        pre-write ``10,0N''). Restore just the first plain
        occurrence in document order -- the same one
        ``_get_plain_property_value'' would read -- and leave
        every other occurrence (plain OR qualified) entirely
        alone. "Plain" here is the same classification
        :meth:`delete_plain_property_copies` uses; the short
        qualified-serialization form (``rdf:parseType="Resource"''
        with a bare ``rdf:value'' child and nothing else) is
        treated as plain because Vireo's own writes preserve it
        and update through it.

        Returns True when a plain target was found (the value was
        written, or already matched); False when no plain
        occurrence remains for the caller to recreate.
        """
        found = _property_occurrences(self.document.root, name)
        if not found:
            return False
        parent_map = _build_parent_map(self.document.root)
        for owner, child in found:
            if child is None:
                if _ancestor_carries_xml_qualifier(owner, parent_map):
                    continue
                if owner.get(name) != value:
                    owner.set(name, value)
                    self.document.dirty = True
                return True
            elif _simple_prop_carries_qualifier(child) or (
                _ancestor_carries_xml_qualifier(child, parent_map)
            ):
                continue
            else:
                # Update whichever spelling actually carries the
                # value -- text, attribute-abbreviated ``rdf:value'',
                # or a nested ``rdf:value'' child -- so a plain
                # short-form qualified serialization (a bare
                # ``rdf:parseType="Resource"'' wrapping just an
                # ``rdf:value'') is restored correctly instead of
                # having the value written into the wrapper's text.
                if _update_simple_property_value(child, value):
                    self.document.dirty = True
                return True
        return False
