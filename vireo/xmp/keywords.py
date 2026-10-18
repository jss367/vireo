"""Keyword edits: ``dc:subject`` and ``lr:hierarchicalSubject``."""

import copy
import logging
from xml.etree import ElementTree as ET

from keyword_normalization import keyword_match_key
from xmp.document import _SidecarDocument
from xmp.namespaces import NS_DC, NS_LR, NS_RDF
from xmp.photo_scope import _all_photo_scoped_values, _photo_scoped_bags, _top_descriptions
from xmp.rdf import (
    _ancestor_carries_xml_qualifier,
    _build_parent_map,
    _has_own_non_language_qualifier,
    _li_carries_qualifier,
    _li_signature,
    _li_value,
    _property_bag_and_wrappers,
    _update_simple_property_value,
    _wrappers_carry_qualifier,
)

# Every module of the package logs as ``xmp``, the logger the single-module
# version used.
log = logging.getLogger("xmp")


def _bag_occurrence_is_qualified(prop, parent_map):
    """Whether merging this array-property occurrence would drop a qualifier."""
    # The effective-language check on the deepest element already
    # walks every ``xml:lang'' in the ancestor chain (honoring
    # any local reset), so counting ``xml:lang'' on the property,
    # wrapper or bag as a standalone own qualifier here would
    # double-classify a container whose reset already made it
    # effectively unqualified. Use the non-language variant of
    # ``_has_own_value_qualifier'' -- it still counts every
    # non-language value qualifier (``foo:source'', identity
    # attributes, ``rdf:datatype'', ...).
    # Compute the effective ``xml:lang`` at the deepest
    # relevant element (the bag when present, else the
    # property). Walking from there upward honors a local
    # ``xml:lang=""`` reset at any level -- on the bag, on a
    # wrapper, on the property itself, or on the owning
    # Description -- so a bag that cancels the owner's ``en''
    # is correctly seen as effectively unqualified and stays
    # a valid reuse target. Every wrapper (an ``rdf:value``,
    # an ``rdf:Description``, or both) and the bag itself
    # would be dropped if this occurrence merged into the
    # target, so any *non-language* attribute they carry
    # counts -- including RDF's attribute abbreviation for a
    # nested property, like
    # ``<rdf:Description foo:source="camera">``, which is
    # equivalent to a sibling ``<foo:source>...</foo:source>``
    # of ``rdf:value``. Sibling qualifier ELEMENTS inside the
    # qualified property carry their own meaning too.
    bag_el, wrappers = _property_bag_and_wrappers(prop)
    deepest = bag_el if bag_el is not None else prop
    return (
        _ancestor_carries_xml_qualifier(deepest, parent_map)
        or _has_own_non_language_qualifier(prop)
        or any(_has_own_non_language_qualifier(w) for w in wrappers)
        or (bag_el is not None and _has_own_non_language_qualifier(bag_el))
        or _wrappers_carry_qualifier(prop)
    )


class _KeywordEdits:
    """Adds, renames and removes keywords in the photo's keyword bags.

    Owns finding (or creating) the bag a new keyword goes into, folding
    duplicate unqualified bags left by earlier writes into it, and the
    keyword-level add, hierarchy replacement and removal.
    """

    def __init__(self, document: _SidecarDocument):
        self.document = document

    def _bag(self, desc, tag_ns, tag_name):
        """Find or create the rdf:Bag of a namespaced array property.

        The property is looked up in every top-level Description, and only
        created under ``desc`` when none carries it; otherwise a second copy
        would sit beside the one another tool wrote. Copies left by an
        earlier write that made that mistake are merged into the first, so
        every reader sees the same list.
        """
        tag = f"{{{tag_ns}}}{tag_name}"
        found = [
            (owner, child)
            for owner in _top_descriptions(self.document.root)
            for child in owner.findall(tag)
        ]

        parent_map = _build_parent_map(self.document.root)
        elem = self._bag_target_property(desc, tag, found, parent_map)
        bag, _wrappers = _property_bag_and_wrappers(elem)
        if bag is None:
            bag = ET.SubElement(elem, f"{{{NS_RDF}}}Bag")
            self.document.dirty = True
        self._merge_duplicate_bags(bag, elem, found, parent_map)
        return bag

    def _bag_target_property(self, desc, tag, found, parent_map):
        """The array-property element new items go into, creating it if needed."""
        if found:
            # Pick an unqualified target so newly added items don't
            # inherit a container-level qualifier (an ``xml:lang`` on the
            # owning Description, on the property element, or on its
            # ``rdf:Bag``, or any other RDF attribute) that applies to
            # every ``rdf:li`` under it. If every existing occurrence is
            # qualified, create a fresh unqualified property under a
            # Description that has no inherited ``xml:*`` attribute of
            # its own -- ``desc`` itself may be the language-qualified
            # Description we're trying to avoid, so the new property has
            # to land somewhere the qualifier does not reach. The
            # qualified copies are left alone in the loop below.
            unqualified_idx = next(
                (i for i, (owner, child) in enumerate(found)
                 if not _bag_occurrence_is_qualified(child, parent_map)),
                None,
            )
            if unqualified_idx is None:
                elem = ET.SubElement(
                    self.document.unqualified_photo_description(), tag,
                )
                self.document.dirty = True
            else:
                elem = found[unqualified_idx][1]
        else:
            # No existing occurrence at all. Create the new property on
            # an unqualified Description for the same reason as above.
            target = desc
            # ``xml:lang``, ``xml:base`` and ``xml:space`` are inherited
            # from any ancestor, including ``rdf:RDF`` or
            # ``x:xmpmeta`` above the owner Description. Walk the whole
            # ancestor chain so a qualifier declared at the sidecar
            # root still counts. Structural RDF attributes like
            # ``rdf:parseType`` describe serialization form rather than
            # value semantics and do not count -- otherwise a keyword
            # array stored in the qualified resource form
            # ``<dc:subject rdf:parseType='Resource'><rdf:value><rdf:Bag>...``
            # would look qualified and force a second bag beside it.
            if _ancestor_carries_xml_qualifier(desc, parent_map):
                target = self.document.unqualified_photo_description()
            elem = ET.SubElement(target, tag)
            self.document.dirty = True
        return elem

    def _merge_duplicate_bags(self, bag, elem, found, parent_map):
        """Fold unqualified duplicate copies of the property into ``bag``."""
        seen = {_li_signature(li) for li in bag.findall(f"{{{NS_RDF}}}li")}
        for owner, extra in found:
            if extra is elem:
                continue
            extra_bag, _extra_wrappers = _property_bag_and_wrappers(extra)
            # A source whose items live under a non-empty effective
            # ``xml:lang`` (from the owner Description, a wrapper, or
            # the bag itself) would silently drop that language when
            # merged into a target that doesn't share it, so walk from
            # the deepest element to catch a local reset at any level
            # and refuse to merge whenever the effective language is
            # non-empty. Every other element that would be removed --
            # the property, an ``rdf:value`` / ``rdf:Description``
            # wrapper, or the ``rdf:Bag`` itself -- carries its own
            # meaning if it has any non-language non-structural
            # attribute (including RDF's attribute abbreviation, e.g.
            # ``<rdf:Description foo:source=...>``, equivalent to a
            # nested qualifier property). Sibling qualifier ELEMENTS
            # alongside the value do too. Any of these leaves the
            # qualified container in place; plain duplicates left by
            # earlier writes still collapse.
            if _bag_occurrence_is_qualified(extra, parent_map):
                continue
            if extra_bag is not None:
                # Copy every item, not just ones with direct text: a
                # structured value (rdf:parseType="Resource" with an
                # rdf:value child) has no text of its own, and skipping it
                # here would drop it when ``extra`` is removed below.
                for li in extra_bag.findall(f"{{{NS_RDF}}}li"):
                    sig = _li_signature(li)
                    if sig in seen:
                        continue
                    bag.append(copy.deepcopy(li))
                    seen.add(sig)
            owner.remove(extra)
            self.document.dirty = True

    def add_keywords(self, flat_keywords=(), hierarchical_keywords=()):
        """Merge keywords into dc:subject and lr:hierarchicalSubject.

        Deduplicate against every photo-scoped bag, not just the merge
        target ``_bag`` picks. When every existing bag is qualified,
        ``_bag`` returns a fresh empty unqualified bag; reading only
        that target would let a keyword already present in a
        qualified sibling bag land as a plain-text duplicate. Only
        create that merge target when there's something to add:
        otherwise re-adding a keyword that already sits in a
        qualified sibling bag would commit an empty ``dc:subject`` /
        ``lr:hierarchicalSubject`` next to the populated one, and a
        reader that resolves to a single occurrence would then see
        the photo as having no keywords at all.
        """
        desc = self.document.description()
        existing_flat = _all_photo_scoped_values(
            self.document.root, f"{{{NS_DC}}}subject",
        )
        to_add_flat = sorted(set(flat_keywords) - existing_flat)
        if to_add_flat:
            dc_bag = self._bag(desc, NS_DC, "subject")
            for kw in to_add_flat:
                ET.SubElement(dc_bag, f"{{{NS_RDF}}}li").text = kw
                self.document.dirty = True

        existing_hier = _all_photo_scoped_values(
            self.document.root, f"{{{NS_LR}}}hierarchicalSubject",
        )
        to_add_hier = sorted(set(hierarchical_keywords) - existing_hier)
        if to_add_hier:
            lr_bag = self._bag(desc, NS_LR, "hierarchicalSubject")
            for kw in to_add_hier:
                ET.SubElement(lr_bag, f"{{{NS_RDF}}}li").text = kw
                self.document.dirty = True

    def replace_keyword_hierarchies(self, replacements):
        """Replace exact reviewed paths; a None replacement removes that path."""
        if not self.document.readable():
            return
        def key(path):
            return tuple(keyword_match_key(part) for part in path.split('|'))
        by_key = {key(source): target for source, target in replacements.items()}
        parent_map = _build_parent_map(self.document.root)
        for bag in _photo_scoped_bags(self.document.root, f"{{{NS_LR}}}hierarchicalSubject"):
            keeper = {}
            for li in list(bag.findall(f"{{{NS_RDF}}}li")):
                old = _li_value(li) or ''
                value = by_key.get(key(old), old)
                if value is None:
                    # Explicit removal. The caller mapped this path to
                    # ``None`` on purpose (an obsolete source/target
                    # hierarchy in ``sync.py``'s merge planner), so
                    # even a qualified item goes -- leaving the stale
                    # hierarchy visible could let a re-import bring
                    # the rejected path back.
                    bag.remove(li)
                    self.document.dirty = True
                    continue
                if value in keeper:
                    # Duplicate of a value we already kept. Prefer the
                    # qualified item as the keeper so its ``foo:source``,
                    # ``xml:lang``, ``rdf:ID`` etc. survive. Qualification
                    # includes container-inherited effective ``xml:lang``:
                    # under an ``rdf:Bag xml:lang="fr"`` a bare-text item
                    # is a French-tagged statement whose language a plain
                    # drop would silently lose. If the existing keeper is
                    # plain (effectively unqualified) and this new item is
                    # qualified, swap them.
                    existing = keeper[value]
                    this_qualified = _li_carries_qualifier(li, parent_map)
                    existing_qualified = _li_carries_qualifier(
                        existing, parent_map,
                    )
                    if this_qualified and not existing_qualified:
                        bag.remove(existing)
                        keeper[value] = li
                        if value != old and _update_simple_property_value(
                            li, value,
                        ):
                            pass
                        self.document.dirty = True
                    elif this_qualified:
                        # Both qualified -- keep both so no qualifier
                        # metadata is lost.
                        if value != old and _update_simple_property_value(
                            li, value,
                        ):
                            self.document.dirty = True
                    else:
                        bag.remove(li)
                        self.document.dirty = True
                    continue
                keeper[value] = li
                if value != old and _update_simple_property_value(li, value):
                    self.document.dirty = True

    def remove_keywords(self, keywords_to_remove, *, hierarchical=True,
                        keep_exact=False):
        """Remove keywords from dc:subject and lr:hierarchicalSubject.

        See the module-level ``remove_keywords`` for the ``hierarchical``
        semantics.

        ``keep_exact`` leaves an entry whose text is exactly one of
        ``keywords_to_remove`` in place, so only spelling variants are
        stripped. The sync path canonicalizes an add by removing variants of
        it before writing the clean form; without this it would delete and
        re-append the identical entry it already found, and a sidecar that
        already says the right thing would never look unchanged.
        """
        if not self.document.readable():
            return False

        # Compare using the same normalized key add_keyword() uses on insert
        # so a DB removal of `apapane` also clears a sidecar `‘apapane` (or
        # `´apapane` / whitespace/casing variants). A plain `.lower()`
        # comparison would leave the quoted <rdf:li> in place; the next XMP
        # import path would then re-add the keyword the user removed. Drop
        # empty keys so removing a keyword whose name normalizes to `""`
        # doesn't accidentally match empty hierarchical segments (e.g.
        # `"|Birds|"` -> `["", "Birds", ""]`).
        remove_keys = {keyword_match_key(kw) for kw in keywords_to_remove}
        remove_keys.discard("")
        if not remove_keys:
            return False
        exact = set(keywords_to_remove) if keep_exact else set()
        # In canonicalization mode (``keep_exact=True'') the caller
        # is stripping variant spellings so ``add_keywords'' can
        # write the canonical. For a QUALIFIED variant, dropping
        # the ``rdf:li'' wholesale would silently discard whatever
        # ``foo:source'', ``rdf:ID'' or other metadata a user or
        # external tool authored on it; rewrite its nested value to
        # the canonical spelling in place instead, mirroring what
        # ``set_location_keywords'' does. Only relevant to the flat
        # branch: hierarchical segments don't have a 1:1 canonical
        # in ``keywords_to_remove'' (a variant can share a segment
        # with multiple keywords), so a qualified hierarchical
        # variant is left in place entirely.
        canonical_by_key = (
            {keyword_match_key(kw): kw for kw in keywords_to_remove}
            if keep_exact
            else {}
        )
        removed = []
        parent_map = _build_parent_map(self.document.root) if keep_exact else None

        for bag in _photo_scoped_bags(self.document.root, f"{{{NS_DC}}}subject"):
            for li in bag.findall(f"{{{NS_RDF}}}li"):
                value = _li_value(li)
                if not value or value in exact:
                    continue
                if keyword_match_key(value) in remove_keys:
                    if (
                        keep_exact
                        and _li_carries_qualifier(li, parent_map)
                    ):
                        canonical = canonical_by_key.get(
                            keyword_match_key(value),
                        )
                        if canonical and _update_simple_property_value(
                            li, canonical,
                        ):
                            self.document.dirty = True
                        continue
                    removed.append(value)
                    bag.remove(li)

        # Hierarchical entries match if any pipe-delimited segment matches.
        # Skipped when hierarchical=False: the sync path uses the flat-only
        # mode to canonicalize add-equivalent variants without accidentally
        # deleting unrelated hierarchies that share a segment with the added
        # flat leaf.
        if hierarchical:
            removed.extend(self._remove_hierarchical_keyword_entries(
                remove_keys, exact, keep_exact, parent_map,
            ))

        if removed:
            self.document.dirty = True
            log.info("Removed keywords from %s: %s", self.document.path, removed)
        return bool(removed)

    def _remove_hierarchical_keyword_entries(
        self, remove_keys, exact, keep_exact, parent_map,
    ):
        """Drop ``lr:hierarchicalSubject`` entries with a matching segment.

        Returns the removed entries' text.
        """
        removed = []
        for bag in _photo_scoped_bags(
            self.document.root, f"{{{NS_LR}}}hierarchicalSubject"
        ):
            for li in bag.findall(f"{{{NS_RDF}}}li"):
                value = _li_value(li)
                if not value or value in exact:
                    continue
                segments = {keyword_match_key(s) for s in value.split("|")}
                segments.discard("")
                if segments & remove_keys:
                    if (
                        keep_exact
                        and _li_carries_qualifier(li, parent_map)
                    ):
                        # See the flat branch in ``remove_keywords``: a qualified
                        # hierarchical variant carries user
                        # metadata a plain drop would lose. No
                        # canonical is available here (segments
                        # match by key, not by whole path), so
                        # leave the item entirely alone.
                        continue
                    removed.append(value)
                    bag.remove(li)
        return removed
