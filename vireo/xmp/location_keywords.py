"""Location keywords: the assigned place written as Lightroom keywords."""

import logging

from keyword_normalization import keyword_match_key
from xmp.document import _SidecarDocument
from xmp.keywords import _KeywordEdits
from xmp.location_marker import (
    LOCATION_KEYWORDS_MARKER,
    LOCATION_KEYWORDS_OWNED,
    _format_location_keywords_owned,
    _parse_location_keywords_owned,
    location_keyword_entries,
)
from xmp.namespaces import NS_DC, NS_LR, NS_RDF
from xmp.photo_scope import _all_photo_scoped_values, _photo_scoped_bags
from xmp.properties import _SimpleProperties
from xmp.rdf import (
    _build_parent_map,
    _li_carries_qualifier,
    _li_value,
    _update_simple_property_value,
)

# Every module of the package logs as ``xmp``, the logger the single-module
# version used.
log = logging.getLogger("xmp")


def _remove_one_owned_bag_entry(bags, exact_value, parent_map, normalized_match):
    """Remove the one entry a Vireo location-keyword write authored.

    Prefers a plain ``<rdf:li>`` whose text is exactly ``exact_value``; with
    none anywhere in ``bags``, falls back to the first plain entry whose
    text satisfies ``normalized_match``. Qualified entries are never
    removed. Returns the removed entry's text, or ``None``.
    """
    plain_exact_targets = [
        (bag, li)
        for bag in bags
        for li in bag.findall(f"{{{NS_RDF}}}li")
        if _li_value(li) == exact_value
        and not _li_carries_qualifier(li, parent_map)
    ]
    if plain_exact_targets:
        # Vireo authored one entry, not many. Remove a single
        # plain occurrence -- if another tool added exact
        # duplicates later, they're user- or tool-owned data
        # we shouldn't sweep away.
        bag, li = plain_exact_targets[0]
        value = _li_value(li)
        bag.remove(li)
        return value
    # No plain exact match, so Vireo's owned entry (if it
    # still exists) is a spelling variant that normalizes
    # to the recorded key. A qualified exact-match duplicate
    # from another tool must NOT suppress that fallback:
    # doing so would clear the ownership marker while
    # leaving Vireo's owned variant behind forever.
    fallback = None
    for bag in bags:
        fallback = next(
            (
                (bag, li) for li in bag.findall(f"{{{NS_RDF}}}li")
                if _li_value(li)
                and normalized_match(_li_value(li))
                and not _li_carries_qualifier(li, parent_map)
            ),
            None,
        )
        if fallback is not None:
            break
    if fallback is not None:
        bag, li = fallback
        value = _li_value(li)
        bag.remove(li)
        return value
    return None


class _LocationKeywordEdits:
    """Writes, replaces and removes the location keywords Vireo owns.

    The entries Vireo wrote are recorded in ``vireo:locationKeywords`` and
    ``vireo:locationKeywordsOwned``, so a later place change or clear removes
    only those, never a keyword the user typed.
    """

    def __init__(
        self,
        document: _SidecarDocument,
        properties: _SimpleProperties,
        keywords: _KeywordEdits,
    ):
        self.document = document
        self.properties = properties
        self.keywords = keywords

    def _reject_piped_location_parts(self, parts):
        """Raise ``ValueError`` when a location name contains ``|``."""
        # A pipe in a location name would corrupt every downstream reader:
        # Lightroom's ``lr:hierarchicalSubject`` uses ``|`` as the segment
        # delimiter, and Vireo's own marker parser splits on the same
        # character. ``get_or_create_text_location`` now rejects the pipe
        # at assignment time, but a legacy row (a keyword created before
        # that gate, or a Google Place name that already carried one) can
        # still reach this method. Raise instead of silently returning:
        # ``sync_to_xmp`` treats a normal return as "the write succeeded"
        # and clears the pending ``location`` change, so a silent skip
        # would strand the photo -- either the old Vireo-owned keyword
        # and marker sit in the sidecar forever (place reassigned) or the
        # new keyword never gets written and no later sync will try
        # again. The raised ``ValueError`` propagates through
        # ``_write_photo_sync``, is recorded as a per-photo failure, and
        # keeps the change queued so the user can rename the location.
        if any("|" in part for part in parts):
            log.warning(
                "Refusing location-keyword write for %s: a name contains"
                " '|' which collides with Lightroom's hierarchy delimiter"
                " (parts=%r)",
                self.document.path, parts,
            )
            raise ValueError(
                f"location name may not contain '|': {parts!r}"
            )

    def _location_keywords_already_present(self, parts):
        """Whether the leaf / hierarchy already sit in the sidecar, normalized.

        Returns ``(leaf_key, existed_flat, existed_hier)``.
        """
        # Look for a pre-existing normalized match of the leaf or hierarchy
        # BEFORE canonicalizing. add_keywords() dedupes on exact text, so a
        # sidecar spelling like `kumeyaay lake` (a Lightroom rewrite, or a
        # keyword the user typed themselves) would otherwise sit beside a
        # clean `Kumeyaay Lake` as a second <rdf:li>. The flat-leaf removal
        # below strips those variants, and add_keywords() would then look
        # like it inserted a fresh entry -- but the entry is really the
        # user's. Claiming it as Vireo-owned would let a later clear or
        # setting-toggle delete the user's keyword. The hierarchy is not
        # canonicalized here, but the same shape of user variant needs the
        # same ownership treatment: _remove_location_keyword_entries matches
        # on normalized keys, so a hier variant would be stripped on removal
        # if we claimed the canonical form we added beside it.
        # Check ownership across EVERY photo-scoped bag, not just the
        # merge target that ``_bag`` picks. ``_bag`` returns a fresh
        # empty bag when every existing occurrence is qualified (an
        # ``xml:lang`` on the property, wrapper or ancestor, an
        # attribute-form qualifier, an identity, etc.), so the target
        # would report "not present" even when the user's keyword
        # sits in one of the qualified bags. Silently claiming the
        # keyword as Vireo-owned would let a later
        # ``remove_vireo_location_keywords`` strip the user's original
        # entry from every photo-scoped bag, an irreversible data
        # loss.
        leaf_key = keyword_match_key(parts[-1])
        path_keys = [keyword_match_key(part) for part in parts]
        flat_values_before = _all_photo_scoped_values(
            self.document.root, f"{{{NS_DC}}}subject",
        )
        hier_values_before = _all_photo_scoped_values(
            self.document.root, f"{{{NS_LR}}}hierarchicalSubject",
        )
        existed_flat = bool(leaf_key) and any(
            keyword_match_key(v) == leaf_key for v in flat_values_before
        )
        existed_hier = any(
            [keyword_match_key(s) for s in v.split("|")] == path_keys
            for v in hier_values_before
        )
        # ``add_keywords`` below creates its merge bags lazily -- only
        # when there's a new value to insert -- so no eager ``_bag``
        # call is needed here. Creating them eagerly would commit an
        # empty ``dc:subject`` / ``lr:hierarchicalSubject`` beside a
        # populated qualified sibling whenever the requested leaf or
        # hierarchy already lives in that qualified bag, and a reader
        # that resolves to a single occurrence would then report the
        # photo as having no keywords.
        return leaf_key, existed_flat, existed_hier

    def _canonicalize_qualified_location_leaf(self, leaf_key, leaf):
        """Respell qualified ``dc:subject`` variants of the leaf in place."""
        # Canonicalize any *qualified* variant of the leaf in place so
        # its user-authored qualifier metadata survives. Removing a
        # qualified ``<rdf:li rdf:parseType='Resource'><rdf:value>paris
        # </rdf:value><foo:source>user</foo:source></rdf:li>`` wholesale
        # (as ``remove_keywords`` does below) would silently drop
        # ``foo:source``. An item under a container-qualified bag
        # (say ``<dc:subject xml:lang="fr"><rdf:Bag><rdf:li>paris
        # </rdf:li></rdf:Bag></dc:subject>``) is language-tagged too,
        # even though the ``rdf:li`` itself is bare-text: dropping and
        # re-adding it would silently lose the ``fr`` tag. Update its
        # nested value to the canonical spelling instead; the
        # subsequent ``add_keywords`` will see it as already present
        # and skip inserting a plain duplicate.
        if not leaf_key:
            return
        parent_map = _build_parent_map(self.document.root)
        for bag in _photo_scoped_bags(
            self.document.root, f"{{{NS_DC}}}subject",
        ):
            for li in bag.findall(f"{{{NS_RDF}}}li"):
                li_value = _li_value(li)
                if (
                    li_value
                    and li_value != leaf
                    and keyword_match_key(li_value) == leaf_key
                    and _li_carries_qualifier(li, parent_map)
                    and _update_simple_property_value(li, leaf)
                ):
                    self.document.dirty = True

    def _add_location_keyword_entries(self, parts, path, existed_flat, existed_hier):
        """Canonicalize the flat leaf and add the missing location entries.

        Returns ``(added_flat, added_hier)``: whether this write inserted the
        flat leaf / hierarchy, so it may claim them as Vireo-owned.
        """
        # Canonicalize a flat variant of the leaf the way the species-keyword
        # path does: add_keywords() dedupes on exact text, so a sidecar
        # spelling like `kumeyaay lake` would otherwise sit beside the clean
        # one as a second <rdf:li>. ``keep_exact`` keeps a re-sync of an
        # already-correct sidecar a no-op. Qualified variants were
        # canonicalized in place above, so ``remove_keywords`` here only
        # sees plain-text variants that are safe to drop.
        self.keywords.remove_keywords({parts[-1]}, hierarchical=False, keep_exact=True)

        # An entry the sidecar already carries -- because the user typed it
        # in Lightroom, or another Vireo keyword shares its name -- is not
        # ours to claim and must not be removed on a later clear. Exact-text
        # matches survive the canonicalization step above and show up as
        # "already present"; normalized variants were stripped by that step,
        # so a straight bag re-read would misread them as fresh inserts.
        # ``existed_*`` captured that pre-canonicalization truth. Re-read
        # across every photo-scoped bag for the same reason ``existed_*``
        # did: a qualified sibling bag can hold the pre-existing entry.
        added_flat = (
            parts[-1] not in _all_photo_scoped_values(
                self.document.root, f"{{{NS_DC}}}subject",
            )
            and not existed_flat
        )
        added_hier = (
            path not in _all_photo_scoped_values(
                self.document.root, f"{{{NS_LR}}}hierarchicalSubject",
            )
            and not existed_hier
        )

        # Skip inserting the canonical hierarchy when the user already
        # has a normalized variant of it: ``add_keywords()`` would
        # otherwise leave both spellings side by side, and the leaked
        # canonical would drift out of step with the user's spelling
        # forever. The species-keyword sync path only canonicalizes
        # flat entries for the same reason.
        #
        # For the flat leaf, skip only when the EXACT canonical
        # string is already present -- the case-variant path relies
        # on ``add_keywords`` inserting the canonical after
        # ``remove_keywords`` stripped the variant. When the exact
        # canonical string sits only in a qualified sibling bag,
        # ``_bag`` will still target a fresh unqualified bag, so
        # skipping the add is the only way to avoid appending a
        # duplicate that ``remove_vireo_location_keywords`` can't
        # clean up (its ownership is False).
        flat_leaf_present = parts[-1] in _all_photo_scoped_values(
            self.document.root, f"{{{NS_DC}}}subject",
        )
        flat_to_add = set() if flat_leaf_present else {parts[-1]}
        hier_to_add = set() if existed_hier else {path}
        self.keywords.add_keywords(
            flat_keywords=flat_to_add, hierarchical_keywords=hier_to_add,
        )
        return added_flat, added_hier

    def set_location_keywords(self, path_parts):
        """Write the assigned place as Lightroom keywords, creating a sidecar.

        ``path_parts`` is the location keyword chain from broadest to leaf,
        e.g. ``["United States", "California", "Kumeyaay Lake"]``. The leaf
        goes into ``dc:subject`` and the whole chain into
        ``lr:hierarchicalSubject`` as one pipe-delimited entry -- the shape
        Lightroom reads back as a nested keyword. Ancestors are deliberately
        not written as separate flat entries: Lightroom derives them from the
        hierarchy, and writing them would put "United States" in the user's
        flat keyword list for every photo.

        An empty chain means the photo no longer has a location, which is the
        removal case. Rewriting a photo whose place changed strips the entries
        recorded by the previous write before adding the new ones, so the
        sidecar never accumulates every place a photo has ever been assigned.
        """
        parts = [part for part in (path_parts or []) if part and part.strip()]
        if not parts:
            return self.remove_vireo_location_keywords()

        self._reject_piped_location_parts(parts)

        path = "|".join(parts)
        was_dirty = self.document.dirty
        desc = self.document.description()
        previous = self.document.get(LOCATION_KEYWORDS_MARKER)
        previous_owned = self.document.get(LOCATION_KEYWORDS_OWNED)
        if previous and previous != path:
            self._remove_location_keyword_entries(previous, previous_owned)

        leaf_key, existed_flat, existed_hier = (
            self._location_keywords_already_present(parts)
        )

        self._canonicalize_qualified_location_leaf(leaf_key, parts[-1])

        added_flat, added_hier = self._add_location_keyword_entries(
            parts, path, existed_flat, existed_hier,
        )

        # A no-op rewrite of the same path must not shrink an ownership
        # claim we made on a previous run: if the first write inserted an
        # entry, a second one that finds it already present (because we
        # wrote it) is still ours. Only a change of path resets ownership,
        # since ``_remove_location_keyword_entries`` above already stripped
        # the previous entries we owned.
        prior_flat, prior_hier = _parse_location_keywords_owned(previous_owned)
        if previous == path:
            owns_flat = prior_flat or added_flat
            owns_hier = prior_hier or added_hier
        else:
            owns_flat = added_flat
            owns_hier = added_hier

        self.properties.set_properties(
            desc,
            {
                LOCATION_KEYWORDS_MARKER: path,
                LOCATION_KEYWORDS_OWNED: _format_location_keywords_owned(
                    owns_flat, owns_hier,
                ),
            },
        )
        return self.document.dirty != was_dirty

    def remove_vireo_location_keywords(self):
        """Remove location keywords only when Vireo previously wrote them."""
        if not self.document.readable():
            return False
        desc = self.document.find_description()
        if desc is None:
            return False
        previous = self.document.get(LOCATION_KEYWORDS_MARKER)
        if not previous:
            return False
        self._remove_location_keyword_entries(
            previous, self.document.get(LOCATION_KEYWORDS_OWNED),
        )
        # Clearing the marker is itself a change worth publishing: it is what
        # keeps a later re-enable from treating stale entries as ours.
        self.properties.delete_property(LOCATION_KEYWORDS_MARKER)
        self.properties.delete_property(LOCATION_KEYWORDS_OWNED)
        self.document.dirty = True
        return True

    def release_location_flat_ownership_for(self, keywords):
        """Drop the marker's flat ownership when an ordinary add claims the leaf.

        When the sync queue holds a ``keyword_add`` for the same leaf name
        Vireo previously wrote as a location keyword, ``add_keywords()`` is a
        no-op (the entry is already in ``dc:subject``) -- but a later
        ``remove_vireo_location_keywords()`` or a place-change would then
        strip the entry the user asked us to keep. Rewrite the owned marker
        so the flat leaf is no longer claimed as ours; the hierarchical
        ownership is left alone because an ordinary ``keyword_add`` only
        touches ``dc:subject``. A no-op when the sidecar has no marker or
        no matching keyword is queued.
        """
        if not keywords:
            return False
        if not self.document.readable():
            return False
        desc = self.document.find_description()
        if desc is None:
            return False
        previous = self.document.get(LOCATION_KEYWORDS_MARKER)
        if not previous:
            return False
        owned = self.document.get(LOCATION_KEYWORDS_OWNED)
        owns_flat, owns_hier = _parse_location_keywords_owned(owned)
        if not owns_flat:
            return False
        leaf, _path = location_keyword_entries(previous)
        leaf_key = keyword_match_key(leaf)
        if not leaf_key:
            return False
        for kw in keywords:
            if keyword_match_key(kw) == leaf_key:
                self.properties.set_properties(desc, {
                    LOCATION_KEYWORDS_OWNED:
                        _format_location_keywords_owned(False, owns_hier),
                })
                self.document.dirty = True
                return True
        return False

    def _remove_location_keyword_entries(self, marker_value, owned_value):
        """Strip only the entries a previous location-keyword write inserted.

        ``owned_value`` says which of the flat leaf and hierarchical path
        the earlier write actually authored (a missing companion attribute
        is treated as both, to keep removal working on legacy sidecars).
        The recorded marker holds the exact text Vireo wrote, so prefer
        entries whose text is that exact spelling: a user or metadata tool
        that later adds a normalized variant (``paris`` beside our
        canonical ``Paris``) must not lose their entry when Vireo cleans
        up. Only when no exact match survives -- e.g. Lightroom rewrote
        our entry with different casing or spacing -- do we fall back to
        a single normalized match, which still catches the rewritten
        entry without deleting every normalized variant a user may have
        added since. The flat match is restricted to the leaf name and
        the hierarchical match requires the whole recorded path.
        """
        leaf, path = location_keyword_entries(marker_value)
        if not path:
            return False
        owns_flat, owns_hier = _parse_location_keywords_owned(owned_value)
        leaf_key = keyword_match_key(leaf)
        path_keys = [keyword_match_key(part) for part in path.split("|")]
        removed = []
        parent_map = _build_parent_map(self.document.root)

        # Collect PLAIN exact matches across every photo-scoped bag
        # first. If Vireo's own canonical entry lives in one bag and a
        # user variant lives in another, per-bag fallback would delete
        # both: the variant as the first bag's fallback (no exact
        # there) and the canonical from the second. Fall back to a
        # normalized match only when no plain exact match survives
        # anywhere. Vireo's own writes create plain-text
        # ``<rdf:li>Value</rdf:li>`` entries, never qualified ones,
        # and never under a container-qualified bag. A qualified exact
        # match -- item-level (``foo:source``, an own ``xml:lang``,
        # ``rdf:parseType``) OR container-level (an ``xml:lang="fr"``
        # on the ``dc:subject`` or ``rdf:Bag`` that reaches this item
        # by inheritance) -- is therefore a user- or tool-added
        # duplicate carrying its own metadata, and removing it would
        # silently discard that data. Prefer plain exact occurrences;
        # preserve qualified duplicates; a qualified exact-match
        # duplicate must NOT suppress the normalized fallback that
        # removes Vireo's owned spelling variant (say Vireo has plain
        # ``paris`` and a user added qualified ``Paris``): otherwise
        # the ownership marker clears while Vireo's ``paris`` stays
        # behind forever.
        if owns_flat and leaf_key:
            flat_bags = list(_photo_scoped_bags(
                self.document.root, f"{{{NS_DC}}}subject",
            ))
            value = _remove_one_owned_bag_entry(
                flat_bags, leaf, parent_map,
                lambda li_value: keyword_match_key(li_value) == leaf_key,
            )
            if value is not None:
                removed.append(value)

        if owns_hier:
            hier_bags = list(_photo_scoped_bags(
                self.document.root, f"{{{NS_LR}}}hierarchicalSubject",
            ))
            value = _remove_one_owned_bag_entry(
                hier_bags, path, parent_map,
                lambda li_value: [
                    keyword_match_key(s) for s in li_value.split("|")
                ] == path_keys,
            )
            if value is not None:
                removed.append(value)

        if removed:
            self.document.dirty = True
            log.info(
                "Removed Vireo location keywords from %s: %s", self.document.path, removed,
            )
        return bool(removed)
