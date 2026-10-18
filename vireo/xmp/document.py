"""The parsed sidecar a ``SidecarEditor`` edits, and its publication."""

import logging
from pathlib import Path
from xml.etree import ElementTree as ET

from xmp.namespaces import NS_RDF, NS_X, NS_XML
from xmp.photo_scope import (
    _VIREO_PHOTO_NODE_ID,
    _effective_xml_base,
    _get_property,
    _photo_subject,
    _register_document_uri,
    _top_descriptions,
)
from xmp.publish import _write_tree_atomic
from xmp.rdf import (
    _ancestor_carries_xml_qualifier,
    _ancestor_xml_attributes,
    _build_parent_map,
)

# Every module of the package logs as ``xmp``, the logger the single-module
# version used.
log = logging.getLogger("xmp")


class _SidecarDocument:
    """One sidecar's tree, parsed at most once, and whether it has changed.

    Owns the parse state (``root``, ``tree``, whether the file existed and
    parsed cleanly), the ``dirty`` flag every edit sets when it changes the
    tree, the Descriptions new properties land on, and the single publish in
    :meth:`commit`.
    """

    def __init__(self, xmp_path):
        self.path = xmp_path
        self.root = None
        self.tree = None
        self._loaded = False
        self._existed = False
        self._parse_failed = False
        self.dirty = False

    # ── Loading ─────────────────────────────────────────────────────────

    def _load(self):
        """Parse the sidecar once, or start a fresh tree when there is none."""
        if self._loaded:
            return
        self._loaded = True
        path = Path(self.path)
        try:
            # Parse directly instead of stat-then-open: one fewer network
            # round trip per sidecar on SMB/NFS.
            tree = ET.parse(path)
        except FileNotFoundError:
            pass
        except ET.ParseError:
            # Its contents are unusable either way: a write replaces the file
            # with a fresh tree, and a pruning operation leaves it alone.
            self._existed = True
            self._parse_failed = True
            log.warning("Corrupt XMP file: %s", path)
        else:
            self._existed = True
            self.root = tree.getroot()
            self.tree = tree
            _register_document_uri(self.root, self.path)
            return
        self.root = ET.Element(f"{{{NS_X}}}xmpmeta")
        self.tree = ET.ElementTree(self.root)
        _register_document_uri(self.root, self.path)

    def readable(self):
        """True when the sidecar existed on disk and parsed cleanly.

        Operations that only ever prune existing metadata check this so a
        missing or corrupt sidecar stays a no-op instead of being recreated
        from an empty tree.
        """
        self._load()
        return self._existed and not self._parse_failed

    # ── Tree access ─────────────────────────────────────────────────────

    def description(self):
        """Return the first rdf:Description, creating the scaffolding if needed.

        New properties are written here. Existing ones are updated wherever
        they already live (see :meth:`_SimpleProperties.set_properties`), because ExifTool
        spreads them across one Description per namespace.
        """
        desc = self.find_description()
        if desc is not None:
            return desc
        if self.root.tag == f"{{{NS_RDF}}}RDF":
            rdf = self.root
        else:
            rdf = self.root.find(f"{{{NS_RDF}}}RDF")
            if rdf is None:
                rdf = ET.SubElement(self.root, f"{{{NS_RDF}}}RDF")
                self.dirty = True
        desc = ET.SubElement(rdf, f"{{{NS_RDF}}}Description")
        # Pin the new Description to the photo's RDF subject. Without this,
        # a sidecar whose existing Descriptions all describe an auxiliary
        # resource (rdf:about="uuid:...") would land the photo's rating and
        # GPS on a Description that _top_descriptions no longer treats as
        # the photo -- the write would be invisible on the next read.
        # Copy the raw ``rdf:about'' spelling from the first existing
        # photo-scoped Description rather than the URI-resolved subject
        # key, so a sidecar whose photo Descriptions all use
        # ``rdf:about=""'' keeps that idiomatic empty spelling rather
        # than gaining a literal ``file:///path/photo.xmp''.
        about, node, rid = self._photo_write_subject()
        if about:
            desc.set(f"{{{NS_RDF}}}about", about)
        if node:
            desc.set(f"{{{NS_RDF}}}nodeID", node)
        if rid:
            desc.set(f"{{{NS_RDF}}}ID", rid)
        self.dirty = True
        return desc

    def _photo_write_subject(self):
        """Return the subject spelling a new photo Description should use.

        The new Description will sit under ``rdf:RDF'' with no local
        ``xml:base'', so any raw ``rdf:about'' spelling that
        depended on local context (an ``xml:base'' on the matching
        Description, for instance) would drift once placed here.
        Use the resolved absolute URI ``_photo_subject'' returns so
        the new Description is context-independent -- with one
        exception: when the photo's resolved subject equals what
        an empty ``rdf:about'' would resolve to at the new
        Description's own context, keep the idiomatic empty
        spelling so plain sidecars stay unchanged. That check
        computes the effective base an empty ``rdf:about'' at
        ``rdf:RDF'' would see; only when the resolved subject
        matches is the empty spelling safe.

        When the resolved subject is empty (either the literal
        ``rdf:about=""'' or the ambiguous fallback from
        ``_photo_subject'') AND an ancestor ``xml:base'' would
        leak into the new Description, a bare ``rdf:about=""''
        would silently resolve into that inherited base and
        fingerprint to a non-empty subject that
        ``_top_descriptions'' can't select on re-read. Pin the
        new Description with ``rdf:nodeID=_VIREO_PHOTO_NODE_ID''
        (a context-independent blank node): ``_photo_subject''
        recognizes that nodeID as the ambiguous-packet photo,
        so Vireo's own writes stay reachable while
        standards-compliant consumers see the write clearly as
        an anonymous Vireo-owned resource instead of one
        misleadingly attributed to the inherited-base subject.

        ``rdf:nodeID'' / ``rdf:ID'' subjects don't resolve, so they
        pass through unchanged.
        """
        resolved = _photo_subject(self.root)
        about, node, rid = resolved
        parent_map = _build_parent_map(self.root)
        anchor = self.root
        if anchor.tag != f"{{{NS_RDF}}}RDF":
            candidate = anchor.find(f"{{{NS_RDF}}}RDF")
            if candidate is not None:
                anchor = candidate
        empty_at_anchor = _effective_xml_base(anchor, parent_map)
        if not about:
            if node or rid:
                return resolved
            if empty_at_anchor:
                return ("", _VIREO_PHOTO_NODE_ID, "")
            return resolved
        if not empty_at_anchor:
            return resolved
        if about == empty_at_anchor:
            return ("", node, rid)
        return resolved

    def find_description(self):
        """Return the first top-level rdf:Description, or None when there is none."""
        self._load()
        descriptions = _top_descriptions(self.root)
        return descriptions[0] if descriptions else None

    def unqualified_photo_description(self):
        """Return a photo-scoped Description free of inherited ``xml:*``.

        ``xml:lang`` (and the other ``xml:*`` attributes) are
        inherited from every ancestor, so any new property added
        under a Description that has one of its own -- or inherits
        one from ``rdf:RDF`` / ``x:xmpmeta`` above -- would silently
        carry that qualifier. Prefer an existing photo-scoped
        Description that carries none of its own; when none exists,
        create a fresh Description scoped to the photo and
        explicitly reset any ``xml:*`` an ancestor carries so the new
        Description starts clean.
        """
        parent_map = _build_parent_map(self.root)
        for desc in _top_descriptions(self.root):
            # ``xml:lang=""`` on the Description itself explicitly
            # cancels every ancestor's language, so a Description
            # carrying only empty ``xml:*`` values is still safe to
            # write new properties under. Use the effective-qualifier
            # check to accept it.
            if not _ancestor_carries_xml_qualifier(desc, parent_map):
                return desc
        # Capture the photo's subject BEFORE inserting the new
        # Description. If every existing photo Description is qualified
        # and pinned to a unique non-empty subject like
        # ``rdf:about='uuid:photo'``, appending a subjectless
        # Description first would make ``_photo_subject`` see two
        # distinct subjects (``uuid:photo`` and empty) on its next call
        # and fall back to the empty subject -- unscoping every
        # original photo Description.
        about, node, rid = self._photo_write_subject()
        if self.root.tag == f"{{{NS_RDF}}}RDF":
            rdf = self.root
        else:
            rdf = self.root.find(f"{{{NS_RDF}}}RDF")
            if rdf is None:
                rdf = ET.SubElement(self.root, f"{{{NS_RDF}}}RDF")
                self.dirty = True
        desc = ET.SubElement(rdf, f"{{{NS_RDF}}}Description")
        if about:
            desc.set(f"{{{NS_RDF}}}about", about)
        if node:
            desc.set(f"{{{NS_RDF}}}nodeID", node)
        if rid:
            desc.set(f"{{{NS_RDF}}}ID", rid)
        # Explicitly reset the ``xml:*`` attributes that permit an
        # empty cancelling value. ``xml:lang=""`` is XML's way of
        # saying "no known language", so it cancels an inherited
        # language on the new Description's descendants. Other
        # inherited ``xml:*`` attributes have stricter grammars --
        # ``xml:space`` accepts only ``default`` or ``preserve``,
        # ``xml:id`` must be a valid non-empty identifier -- so we
        # leave them alone here rather than emitting invalid XML.
        # The new Description isn't in ``parent_map`` yet, so walk
        # from its known parent (``rdf``) upward: those attributes
        # are what it would inherit.
        xml_lang = f"{{{NS_XML}}}lang"
        if xml_lang in _ancestor_xml_attributes(rdf, parent_map):
            desc.set(xml_lang, "")
        self.dirty = True
        return desc

    def get(self, name):
        """Return a simple property's current value, or None when absent."""
        self._load()
        return _get_property(self.root, name)

    # ── Publication ─────────────────────────────────────────────────────

    def commit(self):
        """Publish the accumulated edits; a no-op when nothing changed.

        Returns True when the sidecar was written.
        """
        if not self.dirty:
            return False
        ET.indent(self.tree, space="  ")
        _write_tree_atomic(self.tree, self.path)
        self.dirty = False
        self._existed = True
        self._parse_failed = False
        return True
