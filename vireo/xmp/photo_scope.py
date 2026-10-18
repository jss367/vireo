"""Which ``rdf:Description`` elements describe the photo, and lookups scoped to them.

A sidecar can describe resources other than the photo (an auxiliary
``rdf:about="#thumbnail"``, a struct value) and can spell the photo's own
subject several equivalent ways. This module resolves the photo's subject,
including ``xml:base`` and the sidecar's own document URI, and reads simple
properties and keyword bags only from the Descriptions that carry it.
"""

import contextlib
import urllib.parse
import weakref
from pathlib import Path

from xmp.namespaces import NS_RDF, NS_XML
from xmp.rdf import (
    _build_parent_map,
    _is_plain_property_occurrence,
    _property_bag_and_wrappers,
    _property_occurrence_score,
    _qualified_value,
    _read_bag_values,
)


def _all_photo_scoped_values(root, tag):
    """Return every ``rdf:li`` text under the photo's ``tag`` bags.

    Unlike ``_read_bag_values`` (which reads one bag), this walks every
    photo-scoped occurrence of ``tag`` -- direct child of a photo
    Description, or wrapped in an ``rdf:value`` / ``rdf:Description``
    -- so a keyword's presence is judged against the entire photo
    subject, not just the bag ``_bag`` picked as its merge target.
    """
    values = set()
    for bag in _photo_scoped_bags(root, tag):
        values.update(_read_bag_values(bag))
    return values


def _all_top_descriptions(root):
    """Return every top-level ``rdf:Description``, ignoring photo scoping.

    Nested Descriptions (struct values inside a property) are not included --
    their attributes belong to that struct, not to any top-level subject.
    """
    if root.tag == f"{{{NS_RDF}}}RDF":
        rdfs = [root]
    else:
        rdfs = root.findall(f"{{{NS_RDF}}}RDF")
    return [
        desc for rdf in rdfs for desc in rdf.findall(f"{{{NS_RDF}}}Description")
    ]


# Stable synthetic document URI used when no ``xml:base'' is set on
# any ancestor of a Description and the sidecar's real document URI
# hasn't been registered. Only equivalent RELATIVE spellings of the
# same non-empty ``rdf:about'' need to fingerprint together
# (``photo.jpg'' vs. ``./photo.jpg''). An empty or absent
# ``rdf:about'' stays in the empty-subject bucket. This synthetic
# base is only reached for a genuine relative URI reference (and it
# never changes the meaning of a fully-qualified reference, since
# ``urljoin'' honors the reference's own scheme over the base).
_SUBJECT_FALLBACK_BASE = "file:///_vireo_xmp_/"


# Blank-node identifier Vireo pins to a new Description when it must
# write to an ambiguous packet -- one where ``_photo_subject''
# returns the empty fallback because sibling empty-origin
# Descriptions carry distinct local ``xml:base'' values, so a bare
# ``rdf:about=""'' would silently inherit the ancestor base and
# resolve to a NON-empty subject that ``_top_descriptions'' can't
# select on re-read. ``rdf:nodeID'' is context-independent (a blank
# node ignores ``xml:base'') and self-labels the write as Vireo's,
# so standards-compliant consumers see it clearly instead of a
# write that appears to be about the inherited-base subject.
# ``_photo_subject'' recognizes this nodeID as the photo when the
# packet would otherwise be ambiguous.
_VIREO_PHOTO_NODE_ID = "vireoPhoto"


# Registered sidecar document URIs, keyed on the parsed root element.
# When ``read_sync_preview_metadata'' or ``SidecarEditor'' registers
# the real path, ``_description_subject'' can fold non-empty relative
# ``rdf:about'' spellings with the equivalent absolute ones -- so a
# sidecar carrying ``rdf:about="photo.jpg"'' alongside
# ``rdf:about="file:///photos/photo.jpg"'' in ``/photos/photo.xmp''
# resolves both to the same subject key.
_DOCUMENT_URIS = weakref.WeakKeyDictionary()


def _register_document_uri(root, xmp_path):
    """Record the file URI for ``xmp_path'' keyed on the parsed ``root''.

    Uses ``Path.absolute()'' rather than ``Path.resolve()'' so a
    sidecar accessed through a symlinked directory keeps the
    retrieval path in the URI (Codex finding: otherwise a relative
    ``rdf:about="photo.jpg"'' folds against the physical target
    while a sibling ``rdf:about="file:///alias/photo.jpg"'' keeps
    the alias, and ``_photo_subject'' treats the packet as
    ambiguous).
    """
    try:
        uri = Path(xmp_path).absolute().as_uri()
    except (ValueError, OSError):
        return
    # Some element implementations don't support weakref; the
    # synthetic fallback base still handles relative-only equivalence
    # when that happens.
    with contextlib.suppress(TypeError):
        _DOCUMENT_URIS[root] = uri


def _document_uri_for(elem, parent_map):
    """Return the sidecar URI registered for ``elem''s parsed root, or None."""
    current = elem
    seen = 0
    while current is not None and seen < 4096:
        uri = _DOCUMENT_URIS.get(current)
        if uri is not None:
            return uri
        current = parent_map.get(current)
        seen += 1
    return None


def _effective_xml_base(elem, parent_map):
    """Return the effective ``xml:base`` URI for ``elem``, or the empty string.

    Composes every ancestor's ``xml:base'' per the XML Base
    recommendation. When at least one ``xml:base'' is declared and
    the composed value is still a relative reference, resolves it
    against the sidecar's document URI so ``xml:base="sub/"''
    becomes an absolute path (Codex finding: otherwise a relative
    base leaves ``photo.jpg'' relative and it can't fold with a
    sibling absolute-URI Description).

    Returns ``""'' when no ``xml:base'' is declared anywhere in the
    ancestor chain, even if the sidecar's document URI is
    registered: an *empty* ``rdf:about'' should keep the
    enclosing-photo convention rather than resolving into the
    sidecar's own URI (which would knock the "empty subject wins"
    priority off in ``_photo_subject''). ``_description_subject''
    handles the non-empty relative case separately, using the
    document URI as an implicit resolver only when the reference
    itself isn't empty.
    """
    xml_base = f"{{{NS_XML}}}base"
    chain = []
    current = elem
    while current is not None:
        chain.append(current)
        current = parent_map.get(current)
    chain.reverse()
    base = ""
    declared = False
    for anc in chain:
        b = anc.get(xml_base)
        if b is not None:
            declared = True
            base = urllib.parse.urljoin(base, b) if base else b
    if not declared:
        return ""
    if not urllib.parse.urlparse(base).scheme:
        doc_uri = _document_uri_for(elem, parent_map)
        if doc_uri:
            base = urllib.parse.urljoin(doc_uri, base)
    return base


def _description_subject(desc, parent_map=None):
    """Return the ``(rdf:about, rdf:nodeID, rdf:ID)`` tuple identifying a Description.

    An empty or missing ``rdf:about`` means "the enclosing resource", which
    for a sidecar file is the photo it sits next to. ``rdf:nodeID`` and
    ``rdf:ID`` both name distinct resources; they are never the enclosing
    photo and each must fingerprint separately, so a Description carrying
    one cannot alias into the empty-subject bucket.

    ``rdf:about`` is a URI reference that must be resolved against the
    Description's effective ``xml:base'' before it can be compared with
    other subjects. When a caller passes ``parent_map`` the resolution
    happens here, so ``rdf:about="photo.jpg"'' under
    ``xml:base="file:///photos/"'' fingerprints the same as
    ``rdf:about="file:///photos/photo.jpg"''; an *explicitly empty*
    ``rdf:about=""'' is a URI reference too (RFC 3986 §4.2, the
    empty reference), so it resolves to the base URI when one is
    present. An *absent* ``rdf:about'' is per RDF/XML spec
    equivalent to an ``rdf:about=""'' -- both mean "the enclosing
    resource" -- and must resolve the same way, so both spellings
    fingerprint together whether or not ``xml:base'' is set.

    When no ``xml:base'' is set anywhere, a *relative* non-empty
    ``rdf:about'' still needs a fallback base so equivalent
    spellings like ``photo.jpg'' and ``./photo.jpg'' collapse to
    the same subject; a stable synthetic base
    (:data:`_SUBJECT_FALLBACK_BASE`) is used for that. An
    absent-or-empty ``rdf:about'' stays in the empty bucket, and a
    fully-qualified relative reference (with its own scheme) is
    unaffected because ``urljoin'' honors the reference's own
    scheme over the base.

    Without ``parent_map`` the raw text is used, matching the
    pre-xml:base callers that resolve subjects against one another
    only when they were spelled identically.
    """
    about = desc.get(f"{{{NS_RDF}}}about")
    node = desc.get(f"{{{NS_RDF}}}nodeID")
    rid = desc.get(f"{{{NS_RDF}}}ID")
    if parent_map is not None and (about is not None or (not node and not rid)):
        # ``xml:base'' only affects ``rdf:about'' (and ``rdf:ID''),
        # which are URI references. When a Description uses
        # ``rdf:nodeID'' or ``rdf:ID'' as its subject and carries
        # no ``rdf:about'' at all, the effective base is irrelevant
        # -- the subject is the blank-node id or fragment id, not
        # a URI to resolve. Composing the base into an empty
        # ``about'' anyway would leak the inherited base into
        # the tuple's first slot and knock the Description out of
        # the blank-node / fragment bucket that ``_photo_subject''
        # matches against.
        base = _effective_xml_base(desc, parent_map)
        if base:
            about = urllib.parse.urljoin(base, about or "")
        elif about:
            # Non-empty relative ``rdf:about'' needs a fallback base
            # so equivalent spellings collapse. Prefer the sidecar's
            # actual document URI when it's registered (that folds
            # ``photo.jpg'' with an equivalent absolute
            # ``file:///photos/photo.jpg'' too), and fall back to
            # a stable synthetic base otherwise.
            base = _document_uri_for(desc, parent_map) or _SUBJECT_FALLBACK_BASE
            about = urllib.parse.urljoin(base, about)
    return (about or "", node or "", rid or "")


def _photo_subject(root):
    """Return the ``(about, nodeID, ID)`` tuple identifying the photo's Descriptions.

    ``rdf:about=""`` (or a missing attribute) is the sidecar convention for
    "the enclosing resource" -- the photo. When no top-level Description
    carries the empty subject but exactly one non-fragment, non-blank-node,
    non-``rdf:ID`` ``rdf:about`` remains after filtering out clearly
    auxiliary side-resources (any URI carrying a ``#fragment`` component,
    a blank-node ``rdf:nodeID``, a locally-scoped ``rdf:ID``), treat that
    ``rdf:about`` as the photo's. So a sidecar that pins its Description
    to ``uuid:photo`` alongside an auxiliary ``#thumbnail`` or
    ``uuid:photo#thumbnail`` Description is still handled coherently: a
    URI with any ``#`` in it names a resource *inside* the packet per
    RFC 3986, never the enclosing photo. When more than one
    photo-candidate subject remains, refuse to guess from document order
    and fall back to the empty subject: reads then return nothing rather
    than an auxiliary resource's rating or GPS, and writes land on a
    fresh Description that is unambiguously the photo's.
    """
    empty = ("", "", "")
    descriptions = _all_top_descriptions(root)
    if not descriptions:
        return empty
    parent_map = _build_parent_map(root)
    subjects = set()
    empty_origin = set()
    rdf_about = f"{{{NS_RDF}}}about"
    rdf_node = f"{{{NS_RDF}}}nodeID"
    rdf_id = f"{{{NS_RDF}}}ID"
    for desc in descriptions:
        subject = _description_subject(desc, parent_map)
        subjects.add(subject)
        # Track Descriptions that ORIGINATED from an empty/absent
        # ``rdf:about'' (with no blank-node or rdf:ID). Under an
        # explicit ``xml:base'' they resolve into the base URI, so
        # ``empty in subjects'' no longer fires -- but the enclosing-
        # photo convention still applies: prefer their resolved
        # subject over any non-fragment auxiliary sibling.
        if (
            not desc.get(rdf_about)
            and not desc.get(rdf_node)
            and not desc.get(rdf_id)
        ):
            empty_origin.add(subject)
    if empty in subjects:
        return empty
    if len(empty_origin) == 1:
        # Every empty-originated Description resolves identically
        # (they share the same effective ``xml:base''), so their
        # resolved subject names the photo. When more than one
        # empty-origin subject exists the packet's Descriptions
        # carry conflicting local ``xml:base'' values and can't
        # agree on the photo -- fall back to the empty (ambiguous)
        # subject rather than picking one arbitrarily.
        return next(iter(empty_origin))
    vireo_marker = ("", _VIREO_PHOTO_NODE_ID, "")
    if empty_origin:
        # Multiple conflicting empty-origin subjects. Vireo's
        # own ambiguous-fallback writes are pinned to
        # ``rdf:nodeID=_VIREO_PHOTO_NODE_ID'' (context-independent,
        # not affected by any inherited ``xml:base''); prefer
        # that marker when present so the write is selectable
        # on re-read, and fall back to the empty (ambiguous)
        # subject otherwise.
        if vireo_marker in subjects:
            return vireo_marker
        return empty
    photo_candidates = {
        (about, node, rid)
        for (about, node, rid) in subjects
        if about
        and "#" not in about
        and not node
        and not rid
    }
    if len(photo_candidates) != 1:
        # Same ambiguity, from a different direction: no consensus
        # among non-fragment, non-blank-node, non-``rdf:ID'' photo
        # candidates. Prefer Vireo's ambiguous-fallback marker if
        # present so its own writes stay reachable.
        if vireo_marker in subjects:
            return vireo_marker
        return empty
    return next(iter(photo_candidates))


def _top_descriptions(root):
    """Return the top-level ``rdf:Description`` elements that describe the photo.

    A sidecar may split its properties across several Descriptions: ExifTool
    writes one per namespace, and XMP allows any number. Nested Descriptions
    (struct values inside a property) are not included -- their attributes
    belong to that struct, not to the photo. Descriptions of a different RDF
    subject (an auxiliary resource such as ``rdf:about="#aux"``) are also
    excluded, so reading and writing photo properties can never land on --
    or pick up -- someone else's rating, GPS or keywords.
    """
    subject = _photo_subject(root)
    parent_map = _build_parent_map(root)
    return [
        desc for desc in _all_top_descriptions(root)
        if _description_subject(desc, parent_map) == subject
    ]


def _property_occurrences(root, name):
    """Return ``(desc, element)`` for every place a simple property is stored.

    XMP spells a simple property either as an attribute of a Description
    (``element`` is None) or as a child element holding the value as text,
    which is ExifTool's layout. Readers and writers must honour both, or a
    write lands beside the existing value instead of replacing it.
    """
    found = []
    for desc in _top_descriptions(root):
        if name in desc.attrib:
            found.append((desc, None))
        for child in desc.findall(name):
            found.append((desc, child))
    return found


def _photo_scoped_bags(root, tag):
    """Yield each ``rdf:Bag`` under a photo-scoped Description's property.

    ``tag`` is the Clark-notation property name (e.g. ``{dc}subject``).
    A property may appear more than once across the photo's Descriptions;
    a plain ``.//`` search would also match bags nested inside struct
    values or Descriptions of a different RDF subject (an auxiliary
    resource such as ``rdf:about="#aux"``). For keyword arrays that would
    import someone else's ``dc:subject`` or ``lr:hierarchicalSubject`` as
    the photo's keywords, and delete or rewrite them during a sync.

    A qualified keyword array wraps the bag in an ``rdf:value``, an
    ``rdf:Description``, or both, so follow whichever indirection the
    sidecar uses; otherwise readers hide the keywords and a keyword
    addition creates a second unqualified property beside the
    qualified one instead of merging with it.
    """
    for desc in _top_descriptions(root):
        for prop in desc.findall(tag):
            bag, _wrappers = _property_bag_and_wrappers(prop)
            if bag is not None:
                yield bag


def _plain_property_occurrences(root, name):
    """Return plain occurrences of ``name'', child-element form first.

    Child-element forms are the canonical XMP serialization and
    every write path targets one first, so callers that need "the
    plain occurrence Vireo would write to next" iterate this rather
    than the raw :func:`_property_occurrences` list (which returns
    attribute-form ahead of child elements per Description).
    """
    parent_map = _build_parent_map(root)
    plain = [
        entry for entry in _property_occurrences(root, name)
        if _is_plain_property_occurrence(entry, parent_map)
    ]
    plain.sort(key=lambda entry: 0 if entry[1] is not None else 1)
    return plain


def _get_plain_property_value(root, name):
    """Return the value of the first plain occurrence, or None.

    Callers that need Vireo's own written value -- a coordinate
    Vireo just assigned via ``_SimpleProperties.set_plain_property_value'', for
    instance -- read through this helper instead of
    :func:`_get_property`. It ignores qualified occurrences another
    tool authored (which score higher for read authority but don't
    reflect Vireo's own write), so the sync preview shows what
    Vireo actually wrote while Vireo owns the property. Child-element
    plain forms are preferred over attribute forms so the reader
    stays in sync with the writer's own preference.
    """
    plain = _plain_property_occurrences(root, name)
    if not plain:
        return None
    owner, child = plain[0]
    if child is None:
        return owner.get(name)
    if f"{{{NS_RDF}}}value" in child.attrib or len(child):
        return _qualified_value(child)
    return (child.text or "").strip()


def _get_property(root, name):
    """Return a simple property's value from whichever form stores it."""
    found = _property_occurrences(root, name)
    if not found:
        return None
    parent_map = _build_parent_map(root)
    desc, child = max(
        found,
        key=lambda entry: _property_occurrence_score(entry, parent_map),
    )
    if child is None:
        return desc.get(name)
    # Any qualified form -- element or RDF/XML attribute abbreviation,
    # short or long -- routes through ``_qualified_value``:
    #   <xmp:Rating rdf:parseType='Resource'>
    #     <rdf:value>3</rdf:value><...qualifiers.../>
    #   </xmp:Rating>
    # or
    #   <xmp:Rating><rdf:Description rdf:value='3' foo:source='camera'/>
    #   </xmp:Rating>
    # etc. The container's own ``text`` is only whitespace between
    # its children, so returning it would hide the value from every
    # reader.
    if f"{{{NS_RDF}}}value" in child.attrib or len(child):
        return _qualified_value(child)
    return (child.text or "").strip()
