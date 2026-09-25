"""Consolidated XMP sidecar operations.

Provides read/write/merge/remove for XMP keyword and rating metadata.
All XMP namespace constants and helpers live here as the single source of truth.
"""

import contextlib
import copy
import errno
import logging
import math
import os
import stat
import sys
import urllib.parse
import uuid
import weakref
from pathlib import Path
from xml.etree import ElementTree as ET

from keyword_normalization import keyword_match_key

log = logging.getLogger(__name__)

# ── Namespace constants (single source of truth) ────────────────────────
NS_X = "adobe:ns:meta/"
NS_RDF = "http://www.w3.org/1999/02/22-rdf-syntax-ns#"
NS_DC = "http://purl.org/dc/elements/1.1/"
NS_LR = "http://ns.adobe.com/lightroom/1.0/"
NS_XMP = "http://ns.adobe.com/xap/1.0/"
NS_XMPDM = "http://ns.adobe.com/xmp/1.0/DynamicMedia/"
NS_EXIF = "http://ns.adobe.com/exif/1.0/"
NS_VIREO = "https://vireo.app/ns/1.0/"
NS_XML = "http://www.w3.org/XML/1998/namespace"

# RDF attributes that describe serialization form only. Anything else
# on a property, one of its wrappers, or its ``rdf:Bag`` -- including
# an identity attribute (``rdf:about``, ``rdf:nodeID``, ``rdf:ID``)
# that names a distinct RDF resource other statements may point at,
# or a typed-literal marker (``rdf:datatype``) that changes the
# value's semantics -- is a qualifier that would be silently dropped
# if we removed the element as a duplicate.
_STRUCTURAL_RDF_ATTRIBUTES = frozenset({
    f"{{{NS_RDF}}}parseType",
})

# Register namespaces so ET preserves prefixes on output
ET.register_namespace("x", NS_X)
ET.register_namespace("rdf", NS_RDF)
ET.register_namespace("dc", NS_DC)
ET.register_namespace("lr", NS_LR)
ET.register_namespace("xmp", NS_XMP)
ET.register_namespace("xmpDM", NS_XMPDM)
ET.register_namespace("crs", "http://ns.adobe.com/camera-raw-settings/1.0/")
ET.register_namespace("photoshop", "http://ns.adobe.com/photoshop/1.0/")
ET.register_namespace("exif", NS_EXIF)
ET.register_namespace("tiff", "http://ns.adobe.com/tiff/1.0/")
ET.register_namespace("aux", "http://ns.adobe.com/exif/1.0/aux/")
ET.register_namespace("vireo", NS_VIREO)


# ── Private helpers ─────────────────────────────────────────────────────

# Extended attributes no process is allowed to write. macOS stamps
# ``com.apple.provenance`` on files touched by a launched binary and only the
# kernel may set it, so copying it always fails. Losing it on the replacement
# costs nothing; refusing to publish the sidecar because of it costs the user
# every pending change.
_UNCOPYABLE_XATTRS = frozenset({"com.apple.provenance"})

# Errors that mean "this destination will never accept this attribute" rather
# than "this write went wrong". Retrying them on the next sync cannot help.
_XATTR_SKIP_ERRNOS = frozenset(
    {errno.EACCES, errno.EPERM, errno.ENOTSUP, errno.EOPNOTSUPP}
)

# Attributes that carry access control rather than annotation. Linux stores
# POSIX ACLs and security labels as extended attributes, so failing to copy
# one would publish a sidecar that is readable by more people than the
# original. Never skip these, whatever the errno.
#
# The Linux ``system.`` namespace is reserved for kernel-managed attributes
# with access-control semantics -- ``system.posix_acl_access`` /
# ``system.posix_acl_default`` on native filesystems, ``system.nfs4_acl`` on
# NFSv4 exports, ``system.richacl`` on RichACL mounts. Missing one and
# skipping it on EACCES/EPERM/ENOTSUP would publish a sidecar with weaker
# access than the original, so the prefix is the whole namespace rather than
# an enumerated allow-list.
_CRITICAL_XATTR_PREFIXES = ("system.", "security.")

# Attribute names already reported, so a 2,000-photo sync logs each cause once.
_reported_xattr_skips = set()

_darwin_xattr_api = None


def _log_skipped_xattr(name, source, error):
    """Record an extended attribute the replacement sidecar cannot carry."""
    if name in _UNCOPYABLE_XATTRS:
        log.debug("Skipping kernel-owned xattr %s on %s: %s", name, source, error)
        return
    if name not in _reported_xattr_skips:
        _reported_xattr_skips.add(name)
        log.warning(
            "Could not preserve extended attribute %s (e.g. on %s): %s",
            name, source, error,
        )


def _darwin_xattr():
    """Bind the macOS xattr syscalls once; Python's os module omits them."""
    global _darwin_xattr_api
    if _darwin_xattr_api is None:
        import ctypes

        libc = ctypes.CDLL(None, use_errno=True)
        libc.listxattr.argtypes = [ctypes.c_char_p, ctypes.c_char_p,
                                   ctypes.c_size_t, ctypes.c_int]
        libc.listxattr.restype = ctypes.c_ssize_t
        libc.getxattr.argtypes = [ctypes.c_char_p, ctypes.c_char_p,
                                  ctypes.c_void_p, ctypes.c_size_t,
                                  ctypes.c_uint32, ctypes.c_int]
        libc.getxattr.restype = ctypes.c_ssize_t
        libc.setxattr.argtypes = [ctypes.c_char_p, ctypes.c_char_p,
                                  ctypes.c_void_p, ctypes.c_size_t,
                                  ctypes.c_uint32, ctypes.c_int]
        libc.setxattr.restype = ctypes.c_int
        libc.removexattr.argtypes = [ctypes.c_char_p, ctypes.c_char_p,
                                     ctypes.c_int]
        libc.removexattr.restype = ctypes.c_int
        _darwin_xattr_api = (ctypes, libc)
    return _darwin_xattr_api


def _darwin_xattr_error(path):
    """Raise the errno the last xattr syscall on ``path`` set."""
    ctypes, _ = _darwin_xattr()
    code = ctypes.get_errno()
    raise OSError(code, os.strerror(code), str(path))


def _darwin_list_xattrs(path):
    ctypes, libc = _darwin_xattr()
    encoded = os.fsencode(path)
    size = libc.listxattr(encoded, None, 0, 0)
    if size < 0:
        _darwin_xattr_error(path)
    if size == 0:
        return []
    names = ctypes.create_string_buffer(size)
    size = libc.listxattr(encoded, names, size, 0)
    if size < 0:
        _darwin_xattr_error(path)
    return [n.decode() for n in names.raw[:size].split(b"\0") if n]


def _darwin_get_xattr(path, name):
    ctypes, libc = _darwin_xattr()
    encoded, key = os.fsencode(path), name.encode()
    size = libc.getxattr(encoded, key, None, 0, 0, 0)
    if size < 0:
        _darwin_xattr_error(path)
    value = ctypes.create_string_buffer(max(size, 1))
    size = libc.getxattr(encoded, key, value, size, 0, 0)
    if size < 0:
        _darwin_xattr_error(path)
    return value.raw[:size]


def _darwin_set_xattr(path, name, value):
    _, libc = _darwin_xattr()
    if libc.setxattr(os.fsencode(path), name.encode(), value, len(value), 0, 0) != 0:
        _darwin_xattr_error(path)


def _darwin_remove_xattr(path, name):
    _, libc = _darwin_xattr()
    if libc.removexattr(os.fsencode(path), name.encode(), 0) != 0:
        _darwin_xattr_error(path)


def _copy_xattrs(source, destination, list_xattrs, get_xattr, set_xattr,
                 remove_xattr):
    """Mirror ``source``'s extended attributes onto its replacement.

    Copy attribute by attribute rather than in one all-or-nothing call: a
    single attribute the destination refuses must not sink the whole sidecar
    write. macOS stamps ``com.apple.provenance`` on written files and no
    process may set it, so on an SMB share
    ``copyfile(COPYFILE_ACL | COPYFILE_XATTR)`` failed with EACCES for every
    sidecar that had already been written once -- the sync created each
    sidecar and could then never update it again.
    """
    def skippable(name, error):
        return (
            error.errno in _XATTR_SKIP_ERRNOS
            and not name.startswith(_CRITICAL_XATTR_PREFIXES)
        )

    attributes = {}
    for name in list_xattrs(source):
        try:
            attributes[name] = get_xattr(source, name)
        except OSError as error:
            if not skippable(name, error):
                raise
            _log_skipped_xattr(name, source, error)
    for name in set(list_xattrs(destination)) - attributes.keys():
        try:
            remove_xattr(destination, name)
        except OSError as error:
            if not skippable(name, error):
                raise
    for name, value in attributes.items():
        try:
            set_xattr(destination, name, value)
        except OSError as error:
            if not skippable(name, error):
                raise
            _log_skipped_xattr(name, source, error)


def _preserve_sidecar_access(source, destination, source_stat):
    """Copy access metadata, failing before replacement if preservation fails."""
    if os.name == "posix":
        destination_stat = destination.stat()
        if (destination_stat.st_uid, destination_stat.st_gid) != (
            source_stat.st_uid, source_stat.st_gid,
        ):
            os.chown(destination, source_stat.st_uid, source_stat.st_gid)

    os.chmod(destination, stat.S_IMODE(source_stat.st_mode))
    if sys.platform == "darwin":
        # macOS ACLs are not exposed through Python's xattr API. copyfile(3)
        # copies them natively; omit COPYFILE_DATA and COPYFILE_STAT so the
        # newly serialized content and its modification time stay intact.
        # Access control is not best-effort, so an ACL copy failure still
        # aborts before replacement.
        ctypes, _ = _darwin_xattr()

        # Copy extended attributes BEFORE applying the source ACL: a macOS
        # ACL can allow file-data writes but deny writeextattr, and once
        # the source ACL is in place, setxattr on the destination would
        # return EACCES and _copy_xattrs would then skip the (non-critical)
        # source metadata, publishing a sidecar without it.
        _copy_xattrs(source, destination, _darwin_list_xattrs,
                     _darwin_get_xattr, _darwin_set_xattr, _darwin_remove_xattr)

        copyfile = ctypes.CDLL(None, use_errno=True).copyfile
        copyfile.argtypes = [ctypes.c_char_p, ctypes.c_char_p, ctypes.c_void_p,
                             ctypes.c_uint32]
        copyfile.restype = ctypes.c_int
        copyfile_acl = 1 << 0
        if copyfile(os.fsencode(source), os.fsencode(destination), None,
                    copyfile_acl) != 0:
            error = ctypes.get_errno()
            raise OSError(error, os.strerror(error), str(source))
    elif hasattr(os, "listxattr"):
        # Linux exposes POSIX ACLs as system.posix_acl_access. Unlike
        # shutil.copystat, do not silently ignore permission-copy failures --
        # only the attributes the destination structurally cannot hold.
        _copy_xattrs(source, destination, os.listxattr, os.getxattr,
                     os.setxattr, os.removexattr)

    if hasattr(source_stat, "st_flags"):
        os.chflags(destination, source_stat.st_flags)


def _write_tree_atomic(tree, xmp_path):
    """Publish a complete sidecar without truncating the previous version.

    Keep the temporary file on the same filesystem for atomic replacement,
    preserve existing permissions, and follow sidecar symlinks just as the
    former direct write did. Failed writes leave the original intact.
    """
    path = Path(xmp_path).resolve()
    try:
        source_stat = path.stat()
    except FileNotFoundError:
        source_stat = None
    if source_stat is not None:
        # Replacing a directory entry bypasses the file's write protection on
        # POSIX. Probe write access without truncation to retain the previous
        # writer's permission checks, including ACLs and read-only flags.
        os.close(os.open(path, os.O_WRONLY))
    temp_path = None
    try:
        candidate = path.parent / f".vireo-xmp-{uuid.uuid4().hex}.tmp"
        # 0666 lets the process umask set permissions for new sidecars,
        # matching a direct write (mkstemp would silently restrict to 0600).
        fd = os.open(candidate, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o666)
        temp_path = candidate
        with os.fdopen(fd, "w", encoding="utf-8") as stream:
            tree.write(stream, xml_declaration=True, encoding="unicode")
            stream.flush()
            os.fsync(stream.fileno())
        if source_stat is not None:
            _preserve_sidecar_access(path, temp_path, source_stat)
        os.replace(temp_path, path)
    finally:
        if temp_path is not None:
            temp_path.unlink(missing_ok=True)


def _li_value(li):
    """Return an ``rdf:li``'s value text, following any qualifier form.

    XMP arrays permit a qualified item spelling, where each entry
    wraps its value in an ``rdf:value`` (or the RDF/XML attribute
    abbreviation): ``<rdf:li rdf:parseType='Resource'><rdf:value>
    Heron</rdf:value><foo:qual>...</foo:qual></rdf:li>``. The
    item's direct ``.text`` is then only whitespace between its
    children, so readers must resolve the nested value first;
    otherwise every keyword-set consumer misses the qualified
    entry.
    """
    rdf_value_tag = f"{{{NS_RDF}}}value"
    if rdf_value_tag in li.attrib:
        return li.get(rdf_value_tag)
    nested = li.find(f"{{{NS_RDF}}}Description")
    if nested is not None and rdf_value_tag in nested.attrib:
        return nested.get(rdf_value_tag)
    rdf_value = li.find(rdf_value_tag)
    if rdf_value is not None:
        return (rdf_value.text or "").strip()
    if nested is not None:
        rdf_value = nested.find(rdf_value_tag)
        if rdf_value is not None:
            return (rdf_value.text or "").strip()
    return li.text


def _read_bag_values(bag):
    """Read all rdf:li values from a bag, following qualified spellings."""
    values = set()
    for li in bag.findall(f"{{{NS_RDF}}}li"):
        value = _li_value(li)
        if value:
            values.add(value)
    return values


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


def _li_signature(elem):
    """Structural fingerprint of an rdf:li, including qualifiers and children.

    Two items with the same text but different qualifiers -- an ``xml:lang``
    of ``en`` vs ``fr``, an ``rdf:parseType``, or any other attribute or
    child element -- get distinct fingerprints, so a merge that collapses
    duplicate bags can drop only the items that truly match and keep every
    qualified value the sidecar carried.

    Children include their ``.tail`` so mixed-content items whose value
    threads text between child elements (an XML-literal ending in
    ``<rdf:value>B</rdf:value>C`` vs ``<rdf:value>B</rdf:value>D``) compare
    distinct rather than collapsing on text-only structure. The outer item's
    own tail is whitespace between the item and its siblings inside the bag,
    not part of the item's value, so it is excluded.
    """
    def sig(e, include_tail):
        return (
            e.tag,
            e.text or "",
            (e.tail or "") if include_tail else "",
            tuple(sorted(e.attrib.items())),
            tuple(sig(child, True) for child in e),
        )
    return sig(elem, False)


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


def _parse_xmp(xmp_path):
    """Parse an XMP file, returning (root, tree) or None if missing/corrupt."""
    path = Path(xmp_path)
    if not path.exists():
        return None

    try:
        tree = ET.parse(path)
    except ET.ParseError:
        log.warning("Corrupt XMP file: %s", xmp_path)
        return None

    root = tree.getroot()
    _register_document_uri(root, xmp_path)
    return root, tree


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


# Registered sidecar document URIs, keyed on the parsed root element.
# When ``read_sync_preview_metadata'' or ``SidecarEditor'' registers
# the real path, ``_description_subject'' can fold non-empty relative
# ``rdf:about'' spellings with the equivalent absolute ones -- so a
# sidecar carrying ``rdf:about="photo.jpg"'' alongside
# ``rdf:about="file:///photos/photo.jpg"'' in ``/photos/photo.xmp''
# resolves both to the same subject key.
_DOCUMENT_URIS = weakref.WeakKeyDictionary()


def _register_document_uri(root, xmp_path):
    """Record the file URI for ``xmp_path'' keyed on the parsed ``root''."""
    try:
        uri = Path(xmp_path).resolve(strict=False).as_uri()
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
    """Return the effective ``xml:base`` URI for ``elem``.

    Per the XML Base recommendation, an element's base URI for
    resolving a relative URI reference in one of its own attributes
    is the composition of every ancestor's ``xml:base'', starting
    from the document's base URI (the sidecar's file URI when it
    was registered via :func:`_register_document_uri`) and resolving
    each successive ``xml:base'' against the previous. That
    seeding lets a relative ``xml:base="sub/"'' compose into an
    absolute path -- otherwise ``photo.jpg'' under such a base
    would stay relative and drift out of alignment with a sibling
    absolute-URI Description that names the same photo. Missing
    ``xml:base'' declarations contribute nothing; a bare
    ``xml:base'' on ``rdf:RDF'' or higher is honored just as one
    directly on the Description would be.
    """
    xml_base = f"{{{NS_XML}}}base"
    chain = []
    current = elem
    while current is not None:
        chain.append(current)
        current = parent_map.get(current)
    chain.reverse()
    base = _document_uri_for(elem, parent_map) or ""
    for anc in chain:
        b = anc.get(xml_base)
        if b is not None:
            base = urllib.parse.urljoin(base, b) if base else b
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
    if parent_map is not None:
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
    subjects = {_description_subject(d, parent_map) for d in descriptions}
    if empty in subjects:
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


def _property_bag_and_wrappers(prop):
    """Return ``(bag, wrappers)`` for a keyword-array property.

    A ``dc:subject`` / ``lr:hierarchicalSubject`` value has four
    equivalent serializations:

    * direct child: ``<dc:subject><rdf:Bag>...``.
    * short qualified form: ``<dc:subject rdf:parseType='Resource'>
      <rdf:value><rdf:Bag>...``.
    * long form: ``<dc:subject><rdf:Description><rdf:Bag>...``.
    * long qualified form: ``<dc:subject><rdf:Description>
      <rdf:value><rdf:Bag>...``.

    Callers need the bag itself, and they need the intermediate
    elements (an ``rdf:value``, an ``rdf:Description``) so an
    inherited qualifier on any of them is honored.  Returns ``(None,
    [])`` when no bag is found.
    """
    bag = prop.find(f"{{{NS_RDF}}}Bag")
    if bag is not None:
        return bag, []
    rdf_value = prop.find(f"{{{NS_RDF}}}value")
    if rdf_value is not None:
        bag = rdf_value.find(f"{{{NS_RDF}}}Bag")
        if bag is not None:
            return bag, [rdf_value]
    nested = prop.find(f"{{{NS_RDF}}}Description")
    if nested is not None:
        bag = nested.find(f"{{{NS_RDF}}}Bag")
        if bag is not None:
            return bag, [nested]
        rdf_value = nested.find(f"{{{NS_RDF}}}value")
        if rdf_value is not None:
            bag = rdf_value.find(f"{{{NS_RDF}}}Bag")
            if bag is not None:
                return bag, [nested, rdf_value]
    return None, []


def _build_parent_map(root):
    """Return a ``{child: parent}`` map for ``root`` and all its descendants."""
    return {child: parent for parent in root.iter() for child in parent}


def _walk_ancestors(elem, parent_map):
    """Yield ``elem``, its parent, its grandparent, up to the tree root."""
    current = elem
    while current is not None:
        yield current
        current = parent_map.get(current)


def _ancestor_carries_xml_qualifier(elem, parent_map):
    """True if elem or any ancestor carries an *effective* value-qualifying ``xml:*``.

    Only ``xml:lang`` is checked: it's the one ``xml:*`` attribute
    that changes the meaning of a literal value (a rating, a GPS
    coordinate, a keyword). ``xml:space`` is a whitespace directive
    and ``xml:base`` affects URI resolution -- neither would silently
    alter a literal keyword or numeric value. A Description that
    only inherits ``xml:space`` or ``xml:base`` is still a perfectly
    good target for keyword and simple-property writes.

    A closer element with an empty value (``xml:lang=""``) cancels
    an outer inherited value, so the check computes the *effective*
    setting for each attribute name and only returns True when at
    least one effective value is non-empty.
    """
    tracked = {f"{{{NS_XML}}}lang"}
    effective = {}
    for ancestor in _walk_ancestors(elem, parent_map):
        for name, value in ancestor.attrib.items():
            if name in tracked and name not in effective:
                effective[name] = value
    return any(value for value in effective.values())


def _ancestor_xml_attributes(elem, parent_map):
    """Return the ``xml:*`` attributes carried by ``elem`` or its ancestors.

    When the same attribute appears at multiple levels, the closest
    (deepest) one wins, matching XML's inheritance rules.
    """
    result = {}
    for ancestor in _walk_ancestors(elem, parent_map):
        for name, value in ancestor.attrib.items():
            if name.startswith(f"{{{NS_XML}}}") and name not in result:
                result[name] = value
    return result


def _inherited_xml_attributes(elem, parent_map):
    """Return the ``xml:*`` attributes ``elem`` inherits from its ancestors.

    Only the strictly-ancestor values are returned; ``elem``'s own
    ``xml:*`` attributes are not included.
    """
    inherited = {}
    seen_self = False
    for ancestor in _walk_ancestors(elem, parent_map):
        if not seen_self:
            seen_self = True
            continue
        for name, value in ancestor.attrib.items():
            if name.startswith(f"{{{NS_XML}}}") and name not in inherited:
                inherited[name] = value
    return inherited


def _simple_prop_carries_qualifier(prop):
    """True if a simple property occurrence carries qualifier data.

    Distinguishes a plain unqualified spelling (which is safe to
    remove as a duplicate) from any form that carries qualifier
    attributes or elements the removal would silently drop. Plain
    forms:

    * attribute on the Description (handled separately by the caller).
    * a child element with just text (``<xmp:Rating>3</xmp:Rating>``).
    * the short qualified form with just ``rdf:value`` and nothing
      else (``<xmp:Rating rdf:parseType='Resource'><rdf:value>3
      </rdf:value></xmp:Rating>``).
    * the long form with just a nested ``rdf:Description`` holding
      only ``rdf:value``.

    Anything else -- a value-qualifier attribute anywhere (see
    :func:`_has_own_value_qualifier`, which counts non-structural
    RDF attributes and identity attributes but skips non-semantic
    XML directives like ``xml:space``, ``xml:base`` and an empty
    ``xml:lang''), a sibling qualifier element alongside
    ``rdf:value``, an identity attribute on the property or a
    nested Description -- is a qualifier.
    """
    if _has_own_value_qualifier(prop):
        return True
    if len(prop) == 0:
        return False
    for child in prop:
        tag = child.tag
        if tag == f"{{{NS_RDF}}}value":
            if _has_own_value_qualifier(child):
                return True
            if len(child) and not all(
                c.tag == f"{{{NS_RDF}}}Bag" for c in child
            ):
                return True
        elif tag == f"{{{NS_RDF}}}Description":
            if _simple_prop_carries_qualifier(child):
                return True
        else:
            # Any child that isn't a wrapper is a qualifier sibling.
            return True
    return False


def _has_non_structural_attribute(elem):
    """True if elem carries any attribute that isn't a structural RDF one."""
    return any(
        name not in _STRUCTURAL_RDF_ATTRIBUTES for name in elem.attrib
    )


def _li_carries_qualifier(li, parent_map):
    """True if an ``rdf:li`` carries qualifier metadata a bare-text drop would lose.

    An item is qualified either by its own structure or attributes
    (see :func:`_simple_prop_carries_qualifier`) OR by an effective
    ``xml:lang`` inherited from any container -- the ``rdf:Bag``, the
    property, the owner Description, or higher -- so long as the
    inheritance is not cancelled by a nearer ``xml:lang=""`` reset.
    A ``<dc:subject xml:lang="fr">`` around a plain ``<rdf:li>paris
    </rdf:li>`` makes the item a language-tagged statement in its own
    right: removing the item and re-adding a fresh unqualified ``rdf:li``
    silently drops the language. Callers that need "truly plain" items
    (a Vireo authored one, safe to swap or remove) negate this check.
    """
    return (
        _simple_prop_carries_qualifier(li)
        or _ancestor_carries_xml_qualifier(li, parent_map)
    )


def _has_own_non_language_qualifier(elem):
    """True if ``elem`` carries a non-language, value-semantic own qualifier.

    Same as :func:`_has_own_value_qualifier`, except ANY ``xml:lang''
    (including a non-empty one) is skipped. Callers that already
    apply an effective-language check through the ancestor chain --
    ``_bag``'s ``_prop_is_qualified`` walks
    :func:`_ancestor_carries_xml_qualifier` at the deepest reachable
    element -- must not also count language on the property or a
    wrapper as a standalone qualifier: otherwise
    ``xml:lang="en"`` on a property whose ``rdf:Bag'' cancels it
    with ``xml:lang=""`` re-classifies the effectively-unqualified
    container as qualified, and the caller mints a duplicate bag
    instead of reusing the reset one.
    """
    xml_lang = f"{{{NS_XML}}}lang"
    xml_space = f"{{{NS_XML}}}space"
    xml_base = f"{{{NS_XML}}}base"
    rdf_value = f"{{{NS_RDF}}}value"
    for name in elem.attrib:
        if name in _STRUCTURAL_RDF_ATTRIBUTES:
            continue
        if name in (xml_lang, xml_space, xml_base, rdf_value):
            continue
        return True
    return False


def _has_own_value_qualifier(elem):
    """True if ``elem`` carries a non-structural attribute that changes value semantics.

    Same as :func:`_has_non_structural_attribute`, except three XML
    directives and the ``rdf:value`` attribute are treated as not
    qualifying a literal value:

    * ``xml:lang=""`` is XML's cancel-inheritance form. It doesn't
      itself attach a language to the element's descendants, it just
      says "no known language" on the way in. A bag or wrapper
      carrying only an empty reset is therefore just as reusable as
      one with no ``xml:*`` at all -- callers that also gate on the
      owning Description's effective language (via
      :func:`_ancestor_carries_xml_qualifier`) already refuse to
      merge into a target with a different effective language, so
      the reset can be dropped safely.
    * ``xml:space`` is a whitespace directive. It doesn't change the
      meaning of a literal keyword or numeric value, so a bag
      carrying only ``xml:space="preserve"`` is a valid reuse target.
    * ``xml:base`` affects URI resolution but never changes a literal
      keyword or numeric value the way ``xml:lang`` does.
    * ``rdf:value`` is the RDF/XML attribute abbreviation for the
      value itself -- ``<rdf:Description rdf:value="Paris"/>`` is a
      spelling of ``<rdf:Description><rdf:value>Paris</rdf:value>
      </rdf:Description>``, not a qualifier that decorates the
      value. Skip it here so a serializer's rewrite of a plain leaf
      into that attribute-form Description isn't misread as
      qualifier metadata and locked out of Vireo's own-keyword
      removal.

    Non-empty ``xml:lang`` and ``xml:id`` (an XML identifier the
    element defines in its own right) still count as own qualifiers.
    This mirrors what :func:`_ancestor_carries_xml_qualifier` skips
    on the inherited side, so a directive is treated the same
    whether it sits on the element or on an ancestor.
    """
    xml_lang = f"{{{NS_XML}}}lang"
    xml_space = f"{{{NS_XML}}}space"
    xml_base = f"{{{NS_XML}}}base"
    rdf_value = f"{{{NS_RDF}}}value"
    for name, value in elem.attrib.items():
        if name in _STRUCTURAL_RDF_ATTRIBUTES:
            continue
        if name == xml_lang and value == "":
            continue
        if name in (xml_space, xml_base, rdf_value):
            continue
        return True
    return False


def _wrappers_carry_qualifier(prop):
    """True if the value structure has a sibling qualifier element.

    RDF/XML's qualified property form places the value in ``rdf:value``
    (or a nested ``rdf:Bag`` inside it) and lists the qualifiers as
    sibling children alongside it. For example
    ``<dc:subject rdf:parseType='Resource'><rdf:value>...</rdf:value>
    <foo:source>camera</foo:source></dc:subject>``: ``foo:source``
    applies to the ``dc:subject`` value. These qualifiers don't show
    up as ``xml:*`` attributes anywhere, but they still carry meaning
    that a merge would silently drop when the duplicate container is
    removed.

    Walk the wrapper chain from ``prop`` to the bag's parent and
    return True as soon as any container has a direct child other
    than the next expected element in the chain. The chain ends at
    the ``rdf:Bag``; its ``rdf:li`` items are the value, not
    qualifiers, so we stop before descending into it.
    """
    bag, wrappers = _property_bag_and_wrappers(prop)
    if bag is None:
        return False
    chain = [prop, *wrappers]
    next_in_chain = [*wrappers, bag]
    for container, expected in zip(chain, next_in_chain, strict=True):
        for actual in container:
            if actual is not expected:
                return True
    return False


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


def _qualified_value_element(child):
    """Return the ``rdf:value`` element inside a qualified property, or None.

    A qualified simple property is spelled either as the short form,
    where ``rdf:value`` and the qualifiers are direct children of the
    property element (``rdf:parseType='Resource'``), or as the long
    form, where the property wraps an ``rdf:Description`` that holds
    ``rdf:value`` and the qualifiers. Both serializations are
    semantically identical and readers/writers must accept both.
    """
    rdf_value = child.find(f"{{{NS_RDF}}}value")
    if rdf_value is not None:
        return rdf_value
    nested = child.find(f"{{{NS_RDF}}}Description")
    if nested is not None:
        return nested.find(f"{{{NS_RDF}}}value")
    return None


def _qualified_value_attribute_owner(child):
    """Return the element carrying an ``rdf:value`` attribute, or None.

    RDF/XML's attribute abbreviation lets a qualified property put
    the value in an ``rdf:value`` attribute on the property itself
    (``<xmp:Rating rdf:parseType='Resource' rdf:value='3'
    foo:source='camera'/>``) or on a nested ``rdf:Description``
    (``<xmp:Rating><rdf:Description rdf:value='3' foo:source='camera'
    /></xmp:Rating>``). Both are semantically identical to the
    element form.
    """
    rdf_value_attr = f"{{{NS_RDF}}}value"
    if rdf_value_attr in child.attrib:
        return child
    nested = child.find(f"{{{NS_RDF}}}Description")
    if nested is not None and rdf_value_attr in nested.attrib:
        return nested
    return None


def _qualified_value(child):
    """Return the value string of a qualified simple property, or None.

    Handles every equivalent serialization: an ``rdf:value``
    attribute or child on the property, on a nested ``rdf:Description``
    wrapper, or both. Returns None only when neither spelling carries
    a value.
    """
    rdf_value_tag = f"{{{NS_RDF}}}value"
    owner = _qualified_value_attribute_owner(child)
    if owner is not None:
        return owner.get(rdf_value_tag)
    element = _qualified_value_element(child)
    if element is not None:
        return (element.text or "").strip()
    return None


def _update_simple_property_value(child, value):
    """Update a simple-property occurrence's value in place. Returns True on change.

    Handles every equivalent serialization: attribute form on the
    property or a nested ``rdf:Description``, element form (short
    or long), or a plain-text child. When no spelling carries a
    value yet, creates ``rdf:value`` under the innermost wrapper.
    """
    rdf_value_tag = f"{{{NS_RDF}}}value"
    owner = _qualified_value_attribute_owner(child)
    if owner is not None:
        if owner.get(rdf_value_tag) != value:
            owner.set(rdf_value_tag, value)
            return True
        return False
    rdf_value = child.find(rdf_value_tag)
    value_owner = child
    if rdf_value is None:
        nested = child.find(f"{{{NS_RDF}}}Description")
        if nested is not None:
            value_owner = nested
            rdf_value = nested.find(rdf_value_tag)
    if rdf_value is not None:
        if (rdf_value.text or "") != value:
            rdf_value.text = value
            return True
        return False
    if len(child) == 0:
        if (child.text or "").strip() != value:
            child.text = value
            return True
        return False
    rdf_value = ET.SubElement(value_owner, rdf_value_tag)
    rdf_value.text = value
    return True


def _property_occurrence_score(entry, parent_map=None):
    """How authoritative an ``(owner, child)`` occurrence is.

    A qualified child (child-element form, the RDF/XML attribute
    abbreviation with an ``rdf:value`` attribute, OR a plain-text
    child carrying any non-structural attribute -- ``rdf:datatype``,
    ``foo:source``, ``xml:lang``) is the most authoritative; a plain
    child whose effective inherited ``xml:lang`` (walked through the
    child's ancestor chain and honoring any ``xml:lang=""`` reset)
    resolves to a non-empty language ranks the same, since that
    language qualifies the child's literal value just as an attribute
    on the child would. An attribute-form occurrence whose owning
    Description has non-empty effective ``xml:lang'' is language-tagged
    as well, so it also ranks with the qualified forms; an unqualified
    attribute stays at the bottom. An unqualified child element ranks
    between the two. Readers and writers both use this so they agree
    on which copy holds the truth, and the write path leaves that
    copy in place while removing the others -- ``set_gps_location``
    would otherwise back up the attribute's stale value and later
    restore that instead of the qualified coordinate the write kept.

    ``parent_map`` is optional so legacy callers still work; without
    it the owner-inherited language qualification is not considered.
    """
    owner, child = entry
    if child is None:
        if parent_map is not None and _ancestor_carries_xml_qualifier(
            owner, parent_map,
        ):
            return 2
        return 0
    if (
        len(child)
        or f"{{{NS_RDF}}}value" in child.attrib
        or _has_own_value_qualifier(child)
    ):
        return 2
    if parent_map is not None and _ancestor_carries_xml_qualifier(
        child, parent_map,
    ):
        return 2
    return 1


def _is_plain_property_occurrence(entry, parent_map):
    """True when the ``(owner, child)`` entry is a plain occurrence.

    An attribute-form entry (``child is None``) is plain when its
    owner Description has no effective inherited ``xml:lang''; a
    child-element entry is plain when it carries no own qualifier
    structure or attributes AND no effective inherited ``xml:lang''
    reaches it.
    """
    owner, child = entry
    if child is None:
        return not _ancestor_carries_xml_qualifier(owner, parent_map)
    if _simple_prop_carries_qualifier(child):
        return False
    return not _ancestor_carries_xml_qualifier(child, parent_map)


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
    Vireo just assigned via ``_set_plain_property_value'', for
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


def _format_gps_coordinate(value, positive_ref, negative_ref):
    """Return an XMP GPSCoordinate string such as ``48,51.398N``."""
    ref = positive_ref if value >= 0 else negative_ref
    absolute = abs(float(value))
    degrees = int(absolute)
    minutes = (absolute - degrees) * 60.0
    return f"{degrees},{minutes:.6f}{ref}"


# ── Public API ──────────────────────────────────────────────────────────

def read_keywords(xmp_path):
    """Read dc:subject keywords from an XMP sidecar file.

    Args:
        xmp_path: path to .xmp file

    Returns:
        set of keyword strings (empty if file missing or corrupt)
    """
    result = _parse_xmp(xmp_path)
    if result is None:
        return set()

    root, _tree = result
    keywords = set()
    for bag in _photo_scoped_bags(root, f"{{{NS_DC}}}subject"):
        for li in bag.findall(f"{{{NS_RDF}}}li"):
            value = _li_value(li)
            if value:
                keywords.add(value)
    return keywords


def read_hierarchical_keywords(xmp_path):
    """Read lr:hierarchicalSubject from an XMP sidecar.

    Returns a list of pipe-delimited hierarchy strings, e.g. ['Birds|Raptors|Black kite'].
    """
    result = _parse_xmp(xmp_path)
    if result is None:
        return []

    root, _tree = result
    results = []
    for bag in _photo_scoped_bags(root, f"{{{NS_LR}}}hierarchicalSubject"):
        for li in bag.findall(f"{{{NS_RDF}}}li"):
            value = _li_value(li)
            if value:
                results.append(value)
    return results


def read_vireo_location_keywords(xmp_path):
    """Return the location keyword path Vireo wrote into this sidecar.

    ``None`` when the sidecar is missing, unreadable, or was never written by
    Vireo's location-keyword sync. Import callers use it to tell the entries
    Vireo authored from the ones the user typed in Lightroom.
    """
    result = _parse_xmp(xmp_path)
    if result is None:
        return None
    root, _tree = result
    return _get_property(root, LOCATION_KEYWORDS_MARKER)


def read_vireo_location_keywords_owned(xmp_path):
    """Return the ``vireo:locationKeywordsOwned`` companion value or ``None``.

    ``None`` covers both a missing sidecar and a sidecar that carries the
    marker but no ownership record -- treat that case as "both were ours"
    the way :func:`_parse_location_keywords_owned` does, so import callers
    keep the pre-fix skip behaviour on older sidecars.
    """
    result = _parse_xmp(xmp_path)
    if result is None:
        return None
    root, _tree = result
    return _get_property(root, LOCATION_KEYWORDS_OWNED)


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


def _parse_gps_coordinate(value):
    """Parse a common XMP GPS coordinate into decimal degrees.

    XMP commonly stores coordinates as ``degrees,minutesN`` but files from
    other tools may use decimal degrees or a three-part DMS value.  Preview
    callers need a friendly value without rejecting an otherwise readable
    sidecar just because its coordinate spelling is unfamiliar.
    """
    if value is None:
        return None
    text = str(value).strip()
    if not text:
        return None

    hemisphere = text[-1:].upper()
    if hemisphere in {"N", "S", "E", "W"}:
        text = text[:-1].strip()
    else:
        hemisphere = None

    try:
        parts = [float(part.strip()) for part in text.split(",")]
        if len(parts) == 1:
            decimal = parts[0]
        elif len(parts) == 2:
            decimal = abs(parts[0]) + parts[1] / 60.0
            if parts[0] < 0:
                decimal *= -1
        elif len(parts) == 3:
            decimal = abs(parts[0]) + parts[1] / 60.0 + parts[2] / 3600.0
            if parts[0] < 0:
                decimal *= -1
        else:
            return None
    except (TypeError, ValueError):
        return None

    if hemisphere in {"S", "W"}:
        decimal = -abs(decimal)
    elif hemisphere in {"N", "E"}:
        decimal = abs(decimal)
    return decimal


def _sync_preview_gps_pair(root, namespace=NS_EXIF, prefix="", vireo_owns=False):
    """Read one current or backed-up GPS pair for sync-review display.

    When Vireo owns the current coordinate (``vireo:gpsSource'' is
    set on the sidecar), read only Vireo's plain occurrence so the
    preview reflects the value Vireo just wrote -- otherwise a
    higher-scoring qualified sibling ``foo:source="camera"'' would
    still be returned as authoritative while Vireo's marker says
    ``assigned''. When no Vireo marker is present, fall back to the
    default ranking so a genuinely user-authored qualified value
    still wins.
    """
    def read(name):
        if vireo_owns:
            plain = _get_plain_property_value(root, name)
            if plain is not None:
                return plain
        return _get_property(root, name)

    raw_latitude = read(f"{{{namespace}}}{prefix}GPSLatitude")
    raw_longitude = read(f"{{{namespace}}}{prefix}GPSLongitude")
    if raw_latitude is None and raw_longitude is None:
        return None
    return {
        "latitude": _parse_gps_coordinate(raw_latitude),
        "longitude": _parse_gps_coordinate(raw_longitude),
        "raw_latitude": raw_latitude,
        "raw_longitude": raw_longitude,
    }


def read_sync_preview_metadata(xmp_path):
    """Read the sidecar fields shown by the pending-changes review.

    This intentionally parses a sidecar once per photo so a review containing
    several change types does not repeatedly touch the filesystem.  Missing
    and malformed sidecars are distinguished: that difference matters in a
    screen whose purpose is to explain exactly what will be written.
    """
    path = Path(xmp_path)
    empty = {
        "status": "missing",
        "keywords": set(),
        "hierarchical_keywords": set(),
        "rating": None,
        "rating_writable": False,
        "flag": None,
        "location": None,
        "previous_location": None,
        "location_source": None,
        "location_keywords": None,
        "edit_recipe": None,
    }
    try:
        root = ET.parse(path).getroot()
    except FileNotFoundError:
        # Parsing directly avoids a separate stat() before every open.  That
        # distinction is material when a large sync review lives on SMB/NFS:
        # one extra network round trip per photo can add minutes.
        return empty
    except (ET.ParseError, OSError):
        return {**empty, "status": "unreadable"}
    _register_document_uri(root, path)

    keywords = set()
    for bag in _photo_scoped_bags(root, f"{{{NS_DC}}}subject"):
        for li in bag.findall(f"{{{NS_RDF}}}li"):
            value = _li_value(li)
            if value:
                keywords.add(value)

    hierarchical_keywords = set()
    for bag in _photo_scoped_bags(root, f"{{{NS_LR}}}hierarchicalSubject"):
        for li in bag.findall(f"{{{NS_RDF}}}li"):
            value = _li_value(li)
            if value:
                hierarchical_keywords.add(value)

    if not _top_descriptions(root):
        # The sidecar parsed cleanly, so rating writes will land here even
        # though no Description currently belongs to the photo -- either
        # because the packet has ambiguous non-empty subjects, or because it
        # simply carries no top-level Descriptions yet. ``SidecarEditor.set_rating``
        # creates a fresh empty-subject Description in that case, so the
        # rating-only sync preview must report the write as it would happen,
        # not as "unchanged".
        return {
            **empty,
            "status": "ok",
            "keywords": keywords,
            "hierarchical_keywords": hierarchical_keywords,
            "rating_writable": True,
        }

    pick_to_flag = {"1": "flagged", "0": "none", "-1": "rejected"}
    raw_pick = _get_property(root, f"{{{NS_XMPDM}}}pick")
    location_source = _get_property(root, f"{{{NS_VIREO}}}gpsSource")
    return {
        "status": "ok",
        "keywords": keywords,
        "hierarchical_keywords": hierarchical_keywords,
        "rating": _get_property(root, f"{{{NS_XMP}}}Rating"),
        "rating_writable": True,
        "flag": pick_to_flag.get(raw_pick, raw_pick),
        "location": _sync_preview_gps_pair(
            root, vireo_owns=location_source is not None,
        ),
        "previous_location": _sync_preview_gps_pair(
            root, namespace=NS_VIREO, prefix="previous",
        ),
        "location_source": location_source,
        "location_keywords": _get_property(root, LOCATION_KEYWORDS_MARKER),
        "edit_recipe": _get_property(root, f"{{{NS_VIREO}}}editRecipe"),
    }


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
    """

    def __init__(self, xmp_path):
        self.path = xmp_path
        self._root = None
        self._tree = None
        self._loaded = False
        self._existed = False
        self._parse_failed = False
        self._dirty = False

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
            self._root = tree.getroot()
            self._tree = tree
            _register_document_uri(self._root, self.path)
            return
        self._root = ET.Element(f"{{{NS_X}}}xmpmeta")
        self._tree = ET.ElementTree(self._root)
        _register_document_uri(self._root, self.path)

    def _readable(self):
        """True when the sidecar existed on disk and parsed cleanly.

        Operations that only ever prune existing metadata check this so a
        missing or corrupt sidecar stays a no-op instead of being recreated
        from an empty tree.
        """
        self._load()
        return self._existed and not self._parse_failed

    # ── Tree access ─────────────────────────────────────────────────────

    def _description(self):
        """Return the first rdf:Description, creating the scaffolding if needed.

        New properties are written here. Existing ones are updated wherever
        they already live (see :meth:`_set_properties`), because ExifTool
        spreads them across one Description per namespace.
        """
        desc = self._find_description()
        if desc is not None:
            return desc
        if self._root.tag == f"{{{NS_RDF}}}RDF":
            rdf = self._root
        else:
            rdf = self._root.find(f"{{{NS_RDF}}}RDF")
            if rdf is None:
                rdf = ET.SubElement(self._root, f"{{{NS_RDF}}}RDF")
                self._dirty = True
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
        self._dirty = True
        return desc

    def _photo_write_subject(self):
        """Return the ``rdf:about'' spelling a new photo Description should use.

        The new Description will sit under ``rdf:RDF'' with no local
        ``xml:base'', so any raw ``rdf:about'' copied from an
        existing Description that carries additional context
        (a local ``xml:base'' or a namespace defaulting quirk) would
        drift once placed under the new context. Use the resolved
        absolute URI ``_photo_subject'' returns, so the new
        Description is context-independent -- with one exception:
        when the photo's subject IS the sidecar's document URI
        (the empty ``rdf:about'' at root resolves there), keep the
        idiomatic empty spelling so sidecars using the convention
        stay unchanged. ``rdf:nodeID'' / ``rdf:ID'' subjects don't
        resolve, so they pass through unchanged.
        """
        resolved = _photo_subject(self._root)
        about, node, rid = resolved
        if not about:
            return resolved
        parent_map = _build_parent_map(self._root)
        doc_uri = _document_uri_for(self._root, parent_map)
        if doc_uri and about == doc_uri:
            return ("", node, rid)
        return resolved

    def _find_description(self):
        """Return the first top-level rdf:Description, or None when there is none."""
        self._load()
        descriptions = _top_descriptions(self._root)
        return descriptions[0] if descriptions else None

    def _unqualified_photo_description(self):
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
        parent_map = _build_parent_map(self._root)
        for desc in _top_descriptions(self._root):
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
        if self._root.tag == f"{{{NS_RDF}}}RDF":
            rdf = self._root
        else:
            rdf = self._root.find(f"{{{NS_RDF}}}RDF")
            if rdf is None:
                rdf = ET.SubElement(self._root, f"{{{NS_RDF}}}RDF")
                self._dirty = True
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
        self._dirty = True
        return desc

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
            for owner in _top_descriptions(self._root)
            for child in owner.findall(tag)
        ]

        parent_map = _build_parent_map(self._root)

        def _has_inherited_qualifier(elem):
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
            return _ancestor_carries_xml_qualifier(elem, parent_map)

        _owner_inherits_qualifier = _has_inherited_qualifier
        # The effective-language check on the deepest element already
        # walks every ``xml:lang'' in the ancestor chain (honoring
        # any local reset), so counting ``xml:lang'' on the property,
        # wrapper or bag as a standalone own qualifier here would
        # double-classify a container whose reset already made it
        # effectively unqualified. Use the non-language variant of
        # ``_has_own_value_qualifier'' -- it still counts every
        # non-language value qualifier (``foo:source'', identity
        # attributes, ``rdf:datatype'', ...).
        _has_own_qualifier = _has_own_non_language_qualifier

        def _prop_is_qualified(owner, prop):
            bag_el, wrappers = _property_bag_and_wrappers(prop)
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
            deepest = bag_el if bag_el is not None else prop
            return (
                _ancestor_carries_xml_qualifier(deepest, parent_map)
                or _has_own_qualifier(prop)
                or any(_has_own_qualifier(w) for w in wrappers)
                or (bag_el is not None and _has_own_qualifier(bag_el))
                or _wrappers_carry_qualifier(prop)
            )

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
                 if not _prop_is_qualified(owner, child)),
                None,
            )
            if unqualified_idx is None:
                elem = ET.SubElement(
                    self._unqualified_photo_description(), tag,
                )
                self._dirty = True
            else:
                elem = found[unqualified_idx][1]
        else:
            # No existing occurrence at all. Create the new property on
            # an unqualified Description for the same reason as above.
            target = desc
            if _owner_inherits_qualifier(desc):
                target = self._unqualified_photo_description()
            elem = ET.SubElement(target, tag)
            self._dirty = True
        bag, _wrappers = _property_bag_and_wrappers(elem)
        if bag is None:
            bag = ET.SubElement(elem, f"{{{NS_RDF}}}Bag")
            self._dirty = True
        seen = {_li_signature(li) for li in bag.findall(f"{{{NS_RDF}}}li")}
        for owner, extra in found:
            if extra is elem:
                continue
            extra_bag, extra_wrappers = _property_bag_and_wrappers(extra)
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
            extra_deepest = extra_bag if extra_bag is not None else extra
            if (
                _ancestor_carries_xml_qualifier(extra_deepest, parent_map)
                or _has_own_qualifier(extra)
                or any(_has_own_qualifier(w) for w in extra_wrappers)
                or (
                    extra_bag is not None
                    and _has_own_qualifier(extra_bag)
                )
                or _wrappers_carry_qualifier(extra)
            ):
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
            self._dirty = True
        return bag

    def _get(self, name):
        """Return a simple property's current value, or None when absent."""
        self._load()
        return _get_property(self._root, name)

    def _set_properties(self, desc, values):
        """Set simple properties, marking the tree dirty only for real changes.

        A property the sidecar already carries is updated in place, as an
        attribute or as a child element, whichever form it uses; a
        Description free of inherited ``xml:*`` receives properties that
        are new. Any further copies of the property are removed so no
        reader can pick up a stale value.
        """
        changed = False
        new_desc = None
        parent_map = _build_parent_map(self._root)
        for name, value in values.items():
            found = _property_occurrences(self._root, name)
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
                        new_desc = self._unqualified_photo_description()
                        # ``_unqualified_photo_description`` may have
                        # mutated the tree, so refresh the parent map.
                        parent_map = _build_parent_map(self._root)
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
            owner, child = keeper
            if child is None:
                if owner.get(name) != value:
                    owner.set(name, value)
                    changed = True
            elif f"{{{NS_RDF}}}value" in child.attrib or len(child):
                # Qualified property. Update whichever spelling
                # already carries the value -- attribute or element
                # form, on the property itself or inside a nested
                # ``rdf:Description`` -- in place. That preserves
                # qualifier attributes and sibling elements; the
                # helper only writes a fresh ``rdf:value`` if no
                # form carries one yet.
                if _update_simple_property_value(child, value):
                    changed = True
            elif (child.text or "").strip() != value:
                child.text = value
                changed = True
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
        if changed:
            self._dirty = True
        return changed

    def _delete_property(self, name):
        """Remove every copy of a simple property; True when one existed."""
        found = _property_occurrences(self._root, name)
        for owner, child in found:
            if child is None:
                owner.attrib.pop(name, None)
            else:
                owner.remove(child)
        if found:
            self._dirty = True
        return bool(found)

    def _delete_plain_property_copies(self, name):
        """Remove only plain (unqualified) copies of a simple property.

        Callers that own the plain occurrence (Vireo's own writes) but
        cannot vouch for qualified duplicates -- another tool may have
        added a language-tagged or ``foo:source''-annotated occurrence
        after the initial write -- use this instead of
        :meth:`_delete_property`. Retained qualified duplicates keep
        their metadata AND their original values, matching the
        preservation invariant that ``_set_properties`` applies. True
        when at least one plain copy existed.
        """
        found = _property_occurrences(self._root, name)
        if not found:
            return False
        parent_map = _build_parent_map(self._root)
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
            self._dirty = True
        return removed

    _is_plain_occurrence = staticmethod(_is_plain_property_occurrence)

    def _set_plain_property_value(self, desc, name, value):
        """Set a simple property's value, landing only on plain occurrences.

        Callers use this instead of :meth:`_set_properties` for
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
        parent_map = _build_parent_map(self._root)
        plain = _plain_property_occurrences(self._root, name)
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
                target = self._unqualified_photo_description()
            target.set(name, value)
            changed = True
        if changed:
            self._dirty = True
        return changed

    def _restore_plain_property_value(self, name, value):
        """Restore ``value'' to only plain (unqualified) copies of a property.

        Vireo's own writes land on plain occurrences, so a restore of
        a backed-up value belongs there too. A qualified occurrence
        added by another tool AFTER Vireo's initial write carries
        data we can't restore per-occurrence -- and it's not what
        Vireo overwrote anyway, so overwriting it now with the
        backup would permanently destroy the external data. Leave
        qualified duplicates entirely alone: their metadata and
        their values both survive. "Plain" here is the same
        classification :meth:`_delete_plain_property_copies` uses;
        the short qualified-serialization form (``rdf:parseType=
        "Resource"'' with a bare ``rdf:value'' child and nothing
        else) is treated as plain because Vireo's own writes
        preserve it and update through it.
        """
        found = _property_occurrences(self._root, name)
        if not found:
            return False
        parent_map = _build_parent_map(self._root)
        changed = False
        for owner, child in found:
            if child is None:
                if _ancestor_carries_xml_qualifier(owner, parent_map):
                    continue
                if owner.get(name) != value:
                    owner.set(name, value)
                    changed = True
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
                    changed = True
        if changed:
            self._dirty = True
        return changed

    # ── Mutations ───────────────────────────────────────────────────────

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
        desc = self._description()
        existing_flat = _all_photo_scoped_values(
            self._root, f"{{{NS_DC}}}subject",
        )
        to_add_flat = sorted(set(flat_keywords) - existing_flat)
        if to_add_flat:
            dc_bag = self._bag(desc, NS_DC, "subject")
            for kw in to_add_flat:
                ET.SubElement(dc_bag, f"{{{NS_RDF}}}li").text = kw
                self._dirty = True

        existing_hier = _all_photo_scoped_values(
            self._root, f"{{{NS_LR}}}hierarchicalSubject",
        )
        to_add_hier = sorted(set(hierarchical_keywords) - existing_hier)
        if to_add_hier:
            lr_bag = self._bag(desc, NS_LR, "hierarchicalSubject")
            for kw in to_add_hier:
                ET.SubElement(lr_bag, f"{{{NS_RDF}}}li").text = kw
                self._dirty = True

    def replace_keyword_hierarchies(self, replacements):
        """Replace exact reviewed paths; a None replacement removes that path."""
        if not self._readable():
            return
        def key(path):
            return tuple(keyword_match_key(part) for part in path.split('|'))
        by_key = {key(source): target for source, target in replacements.items()}
        parent_map = _build_parent_map(self._root)
        for bag in _photo_scoped_bags(self._root, f"{{{NS_LR}}}hierarchicalSubject"):
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
                    self._dirty = True
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
                        self._dirty = True
                    elif this_qualified:
                        # Both qualified -- keep both so no qualifier
                        # metadata is lost.
                        if value != old and _update_simple_property_value(
                            li, value,
                        ):
                            self._dirty = True
                    else:
                        bag.remove(li)
                        self._dirty = True
                    continue
                keeper[value] = li
                if value != old and _update_simple_property_value(li, value):
                    self._dirty = True

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
        if not self._readable():
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
        removed = []

        for bag in _photo_scoped_bags(self._root, f"{{{NS_DC}}}subject"):
            for li in bag.findall(f"{{{NS_RDF}}}li"):
                value = _li_value(li)
                if not value or value in exact:
                    continue
                if keyword_match_key(value) in remove_keys:
                    removed.append(value)
                    bag.remove(li)

        # Hierarchical entries match if any pipe-delimited segment matches.
        # Skipped when hierarchical=False: the sync path uses the flat-only
        # mode to canonicalize add-equivalent variants without accidentally
        # deleting unrelated hierarchies that share a segment with the added
        # flat leaf.
        if hierarchical:
            for bag in _photo_scoped_bags(
                self._root, f"{{{NS_LR}}}hierarchicalSubject"
            ):
                for li in bag.findall(f"{{{NS_RDF}}}li"):
                    value = _li_value(li)
                    if not value or value in exact:
                        continue
                    segments = {keyword_match_key(s) for s in value.split("|")}
                    segments.discard("")
                    if segments & remove_keys:
                        removed.append(value)
                        bag.remove(li)

        if removed:
            self._dirty = True
            log.info("Removed keywords from %s: %s", self.path, removed)
        return bool(removed)

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
        Description currently belongs to the photo. ``_description()`` handles
        that by adding a fresh Description scoped to the photo, so the rating
        lands there instead of being silently dropped -- which would let the
        sync caller clear the queued rating with nothing written.
        """
        if create:
            return self._set_properties(
                self._description(), {f"{{{NS_XMP}}}Rating": str(rating)},
            )
        if not self._dirty and not self._readable():
            return False
        return self._set_properties(
            self._description(), {f"{{{NS_XMP}}}Rating": str(rating)},
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
        desc = self._description()
        return self._set_properties(desc, {f"{{{NS_XMPDM}}}pick": values[flag]})

    def set_gps_location(self, latitude, longitude, source="assigned"):
        """Write Lightroom-compatible GPS coordinates, creating a sidecar if needed."""
        lat = float(latitude)
        lon = float(longitude)
        if not math.isfinite(lat) or not (-90.0 <= lat <= 90.0):
            raise ValueError("latitude must be between -90 and 90")
        if not math.isfinite(lon) or not (-180.0 <= lon <= 180.0):
            raise ValueError("longitude must be between -180 and 180")

        desc = self._description()
        marker = f"{{{NS_VIREO}}}gpsSource"
        exif_attrs = {
            "GPSLatitude": f"{{{NS_EXIF}}}GPSLatitude",
            "GPSLongitude": f"{{{NS_EXIF}}}GPSLongitude",
            "GPSMapDatum": f"{{{NS_EXIF}}}GPSMapDatum",
            "GPSVersionID": f"{{{NS_EXIF}}}GPSVersionID",
        }

        # First Vireo write: preserve any GPS another app had already
        # written so clearing the Vireo-assigned location can restore
        # it. Rewrites of an existing Vireo GPS keep the original
        # backup. Back up the PLAIN occurrence's value specifically
        # (via ``_get_plain_property_value''): that's the occurrence
        # ``_set_plain_property_value'' will overwrite, so the backup
        # matches what a later restore needs to write back into it.
        # A higher-scoring qualified sibling isn't Vireo's to back up
        # and isn't what the restore path targets; capturing its
        # value here would leave the plain occurrence's original
        # coordinate permanently lost after a clear-and-restore.
        # When no plain occurrence exists Vireo will create one and
        # a later remove can just delete it -- no backup is needed.
        changed = False
        if self._get(marker) is None:
            for name, attr in exif_attrs.items():
                existing = _get_plain_property_value(self._root, attr)
                if existing is not None:
                    changed |= self._set_properties(
                        desc, {f"{{{NS_VIREO}}}previous{name}": existing},
                    )

        # Write the GPS attrs through the plain-only path so an
        # externally-authored qualified copy (say ``foo:source="camera"'')
        # isn't overwritten -- Vireo's single ``vireo:previousGPS*''
        # backup can't restore that value per-occurrence, so a later
        # clear would permanently replace the external's coordinate
        # with Vireo's assigned one. The Vireo-owned ``vireo:gpsSource''
        # marker has no external qualified copies to preserve, so
        # the standard writer still handles it.
        gps_values = {
            exif_attrs["GPSLatitude"]: _format_gps_coordinate(lat, "N", "S"),
            exif_attrs["GPSLongitude"]: _format_gps_coordinate(lon, "E", "W"),
            exif_attrs["GPSMapDatum"]: "WGS-84",
            exif_attrs["GPSVersionID"]: "2.3.0.0",
        }
        for attr, val in gps_values.items():
            changed |= self._set_plain_property_value(desc, attr, val)
        changed |= self._set_properties(
            desc, {marker: source or "assigned"},
        )
        return changed

    def remove_vireo_gps_location(self):
        """Remove GPS fields only when Vireo previously wrote them."""
        if not self._readable():
            return False
        desc = self._find_description()
        if desc is None:
            return False

        marker = f"{{{NS_VIREO}}}gpsSource"
        if self._get(marker) is None:
            return False

        removed = False
        for name in ("GPSLatitude", "GPSLongitude", "GPSMapDatum", "GPSVersionID"):
            gps_attr = f"{{{NS_EXIF}}}{name}"
            previous_attr = f"{{{NS_VIREO}}}previous{name}"
            previous = self._get(previous_attr)
            if previous is not None:
                # Restore the backup only onto plain (unqualified)
                # copies -- those are what Vireo overwrote. A
                # qualified copy another tool added between the
                # initial Vireo write and this removal isn't ours
                # to touch: we didn't overwrite it and we can't
                # back it up per-occurrence, so leave its metadata
                # AND its value alone.
                self._restore_plain_property_value(gps_attr, previous)
                self._delete_property(previous_attr)
                removed = True
            elif self._delete_plain_property_copies(gps_attr):
                # Vireo originally created this field, so only its
                # plain occurrence is ours to remove. A qualified
                # duplicate another tool added after the initial write
                # carries data we can't restore per-occurrence -- and
                # there's no backup to restore anyway -- so leave it
                # in place with its metadata AND original value intact.
                removed = True

        removed |= self._delete_property(marker)
        return removed

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
                self.path, parts,
            )
            raise ValueError(
                f"location name may not contain '|': {parts!r}"
            )

        path = "|".join(parts)
        was_dirty = self._dirty
        desc = self._description()
        previous = self._get(LOCATION_KEYWORDS_MARKER)
        previous_owned = self._get(LOCATION_KEYWORDS_OWNED)
        if previous and previous != path:
            self._remove_location_keyword_entries(previous, previous_owned)

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
            self._root, f"{{{NS_DC}}}subject",
        )
        hier_values_before = _all_photo_scoped_values(
            self._root, f"{{{NS_LR}}}hierarchicalSubject",
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
        if leaf_key:
            parent_map = _build_parent_map(self._root)
            for bag in _photo_scoped_bags(
                self._root, f"{{{NS_DC}}}subject",
            ):
                for li in bag.findall(f"{{{NS_RDF}}}li"):
                    li_value = _li_value(li)
                    if (
                        li_value
                        and li_value != parts[-1]
                        and keyword_match_key(li_value) == leaf_key
                        and _li_carries_qualifier(li, parent_map)
                        and _update_simple_property_value(li, parts[-1])
                    ):
                        self._dirty = True

        # Canonicalize a flat variant of the leaf the way the species-keyword
        # path does: add_keywords() dedupes on exact text, so a sidecar
        # spelling like `kumeyaay lake` would otherwise sit beside the clean
        # one as a second <rdf:li>. ``keep_exact`` keeps a re-sync of an
        # already-correct sidecar a no-op. Qualified variants were
        # canonicalized in place above, so ``remove_keywords`` here only
        # sees plain-text variants that are safe to drop.
        self.remove_keywords({parts[-1]}, hierarchical=False, keep_exact=True)

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
                self._root, f"{{{NS_DC}}}subject",
            )
            and not existed_flat
        )
        added_hier = (
            path not in _all_photo_scoped_values(
                self._root, f"{{{NS_LR}}}hierarchicalSubject",
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
            self._root, f"{{{NS_DC}}}subject",
        )
        flat_to_add = set() if flat_leaf_present else {parts[-1]}
        hier_to_add = set() if existed_hier else {path}
        self.add_keywords(
            flat_keywords=flat_to_add, hierarchical_keywords=hier_to_add,
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

        self._set_properties(
            desc,
            {
                LOCATION_KEYWORDS_MARKER: path,
                LOCATION_KEYWORDS_OWNED: _format_location_keywords_owned(
                    owns_flat, owns_hier,
                ),
            },
        )
        return self._dirty != was_dirty

    def remove_vireo_location_keywords(self):
        """Remove location keywords only when Vireo previously wrote them."""
        if not self._readable():
            return False
        desc = self._find_description()
        if desc is None:
            return False
        previous = self._get(LOCATION_KEYWORDS_MARKER)
        if not previous:
            return False
        self._remove_location_keyword_entries(
            previous, self._get(LOCATION_KEYWORDS_OWNED),
        )
        # Clearing the marker is itself a change worth publishing: it is what
        # keeps a later re-enable from treating stale entries as ours.
        self._delete_property(LOCATION_KEYWORDS_MARKER)
        self._delete_property(LOCATION_KEYWORDS_OWNED)
        self._dirty = True
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
        if not self._readable():
            return False
        desc = self._find_description()
        if desc is None:
            return False
        previous = self._get(LOCATION_KEYWORDS_MARKER)
        if not previous:
            return False
        owned = self._get(LOCATION_KEYWORDS_OWNED)
        owns_flat, owns_hier = _parse_location_keywords_owned(owned)
        if not owns_flat:
            return False
        leaf, _path = location_keyword_entries(previous)
        leaf_key = keyword_match_key(leaf)
        if not leaf_key:
            return False
        for kw in keywords:
            if keyword_match_key(kw) == leaf_key:
                self._set_properties(desc, {
                    LOCATION_KEYWORDS_OWNED:
                        _format_location_keywords_owned(False, owns_hier),
                })
                self._dirty = True
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
        parent_map = _build_parent_map(self._root)

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
                self._root, f"{{{NS_DC}}}subject",
            ))
            plain_exact_targets = [
                (bag, li)
                for bag in flat_bags
                for li in bag.findall(f"{{{NS_RDF}}}li")
                if _li_value(li) == leaf
                and not _li_carries_qualifier(li, parent_map)
            ]
            if plain_exact_targets:
                # Vireo authored one entry, not many. Remove a single
                # plain occurrence -- if another tool added exact
                # duplicates later, they're user- or tool-owned data
                # we shouldn't sweep away.
                bag, li = plain_exact_targets[0]
                removed.append(_li_value(li))
                bag.remove(li)
            else:
                # No plain exact match, so Vireo's owned entry (if it
                # still exists) is a spelling variant that normalizes
                # to ``leaf_key``. A qualified exact-match duplicate
                # from another tool must NOT suppress that fallback:
                # doing so would clear the ownership marker while
                # leaving Vireo's owned variant behind forever.
                fallback = None
                for bag in flat_bags:
                    fallback = next(
                        (
                            (bag, li) for li in bag.findall(f"{{{NS_RDF}}}li")
                            if _li_value(li)
                            and keyword_match_key(_li_value(li)) == leaf_key
                            and not _li_carries_qualifier(li, parent_map)
                        ),
                        None,
                    )
                    if fallback is not None:
                        break
                if fallback is not None:
                    bag, li = fallback
                    removed.append(_li_value(li))
                    bag.remove(li)

        if owns_hier:
            hier_bags = list(_photo_scoped_bags(
                self._root, f"{{{NS_LR}}}hierarchicalSubject",
            ))
            plain_exact_targets = [
                (bag, li)
                for bag in hier_bags
                for li in bag.findall(f"{{{NS_RDF}}}li")
                if _li_value(li) == path
                and not _li_carries_qualifier(li, parent_map)
            ]
            if plain_exact_targets:
                # See the flat branch above: only remove one plain
                # occurrence.
                bag, li = plain_exact_targets[0]
                removed.append(_li_value(li))
                bag.remove(li)
            else:
                # See the flat branch above: a qualified exact
                # duplicate must not suppress the normalized fallback
                # that removes Vireo's owned spelling variant.
                fallback = None
                for bag in hier_bags:
                    fallback = next(
                        (
                            (bag, li) for li in bag.findall(f"{{{NS_RDF}}}li")
                            if _li_value(li)
                            and [
                                keyword_match_key(s)
                                for s in _li_value(li).split("|")
                            ]
                            == path_keys
                            and not _li_carries_qualifier(li, parent_map)
                        ),
                        None,
                    )
                    if fallback is not None:
                        break
                if fallback is not None:
                    bag, li = fallback
                    removed.append(_li_value(li))
                    bag.remove(li)

        if removed:
            self._dirty = True
            log.info(
                "Removed Vireo location keywords from %s: %s", self.path, removed,
            )
        return bool(removed)

    def set_edit_recipe(self, recipe_json):
        """Write or clear Vireo's non-destructive edit recipe marker."""
        recipe_json = recipe_json or ""
        recipe_attr = f"{{{NS_VIREO}}}editRecipe"
        version_attr = f"{{{NS_VIREO}}}editRecipeSchema"

        if recipe_json:
            desc = self._description()
            return self._set_properties(
                desc, {recipe_attr: recipe_json, version_attr: "1"},
            )

        if not self._readable():
            return False
        desc = self._find_description()
        if desc is None:
            return False
        removed = False
        for attr in (recipe_attr, version_attr):
            removed |= self._delete_property(attr)
        return removed

    # ── Publication ─────────────────────────────────────────────────────

    def commit(self):
        """Publish the accumulated edits; a no-op when nothing changed.

        Returns True when the sidecar was written.
        """
        if not self._dirty:
            return False
        ET.indent(self._tree, space="  ")
        _write_tree_atomic(self._tree, self.path)
        self._dirty = False
        self._existed = True
        self._parse_failed = False
        return True


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
