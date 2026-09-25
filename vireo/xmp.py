"""Consolidated XMP sidecar operations.

Provides read/write/merge/remove for XMP keyword and rating metadata.
All XMP namespace constants and helpers live here as the single source of truth.
"""

import copy
import errno
import logging
import math
import os
import stat
import sys
import uuid
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


def _read_bag_values(bag):
    """Read all rdf:li values from a bag."""
    values = set()
    for li in bag.findall(f"{{{NS_RDF}}}li"):
        if li.text:
            values.add(li.text)
    return values


def _li_signature(elem):
    """Structural fingerprint of an rdf:li, including qualifiers and children.

    Two items with the same text but different qualifiers -- an ``xml:lang``
    of ``en`` vs ``fr``, an ``rdf:parseType``, or any other attribute or
    child element -- get distinct fingerprints, so a merge that collapses
    duplicate bags can drop only the items that truly match and keep every
    qualified value the sidecar carried.
    """
    return (
        elem.tag,
        elem.text or "",
        tuple(sorted(elem.attrib.items())),
        tuple(_li_signature(child) for child in elem),
    )


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

    return tree.getroot(), tree


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


def _description_subject(desc):
    """Return the ``(rdf:about, rdf:nodeID)`` tuple identifying a Description.

    An empty or missing ``rdf:about`` means "the enclosing resource", which
    for a sidecar file is the photo it sits next to. The two spellings are
    equivalent, so treat a missing attribute as ``""``.
    """
    about = desc.get(f"{{{NS_RDF}}}about")
    node = desc.get(f"{{{NS_RDF}}}nodeID")
    return (about or "", node or "")


def _photo_subject(root):
    """Return the ``(about, nodeID)`` tuple identifying the photo's Descriptions.

    ``rdf:about=""`` (or a missing attribute) is the sidecar convention for
    "the enclosing resource" -- the photo. When no top-level Description
    carries the empty subject but every Description shares a single
    non-empty subject, treat that as the photo's, so a sidecar written by
    a tool that pins its Descriptions is still handled coherently. When
    the sidecar carries several distinct non-empty subjects, refuse to
    guess from document order: fall back to the empty subject, which
    leaves reads returning nothing rather than an auxiliary resource's
    rating or GPS, and lets writes land on a fresh Description that is
    unambiguously the photo's.
    """
    descriptions = _all_top_descriptions(root)
    if not descriptions:
        return ("", "")
    empty = ("", "")
    subjects = {_description_subject(d) for d in descriptions}
    if empty in subjects or len(subjects) > 1:
        return empty
    return next(iter(subjects))


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
    return [
        desc for desc in _all_top_descriptions(root)
        if _description_subject(desc) == subject
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
    """
    for desc in _top_descriptions(root):
        for prop in desc.findall(tag):
            bag = prop.find(f"{{{NS_RDF}}}Bag")
            if bag is not None:
                yield bag


def _get_property(root, name):
    """Return a simple property's value from whichever form stores it."""
    for desc, child in _property_occurrences(root, name):
        if child is None:
            return desc.get(name)
        return (child.text or "").strip()
    return None


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
            if li.text:
                keywords.add(li.text)
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
            if li.text:
                results.append(li.text)
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


def _sync_preview_gps_pair(root, namespace=NS_EXIF, prefix=""):
    """Read one current or backed-up GPS pair for sync-review display."""
    raw_latitude = _get_property(root, f"{{{namespace}}}{prefix}GPSLatitude")
    raw_longitude = _get_property(root, f"{{{namespace}}}{prefix}GPSLongitude")
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

    keywords = set()
    for bag in _photo_scoped_bags(root, f"{{{NS_DC}}}subject"):
        for li in bag.findall(f"{{{NS_RDF}}}li"):
            if li.text:
                keywords.add(li.text)

    hierarchical_keywords = set()
    for bag in _photo_scoped_bags(root, f"{{{NS_LR}}}hierarchicalSubject"):
        for li in bag.findall(f"{{{NS_RDF}}}li"):
            if li.text:
                hierarchical_keywords.add(li.text)

    if not _top_descriptions(root):
        return {
            **empty,
            "status": "ok",
            "keywords": keywords,
            "hierarchical_keywords": hierarchical_keywords,
        }

    pick_to_flag = {"1": "flagged", "0": "none", "-1": "rejected"}
    raw_pick = _get_property(root, f"{{{NS_XMPDM}}}pick")
    return {
        "status": "ok",
        "keywords": keywords,
        "hierarchical_keywords": hierarchical_keywords,
        "rating": _get_property(root, f"{{{NS_XMP}}}Rating"),
        "rating_writable": True,
        "flag": pick_to_flag.get(raw_pick, raw_pick),
        "location": _sync_preview_gps_pair(root),
        "previous_location": _sync_preview_gps_pair(
            root, namespace=NS_VIREO, prefix="previous",
        ),
        "location_source": _get_property(root, f"{{{NS_VIREO}}}gpsSource"),
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
            return
        self._root = ET.Element(f"{{{NS_X}}}xmpmeta")
        self._tree = ET.ElementTree(self._root)

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
        about, node = _photo_subject(self._root)
        if about:
            desc.set(f"{{{NS_RDF}}}about", about)
        if node:
            desc.set(f"{{{NS_RDF}}}nodeID", node)
        self._dirty = True
        return desc

    def _find_description(self):
        """Return the first top-level rdf:Description, or None when there is none."""
        self._load()
        descriptions = _top_descriptions(self._root)
        return descriptions[0] if descriptions else None

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
        if found:
            elem = found[0][1]
        else:
            elem = ET.SubElement(desc, tag)
            self._dirty = True
        bag = elem.find(f"{{{NS_RDF}}}Bag")
        if bag is None:
            bag = ET.SubElement(elem, f"{{{NS_RDF}}}Bag")
            self._dirty = True
        seen = {_li_signature(li) for li in bag.findall(f"{{{NS_RDF}}}li")}
        for owner, extra in found[1:]:
            extra_bag = extra.find(f"{{{NS_RDF}}}Bag")
            if extra_bag is not None:
                for li in extra_bag.findall(f"{{{NS_RDF}}}li"):
                    if not li.text:
                        continue
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
        attribute or as a child element, whichever form it uses; ``desc`` only
        receives properties that are new. Any further copies of the property
        are removed so no reader can pick up a stale value.
        """
        changed = False
        for name, value in values.items():
            found = _property_occurrences(self._root, name)
            if not found:
                desc.set(name, value)
                changed = True
                continue
            owner, child = found[0]
            if child is None:
                if owner.get(name) != value:
                    owner.set(name, value)
                    changed = True
            elif (child.text or "").strip() != value or len(child):
                for grandchild in list(child):
                    child.remove(grandchild)
                child.text = value
                changed = True
            for owner, child in found[1:]:
                if child is None:
                    del owner.attrib[name]
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

    # ── Mutations ───────────────────────────────────────────────────────

    def add_keywords(self, flat_keywords=(), hierarchical_keywords=()):
        """Merge keywords into dc:subject and lr:hierarchicalSubject."""
        desc = self._description()
        dc_bag = self._bag(desc, NS_DC, "subject")
        existing_flat = _read_bag_values(dc_bag)
        for kw in sorted(set(flat_keywords) - existing_flat):
            ET.SubElement(dc_bag, f"{{{NS_RDF}}}li").text = kw
            self._dirty = True

        lr_bag = self._bag(desc, NS_LR, "hierarchicalSubject")
        existing_hier = _read_bag_values(lr_bag)
        for kw in sorted(set(hierarchical_keywords) - existing_hier):
            ET.SubElement(lr_bag, f"{{{NS_RDF}}}li").text = kw
            self._dirty = True

    def replace_keyword_hierarchies(self, replacements):
        """Replace exact reviewed paths; a None replacement removes that path."""
        if not self._readable():
            return
        def key(path):
            return tuple(keyword_match_key(part) for part in path.split('|'))
        by_key = {key(source): target for source, target in replacements.items()}
        for bag in _photo_scoped_bags(self._root, f"{{{NS_LR}}}hierarchicalSubject"):
            seen = set()
            for li in list(bag.findall(f"{{{NS_RDF}}}li")):
                old = li.text or ''
                value = by_key.get(key(old), old)
                if value is None or value in seen:
                    bag.remove(li)
                    self._dirty = True
                    continue
                seen.add(value)
                if value != old:
                    li.text = value
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
                if not li.text or li.text in exact:
                    continue
                if keyword_match_key(li.text) in remove_keys:
                    removed.append(li.text)
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
                    if not li.text or li.text in exact:
                        continue
                    segments = {keyword_match_key(s) for s in li.text.split("|")}
                    segments.discard("")
                    if segments & remove_keys:
                        removed.append(li.text)
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
        """
        if create:
            return self._set_properties(
                self._description(), {f"{{{NS_XMP}}}Rating": str(rating)},
            )
        if not self._dirty and not self._readable():
            return False
        desc = self._find_description()
        if desc is None:
            return False
        return self._set_properties(desc, {f"{{{NS_XMP}}}Rating": str(rating)})

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

        # First Vireo write: preserve any GPS another app had already written
        # so clearing the Vireo-assigned location can restore it. Rewrites of
        # an existing Vireo GPS keep the original backup.
        changed = False
        if self._get(marker) is None:
            for name, attr in exif_attrs.items():
                existing = self._get(attr)
                if existing is not None:
                    changed |= self._set_properties(
                        desc, {f"{{{NS_VIREO}}}previous{name}": existing},
                    )

        changed |= self._set_properties(desc, {
            exif_attrs["GPSLatitude"]: _format_gps_coordinate(lat, "N", "S"),
            exif_attrs["GPSLongitude"]: _format_gps_coordinate(lon, "E", "W"),
            exif_attrs["GPSMapDatum"]: "WGS-84",
            exif_attrs["GPSVersionID"]: "2.3.0.0",
            marker: source or "assigned",
        })
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
                self._set_properties(desc, {gps_attr: previous})
                self._delete_property(previous_attr)
                removed = True
            elif self._delete_property(gps_attr):
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
        dc_bag = self._bag(desc, NS_DC, "subject")
        lr_bag = self._bag(desc, NS_LR, "hierarchicalSubject")
        leaf_key = keyword_match_key(parts[-1])
        path_keys = [keyword_match_key(part) for part in parts]
        existed_flat = bool(leaf_key) and any(
            keyword_match_key(v) == leaf_key
            for v in _read_bag_values(dc_bag)
        )
        existed_hier = any(
            [keyword_match_key(s) for s in v.split("|")] == path_keys
            for v in _read_bag_values(lr_bag)
        )

        # Canonicalize a flat variant of the leaf the way the species-keyword
        # path does: add_keywords() dedupes on exact text, so a sidecar
        # spelling like `kumeyaay lake` would otherwise sit beside the clean
        # one as a second <rdf:li>. ``keep_exact`` keeps a re-sync of an
        # already-correct sidecar a no-op.
        self.remove_keywords({parts[-1]}, hierarchical=False, keep_exact=True)

        # An entry the sidecar already carries -- because the user typed it
        # in Lightroom, or another Vireo keyword shares its name -- is not
        # ours to claim and must not be removed on a later clear. Exact-text
        # matches survive the canonicalization step above and show up as
        # "already present"; normalized variants were stripped by that step,
        # so a straight bag re-read would misread them as fresh inserts.
        # ``existed_*`` captured that pre-canonicalization truth.
        added_flat = (
            parts[-1] not in _read_bag_values(dc_bag) and not existed_flat
        )
        added_hier = path not in _read_bag_values(lr_bag) and not existed_hier

        # Skip inserting the canonical hierarchy when the user already has a
        # normalized variant of it: add_keywords() would otherwise leave both
        # spellings side by side, and the leaked canonical would drift out of
        # step with the user's spelling forever. The species-keyword sync path
        # only canonicalizes flat entries for the same reason.
        hier_to_add = set() if existed_hier else {path}
        self.add_keywords(
            flat_keywords={parts[-1]}, hierarchical_keywords=hier_to_add,
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

        if owns_flat and leaf_key:
            for bag in _photo_scoped_bags(self._root, f"{{{NS_DC}}}subject"):
                exact = [
                    li for li in bag.findall(f"{{{NS_RDF}}}li")
                    if li.text == leaf
                ]
                if exact:
                    targets = exact
                else:
                    fallback = next(
                        (
                            li for li in bag.findall(f"{{{NS_RDF}}}li")
                            if li.text and keyword_match_key(li.text) == leaf_key
                        ),
                        None,
                    )
                    targets = [fallback] if fallback is not None else []
                for li in targets:
                    removed.append(li.text)
                    bag.remove(li)

        if owns_hier:
            for bag in _photo_scoped_bags(
                self._root, f"{{{NS_LR}}}hierarchicalSubject"
            ):
                exact = [
                    li for li in bag.findall(f"{{{NS_RDF}}}li")
                    if li.text == path
                ]
                if exact:
                    targets = exact
                else:
                    fallback = next(
                        (
                            li for li in bag.findall(f"{{{NS_RDF}}}li")
                            if li.text
                            and [keyword_match_key(s) for s in li.text.split("|")]
                            == path_keys
                        ),
                        None,
                    )
                    targets = [fallback] if fallback is not None else []
                for li in targets:
                    removed.append(li.text)
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
