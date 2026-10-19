"""XMP namespace URIs and the prefixes sidecars are serialized with.

``STANDARD_PREFIXES`` is the prefix ``xmp.markup`` declares a namespace with
when a sidecar doesn't already use one for it. It is also registered with
ElementTree at import, a process-wide setting made once, so a tree printed
with ``ET.tostring`` while debugging reads ``dc:`` rather than ``ns0:``;
sidecars themselves never go through ElementTree's serializer.
"""

from xml.etree import ElementTree as ET

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

# The prefix each namespace is declared with when the sidecar has none of
# its own for it: the ones Adobe's XMP specification and Lightroom use.
STANDARD_PREFIXES = {
    NS_X: "x",
    NS_RDF: "rdf",
    NS_DC: "dc",
    NS_LR: "lr",
    NS_XMP: "xmp",
    NS_XMPDM: "xmpDM",
    "http://ns.adobe.com/camera-raw-settings/1.0/": "crs",
    "http://ns.adobe.com/photoshop/1.0/": "photoshop",
    NS_EXIF: "exif",
    "http://ns.adobe.com/tiff/1.0/": "tiff",
    "http://ns.adobe.com/exif/1.0/aux/": "aux",
    "http://ns.adobe.com/xap/1.0/mm/": "xmpMM",
    "http://ns.adobe.com/xap/1.0/sType/ResourceEvent#": "stEvt",
    "http://ns.adobe.com/xap/1.0/sType/ResourceRef#": "stRef",
    NS_VIREO: "vireo",
}

for _uri, _prefix in STANDARD_PREFIXES.items():
    ET.register_namespace(_prefix, _uri)
