"""XMP namespace URIs and the prefixes sidecars are serialized with.

Registering a prefix is a process-wide ElementTree setting: importing the
``xmp`` package is what makes every sidecar Vireo writes spell ``dc:``,
``lr:``, ``crs:`` and the rest instead of ``ns0:``.
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
