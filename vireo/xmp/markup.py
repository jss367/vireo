"""Read a sidecar keeping the markup ElementTree drops, and write it back.

ElementTree's tree holds elements, attributes and text. A sidecar written by
another tool carries more than that: the ``<?xpacket?>`` instructions that
wrap the packet, comments, whether it starts with an XML declaration, and the
prefix it declared each namespace with, on the element it declared it on.
ElementTree's own serializer drops the first three, declares every namespace
on the root, and renames each namespace it has no registered prefix for to
``ns0``...``nsN``. Readers that resolve names by URI don't mind, but the
sidecar Lightroom gets back no longer looks like the one it wrote, and a tool
that matches prefixes textually breaks.

``parse_sidecar`` records that markup beside the tree (``SourceMarkup``) and
``serialize_sidecar`` writes it back: each namespace declaration on its
element with its prefix, each comment and processing instruction where it
was. A namespace an edit introduced that no declaration in scope covers is
declared on the nearest enclosing ``rdf:Description`` (or the element itself
when there is none) -- where Adobe's XMP Toolkit, ExifTool and Exiv2 declare
them -- with the prefix the sidecar already uses for it, else the standard
one from ``namespaces.STANDARD_PREFIXES``. A sidecar Vireo creates from
scratch takes the same path with nothing recorded.

Prefixes are chosen per document. Nothing here reads or writes
ElementTree's process-wide prefix registry, so writes on concurrent pool
threads cannot affect each other's output.
"""

import re
from xml.etree import ElementTree as ET

from xmp.namespaces import NS_RDF, NS_XML, STANDARD_PREFIXES

# The declaration a sidecar Vireo creates is written with -- the one
# ``ElementTree.write`` produced before this module existed.
_DEFAULT_DECLARATION = "<?xml version='1.0' encoding='utf-8'?>"
_UTF8_BOM = b"\xef\xbb\xbf"
_UTF16_BOMS = (b"\xff\xfe", b"\xfe\xff")
_DECLARATION = re.compile(rb"<\?xml[ \t\r\n][^>]*?\?>")
_DECLARED_ENCODING = re.compile(rb"""encoding[ \t\r\n]*=[ \t\r\n]*["']([^"']*)["']""")
_RDF_DESCRIPTION = f"{{{NS_RDF}}}Description"
_RDF_ABOUT = f"{{{NS_RDF}}}about"


class SourceMarkup:
    """What a parsed sidecar held besides its element tree.

    ``declaration`` is the source's XML declaration (``None`` when it had
    none) and ``bom`` whether it started with a UTF-8 byte-order mark.
    ``prolog`` and ``epilog`` are the comments and processing instructions
    before and after the root element; ``inner`` maps an element to the ones
    inside it, each paired with the element siblings that preceded it.
    ``namespaces`` maps an element to the ``(prefix, uri)`` declarations it
    carried, in source order.
    """

    def __init__(self):
        self.declaration = None
        self.bom = False
        self.prolog = []
        self.epilog = []
        self.inner = {}
        self.namespaces = {}

    def bindings(self):
        """``(prefixes by URI, URIs by prefix)`` over every declaration."""
        prefixes, uris = {}, {}
        for declarations in self.namespaces.values():
            for prefix, uri in declarations:
                prefixes.setdefault(uri, []).append(prefix)
                uris.setdefault(prefix, set()).add(uri)
        return prefixes, uris


class SidecarTree(ET.ElementTree):
    """An ``ElementTree`` carrying its source's ``SourceMarkup``.

    ``markup`` is ``None`` for a tree Vireo built from scratch.
    """

    def __init__(self, element=None, markup=None):
        super().__init__(element)
        self.markup = markup


class _RecordingBuilder:
    """Parser target: builds the tree and records the markup around it."""

    def __init__(self):
        self._builder = ET.TreeBuilder()
        self.markup = SourceMarkup()
        self._open = []
        self._children_seen = []
        self._pending_namespaces = []
        self._root_closed = False
        self._inner = []

    def start_ns(self, prefix, uri):
        self._pending_namespaces.append((prefix, uri))

    def start(self, tag, attrs):
        elem = self._builder.start(tag, attrs)
        if self._pending_namespaces:
            self.markup.namespaces[elem] = self._pending_namespaces
            self._pending_namespaces = []
        if self._children_seen:
            self._children_seen[-1] += 1
        self._open.append(elem)
        self._children_seen.append(0)
        return elem

    def end(self, tag):
        self._open.pop()
        self._children_seen.pop()
        self._root_closed = not self._open
        return self._builder.end(tag)

    def data(self, data):
        self._builder.data(data)

    def comment(self, text):
        self._keep(ET.Comment(text))

    def pi(self, target, data):
        self._keep(ET.PI(target, data))

    def _keep(self, node):
        if self._open:
            self._inner.append((self._open[-1], self._children_seen[-1], node))
        elif self._root_closed:
            self.markup.epilog.append(node)
        else:
            self.markup.prolog.append(node)

    def close(self):
        root = self._builder.close()
        for parent, preceding, node in self._inner:
            # Anchor to the preceding siblings themselves now that the
            # parent's children are final: an edit may remove the nearest one,
            # and the node then follows the nearest survivor.
            self.markup.inner.setdefault(parent, []).append(
                (tuple(parent[:preceding]), node),
            )
        return root


def parse_sidecar(path):
    """Parse ``path`` into a ``SidecarTree`` that remembers its markup.

    Raises what ``ET.parse`` raises: ``FileNotFoundError`` and the other
    ``OSError``s from reading, ``ET.ParseError`` for malformed XML.
    """
    with open(path, "rb") as stream:
        data = stream.read()
    target = _RecordingBuilder()
    parser = ET.XMLParser(target=target)
    parser.feed(data)
    root = parser.close()
    markup = target.markup
    markup.bom = data.startswith(_UTF8_BOM)
    declaration = _DECLARATION.match(data, len(_UTF8_BOM) if markup.bom else 0)
    if declaration is not None:
        markup.declaration = _utf8_declaration(declaration.group(0))
    elif data.startswith(_UTF16_BOMS):
        # The rewrite is UTF-8; a UTF-16 source's declaration (it almost
        # certainly had one) is not readable from the raw bytes here.
        markup.declaration = _DEFAULT_DECLARATION
    return SidecarTree(root, markup)


def _utf8_declaration(raw):
    """The source's declaration, unless it names an encoding besides UTF-8.

    The rewrite is always UTF-8, so a declaration naming another encoding
    would have readers decode it wrongly; that one is replaced.
    """
    encoding = _DECLARED_ENCODING.search(raw)
    if encoding is not None and encoding.group(1).lower() not in (b"utf-8", b"utf8"):
        return _DEFAULT_DECLARATION
    return raw.decode("ascii", "replace")


# ── Serialization ───────────────────────────────────────────────────────


def _escape_text(text):
    return text.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")


def _escape_attribute(text):
    # ElementTree's escapes in ElementTree's order, so an unchanged value is
    # written exactly as before.
    return (
        _escape_text(text).replace('"', "&quot;").replace("\r", "&#13;")
        .replace("\n", "&#10;").replace("\t", "&#09;")
    )


def _node_text(node):
    if node.tag is ET.Comment:
        return f"<!--{node.text}-->"
    return f"<?{node.text}?>"


def _split(name):
    if name[:1] == "{":
        uri, _, local = name[1:].partition("}")
        return uri, local
    return None, name


class _Scope:
    """The namespace declarations of one element being written."""

    def __init__(self, elem, declarations, parent):
        self.elem = elem
        self.declarations = list(declarations)
        self.parent = parent

    def resolve(self, prefix):
        """The URI ``prefix`` is bound to here, or None."""
        scope = self
        while scope is not None:
            for bound, uri in reversed(scope.declarations):
                if bound == prefix:
                    return uri
            scope = scope.parent
        return NS_XML if prefix == "xml" else None

    def prefix_for(self, uri, attribute):
        """A prefix bound to ``uri`` here, or None when none is in scope.

        An attribute needs a real prefix: the default namespace does not
        apply to attributes.
        """
        if uri == NS_XML:
            return "xml"
        scope = self
        while scope is not None:
            for prefix, bound in reversed(scope.declarations):
                if (
                    bound == uri
                    and (prefix or not attribute)
                    and self.resolve(prefix) == uri
                ):
                    return prefix
            scope = scope.parent
        return None

    def host(self):
        """The scope a namespace this element lacks is declared on."""
        scope = self
        while scope is not None:
            if scope.elem.tag == _RDF_DESCRIPTION:
                return scope
            scope = scope.parent
        return self


class _Writer:
    """Serializes one tree, with the namespace declarations it had to add."""

    def __init__(self, markup):
        self.from_source = markup is not None
        self.markup = markup if markup is not None else SourceMarkup()
        self.out = []
        self._source_prefixes, self._source_uris = self.markup.bindings()
        self._added = {}

    def document(self, root):
        markup = self.markup
        if markup.bom:
            self.out.append("﻿")
        if markup.declaration is not None:
            self.out.append(markup.declaration + "\n")
        elif not self.from_source:
            self.out.append(_DEFAULT_DECLARATION + "\n")
        for node in markup.prolog:
            self.out.append(_node_text(node) + "\n")
        self._element(root, None)
        for node in markup.epilog:
            self.out.append("\n" + _node_text(node))
        return "".join(self.out)

    def _element(self, elem, parent_scope):
        """Write ``elem`` and its content, without its tail."""
        scope = _Scope(elem, self.markup.namespaces.get(elem, ()), parent_scope)
        tag = self._qname(elem.tag, scope, attribute=False)
        attributes = [
            (self._qname(name, scope, attribute=True), value)
            for name, value in elem.attrib.items()
        ]
        start = len(self.out)
        self.out.append("")  # the start tag, once every declaration is known
        children = list(elem)
        kept = self._kept_inside(elem, children)
        if elem.text or children or kept:
            self._content(elem, scope, children, kept)
            self.out.append(f"</{tag}>")
            close = ">"
        else:
            close = " />"
        self.out[start] = self._start_tag(tag, attributes, scope, elem) + close

    def _content(self, elem, scope, children, kept):
        text = elem.text or ""
        # The indentation in front of each child, reused to set off a
        # comment kept between two of them.
        separator = text if text.isspace() else ""
        self.out.append(_escape_text(text))
        for node in kept.get(-1, ()):
            self.out.append(_node_text(node) + separator)
        last = len(children) - 1
        for index, child in enumerate(children):
            self._element(child, scope)
            tail = _escape_text(child.tail or "")
            after = kept.get(index, ())
            if index == last:
                # The last child's tail indents the closing tag, so what
                # followed that child goes in front of it.
                for node in after:
                    self.out.append(separator + _node_text(node))
                self.out.append(tail)
            else:
                self.out.append(tail)
                for node in after:
                    self.out.append(_node_text(node) + separator)

    def _kept_inside(self, elem, children):
        """Group the comments and PIs inside ``elem`` by the child they follow.

        -1 is "before the first child". A node whose preceding siblings were
        all removed moves to the front.
        """
        nodes = self.markup.inner.get(elem)
        if not nodes:
            return {}
        position = {id(child): index for index, child in enumerate(children)}
        by_position = {}
        for preceding, node in nodes:
            anchor = next(
                (position[id(sibling)] for sibling in reversed(preceding)
                 if id(sibling) in position),
                -1,
            )
            by_position.setdefault(anchor, []).append(node)
        return by_position

    def _qname(self, name, scope, attribute):
        uri, local = _split(name)
        if uri is None:
            if not attribute and scope.resolve(""):
                # An element in no namespace under a default namespace.
                scope.declarations.append(("", ""))
            return local
        prefix = scope.prefix_for(uri, attribute)
        if prefix is None:
            prefix = self._new_prefix(uri, scope)
            scope.host().declarations.append((prefix, uri))
            self._added[prefix] = uri
        return f"{prefix}:{local}" if prefix else local

    def _new_prefix(self, uri, scope):
        """A prefix for ``uri`` that nothing in the document binds differently.

        Prefer the one the sidecar already uses for it, then the standard
        one. A prefix bound to another URI anywhere -- in the source or by an
        earlier addition -- is skipped, so the new declaration cannot capture
        a name meant to resolve elsewhere.
        """
        def free(prefix):
            return bool(
                prefix
                and self._source_uris.get(prefix, {uri}) == {uri}
                and self._added.get(prefix, uri) == uri
                and scope.resolve(prefix) is None
            )

        for prefix in (*self._source_prefixes.get(uri, ()),
                       STANDARD_PREFIXES.get(uri)):
            if free(prefix):
                return prefix
        number = 0
        while not free(f"ns{number}"):
            number += 1
        return f"ns{number}"

    def _start_tag(self, tag, attributes, scope, elem):
        declarations = [
            f'xmlns:{prefix}="{_escape_attribute(uri)}"' if prefix
            else f'xmlns="{_escape_attribute(uri)}"'
            for prefix, uri in scope.declarations
        ]
        parts = [
            f'{name}="{_escape_attribute(value)}"' for name, value in attributes
        ]
        if declarations and _RDF_ABOUT in elem.attrib:
            # Adobe's toolkit and ExifTool write ``rdf:about`` ahead of a
            # Description's namespace declarations.
            about = list(elem.attrib).index(_RDF_ABOUT)
            parts = [parts[about], *declarations, *parts[:about], *parts[about + 1:]]
        else:
            parts = [*declarations, *parts]
        return f"<{tag}" + "".join(" " + part for part in parts)


def serialize_sidecar(tree):
    """The text of ``tree``, with its source's markup when it has one."""
    markup = tree.markup if isinstance(tree, SidecarTree) else None
    return _Writer(markup).document(tree.getroot())
