"""Element-level RDF structure: values, qualifiers and item fingerprints.

These helpers answer questions about one element and its ancestors: which
value an ``rdf:li`` or a simple property carries in whichever serialization
it uses, whether it carries qualifier data a rewrite would drop, and how
authoritative one occurrence of a property is next to another. Which
Descriptions describe the photo is ``photo_scope``'s question.
"""

from xml.etree import ElementTree as ET

from xmp.namespaces import _STRUCTURAL_RDF_ATTRIBUTES, NS_RDF, NS_XML


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
