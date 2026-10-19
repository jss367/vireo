"""Selected XML definitions and invented memory trees; never sidecar I/O."""
import __future__

import ast
import copy
import re
from pathlib import Path
from types import SimpleNamespace
from xml.etree import ElementTree as ET

VIREO = Path(__file__).resolve().parents[1]


def _namespace():
    ns = {"ET": ET, "re": re, "copy": copy}
    for module in ("namespaces.py", "markup.py", "rdf.py", "keywords.py"):
        tree = ast.parse((VIREO / "xmp" / module).read_text(encoding="utf-8"))
        nodes = [n for n in tree.body if not isinstance(n, (ast.Import, ast.ImportFrom))]
        # The module's logger is unrelated to these pure methods.
        nodes = [n for n in nodes if not (isinstance(n, ast.Assign) and any(
            isinstance(t, ast.Name) and t.id == "log" for t in n.targets
        ))]
        exec(compile(ast.Module(body=nodes, type_ignores=[]), module, "exec",
                     flags=__future__.annotations.compiler_flag), ns)
    return ns


def _tree(ns, xml):
    target = ns["_RecordingBuilder"]()
    parser = ET.XMLParser(target=target)
    parser.feed(xml)
    return ns["SidecarTree"](parser.close(), target.markup)


def _merge(ns, first, second, extra_property=None):
    later = extra_property or f'<dc:subject><rdf:Bag>{second}</rdf:Bag></dc:subject>'
    xml = (f'<rdf:RDF xmlns:rdf="{ns["NS_RDF"]}" xmlns:dc="{ns["NS_DC"]}">'
           '<rdf:Description rdf:about="">'
           f'<dc:subject><rdf:Bag>{first}</rdf:Bag></dc:subject>'
           f'{later}'
           '</rdf:Description></rdf:RDF>')
    tree = _tree(ns, xml)
    owner = tree.getroot()[0]
    props = list(owner)
    bag = props[0][0]
    editor = ns["_KeywordEdits"](SimpleNamespace(tree=tree, dirty=False))
    editor._merge_duplicate_bags(bag, props[0], [(owner, p) for p in props],
                                 ns["_build_parent_map"](tree.getroot()))
    return tree, owner


def _stable(ns, tree):
    ET.indent(tree)
    text = ns["serialize_sidecar"](tree)
    for _ in range(3):
        tree = _tree(ns, text)
        ET.indent(tree)
        again = ns["serialize_sidecar"](tree)
        assert again == text
    return text


def test_copy_from_later_bag_preserves_local_namespace_and_shadowing():
    ns = _namespace()
    tree, owner = _merge(ns, '<rdf:li>A</rdf:li>',
                         '<rdf:li xmlns:q="urn:outer" rdf:parseType="Resource">'
                         '<q:value>Heron</q:value><q:child xmlns:q="urn:inner">B</q:child>'
                         '</rdf:li>')
    text = _stable(ns, tree)
    assert len(owner) == 1 and len(owner[0][0]) == 2
    assert 'xmlns:q="urn:outer"' in text and 'xmlns:q="urn:inner"' in text
    assert '<q:value>Heron</q:value>' in text
    carrier_open = text[:text.index('<q:value>')].rsplit('<rdf:li', 1)[1]
    assert 'xmlns:q="urn:outer"' in carrier_open
    copied = ET.fromstring(text)[0][0][0][1]
    assert copied[0].tag == '{urn:outer}value' and copied[1].tag == '{urn:inner}child'


def test_copy_from_later_bag_keeps_inner_comment_pi_and_sibling_anchors():
    ns = _namespace()
    tree, owner = _merge(ns, '<rdf:li>A</rdf:li>',
                         '<rdf:li rdf:parseType="Resource"><rdf:value>B</rdf:value>'
                         '<!--after value--><?keep data?><rdf:other>C</rdf:other></rdf:li>')
    text = _stable(ns, tree)
    assert len(owner) == 1 and len(owner[0][0]) == 2
    assert text.index('</rdf:value>') < text.index('<!--after value-->') < text.index('<?keep data?>') < text.index('<rdf:other>')


def test_container_comment_and_pi_are_not_removed_with_a_later_bag():
    ns = _namespace()
    tree, owner = _merge(ns, '<rdf:li>A</rdf:li>',
                         '<!--keep--><rdf:li>B</rdf:li><?keep data?>')
    text = _stable(ns, tree)
    assert '<!--keep-->' in text and '<?keep data?>' in text
    assert text.index('<!--keep-->') < text.index('>B</rdf:li>') < text.index('<?keep data?>')
    assert len(owner) == 2


def test_duplicate_signature_does_not_discard_a_unique_inner_comment():
    ns = _namespace()
    tree, owner = _merge(ns, '<rdf:li>A</rdf:li>', '<rdf:li>A<!--unique--></rdf:li>')
    assert '<!--unique-->' in _stable(ns, tree)
    assert len(owner) == 2


def test_removed_wrapper_keeps_its_markup_and_local_bindings():
    ns = _namespace()
    tree, owner = _merge(ns, '<rdf:li>A</rdf:li>', '', extra_property=(
        '<dc:subject><!--wrapper--><rdf:Bag xmlns:q="urn:wrapper">'
        '<rdf:li>B</rdf:li></rdf:Bag></dc:subject>'
    ))
    text = _stable(ns, tree)
    assert '<!--wrapper-->' in text and 'xmlns:q="urn:wrapper"' in text
    assert len(owner) == 2


def test_plain_duplicate_bags_still_fold_without_changing_values():
    ns = _namespace()
    tree, owner = _merge(ns, '<rdf:li>A</rdf:li>', '<rdf:li>A</rdf:li><rdf:li>B</rdf:li>')
    assert len(owner) == 1
    assert [li.text for li in owner[0][0]] == ['A', 'B']
    _stable(ns, tree)
