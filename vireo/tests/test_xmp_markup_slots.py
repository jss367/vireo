"""Character data around retained markup stays stable without sidecar I/O."""

from xml.etree import ElementTree as ET

from xmp.markup import SidecarTree, _RecordingBuilder, serialize_sidecar


def _memory_tree(source):
    target = _RecordingBuilder()
    parser = ET.XMLParser(target=target)
    parser.feed(source)
    return SidecarTree(parser.close(), target.markup)


def _rewrite(source):
    tree = _memory_tree(source)
    ET.indent(tree)
    return serialize_sidecar(tree)


def test_whitespace_leaf_round_trips_without_character_data_growth():
    source = '<p xml:space="preserve"> <!--c--> </p>'
    for _ in range(5):
        rewritten = _rewrite(source)
        assert rewritten == source
        assert ET.fromstring(rewritten).text == "  "
        source = rewritten


def test_whitespace_leaf_with_multiple_comment_and_pi_slots():
    source = '<p xml:space="preserve"> <!--a-->\t<?keep value?> <!--b-->  </p>'
    for _ in range(5):
        assert _rewrite(source) == source


def test_leaf_offsets_preserve_mixed_character_data_and_escaping():
    source = '<p>é&amp;<!--a--> x<?keep value?>y&lt;<!--b--></p>'
    assert _rewrite(source) == source


def test_edited_shorter_whitespace_leaf_appends_out_of_range_markup():
    tree = _memory_tree('<p xml:space="preserve">  <!--a-->  <?keep value?> </p>')
    tree.getroot().text = " "
    assert serialize_sidecar(tree) == (
        '<p xml:space="preserve"> <!--a--><?keep value?></p>'
    )


def test_edited_longer_whitespace_leaf_partitions_at_original_offsets():
    tree = _memory_tree('<p xml:space="preserve"> <!--a--> <?keep value?> </p>')
    tree.getroot().text = "      "
    assert serialize_sidecar(tree) == (
        '<p xml:space="preserve"> <!--a--> <?keep value?>    </p>'
    )


def test_edited_nonwhitespace_and_empty_leaf_values():
    for value, content in (("edited", "e<!--a-->d<?keep value?>ited"),
                           ("", "<!--a--><?keep value?>")):
        tree = _memory_tree('<p> <!--a--> <?keep value?> </p>')
        tree.getroot().text = value
        assert serialize_sidecar(tree) == f'<p>{content}</p>'


def test_removed_child_anchor_falls_back_without_duplicating_leaf_text():
    tree = _memory_tree('<p xml:space="preserve"> <a /> <!--a--><?keep value?> </p>')
    elem = tree.getroot()
    elem.remove(elem[0])
    assert serialize_sidecar(tree) == (
        '<p xml:space="preserve"> <!--a--><?keep value?></p>'
    )


def test_element_child_indentation_and_removed_sibling_fallback_stay_stable():
    tree = _memory_tree('<p><a /><!--a--><?keep value?><b /></p>')
    elem = tree.getroot()
    elem.remove(elem[0])
    ET.indent(tree)
    output = serialize_sidecar(tree)
    assert '<!--a-->' in output and '<?keep value?>' in output
    assert output.index('<!--a-->') < output.index('<?keep value?>') < output.index('<b />')
    assert _rewrite(output) == output
