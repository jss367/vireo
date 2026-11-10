import re


def test_photo_editor_layout_uses_available_image_width(client_with_photo):
    app, _db, photo_id = client_with_photo
    html = app.test_client().get(f"/edit/{photo_id}").get_data(as_text=True)

    image_style = re.search(r"#editorImg\s*\{([^}]+)\}", html)
    assert image_style is not None
    assert re.search(r"(?:^|;)\s*max-width:\s*100%\s*;", image_style.group(1))
    assert "100vw - 700px" not in html
    assert "grid-template-columns: minmax(360px, 1fr) 340px;" in html
    panel = html.split('<aside class="editor-panel">', 1)[1].split("</aside>", 1)[0]
    assert panel.index(">Adjustments</div>") < panel.index(">Transform</div>")
    history = re.search(r'<details\b[^>]*class="history-panel"[^>]*>', panel)
    assert history is not None
    assert not re.search(r"\bopen(?:\s|=|>)", history.group(0))
    assert panel.index(">Versions</div>") < history.start() < panel.index(">Presets</div>")
    assert '<summary class="panel-title">Edit History</summary>' in panel
    assert panel.count('id="historyList"') == 1

