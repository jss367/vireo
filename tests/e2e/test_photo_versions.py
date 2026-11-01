from pathlib import Path

from PIL import Image, ImageDraw
from playwright.sync_api import expect


def open_editor(live_server, page):
    pid = live_server["data"]["photos"][0]
    db = live_server["db"]
    photo = db.photos.get(pid)
    source = Path(live_server["app"].config["THUMB_CACHE_DIR"]).parent / "version-originals"
    source.mkdir(exist_ok=True)
    image = Image.new("RGB", (600, 400), "#6699cc")
    draw = ImageDraw.Draw(image)
    draw.rectangle((0, 250, 600, 400), fill="#557744")
    draw.ellipse((400, 40, 500, 140), fill="#ffcc66")
    image.save(source / photo["filename"])
    db.conn.execute("UPDATE folders SET path=? WHERE id=?", (str(source), photo["folder_id"]))
    db.conn.execute("UPDATE photos SET width=600, height=400 WHERE id=?", (pid,))
    db.commit()
    live_server["app"].config["EDIT_PREVIEW_IN_PROCESS"] = True
    page.goto(f"{live_server['url']}/edit/{pid}")
    page.wait_for_function("() => !editorState.loading && editorState.photo")
    expect(page.locator("#editorVersionSelect option")).to_have_count(1)
    return pid


def create_copy(page, name):
    pid = page.evaluate("() => editorState.photoId")
    page.goto(page.url.split("/edit/")[0] + "/browse")
    page.locator(f'.grid-card[data-id="{pid}"]').click(button="right")
    page.locator(".vireo-ctx-menu").get_by_text("Create Virtual Copy", exact=True).click()
    page.wait_for_function("() => typeof editorState !== 'undefined' && !editorState.loading && !!editorState.versionId")
    expect(page.locator("#editorVersionSelect option")).to_have_count(2)
    page.once("dialog", lambda dialog: dialog.accept(name))
    page.locator("#editorVersions").get_by_role("button", name="Rename", exact=True).click()
    expect(page.locator("#editorVersionSelect option:checked")).to_have_text(name)
    page.wait_for_function("() => !editorState.loading")


def test_copy_save_switch_reload_and_export_identity(live_server, page):
    pid = open_editor(live_server, page)
    page.evaluate("() => rotateRecipe(90)")
    page.locator("#saveBtn").click()
    page.wait_for_function("() => !isEditorDirty() && !Object.keys(editorState.savingPhotoIds).length")
    create_copy(page, "Portrait crop")
    version_id = page.evaluate("() => editorState.versionId")
    assert "version=" + version_id in page.url
    assert live_server["db"].get_photo_edit_recipe(pid)["rotation"] == 90
    assert page.evaluate("() => editorState.recipe.rotation") == 90
    page.evaluate("() => rotateRecipe(90)")
    page.locator("#saveBtn").click()
    page.wait_for_function("() => !isEditorDirty() && !Object.keys(editorState.savingPhotoIds).length")
    page.get_by_label("Editing", exact=True).select_option("")
    page.wait_for_function("() => !editorState.loading && editorState.versionId === null")
    assert page.evaluate("() => editorState.recipe.rotation") == 90
    page.get_by_label("Editing", exact=True).select_option(version_id)
    page.wait_for_function("() => !editorState.loading && !!editorState.versionId")
    assert page.evaluate("() => editorState.recipe.rotation") == 180
    page.reload()
    page.wait_for_function("() => !editorState.loading && !!editorState.versionId")
    assert page.evaluate("() => editorState.recipe.rotation") == 180
    page.locator("#exportBtn").click()
    request = page.evaluate("() => buildExportPreflightRequest()")
    assert request["version_id"] == version_id
    assert request["version_revision"]
    page.evaluate("closeExportModal()")
    page.locator("#editorVersions").scroll_into_view_if_needed()
    page.screenshot(path=str(Path(__file__).resolve().parents[2] / ".context" / "photo-versions.png"))


def test_snapshot_restore_does_not_change_original(live_server, page):
    pid = open_editor(live_server, page)
    create_copy(page, "Warm")
    page.evaluate("() => rotateRecipe(90)")
    page.once("dialog", lambda dialog: dialog.accept("Quarter turn"))
    page.get_by_role("button", name="Save Snapshot", exact=True).click()
    expect(page.get_by_label("Snapshots", exact=True).locator("option")).to_have_count(2)
    page.wait_for_function("() => !editorState.loading")
    page.evaluate("() => resetAllEdits()")
    page.once("dialog", lambda dialog: dialog.accept())
    page.get_by_label("Snapshots", exact=True).select_option(label="Quarter turn")
    page.get_by_role("button", name="Restore", exact=True).click()
    page.wait_for_function("() => !isEditorDirty() && editorState.recipe.rotation === 90")
    assert live_server["db"].get_photo_edit_recipe(pid) is None


def test_stale_tab_cannot_overwrite_a_copy(live_server, page):
    pid = open_editor(live_server, page)
    create_copy(page, "Color")
    version = page.evaluate("() => editorState.version")
    response = page.request.put(f"{live_server['url']}/api/photos/{pid}/versions/{version['id']}",
                                data={"revision": version["revision"], "recipe": {"rotation": 180}})
    assert response.ok
    page.evaluate("() => rotateRecipe(90)")
    page.locator("#saveBtn").click()
    expect(page.locator("#editorStatus")).to_contain_text("changed in another editor")
    assert page.evaluate("isEditorDirty()")
    assert live_server["db"].photo_versions.get(pid, version["id"])["recipe"]["rotation"] == 180


def test_missing_copy_never_opens_original_for_editing(live_server, page):
    pid = live_server["data"]["photos"][0]
    page.goto(f"{live_server['url']}/edit/{pid}?version=missing")
    expect(page.locator("#editorFilename")).to_have_text("Photo unavailable")
    expect(page.locator("#saveBtn")).to_be_disabled()
    assert page.evaluate("() => editorState.loading")


def test_browse_can_create_a_copy_and_undo_returns_to_original(live_server, page):
    pid = open_editor(live_server, page)
    page.goto(f"{live_server['url']}/browse")
    page.locator(f'.grid-card[data-id="{pid}"]').click(button="right")
    page.locator(".vireo-ctx-menu").get_by_text("Create Virtual Copy", exact=True).click()
    page.wait_for_function("() => typeof editorState !== 'undefined' && !editorState.loading && !!editorState.versionId")
    expect(page.locator("#editorVersionSelect option")).to_have_count(2)
    page.evaluate("doUndo()")
    page.wait_for_function("() => !editorState.loading && editorState.versionId === null")
    expect(page.locator("#editorVersionSelect option")).to_have_count(1)
    assert "version=" not in page.url


def test_rename_delete_and_undo_preserve_copy(live_server, page):
    open_editor(live_server, page)
    create_copy(page, "Color")
    current = page.locator("#editorVersions")
    page.once("dialog", lambda dialog: dialog.accept("Monochrome"))
    current.get_by_role("button", name="Rename", exact=True).click()
    expect(page.locator("#editorVersionSelect option:checked")).to_have_text("Monochrome")
    expect(page.locator("#editorSubhead")).to_contain_text("Monochrome")
    page.once("dialog", lambda dialog: dialog.accept())
    current.get_by_role("button", name="Delete", exact=True).click()
    page.wait_for_function("() => !editorState.loading && editorState.versionId === null")
    expect(page.locator("#editorVersionSelect option")).to_have_count(1)
    page.evaluate("doUndo()")
    expect(page.locator("#editorVersionSelect option")).to_have_count(2)
    expect(page.locator("#editorVersions")).to_contain_text("Monochrome")


def test_copy_selector_keeps_current_version_when_discard_is_canceled(live_server, page):
    open_editor(live_server, page)
    expect(page.get_by_role("button", name="Create Virtual Copy", exact=True)).to_have_count(0)
    expect(page.locator("#editorVersions img")).to_have_count(0)
    expect(page.get_by_label("Snapshots", exact=True)).to_have_count(0)
    create_copy(page, "Color")
    version_id = page.evaluate("() => editorState.versionId")
    page.evaluate("() => rotateRecipe(90)")
    page.once("dialog", lambda dialog: dialog.dismiss())
    page.get_by_label("Editing", exact=True).select_option("")
    expect(page.get_by_label("Editing", exact=True)).to_have_value(version_id)
    assert page.evaluate("() => editorState.versionId") == version_id
    assert page.evaluate("isEditorDirty()")
    page.once("dialog", lambda dialog: dialog.accept())
    page.get_by_label("Editing", exact=True).select_option("")
    page.wait_for_function("() => !editorState.loading && editorState.versionId === null")
    expect(page.get_by_label("Editing", exact=True)).to_have_value("")
    assert not page.evaluate("isEditorDirty()")


def test_snapshot_picker_supports_restore_rename_and_delete(live_server, page):
    pid = open_editor(live_server, page)
    page.evaluate("() => rotateRecipe(90)")
    page.once("dialog", lambda dialog: dialog.accept("Quarter turn"))
    page.get_by_role("button", name="Save Snapshot", exact=True).click()
    snapshots = page.get_by_label("Snapshots", exact=True)
    expect(snapshots.locator("option")).to_have_count(2)
    page.wait_for_function("() => !editorState.loading")
    assert page.evaluate("isEditorDirty()")
    assert live_server["db"].get_photo_edit_recipe(pid) is None
    page.evaluate("() => rotateRecipe(90)")
    snapshots.select_option(label="Quarter turn")
    picker = snapshots.locator("..")
    page.once("dialog", lambda dialog: dialog.dismiss())
    picker.get_by_role("button", name="Restore", exact=True).click()
    assert page.evaluate("() => editorState.recipe.rotation") == 180
    page.once("dialog", lambda dialog: dialog.accept())
    picker.get_by_role("button", name="Restore", exact=True).click()
    page.wait_for_function("() => !isEditorDirty() && !Object.keys(editorState.savingPhotoIds).length")
    assert live_server["db"].get_photo_edit_recipe(pid)["rotation"] == 90
    assert page.evaluate("() => editorState.versionId") is None
    snapshots.select_option(label="Quarter turn")
    page.once("dialog", lambda dialog: dialog.accept("Portrait"))
    picker.get_by_role("button", name="Rename", exact=True).click()
    expect(snapshots.locator("option").filter(has_text="Portrait")).to_have_count(1)
    page.wait_for_function("() => !editorState.loading")
    snapshots.select_option(label="Portrait")
    page.locator("#editorVersions").locator("..").screenshot(
        path=str(Path(__file__).resolve().parents[2] / ".context" / "editor-versions.png")
    )
    page.once("dialog", lambda dialog: dialog.accept())
    picker.get_by_role("button", name="Delete", exact=True).click()
    expect(snapshots).to_have_count(0)
    assert live_server["db"].get_photo_edit_recipe(pid)["rotation"] == 90
