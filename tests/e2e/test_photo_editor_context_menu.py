import re

from playwright.sync_api import expect


def _open_editor(live_server, page):
    photo_id = live_server["data"]["photos"][0]
    page.goto(f"{live_server['url']}/edit/{photo_id}")
    expect(page.locator("#editorFilename")).to_have_text("hawk1.jpg")
    page.wait_for_function("() => !editorState.loading")


def test_photo_editor_right_click_opens_useful_menu(live_server, page):
    _open_editor(live_server, page)

    page.locator("#editorCanvasWrap").click(button="right", position={"x": 20, "y": 20})

    menu = page.locator(".vireo-ctx-menu")
    expect(menu).to_be_visible()
    for label in (
        "Save Changes",
        "Revert to Saved",
        "Show Saved Version",
        "Fit to Window",
        "View at 100%",
        "Auto Tone",
        "Edit Crop",
        "Full Frame",
        "Rotate Left",
        "Rotate Right",
        "Flip Horizontal",
        "Flip Vertical",
        "Copy Edit Settings",
        "Reset All Edits",
        "Export…",
        "Send to iNaturalist",
    ):
        expect(menu.get_by_text(label, exact=True)).to_be_attached()

    expect(menu.get_by_text("Save Changes", exact=True)).to_have_class(
        re.compile(r"vireo-ctx-disabled")
    )


def test_photo_editor_context_transform_updates_recipe(live_server, page):
    _open_editor(live_server, page)

    page.locator("#editorCanvasWrap").click(button="right", position={"x": 20, "y": 20})
    page.locator(".vireo-ctx-menu").get_by_text("Rotate Right", exact=True).click()

    expect(page.locator(".vireo-ctx-menu")).to_be_hidden()
    assert page.evaluate("() => editorState.recipe.rotation") == 90
    expect(page.locator("#saveBtn")).to_be_enabled()

    page.locator("#editorCanvasWrap").click(button="right", position={"x": 20, "y": 20})
    save_item = page.locator(".vireo-ctx-menu").get_by_text("Save Changes", exact=True)
    expect(save_item).not_to_have_class(re.compile(r"vireo-ctx-disabled"))


def test_photo_editor_context_disables_save_while_request_is_pending(
    live_server, page
):
    _open_editor(live_server, page)
    page.evaluate(
        """() => {
          rotateRecipe(90);
          const originalSafeFetch = window.safeFetch;
          window.safeFetch = function(url, options, config) {
            if (url.includes('/edit-recipe') && options && options.method === 'PUT') {
              return new Promise(resolve => { window.__resolveEditorSave = resolve; });
            }
            return originalSafeFetch(url, options, config);
          };
          window.__pendingEditorSave = saveRecipe();
        }"""
    )

    pending_items = page.evaluate(
        """() => {
          const wanted = new Set([
            'Save Changes', 'Revert to Saved', 'Auto Tone', 'Edit Crop',
            'Full Frame', 'Rotate Left', 'Rotate Right', 'Flip Horizontal',
            'Flip Vertical', 'Reset All Edits', 'Export…', 'Send to iNaturalist'
          ]);
          return buildPhotoEditorContextMenu()
            .filter(entry => wanted.has(entry.label))
            .map(entry => ({
              label: entry.label,
              disabled: entry.disabled,
              hint: entry.disabledHint,
            }));
        }"""
    )
    assert {item["label"] for item in pending_items} == {
        "Save Changes",
        "Revert to Saved",
        "Auto Tone",
        "Edit Crop",
        "Full Frame",
        "Rotate Left",
        "Rotate Right",
        "Flip Horizontal",
        "Flip Vertical",
        "Reset All Edits",
        "Export…",
        "Send to iNaturalist",
    }
    assert all(item["disabled"] is True for item in pending_items)
    assert all(item["hint"] == "Saving changes" for item in pending_items)

    # Saving must remain an independent state even if the working recipe
    # temporarily becomes clean while the request is unresolved.
    page.evaluate(
        """() => {
          editorState.recipe = cloneRecipe(editorState.savedRecipe);
          ensureCrop(editorState.recipe);
          syncControls();
        }"""
    )
    assert page.evaluate(
        """() => buildPhotoEditorContextMenu()
          .filter(item => item.label === 'Export…' || item.label === 'Send to iNaturalist')
          .every(item => item.disabled && item.disabledHint === 'Saving changes')"""
    ) is True

    page.evaluate("() => window.__resolveEditorSave({recipe: {rotation: 90}})")
    # The reset above is a newer edit. Completing the PUT updates the saved
    # baseline without replacing that working recipe or marking it clean.
    assert page.evaluate("window.__pendingEditorSave") is False
    assert page.evaluate("editorState.savedRecipe.rotation") == 90
    assert page.evaluate("editorState.recipe.rotation || 0") == 0
    assert page.evaluate("isEditorDirty()") is True
    expect(page.locator("#saveBtn")).to_be_enabled()
    assert page.evaluate(
        """() => buildPhotoEditorContextMenu()
          .find(item => item.label === 'Save Changes').disabled"""
    ) is False


def test_photo_editor_save_uses_authoritative_staleness_from_response(
    live_server, page
):
    _open_editor(live_server, page)
    page.evaluate(
        """() => {
          editorState.localStale = true;
          editorState.savedLocalStale = true;
          rotateRecipe(90);
          const originalSafeFetch = window.safeFetch;
          window.safeFetch = function(url, options, config) {
            if (url.includes('/edit-recipe') && options && options.method === 'PUT') {
              return new Promise(resolve => { window.__resolveStaleMaskSave = resolve; });
            }
            return originalSafeFetch(url, options, config);
          };
          window.__staleMaskSave = saveRecipe();
        }"""
    )

    page.evaluate(
        """() => {
          // Simulate Update Mask completing while the old-mask PUT is pending.
          // The response describes the recipe that actually won persistence.
          editorState.localStale = false;
          window.__resolveStaleMaskSave({
            recipe: {rotation: 90},
            local_mask_stale: true,
          });
        }"""
    )
    page.evaluate("() => window.__staleMaskSave")

    assert page.evaluate("() => editorState.localStale") is True
    assert page.evaluate("() => editorState.savedLocalStale") is True


def test_photo_editor_ignores_staleness_for_an_older_saved_recipe(
    live_server, page
):
    _open_editor(live_server, page)

    applied = page.evaluate(
        """() => {
          const requestedSavedRecipeSeq = editorState.savedRecipeSeq;
          editorState.savedRecipe = {version: 1, rotation: 90};
          editorState.savedRecipeSeq += 1;
          editorState.localStale = false;
          editorState.savedLocalStale = false;
          return applyLoadedLocalStaleness(
            editorState.photoId,
            editorState.loadSeq,
            requestedSavedRecipeSeq,
            {local_mask_stale: true}
          );
        }"""
    )

    assert applied is False
    assert page.evaluate("() => editorState.localStale") is False
    assert page.evaluate("() => editorState.savedLocalStale") is False


def test_photo_editor_context_revert_restores_saved_recipe(live_server, page):
    _open_editor(live_server, page)
    page.evaluate("() => rotateRecipe(90)")
    assert page.evaluate("() => isEditorDirty()") is True

    page.once("dialog", lambda dialog: dialog.accept())
    page.locator("#editorCanvasWrap").click(button="right", position={"x": 20, "y": 20})
    page.locator(".vireo-ctx-menu").get_by_text("Revert to Saved", exact=True).click()

    page.wait_for_function("() => !isEditorDirty()")
    assert page.evaluate("() => Number(editorState.recipe.rotation || 0)") == 0
    expect(page.locator("#saveBtn")).to_be_disabled()


def test_photo_editor_context_revert_restores_saved_stale_mask_state(
    live_server, page
):
    _open_editor(live_server, page)
    page.evaluate(
        """() => {
          editorState.savedLocalStale = true;
          editorState.localStale = false;
          editorState.localAvailable = true;
          rotateRecipe(90);
        }"""
    )

    page.once("dialog", lambda dialog: dialog.accept())
    page.locator("#editorCanvasWrap").click(button="right", position={"x": 20, "y": 20})
    page.locator(".vireo-ctx-menu").get_by_text("Revert to Saved", exact=True).click()

    assert page.evaluate("() => editorState.localStale") is True
    expect(page.locator("#localStaleBanner")).to_be_visible()


def test_photo_editor_context_revert_ignores_pending_mask_update(
    live_server, page
):
    _open_editor(live_server, page)
    page.evaluate(
        """() => {
          const saved = cloneRecipe(editorState.savedRecipe);
          saved.local = {
            mask: {ref: 'saved-mask', source_digest: 'saved-digest'},
            regions: [
              {region: 'subject', adjustments: {exposure: 0.5}}
            ],
          };
          editorState.savedRecipe = saved;
          editorState.savedRecipeSeq += 1;
          editorState.recipe = cloneRecipe(saved);
          editorState.savedLocalStale = true;
          editorState.localStale = true;
          syncControls();
          rotateRecipe(90);

          const originalSafeFetch = window.safeFetch;
          window.safeFetch = function(url, options, config) {
            if (url.includes('/local-mask/snapshot') &&
                options && options.method === 'POST') {
              return new Promise(resolve => {
                window.__resolvePendingMaskUpdate = resolve;
              });
            }
            return originalSafeFetch(url, options, config);
          };
          window.__pendingMaskUpdate = updateLocalMask();
        }"""
    )

    page.once("dialog", lambda dialog: dialog.accept())
    page.locator("#editorCanvasWrap").click(button="right", position={"x": 20, "y": 20})
    page.locator(".vireo-ctx-menu").get_by_text("Revert to Saved", exact=True).click()

    page.wait_for_function("() => !isEditorDirty()")
    page.evaluate(
        """() => {
          window.__resolvePendingMaskUpdate({
            mask: {ref: 'new-mask', source_digest: 'new-digest'},
          });
          return window.__pendingMaskUpdate;
        }"""
    )

    assert page.evaluate("() => !isEditorDirty()") is True
    assert page.evaluate("() => editorState.recipe.local.mask.ref") == "saved-mask"
    assert page.evaluate("() => editorState.localMask.ref") == "saved-mask"
    assert page.evaluate("() => editorState.localStale") is True


def test_photo_editor_keeps_native_menu_for_inputs(live_server, page):
    _open_editor(live_server, page)

    page.locator("#cropX").click(button="right")

    expect(page.locator(".vireo-ctx-menu")).to_have_count(0)


def test_photo_editor_export_opens_standard_export_for_current_photo(
    live_server, page
):
    _open_editor(live_server, page)
    photo_id = page.evaluate("() => editorState.photoId")

    page.locator("#editorCanvasWrap").click(button="right", position={"x": 20, "y": 20})
    page.locator(".vireo-ctx-menu").get_by_text("Export…", exact=True).click()

    expect(page.locator("#exportOverlay")).to_have_class("modal-overlay open")
    assert page.evaluate("() => editorState.photoId") == photo_id
    expect(page.locator("#exportSubmitBtn")).to_have_text("Export Photo")


def test_photo_editor_context_sends_current_photo_to_inaturalist(
    live_server, page
):
    _open_editor(live_server, page)
    photo_id = page.evaluate("() => editorState.photoId")
    page.evaluate(
        """() => {
          window.__inatTarget = null;
          window.submitToInat = function(id) { window.__inatTarget = id; };
        }"""
    )

    page.locator("#editorCanvasWrap").click(button="right", position={"x": 20, "y": 20})
    page.locator(".vireo-ctx-menu").get_by_text(
        "Send to iNaturalist", exact=True
    ).click()

    assert page.evaluate("() => window.__inatTarget") == photo_id


def test_photo_editor_inaturalist_requires_saved_changes_but_export_saves_them(
    live_server, page
):
    _open_editor(live_server, page)
    page.evaluate("() => rotateRecipe(90)")

    page.locator("#editorCanvasWrap").click(button="right", position={"x": 20, "y": 20})
    menu = page.locator(".vireo-ctx-menu")
    expect(menu.get_by_text("Export…", exact=True)).not_to_have_class(
        re.compile(r"vireo-ctx-disabled")
    )
    inat_item = menu.get_by_text("Send to iNaturalist", exact=True)
    expect(inat_item).to_have_class(re.compile(r"vireo-ctx-disabled"))
    expect(inat_item).to_have_attribute("title", re.compile("Save changes"))

    menu.get_by_text("Export…", exact=True).click()
    expect(page.locator("#exportOverlay")).to_have_class("modal-overlay open")
    page.wait_for_function("() => !isEditorDirty()")
