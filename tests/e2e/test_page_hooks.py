"""Each page registers the page hooks the shared navbar and lightbox call.

The navbar and lightbox reach page functions only through
``Vireo.pageHook(name)`` (vireo/static/vireo-page-hooks.js), so a page that
forgets to register one silently loses the command that uses it: the native
menu's Rate on Browse, the lightbox's Delete grid update, undo/redo's reload.
This test loads every page and compares what it registered with what it
should, so a lost registration fails here instead of in the desktop app.
"""
import pytest

_HISTORY = {"beforeHistoryChange", "afterHistoryChange"}
# The export modal's shared controllers (vireo-export-presets.js,
# vireo-export-collisions.js) call back into Browse and the Photo Editor.
_EXPORT_MODAL = {
    "applyExportPreset", "markExportCustom", "updateExportFormatControls",
    "updateExportPreview", "selectedExportMetadataFields", "buildExportPreflightRequest",
}

EXPECTED_HOOKS = {
    "/browse": {
        "getActiveSelection", "getSelectedPhotoId", "getPhotos", "getDetailPhotoId",
        "openBrowseShortcutPhoto", "moveBrowseSelection", "batchSetRating",
        "batchSetFlag", "setRatingFor", "setFlagFor", "setColorLabelFor",
        "setWildlifeExcludedFor", "openExportModal", "openPhotoExportModal",
        "revealPhoto", "copyPhotoPaths", "openBrowseCompare", "batchAddKeyword",
        "addToCollection", "openCaptureTimeModal", "batchDelete",
        "removeDeletedLightboxPhoto", "afterHistoryChange", "runSync",
    } | _EXPORT_MODAL,
    "/misses": {
        "getActiveSelection", "openBrowseShortcutPhoto", "afterHistoryChange",
        "loadMisses", "handleMissesLightboxFlagShortcut",
    },
    "/review": {
        "getGroupModalSelection", "getPredictions", "setReviewRating",
        "setReviewFlag", "revealReviewPhoto", "acceptPrediction",
        "rejectPrediction", "acceptAllPending", "runSync",
    },
    "/pipeline/review": {
        "getActiveSelection", "getGroupModalSelection", "batchSetRating",
        "batchSetFlag", "setRatingFor", "setFlagFor", "setWildlifeExcludedFor",
        "revealPhoto", "copyPhotoPaths", "batchAddKeyword", "addToCollection",
        "openPipelineLightbox", "getLightboxOpenOptions",
        "getLightboxBrowseDisabledHint", "getPhotoExternalEditDisabledHint",
        "runSync",
    } | _HISTORY,
    "/locations/review": _HISTORY | {"runSync"},
    "/life-list": {"afterHistoryChange", "openExportModal"},
    "/edit": _HISTORY | _EXPORT_MODAL | {"openExportModal", "localHistoryStatus", "changeLocalHistory"},
    "/pipeline/rapid-review": _HISTORY,
    "/cull": _HISTORY,
    "/highlights": {"afterHistoryChange", "loadHighlights", "updateHighlightsLightboxControls"},
    "/pipeline": {"startPipeline"},
    "/duplicates": {"startScan"},
    "/settings": {"verifyAllModels"},
    # Pages the native menu and lightbox act on without page state.
    "/jobs": set(),
    "/keywords": set(),
    "/move": set(),
    "/import": set(),
    "/workspace": set(),
    "/dashboard": set(),
    "/storage": set(),
}


@pytest.mark.parametrize("path", sorted(EXPECTED_HOOKS))
def test_page_registers_its_hooks(live_server, page, path):
    errors = []
    page.on("pageerror", lambda error: errors.append(str(error)))
    page.goto(f"{live_server['url']}{path}")
    page.wait_for_load_state("load")
    registered = page.evaluate(
        "() => Vireo.pageHookNames.filter(name => Vireo.pageHook(name) !== null)"
    )
    assert set(registered) == EXPECTED_HOOKS[path]
    assert errors == []


def test_unknown_hook_names_throw(live_server, page):
    page.goto(f"{live_server['url']}/jobs")
    messages = page.evaluate(
        """() => [
          () => Vireo.registerPageHooks({afterHistoryChang: () => {}}),
          () => Vireo.pageHook('getActiveSelect'),
          () => Vireo.registerPageHooks({afterHistoryChange: 'loadMisses'}),
        ].map(call => { try { call(); return null; } catch (e) { return e.message; } })"""
    )
    assert messages == [
        "Unknown page hook: afterHistoryChang",
        "Unknown page hook: getActiveSelect",
        "Page hook afterHistoryChange must be a function",
    ]
