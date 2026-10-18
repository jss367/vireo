"""Reach the shared navbar's and lightbox's internals from a browser test.

The navbar and lightbox are ES modules (vireo/static/navbar-boot.js), so only
the handful of names classic pages still call are globals (navbar-globals.js).
Browser tests reach much further in, reading and setting lightbox state by the
global names these scripts used to declare (``_lbCurrentSrcKey``,
``vireoLightboxViewport``, ``LB_DETAIL_SHOW_DELAY_MS``, ...).

``expose_navbar_modules(page)`` keeps those tests working without the page
publishing anything: a test-side ``import()`` of a module URL the page already
loaded returns the page's own instance (see module_pages.py), and the test
defines window accessors onto it:

- every export of every navbar and lightbox module, read live (a module's
  ``let`` reassigned later reads its current value);
- the old global names of state that moved onto exported state objects, read
  and written through the object (``_lbCurrentSrcKey = 'full'`` sets
  ``lbState.currentSrcKey``);
- ``vireoLightboxSession`` / ``vireoLightboxViewport`` assignments, which
  replace the controller through ``replaceLightboxControllers`` so every
  lightbox module sees the stub, as reassigning the global once did.

Names a page already has on window (the bridge's globals, which tests also
stub) are left alone. The ``page`` fixture in conftest.py installs this on
every page: an init script defines the accessors once each document has
parsed, and ``page.goto`` / ``page.reload`` wait for them before returning,
then run the ``page.after_navigation`` callbacks.
"""
import json
import re
from pathlib import Path

STATIC = Path(__file__).resolve().parents[2] / "vireo" / "static"

# Old global name -> (module, state object, field) for state that moved onto an
# exported object, so tests can still read and assign it by that name.
STATE_FIELDS = {
    "_lbAdjustSaveTimer": ("lightbox/shared-state.js", "lbState", "adjustSaveTimer"),
    "_lbAdjustSeq": ("lightbox/shared-state.js", "lbState", "adjustSeq"),
    "_lbAdjustmentSource": ("lightbox/shared-state.js", "lbState", "adjustmentSource"),
    "_lbCurrentEditRecipe": ("lightbox/shared-state.js", "lbState", "currentEditRecipe"),
    "_lbCurrentSrcKey": ("lightbox/shared-state.js", "lbState", "currentSrcKey"),
    "_lbCurrentWildlifeExcluded": ("lightbox/shared-state.js", "lbState", "currentWildlifeExcluded"),
    "_lbDeferredOverlayApply": ("lightbox/shared-state.js", "lbState", "deferredOverlayApply"),
    "_lbDesiredSrcKey": ("lightbox/shared-state.js", "lbState", "desiredSrcKey"),
    "_lbDetailStatusHold": ("lightbox/shared-state.js", "lbState", "detailStatusHold"),
    "_lbEditRecipe": ("lightbox/shared-state.js", "lbState", "editRecipe"),
    "_lbEditRecipeLoaded": ("lightbox/shared-state.js", "lbState", "editRecipeLoaded"),
    "_lbEditRecipeWriteSeq": ("lightbox/shared-state.js", "lbState", "editRecipeWriteSeq"),
    "_lbEscToken": ("lightbox/shared-state.js", "lbState", "escToken"),
    "_lbFlagEditSeq": ("lightbox/shared-state.js", "lbState", "flagEditSeq"),
    "_lbFlagPendingWrites": ("lightbox/shared-state.js", "lbState", "flagPendingWrites"),
    "_lbFullLongEdge": ("lightbox/shared-state.js", "lbState", "fullLongEdge"),
    "_lbFullUsesOriginal": ("lightbox/shared-state.js", "lbState", "fullUsesOriginal"),
    "_lbInitialDecodePending": ("lightbox/shared-state.js", "lbState", "initialDecodePending"),
    "_lbOriginalUnavailable": ("lightbox/shared-state.js", "lbState", "originalUnavailable"),
    "_lbPhotoH": ("lightbox/shared-state.js", "lbState", "photoH"),
    "_lbPhotoOrientation": ("lightbox/shared-state.js", "lbState", "photoOrientation"),
    "_lbPhotoW": ("lightbox/shared-state.js", "lbState", "photoW"),
    "_lbPreviewLoading": ("lightbox/shared-state.js", "lbState", "previewLoading"),
    "_lbPreviewMaxSize": ("lightbox/shared-state.js", "lbState", "previewMaxSize"),
    "_lbProgressiveTargetKey": ("lightbox/shared-state.js", "lbState", "progressiveTargetKey"),
    "_lbReadOnly": ("lightbox/shared-state.js", "lbState", "readOnly"),
    "_lbReadOnlyMessage": ("lightbox/shared-state.js", "lbState", "readOnlyMessage"),
    "_lbSessionFullUsesOriginal": ("lightbox/shared-state.js", "lbState", "sessionFullUsesOriginal"),
    "_lbVisualTransitionPending": ("lightbox/shared-state.js", "lbState", "visualTransitionPending"),
    "_lightboxPhotoList": ("lightbox/shared-state.js", "lbState", "photoList"),
    "_lightboxZoomHandled": ("lightbox/shared-state.js", "lbState", "zoomHandled"),
    "_lbBoxesVisible": ("lightbox/overlays.js", "lbOverlays", "boxesVisible"),
    "_lbMasksVisible": ("lightbox/overlays.js", "lbOverlays", "masksVisible"),
    "_lbEyeVisible": ("lightbox/overlays.js", "lbOverlays", "eyeVisible"),
    "_lbTrackEyeEnabled": ("lightbox/overlays.js", "lbOverlays", "trackEyeEnabled"),
    "_lbInfoVisible": ("lightbox/overlays.js", "lbOverlays", "infoVisible"),
    "_lbChromeVisible": ("lightbox/overlays.js", "lbOverlays", "chromeVisible"),
    "_lbMaskCurrentUrl": ("lightbox/overlays.js", "lbOverlays", "maskCurrentUrl"),
    "LB_DETAIL_SHOW_DELAY_MS": ("lightbox/source-loading.js", "lbDetailStatus", "showDelayMs"),
    "LB_DETAIL_SETTLED_MS": ("lightbox/source-loading.js", "lbDetailStatus", "settledMs"),
    "LB_DETAIL_FADE_MS": ("lightbox/source-loading.js", "lbDetailStatus", "fadeMs"),
    "_lbDetailStatusShown": ("lightbox/source-loading.js", "lbDetailStatus", "shown"),
    "_lbDetailStatusSettled": ("lightbox/source-loading.js", "lbDetailStatus", "settled"),
    "_lbDetailShowTimer": ("lightbox/source-loading.js", "lbDetailStatus", "showTimer"),
    "_lbDetailSettleTimer": ("lightbox/source-loading.js", "lbDetailStatus", "settleTimer"),
    "_lbDetailFadeTimer": ("lightbox/source-loading.js", "lbDetailStatus", "fadeTimer"),
    "_missingFoldersLastIds": ("navbar-folder-banners.js", "missingFolders", "lastIds"),
    "_missingFoldersObservationGen": ("navbar-folder-banners.js", "missingFolders", "observationGen"),
    "_missingFoldersMutationGen": ("navbar-folder-banners.js", "missingFolders", "mutationGen"),
    "_missingFoldersMutationInFlight": ("navbar-folder-banners.js", "missingFolders", "mutationInFlight"),
    "_missingFoldersMutationEpoch": ("navbar-folder-banners.js", "missingFolders", "mutationEpoch"),
    "_missingFoldersReconciliationPending": ("navbar-folder-banners.js", "missingFolders", "reconciliationPending"),
    "_missingFoldersServerVersion": ("navbar-folder-banners.js", "missingFolders", "serverVersion"),
}


def navbar_module_paths():
    """Every module navbar-boot.js imports, as /static paths."""
    entry = (STATIC / "navbar-boot.js").read_text(encoding="utf-8")
    return ["navbar-boot.js"] + re.findall(r"^import (?:\{[^}]*\} from )?'\./([\w/.-]+\.js)';$", entry, re.M)


_EXPOSE = """async ([modules, fields]) => {
  if (document.readyState === 'loading') {
    await new Promise(resolve => document.addEventListener('DOMContentLoaded', resolve, {once: true}));
  }
  if (!document.querySelector('script[type="module"][src="/static/navbar-boot.js"]')) return false;
  const loaded = {};
  for (const path of modules) loaded[path] = await import('/static/' + path);
  const define = (name, descriptor) => {
    const own = Object.getOwnPropertyDescriptor(window, name);
    if (own && !own.configurable) return;
    Object.defineProperty(window, name, {configurable: true, enumerable: true, ...descriptor});
  };
  for (const path of modules) {
    const namespace = loaded[path];
    for (const name of Object.keys(namespace)) {
      if (name in window && !['vireoLightboxSession', 'vireoLightboxViewport'].includes(name)) continue;
      define(name, {get: () => namespace[name]});
    }
  }
  const state = loaded['lightbox/state.js'];
  for (const [name, key] of [['vireoLightboxSession', 'session'], ['vireoLightboxViewport', 'viewport']]) {
    define(name, {
      get: () => state[name],
      set: value => state.replaceLightboxControllers({[key]: value}),
    });
  }
  for (const [name, [path, object, field]] of Object.entries(fields)) {
    const holder = loaded[path][object];
    define(name, {get: () => holder[field], set: value => { holder[field] = value; }});
  }
  return true;
}"""


def expose_navbar_modules(page):
    """Define window accessors for the navbar's and lightbox's internals (see
    the module docstring). Returns False on a page without the navbar."""
    return page.evaluate(_EXPOSE, [navbar_module_paths(), STATE_FIELDS])


def install_navbar_exposure(page):
    """Run expose_navbar_modules after every page.goto and page.reload, then
    each callable in ``page.after_navigation`` (test helpers that patch the
    page's modules once they have loaded append to it).

    A ``wait_until="commit"`` navigation returns before the page's scripts
    run, so it is left alone."""
    goto, reload = page.goto, page.reload
    page.after_navigation = []
    # Navigations the test does not make through goto/reload (a link, a
    # keyboard shortcut) still get the accessors, once the document has
    # parsed; goto and reload also wait for them before returning.
    page.add_init_script(
        f"({_EXPOSE})({json.dumps([navbar_module_paths(), STATE_FIELDS])}).catch(() => {{}});"
    )

    def after(kwargs):
        if kwargs.get("wait_until") != "commit" and expose_navbar_modules(page):
            for callback in page.after_navigation:
                callback(page)

    def goto_and_expose(*args, **kwargs):
        response = goto(*args, **kwargs)
        after(kwargs)
        return response

    def reload_and_expose(*args, **kwargs):
        response = reload(*args, **kwargs)
        after(kwargs)
        return response

    page.goto = goto_and_expose
    page.reload = reload_and_expose
    return page


__all__ = ["expose_navbar_modules", "install_navbar_exposure", "navbar_module_paths", "STATE_FIELDS"]
