// Navbar state that classic pages also read and write as window properties.
// The navbar modules use this object; navbar-globals.js publishes
// window._vireoShortcuts and window._vireoNativeMenuPhotoIdsOverride as
// accessors onto it, so a page's `window._vireoShortcuts = cfg...` lands here.
// This module imports nothing, so it is always evaluated first.

export const navbarState = {
  /**
   * The user's keyboard shortcuts by scope (browse, lightbox, navigation, ...)
   * from /api/config, set by the page or by the navbar once they load.
   * @type {Record<string, Record<string, string>> | undefined}
   */
  shortcuts: undefined,
  /**
   * The photos a native-menu command targets instead of the page's
   * selection, set just before the command opens a page dialog.
   * @type {number[] | null}
   */
  nativeMenuPhotoIdsOverride: null,
};
