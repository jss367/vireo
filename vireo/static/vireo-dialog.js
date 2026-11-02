/* Shared lifecycle for classic-script dialogs. Load after keymap.js.
   Owners keep their cleanup in onDismiss and call close() on every close path. */
(function(/** @type {Window} */ global) {
  'use strict';

  /** @type {VireoDialogSession[]} */
  const stack = [];
  /** @type {Map<HTMLElement, boolean>} */
  const background = new Map();
  const focusableSelector = 'a[href], area[href], button, input, select, textarea, iframe, [tabindex], [contenteditable]';

  function updateBackground() {
    background.forEach(function(wasInert, element) { element.inert = wasInert; });
    background.clear();
    const top = stack.at(-1);
    if (!top) return;
    // Inert siblings along the ancestor path, rather than the ancestor that
    // contains the dialog. This also works for dialogs inside a page layout.
    let branch = top.overlay;
    while (branch.parentElement && branch !== document.body) {
      const parent = branch.parentElement;
      for (const sibling of parent.children) {
        if (sibling === branch || !(sibling instanceof HTMLElement)) continue;
        background.set(sibling, sibling.inert);
        sibling.inert = true;
      }
      branch = parent;
    }
  }

  /** @param {HTMLElement | null} element */
  function canFocus(element) {
    return !!element && element.isConnected && !element.matches(':disabled') &&
      !element.closest('[inert]') && element.getClientRects().length > 0 &&
      global.getComputedStyle(element).visibility === 'visible';
  }

  /** @param {HTMLElement} overlay */
  function focusableElements(overlay) {
    return Array.from(overlay.querySelectorAll(focusableSelector))
      .filter(function(element) { return element instanceof HTMLElement; })
      .filter(function(element) { return element.tabIndex >= 0 && canFocus(element); })
      .sort(function(a, b) { return (a.tabIndex || Infinity) - (b.tabIndex || Infinity); });
  }

  /**
   * @param {HTMLElement} overlay
   * @param {VireoDialogOptions} [options]
   * @returns {VireoDialogController}
   */
  function create(overlay, options = {}) {
    const keymap = global.Keymap;
    /** @type {VireoDialogSession | null} */
    let session = null;
    if (!overlay.hasAttribute('tabindex')) overlay.tabIndex = -1;

    function focusInside() {
      const initial = options.initialFocus;
      const target = initial && overlay.contains(initial) && canFocus(initial)
        ? initial : focusableElements(overlay)[0] || overlay;
      target.focus({preventScroll: true});
    }

    /** @param {KeyboardEvent} event */
    function trapTab(event) {
      if (!session || stack.at(-1) !== session || event.key !== 'Tab' ||
          event.ctrlKey || event.metaKey || event.altKey) return;
      // Recompute on each press: rule controls may be added, hidden or disabled.
      const elements = focusableElements(overlay);
      const active = document.activeElement;
      const index = active instanceof HTMLElement ? elements.indexOf(active) : -1;
      const next = index < 0 ? (event.shiftKey ? elements.length - 1 : 0)
        : (index + (event.shiftKey ? -1 : 1) + elements.length) % elements.length;
      // Own every Tab step: WebKit's default keyboard preference can skip
      // buttons and move focus into browser chrome even with an inert page.
      event.preventDefault();
      const target = elements[next] || overlay;
      target.focus({preventScroll: true});
      // A dialog's own scrollable list (e.g. the batch
      // Add-to-Collection chooser's max-height:200px overflow-y:auto area)
      // would otherwise hide the newly focused button when Tab lands below
      // the fold, since preventScroll suppresses the browser's default
      // focus-scroll. block/inline 'nearest' only scrolls the nearest
      // scrollable ancestor that already needs to scroll to show the
      // element, so visible controls are untouched and the page-level
      // scroll (already locked while a dialog is open) is unaffected.
      if (typeof target.scrollIntoView === 'function') {
        target.scrollIntoView({block: 'nearest', inline: 'nearest'});
      }
    }

    /** @param {FocusEvent} event */
    function containFocus(event) {
      if (session && stack.at(-1) === session &&
          event.target instanceof Node && !overlay.contains(event.target)) focusInside();
    }

    /** @param {Element | null} [returnFocus] */
    function open(returnFocus = document.activeElement) {
      if (session) return;
      overlay.classList.add('open');
      const originalZIndex = overlay.style.zIndex;
      const top = stack.at(-1);
      if (top) {
        overlay.style.zIndex = String(Math.max(
          Number(global.getComputedStyle(top.overlay).zIndex) || 0,
          Number(global.getComputedStyle(overlay).zIndex) || 0
        ) + 1);
      }
      session = {
        overlay: overlay,
        originalZIndex: originalZIndex,
        returnFocus: returnFocus instanceof HTMLElement ? returnFocus : null,
        focusInside: focusInside,
        escToken: keymap.pushEsc(function() { (options.onDismiss || close)(); }),
      };
      stack.push(session);
      keymap.lockBodyScroll();
      updateBackground();
      document.addEventListener('keydown', trapTab, true);
      document.addEventListener('focusin', containFocus);
      focusInside();
    }

    function close() {
      if (!session) return;
      const closing = session;
      const wasTop = stack.at(-1) === closing;
      stack.splice(stack.indexOf(closing), 1);
      session = null;
      keymap.popEsc(closing.escToken);
      keymap.unlockBodyScroll();
      document.removeEventListener('keydown', trapTab, true);
      document.removeEventListener('focusin', containFocus);
      overlay.classList.remove('open');
      overlay.style.zIndex = closing.originalZIndex;
      // If a lower dialog closes first, its opener becomes the return target
      // for any upper dialog whose original opener was inside the lower one.
      for (const other of stack) {
        if (other.returnFocus && overlay.contains(other.returnFocus)) other.returnFocus = closing.returnFocus;
      }
      updateBackground();
      if (!wasTop) return;
      const top = stack.at(-1);
      const target = closing.returnFocus;
      if (target && canFocus(target) && (!top || top.overlay.contains(target))) {
        target.focus({preventScroll: true});
      } else if (top) {
        top.focusInside();
      }
    }

    return {open: open, close: close};
  }

  // Overlays that have not yet migrated to this lifecycle (help, report issue,
  // the lightbox) still open as plain siblings of the active dialog's branch.
  // updateBackground() therefore marks them inert while a dialog is on the
  // stack, which would present an inert help/report overlay whose controls
  // cannot receive focus. Owners of those overlays guard their keyboard
  // shortcuts with hasActive() and treat the press as a no-op until the
  // managed dialog is dismissed.
  function hasActive() { return stack.length > 0; }

  global.VireoDialog = {create: create, hasActive: hasActive};
})(window);
