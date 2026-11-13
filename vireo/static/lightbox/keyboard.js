import { navbarState } from '../navbar-state.js';
import { lbState } from './shared-state.js';
import { vireoLightboxSession, vireoLightboxViewport } from './state.js';
import { _lbApplyFlag } from './flags.js';
import { toggleLightboxBoxes, toggleLightboxChrome } from './overlays.js';
import {
  exitLightboxFullscreen, lightboxKeyMatchesConfiguredBrowseShortcut, lightboxNav,
  requestLightboxFullscreen,
} from './navigation.js';
import { closeLightbox, toggleLightboxZoom } from './controls.js';

// NOTE: Page navigation shortcuts (Cmd+1..0, Cmd+Shift+D/W/L, Cmd+,) are
// handled by the native menu bar when running inside the Tauri desktop shell.
// See src-tauri/src/menu.rs for the menu definition.
// If adding JS-based navigation shortcuts, guard with:
//   if (window.__TAURI_INTERNALS__) return;
// True when the key event's target is a field where typing/arrows must edit
// the field instead of driving lightbox shortcuts. Checkboxes and radios are
// deliberately NOT editable: the View menu's toggles keep focus after a
// click, and they consume no text or arrow keys, so hotkeys (b, h, arrows)
// must keep working while one is focused. Text inputs, selects, and range
// sliders (the Adjust panel) genuinely consume keys, so they stay guarded.
/** @param {EventTarget | null} t */ function _lbKeyTargetEditable(t) {
  if (!(t instanceof HTMLElement)) return false;
  if (t instanceof HTMLInputElement) return !(t.type === 'checkbox' || t.type === 'radio');
  return t.tagName === 'TEXTAREA' || t.tagName === 'SELECT' || !!t.isContentEditable;
}
/** @param {KeyboardEvent} e */ function consumeKey(e) {
  e.preventDefault();
  e.stopImmediatePropagation();
}

// Whether the lightbox is the surface the keyboard belongs to right now.
function lightboxHasKeyboard() {
  // Esc handling for the lightbox + overlay cascade is owned by the Keymap.pushEsc
  // stack now: each open*() pushes its close*() and each close*() pops it. The
  // listener below still handles arrow keys / +/-/0 / B / Z / F / G while the
  // lightbox is open.
  if (!document.getElementById('lightboxOverlay')?.classList.contains('active')) return false;
  // .grm-overlay.open is deliberately NOT in this suppression list: the burst
  // modal can only sit *underneath* the lightbox (review's "Open in Lightbox"),
  // so the lightbox keeps keyboard precedence; the burst modal's own keydown
  // handler bails while the lightbox is open.
  if (document.querySelector('.pipeline-overlay.active, .similar-overlay.active, .modal-overlay.open, .inspect-overlay.open, .shortcuts-overlay.open, .help-overlay.active, .report-overlay.active')) return false;
  // The command palette (Cmd+K) opens over the lightbox and toggles `hidden`
  // rather than a class; while it is open it owns the keyboard.
  var cmdPalette = document.getElementById('commandPalette');
  return !(cmdPalette && !cmdPalette.hidden);
}

// Each handler below returns true when the key was its to deal with, which
// ends the event for the lightbox.

/** @param {KeyboardEvent} e @param {boolean} editable */
function handleArrowKey(e, editable) {
  if (editable || (e.key !== 'ArrowRight' && e.key !== 'ArrowLeft')) return false;
  lightboxNav(e.key === 'ArrowRight' ? 1 : -1);
  consumeKey(e);
  return true;
}

/** @param {KeyboardEvent} e @param {boolean} editable */
function handleFullscreenKey(e, editable) {
  if (editable || e.ctrlKey || e.metaKey || e.altKey || e.shiftKey || lightboxKeyMatchesConfiguredBrowseShortcut(e)) return false;
  if (e.key.toLowerCase() === 'f') {
    requestLightboxFullscreen();
    consumeKey(e);
    return true;
  }
  if (e.key.toLowerCase() === 'g') {
    exitLightboxFullscreen();
    closeLightbox();
    consumeKey(e);
    return true;
  }
  return false;
}

/** @param {KeyboardEvent} e @param {boolean} editable */
function handleViewToggleKey(e, editable) {
  if (editable) return false;
  var boxesKey = (navbarState.shortcuts && navbarState.shortcuts.browse && navbarState.shortcuts.browse.toggle_boxes) || 'b';
  if (matchesShortcut(e, boxesKey)) {
    toggleLightboxBoxes();
    consumeKey(e);
    return true;
  }
  // Presence check: `''` means the migration blanked this to avoid stealing
  // an existing user binding on 'h'. Only fall back to the 'h' default when
  // the key is genuinely absent from config (shortcuts not yet loaded).
  var browseSc = navbarState.shortcuts && navbarState.shortcuts.browse;
  var uiKey = browseSc && 'toggle_ui' in browseSc ? browseSc.toggle_ui : 'h';
  if (matchesShortcut(e, uiKey)) {
    toggleLightboxChrome();
    consumeKey(e);
    return true;
  }
  return false;
}

/** @param {KeyboardEvent} e @param {boolean} editable */
function handleZoomToggleKey(e, editable) {
  var zoomKey = (navbarState.shortcuts && navbarState.shortcuts.browse) ? navbarState.shortcuts.browse.zoom : 'z';
  if (editable || !matchesShortcut(e, zoomKey)) return false;
  // Zoom toggle — simulate a click in the center
  var wrap = document.getElementById('lightboxWrap'); if (!wrap) return true;
  var rect = wrap.getBoundingClientRect();
  toggleLightboxZoom({clientX: rect.left + rect.width/2, clientY: rect.top + rect.height/2});
  consumeKey(e);
  // Handled, so a user-remapped zoomKey (e.g. '+') doesn't also trigger step-zoom.
  return true;
}

// Flag / Reject / Unflag — operate on the photo currently displayed in the
// lightbox. The lightbox is shared across pages; the setFlagFor (browse) and
// setReviewFlag (review) page hooks update their page's local model, so prefer them
// when available so badges/grids stay in sync without a refetch. The bare
// POST fallback covers pages that open the lightbox without a flag helper
// (misses, pipeline-review). Not limited to unmodified keys, so user
// rebindings to combos like Ctrl+P still match — matchesShortcut already
// enforces the exact modifier set the binding declares.
/** @param {KeyboardEvent} e @param {boolean} editable */
function handleFlagKey(e, editable) {
  if (editable) return false;
  var flagKey = (navbarState.shortcuts && navbarState.shortcuts.browse && navbarState.shortcuts.browse.flag) || 'p';
  var rejectKey = (navbarState.shortcuts && navbarState.shortcuts.browse && navbarState.shortcuts.browse.reject) || 'x';
  var unflagKey = (navbarState.shortcuts && navbarState.shortcuts.browse && navbarState.shortcuts.browse.unflag) || 'u';
  /** @type {LightboxFlag | null} */ var lbFlag = null;
  if (matchesShortcut(e, flagKey)) lbFlag = 'flagged';
  else if (matchesShortcut(e, rejectKey)) lbFlag = 'rejected';
  else if (matchesShortcut(e, unflagKey)) lbFlag = 'none';
  var pid = vireoLightboxSession.requestedPhotoId();
  if (lbFlag === null || pid == null) return false;
  var missesFlagShortcut = Vireo.pageHook('handleMissesLightboxFlagShortcut');
  if (!(missesFlagShortcut && missesFlagShortcut(pid, lbFlag) === true)) _lbApplyFlag(pid, lbFlag);
  consumeKey(e);
  return true;
}

// Continuous zoom keyboard shortcuts. stopImmediatePropagation prevents these keys
// from reaching the browse-page rating handler — otherwise pressing '0' to zoom-to-fit
// would also fire rate_0 and silently clear the photo's rating.
/** @param {KeyboardEvent} e @param {boolean} editable */
function handleZoomStepKey(e, editable) {
  if (editable || e.ctrlKey || e.metaKey || e.altKey) return;
  if (e.key === '+' || e.key === '=') {
    vireoLightboxViewport.stepZoom(1);
    consumeKey(e);
  } else if (e.key === '-' || e.key === '_') {
    vireoLightboxViewport.stepZoom(-1);
    consumeKey(e);
  } else if (e.key === '0') {
    vireoLightboxViewport.fit();
    consumeKey(e);
  }
}

/** @param {KeyboardEvent} e */
function onLightboxKeydown(e) {
  if (!lightboxHasKeyboard()) return;
  // A form field keeps its keys: arrows inside one (e.g. the mask-variant
  // <select>) change its value, not the photo, and modals opened from the
  // lightbox (e.g. iNaturalist) need '-' / '0' as literal input.
  var editable = _lbKeyTargetEditable(e.target);
  if (handleArrowKey(e, editable)) return;
  // The outgoing photo is still visible while the incoming source decodes.
  // Suppress photo-targeted shortcuts until the visible identity commits so
  // ratings, edits, and zoom operations cannot affect a hidden photo.
  if (lbState.visualTransitionPending && e.key !== 'Escape') {
    consumeKey(e);
    return;
  }
  if (handleFullscreenKey(e, editable) || handleViewToggleKey(e, editable) ||
      handleZoomToggleKey(e, editable) || handleFlagKey(e, editable)) return;
  handleZoomStepKey(e, editable);
}

/* ---------- Ctrl+Tab / Ctrl+Shift+Tab — cycle navbar tabs ---------- */
/** @param {KeyboardEvent} e */
function cycleNavTabs(e) {
  if (e.key !== 'Tab' || !e.ctrlKey || e.altKey || e.metaKey) return;
  e.preventDefault();
  var navLinks = Array.from(/** @type {NodeListOf<HTMLAnchorElement>} */ (document.querySelectorAll('#navTabStrip .nav-tab[href]')));
  if (!navLinks.length) return;
  var activeIdx = -1;
  for (var i = 0; i < navLinks.length; i++) {
    if (navLinks[i]?.classList.contains('active')) { activeIdx = i; break; }
  }
  var dir = e.shiftKey ? -1 : 1;
  var next = (activeIdx + dir + navLinks.length) % navLinks.length;
  var link = navLinks[next]; if (link) window.location.href = link.href;
}

// The lightbox's keys and Ctrl+Tab tab cycling. Bound by navbar-boot.js last
// among the navbar's document keydown listeners, the classic scripts' order:
// the lightbox handler stops propagation for the keys it takes.
export function bindLightboxKeyboard() {
  document.addEventListener('keydown', onLightboxKeydown);
  document.addEventListener('keydown', cycleNavTabs);
}

/* ---------- Keyboard Shortcut Helpers (global) ---------- */
/** @param {string} str */ export function parseShortcut(str) {
  var parts = str.toLowerCase().split('+');
  var key = /** @type {string} */ (parts.pop());
  var mods = {ctrl: false, meta: false, shift: false, alt: false};
  parts.forEach(function(m) { if (m === 'ctrl' || m === 'meta' || m === 'shift' || m === 'alt') mods[m] = true; });
  return {key: key, ctrl: mods.ctrl, meta: mods.meta, shift: mods.shift, alt: mods.alt};
}

export function matchesShortcut(/** @type {KeyboardEvent} */ e, /** @type {string | undefined} */ shortcutStr) {
  if (!shortcutStr) return false;
  var sc = parseShortcut(shortcutStr);
  if (e.key.toLowerCase() !== sc.key) return false;
  var wantCtrl = sc.ctrl || sc.meta;
  var hasCtrl = e.ctrlKey || e.metaKey;
  if (wantCtrl !== hasCtrl) return false;
  if (sc.shift !== e.shiftKey) return false;
  if (sc.alt !== e.altKey) return false;
  return true;
}

/** @param {string} str */ export function formatShortcut(str) {
  if (!str) return 'Unassigned';
  return str.split('+').map(function(p) {
    if (p === ' ') return 'Space';
    return p.charAt(0).toUpperCase() + p.slice(1);
  }).join('+');
}
