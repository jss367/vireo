
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
function _lbKeyTargetEditable(t) {
  if (!t) return false;
  if (t.tagName === 'INPUT') return !(t.type === 'checkbox' || t.type === 'radio');
  return t.tagName === 'TEXTAREA' || t.tagName === 'SELECT' || !!t.isContentEditable;
}
document.addEventListener('keydown', function(e) {
  // Esc handling for the lightbox + overlay cascade is owned by the Keymap.pushEsc
  // stack now: each open*() pushes its close*() and each close*() pops it. The
  // listener below still handles arrow keys / +/-/0 / B / Z / F / G while the
  // lightbox is open.
  if (!document.getElementById('lightboxOverlay').classList.contains('active')) return;
  // .grm-overlay.open is deliberately NOT in this suppression list: the burst
  // modal can only sit *underneath* the lightbox (review's "Open in Lightbox"),
  // so the lightbox keeps keyboard precedence; the burst modal's own keydown
  // handler bails while the lightbox is open.
  if (document.querySelector('.pipeline-overlay.active, .similar-overlay.active, .modal-overlay.open, .inspect-overlay.open, .shortcuts-overlay.open, .help-overlay.active, .report-overlay.active')) return;
  // The command palette (Cmd+K) opens over the lightbox and toggles `hidden`
  // rather than a class; while it is open it owns the keyboard.
  var cmdPalette = document.getElementById('commandPalette');
  if (cmdPalette && !cmdPalette.hidden) return;
  var editable0 = _lbKeyTargetEditable(e.target);
  // Editable check must come before arrow handling so arrows inside form
  // fields (e.g. the mask-variant <select>) change the value, not the photo.
  if (!editable0 && e.key === 'ArrowRight') {
    lightboxNav(1);
    e.preventDefault();
    e.stopImmediatePropagation();
    return;
  }
  if (!editable0 && e.key === 'ArrowLeft') {
    lightboxNav(-1);
    e.preventDefault();
    e.stopImmediatePropagation();
    return;
  }
  // The outgoing photo is still visible while the incoming source decodes.
  // Suppress photo-targeted shortcuts until the visible identity commits so
  // ratings, edits, and zoom operations cannot affect a hidden photo.
  if (_lbVisualTransitionPending && e.key !== 'Escape') {
    e.preventDefault();
    e.stopImmediatePropagation();
    return;
  }
  if (!editable0 && !e.ctrlKey && !e.metaKey && !e.altKey && !e.shiftKey && !lightboxKeyMatchesConfiguredBrowseShortcut(e)) {
    if (e.key.toLowerCase() === 'f') {
      requestLightboxFullscreen();
      e.preventDefault();
      e.stopImmediatePropagation();
      return;
    }
    if (e.key.toLowerCase() === 'g') {
      exitLightboxFullscreen();
      closeLightbox();
      e.preventDefault();
      e.stopImmediatePropagation();
      return;
    }
  }
  var boxesKey = (window._vireoShortcuts && window._vireoShortcuts.browse && window._vireoShortcuts.browse.toggle_boxes) || 'b';
  if (matchesShortcut(e, boxesKey)) {
    if (!_lbKeyTargetEditable(e.target)) {
      toggleLightboxBoxes();
      e.preventDefault();
      e.stopImmediatePropagation();
      return;
    }
  }
  // Presence check: `''` means the migration blanked this to avoid stealing
  // an existing user binding on 'h'. Only fall back to the 'h' default when
  // the key is genuinely absent from config (shortcuts not yet loaded).
  var browseSc = window._vireoShortcuts && window._vireoShortcuts.browse;
  var uiKey = browseSc && 'toggle_ui' in browseSc ? browseSc.toggle_ui : 'h';
  if (matchesShortcut(e, uiKey)) {
    if (!_lbKeyTargetEditable(e.target)) {
      toggleLightboxChrome();
      e.preventDefault();
      e.stopImmediatePropagation();
      return;
    }
  }
  var zoomKey = (window._vireoShortcuts && window._vireoShortcuts.browse) ? window._vireoShortcuts.browse.zoom : 'z';
  if (matchesShortcut(e, zoomKey) && !_lbKeyTargetEditable(e.target)) {
    // Zoom toggle — simulate a click in the center
    var wrap = document.getElementById('lightboxWrap');
    var rect = wrap.getBoundingClientRect();
    toggleLightboxZoom({clientX: rect.left + rect.width/2, clientY: rect.top + rect.height/2, stopPropagation: function(){}});
    e.preventDefault();
    e.stopImmediatePropagation();
    return;  // Stop here so a user-remapped zoomKey (e.g. '+') doesn't also trigger step-zoom below.
  }
  // Skip when focus is in a form field — modals opened from the lightbox (e.g. iNaturalist)
  // need '-' / '0' as literal input in lat/lon fields.
  var editable = _lbKeyTargetEditable(e.target);
  // Flag / Reject / Unflag — operate on the photo currently displayed in the
  // lightbox. The lightbox is shared across pages; setFlagFor (browse) and
  // setReviewFlag (review) update their page's local model, so prefer them
  // when available so badges/grids stay in sync without a refetch. The bare
  // POST fallback covers pages that open the lightbox without a flag helper
  // (misses, pipeline-review). Lives outside the no-modifier guard below so
  // user rebindings to combos like Ctrl+P still match — matchesShortcut
  // already enforces the exact modifier set the binding declares.
  if (!editable) {
    var flagKey = (window._vireoShortcuts && window._vireoShortcuts.browse && window._vireoShortcuts.browse.flag) || 'p';
    var rejectKey = (window._vireoShortcuts && window._vireoShortcuts.browse && window._vireoShortcuts.browse.reject) || 'x';
    var unflagKey = (window._vireoShortcuts && window._vireoShortcuts.browse && window._vireoShortcuts.browse.unflag) || 'u';
    var lbFlag = null;
    if (matchesShortcut(e, flagKey)) lbFlag = 'flagged';
    else if (matchesShortcut(e, rejectKey)) lbFlag = 'rejected';
    else if (matchesShortcut(e, unflagKey)) lbFlag = 'none';
    if (lbFlag !== null && _lightboxCurrentId != null) {
      var pid = _lightboxCurrentId;
      if (
        typeof window.handleMissesLightboxFlagShortcut === 'function' &&
        window.handleMissesLightboxFlagShortcut(pid, lbFlag) === true
      ) {
        e.preventDefault();
        e.stopImmediatePropagation();
        return;
      }
      _lbApplyFlag(pid, lbFlag);
      e.preventDefault();
      e.stopImmediatePropagation();
      return;
    }
  }
  // Continuous zoom keyboard shortcuts. stopImmediatePropagation prevents these keys
  // from reaching the browse-page rating handler — otherwise pressing '0' to zoom-to-fit
  // would also fire rate_0 and silently clear the photo's rating.
  if (!editable && !e.ctrlKey && !e.metaKey && !e.altKey) {
    if (e.key === '+' || e.key === '=') {
      _lbClearPendingViewportRestore();
      _lbSetZoom(_lbZoom * 1.25, null, null);
      e.preventDefault();
      e.stopImmediatePropagation();
    } else if (e.key === '-' || e.key === '_') {
      _lbClearPendingViewportRestore();
      _lbSetZoom(_lbZoom * 0.8, null, null);
      e.preventDefault();
      e.stopImmediatePropagation();
    } else if (e.key === '0') {
      _lbClearPendingViewportRestore();
      _lbSetZoom(1.0, null, null);
      e.preventDefault();
      e.stopImmediatePropagation();
    }
  }
});

/* ---------- Ctrl+Tab / Ctrl+Shift+Tab — cycle navbar tabs ---------- */
document.addEventListener('keydown', function(e) {
  if (e.key !== 'Tab' || !e.ctrlKey || e.altKey || e.metaKey) return;
  e.preventDefault();
  var navLinks = Array.prototype.slice.call(
    document.querySelectorAll('#navTabStrip .nav-tab[href]')
  );
  if (!navLinks.length) return;
  var activeIdx = -1;
  for (var i = 0; i < navLinks.length; i++) {
    if (navLinks[i].classList.contains('active')) { activeIdx = i; break; }
  }
  var dir = e.shiftKey ? -1 : 1;
  var next = (activeIdx + dir + navLinks.length) % navLinks.length;
  window.location.href = navLinks[next].getAttribute('href');
});

/* ---------- Keyboard Shortcut Helpers (global) ---------- */
function parseShortcut(str) {
  var parts = str.toLowerCase().split('+');
  var key = parts.pop();
  var mods = {ctrl: false, meta: false, shift: false, alt: false};
  parts.forEach(function(m) { if (m in mods) mods[m] = true; });
  return {key: key, ctrl: mods.ctrl, meta: mods.meta, shift: mods.shift, alt: mods.alt};
}

function matchesShortcut(e, shortcutStr) {
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

function formatShortcut(str) {
  if (!str) return 'Unassigned';
  return str.split('+').map(function(p) {
    if (p === ' ') return 'Space';
    return p.charAt(0).toUpperCase() + p.slice(1);
  }).join('+');
}
