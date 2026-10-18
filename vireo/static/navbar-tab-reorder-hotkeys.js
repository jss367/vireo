import { navbarState } from './navbar-state.js';
import { vireoSetNumberedNavigationMode } from './navbar-command-palette-open.js';
import { _navTabs, vireoResolveNavigationHref } from './navbar-tabs.js';
import { formatShortcut, parseShortcut } from './lightbox/keyboard.js';

/* ---------- Pointer drag-reorder over the unified strip ---------- */
/** @type {number | null} */ var _sheetEscToken = null;
/** @type {(() => void) | null} */ export var _applyNavHotkeyHints = null;

function strip() { return /** @type {HTMLElement} */ (document.getElementById('navTabStrip')); }
function tabAnchors() {
  const s = strip();
  return s ? Array.from(/** @type {NodeListOf<HTMLAnchorElement>} */ (s.querySelectorAll('.nav-tab:not(.is-ephemeral)'))) : [];
}

/** @type {HTMLAnchorElement | null} */ let draggedEl = null;
/** @type {HTMLAnchorElement | null} */ let pressedEl = null;
/** @type {number | null} */ let pointerId = null;
let startX = 0;
let startY = 0;
/** @type {HTMLElement | null} */ let indicator = null;
let suppressClick = false;
const DRAG_THRESHOLD = 4;

function createIndicator() {
  const el = document.createElement('span');
  el.className = 'nav-drop-indicator';
  return el;
}
function removeIndicator() {
  if (indicator && indicator.parentNode) indicator.parentNode.removeChild(indicator);
  indicator = null;
}
function visibleDropAnchors() {
  return tabAnchors().filter(a => a !== draggedEl && a.style.display !== 'none');
}
/** @param {number} clientX */
function dropReference(clientX) {
  const anchors = visibleDropAnchors();
  if (!anchors.length) return null;
  for (const anchor of anchors) {
    const rect = anchor.getBoundingClientRect();
    if (clientX < rect.left + rect.width / 2) return anchor;
  }
  return anchors[anchors.length - 1]?.nextSibling || null;
}
/** @param {number} clientX */
function showDropIndicator(clientX) {
  const s = strip();
  if (!s) return;
  removeIndicator();
  indicator = createIndicator();
  s.insertBefore(indicator, dropReference(clientX));
}
function persistCurrentOrder() {
  const newOrder = tabAnchors().map(a => a.dataset.navId).filter((id) => typeof id === 'string');
  if (_navTabs) _navTabs.setTabs(newOrder);
}
/** @param {number} clientX @param {number} clientY */
function pointInsideStrip(clientX, clientY) {
  const s = strip();
  if (!s) return false;
  const rect = s.getBoundingClientRect();
  return clientX >= rect.left && clientX <= rect.right &&
    clientY >= rect.top && clientY <= rect.bottom;
}
/** @param {number} clientX */
function commitReorder(clientX) {
  const s = strip();
  if (!s || !draggedEl) return;
  removeIndicator();
  s.insertBefore(draggedEl, dropReference(clientX));
  persistCurrentOrder();
}
function finishDrag() {
  if (draggedEl) draggedEl.classList.remove('dragging');
  removeIndicator();
  draggedEl = null;
  pressedEl = null;
  pointerId = null;
}

function init() {
  const s = strip();
  if (!s) return;
  // Native HTML drag-and-drop is unreliable in macOS WKWebView: depending
  // on the release position it can omit `drop`, `dragover`, or report a
  // spurious `dragleave`. Pointer capture gives us one dependable stream
  // through release, including when the pointer leaves the navbar.
  s.addEventListener('pointerdown', function(e) {
    if (e.button !== 0 || !e.isPrimary) return;
    if (/** @type {HTMLElement} */ (e.target).closest('.nav-tab-close, .nav-tab-pin')) return;
    const link = /** @type {HTMLAnchorElement | null} */ (/** @type {HTMLElement} */ (e.target).closest('.nav-tab:not(.is-ephemeral)'));
    if (!link || !s.contains(link)) return;
    pressedEl = link;
    pointerId = e.pointerId;
    startX = e.clientX;
    startY = e.clientY;
    if (link.setPointerCapture) link.setPointerCapture(e.pointerId);
  });

  document.addEventListener('pointermove', function(e) {
    if (!pressedEl || e.pointerId !== pointerId) return;
    if (!draggedEl && Math.hypot(e.clientX - startX, e.clientY - startY) < DRAG_THRESHOLD) return;
    if (!draggedEl) {
      draggedEl = pressedEl;
      draggedEl.classList.add('dragging');
    }
    e.preventDefault();
    if (pointInsideStrip(e.clientX, e.clientY)) showDropIndicator(e.clientX);
    else removeIndicator();
  });

  document.addEventListener('pointerup', function(e) {
    if (!pressedEl || e.pointerId !== pointerId) return;
    if (!draggedEl) {
      finishDrag();
      return;
    }
    e.preventDefault();
    e.stopPropagation();
    suppressClick = true;
    if (pointInsideStrip(e.clientX, e.clientY)) commitReorder(e.clientX);
    finishDrag();
    setTimeout(function() { suppressClick = false; }, 0);
  });

  document.addEventListener('pointercancel', finishDrag);
  s.addEventListener('click', function(e) {
    if (!suppressClick) return;
    e.preventDefault();
    e.stopPropagation();
  }, true);
}

// Module scripts always run before DOMContentLoaded.
document.addEventListener('DOMContentLoaded', init);

/* ---------- Navigation Hotkeys ---------- */
// Map from navigation shortcut key → href path
/** @type {Record<string,string>} */ var NAV_ROUTES = {
  pipeline: '/pipeline', lightroom: '/lightroom', pipeline_review: '/pipeline/review',
  review: '/review', cull: '/cull', browse: '/browse', map: '/map',
  dashboard: '/dashboard', storage: '/storage', audit: '/audit',
  id_conflicts: '/id-conflicts', workspace: '/workspace', shortcuts: '/shortcuts',
  settings: '/settings', keywords: '/keywords'
};

/** @type {Record<string,string>} */ var NAV_DEFAULTS = {
  pipeline: '', lightroom: '', pipeline_review: '', review: '',
  cull: '', browse: '', map: '',
  dashboard: '', storage: '', audit: '', id_conflicts: '', workspace: '',
  shortcuts: '', settings: '', keywords: ''
};

/** @param {string | undefined} keyStr */
function isBareShortcut(keyStr) {
  if (!keyStr) return false;
  var sc = parseShortcut(keyStr);
  return !sc.ctrl && !sc.meta && !sc.shift && !sc.alt;
}

/** @param {string | undefined} keyStr @param {Record<string,Record<string,string>>} shortcuts */
function isBarePageShortcut(keyStr, shortcuts) {
  if (!isBareShortcut(keyStr)) return false;
  var key = parseShortcut(keyStr || '').key;

  // Bare navigation shortcuts are intentionally limited to unused letters.
  // Non-letter keys retain their browser or page behavior unless combined
  // with a modifier.
  if (!/^[a-z]$/.test(key)) return true;

  // Keep default and fixed page-local letters available in their owning views.
  var reservedDefaults = [
    'a', 'b', 'c', 'e', 'f', 'g', 'h', 'j', 'k', 'p', 's', 'u', 'x', 'z'
  ];
  if (reservedDefaults.indexOf(key) !== -1) {
    return true;
  }

  // Reserve bare keys that are assigned to configurable page actions. Bare
  // letters unused by a page action remain available for navigation.
  for (var ctx in shortcuts) {
    if (ctx === 'navigation') continue;
    var pageShortcuts = shortcuts[ctx] || {};
    for (var action in pageShortcuts) {
      var shortcut = pageShortcuts[action];
      if (isBareShortcut(shortcut) && parseShortcut(shortcut || '').key === key) return true;
    }
  }
  return false;
}

/** @param {string | undefined} keyStr @param {Record<string,Record<string,string>>} shortcuts */
function navigationShortcutOrEmpty(keyStr, shortcuts) {
  return isBarePageShortcut(keyStr, shortcuts) ? '' : (keyStr || '');
}

// Build reverse map: href → shortcut action name
/** @type {Record<string,string>} */ var hrefToAction = {};
for (var action in NAV_ROUTES) hrefToAction[NAV_ROUTES[action] || ''] = action;

// Replace the leading label text node of a dynamic .nav-tab anchor
// with a label fragment, preserving sibling elements (badge, close
// button, etc.). Returns true if a label node was found+replaced.
/** @param {HTMLElement} link @param {DocumentFragment} fragment */
function replaceTabLabel(link, fragment) {
  // Remove any existing .hk / .hk-suffix from a previous run
  var hk = /** @type {HTMLElement | null} */ (link.querySelector('.hk'));
  if (hk && hk.parentNode === link) hk.remove();
  var hkSuf = /** @type {HTMLElement | null} */ (link.querySelector('.hk-suffix'));
  if (hkSuf && hkSuf.parentNode === link) hkSuf.remove();
  // Find the leading text node and replace it.
  for (const n of link.childNodes) {
    if (n.nodeType === 3 /* Node.TEXT_NODE */ && (n.textContent || '').trim().length > 0) {
      link.replaceChild(fragment, n);
      return true;
    }
  }
  return false;
}

/** @param {Record<string,string>} navShortcuts */
function applyHotkeyHints(navShortcuts) {
  var navLinks = /** @type {NodeListOf<HTMLAnchorElement>} */ (document.querySelectorAll('.navbar a[href]:not(.brand):not(.nav-icon):not([data-tab])'));
  for (const link of navLinks) {
    var href = link.getAttribute('href');
    var action = hrefToAction[href || ''];
    if (!action) continue;
    var keyStr = navShortcuts[action];
    if (!keyStr) continue;

    var isDynamicTab = link.classList.contains('nav-tab');
    var label;
    if (isDynamicTab) {
      // Dynamic tabs have label as a leading text node; siblings are
      // badge / close-button spans we must preserve.
      var labelNode = null;
      for (const cn of link.childNodes) {
        if (cn.nodeType === 3 && (cn.textContent || '').trim().length > 0) {
          labelNode = cn; break;
        }
      }
      label = labelNode ? (labelNode.textContent || '').trim() : (link.textContent || '').trim();
    } else {
      label = (link.textContent || '').trim();
    }

    var sc = parseShortcut(keyStr);
    // Only do inline highlight for single bare keys (no modifiers)
    if (!sc.ctrl && !sc.meta && !sc.shift && !sc.alt && sc.key.length === 1) {
      var idx = label.toLowerCase().indexOf(sc.key.toLowerCase());
      if (idx !== -1) {
        if (isDynamicTab) {
          // Build a DocumentFragment so we don't clobber siblings
          var frag = document.createDocumentFragment();
          if (idx > 0) frag.appendChild(document.createTextNode(label.substring(0, idx)));
          var hkSpan = document.createElement('span');
          hkSpan.className = 'hk';
          hkSpan.textContent = label.charAt(idx);
          frag.appendChild(hkSpan);
          if (idx + 1 < label.length) {
            frag.appendChild(document.createTextNode(label.substring(idx + 1)));
          }
          replaceTabLabel(link, frag);
        } else {
          // Legacy linger anchors: safe to replace innerHTML.
          link.innerHTML = escapeHtml(label.substring(0, idx)) +
            '<span class="hk">' + escapeHtml(label.charAt(idx)) + '</span>' +
            escapeHtml(label.substring(idx + 1));
        }
        continue;
      }
    }
    // Suffix: append (key) after label
    if (isDynamicTab) {
      var frag2 = document.createDocumentFragment();
      frag2.appendChild(document.createTextNode(label + ' '));
      var sufSpan = document.createElement('span');
      sufSpan.className = 'hk-suffix';
      sufSpan.textContent = '(' + formatShortcut(keyStr) + ')';
      frag2.appendChild(sufSpan);
      replaceTabLabel(link, frag2);
    } else {
      link.innerHTML = escapeHtml(label) + ' <span class="hk-suffix">(' +
        escapeHtml(formatShortcut(keyStr)) + ')</span>';
    }
  }
}

// CSS selector for any open overlay/modal. The legacy dispatcher used this
// to bail out before navigating; the new Keymap dispatcher has no such
// built-in suppression, so each registered nav action checks it itself
// (Option B from the migration review). Without this, pressing a configured
// navigation chord while the lightbox is open would close it AND navigate.
export var OVERLAY_SELECTOR = '.lightbox-overlay.active, .pipeline-overlay.active, .similar-overlay.active, .modal-overlay.open, .grm-overlay.open, .inspect-overlay.open, .shortcuts-overlay.open, .help-overlay.active, .report-overlay.active';

/** @param {Record<string,string>} navShortcuts */
function setupNavKeydown(navShortcuts) {
  if (!window.Keymap || !window.Keymap.register) return;

  Object.keys(NAV_ROUTES).forEach(function (action) {
    var key = navShortcuts[action];
    if (!key) return;
    var route = NAV_ROUTES[action];
    if (!route) return;
    window.Keymap.register('global', {
      key: key,
      name: action,
      description: 'Go to ' + action.replace(/_/g, ' '),
      category: 'Navigation',
      action: function () {
        // Suppress while any overlay/modal is open — preserves the legacy
        // dispatcher's overlay-aware behavior (see OVERLAY_SELECTOR comment).
        // Return false so the dispatcher does NOT preventDefault and another
        // candidate (or the browser) can still handle the key.
        if (document.querySelector(OVERLAY_SELECTOR)) return false;
        // Page-shadows-global precedence for legacy _vireoShortcuts bindings:
        // browse/review page handlers still listen on document directly (they
        // migrate into the registry in PR 4). If the current page has bound
        // this key to one of its own actions, yield so the page's bubble-phase
        // listener can handle it. Without this, a user who remaps e.g. browse
        // 'flag' to 'b' would navigate away instead of flagging.
        if (_pageCtx && navbarState.shortcuts && navbarState.shortcuts[_pageCtx]) {
          var pageShortcuts = navbarState.shortcuts[_pageCtx];
          for (var a in pageShortcuts) {
            if (pageShortcuts[a] === key) return false;
          }
        }
        // Don't navigate if we're already on this page.
        if (window.location.pathname === route) return false;
        window.location.href = vireoResolveNavigationHref(route || '/browse');
      }
    });
  });
}

// Determine page scope and tell the Keymap dispatcher synchronously, before
// any /api/config fetch resolves. Otherwise page-scoped shortcuts registered
// by other scripts would race the fetch and dispatch on the wrong scope.
var _path = location.pathname;
/** @type {string | null} */ var _pageCtx = null;
if (_path === '/browse') _pageCtx = 'browse';
else if (_path === '/review') _pageCtx = 'review';
else if (_path.startsWith('/pipeline/rapid-review')) _pageCtx = 'pipeline_rapid_review';
if (_pageCtx && window.Keymap && window.Keymap.setScope) {
  window.Keymap.setScope(_pageCtx);
}

// Apply the config once every script has run, the page's included (a page
// script may set the shortcuts first). Module scripts always run before
// DOMContentLoaded, so this waits for it.
/** @type {Promise<void>} */ var scriptsLoaded = new Promise(function(resolve) {
  document.addEventListener('DOMContentLoaded', () => resolve(), { once: true });
});

// Fetch config and apply
fetch('/api/config')
  .then(function(r) { return r.ok ? r.json() : null; })
  .then(function(cfg) { return scriptsLoaded.then(function() { return cfg; }); })
  .then(function(/** @type {{numbered_navigation_mode: string; keyboard_shortcuts?: Record<string,Record<string,string>>} | null} */ cfg) {
    if (!cfg) return;
    vireoSetNumberedNavigationMode(cfg.numbered_navigation_mode);
    var shortcuts = cfg.keyboard_shortcuts || {};
    /** @type {Record<string,string>} */ var navShortcuts = {};
    // Merge defaults with user overrides
    for (var key in NAV_DEFAULTS) navShortcuts[key] = NAV_DEFAULTS[key] || '';
    if (shortcuts.navigation) {
      for (key in shortcuts.navigation) navShortcuts[key] = shortcuts.navigation[key] || '';
    }
    for (key in navShortcuts) {
      navShortcuts[key] = navigationShortcutOrEmpty(navShortcuts[key], shortcuts);
    }
    // Store for other scripts to access
    navbarState.shortcuts = navbarState.shortcuts || shortcuts;
    navbarState.shortcuts.navigation = navShortcuts;

    // Expose so renderStrip() (in another IIFE) can re-apply hints
    // after dynamic tab anchors are inserted.
    _applyNavHotkeyHints = function() { applyHotkeyHints(navShortcuts); };
    applyHotkeyHints(navShortcuts);
    setupNavKeydown(navShortcuts);
  })
  .catch(function() {});

/* ---------- Shortcuts Cheat Sheet (?) ---------- */
/** @type {Record<string,Record<string,string>>} */ var SC_LABELS = {
  global: {
    _title: 'Global',
    open_shortcuts: 'Open keyboard shortcuts',
    command_palette: 'Open command palette',
    next_tab: 'Next tab',
    previous_tab: 'Previous tab',
    close_tab: 'Close current tab',
    help: 'Open help',
  },
  navigation: {
    _title: 'Navigation',
    import: 'Import', pipeline: 'Process', pipeline_review: 'Process Review',
    review: 'Review', cull: 'Cull', browse: 'Browse', map: 'Map',
    dashboard: 'Dashboard', storage: 'Storage', audit: 'Audit',
    id_conflicts: 'ID Conflicts', workspace: 'Workspace', keywords: 'Keywords',
    shortcuts: 'Shortcuts', settings: 'Settings',
  },
  review: {
    _title: 'Review',
    accept: 'Accept prediction', skip: 'Skip / reject',
  },
  pipeline_rapid_review: {
    _title: 'Rapid Review',
    pick: 'Pick', reject: 'Reject', next: 'Next / review',
    back: 'Back / undo', clear: 'Clear to review',
    apply: 'Apply', exit: 'Exit', zoom: 'Toggle zoom',
  },
  browse: {
    _title: 'Browse',
    rate_0: 'Clear rating', rate_1: 'Rate 1', rate_2: 'Rate 2',
    rate_3: 'Rate 3', rate_4: 'Rate 4', rate_5: 'Rate 5',
    flag: 'Flag', reject: 'Reject', unflag: 'Unflag',
    undo: 'Undo', redo: 'Redo', select_all: 'Select all', compare: 'Compare',
    zoom: 'Toggle zoom',
    toggle_boxes: 'Toggle detection boxes',
  },
  misses: {
    _title: 'Misses',
    next: 'Next miss',
    previous: 'Previous miss',
    extend_next: 'Extend selection to next miss',
    extend_previous: 'Extend selection to previous miss',
    flag: 'Flag focused or selected photo',
    reject: 'Reject focused or selected photo',
    unmark_missed: 'Unmark as missed',
    open: 'Open focused photo',
    clear_selection: 'Clear selection',
  },
  lightbox: {
    _title: 'Lightbox',
    next: 'Next photo',
    previous: 'Previous photo',
    flag: 'Flag current photo',
    reject: 'Reject current photo',
    unflag: 'Clear pick/reject flag',
    zoom: 'Toggle 1:1 zoom',
    zoom_in: 'Zoom in',
    zoom_out: 'Zoom out',
    fit: 'Fit to screen',
    toggle_boxes: 'Toggle detection boxes',
    toggle_ui: 'Toggle UI controls',
    fullscreen: 'Enter fullscreen',
    close_return: 'Close and return',
  },
};

/** @type {NavShortcutDefaults} */ var SC_DEFAULTS = {
  global: {
    open_shortcuts: '?',
    command_palette: 'ctrl+k',
    next_tab: 'ctrl+tab',
    previous_tab: 'ctrl+shift+tab',
    close_tab: 'ctrl+w',
    help: 'f1',
  },
  navigation: {
    import: '', pipeline: '', pipeline_review: '', review: '',
    cull: '', browse: '', map: '',
    dashboard: '', storage: '', audit: '', id_conflicts: '', workspace: '',
    shortcuts: '', settings: '', keywords: '',
  },
  review: { accept: 'a', skip: 's' },
  pipeline_rapid_review: {
    pick: 'p', reject: 'x', next: 'arrowright', back: 'arrowleft',
    clear: 'u', apply: 'enter', exit: 'escape', zoom: 'z',
  },
  browse: {
    rate_0: '0', rate_1: '1', rate_2: '2', rate_3: '3',
    rate_4: '4', rate_5: '5',
    flag: 'p', reject: 'x', unflag: 'u',
    undo: 'ctrl+z', redo: 'ctrl+shift+z', select_all: 'ctrl+a', compare: 'c', zoom: 'z',
    toggle_boxes: 'b',
    toggle_ui: 'h',
  },
  misses: {
    next: 'j',
    previous: 'k',
    extend_next: 'shift+j',
    extend_previous: 'shift+k',
    flag: 'p',
    reject: 'x',
    unmark_missed: 'u',
    open: 'enter',
    clear_selection: 'escape',
  },
  lightbox: {
    next: 'arrowright',
    previous: 'arrowleft',
    flag: 'p',
    reject: 'x',
    unflag: 'u',
    zoom: 'z',
    zoom_in: '+',
    zoom_out: '-',
    fit: '0',
    toggle_boxes: 'b',
    toggle_ui: 'h',
    fullscreen: 'f',
    close_return: 'g',
  },
};

/** @param {Record<string,Record<string,string>>} shortcuts @param {string} ctx @param {string} action */
function sheetShortcutValue(shortcuts, ctx, action) {
  // Misses and Lightbox intentionally reuse configurable Browse bindings
  // for photo flag actions. Show the effective key and contextual label
  // here without making those fixed page behaviors editable separately.
  if (ctx === 'misses') {
    var browse = (shortcuts && shortcuts.browse) || {};
    var browseDefaults = SC_DEFAULTS.browse || {};
    if (action === 'flag') return browse.flag || browseDefaults.flag || SC_DEFAULTS.misses.flag;
    if (action === 'reject') return browse.reject || browseDefaults.reject || SC_DEFAULTS.misses.reject;
    if (action === 'unmark_missed') return browse.unflag || browseDefaults.unflag || SC_DEFAULTS.misses.unmark_missed;
  }
  if (ctx === 'lightbox') {
    var browse2 = (shortcuts && shortcuts.browse) || {};
    var browseDefaults2 = SC_DEFAULTS.browse || {};
    if (action === 'flag') return browse2.flag || browseDefaults2.flag || SC_DEFAULTS.lightbox.flag;
    if (action === 'reject') return browse2.reject || browseDefaults2.reject || SC_DEFAULTS.lightbox.reject;
    if (action === 'unflag') return browse2.unflag || browseDefaults2.unflag || SC_DEFAULTS.lightbox.unflag;
    if (action === 'zoom') return browse2.zoom || browseDefaults2.zoom || SC_DEFAULTS.lightbox.zoom;
    if (action === 'toggle_boxes') return browse2.toggle_boxes || browseDefaults2.toggle_boxes || SC_DEFAULTS.lightbox.toggle_boxes;
    if (action === 'toggle_ui') {
      // Presence-checked so an explicit '' (migrated on install to resolve
      // a pre-existing 'h' binding conflict) reads as Unassigned instead
      // of silently falling through to the 'h' default.
      if ('toggle_ui' in browse2) return browse2.toggle_ui;
      if ('toggle_ui' in browseDefaults2) return browseDefaults2.toggle_ui;
      return SC_DEFAULTS.lightbox.toggle_ui;
    }
  }
  var values = (shortcuts && shortcuts[ctx]) || {};
  var defaults = SC_DEFAULTS[ctx] || {};
  return values[action] || defaults[action] || '';
}

function renderSheet() {
  var shortcuts = navbarState.shortcuts || {};
  var html = '';
  for (var ctx in SC_LABELS) {
    var labels = SC_LABELS[ctx];
    if (!labels) continue;
    html += '<div class="sc-group-title">' + labels._title + '</div>';
    for (var action in labels) {
      if (action === '_title') continue;
      var key = sheetShortcutValue(shortcuts, ctx, action);
      html += '<div class="sc-row">' +
        '<span class="sc-key">' + formatShortcut(key || '') + '</span>' +
        '<span class="sc-label">' + labels[action] + '</span>' +
        '</div>';
    }
  }
  /** @type {HTMLElement} */ (document.getElementById('shortcutsSheetContent')).innerHTML = html;
}

export function openShortcutsSheet() {
  var alreadyOpen = !!_sheetEscToken;
  if (alreadyOpen) Keymap.popEsc(_sheetEscToken ?? -1);
  _sheetEscToken = Keymap.pushEsc(function() { closeShortcutsSheet(); });
  renderSheet();
  /** @type {HTMLElement} */ (document.getElementById('shortcutsCheatSheet')).classList.add('open');
  if (!alreadyOpen) Keymap.lockBodyScroll();
}

export function closeShortcutsSheet() {
  var wasOpen = !!_sheetEscToken;
  if (wasOpen) { Keymap.popEsc(_sheetEscToken ?? -1); _sheetEscToken = null; }
  /** @type {HTMLElement} */ (document.getElementById('shortcutsCheatSheet')).classList.remove('open');
  if (wasOpen) Keymap.unlockBodyScroll();
}

// The '?' shortcuts sheet. Bound by navbar-boot.js after the command palette's
// keys and before the lightbox's, the classic scripts' order.
export function bindShortcutsSheetKeys() {
  // Listen for ? key. Esc is owned by the Keymap.pushEsc stack — this listener
  // only opens the sheet on '?' and consumes non-Esc keys while the sheet is
  // open so page handlers (rating, navigation, etc.) don't also fire.
  document.addEventListener('keydown', function(e) {
    if (/** @type {HTMLElement} */ (e.target).tagName === 'INPUT' || /** @type {HTMLElement} */ (e.target).tagName === 'TEXTAREA' || /** @type {HTMLElement} */ (e.target).tagName === 'SELECT') return;
    if (/** @type {HTMLElement | null} */ (document.querySelector('.pipeline-overlay.active, .similar-overlay.active, .modal-overlay.open, .grm-overlay.open, .inspect-overlay.open, .help-overlay.active'))) return;
    var lightboxOverlay = /** @type {HTMLElement} */ (document.getElementById('lightboxOverlay'));
    if (lightboxOverlay && (document.fullscreenElement === lightboxOverlay || document.webkitFullscreenElement === lightboxOverlay)) return;
    // When sheet is open, consume non-Esc keys so page handlers don't fire.
    // Esc is handled by the Keymap.pushEsc stack and must reach the dispatcher.
    if (/** @type {HTMLElement} */ (document.getElementById('shortcutsCheatSheet')).classList.contains('open')) {
      if (e.key === 'Escape') return;
      e.preventDefault();
      e.stopImmediatePropagation();
      return;
    }
    if (e.key === '?' && !e.ctrlKey && !e.metaKey && !e.altKey) {
      e.preventDefault();
      openShortcutsSheet();
    }
  });
}

