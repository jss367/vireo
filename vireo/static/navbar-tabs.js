import { vireoSyncNumberedNavigation } from './navbar-command-palette-open.js';
import { _applyNavHotkeyHints } from './navbar-tab-reorder-hotkeys.js';

/* ---------- Unified tab strip + ephemeral slot + overflow ---------- */
// Preserve the photo context when leaving the editor for Browse. Browse's
// existing photo_id deep link owns the folder switch, bounded page load,
// centering, and highlight; navigation surfaces only need to hand it the
// editor photo currently shown in the URL.
/** @type {NavbarApi['vireoResolveNavigationHref']} */ export function vireoResolveNavigationHref(path) {
  if (path !== '/browse') return path;
  var match = window.location.pathname.match(/^\/edit\/(\d+)\/?$/);
  return match ? '/browse?photo_id=' + encodeURIComponent(match[1] || '') : path;
}

// Hardcoded fallbacks so the strip has something to render even when
// /api/workspace/tabs fails (network error, 5xx, invalid JSON). Without
// these, a transient fetch failure on first load leaves the navbar
// empty and breaks tab-based keyboard flows (cmd+1..9, Ctrl+Tab,
// drag/close). Templates are Jinja-free by policy, so we mirror the
// canonical lists here — keep in sync with:
//   - vireo/app.py:ALL_PAGES        (page registry)
//   - vireo/db.py:DEFAULT_TABS      (default pinned-tabs list)
export var NAV_DEFAULT_TABS = [
  'import', 'browse', 'pipeline', 'pipeline_review',
  'review', 'cull', 'jobs',
  'highlights', 'misses', 'storage', 'settings'
];
export var NAV_ALL_PAGES = [
  {id: 'import',          label: 'Import',          href: '/import',
   keywords: 'import add photos card copy ingest new'},
  {id: 'pipeline',        label: 'Process',         href: '/pipeline',
   keywords: 'process classify detect group stages'},
  {id: 'jobs',            label: 'Jobs',            href: '/jobs'},
  {id: 'pipeline_review', label: 'Process Review',  href: '/pipeline/review'},
  {id: 'pipeline_rapid_review', label: 'Rapid Review', href: '/pipeline/rapid-review'},
  {id: 'review',          label: 'Review',          href: '/review'},
  {id: 'cull',            label: 'Cull',            href: '/cull'},
  {id: 'misses',          label: 'Misses',          href: '/misses'},
  {id: 'highlights',      label: 'Highlights',      href: '/highlights'},
  {id: 'life_list',       label: 'Life List',       href: '/life-list'},
  {id: 'inaturalist',    label: 'iNaturalist',    href: '/inaturalist'},
  {id: 'browse',          label: 'Browse',          href: '/browse'},
  {id: 'edit',            label: 'Edit',            href: '/edit'},
  {id: 'map',             label: 'Map',             href: '/map'},
  {id: 'location_review', label: 'Review Photo Locations', href: '/locations/review',
   keywords: 'location review map coordinates collections gps places'},
  {id: 'dashboard',       label: 'Dashboard',       href: '/dashboard'},
  {id: 'storage',         label: 'Storage',         href: '/storage'},
  {id: 'audit',           label: 'Audit',           href: '/audit'},
  {id: 'card_cleanup',    label: 'Card cleanup',    href: '/card-cleanup',
   keywords: 'card cleanup free space delete verified memory card format sd'},
  {id: 'move',            label: 'Move',            href: '/move'},
  {id: 'id_conflicts',    label: 'ID Conflicts',    href: '/id-conflicts',
   keywords: 'compare conflict prediction model disagreement species keyword classify review'},
  {id: 'settings',        label: 'Settings',        href: '/settings'},
  {id: 'workspace',       label: 'Workspace',       href: '/workspace'},
  {id: 'lightroom',       label: 'Lightroom',       href: '/lightroom'},
  {id: 'shortcuts',       label: 'Shortcuts',       href: '/shortcuts'},
  {id: 'keywords',        label: 'Keywords',        href: '/keywords'},
  {id: 'duplicates',      label: 'Duplicates',      href: '/duplicates'},
  {id: 'logs',            label: 'Logs',            href: '/logs'}
];

const STRIP   = () => /** @type {HTMLElement} */ (document.getElementById('navTabStrip'));
const OVERBTN = () => /** @type {HTMLButtonElement} */ (document.getElementById('navOverflowBtn'));
const OVERMENU = () => /** @type {HTMLElement} */ (document.getElementById('navOverflowMenu'));

// Seed from hardcoded fallbacks; the fetch below replaces them with
// the workspace's actual tabs on success, but if the fetch fails the
// defaults remain so the strip is never empty.
let TABS = NAV_DEFAULT_TABS.slice();
let ALL_PAGES = NAV_ALL_PAGES.slice();
/** @type {Record<string,NavPage>} */ let pageById = {};
ALL_PAGES.forEach(p => { pageById[p.id] = p; });

/** @param {string} url @param {Record<string,unknown>} body */
function postJSON(url, body) {
  return fetch(url, {
    method: 'POST',
    headers: {'Content-Type': 'application/json'},
    body: JSON.stringify(body || {}),
  }).then(r => r.json());
}

function currentNavId() {
  const p = window.location.pathname;
  if (p.startsWith('/pipeline/rapid-review')) return 'pipeline_rapid_review';
  if (p.startsWith('/pipeline/review')) return 'pipeline_review';
  if (p.startsWith('/locations/review')) return 'location_review';
  if (p === '/' || p.startsWith('/browse')) return 'browse';
  const seg = (p.split('/')[1] || '').replace(/-/g, '_');
  return seg;
}

/** @param {NavPage} page @param {{ephemeral?: boolean} | undefined} [opts] */
function buildTabAnchor(page, opts) {
  const a = document.createElement('a');
  a.href = vireoResolveNavigationHref(page.href);
  // The editor swaps photos with history.replaceState(), without rebuilding
  // the tab strip. Resolve again at activation time so Prev/Next and search
  // navigation cannot leave Browse pointing at the previous photo.
  a.addEventListener('click', () => {
    a.href = vireoResolveNavigationHref(page.href);
  });
  a.draggable = false;
  a.className = 'nav-tab';
  if (opts && opts.ephemeral) a.classList.add('is-ephemeral');
  a.dataset.navId = page.id;
  a.dataset.testid = 'nav-' + page.id;
  a.appendChild(document.createTextNode(page.label));
  if (page.id === 'jobs') {
    const badge = document.createElement('span');
    badge.className = 'nav-job-badge';
    badge.id = 'navJobBadge';
    a.appendChild(badge);
  }
  if (opts && opts.ephemeral) {
    const pin = document.createElement('span');
    pin.className = 'nav-tab-pin';
    pin.title = 'Pin tab';
    pin.dataset.testid = 'nav-tab-pin-' + page.id;
    pin.textContent = '📌';
    pin.addEventListener('click', (ev) => {
      ev.preventDefault();
      ev.stopPropagation();
      pinTab(page.id);
    });
    a.appendChild(pin);
  }
  const close = document.createElement('span');
  close.className = 'nav-tab-close';
  close.title = 'Close tab';
  close.innerHTML = '&times;';
  close.addEventListener('click', (ev) => {
    ev.preventDefault();
    ev.stopPropagation();
    if (opts && opts.ephemeral) clearEphemeral(); else unpinTab(page.id);
  });
  a.appendChild(close);
  return a;
}

function applyActiveClass() {
  const cur = currentNavId();
  /** @type {NodeListOf<HTMLAnchorElement>} */ (STRIP().querySelectorAll('a.nav-tab')).forEach(a => {
    if (a.dataset.navId === cur) a.classList.add('active');
    else a.classList.remove('active');
  });
}

/** @type {string | null} */ let scrollKey = null;

function saveStripScroll() {
  const strip = STRIP();
  if (scrollKey && strip?.childElementCount) sessionStorage.setItem(scrollKey, String(strip.scrollLeft));
}

// Mark the sides of the strip that have tabs scrolled out of view; the
// stylesheet fades those edges. One pixel of slack absorbs fractional widths.
function updateStripOverflow() {
  const strip = STRIP();
  if (!strip) return;
  const hidden = strip.scrollWidth - strip.clientWidth;
  strip.classList.toggle('more-left', strip.scrollLeft > 1);
  strip.classList.toggle('more-right', hidden - strip.scrollLeft > 1);
}

// A mouse wheel only scrolls vertically, so over an overflowing strip it
// moves the tabs sideways instead. A sideways gesture keeps its own scroll.
// At the boundary in the wheel direction the strip cannot move, so the
// wheel falls through to the page instead of being swallowed in place.
// Firefox and some devices report deltaY in lines or pages, not pixels
// (WheelEvent.deltaMode 1 or 2), so a 1-3 raw delta would advance the
// strip by 1-3 pixels while preventDefault() swallowed the native scroll.
/** @param {WheelEvent} event */
function scrollStripWithWheel(event) {
  const strip = STRIP();
  if (event.ctrlKey || Math.abs(event.deltaY) <= Math.abs(event.deltaX)) return;
  if (strip.scrollWidth <= strip.clientWidth) return;
  const hidden = strip.scrollWidth - strip.clientWidth;
  if (event.deltaY > 0 && hidden - strip.scrollLeft <= 1) return;
  if (event.deltaY < 0 && strip.scrollLeft <= 1) return;
  event.preventDefault();
  const step = event.deltaMode === 1 ? event.deltaY * 16
             : event.deltaMode === 2 ? event.deltaY * strip.clientWidth
             : event.deltaY;
  strip.scrollLeft += step;
}

function watchStripOverflow() {
  const strip = STRIP();
  if (!strip || strip.dataset.overflowWatched) return;
  strip.dataset.overflowWatched = '1';
  strip.addEventListener('scroll', updateStripOverflow, {passive: true});
  strip.addEventListener('wheel', scrollStripWithWheel, {passive: false});
  new ResizeObserver(updateStripOverflow).observe(strip);
}

/** @param {string | null} nextScrollKey */
function renderStrip(nextScrollKey = scrollKey) {
  const strip = STRIP();
  if (!strip) return;
  let scrollLeft = strip.scrollLeft;
  if (nextScrollKey !== scrollKey) {
    saveStripScroll();
    scrollKey = nextScrollKey;
    scrollLeft = scrollKey ? Number(sessionStorage.getItem(scrollKey)) || 0 : 0;
  }
  strip.textContent = '';
  TABS.forEach(id => {
    const page = pageById[id];
    if (page) strip.appendChild(buildTabAnchor(page, {ephemeral: false}));
  });
  // Ephemeral tab: only if currently on an unpinned page
  const cur = currentNavId();
  if (cur && !TABS.includes(cur) && pageById[cur]) {
    strip.appendChild(buildTabAnchor(pageById[cur], {ephemeral: true}));
  }
  strip.scrollLeft = scrollLeft;
  watchStripOverflow();
  updateStripOverflow();
  applyActiveClass();
  rebuildPagesMenu();
  if (vireoSyncNumberedNavigation) vireoSyncNumberedNavigation();
  // Re-apply hotkey hints (underline + (key) suffix) — tabs are
  // built async, so the initial DOM-ready call doesn't see them.
  if (typeof _applyNavHotkeyHints === 'function') {
    _applyNavHotkeyHints();
  }
}

function rebuildPagesMenu() {
  const menu = OVERMENU();
  if (!menu) return;
  menu.textContent = '';
  const cur = currentNavId();
  ALL_PAGES.forEach(page => {
    const item = document.createElement('button');
    item.type = 'button';
    item.className = 'nav-overflow-item';
    if (page.id === cur) item.setAttribute('aria-current', 'page');
    item.appendChild(document.createTextNode(page.label));
    item.addEventListener('click', () => {
      window.location.href = vireoResolveNavigationHref(page.href);
    });
    menu.appendChild(item);
  });
}

/** @type {NavbarApi['toggleOverflowMenu']} */ export function toggleOverflowMenu(ev) {
  if (ev) ev.stopPropagation();
  const menu = OVERMENU();
  const btn = OVERBTN();
  if (!menu || !btn) return;
  menu.hidden = !menu.hidden;
  btn.setAttribute('aria-expanded', String(!menu.hidden));
}

document.addEventListener('click', event => {
  if (!(/** @type {Element} */ (event.target)).closest('.nav-pages')) closePagesMenu();
});

document.addEventListener('keydown', event => {
  if (event.key === 'Escape' && OVERMENU() && !OVERMENU().hidden) {
    event.preventDefault();
    event.stopPropagation();
    closePagesMenu();
    OVERBTN().focus();
  }
}, true);

function closePagesMenu() {
  const menu = OVERMENU();
  if (menu && !menu.hidden) {
    menu.hidden = true;
    OVERBTN().setAttribute('aria-expanded', 'false');
  }
}

/** @param {string} navId */
function adjacentTabId(navId) {
  const idx = TABS.indexOf(navId);
  if (idx === -1) return null;
  if (idx + 1 < TABS.length) return TABS[idx + 1];
  if (idx - 1 >= 0) return TABS[idx - 1];
  return null;
}

/** @param {string} navId */
function unpinTab(navId) {
  const isOnTab = currentNavId() === navId;
  const nextId = isOnTab ? adjacentTabId(navId) : null;
  postJSON('/api/workspace/tabs/unpin', {nav_id: navId}).then(() => {
    if (isOnTab) {
      window.location.href = vireoResolveNavigationHref(
        (nextId && pageById[nextId]?.href) || '/browse'
      );
    } else {
      return fetchAndRender();
    }
  });
}

function clearEphemeral() {
  // The temporary tab follows the last pinned tab in the scrollable strip.
  const adjId = TABS[TABS.length - 1];
  const next = adjId ? pageById[adjId] : null;
  window.location.href = vireoResolveNavigationHref(
    next ? next.href : '/browse'
  );
}

/** @type {NavbarApi['pinTab']} */ function pinTab(navId) {
  return postJSON('/api/workspace/tabs/pin', {nav_id: navId})
    .then(() => fetchAndRender());
}

function fetchAndRender() {
  return fetch('/api/workspace/tabs')
    .then(r => r.json())
    .then((/** @type {NavTabState} */ state) => {
      TABS = (state && state.tabs) || [];
      ALL_PAGES = (state && state.all_pages) || [];
      pageById = {};
      ALL_PAGES.forEach(p => { pageById[p.id] = p; });
      renderStrip(state?.workspace_id == null ? null : 'vireo_nav_scroll_' + state.workspace_id);
      return state;
    })
    .catch(err => {
      // Preserve previously-seeded state (server-rendered defaults on
      // first load, or the last successful fetch afterward) so the
      // strip still renders. Without this, a transient failure makes
      // the navbar disappear and breaks cmd+1..9 / Ctrl+Tab.
      console.warn('[nav] /api/workspace/tabs failed; keeping current state', err);
      renderStrip();
    });
}

window.addEventListener('pagehide', saveStripScroll);

// Expose for the palette + drag-reorder code in the next scripts
/** @type {NavTabs} */
export var _navTabs = {
  fetchAndRender,
  getTabs: () => TABS.slice(),
  getAllPages: () => ALL_PAGES.slice(),
  setTabs: (newOrder) => {
    return postJSON('/api/workspace/tabs/reorder', {tabs: newOrder})
      .then(() => fetchAndRender());
  },
};

export function vireoRefreshNavigationHrefs() {
  const browse = pageById.browse;
  if (!browse) return;
  /** @type {NodeListOf<HTMLAnchorElement>} */ (document.querySelectorAll('.nav-tab[data-nav-id="browse"]')).forEach(a => {
    a.href = vireoResolveNavigationHref(browse.href);
  });
}

// The tab strip only needs the navbar DOM above, not the full page. Start it
// immediately so pages with slow blocking scripts (notably /map's Leaflet
// CDN scripts) still show an ephemeral tab that can be pinned.
if (STRIP()) {
  fetchAndRender();
} else if (document.readyState === 'loading') {
  document.addEventListener('DOMContentLoaded', fetchAndRender);
} else {
  fetchAndRender();
}
