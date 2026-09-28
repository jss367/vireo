/* ---------- Unified tab strip + ephemeral slot + overflow ---------- */
// Preserve the photo context when leaving the editor for Browse. Browse's
// existing photo_id deep link owns the folder switch, bounded page load,
// centering, and highlight; navigation surfaces only need to hand it the
// editor photo currently shown in the URL.
window.vireoResolveNavigationHref = function(path) {
  if (path !== '/browse') return path;
  var match = window.location.pathname.match(/^\/edit\/(\d+)\/?$/);
  return match ? '/browse?photo_id=' + encodeURIComponent(match[1]) : path;
};

// Hardcoded fallbacks so the strip has something to render even when
// /api/workspace/tabs fails (network error, 5xx, invalid JSON). Without
// these, a transient fetch failure on first load leaves the navbar
// empty and breaks tab-based keyboard flows (cmd+1..9, Ctrl+Tab,
// drag/close). Templates are Jinja-free by policy, so we mirror the
// canonical lists here — keep in sync with:
//   - vireo/app.py:ALL_PAGES        (page registry)
//   - vireo/db.py:DEFAULT_TABS      (default pinned-tabs list)
window.NAV_DEFAULT_TABS = [
  'import', 'browse', 'pipeline', 'pipeline_review',
  'review', 'cull', 'jobs',
  'highlights', 'misses', 'storage', 'settings'
];
window.NAV_ALL_PAGES = [
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

(function() {
  const STRIP   = () => document.getElementById('navTabStrip');
  const OVERBTN = () => document.getElementById('navOverflowBtn');
  const OVERMENU = () => document.getElementById('navOverflowMenu');

  // Seed from hardcoded fallbacks; the fetch below replaces them with
  // the workspace's actual tabs on success, but if the fetch fails the
  // defaults remain so the strip is never empty.
  let TABS = window.NAV_DEFAULT_TABS.slice();
  let ALL_PAGES = window.NAV_ALL_PAGES.slice();
  let pageById = {};
  ALL_PAGES.forEach(p => { pageById[p.id] = p; });
  let ephemeralId = null; // nav-id of the ephemeral tab, if any

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

  function buildTabAnchor(page, opts) {
    const a = document.createElement('a');
    a.href = window.vireoResolveNavigationHref(page.href);
    // The editor swaps photos with history.replaceState(), without rebuilding
    // the tab strip. Resolve again at activation time so Prev/Next and search
    // navigation cannot leave Browse pointing at the previous photo.
    a.addEventListener('click', () => {
      a.href = window.vireoResolveNavigationHref(page.href);
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
        window.pinTab(page.id);
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
    STRIP().querySelectorAll('.nav-tab').forEach(a => {
      if (a.dataset.navId === cur) a.classList.add('active');
      else a.classList.remove('active');
    });
  }

  function renderStrip() {
    const strip = STRIP();
    if (!strip) return;
    strip.textContent = '';
    TABS.forEach(id => {
      const page = pageById[id];
      if (page) strip.appendChild(buildTabAnchor(page, {ephemeral: false}));
    });
    // Ephemeral tab: only if currently on an unpinned page
    const cur = currentNavId();
    if (cur && !TABS.includes(cur) && pageById[cur]) {
      ephemeralId = cur;
      strip.appendChild(buildTabAnchor(pageById[cur], {ephemeral: true}));
    } else {
      ephemeralId = null;
    }
    applyActiveClass();
    recomputeOverflow();
    // Re-apply hotkey hints (underline + (key) suffix) — tabs are
    // built async, so the initial DOM-ready call doesn't see them.
    if (typeof window._applyNavHotkeyHints === 'function') {
      window._applyNavHotkeyHints();
    }
  }

  function recomputeOverflow() {
    const strip = STRIP();
    const overBtn = OVERBTN();
    if (!strip || !overBtn) return;
    // Reset: show all tabs
    const tabs = Array.from(strip.querySelectorAll('.nav-tab'));
    tabs.forEach(t => { t.style.display = ''; });
    overBtn.hidden = true;
    // Detect overflow against parent (the navbar). The strip is a flex
    // child with min-width:0 + overflow:hidden — its scrollWidth >
    // clientWidth means tabs are clipped.
    if (strip.scrollWidth <= strip.clientWidth) return;
    // Hide tabs from the right end until the strip fits, but never hide
    // the active tab or the ephemeral tab.
    overBtn.hidden = false;
    const cur = currentNavId();
    const protectedIds = new Set([cur, ephemeralId].filter(Boolean));
    // A newly pinned tab is appended to the end of TABS; keep it visible
    // instead of immediately moving it into overflow on narrower navbars.
    if (TABS.length) protectedIds.add(TABS[TABS.length - 1]);
    // Walk right-to-left, hiding pinned tabs that aren't protected.
    for (let i = tabs.length - 1; i >= 0; i--) {
      if (strip.scrollWidth <= strip.clientWidth) break;
      const t = tabs[i];
      if (protectedIds.has(t.dataset.navId)) continue;
      t.style.display = 'none';
    }
    rebuildOverflowMenu(tabs);
  }

  function rebuildOverflowMenu(tabs) {
    const menu = OVERMENU();
    if (!menu) return;
    menu.textContent = '';
    tabs.forEach(t => {
      if (t.style.display !== 'none') return;
      const page = pageById[t.dataset.navId];
      if (!page) return;
      const item = document.createElement('button');
      item.type = 'button';
      item.className = 'nav-overflow-item';
      item.dataset.navId = page.id;
      item.appendChild(document.createTextNode(page.label));
      item.addEventListener('click', () => {
        window.location.href = window.vireoResolveNavigationHref(page.href);
      });
      menu.appendChild(item);
    });
  }

  window.toggleOverflowMenu = function(ev) {
    if (ev) ev.stopPropagation();
    const menu = OVERMENU();
    const btn = OVERBTN();
    if (!menu || !btn) return;
    if (menu.hasAttribute('hidden')) {
      menu.removeAttribute('hidden');
      const r = btn.getBoundingClientRect();
      menu.style.left = r.left + 'px';
      setTimeout(() => {
        document.addEventListener('click', closeOverflowMenuOnOutside, {once: true});
      }, 0);
    } else {
      menu.setAttribute('hidden', '');
    }
  };

  function closeOverflowMenuOnOutside(e) {
    const menu = OVERMENU();
    if (!menu) return;
    if (e.target.closest('.nav-overflow-menu, .nav-overflow-btn')) {
      document.addEventListener('click', closeOverflowMenuOnOutside, {once: true});
      return;
    }
    menu.setAttribute('hidden', '');
  }

  function adjacentTabId(navId) {
    const idx = TABS.indexOf(navId);
    if (idx === -1) return null;
    if (idx + 1 < TABS.length) return TABS[idx + 1];
    if (idx - 1 >= 0) return TABS[idx - 1];
    return null;
  }

  function unpinTab(navId) {
    const isOnTab = currentNavId() === navId;
    const nextId = isOnTab ? adjacentTabId(navId) : null;
    postJSON('/api/workspace/tabs/unpin', {nav_id: navId}).then(() => {
      if (isOnTab) {
        window.location.href = window.vireoResolveNavigationHref(
          nextId ? pageById[nextId].href : '/browse'
        );
      } else {
        return fetchAndRender();
      }
    });
  }

  function clearEphemeral() {
    // Closing ephemeral === navigate away from the unpinned page.
    // Ephemeral renders at the right end of the strip, so its visually
    // adjacent neighbor is the rightmost *visible* pinned tab. Under
    // overflow, recomputeOverflow() hides rightmost pinned tabs first,
    // so TABS[length-1] may be hidden; walk right-to-left to find the
    // first one that's actually rendered.
    const strip = STRIP();
    let adjId = null;
    if (strip) {
      for (let i = TABS.length - 1; i >= 0; i--) {
        const id = TABS[i];
        const t = strip.querySelector(
          '.nav-tab[data-nav-id="' + id + '"]:not(.is-ephemeral)'
        );
        if (t && t.style.display !== 'none') { adjId = id; break; }
      }
    }
    if (!adjId && TABS.length) adjId = TABS[TABS.length - 1];
    const next = adjId ? pageById[adjId] : null;
    window.location.href = window.vireoResolveNavigationHref(
      next ? next.href : '/browse'
    );
  }

  window.pinTab = function(navId) {
    return postJSON('/api/workspace/tabs/pin', {nav_id: navId})
      .then(() => fetchAndRender());
  };

  function fetchAndRender() {
    return fetch('/api/workspace/tabs')
      .then(r => r.json())
      .then(state => {
        TABS = (state && state.tabs) || [];
        ALL_PAGES = (state && state.all_pages) || [];
        pageById = {};
        ALL_PAGES.forEach(p => { pageById[p.id] = p; });
        renderStrip();
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

  // Re-recompute overflow on viewport resize *and* when individual tabs
  // change width (e.g. the Jobs tab's badge updates as background jobs
  // start/finish — without observing the tabs themselves, those content
  // changes never trigger a resize event and overflowing tabs stay
  // clipped instead of moving into the `…` menu).
  let tabsRO = null;
  if (window.ResizeObserver) {
    const navbarRO = new ResizeObserver(() => recomputeOverflow());
    if (document.querySelector('.navbar')) navbarRO.observe(document.querySelector('.navbar'));
    tabsRO = new ResizeObserver(() => recomputeOverflow());
    // Observe each rendered tab; renderStrip() replaces tab nodes, so
    // re-attach the observer on the MutationObserver below.
    function observeTabs() {
      tabsRO.disconnect();
      const strip = STRIP();
      if (!strip) return;
      strip.querySelectorAll('.nav-tab').forEach(t => tabsRO.observe(t));
    }
    const stripEl = STRIP();
    if (stripEl) {
      observeTabs();
      new MutationObserver(observeTabs).observe(stripEl, {childList: true});
    }
  } else {
    window.addEventListener('resize', recomputeOverflow);
  }

  // Expose for the palette + drag-reorder code in the next scripts
  window._navTabs = {
    fetchAndRender,
    getTabs: () => TABS.slice(),
    getAllPages: () => ALL_PAGES.slice(),
    setTabs: (newOrder) => {
      return postJSON('/api/workspace/tabs/reorder', {tabs: newOrder})
        .then(() => fetchAndRender());
    },
  };

  window.vireoRefreshNavigationHrefs = function() {
    const browse = pageById.browse;
    if (!browse) return;
    document.querySelectorAll('.nav-tab[data-nav-id="browse"]').forEach(a => {
      a.href = window.vireoResolveNavigationHref(browse.href);
    });
  };

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
})();
