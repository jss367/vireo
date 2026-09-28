(function() {
  // Seed from the same hardcoded globals the navbar strip uses so the
  // palette is usable even when /api/workspace/tabs fails on initial
  // load. refreshFromTabsState() replaces these with the workspace's
  // actual state on success.
  let allPages = (window.NAV_ALL_PAGES || []).slice();
  let pinnedSet = new Set(window.NAV_DEFAULT_TABS || []);
  let fuse = null;
  let results = [];
  let selectedIndex = 0;

  function rebuildFuse() {
    if (window.Fuse) {
      fuse = new Fuse(allPages, {
        keys: [
          {name: 'label',    weight: 0.6},
          {name: 'keywords', weight: 0.3},
          {name: 'id',       weight: 0.1},
        ],
        threshold: 0.4,
        ignoreLocation: true,
      });
    }
  }
  rebuildFuse();

  function refreshFromTabsState() {
    return fetch('/api/workspace/tabs').then(r => r.json()).then(state => {
      if (Array.isArray(state.all_pages) && state.all_pages.length) {
        allPages = state.all_pages;
      }
      if (Array.isArray(state.tabs)) {
        pinnedSet = new Set(state.tabs);
      }
      rebuildFuse();
    });
  }

  function currentNavId() {
    const p = window.location.pathname;
    if (p.startsWith('/pipeline/rapid-review')) return 'pipeline_rapid_review';
    if (p.startsWith('/pipeline/review')) return 'pipeline_review';
    if (p.startsWith('/locations/review')) return 'location_review';
    if (p === '/' || p.startsWith('/browse')) return 'browse';
    return (p.split('/')[1] || '').replace(/-/g, '_');
  }

  function defaultSorted() {
    // Pinned in pinned-order (use _navTabs if available), then unpinned
    // alphabetically by label.
    const pinnedOrder = (window._navTabs ? window._navTabs.getTabs() : []);
    const pinnedById = new Map(allPages.map(p => [p.id, p]));
    const out = [];
    pinnedOrder.forEach(id => {
      if (pinnedById.has(id)) out.push(pinnedById.get(id));
    });
    const unpinned = allPages
      .filter(p => !pinnedSet.has(p.id))
      .sort((a, b) => a.label.localeCompare(b.label));
    return out.concat(unpinned);
  }

  function render(query) {
    const list = document.getElementById('cmdPaletteResults');
    if (!list) return;
    list.textContent = '';
    if (!query) {
      results = defaultSorted();
    } else {
      const options = VireoTextSearch.readOptions('cmdPalette');
      if ((options.matchCase || options.wholeWord) && window.VireoTextSearch) {
        results = allPages.filter(p =>
          VireoTextSearch.matchesFields(
            [p.label, p.id, p.keywords || ''],
            query,
            options
          )
        );
      } else if (fuse) {
        results = fuse.search(query).map(r => r.item);
      } else {
        results = allPages.filter(p =>
          VireoTextSearch.matchesFields([p.label, p.id, p.keywords || ''], query)
        );
      }
    }
    if (selectedIndex >= results.length) selectedIndex = 0;
    const cur = currentNavId();
    results.forEach((p, idx) => {
      const row = document.createElement('div');
      row.className = 'cmd-palette-result' + (idx === selectedIndex ? ' selected' : '');
      if (p.id === cur) row.classList.add('is-current');
      row.dataset.navId = p.id;
      row.appendChild(document.createTextNode(p.label));
      if (pinnedSet.has(p.id)) {
        const pin = document.createElement('span');
        pin.className = 'cmd-palette-result-pinned';
        pin.textContent = '📌';
        row.appendChild(pin);
      }
      row.addEventListener('click', () => navigateTo(p));
      list.appendChild(row);
    });
  }

  function navigateTo(page) {
    window.closeCommandPalette();
    window.location.href = window.vireoResolveNavigationHref(page.href);
  }

  // Wire input handlers
  document.addEventListener('DOMContentLoaded', () => {
    const input = document.getElementById('cmdPaletteInput');
    if (!input) return;
    input.addEventListener('input', () => {
      selectedIndex = 0;
      render(input.value);
    });
    input.addEventListener('keydown', (e) => {
      if (e.key === 'ArrowDown') {
        e.preventDefault();
        if (results.length === 0) return;
        selectedIndex = (selectedIndex + 1) % results.length;
        render(input.value);
      } else if (e.key === 'ArrowUp') {
        e.preventDefault();
        if (results.length === 0) return;
        selectedIndex = (selectedIndex - 1 + results.length) % results.length;
        render(input.value);
      } else if (e.key === 'Enter') {
        e.preventDefault();
        const target = results[selectedIndex];
        if (target) navigateTo(target);
      }
    });
  });

  // Re-fetch state when palette opens (in case tabs changed). If the
  // refresh fails (network blip, JSON parse error), still open the
  // palette with whatever state we already have — a stale-but-open
  // palette beats a no-op shortcut.
  const origOpen = window.openCommandPalette;
  window.openCommandPalette = function() {
    selectedIndex = 0;
    const open = () => origOpen();
    refreshFromTabsState().then(open, open);
  };
  window._cmdPaletteRender = render;

  refreshFromTabsState();
})();
