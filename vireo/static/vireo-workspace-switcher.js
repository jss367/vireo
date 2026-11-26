/* Workspace navigation owns menu identity, request generations and dismissal.
 * Call close() when another surface opens; never mutate its DOM/state externally.
 * create() builds one state record, never hands it out, and passes it to the
 * functions below.
 */
(function(root) {
  'use strict';
  /** @param {unknown} str */
  function wsEscapeHtml(str) {
    if (str == null) return '';
    var div = document.createElement('div');
    div.appendChild(document.createTextNode(String(str)));
    return div.innerHTML;
  }

  /* The search field stays hidden below this many workspaces: a shorter list
   * is read faster than it is searched.
   */
  var SEARCH_MIN_WORKSPACES = 8;

  function searchInput(/** @type {WorkspaceSwitcherContext} */ switcher) {
    return /** @type {HTMLInputElement | null} */ (switcher.document.getElementById('wsMenuSearch'));
  }

  /** Every word of the query appears somewhere in the name, in any case. @param {string} name @param {string} query */
  function nameMatches(name, query) {
    var text = name.toLowerCase();
    return query.toLowerCase().split(/\s+/).filter(Boolean).every(function(word) {
      return text.indexOf(word) !== -1;
    });
  }

  /** The workspaces the menu lists, in the order it lists them: pinned first. */
  function listedWorkspaces(/** @type {WorkspaceSwitcherContext} */ switcher) {
    var matching = switcher.workspaces.filter(function(w) { return nameMatches(w.name, switcher.query); });
    return matching.filter(function(w) { return !!w.pinned_at; })
      .concat(matching.filter(function(w) { return !w.pinned_at; }));
  }

  function focusSearch(/** @type {WorkspaceSwitcherContext} */ switcher) {
    var search = searchInput(switcher);
    if (switcher.open && search && !search.hidden) search.focus();
  }

  function closeMenu(/** @type {WorkspaceSwitcherContext} */ switcher) {
    switcher.open = false;
    ++switcher.generation;
    switcher.query = '';
    switcher.highlightId = null;
    var search = searchInput(switcher);
    if (search) search.value = '';
    var menu = /** @type {HTMLElement} */ (switcher.document.getElementById('wsMenu'));
    if (menu) menu.classList.remove('open');
    if (switcher.outsideClickTimer !== null) switcher.window.clearTimeout(switcher.outsideClickTimer);
    switcher.outsideClickTimer = null;
    switcher.document.removeEventListener('click', switcher.onOutsideClick);
  }

  async function loadCurrentName(/** @type {WorkspaceSwitcherContext} */ switcher) {
    try {
      var ws = await /** @type {Promise<WorkspaceSummary>} */ (switcher.options.fetch('/api/workspaces/active?include_folders=0', {}, { toast: false }));
      var el = /** @type {HTMLElement} */ (switcher.document.getElementById('wsCurrentName'));
      if (el && ws && ws.name) el.textContent = ws.name;
    } catch(e) {}
  }

  function toggleWsDropdown(/** @type {WorkspaceSwitcherContext} */ switcher) {
    if (switcher.open) {
      closeMenu(switcher);
      return;
    }
    switcher.open = true;
    var menu = /** @type {HTMLElement} */ (switcher.document.getElementById('wsMenu'));
    // closeMenu leaves the previous filter's rows in the DOM; render the cached
    // workspaces so the visible list matches the reset query before the menu
    // becomes visible, and stays correct if the reload below fails.
    if (switcher.workspaces.length) renderWorkspaceMenu(switcher);
    var loading = loadWorkspaceMenu(switcher).then(function() { focusSearch(switcher); });
    menu.classList.add('open');
    // On a reopen the field is already showing, so typing can start before the list returns.
    focusSearch(switcher);
    switcher.outsideClickTimer = switcher.window.setTimeout(function() {
      switcher.outsideClickTimer = null;
      if (switcher.open) switcher.document.addEventListener('click', switcher.onOutsideClick);
    }, 0);
    return loading;
  }

  /** @param {MouseEvent} e */
  function closeWsDropdownOutside(/** @type {WorkspaceSwitcherContext} */ switcher, e) {
    var dd = /** @type {HTMLElement} */ (switcher.document.getElementById('wsDropdown'));
    if (!(e.target instanceof Node) || !dd.contains(e.target)) {
      closeMenu(switcher);
    }
  }

  /** @param {WorkspaceSummary} ws */
  function renderWsItem(/** @type {WorkspaceSwitcherContext} */ switcher, ws) {
    var isActive = ws.id === switcher.activeId;
    var isPinned = !!ws.pinned_at;
    var div = switcher.document.createElement('div');
    div.className = 'ws-menu-item' + (isActive ? ' active' : '') +
      (ws.id === switcher.highlightId ? ' highlighted' : '');
    // Title is "Unpin" if pinned, "Pin" if not. Filled star for pinned, outline for unpinned.
    div.innerHTML =
      '<span class="ws-item-name">' + wsEscapeHtml(ws.name) + '</span>' +
      '<span class="ws-item-right">' +
        (isActive ? '<span class="ws-item-check">&#10003;</span>' : '') +
        '<button class="ws-pin-btn' + (isPinned ? ' is-pinned' : '') + '" ' +
          'title="' + (isPinned ? 'Unpin' : 'Pin') + ' workspace" ' +
          'data-ws-id="' + ws.id + '" data-pinned="' + (isPinned ? '1' : '0') + '">' +
          (isPinned ? '&#9733;' : '&#9734;') +
        '</button>' +
      '</span>';
    div.onclick = function(e) {
      // Pin button clicks are handled separately and must not switch workspaces.
      // e.target can be a text node in some browsers; walk up to the nearest Element first.
      var t = e.target instanceof Node ? e.target : null;
      if (t && !(t instanceof Element)) t = t.parentElement;
      if (t instanceof Element && t.closest('.ws-pin-btn')) return;
      switchWorkspace(switcher, ws.id, ws.name);
    };
    var pinBtn = /** @type {HTMLButtonElement} */ (div.querySelector('.ws-pin-btn'));
    pinBtn.onclick = function(e) {
      e.stopPropagation();
      toggleWorkspacePin(switcher, ws.id, !isPinned);
    };
    return div;
  }

  function renderWorkspaceMenu(/** @type {WorkspaceSwitcherContext} */ switcher) {
    var list = /** @type {HTMLElement} */ (switcher.document.getElementById('wsMenuList'));
    var search = searchInput(switcher);
    if (search) search.hidden = switcher.workspaces.length < SEARCH_MIN_WORKSPACES;
    list.innerHTML = '';
    var listed = listedWorkspaces(switcher);
    var searching = !!switcher.query.trim();
    if (searching && listed.length === 0) {
      var empty = switcher.document.createElement('div');
      empty.className = 'ws-menu-empty';
      empty.textContent = 'No workspace matches \u201c' + switcher.query.trim() + '\u201d';
      list.appendChild(empty);
      return;
    }
    // Enter acts on the highlighted row, so a search always highlights one that is listed.
    var highlighted = listed.some(function(w) { return w.id === switcher.highlightId; });
    var first = listed[0];
    if (!highlighted) switcher.highlightId = searching && first ? first.id : null;
    var pinned = listed.filter(function(w) { return !!w.pinned_at; });
    var unpinned = listed.filter(function(w) { return !w.pinned_at; });
    /** @param {string} text */
    function addHeader(text) {
      var h = switcher.document.createElement('div');
      h.className = 'ws-menu-header';
      h.textContent = text;
      list.appendChild(h);
    }
    if (pinned.length > 0) {
      addHeader('Pinned');
      pinned.forEach(function(ws) { list.appendChild(renderWsItem(switcher, ws)); });
      if (unpinned.length > 0) {
        var divider = switcher.document.createElement('div');
        divider.className = 'ws-menu-divider';
        list.appendChild(divider);
        addHeader('Workspaces');
      }
    } else if (unpinned.length > 0 || !searching) {
      addHeader('Workspaces');
    }
    unpinned.forEach(function(ws) { list.appendChild(renderWsItem(switcher, ws)); });
    var row = list.querySelector('.ws-menu-item.highlighted');
    if (row) row.scrollIntoView({ block: 'nearest' });
  }

  /** @param {number} step 1 for the next listed workspace, -1 for the previous one */
  function moveHighlight(/** @type {WorkspaceSwitcherContext} */ switcher, step) {
    var listed = listedWorkspaces(switcher);
    var at = listed.findIndex(function(w) { return w.id === switcher.highlightId; });
    var next = at < 0 ? (step > 0 ? 0 : listed.length - 1) : (at + step + listed.length) % listed.length;
    var target = listed[next];
    if (!target) return;
    switcher.highlightId = target.id;
    renderWorkspaceMenu(switcher);
  }

  /** @param {KeyboardEvent} e */
  function onSearchKeydown(/** @type {WorkspaceSwitcherContext} */ switcher, e) {
    if (e.key === 'ArrowDown' || e.key === 'ArrowUp') {
      e.preventDefault();
      moveHighlight(switcher, e.key === 'ArrowDown' ? 1 : -1);
    } else if (e.key === 'Enter') {
      e.preventDefault();
      var target = listedWorkspaces(switcher).find(function(w) { return w.id === switcher.highlightId; });
      if (target) switchWorkspace(switcher, target.id, target.name);
    } else if (e.key === 'Escape') {
      // Escape closes only this menu, not a lightbox or modal listening on the document.
      e.preventDefault();
      e.stopPropagation();
      closeMenu(switcher);
      var button = switcher.document.getElementById('wsCurrentBtn');
      if (button) button.focus();
    }
  }

  function bindSearch(/** @type {WorkspaceSwitcherContext} */ switcher) {
    var search = searchInput(switcher);
    if (!search) return;
    search.addEventListener('input', function() {
      switcher.query = /** @type {HTMLInputElement} */ (search).value;
      switcher.highlightId = null;
      renderWorkspaceMenu(switcher);
    });
    search.addEventListener('keydown', function(e) { onSearchKeydown(switcher, e); });
  }

  async function loadWorkspaceMenu(/** @type {WorkspaceSwitcherContext} */ switcher) {
    var generation = ++switcher.generation;

    // Skip folder photo counts for navigation. Load identity in parallel so
    // the workspace names remain usable even if this request is delayed.
    var activePromise = (/** @type {Promise<WorkspaceSummary>} */ (switcher.options.fetch('/api/workspaces/active?include_folders=0', {}, { toast: false })))
      .catch(function(e) {
        console.error('Failed to load active workspace', e);
        return null;
      });

    try {
      var workspaces = await /** @type {Promise<WorkspaceSummary[]>} */ (switcher.options.fetch('/api/workspaces', {}, { toast: false }));
      if (generation !== switcher.generation) return;
      switcher.workspaces = workspaces;
      renderWorkspaceMenu(switcher);

      activePromise.then(function(active) {
        if (active && generation === switcher.generation) {
          switcher.activeId = active.id;
          renderWorkspaceMenu(switcher);
        }
      });
    } catch(e) {
      console.error('Failed to load workspaces', e);
    }
  }

  /** @param {number} wsId @param {boolean} pinned */
  async function toggleWorkspacePin(/** @type {WorkspaceSwitcherContext} */ switcher, wsId, pinned) {
    try {
      await switcher.options.fetch('/api/workspaces/' + wsId + '/pin', {
        method: 'POST',
        headers: {'Content-Type': 'application/json'},
        body: JSON.stringify({ pinned: pinned })
      }, { toast: false });
      // Re-render with updated grouping/order.
      if (switcher.open) await loadWorkspaceMenu(switcher);
    } catch(e) {
      console.error('Failed to toggle pin', e);
    }
  }

  /** @param {number} wsId @param {string} wsName */
  async function switchWorkspace(/** @type {WorkspaceSwitcherContext} */ switcher, wsId, wsName) {
    try {
      var data = await /** @type {Promise<{restore_path?: string}>} */ (switcher.options.fetch('/api/workspaces/' + wsId + '/activate', {
        method: 'POST',
        headers: {'Content-Type': 'application/json'},
        body: JSON.stringify({ current_path: switcher.window.location.pathname })
      }));
      /** @type {HTMLElement} */ (switcher.document.getElementById('wsCurrentName')).textContent = wsName;
      closeMenu(switcher);
      // Flag that a workspace switch just happened (for drift check)
      switcher.window.sessionStorage.setItem('vireo_ws_switched', '1');
      switcher.options.clearWorkspaceCursors();
      // Redirect to the workspace's saved page, or reload current
      switcher.options.navigate(data.restore_path || switcher.window.location.pathname);
    } catch(e) {}
  }

  /* ---------- Create Workspace modal ---------- */
  function showCreateWorkspaceModal(/** @type {WorkspaceSwitcherContext} */ switcher) {
    closeMenu(switcher);
    /** @type {HTMLInputElement} */ (switcher.document.getElementById('newWsName')).value = '';
    /** @type {HTMLElement} */ (switcher.document.getElementById('newWsError')).style.display = 'none';
    /** @type {HTMLElement} */ (switcher.document.getElementById('createWsModal')).classList.add('open');
    /** @type {HTMLInputElement} */ (switcher.document.getElementById('newWsName')).focus();
  }

  function hideCreateWsModal(/** @type {WorkspaceSwitcherContext} */ switcher) {
    /** @type {HTMLElement} */ (switcher.document.getElementById('createWsModal')).classList.remove('open');
  }

  async function createWorkspace(/** @type {WorkspaceSwitcherContext} */ switcher) {
    var name = /** @type {HTMLInputElement} */ (switcher.document.getElementById('newWsName')).value.trim();
    if (!name) {
      /** @type {HTMLElement} */ (switcher.document.getElementById('newWsError')).textContent = 'Name is required';
      /** @type {HTMLElement} */ (switcher.document.getElementById('newWsError')).style.display = 'block';
      return;
    }

    try {
      var data = await /** @type {Promise<{id: number}>} */ (switcher.options.fetch('/api/workspaces', {
        method: 'POST',
        headers: {'Content-Type': 'application/json'},
        body: JSON.stringify({ name: name })
      }, { toast: false }));
      hideCreateWsModal(switcher);
      // Activate the new workspace, then go straight to pipeline
      await switcher.options.fetch('/api/workspaces/' + data.id + '/activate', {
        method: 'POST',
        headers: {'Content-Type': 'application/json'},
        body: JSON.stringify({ current_path: switcher.window.location.pathname })
      });
      switcher.window.sessionStorage.setItem('vireo_ws_switched', '1');
      switcher.options.clearWorkspaceCursors();
      switcher.options.navigate('/pipeline');
    } catch(e) {
      /** @type {HTMLElement} */ (switcher.document.getElementById('newWsError')).textContent = (e instanceof Error ? e.message : '') || 'Failed to create';
      /** @type {HTMLElement} */ (switcher.document.getElementById('newWsError')).style.display = 'block';
    }
  }

  /** @param {WorkspaceSwitcherOptions} options @returns {WorkspaceSwitcher} */
  function create(options) {
    var window = options.window || root;
    /** @type {WorkspaceSwitcherContext} */
    var switcher = {
      window: window,
      document: window.document,
      options: options,
      open: false,
      activeId: null,
      workspaces: [],
      query: '',
      highlightId: null,
      generation: 0,
      outsideClickTimer: null,
      onOutsideClick: function(e) { closeWsDropdownOutside(switcher, e); }
    };
    bindSearch(switcher);
    return Object.freeze({
      loadCurrentName: loadCurrentName.bind(null, switcher),
      toggle: toggleWsDropdown.bind(null, switcher),
      close: closeMenu.bind(null, switcher),
      showCreate: showCreateWorkspaceModal.bind(null, switcher),
      hideCreate: hideCreateWsModal.bind(null, switcher),
      createWorkspace: createWorkspace.bind(null, switcher)
    });
  }
  root.VireoWorkspaceSwitcher = Object.freeze({ create: create });
})(window);
