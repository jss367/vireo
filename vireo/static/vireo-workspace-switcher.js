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

  function closeMenu(/** @type {WorkspaceSwitcherContext} */ switcher) {
    switcher.open = false;
    ++switcher.generation;
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
    var loading = loadWorkspaceMenu(switcher);
    menu.classList.add('open');
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

  /** @param {WorkspaceSummary} ws @param {number | null} activeId */
  function renderWsItem(/** @type {WorkspaceSwitcherContext} */ switcher, ws, activeId) {
    var isActive = ws.id === activeId;
    var isPinned = !!ws.pinned_at;
    var div = switcher.document.createElement('div');
    div.className = 'ws-menu-item' + (isActive ? ' active' : '');
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

  /** @param {WorkspaceSummary[]} workspaces @param {number | null} activeId */
  function renderWorkspaceMenu(/** @type {WorkspaceSwitcherContext} */ switcher, workspaces, activeId) {
    var list = /** @type {HTMLElement} */ (switcher.document.getElementById('wsMenuList'));
    list.innerHTML = '';
    var pinned = workspaces.filter(function(w) { return !!w.pinned_at; });
    var unpinned = workspaces.filter(function(w) { return !w.pinned_at; });
    /** @param {string} text */
    function addHeader(text) {
      var h = switcher.document.createElement('div');
      h.className = 'ws-menu-header';
      h.textContent = text;
      list.appendChild(h);
    }
    if (pinned.length > 0) {
      addHeader('Pinned');
      pinned.forEach(function(ws) { list.appendChild(renderWsItem(switcher, ws, activeId)); });
      if (unpinned.length > 0) {
        var divider = switcher.document.createElement('div');
        divider.className = 'ws-menu-divider';
        list.appendChild(divider);
        addHeader('Workspaces');
      }
    } else {
      addHeader('Workspaces');
    }
    unpinned.forEach(function(ws) { list.appendChild(renderWsItem(switcher, ws, activeId)); });
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
      renderWorkspaceMenu(switcher, workspaces, switcher.activeId);

      activePromise.then(function(active) {
        if (active && generation === switcher.generation) {
          switcher.activeId = active.id;
          renderWorkspaceMenu(switcher, workspaces, active.id);
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
      generation: 0,
      outsideClickTimer: null,
      onOutsideClick: function(e) { closeWsDropdownOutside(switcher, e); }
    };
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
