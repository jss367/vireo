/* Workspace navigation owns menu identity, request generations and dismissal.
 * Call close() when another surface opens; never mutate its DOM/state externally.
 */
(function(root) {
  'use strict';
  function create(options) {
    var window = options.window || root;
    var document = window.document;
    /* ---------- Workspace switcher ---------- */
    var _wsDropdownOpen = false;
    var _wsActiveId = null;
    var _wsMenuLoadGeneration = 0;
    var outsideClickTimer = null;

    function close() {
      _wsDropdownOpen = false;
      ++_wsMenuLoadGeneration;
      var menu = document.getElementById('wsMenu');
      if (menu) menu.classList.remove('open');
      if (outsideClickTimer !== null) window.clearTimeout(outsideClickTimer);
      outsideClickTimer = null;
      document.removeEventListener('click', closeWsDropdownOutside);
    }

    function wsEscapeHtml(str) {
      if (str == null) return '';
      var div = document.createElement('div');
      div.appendChild(document.createTextNode(String(str)));
      return div.innerHTML;
    }

    async function loadCurrentName() {
      try {
        var ws = await options.fetch('/api/workspaces/active?include_folders=0', {}, { toast: false });
        var el = document.getElementById('wsCurrentName');
        if (el && ws && ws.name) el.textContent = ws.name;
      } catch(e) {}
    }

    function toggleWsDropdown() {
      if (_wsDropdownOpen) {
        close();
        return;
      }
      _wsDropdownOpen = true;
      var menu = document.getElementById('wsMenu');
      var loading = loadWorkspaceMenu();
      menu.classList.add('open');
      outsideClickTimer = window.setTimeout(function() {
        outsideClickTimer = null;
        if (_wsDropdownOpen) document.addEventListener('click', closeWsDropdownOutside);
      }, 0);
      return loading;
    }

    function closeWsDropdownOutside(e) {
      var dd = document.getElementById('wsDropdown');
      if (!dd.contains(e.target)) {
        close();
      }
    }

    function renderWsItem(ws, activeId) {
      var isActive = ws.id === activeId;
      var isPinned = !!ws.pinned_at;
      var div = document.createElement('div');
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
        var t = e.target;
        if (t && t.nodeType !== 1) t = t.parentElement;
        if (t && t.closest && t.closest('.ws-pin-btn')) return;
        switchWorkspace(ws.id, ws.name);
      };
      var pinBtn = div.querySelector('.ws-pin-btn');
      pinBtn.onclick = function(e) {
        e.stopPropagation();
        toggleWorkspacePin(ws.id, !isPinned);
      };
      return div;
    }

    function renderWorkspaceMenu(workspaces, activeId) {
      var list = document.getElementById('wsMenuList');
      list.innerHTML = '';
      var pinned = workspaces.filter(function(w) { return !!w.pinned_at; });
      var unpinned = workspaces.filter(function(w) { return !w.pinned_at; });
      function addHeader(text) {
        var h = document.createElement('div');
        h.className = 'ws-menu-header';
        h.textContent = text;
        list.appendChild(h);
      }
      if (pinned.length > 0) {
        addHeader('Pinned');
        pinned.forEach(function(ws) { list.appendChild(renderWsItem(ws, activeId)); });
        if (unpinned.length > 0) {
          var divider = document.createElement('div');
          divider.className = 'ws-menu-divider';
          list.appendChild(divider);
          addHeader('Workspaces');
        }
      } else {
        addHeader('Workspaces');
      }
      unpinned.forEach(function(ws) { list.appendChild(renderWsItem(ws, activeId)); });
    }

    async function loadWorkspaceMenu() {
      var generation = ++_wsMenuLoadGeneration;

      // Skip folder photo counts for navigation. Load identity in parallel so
      // the workspace names remain usable even if this request is delayed.
      var activePromise = options.fetch('/api/workspaces/active?include_folders=0', {}, { toast: false })
        .catch(function(e) {
          console.error('Failed to load active workspace', e);
          return null;
        });

      try {
        var workspaces = await options.fetch('/api/workspaces', {}, { toast: false });
        if (generation !== _wsMenuLoadGeneration) return;
        renderWorkspaceMenu(workspaces, _wsActiveId);

        activePromise.then(function(active) {
          if (active && generation === _wsMenuLoadGeneration) {
            _wsActiveId = active.id;
            renderWorkspaceMenu(workspaces, active.id);
          }
        });
      } catch(e) {
        console.error('Failed to load workspaces', e);
      }
    }

    async function toggleWorkspacePin(wsId, pinned) {
      try {
        await options.fetch('/api/workspaces/' + wsId + '/pin', {
          method: 'POST',
          headers: {'Content-Type': 'application/json'},
          body: JSON.stringify({ pinned: pinned })
        }, { toast: false });
        // Re-render with updated grouping/order.
        if (_wsDropdownOpen) await loadWorkspaceMenu();
      } catch(e) {
        console.error('Failed to toggle pin', e);
      }
    }

    async function switchWorkspace(wsId, wsName) {
      try {
        var data = await options.fetch('/api/workspaces/' + wsId + '/activate', {
          method: 'POST',
          headers: {'Content-Type': 'application/json'},
          body: JSON.stringify({ current_path: window.location.pathname })
        });
        document.getElementById('wsCurrentName').textContent = wsName;
        close();
        // Flag that a workspace switch just happened (for drift check)
        window.sessionStorage.setItem('vireo_ws_switched', '1');
        options.clearWorkspaceCursors();
        // Redirect to the workspace's saved page, or reload current
        options.navigate(data.restore_path || window.location.pathname);
      } catch(e) {}
    }

    /* ---------- Create Workspace modal ---------- */
    function showCreateWorkspaceModal() {
      close();
      document.getElementById('newWsName').value = '';
      document.getElementById('newWsError').style.display = 'none';
      document.getElementById('createWsModal').classList.add('open');
      document.getElementById('newWsName').focus();
    }

    function hideCreateWsModal() {
      document.getElementById('createWsModal').classList.remove('open');
    }

    async function createWorkspace() {
      var name = document.getElementById('newWsName').value.trim();
      if (!name) {
        document.getElementById('newWsError').textContent = 'Name is required';
        document.getElementById('newWsError').style.display = 'block';
        return;
      }

      try {
        var data = await options.fetch('/api/workspaces', {
          method: 'POST',
          headers: {'Content-Type': 'application/json'},
          body: JSON.stringify({ name: name })
        }, { toast: false });
        hideCreateWsModal();
        // Activate the new workspace, then go straight to pipeline
        await options.fetch('/api/workspaces/' + data.id + '/activate', {
          method: 'POST',
          headers: {'Content-Type': 'application/json'},
          body: JSON.stringify({ current_path: window.location.pathname })
        });
        window.sessionStorage.setItem('vireo_ws_switched', '1');
        options.clearWorkspaceCursors();
        options.navigate('/pipeline');
      } catch(e) {
        document.getElementById('newWsError').textContent = e.message || 'Failed to create';
        document.getElementById('newWsError').style.display = 'block';
      }
    }

    return Object.freeze({
      loadCurrentName: loadCurrentName,
      toggle: toggleWsDropdown,
      close: close,
      showCreate: showCreateWorkspaceModal,
      hideCreate: hideCreateWsModal,
      createWorkspace: createWorkspace
    });
  }
  root.VireoWorkspaceSwitcher = Object.freeze({ create: create });
})(window);
