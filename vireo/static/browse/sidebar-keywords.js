/* Browse: keyword tree sidebar.
   Classic script sharing page globals; browse.html loads the
   browse/*.js files in dependency order. */

/* ---------- Keyword Tree ---------- */
function reconcileKeywordLoadRenders() {
  // Do not discard the newest usable keyword response merely because a
  // later request started. Pending newer generations still block it, while
  // failed newer generations fall back to the newest success just like the
  // folder and collection loaders.
  var gen = keywordLoadGen;
  while (gen > keywordRenderDecisionGen) {
    var state = keywordLoadStates[gen];
    if (!state || state.status === 'pending') return;
    if (state.status === 'success') {
      var shouldRender = !state.shouldRender || state.shouldRender();
      keywordRenderDecisionGen = gen;
      if (shouldRender) renderKeywordTree(state.data);
      Object.keys(keywordLoadStates).forEach(function(key) {
        if (Number(key) <= keywordLoadGen) delete keywordLoadStates[key];
      });
      return;
    }
    gen--;
  }
}

async function loadKeywords(opts) {
  var myGen = ++keywordLoadGen;
  keywordLoadStates[myGen] = {
    status: 'pending',
    shouldRender: opts && opts.shouldRender
  };
  try {
    var data = await safeFetch('/api/keywords', {}, { toast: false });
    var state = keywordLoadStates[myGen];
    if (!state) return data;
    state.status = 'success';
    state.data = data;
    reconcileKeywordLoadRenders();
    return data;
  } catch(e) {
    var failedState = keywordLoadStates[myGen];
    if (failedState) {
      failedState.status = 'failure';
      reconcileKeywordLoadRenders();
    }
    return null;
  }
}

function renderKeywordTree(keywords) {
  _rememberKeywordNames(keywords);
  var byParent = {};
  keywords.forEach(function(k) {
    var pid = k.parent_id || 'root';
    if (!byParent[pid]) byParent[pid] = [];
    byParent[pid].push(k);
  });

  var treeTypeIcons = {general:'',taxonomy:'🌿',individual:'👤',location:'📍',genre:'🎭'};
  function buildTree(parentId, depth) {
    var children = byParent[parentId] || [];
    var html = '';
    children.forEach(function(k) {
      var hasChildren = byParent[k.id] && byParent[k.id].length > 0;
      var indent = '';
      for (var i = 0; i < depth; i++) indent += '<span class="tree-indent"></span>';
      var toggle = hasChildren ? '<span class="tree-toggle" onclick="toggleTree(event,this)">&#9654;</span>' : '<span class="tree-indent"></span>';
      var activeClass = activeKeyword === k.name ? ' active' : '';
      var tIcon = treeTypeIcons[k.type] || '';
      html += '<div class="tree-item' + activeClass + '" data-keyword="' + escapeAttr(k.name) + '">' +
        indent + toggle +
        (tIcon ? '<span style="font-size:10px;margin-right:3px;opacity:0.7">' + tIcon + '</span>' : '') +
        '<span>' + escapeHtml(k.name) + '</span>' +
      '</div>';
      if (hasChildren) {
        html += '<div class="tree-children">' + buildTree(k.id, depth + 1) + '</div>';
      }
    });
    return html;
  }

  var tree = document.getElementById('keywordTree');
  tree.innerHTML = buildTree('root', 0) || '<div style="font-size:12px;color:var(--text-ghost);padding:4px 8px;">No keywords</div>';
  // Delegate keyword clicks (XSS-safe: avoids inline onclick with user data)
  tree.onclick = function(e) {
    var item = e.target.closest('.tree-item[data-keyword]');
    if (item) filterByKeyword(item.dataset.keyword);
  };
}

async function filterByKeyword(name) {
  // Bump the scope generation BEFORE the readiness guard so an in-flight
  // queued filterByCollection (awaiting browseFilterInitPromise) sees the
  // newer intent and bails out on resume. Without this, the collection
  // could resume after fields load and clobber the user's later click
  // (Codex review r3624549744).
  var myScopeGen = ++browseScopeGen;
  // The sidebar tree is rendered from the /photos init payload, so a
  // slow /api/filters/fields (or workspace) round-trip leaves clickable
  // keywords in the DOM before VireoFilter.init resolves. Without this
  // guard, addRule -> makeRule dereferences state.fields (still null)
  // and throws. Dropping the click outright would leave the bootstrap
  // grid (e.g. a ``?collection_id=A`` deep link's collection scope) in
  // place while the user's later keyword selection is silently lost
  // (Codex review r3624927534) — queue behind init and apply once ready,
  // matching filterByCollection's pattern.
  if (window.VireoFilter && !VireoFilter.isReady()) {
    if (!browseFilterInitPromise) return;
    try {
      await browseFilterInitPromise;
    } catch (e) {
      return;
    }
    if (!VireoFilter.isReady()) return;
    // A later sidebar click (folder/keyword/collection) advanced the scope
    // while we were waiting for filter-bar init. Applying this stale
    // keyword now would clobber the user's newer selection.
    if (browseScopeGen !== myScopeGen) return;
  }
  if (activeKeyword === name) {
    activeKeyword = null;
  } else {
    activeKeyword = name;
  }
  activeFolderId = null;
  activeCollectionId = null;
  clearOfflineCollectionState();
  // The rule toggles: adding an identical keyword rule removes it. The
  // filter bar's onChange handles the reload.
  VireoFilter.addRule('keyword', 'is', name);
  document.querySelectorAll('#keywordTree .tree-item').forEach(function(el) {
    el.classList.toggle('active', el.dataset.keyword === activeKeyword);
  });
}
