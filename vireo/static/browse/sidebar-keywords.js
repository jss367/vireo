/* Browse: keyword tree sidebar. ES module (see
   browse/README.md). */
import { state } from './state.js';
import { VireoFilter, escapeAttr, escapeHtml, safeFetch } from './shared.js';
import { _rememberKeywordNames } from './batch-actions.js';
import { clearOfflineCollectionState, syncSidebarHighlights } from './sidebar-collections.js';
import { bindBrowseMarkup } from './markup.js';

/* ---------- Keyword Tree ---------- */
function reconcileKeywordLoadRenders() {
  // Do not discard the newest usable keyword response merely because a
  // later request started. Pending newer generations still block it, while
  // failed newer generations fall back to the newest success just like the
  // folder and collection loaders.
  var gen = state.keywordLoadGen;
  while (gen > state.keywordRenderDecisionGen) {
    var loadState = state.keywordLoadStates[gen];
    if (!loadState || loadState.status === 'pending') return;
    if (loadState.status === 'success' && loadState.data) {
      var shouldRender = !loadState.shouldRender || loadState.shouldRender();
      state.keywordRenderDecisionGen = gen;
      if (shouldRender) renderKeywordTree(loadState.data);
      Object.keys(state.keywordLoadStates).forEach(function(key) {
        if (Number(key) <= state.keywordLoadGen) delete state.keywordLoadStates[key];
      });
      return;
    }
    gen--;
  }
}

export async function loadKeywords(/** @type {{shouldRender?: () => boolean} | undefined} */ opts = undefined) {
  var myGen = ++state.keywordLoadGen;
  state.keywordLoadStates[myGen] = {
    status: 'pending',
    shouldRender: opts && opts.shouldRender
  };
  try {
    var data = await safeFetch('/api/keywords', {}, { toast: false });
    var loadState = state.keywordLoadStates[myGen];
    if (!loadState) return data;
    loadState.status = 'success';
    loadState.data = data;
    reconcileKeywordLoadRenders();
    return data;
  } catch(e) {
    var failedState = state.keywordLoadStates[myGen];
    if (failedState) {
      failedState.status = 'failure';
      reconcileKeywordLoadRenders();
    }
    return null;
  }
}

export function renderKeywordTree(/** @type {BrowseKeyword[]} */ keywords) {
  _rememberKeywordNames(keywords);
  /** @type {Record<string, BrowseKeyword[]>} */ var byParent = {};
  keywords.forEach(function(k) {
    var pid = k.parent_id || 'root';
    if (!byParent[pid]) byParent[pid] = [];
    (byParent[pid] || (byParent[pid] = [])).push(k);
  });

  /** @type {Record<string, string>} */ var treeTypeIcons = {general:'',taxonomy:'🌿',individual:'👤',location:'📍',genre:'🎭'};
  function buildTree(/** @type {number | string} */ parentId, /** @type {number} */ depth) {
    var children = byParent[parentId] || [];
    var html = '';
    children.forEach(function(k) {
      var hasChildren = byParent[k.id] && (byParent[k.id]?.length || 0) > 0;
      var indent = '';
      for (var i = 0; i < depth; i++) indent += '<span class="tree-indent"></span>';
      var toggle = hasChildren ? '<span class="tree-toggle" data-browse-on="click:toggleTree">&#9654;</span>' : '<span class="tree-indent"></span>';
      var activeClass = state.activeKeyword === k.name ? ' active' : '';
      var tIcon = treeTypeIcons[k.type] || '';
      html += '<div class="tree-item' + activeClass + '" data-keyword="' + escapeAttr(k.name) + '" data-browse-on="click:filterByKeyword">' +
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

  var tree = /** @type {HTMLElement} */ (document.getElementById('keywordTree'));
  tree.innerHTML = buildTree('root', 0) || '<div style="font-size:12px;color:var(--text-ghost);padding:4px 8px;">No keywords</div>';
  // The keyword name travels in data-keyword, never in handler code.
  bindBrowseMarkup(tree);
}

export async function filterByKeyword(/** @type {string} */ name) {
  // Bump the scope generation BEFORE the readiness guard so an in-flight
  // queued filterByCollection (awaiting browseFilterInitPromise) sees the
  // newer intent and bails out on resume. Without this, the collection
  // could resume after fields load and clobber the user's later click
  // (Codex review r3624549744).
  var myScopeGen = ++state.browseScopeGen;
  // The sidebar tree is rendered from the /photos init payload, so a
  // slow /api/filters/fields (or workspace) round-trip leaves clickable
  // keywords in the DOM before VireoFilter.init resolves. Without this
  // guard, addRule -> makeRule dereferences state.fields (still null)
  // and throws. Dropping the click outright would leave the bootstrap
  // grid (e.g. a ``?collection_id=A`` deep link's collection scope) in
  // place while the user's later keyword selection is silently lost
  // (Codex review r3624927534) — queue behind init and apply once ready,
  // matching filterByCollection's pattern.
  if (VireoFilter && !VireoFilter.isReady()) {
    if (!state.browseFilterInitPromise) return;
    try {
      await state.browseFilterInitPromise;
    } catch (e) {
      return;
    }
    if (!VireoFilter.isReady()) return;
    // A later sidebar click (folder/keyword/collection) advanced the scope
    // while we were waiting for filter-bar init. Applying this stale
    // keyword now would clobber the user's newer selection.
    if (state.browseScopeGen !== myScopeGen) return;
  }
  if (state.activeKeyword === name) {
    state.activeKeyword = null;
  } else {
    state.activeKeyword = name;
  }
  state.activeFolderId = null;
  state.activeCollectionId = null;
  clearOfflineCollectionState();
  // The rule toggles: adding an identical keyword rule removes it. The
  // filter bar's onChange handles the reload.
  VireoFilter.addRule('keyword', 'is', name);
  syncSidebarHighlights();
}
