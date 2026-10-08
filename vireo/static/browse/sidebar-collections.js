/* Browse: collections sidebar, counts, offline members, membership refresh.
   Classic script sharing page globals; browse.html loads the
   browse/*.js files in dependency order. */

function collectionCountMarkup(collection) {
  if (collection.photo_count == null) return '';
  var total = Number(collection.photo_count);
  var offline = Number(collection.offline_photo_count || 0);
  var html = total.toLocaleString();
  if (offline > 0) {
    html += ' <span class="collection-offline-count" title="' +
      offline.toLocaleString() + ' photo' + (offline === 1 ? '' : 's') +
      ' currently offline">\u00b7 ' + offline.toLocaleString() + ' offline</span>';
  }
  return html;
}

function renderCollectionList(collections) {
  collectionsById = {};
  var html = '';
  collections.forEach(function(c) {
    collectionsById[c.id] = c;
    var canAddPhotos = collectionAcceptsManualPhotos(c);
    var kind = canAddPhotos ? 'manual' : 'smart';
    var kindLabel = canAddPhotos ? 'Manual' : 'Smart';
    // count_error: /api/collections couldn't resolve this collection's rules,
    // so filtering to it would 400 the /photos endpoint. Show it as
    // unavailable (with edit still reachable via right-click) instead of
    // rendering a clickable filter that silently fails.
    var unavailable = !!c.count_error;
    // Suppress location review on unavailable collections because the review
    // page resolves the same collection rules when it builds its queue.
    var resolveBtn = (c.name === 'GPS Without Location Keyword' && !unavailable)
      ? '<button class="collection-resolve-btn" type="button" title="Review photo locations on a map" onclick="event.stopPropagation(); reviewLocationsForCollection(' + c.id + ')">Review on Map</button>'
      : '';
    var itemClass = 'tree-item' + (unavailable ? ' unavailable' : '');
    var titleAttr = unavailable
      ? ' title="This collection\'s rules could not be resolved. Right-click → Edit Rules to fix it."'
      : '';
    var trailing = unavailable
      ? '<span class="collection-unavailable-badge">unavailable</span>'
      : '<span class="count">' + collectionCountMarkup(c) + '</span>';
    var visualMark = c.visual_json
      ? '<span class="collection-visual-mark" title="Includes a visual search clause">\u2726</span>'
      : '';
    html += '<div class="' + itemClass + '" data-collection-id="' + c.id + '" data-collection-kind="' + kind + '"' + titleAttr + ' onclick="filterByCollection(' + c.id + ')">' +
      '<span class="collection-name">' + escapeHtml(c.name) + '</span>' + visualMark +
      '<span class="collection-kind-badge ' + kind + '">' + kindLabel + '</span>' +
      resolveBtn +
      trailing + '</div>';
  });
  document.getElementById('collectionList').innerHTML = html || '<div style="font-size:12px;color:var(--text-ghost);padding:4px 8px;">No collections</div>';
}

/* ---------- Collections ---------- */
function reconcileCollectionLoadRenders() {
  // Mirror reconcileFolderLoadRenders: don't render an older successful
  // response while a newer request is still pending, but if every newer
  // request failed, fall back to the newest available success rather than
  // leaving the collection list stale. Otherwise a health-owned request
  // that observed a just-created collection could be silently dropped by
  // a subsequent mutation-triggered reload that transiently fails, so the
  // collection stays absent until the next explicit list refresh
  // (Codex review r3687331931).
  var gen = collectionLoadGen;
  while (gen > collectionRenderDecisionGen) {
    var state = collectionLoadStates[gen];
    if (!state || state.status === 'pending') return;
    if (state.status === 'success') {
      var shouldRender = !state.shouldRender || state.shouldRender();
      collectionRenderDecisionGen = gen;
      if (shouldRender) renderCollectionList(state.data);
      Object.keys(collectionLoadStates).forEach(function(key) {
        if (Number(key) <= collectionLoadGen) delete collectionLoadStates[key];
      });
      return;
    }
    gen--;
  }
}

async function loadCollections(opts) {
  var myGen = ++collectionLoadGen;
  collectionLoadStates[myGen] = {
    status: 'pending',
    shouldRender: opts && opts.shouldRender
  };
  try {
    var data = await safeFetch('/api/collections', {}, { toast: false });
    var state = collectionLoadStates[myGen];
    if (!state) return data;
    state.status = 'success';
    state.data = data;
    reconcileCollectionLoadRenders();
    return data;
  } catch(e) {
    var failedState = collectionLoadStates[myGen];
    if (failedState) {
      failedState.status = 'failure';
      reconcileCollectionLoadRenders();
    }
    return null;
  }
}

async function loadCollectionCounts() {
  var gen = ++collectionCountLoadGen;
  try {
    var data = await safeFetch('/api/collections', {}, { toast: false });
    if (gen !== collectionCountLoadGen) return;
    if (!data) return;
    var items = document.querySelectorAll('#collectionList .tree-item');
    var metaById = {};
    data.forEach(function(c) { metaById[c.id] = c; });
    items.forEach(function(el) {
      var id = parseInt(el.dataset.collectionId, 10);
      if (isNaN(id)) {
        // Fallback for any stragglers without data-collection-id.
        var onclick = el.getAttribute('onclick') || '';
        var m = onclick.match(/filterByCollection\((\d+)\)/);
        if (m) id = parseInt(m[1], 10);
      }
      var meta = !isNaN(id) ? metaById[id] : null;
      if (!meta) return;
      // Keep our in-memory copy in sync so filterByCollection's guard fires
      // for collections that transitioned to (or out of) unavailable.
      collectionsById[id] = meta;
      var wasUnavailable = el.classList.contains('unavailable');
      var isUnavailable = !!meta.count_error;
      if (wasUnavailable !== isUnavailable) {
        el.classList.toggle('unavailable', isUnavailable);
        var trailing = el.querySelector('.count, .collection-unavailable-badge');
        if (trailing) {
          if (isUnavailable) {
            trailing.className = 'collection-unavailable-badge';
            trailing.textContent = 'unavailable';
          } else {
            trailing.className = 'count';
            trailing.innerHTML = collectionCountMarkup(meta);
          }
        }
        if (isUnavailable) {
          el.setAttribute(
            'title',
            "This collection's rules could not be resolved. Right-click → Edit Rules to fix it."
          );
        } else {
          el.removeAttribute('title');
        }
      } else if (!isUnavailable) {
        var span = el.querySelector('.count');
        if (span && meta.photo_count != null) {
          span.innerHTML = collectionCountMarkup(meta);
        }
      }
    });
  } catch(e) {}
}

function scheduleCollectionCountsRefresh() {
  if (collectionCountRefreshTimer) clearTimeout(collectionCountRefreshTimer);
  collectionCountRefreshTimer = setTimeout(function() {
    collectionCountRefreshTimer = null;
    loadCollectionCounts();
  }, 150);
}

function hasCollectionAvailabilityScope() {
  return openedCollectionId != null || activeCollectionId != null;
}

function renderOfflineCollectionNotice() {
  var notice = document.getElementById('offlineCollectionNotice');
  if (!notice) return;
  if (!hasCollectionAvailabilityScope() || collectionOfflineTotal <= 0) {
    notice.style.display = 'none';
    return;
  }
  var text = document.getElementById('offlineCollectionText');
  var toggle = document.getElementById('offlineCollectionToggle');
  var available = collectionAvailableTotal.toLocaleString();
  var inventory = collectionInventoryTotal.toLocaleString();
  var offline = collectionOfflineTotal.toLocaleString();
  text.textContent = available + ' of ' + inventory + ' photos available \u00b7 ' +
    offline + ' offline' +
    (showOfflineCollectionPhotos
      ? ' (shown read-only)'
      : ' (hidden)');
  toggle.textContent = showOfflineCollectionPhotos
    ? 'Hide offline photos'
    : 'Show offline photos';
  notice.style.display = 'flex';
}

function clearOfflineCollectionState() {
  showOfflineCollectionPhotos = false;
  collectionInventoryTotal = 0;
  collectionAvailableTotal = 0;
  collectionOfflineTotal = 0;
  renderOfflineCollectionNotice();
}

function updateOfflineCollectionState(data) {
  if (!hasCollectionAvailabilityScope()) {
    clearOfflineCollectionState();
    return;
  }
  // These are photo counts. ``data.total`` is the logical item count, which
  // Stacks collapses, so fall back through ``underlying_total`` first —
  // otherwise a stacked view would report "N of M photos" in stacks.
  var photoTotal = data.underlying_total != null
    ? Number(data.underlying_total) || 0
    : Number(data.total) || 0;
  collectionInventoryTotal = data.inventory_total != null
    ? Number(data.inventory_total) || 0
    : photoTotal;
  collectionAvailableTotal = data.available_total != null
    ? Number(data.available_total) || 0
    : photoTotal;
  collectionOfflineTotal = Number(data.offline_total || 0);
  // Visual collections intentionally skip sidebar counts until their prompt
  // is resolved. Once opened, promote the resolved membership into the same
  // total/offline presentation as metadata collections.
  var collectionId = openedCollectionId != null
    ? openedCollectionId
    : activeCollectionId;
  var meta = collectionsById[collectionId];
  if (meta && data.inventory_total != null) {
    meta.photo_count = collectionInventoryTotal;
    meta.available_photo_count = collectionAvailableTotal;
    meta.offline_photo_count = collectionOfflineTotal;
    var row = document.querySelector(
      '#collectionList .tree-item[data-collection-id="' + collectionId + '"] .count'
    );
    if (row) row.innerHTML = collectionCountMarkup(meta);
  }
  if (collectionOfflineTotal <= 0) showOfflineCollectionPhotos = false;
  renderOfflineCollectionNotice();
}

function toggleOfflineCollectionPhotos() {
  if (!hasCollectionAvailabilityScope() || collectionOfflineTotal <= 0) return;
  showOfflineCollectionPhotos = !showOfflineCollectionPhotos;
  renderOfflineCollectionNotice();
  resetAndLoad({ preserveCollection: true });
}

/* Mutation kinds, as declared per filter field in vireo/filter_fields.py
   (``changed_by``). A membership-change refresh names what it just changed so
   the filter bar can answer whether the active expression can even notice —
   adding a keyword cannot change which photos a "Rating >= 3" filter matches,
   and reloading for it costs the user their place in the grid and their
   selection for nothing. */
var MUTATION_KEYWORD = 'keyword';
var MUTATION_PREDICTION = 'prediction';
var MUTATION_WILDLIFE = 'wildlife_excluded';

/* Sorts whose key is the photo's top prediction confidence — mirrors
   _PREDICTION_CONFIDENCE_SORTS in vireo/db.py. */
var PREDICTION_CONFIDENCE_SORTS = ['prediction_confidence',
  'prediction_confidence_asc'];

/* Whether the sort DROPDOWN is set to a confidence sort. Deliberately not
   "the grid is ordered by confidence": a healthy visual clause keeps results
   similarity-ranked while this still reads prediction_confidence. Only the
   reload heuristic below may use it — a card that wants to describe its own
   badge reads the server's per-card
   ``prediction_confidence_is_stack_lead`` flag instead. */
function sortSelectRanksOnPredictionConfidence() {
  var el = document.getElementById('sortSelect');
  return !!el && PREDICTION_CONFIDENCE_SORTS.indexOf(el.value) !== -1;
}

function refreshActiveCollectionAfterMembershipChange(mutations) {
  if (activeCollectionId) {
    filterByCollection(activeCollectionId, { preserveAnchor: false });
    return;
  }
  // A collection opened into the filter bar carries a static rules snapshot
  // (the saved photo_ids list plus any user-picked chips). loadCollections()
  // refreshes the sidebar counts but the bar keeps the STALE photo_ids
  // rule until the user manually reopens the collection, so an add/remove
  // just made from Browse doesn't show up in the current grid (CodeRabbit
  // review r3620473562). Refetch the collection metadata and reopen so
  // the bar's photo_ids expression matches the fresh membership.
  if (openedCollectionId) {
    var reopenId = openedCollectionId;
    // Ensure filterByCollection sees the freshest rules/visual_json, not
    // whatever the last renderCollectionList cached; loadCollections
    // (called by callers of this function) refreshes collectionsById via
    // loadCollectionCounts, but that's async — sequence with a fetch.
    safeFetch('/api/collections', {}, { toast: false }).then(function(list) {
      if (Array.isArray(list)) {
        list.forEach(function(c) { collectionsById[c.id] = c; });
      }
      // The user can switch scope (folder click, keyword click, filter
      // change) while this fetch is in flight; those paths clear
      // ``openedCollectionId``. Reopening unconditionally would reinstate
      // the old collection and wipe the user's new scope (Codex review
      // r3622743705). Only reopen if the same collection is still open.
      if (openedCollectionId === reopenId) {
        filterByCollection(reopenId, { preserveAnchor: false });
      }
    }).catch(function() {
      // Even without the refresh, opening the cached copy is closer to
      // right than leaving the stale expression in place — but still
      // only if the user hasn't already moved on.
      if (openedCollectionId === reopenId) {
        filterByCollection(reopenId, { preserveAnchor: false });
      }
    });
    return;
  }
  // A prediction edit moves a photo under the prediction-confidence sorts
  // even with no filter active, so the filter-only checks below would skip
  // the reload the grid needs. Rejecting the top pick promotes the next
  // guess (or leaves the photo unscored); accepting a runner-up rejects the
  // old top pick. Either way the photo's sort key AND the number its badge
  // shows have changed, and without reloading the grid keeps the old order
  // and the old score until a manual re-sort (Codex P2 on PR #1670). Same
  // options as onSortChanged(), which is the same situation: the order moved
  // under the photo the user is looking at, so hold onto it.
  //
  // Reading the dropdown rather than what the server actually ranked by is
  // deliberate here: with a healthy visual clause the order will not move,
  // but every affected card's badge number still did, so the reload is
  // wanted either way.
  var predictionEdit = !mutations ||
    mutations.indexOf(MUTATION_PREDICTION) !== -1;
  if (predictionEdit && sortSelectRanksOnPredictionConfidence()) {
    // A batch spanning several cards has no single-photo anchor. Keep its
    // grid window and surviving selection so accepting a species can be
    // followed by another edit on those same photos.
    reloadBrowseResults(selectedPhotos.size > 0
      ? { preserveScroll: true }
      : { preserveAnchor: true, focusAnchor: true });
    return;
  }
  // Collections open into the filter bar now (Phase 5): when a membership
  // change (tag/untag, add/remove) happens while an expression is active,
  // re-evaluate it so photos that no longer match leave the grid — but only
  // when the expression reads something this edit can actually move. A
  // caller that doesn't say what it changed is treated as "anything".
  if (!window.VireoFilter || !VireoFilter.hasFilters()) return;
  if (mutations && VireoFilter.dependsOnMutation &&
      !VireoFilter.dependsOnMutation(mutations)) {
    return;
  }
  resetAndLoad({ preserveAnchor: false, preserveScroll: true });
}

function refreshBrowseSidebarCounts() {
  loadFolders();
  loadKeywords();
  loadCollectionCounts();
}

async function filterByCollection(id, options) {
  // The sidebar collection list is rendered from the initial /photos payload,
  // so a slow /api/filters/fields (or workspace) round-trip leaves clickable
  // collections in the DOM before VireoFilter.init resolves. Without this
  // guard, the saved expression is pushed into the filter bar while
  // state.fields is still null and the in-flight init's restorePersisted/
  // finish then overwrites the just-loaded rules/visual clause. Do not drop
  // the click, though: wait for the in-flight init and apply the collection
  // as soon as the filter bar is ready. The bootstrap deep-link caller fires
  // from inside browseFilterInitPromise.then(...), so it proceeds immediately.
  var myScopeGen = ++browseScopeGen;
  if (window.VireoFilter && !VireoFilter.isReady()) {
    if (!browseFilterInitPromise) return;
    try {
      await browseFilterInitPromise;
    } catch (e) {
      return;
    }
    if (!VireoFilter.isReady()) return;
    // A later sidebar click (folder/keyword/collection) advanced the scope
    // generation while we were waiting for filter-bar init. Applying this
    // stale collection now would clobber the user's newer selection
    // (Codex review r3624395785).
    if (browseScopeGen !== myScopeGen) return;
  }
  // Guard: degraded (count_error) collections can't resolve their rules.
  var collectionMeta = collectionsById[id];
  if (!collectionMeta) {
    // Programmatic callers (deep links, tests, refresh paths) can race the
    // sidebar render — fetch the list before giving up.
    await loadCollections();
    collectionMeta = collectionsById[id];
  }
  if (collectionMeta && collectionMeta.count_error) {
    if (typeof showToast === 'function') {
      showToast(
        (collectionMeta.name || 'This collection') +
          " is unavailable \u2014 its rules could not be resolved. Right-click \u2192 Edit Rules to fix it.",
        'error'
      );
    }
    return;
  }
  if (!collectionMeta) return;
  // Opening a collection loads its saved expression into the filter bar as
  // editable chips (rules + visual clause round-trip). The expression IS
  // the filter; loadExpression's onChange reload refreshes the grid with
  // anchor preservation handled by the shared path.
  activeFolderId = null;
  activeKeyword = null;
  activeCollectionId = null;
  dashboardCollectionScope = false;
  clearOfflineCollectionState();
  // Track which collection is currently loaded so post-membership refresh
  // can pull fresh rules/photo_ids without waiting for a user reopen.
  openedCollectionId = id;
  var rules;
  var visual;
  try { rules = JSON.parse(collectionMeta.rules || '[]'); } catch (e) { rules = []; }
  try {
    visual = collectionMeta.visual_json ? JSON.parse(collectionMeta.visual_json) : null;
  } catch (e) { visual = null; }
  // Membership-refresh callers pass {preserveAnchor: false} to avoid paging
  // through the whole refreshed collection looking for a photo that may
  // have just left it; propagate via a distinct reason so the shared
  // onChange handler skips anchor preservation for that path (Codex review
  // r3622521603).
  var preserveAnchor = !(options && options.preserveAnchor === false);
  VireoFilter.loadExpression(rules, visual, {
    reason: preserveAnchor ? 'expressionLoaded' : 'expressionRefreshed',
  });
}
