// The filter bar: which duplicate groups match the active filters (a
// matching member reveals its whole group), the render that waits for a
// filter refresh in flight, and the filter-restricted view of a bucket.
// Classic page script.

async function refreshFilterMatches() {
  var myEpoch = ++_filterEpoch;
  var settlePending = function() {};
  /** @type {Promise<void>} */
  var thisRefresh = new Promise(function(resolve) { settlePending = function() { resolve(); }; });
  _pendingFilterRefresh = thisRefresh;
  try {
    var hasFilters = window.VireoFilter && VireoFilter.hasFilters && VireoFilter.hasFilters()
      && !VireoFilter.isMuted();
    var visualInfo = null;
    var nextMatchedIds = null;
    if (hasFilters) {
      try {
        /** @type {{rules: unknown, ids_only: boolean, visual?: unknown}} */
        var body = { rules: VireoFilter.getRules(), ids_only: true };
        var visual = VireoFilter.getVisual ? VireoFilter.getVisual() : null;
        if (visual) body.visual = visual;
        var data = /** @type {{ids?: number[], visual?: unknown}} */ (await safeFetch('/api/photos/query', {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify(body),
        }, { toast: false }));
        if (myEpoch !== _filterEpoch) return;  // superseded — drop
        nextMatchedIds = new Set(data.ids || []);
        visualInfo = data.visual || null;
      } catch (e) {
        if (myEpoch !== _filterEpoch) return;  // superseded — drop
        // Failing the query (400 on a stale/deep-linked rule, transient
        // network error) must NOT null out _filterMatchedIds — the old
        // behaviour let ``groupMatchesFilter`` treat null as "no filters
        // active" and widen the render to the entire scan while the chips
        // stayed on-screen, silently re-enabling resolve/bulk controls for
        // groups the user meant to exclude. Empty set instead so every
        // group correctly fails the filter and the "N hidden" empty-state
        // renders.
        nextMatchedIds = new Set();
        if (typeof showToast === 'function') {
          showToast('Filter query failed — check the rules or your connection', 'error');
        }
      }
    }
    _filterMatchedIds = nextMatchedIds;
    if (window.VireoFilter && VireoFilter.setResultTotal) {
      VireoFilter.setResultTotal(_filterMatchedIds ? _filterMatchedIds.size : null);
    }
    // Surface the visual clause's status so the filter bar's visual chip
    // can warn on fallback. Without this, a Duplicates filter with an
    // unresolvable visual clause (no model / no embeddings / encoding
    // failed) would show the chip while groups + bulk actions ran on the
    // broadened metadata-only match set.
    if (window.VireoFilter && VireoFilter.setVisualInfo) {
      VireoFilter.setVisualInfo(visualInfo);
    }
    // Never re-render a cached snapshot while a new scan is in flight.
    // ``renderResults`` hides the progress UI and re-enables the Scan
    // button, so filter edits (or pause/resume) mid-scan would otherwise
    // resurrect stale duplicate cards + actions and even let the user
    // kick off a second overlapping scan.
    if (_lastScanResult && !_scanInProgress) renderResults(_lastScanResult);
    // Release ``tryRestoreLastScan`` — the first pass has now set
    // ``_filterMatchedIds`` (or left it null when no filters are active),
    // so restoring a cached scan can safely render.
    if (_filterReadyResolve) { _filterReadyResolve(); _filterReadyResolve = null; }
  } finally {
    // Only clear the pointer if a newer refresh hasn't already claimed
    // it — otherwise we'd drop the tracker for the newest in-flight
    // query and defeat the mid-scan wait in ``renderScanWhenFilterReady``.
    if (_pendingFilterRefresh === thisRefresh) _pendingFilterRefresh = null;
    settlePending();
  }
}

// Await any in-flight ``refreshFilterMatches`` before rendering a
// just-completed scan. Loops so a filter edit that starts DURING the
// wait (replacing ``_pendingFilterRefresh``) is also awaited — without
// this the initial post-scan render would use the older refresh's
// ``_filterMatchedIds`` and the newer refresh's line-576 re-render
// would show a brief flash of the outdated filter state.
/** @param {DuplicateScan | null | undefined} result */
async function renderScanWhenFilterReady(result) {
  while (_pendingFilterRefresh) {
    var current = _pendingFilterRefresh;
    try { await current; } catch (e) { /* handled inside refresh */ }
    if (_pendingFilterRefresh === current) break;
  }
  renderResults(/** @type {DuplicateScan} */ (result || {}));
}

// Return a filter-restricted view of a bucket. When ``_filterMatchedIds``
// is active, keep only file_hashes whose proposal matches the filter,
// and recompute group_count/total_size/example_filenames from that
// subset. Returns ``null`` when no groups survive the filter. Callers
// that render the Bulk-decide card AND ``bulkResolveByFolder`` MUST
// both go through this — otherwise clicking "Keep X for all" would
// send the raw ``bucket.file_hashes`` to
// ``/api/duplicates/bulk-resolve``, rejecting (and with delete enabled,
// trashing) duplicate groups the active filter had removed from view.
/**
 * @param {DuplicateBucket | undefined} bucket
 * @returns {DuplicateBucket | null}
 */
function visibleBucket(bucket) {
  if (!bucket) return null;
  if (!_filterMatchedIds && _duplicateCategory === 'all') return bucket;
  var proposals = (_lastScanResult && _lastScanResult.proposals) || [];
  /** @type {Record<string, DuplicateProposal>} */
  var proposalsByHash = {};
  proposals.forEach(function(p) { proposalsByHash[p.file_hash] = p; });
  var keptHashes = (bucket.file_hashes || []).filter(function(h) {
    var p = proposalsByHash[h];
    return p && groupMatchesFilter(p);
  });
  if (keptHashes.length === 0) return null;
  if (keptHashes.length === (bucket.file_hashes || []).length) return bucket;
  var keptProposals = keptHashes.map(function(h) { return /** @type {DuplicateProposal} */ (proposalsByHash[h]); });
  var newTotalSize = keptProposals.reduce(function(n, p) {
    return n + ((p.losers || []).length * (p.winner && p.winner.file_size || 0));
  }, 0);
  var newExamples = keptProposals.slice(0, 3).map(function(p) {
    return p.winner && p.winner.filename || '';
  });
  // Recompute folders from the kept subset — mirrors ``bucket_unresolved_proposals``:
  // include the parent directory of every candidate (winner + losers) that still
  // exists on disk. Without this, ``renderBucket`` would keep offering a
  // ``Keep <folder> for all`` button for a folder that only appears in
  // filtered-out groups, and ``bulkResolveByFolder`` would then post those
  // visible hashes with a ``keep_folder`` value none of them contain — silently
  // skipping every remaining group.
  /** @type {Record<string, boolean>} */
  var keptFolderSet = {};
  keptProposals.forEach(function(p) {
    /** @type {(DuplicatePhoto | undefined)[]} */
    var cands = [p.winner].concat(p.losers || []);
    cands.forEach(function(c) {
      // Offline copies are unknown, not gone: still count their folder.
      if (!c || (c.exists === false && !c.volume_offline)) return;
      var path = c.path || '';
      var sep = Math.max(path.lastIndexOf('/'), path.lastIndexOf('\\'));
      if (sep <= 0) return;
      keptFolderSet[path.substring(0, sep)] = true;
    });
  });
  var newFolders = (bucket.folders || []).filter(function(f) {
    return keptFolderSet[f];
  });
  return Object.assign({}, bucket, {
    file_hashes: keptHashes,
    group_count: keptHashes.length,
    total_size: newTotalSize,
    example_filenames: newExamples,
    folders: newFolders,
  });
}
