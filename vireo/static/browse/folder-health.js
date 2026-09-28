/* Browse: refreshing Browse when folder health (missing/restored) changes.
   Classic script sharing page globals; browse.html loads the
   browse/*.js files in dependency order. */

// ``folderHealthRefreshSeq`` is initialized above the bootstrapBrowse()
// invocation so the pre-await snapshot inside bootstrap reads ``0`` instead
// of ``undefined`` (Codex review r3686317674).
function folderRowsContainScope(rows, folderId, scopeId) {
  var byId = {};
  (rows || []).forEach(function(row) { byId[row.id] = row; });
  var currentId = folderId;
  var seen = {};
  while (currentId != null && !seen[currentId]) {
    if (currentId === scopeId) return true;
    seen[currentId] = true;
    var row = byId[currentId];
    currentId = row ? row.parent_id : null;
  }
  return false;
}

function folderHealthTouchesActiveScope(detail, beforeRows, afterRows) {
  if (!activeFolderId) return true;
  var restored = detail && Array.isArray(detail.restored) ? detail.restored : [];
  var wentMissing = detail && Array.isArray(detail.wentMissing) ? detail.wentMissing : [];
  var changedIds = restored.concat(wentMissing);
  // The null-baseline fallback intentionally carries no ids. Conservatively
  // reload because the server only knows that this workspace changed.
  if (!changedIds.length) return true;
  return changedIds.some(function(folderId) {
    return folderRowsContainScope(beforeRows, folderId, activeFolderId) ||
      folderRowsContainScope(afterRows, folderId, activeFolderId);
  });
}

async function refreshBrowseAfterFolderHealthChange(detail) {
  var seq = ++folderHealthRefreshSeq;
  // A health-driven dataset refresh is not a user scope change: bumping
  // browseScopeGen here would make any in-flight sidebar click
  // (filterByFolder/filterByKeyword/filterByCollection) or the initial
  // collection deep-link replay look stale, and its own guard would then
  // silently discard the requested scope — leaving Browse on the workspace
  // grid instead of the user's newer selection (Codex review r3684907518).
  // folderHealthRefreshSeq below is the only counter this refresh needs.

  // Apply the effective ``perPage`` (and other cfg values) before any
  // loadPhotos call. Without this, a health event that fires while
  // ``_cfgPromise`` is still pending calls resetAndLoad → loadPhotos with
  // the hard-coded default (50), and bootstrap later applies the configured
  // size — its own init response is discarded by the health-generation
  // guard, but the perPage variable it wrote is not, so subsequent
  // pagination computes offsets against the new size while the health
  // refresh loaded page 1 with the old size: photos are silently skipped
  // or duplicated across the boundary (Codex review r3686912883).
  // applyBrowseConfig is idempotent — safe to call again after bootstrap.
  try { applyBrowseConfig(await _cfgPromise); } catch (e) {}
  if (seq !== folderHealthRefreshSeq) return;

  // Refresh the scope controls first. If the selected folder just went
  // offline it disappears from /api/folders; clear that stale scope before
  // reloading the grid so Browse falls back to the available workspace.
  //
  // Pass ``shouldRender`` into each loader: without it, two overlapping
  // health events (a folder flapping, or the modal check racing the
  // ten-minute poll) can complete out of order — the older fetch's
  // render*Tree call would clobber the newer render, and the post-await
  // seq check would fire too late to undo the DOM mutation. Guarding
  // *before* render means only the winning generation's data ever reaches
  // the tree (Codex review r3685193225).
  var isCurrent = function() { return seq === folderHealthRefreshSeq; };
  var beforeFolderRows = browseFolderRows.slice();
  var loaded = await Promise.all([
    loadFolders({ shouldRender: isCurrent }),
    loadKeywords({ shouldRender: isCurrent }),
    loadCollections({ shouldRender: isCurrent }),
  ]);
  if (!isCurrent()) return;
  // If ``/api/folders`` failed transiently during this health transition,
  // the folder tree DOM (and the cached ``browseFolderRows``) are still
  // the pre-transition state — a membership check against either would
  // preserve a folder that just went missing, and ``resetAndLoad`` would
  // then reload an empty folder-scoped grid. Because the navbar has
  // already advanced ``_missingFoldersLastIds`` before dispatching this
  // event, later polls see no transition and never repair the view, so
  // schedule a retry via the same event listener rather than committing
  // to a decision from stale data (Codex review r3687062925).
  if (loaded[0] === null) {
    var retryAttempt = detail && Number(detail.refreshRetryAttempt) || 0;
    // Bound the short-term recovery loop. A prolonged/permanent endpoint
    // outage must fall back to the normal ten-minute health poll instead of
    // issuing folder/keyword/collection requests forever.
    if (retryAttempt >= 3) {
      // The navbar advanced ``_missingFoldersLastIds`` before dispatching
      // this event, so its 10-minute poll will see unchanged IDs once
      // ``/api/folders`` recovers and by default emits nothing — the page
      // would then remain on pre-transition data indefinitely. Flag the
      // navbar so the next successful poll fires a synthetic reconciliation
      // event regardless of the ID diff, letting this handler re-attempt
      // the refresh with a healthy endpoint (Codex review r3687331927).
      if (typeof window.markMissingFoldersReconciliationPending === 'function') {
        window.markMissingFoldersReconciliationPending();
      }
      return;
    }
    var retryDetail = Object.assign({}, detail || {}, {
      source: 'refresh-retry',
      refreshRetryAttempt: retryAttempt + 1
    });
    setTimeout(function() {
      document.dispatchEvent(new CustomEvent('vireo:folder-health-changed', {
        // Preserve restored/wentMissing ids so the successful retry can still
        // distinguish an unrelated sibling transition from one that affects
        // the active folder scope. Dropping them makes the conservative
        // no-id fallback reset an unaffected grid and selection.
        detail: retryDetail
      }));
    }, 2000 * Math.pow(2, retryAttempt));
    return;
  }
  // Folder health can change which visible ancestor represents a hidden
  // local session. Refresh the authoritative local-folder payload after the
  // fresh tree has rendered so recovery badges move to the correct ancestor
  // (or synthesize a top-level row) immediately. Encoding folder health in
  // the 15-second blocker fingerprint either misses non-mapped ancestors or
  // makes that lightweight polling payload grow with catalog size.
  if (window.vireoLocalFolders &&
      typeof window.vireoLocalFolders.load === 'function') {
    window.vireoLocalFolders.load();
  }
  // Use the health-owned response rather than the DOM. A later unrelated
  // loadFolders() call may supersede this render while still being in flight;
  // in that window the DOM is intentionally old even though this response
  // already proves whether the selected folder is available.
  var currentFolderRows = loaded[0];
  var activeScopeWasAffected = folderHealthTouchesActiveScope(
    detail, beforeFolderRows, currentFolderRows);
  if (activeFolderId &&
      !currentFolderRows.some(function(folder) { return folder.id === activeFolderId; })) {
    activeFolderId = null;
    activeScopeWasAffected = true;
  }

  // A transition in an unrelated folder cannot change a leaf-folder result
  // set. Keep its selection, detail panel, and scroll position intact while
  // still refreshing workspace-wide sidebar and summary counts.
  if (!activeScopeWasAffected && browseDatasetReady) {
    loadCollectionCounts();
    loadSummary();
    if (timelineMode) loadCalendarData();
    return;
  }

  // Preserve the active collection: resetAndLoad's default is to clear
  // ``activeCollectionId`` for non-dashboard scopes, which would kick a user
  // viewing a normal collection back to the unscoped workspace grid whenever
  // folder health changes (CodeRabbit review r3684913393).
  var gridReloadStatus = await resetAndLoad({
    preserveCollection: true,
    preserveAnchor: true,
    // Health can move photos in and out of the result set, so let the
    // server place the anchor rather than paging towards where it used to
    // be (Codex P1 on PR #1695).
    focusAnchor: true
  });
  if (!isCurrent()) return;
  if (gridReloadStatus === false) {
    // resetAndLoad has already cleared the pre-transition grid. Keep the
    // navbar reconciliation marker set so an unchanged normal poll retries
    // after a transient first-page query/collection failure instead of
    // leaving Browse empty indefinitely.
    if (typeof window.markMissingFoldersReconciliationPending === 'function') {
      window.markMissingFoldersReconciliationPending();
    }
    return;
  }
  loadCollectionCounts();
  loadSummary();
  if (timelineMode) loadCalendarData();
}

// Track the currently-running refresh so awaiters (the photo deep-link
// loader in particular) can wait for it to settle before re-snapshotting
// their generation counter and retrying. Without this, a health event
// that fires while ``?photo_id=…`` is awaiting ``_cfgPromise`` /
// ``/api/photos/<id>`` / ``/api/browse/init`` used to cancel the deep
// link permanently — the refresh has no idea what folder the target
// photo lives in, so Browse fell back to the unscoped workspace grid
// without scrolling to the requested photo (and VireoFilter was left
// uninitialized) (Codex review r3686778061).
var _activeFolderHealthRefresh = Promise.resolve();

async function waitForFolderHealthRefreshesToSettle() {
  // A second event can replace the active promise while an awaiter is still
  // waiting on the first. Keep observing until the promise that just settled
  // is still the active one, so photo deep-link retries cannot resume in the
  // gap between overlapping refreshes.
  while (true) {
    var observed = _activeFolderHealthRefresh;
    try { await observed; } catch (e) { /* keep draining newer refreshes */ }
    if (observed === _activeFolderHealthRefresh) return;
  }
}
