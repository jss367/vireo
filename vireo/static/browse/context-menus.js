/* Browse: right-click menus for grid cards, folders and collections.
   Classic script sharing page globals; browse.html loads the
   browse/*.js files in dependency order. */

/* ---------- Right-click context menu on grid cards ---------- */
// Single-photo wrappers around the existing batch endpoints. The detail-panel
// helpers (setFlag, setColorLabel) are coupled to selectedPhotoId; the context
// menu needs to target an arbitrary id without mutating that focus, so we
// post directly to the batch endpoints with a 1-photo list.
// Refresh the batch inspector if it's currently rendered, else fall back to
// the caller-supplied single-photo update. Prevents context-menu edits on a
// multi-selection anchor from collapsing the panel back to a single-detail
// view via loadDetail(anchorId) while the user still has N photos selected.
function _refreshInspectorAfterSinglePhotoEdit(photoId, singleFallback) {
  var detail = document.getElementById('detailContent');
  if (detail && detail.classList.contains('batch-mode')) {
    // Selection is unchanged — this is a state-only refresh after a
    // context-menu edit. Preserve any in-progress location input.
    renderBatchInspector(getActiveSelection(), { preserveLocation: true });
    return;
  }
  if (selectedPhotoId === photoId) singleFallback();
}

async function setRatingFor(photoId, rating) {
  try {
    await safeFetch('/api/batch/rating', {
      method: 'POST', headers: {'Content-Type': 'application/json'},
      body: JSON.stringify({photo_ids: [photoId], rating: rating}),
    });
  } catch(e) { return; }
  var p = findBrowsePhoto(photoId);
  if (p) p.rating = rating;
  await reconcileBrowseStackCovers([photoId]);
  refreshGridCards([photoId]);
  refreshExpandedBrowseStackMembers([photoId]);
  scheduleCollectionCountsRefresh();
  _refreshInspectorAfterSinglePhotoEdit(photoId, function() { loadDetail(photoId); });
  refreshPendingSyncBanner();
}

async function setFlagFor(photoId, flag) {
  try {
    await safeFetch('/api/batch/flag', {
      method: 'POST', headers: {'Content-Type': 'application/json'},
      body: JSON.stringify({photo_ids: [photoId], flag: flag}),
    });
  } catch(e) { return false; }
  var p = findBrowsePhoto(photoId);
  if (p) p.flag = flag;
  _clearRepresentativeStateIfIneligible(photoId, flag);
  await reconcileBrowseStackCovers([photoId]);
  refreshGridCards([photoId]);
  refreshExpandedBrowseStackMembers([photoId]);
  _refreshInspectorAfterSinglePhotoEdit(photoId, function() { updateDetailFlagButtons(flag); });
  scheduleCollectionCountsRefresh();
  return true;
}

async function setColorLabelFor(photoId, color) {
  _noteColorLabelEdits([photoId]);
  try {
    await safeFetch('/api/batch/color_label', {
      method: 'POST', headers: {'Content-Type': 'application/json'},
      body: JSON.stringify({photo_ids: [photoId], color: color}),
    });
  } catch(e) { _recoverColorLabelsAfterFailedWrite([photoId]); return; }
  _noteColorLabelEdits([photoId]);
  if (color) colorLabels[photoId] = color;
  else delete colorLabels[photoId];
  colorLabelsFetched.add(photoId);
  refreshGridCards([photoId]);
  refreshExpandedBrowseStackMembers([photoId]);
  _refreshInspectorAfterSinglePhotoEdit(photoId, updateDetailColors);
  scheduleCollectionCountsRefresh();
}

function revealPhoto(photoId) {
  safeFetch('/api/files/reveal', {
    method: 'POST', headers: {'Content-Type': 'application/json'},
    body: JSON.stringify({photo_id: photoId}),
  }, { toast: false }).then(function(data) {
    if (typeof showRevealFeedback === 'function') showRevealFeedback(data);
  }).catch(function(err) {
    if (typeof showToast === 'function') {
      showToast('Reveal failed: ' + (err.message || 'request failed'), 'error');
    } else {
      console.error('revealPhoto failed', err);
    }
  });
}

function viewPhotoOnMap(photoId) {
  window.location.href = '/map?photo_id=' + encodeURIComponent(photoId);
}

// One photo opens the whole map focused on its marker; several open a map of
// just those photos. The ids travel in sessionStorage because a large
// selection would not fit in a URL.
function viewPhotosOnMap(photoIds) {
  if (photoIds.length === 1) {
    viewPhotoOnMap(photoIds[0]);
    return;
  }
  try {
    sessionStorage.setItem('vireoMapSelection', JSON.stringify({
      photo_ids: photoIds.map(Number),
    }));
  } catch (e) {
    // sessionStorage's per-origin quota (~5MB) only runs out for selections
    // of hundreds of thousands of photos.
    showToast(
      'Could not open ' + photoIds.length.toLocaleString() +
        ' photos on the map: the selection is too large for the browser to hand over. Select fewer photos.',
      'error'
    );
    return;
  }
  window.location.href = '/map?source=selection';
}

async function copyPhotoPaths(photoIds) {
  var settled = await Promise.allSettled(photoIds.map(function(id) {
    return safeFetch('/api/photos/' + id, {}, { toast: false });
  }));
  var paths = settled
    .filter(function(s) { return s.status === 'fulfilled' && s.value && s.value.path; })
    .map(function(s) { return s.value.path; });
  var failed = settled.length - paths.length;
  if (!paths.length) {
    if (failed > 0) {
      console.warn('copyPhotoPaths: ' + failed + ' path(s) failed');
      showToast(
        failed === 1 ? '1 path could not be copied' : failed + ' paths could not be copied',
        'error'
      );
    }
    return;
  }
  if (navigator.clipboard) {
    try {
      await navigator.clipboard.writeText(paths.join('\n'));
      if (failed > 0) {
        showToast(
          paths.length + ' of ' + photoIds.length + ' paths copied; ' +
            failed + ' could not be copied',
          'warning'
        );
      } else {
        showToast(photoIds.length === 1 ? 'Path copied' : 'Paths copied', 'success');
      }
    } catch (e) {
      console.error(e);
    }
  }
  if (failed > 0) {
    console.warn('copyPhotoPaths: ' + failed + ' path(s) failed');
  }
}

// Open a single grid photo straight in the in-app editor, skipping the
// lightbox. Hand off the current grid's ordered list so the editor can offer
// Prev/Next, mirroring the lightbox "Edit Photo" handoff.
function openPhotoEditor(photoId) {
  var navigationPhotos = browsePhotoNavigationList(photoId);
  if (window.vireoEditNav) {
    window.vireoEditNav.setList(navigationPhotos, photoId);
    window.vireoEditNav.setLastPhoto(photoId);
  }
  window.location.href = '/edit/' + photoId;
}

function buildPhotoContextMenu(photoIds, contextPhotoId) {
  var one = photoIds.length === 1;
  var hint = one ? undefined : 'Select a single photo';
  // A right-click identifies an unambiguous copy source even when that image
  // belongs to a multi-selection. The batch-bar More menu only has a source
  // when exactly one photo is selected.
  var developmentSourceId = contextPhotoId != null
    ? Number(contextPhotoId)
    : (one ? Number(photoIds[0]) : null);
  // Only a loaded photo's known status disables "View on Map"; anything else
  // goes to the map, which explains a photo it cannot place. A right-click on
  // an expanded stack member finds it through browseStackMembers, not the
  // top-level photos array, so look it up through findBrowsePhoto.
  var oneLoaded = one ? findBrowsePhoto(Number(photoIds[0])) : null;
  var noMapLocation = !!oneLoaded && oneLoaded.location_status === 'none';
  var copiedDevelopment = window.vireoEditNav
    ? window.vireoEditNav.getCopiedRecipe()
    : null;

  var rateChip = function(n) {
    return {
      label: n === 0 ? '\u2606' : String(n),
      title: n === 0 ? 'No rating' : 'Rate ' + n,
      onClick: function() {
        batchSetRating(n, photoIds);
      },
    };
  };
  var colorChip = function(c, icon, title) {
    return {
      label: icon,
      title: c && window.VireoColorLabels
        ? window.VireoColorLabels.title(c, title)
        : title,
      color: c,
      colorBaseTitle: title,
      onClick: function() {
        batchSetColorLabel(c, photoIds);
      },
    };
  };
  var flagChip = function(f, icon, title) {
    return {
      label: icon, title: title,
      onClick: function() {
        batchSetFlag(f, photoIds);
      },
    };
  };

  return [
    { chips: [0, 1, 2, 3, 4, 5].map(rateChip) },
    { chips: [
      colorChip(null, '\u25CB', 'No color'),
      colorChip('red', '\u25CF', 'Red'),
      colorChip('yellow', '\u25CF', 'Yellow'),
      colorChip('green', '\u25CF', 'Green'),
      colorChip('blue', '\u25CF', 'Blue'),
      colorChip('purple', '\u25CF', 'Purple'),
    ] },
    { chips: [
      flagChip('flagged', '\u2691', 'Flag as pick'),
      flagChip('rejected', '\u2715', 'Reject'),
      flagChip('none', '\u25CB', 'Unflag'),
    ] },
    { separator: true },
    { label: 'Find Similar', disabled: !one, disabledHint: hint,
      onClick: function() { if (typeof findSimilar === 'function') findSimilar(photoIds[0]); } },
    { label: 'View on Map', disabled: noMapLocation,
      disabledHint: 'No map coordinates: no EXIF GPS and no location linked to a place',
      onClick: function() { viewPhotosOnMap(photoIds); } },
    { label: 'Review on Map',
      onClick: function() { reviewLocationsForSelection(); } },
    { label: 'Add Locations by Capture Time',
      onClick: function() { reviewLocationsForSelection('time'); } },
    { label: 'Compare', disabled: photoIds.length < 2, disabledHint: 'Select at least two photos',
      onClick: function() { openBrowseCompare(); } },
    { label: 'Best Batch',
      onClick: function() { openBestBatchForIds(photoIds, photoIds[0]); } },
    { label: 'Review Burst', disabled: photoIds.length < 2, disabledHint: 'Select at least two photos',
      onClick: function() { openSelectedInBurstReview(); } },
    { separator: true },
    { label: 'Edit Photo', disabled: !one, disabledHint: hint,
      onClick: function() { openPhotoEditor(photoIds[0]); } },
    { label: 'Copy Development Settings',
      disabled: developmentSourceId == null,
      disabledHint: 'Right-click a photo to choose the settings to copy',
      onClick: function() { copyDevelopmentSettingsFromPhoto(developmentSourceId); } },
    { label: 'Paste Development Settings',
      disabled: _browseDevelopmentPasteInFlight || !(copiedDevelopment && copiedDevelopment.recipe),
      disabledHint: _browseDevelopmentPasteInFlight
        ? 'A development settings paste is already running'
        : 'Copy development settings from a photo first',
      onClick: function() { pasteEditSettingsToSelection(); } },
    { label: 'Develop',
      onClick: function() { developSelected(); } },
  ].concat(buildOpenInEditorMenuItems(photoIds)).concat([
    { label: window.VIREO_REVEAL_LABEL, disabled: !one, disabledHint: hint,
      onClick: function() { revealPhoto(photoIds[0]); } },
    { label: 'Copy Path',
      onClick: function() { copyPhotoPaths(photoIds); } },
    { separator: true },
    { label: 'Add Keyword\u2026', onClick: function() { batchAddKeyword(); } },
    { label: 'Add to Collection\u2026', onClick: function() { addToCollection(); } },
  ]).concat(window.buildSpeciesHighlightMenuItems(photoIds, {
    showFetchFallback: true,
  })).concat(window.buildSpeciesRepresentativeMenuItems(photoIds, {
    getPhoto: function(id) {
      return findBrowsePhoto(id);
    },
  })).concat([
    { label: 'Adjust Capture Time\u2026', onClick: function() { openCaptureTimeModal(); } },
    { separator: true },
    { label: 'Send to iNaturalist', onClick: function() { batchSubmitInat(); } },
    { label: 'Make Offline', onClick: function() { makeAvailableOffline(); } },
    { label: 'Prepare Full Resolution',
      onClick: function() { prepareFullResolutionSelection(photoIds); } },
    { label: 'Export\u2026', onClick: function() { openExportModal(); } },
    { label: 'Create Panorama\u2026', disabled: photoIds.length < 2 || photoIds.length > 12,
      disabledHint: 'Select 2–12 overlapping photos',
      onClick: function() { openPanoramaModal(photoIds); } },
    { separator: true },
    { label: 'Delete', onClick: function() { batchDelete(); } },
  ]);
}

// The More button on the batch bar opens the same menu as right-click on a
// card, anchored under the button. openContextMenu only reads clientX/Y and
// clamps to the viewport, so a synthetic event object is enough.
function openBatchMoreMenu(btn) {
  var ids = getActiveSelection();
  if (!ids.length) return;
  var r = btn.getBoundingClientRect();
  openContextMenu({ clientX: r.left, clientY: r.bottom + 4 },
                  buildPhotoContextMenu(ids));
}

// Document-level delegation: grids re-render on sort/filter/scroll, so
// per-card listeners would go stale.
document.addEventListener('contextmenu', function(e) {
  var card = e.target.closest('.grid-card, .browse-stack-member');
  if (!card || !card.dataset.id) return;
  // Ignore right-clicks that came from inside the lightbox or a modal; this
  // handler owns only grid-card context menus. Modal right-clicks are either
  // handled by their own delegation (lightbox) or fall through to the browser.
  if (e.target.closest('.vireo-ctx-menu')) return;
  if (card.classList.contains('offline')) return;
  e.preventDefault();
  var pid = parseInt(card.dataset.id, 10);
  // Finder-style coercion: right-click on an item outside the selection
  // replaces the selection with that one item. Fold selectedPhotoId in first
  // so a single-focus click doesn't get silently dropped.
  if (selectedPhotos.size === 0 && selectedPhotoId !== null) {
    selectedPhotos.add(selectedPhotoId);
  }
  // The "item" a collapsed stack card offers is the stack, so coercion
  // replaces the selection with every frame behind it unless they are all in
  // the selection already. A tray member coerces to itself: that is how a
  // single frame gets a context menu of its own.
  var stackIds = card.classList.contains('browse-stack-member')
    ? [pid]
    : browseSelectionIdsForClick(pid);
  var wholeStackSelected = stackIds.every(function(stackId) {
    return selectedPhotos.has(stackId);
  });
  var ids;
  if (stackIds.length > 1 && !wholeStackSelected) {
    // A selection change like any other: the generation has to move, or a
    // restore still waiting on stack hydration (undo/redo) considers itself
    // current and merges the pre-undo ids into the stack just chosen.
    // selectPhoto() and selectBrowseStackAll() bump it for the same reason.
    // Codex P2 on PR #1672.
    anchorRestoreEpoch++;
    selectedPhotos = new Set(stackIds);
    selectedPhotoId = null;
    selectedIndex = photos.findIndex(function(p) { return p.id === pid; });
    abandonDetailFocusForBatch();
    ids = stackIds;
  } else {
    var beforeCoercion = selectionIdsKey(Array.from(selectedPhotos));
    ids = coerceSelectionOnContext(selectedPhotos, pid);
    // Coercion replacing the selection is a selection change like any other,
    // so the generation moves with it — otherwise a restore still waiting on
    // stack hydration (undo/redo) considers itself current and merges the
    // pre-undo ids into the card just right-clicked. Only when it actually
    // changed: right-clicking inside the selection keeps it, and retiring a
    // pending restore for that would lose a selection nobody replaced.
    // Codex P2 on PR #1672.
    if (selectionIdsKey(ids) !== beforeCoercion) anchorRestoreEpoch++;
  }
  // If coercion replaced the set, align selectedPhotoId (so the detail panel
  // reflects the right-clicked photo) and selectedIndex (the Shift-range
  // anchor — a subsequent Shift-click must range-select from this card).
  if (ids.length === 1 && ids[0] === pid && selectedPhotoId !== pid) {
    selectedPhotoId = pid;
    selectedIndex = photos.findIndex(function(p) { return p.id === pid; });
    if (selectedIndex < 0) {
      var coverId = browseStackCoverIdForPhoto(pid);
      selectedIndex = photos.findIndex(function(p) { return p.id === coverId; });
    }
    loadDetail(pid);
  }
  // A right-click is a click: it is the card the user is pointing at, and it
  // is on screen by construction. Without this, a resize after right-clicking
  // a card would still be judged against wherever the *previous* selection was
  // scrolled to, and would refuse to keep this one visible.
  lastClickedPhotoId = pid;
  refreshCardSelectionVisuals();
  noteFocusedCardVisibility();
  updateBatchBar();
  openContextMenu(e, buildPhotoContextMenu(ids, pid));
});

/* ---------- Folder tree context menu ---------- */
function revealFolder(fid) {
  safeFetch('/api/files/reveal', {
    method: 'POST',
    headers: {'Content-Type': 'application/json'},
    body: JSON.stringify({folder_id: fid}),
  }, { toast: false }).then(function(data) {
    if (typeof showRevealFeedback === 'function') showRevealFeedback(data);
  }).catch(function(err) {
    if (typeof showToast === 'function') {
      showToast('Reveal failed: ' + (err.message || 'request failed'), 'error');
    } else {
      console.error('revealFolder failed', err);
    }
  });
}

async function copyFolderPath(fid) {
  try {
    var resp = await fetch('/api/folders/' + fid);
    if (!resp.ok) return;
    var folder = await resp.json();
    if (folder && folder.path) {
      await navigator.clipboard.writeText(folder.path);
    }
  } catch (err) {
    console.error('copyFolderPath failed', err);
  }
}

var folderWorkspacesRequestSeq = 0;

function hideFolderWorkspaces() {
  folderWorkspacesRequestSeq += 1;
  document.getElementById('folderWorkspacesModal').classList.remove('open');
}

async function showFolderWorkspaces(fid) {
  var requestSeq = ++folderWorkspacesRequestSeq;
  var modal = document.getElementById('folderWorkspacesModal');
  var path = document.getElementById('folderWorkspacesPath');
  var list = document.getElementById('folderWorkspacesList');
  path.textContent = '';
  list.textContent = '';
  var loading = document.createElement('div');
  loading.style.cssText = 'color:var(--text-secondary);font-size:13px;';
  loading.textContent = 'Loading workspaces…';
  list.appendChild(loading);
  modal.classList.add('open');

  try {
    var data = await safeFetch('/api/folders/' + fid + '/workspaces', {}, { toast: false });
    if (requestSeq !== folderWorkspacesRequestSeq) return;
    path.textContent = data.folder && data.folder.path ? data.folder.path : '';
    list.textContent = '';
    (data.workspaces || []).forEach(function(workspace) {
      var row = document.createElement('div');
      row.style.cssText = 'display:flex;align-items:center;gap:8px;padding:8px 10px;background:var(--bg-tertiary);border:1px solid var(--border-secondary);border-radius:5px;font-size:13px;';

      var name = document.createElement('span');
      name.style.cssText = 'flex:1;min-width:0;overflow:hidden;text-overflow:ellipsis;white-space:nowrap;';
      name.textContent = workspace.name;
      name.title = workspace.name;
      row.appendChild(name);

      if (workspace.is_root) {
        var rootBadge = document.createElement('span');
        rootBadge.style.cssText = 'font-size:10px;color:var(--text-faint);';
        rootBadge.textContent = 'Root';
        row.appendChild(rootBadge);
      }
      if (workspace.is_active) {
        var activeBadge = document.createElement('span');
        activeBadge.style.cssText = 'font-size:10px;color:var(--accent);font-weight:600;';
        activeBadge.textContent = 'Current';
        row.appendChild(activeBadge);
      }
      list.appendChild(row);
    });
    if (!list.children.length) {
      var empty = document.createElement('div');
      empty.style.cssText = 'color:var(--text-secondary);font-size:13px;';
      empty.textContent = 'This folder is not associated with any workspace.';
      list.appendChild(empty);
    }
  } catch (err) {
    if (requestSeq !== folderWorkspacesRequestSeq) return;
    list.textContent = '';
    var error = document.createElement('div');
    error.style.cssText = 'color:var(--danger);font-size:13px;';
    error.textContent = (err && err.message) || 'Could not load associated workspaces.';
    list.appendChild(error);
  }
}

function moveFolder(fid) {
  window.location.href = '/move?folder_id=' + encodeURIComponent(fid);
}

function workLocallyFolder(fid) {
  if (window.vireoLocalFolders) {
    window.vireoLocalFolders.stage(fid);
    return;
  }
  window.location.href = '/workspace#work-locally';
}

async function rescanFolder(fid) {
  showToast('Starting folder rescan...', 'info');
  try {
    var data = await safeFetch('/api/folders/' + fid + '/rescan', {
      method: 'POST',
      headers: {'Content-Type': 'application/json'},
      body: JSON.stringify({incremental: true}),
    }, { toast: false });
    var suffix = data && data.job_id ? ' (' + data.job_id + ')' : '';
    showToast('Folder rescan queued' + suffix + '.', 'success');
  } catch (err) {
    console.error('rescanFolder failed', err);
    showToast((err && err.message) || 'Could not start folder rescan.', 'error');
  }
}

async function removeWorkspaceRootFromBrowse(fid) {
  var folder = browseFolderRows.find(function(row) {
    return Number(row.id) === Number(fid);
  });
  if (!folder || !folder.is_workspace_root) return;

  // Bind the DELETE to the workspace this tree was rendered against, not
  // whatever ``/api/workspaces/active`` returns at click time. Workspace
  // activation is persisted globally, so another tab switching workspaces
  // after render would otherwise redirect the destructive call at the
  // wrong workspace (Codex review r3798912101).
  var wsId = browseWorkspaceId;
  if (wsId == null) {
    showToast(
      'Cannot remove the folder yet — the workspace context is still loading. Try again in a moment.',
      'error'
    );
    return;
  }

  var path = folder.path || folder.name || 'this folder';
  var msg = 'Remove "' + path + '" from this workspace?\n\n' +
    'Photos in this folder and its subfolders will no longer appear in this workspace, ' +
    'and predictions, collections, and pending changes referencing them will be hidden here too.\n\n' +
    'Photos and folders are not deleted. You can re-add the folder later to restore visibility.';
  if (!confirm(msg)) return;

  // Snapshot every folder ID within the removed root's subtree BEFORE the
  // DELETE mutates state. The URL cleanup below needs this so that a
  // descendant scope (opened via ``?folder_id=<descendant>``) is stripped
  // as well; without it, a subsequent reload restores the detached
  // descendant and loads an empty Browse view because the refreshed tree
  // no longer contains it (Codex review r3799361364).
  var removedSubtreeIds = (function collectSubtreeIds() {
    var childrenByParent = {};
    browseFolderRows.forEach(function(row) {
      var pid = row.parent_id;
      if (pid == null) return;
      var key = Number(pid);
      if (!childrenByParent[key]) childrenByParent[key] = [];
      childrenByParent[key].push(Number(row.id));
    });
    var ids = {};
    ids[Number(fid)] = true;
    var queue = [Number(fid)];
    while (queue.length) {
      var current = queue.shift();
      (childrenByParent[current] || []).forEach(function(childId) {
        if (!ids[childId]) {
          ids[childId] = true;
          queue.push(childId);
        }
      });
    }
    return ids;
  })();

  try {
    // Verify the workspace the tree was rendered against is still the
    // globally-active one. If another tab has switched workspaces since
    // render, refuse the DELETE and reload — running it silently against
    // the render-time workspace would then leave this tab holding a mixed
    // view (deleted folder gone from workspace A but the rest of the page
    // still showing A's data while the app is globally on B).
    var active = await safeFetch(
      '/api/workspaces/active', {}, { toast: false }
    );
    if (active && Number(active.id) !== Number(wsId)) {
      showToast(
        'The active workspace changed in another window. Reloading to sync.',
        'info'
      );
      window.location.reload();
      return;
    }

    await safeFetch('/api/workspaces/' + wsId + '/folders/' + fid, {
      method: 'DELETE',
    });

    // The DELETE succeeded, so strip any ``?folder_id=<id>`` from the
    // current URL when it points at a folder that will not survive the
    // removal. That includes the removed root, its visible descendants,
    // and any descendant already flagged ``missing`` before the DELETE —
    // ``browseFolderRows`` excludes missing rows, so a health refresh
    // that removed the descendant before the click would leave it out of
    // ``removedSubtreeIds`` and the URL would still restore an empty
    // scope on any subsequent reload (Codex reviews r3799300182,
    // r3799361364, and r3799431533). Treating "not currently visible in
    // ``browseFolderRows``" as also-strip covers the missing-descendant
    // case without pulling in catalog ancestry.
    try {
      var urlParams = new URLSearchParams(window.location.search);
      var urlFolderId = parseInt(urlParams.get('folder_id'), 10);
      if (!isNaN(urlFolderId)) {
        var stillVisible = !removedSubtreeIds[Number(urlFolderId)] &&
          browseFolderRows.some(function(row) {
            return Number(row.id) === Number(urlFolderId);
          });
        if (!stillVisible) {
          urlParams.delete('folder_id');
          var newSearch = urlParams.toString();
          var newUrl = window.location.pathname +
            (newSearch ? '?' + newSearch : '') +
            window.location.hash;
          window.history.replaceState({}, '', newUrl);
        }
      }
    } catch (urlErr) {
      console.warn('Could not strip folder_id from URL after removal', urlErr);
    }

    // Refresh the authoritative local-folder payload before rebuilding
    // the tree. Without this, ``window.vireoLocalFolderData`` still
    // carries the removed root when ``renderFolderTree`` runs, so
    // ``folderLocalStatuses`` synthesizes a phantom top-level row for it
    // and leaves the removed folder visible until the next blocker poll
    // (Codex review r3798912105). ``load()`` swallows fetch failures and
    // returns ``null``; rendering against the pre-DELETE
    // ``window.vireoLocalFolderData`` in that case would resurrect the
    // removed root as a phantom, so reload instead of rebuilding from
    // stale local state (Codex review r3799142116).
    if (window.vireoLocalFolders &&
        typeof window.vireoLocalFolders.load === 'function') {
      var localRefreshed = await window.vireoLocalFolders.load();
      if (localRefreshed === null) {
        window.location.reload();
        return;
      }
    }

    var loaded = await Promise.all([
      loadFolders(),
      loadKeywords(),
      loadCollections(),
    ]);
    // A transient folder-list failure leaves the old tree in place. Reloading
    // is safer than showing a removed root until the next health transition.
    if (loaded[0] === null) {
      window.location.reload();
      return;
    }
    if (activeFolderId && !loaded[0].some(function(row) {
      return Number(row.id) === Number(activeFolderId);
    })) {
      activeFolderId = null;
    }
    await resetAndLoad({ preserveCollection: !!activeCollectionId });
    loadCollectionCounts();
    loadSummary();
    if (timelineMode) loadCalendarData();
    showToast('Folder removed from this workspace.', 'success');
  } catch (err) {
    console.error('removeWorkspaceRootFromBrowse failed', err);
  }
}

document.addEventListener('contextmenu', function(e) {
  var ti = e.target.closest('.tree-item[data-folder-id]');
  if (!ti) return;
  // Don't fire if the right-click landed on another context menu.
  if (e.target.closest('.vireo-ctx-menu')) return;
  e.preventDefault();
  var fid = parseInt(ti.dataset.folderId, 10);
  if (isNaN(fid)) return;
  var items = [
    { label: 'Filter by this folder', onClick: function() { filterByFolder(fid); } },
    { separator: true },
    { label: window.VIREO_REVEAL_LABEL, onClick: function() { revealFolder(fid); } },
    { label: 'Copy Path', onClick: function() { copyFolderPath(fid); } },
    { label: 'Show Associated Workspaces…', onClick: function() { showFolderWorkspaces(fid); } },
    { separator: true },
    { label: 'Work Locally\u2026', onClick: function() { workLocallyFolder(fid); } },
    { label: 'Move\u2026', onClick: function() { moveFolder(fid); } },
    { label: 'Rescan this Folder', onClick: function() { rescanFolder(fid); } },
  ];
  if (ti.dataset.workspaceRoot === '1') {
    items.push({ separator: true });
    items.push({
      label: 'Remove from This Workspace\u2026',
      onClick: function() { removeWorkspaceRootFromBrowse(fid); },
    });
  }
  openContextMenu(e, items);
});

/* ---------- Collection sidebar context menu ---------- */
async function renameCollection(cid, currentName) {
  var next = window.prompt('Rename collection', currentName || '');
  if (next === null) return;             // user cancelled
  next = next.trim();
  if (!next || next === currentName) return;
  try {
    await safeFetch('/api/collections/' + cid, {
      method: 'PUT',
      headers: {'Content-Type': 'application/json'},
      body: JSON.stringify({name: next}),
    });
  } catch (err) {
    console.error('renameCollection failed', err);
    return;
  }
  await loadCollections();
  loadCollectionCounts();
}

async function duplicateCollection(cid) {
  try {
    await safeFetch('/api/collections/' + cid + '/duplicate', {
      method: 'POST',
      headers: {'Content-Type': 'application/json'},
      body: '{}',
    });
  } catch (err) {
    console.error('duplicateCollection failed', err);
    return;
  }
  await loadCollections();
  loadCollectionCounts();
}

async function deleteCollectionById(cid, name) {
  if (!window.confirm('Delete collection "' + (name || '') + '"?')) return;
  try {
    await safeFetch('/api/collections/' + cid, { method: 'DELETE' });
  } catch (err) {
    console.error('deleteCollectionById failed', err);
    return;
  }
  // If we were filtering by the deleted collection, clear the filter.
  // ``activeCollectionId`` covers dashboard-scope collection links; opened
  // collections now live under ``openedCollectionId`` (their saved
  // expression is loaded into the filter bar instead of being applied as a
  // scope), so we need to clear that path too — otherwise the sidebar row
  // disappears but the filter bar and grid keep applying the deleted
  // collection's expression until the user manually clears it (Codex
  // review r3621903982).
  if (activeCollectionId === cid) {
    activeCollectionId = null;
    resetAndLoad();
  } else if (openedCollectionId === cid) {
    openedCollectionId = null;
    if (VireoFilter.hasFilters()) VireoFilter.clearAll(true);
    resetAndLoad();
  }
  await loadCollections();
  loadCollectionCounts();
}

document.addEventListener('contextmenu', function(e) {
  var ti = e.target.closest('.tree-item[data-collection-id]');
  if (!ti) return;
  // Don't fire inside another menu, and don't collide with folder-tree delegate.
  if (e.target.closest('.vireo-ctx-menu')) return;
  if (ti.hasAttribute('data-folder-id')) return;
  e.preventDefault();
  var cid = parseInt(ti.dataset.collectionId, 10);
  if (isNaN(cid)) return;
  var nameEl = ti.querySelector('.collection-name');
  var name = nameEl ? nameEl.textContent : '';
  var meta = collectionsById[cid];
  var isUnavailable = !!(meta && meta.count_error);
  // Degraded collections can't be filtered (the /photos endpoint would 400)
  // and location review reads from the same collection rules — hide both while
  // keeping the edit/rename/duplicate/delete actions reachable so the user
  // can fix or discard the row.
  var items = [];
  if (!isUnavailable) {
    items.push({ label: 'Filter by this Collection',
      onClick: function() { filterByCollection(cid); } });
    if (!(collectionsById[cid] || {}).has_visual) {
      items.push({ label: 'Add Locations by Capture Time',
        onClick: function() { reviewLocationsForCollection(cid, 'time'); } });
    }
    if (name === 'GPS Without Location Keyword') {
      items.push({
        label: 'Review Photo Locations',
        onClick: function() { reviewLocationsForCollection(cid); },
      });
    }
  }
  var editActions = [
    { label: 'Edit Rules',
      onClick: function() { editCollection(cid); } },
    { label: 'Rename',
      onClick: function() { renameCollection(cid, name); } },
    { label: 'Duplicate',
      onClick: function() { duplicateCollection(cid); } },
    { separator: true },
    { label: 'Delete Collection',
      onClick: function() { deleteCollectionById(cid, name); } },
  ];
  // Only emit a leading separator when there are filter/review actions above it
  // to divide from — for degraded rows the menu starts at Edit Rules.
  if (items.length) items.push({ separator: true });
  items = items.concat(editActions);
  openContextMenu(e, items);
});
