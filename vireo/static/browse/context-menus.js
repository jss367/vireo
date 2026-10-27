/* Browse: right-click menus for grid cards, folders and collections. ES module (see
   browse/README.md). */
import { selection } from './selection-state.js';
import { _noteColorLabelEdits, state } from './state.js';
import {
  VireoColorLabels, VireoFilter, VireoReleaseOriginals, buildOpenInEditorMenuItems,
  buildSpeciesHighlightMenuItems, buildSpeciesRepresentativeMenuItems, closeContextMenu, coerceSelectionOnContext,
  editNav, findSimilar, localFolders, openContextMenu, openPanoramaModal, pendingArchives, revealLabel,
  safeFetch, showPendingArchiveForFolder, showRevealFeedback, showToast,
} from './shared.js';
import {
  _clearRepresentativeStateIfIneligible, addToCollection, batchAddKeyword, batchDelete,
  batchSetColorLabel, batchSetFlag, batchSetRating, batchSubmitInat, developSelected,
  openCaptureTimeModal, reviewLocationsForCollection, reviewLocationsForSelection,
} from './batch-actions.js';
import { openSelectedInBurstReview, updateBatchBar } from './batch-bar.js';
import { openBestBatchForIds } from './best-batch.js';
import { loadCalendarData } from './calendar.js';
import { editCollection } from './collection-editor.js';
import {
  abandonDetailFocusForBatch, loadDetail, loadSummary, updateDetailFlagButtons,
} from './detail.js';
import {
  _recoverColorLabelsAfterFailedWrite, refreshPendingSyncBanner, renderBatchInspector,
  updateDetailColors,
} from './edit-actions.js';
import { makeAvailableOffline, openExportModal, prepareFullResolutionSelection } from './export.js';
import { refreshGridCards } from './grid.js';
import { resetAndLoad } from './loading.js';
import { noteFocusedCardVisibility } from './scroll.js';
import {
  _browseDevelopmentPasteInFlight, copyDevelopmentSettingsFromPhoto, findBrowsePhoto,
  openBrowseCompare, pasteEditSettingsToSelection,
} from './selection-panel.js';
import { getActiveSelection, refreshCardSelectionVisuals, selectionIdsKey } from './selection.js';
import {
  filterByCollection, loadCollectionCounts, loadCollections, scheduleCollectionCountsRefresh,
} from './sidebar-collections.js';
import { filterByFolder, loadFolders } from './sidebar-folders.js';
import { loadKeywords } from './sidebar-keywords.js';
import {
  browsePhotoNavigationList, browseSelectionIdsForClick, browseStackCoverIdForPhoto,
  browseStackMemberIdsFor, reconcileBrowseStackCovers, refreshExpandedBrowseStackMembers,
} from './stacks.js';

/* ---------- Right-click context menu on grid cards ---------- */
// Single-photo wrappers around the existing batch endpoints. The detail-panel
// helpers (setFlag, setColorLabel) are coupled to selectedPhotoId; the context
// menu needs to target an arbitrary id without mutating that focus, so we
// post directly to the batch endpoints with a 1-photo list.
// Refresh the batch inspector if it's currently rendered, else fall back to
// the caller-supplied single-photo update. Prevents context-menu edits on a
// multi-selection anchor from collapsing the panel back to a single-detail
// view via loadDetail(anchorId) while the user still has N photos selected.
function _refreshInspectorAfterSinglePhotoEdit(/** @type {number} */ photoId, /** @type {() => void} */ singleFallback) {
  var detail = /** @type {HTMLElement} */ (document.getElementById('detailContent'));
  if (detail && detail.classList.contains('batch-mode')) {
    // Selection is unchanged — this is a state-only refresh after a
    // context-menu edit. Preserve any in-progress location input.
    renderBatchInspector(getActiveSelection(), { preserveLocation: true });
    return;
  }
  if (selection.focusedPhotoId === photoId) singleFallback();
}

export async function setRatingFor(/** @type {number} */ photoId, /** @type {number} */ rating) {
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

export async function setFlagFor(/** @type {number} */ photoId, /** @type {BrowsePhoto['flag']} */ flag) {
  try {
    await safeFetch('/api/batch/flag', {
      method: 'POST', headers: {'Content-Type': 'application/json'},
      body: JSON.stringify({photo_ids: [photoId], flag: flag}),
    });
  } catch(e) { return false; }
  var p = findBrowsePhoto(photoId);
  if (p) p.flag = flag;
  _clearRepresentativeStateIfIneligible(photoId, /** @type {BrowsePhoto['flag']} */ flag);
  await reconcileBrowseStackCovers([photoId]);
  refreshGridCards([photoId]);
  refreshExpandedBrowseStackMembers([photoId]);
  _refreshInspectorAfterSinglePhotoEdit(photoId, function() { updateDetailFlagButtons(flag); });
  scheduleCollectionCountsRefresh();
  return true;
}

export async function setColorLabelFor(/** @type {number} */ photoId, /** @type {string | null} */ color) {
  _noteColorLabelEdits([photoId]);
  try {
    await safeFetch('/api/batch/color_label', {
      method: 'POST', headers: {'Content-Type': 'application/json'},
      body: JSON.stringify({photo_ids: [photoId], color: color}),
    });
  } catch(e) { _recoverColorLabelsAfterFailedWrite([photoId]); return; }
  _noteColorLabelEdits([photoId]);
  if (color) state.colorLabels[photoId] = color;
  else delete state.colorLabels[photoId];
  state.colorLabelsFetched.add(photoId);
  refreshGridCards([photoId]);
  refreshExpandedBrowseStackMembers([photoId]);
  _refreshInspectorAfterSinglePhotoEdit(photoId, updateDetailColors);
  scheduleCollectionCountsRefresh();
}

export function revealPhoto(/** @type {number} */ photoId) {
  safeFetch('/api/files/reveal', {
    method: 'POST', headers: {'Content-Type': 'application/json'},
    body: JSON.stringify({photo_id: photoId}),
  }, { toast: false }).then(function(data) {
    showRevealFeedback(data);
  }).catch(function(err) {
    if (typeof showToast === 'function') {
      showToast('Reveal failed: ' + (err.message || 'request failed'), 'error');
    } else {
      console.error('revealPhoto failed', err);
    }
  });
}

function viewPhotoOnMap(/** @type {number} */ photoId) {
  window.location.href = '/map?photo_id=' + encodeURIComponent(photoId);
}

// One photo opens the whole map focused on its marker; several open a map of
// just those photos. The ids travel in sessionStorage because a large
// selection would not fit in a URL.
function viewPhotosOnMap(/** @type {number[]} */ photoIds) {
  if (photoIds.length === 1) {
    if (photoIds[0] != null) viewPhotoOnMap(photoIds[0]);
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

// Stack membership comes from the same projection as the visible grid.
// The old burst_id metadata column is not a reliable burst identifier.
export function filterToBrowseGroup(/** @type {number} */ photoId) {
  var coverId = browseStackCoverIdForPhoto(photoId);
  var cover = findBrowsePhoto(coverId == null ? photoId : coverId);
  var ids = browseStackMemberIdsFor(cover);
  if (!ids || !cover?.browse_stack) return;
  VireoFilter.selectPhotoGroup(ids,
    (cover.browse_stack.kind === 'burst' ? 'Burst' : 'Duplicate group') + ' containing ' + cover.filename);
}

export async function copyPhotoPaths(/** @type {number[]} */ photoIds) {
  var settled = await Promise.allSettled(photoIds.map(function(id) {
    return safeFetch('/api/photos/' + id, {}, { toast: false });
  }));
  var paths = settled
    .filter(function(s) { return s.status === 'fulfilled' && s.value && s.value.path; })
    .flatMap(function(s) { return s.status === 'fulfilled' && s.value?.path ? [s.value.path] : []; });
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
function openPhotoEditor(/** @type {number} */ photoId) {
  var navigationPhotos = browsePhotoNavigationList(photoId);
  var nav = editNav();
  if (nav) {
    nav.setList(navigationPhotos, photoId);
    nav.setLastPhoto(photoId);
  }
  window.location.href = '/edit/' + photoId;
}

export function buildPhotoContextMenu(/** @type {number[]} */ photoIds, /** @type {number | null} */ contextPhotoId, /** @type {HTMLElement | null | undefined} */ returnFocus = undefined) {
  // Capture the opener now: closeContextMenu removes the clicked menu item
  // before invoking its onClick, so document.activeElement would be body
  // (right-click) or an unrelated prior control by the time a batch dialog
  // read it. The caller passes the right-clicked grid card or the batch-bar
  // More button; both carry tabindex="-1" so the batch dialog's close path
  // can .focus() them on Escape or Cancel.
  var opener = returnFocus || null;
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
  var groupCoverId = browseStackCoverIdForPhoto(developmentSourceId);
  var groupPhotoId = groupCoverId == null ? developmentSourceId : groupCoverId; var groupPhoto = groupPhotoId == null ? null : findBrowsePhoto(groupPhotoId);
  var groupStack = groupPhoto && groupPhoto.browse_stack;
  var nav = editNav();
  var copiedDevelopment = nav ? nav.getCopiedRecipe() : null;

  var rateChip = function(/** @type {number} */ n) {
    return {
      label: n === 0 ? '\u2606' : String(n),
      title: n === 0 ? 'No rating' : 'Rate ' + n,
      onClick: function() {
        batchSetRating(n, photoIds);
      },
    };
  };
  var colorChip = function(/** @type {string | null} */ c, /** @type {string} */ icon, /** @type {string} */ title) {
    return {
      label: icon,
      title: c && VireoColorLabels
        ? VireoColorLabels.title(c, title)
        : title,
      color: c,
      colorBaseTitle: title,
      onClick: function() {
        batchSetColorLabel(c, photoIds);
      },
    };
  };
  var flagChip = function(/** @type {NonNullable<BrowsePhoto['flag']>} */ f, /** @type {string} */ icon, /** @type {string} */ title) {
    return {
      label: icon, title: title,
      onClick: function() {
        batchSetFlag(f, photoIds);
      },
    };
  };

  return /** @type {BrowseMenuItem[]} */ ([
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
    { label: 'Filter to This Burst',
      disabled: !groupStack || groupStack.kind !== 'burst',
      disabledHint: 'Enable Stacks and choose a burst',
      onClick: function() { if (developmentSourceId != null) filterToBrowseGroup(developmentSourceId); } },
    { label: 'Filter to This Duplicate Group',
      disabled: !groupStack || groupStack.kind !== 'duplicate',
      disabledHint: 'Enable Stacks and choose a duplicate group',
      onClick: function() { if (developmentSourceId != null) filterToBrowseGroup(developmentSourceId); } },
    { label: 'Find Similar', disabled: !one, disabledHint: hint,
      onClick: function() { if (photoIds[0] != null) findSimilar(photoIds[0]); } },
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
      onClick: function() { if (photoIds[0] != null) openPhotoEditor(photoIds[0]); } },
    { label: 'Copy Development Settings',
      disabled: developmentSourceId == null,
      disabledHint: 'Right-click a photo to choose the settings to copy',
      onClick: function() { if (developmentSourceId != null) copyDevelopmentSettingsFromPhoto(developmentSourceId); } },
    { label: 'Paste Development Settings',
      disabled: _browseDevelopmentPasteInFlight || !(copiedDevelopment && copiedDevelopment.recipe),
      disabledHint: _browseDevelopmentPasteInFlight
        ? 'A development settings paste is already running'
        : 'Copy development settings from a photo first',
      onClick: function() { pasteEditSettingsToSelection(); } },
    { label: 'Develop',
      onClick: function() { developSelected(); } },
  ]).concat(buildOpenInEditorMenuItems(photoIds)).concat([
    { label: revealLabel(), disabled: !one, disabledHint: hint,
      onClick: function() { if (photoIds[0] != null) revealPhoto(photoIds[0]); } },
    { label: 'Copy Path',
      onClick: function() { copyPhotoPaths(photoIds); } },
    { separator: true },
    { label: 'Add Keyword\u2026', onClick: function() { batchAddKeyword(opener); } },
    { label: 'Add to Collection\u2026', onClick: function() { addToCollection(opener); } },
  ]).concat(buildSpeciesHighlightMenuItems(photoIds, {
    showFetchFallback: true,
  })).concat(buildSpeciesRepresentativeMenuItems(photoIds, {
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
    { label: 'Release Local Originals…', disabled: VireoReleaseOriginals.isRunning(),
      onClick: function() {
        VireoReleaseOriginals.release({photo_ids: photoIds}, photoIds.length.toLocaleString() + ' selected photos');
      } },
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
// clamps to the viewport, so a synthetic event object is enough. The More
// button is a <button>, so pass it as the return-focus target: batch dialogs
// launched from Add Keyword / Add to Collection restore focus to it on
// Escape or Cancel instead of falling back to document.body (WebKit does
// not necessarily focus a button on pointer click).
export function openBatchMoreMenu(/** @type {HTMLElement} */ btn) {
  var ids = getActiveSelection();
  if (!ids.length) return;
  var r = btn.getBoundingClientRect();
  openContextMenu({ clientX: r.left, clientY: r.bottom + 4 },
                  buildPhotoContextMenu(ids, null, btn));
}

// Document-level delegation: grids re-render on sort/filter/scroll, so
// per-card listeners would go stale.
document.addEventListener('contextmenu', function(e) {
  var card = /** @type {HTMLElement | null} */ (e.target instanceof Element ? e.target.closest('.grid-card, .browse-stack-member') : null);
  if (!card || !card.dataset.id) return;
  // Ignore right-clicks that came from inside the lightbox or a modal; this
  // handler owns only grid-card context menus. Modal right-clicks are either
  // handled by their own delegation (lightbox) or fall through to the browser.
  if (/** @type {HTMLElement | null} */ (e.target instanceof Element ? e.target.closest('.vireo-ctx-menu') : null)) return;
  if (card.classList.contains('offline')) return;
  e.preventDefault();
  var pid = parseInt(card.dataset.id, 10);
  // Finder-style coercion: right-click on an item outside the selection
  // replaces the selection with that one item. Fold selectedPhotoId in first
  // so a single-focus click doesn't get silently dropped.
  if (selection.batchIds.size === 0 && selection.focusedPhotoId !== null) {
    selection.addToBatch(selection.focusedPhotoId);
  }
  // The "item" a collapsed stack card offers is the stack, so coercion
  // replaces the selection with every frame behind it unless they are all in
  // the selection already. A tray member coerces to itself: that is how a
  // single frame gets a context menu of its own.
  var stackIds = card.classList.contains('browse-stack-member')
    ? [pid]
    : browseSelectionIdsForClick(pid);
  var wholeStackSelected = stackIds.every(function(stackId) {
    return selection.batchIds.has(stackId);
  });
  var ids;
  if (stackIds.length > 1 && !wholeStackSelected) {
    // A selection change like any other: the generation has to move, or a
    // restore still waiting on stack hydration (undo/redo) considers itself
    // current and merges the pre-undo ids into the stack just chosen.
    // selectPhoto() and selectBrowseStackAll() bump it for the same reason.
    // Codex P2 on PR #1672.
    state.anchorRestoreEpoch++;
    selection.selectBatch(stackIds, state.photos.findIndex(function(p) { return p.id === pid; }));
    abandonDetailFocusForBatch();
    ids = stackIds;
  } else {
    var beforeCoercion = selectionIdsKey(Array.from(selection.batchIds));
    ids = coerceSelectionOnContext(new Set(selection.batchIds), pid);
    // Coercion replacing the selection is a selection change like any other,
    // so the generation moves with it — otherwise a restore still waiting on
    // stack hydration (undo/redo) considers itself current and merges the
    // pre-undo ids into the card just right-clicked. Only when it actually
    // changed: right-clicking inside the selection keeps it, and retiring a
    // pending restore for that would lose a selection nobody replaced.
    // Codex P2 on PR #1672.
    if (selectionIdsKey(ids) !== beforeCoercion) { state.anchorRestoreEpoch++; selection.replaceBatch(ids); }
  }
  // If coercion replaced the set, align selectedPhotoId (so the detail panel
  // reflects the right-clicked photo) and selectedIndex (the Shift-range
  // anchor — a subsequent Shift-click must range-select from this card).
  if (ids.length === 1 && ids[0] === pid && selection.focusedPhotoId !== pid) {
    selection.focus(pid, state.photos.findIndex(function(p) { return p.id === pid; }));
    if (selection.anchorIndex < 0) {
      var coverId = browseStackCoverIdForPhoto(pid);
      selection.reanchor(state.photos.findIndex(function(p) { return p.id === coverId; }));
    }
    loadDetail(pid);
  }
  // A right-click is a click: it is the card the user is pointing at, and it
  // is on screen by construction. Without this, a resize after right-clicking
  // a card would still be judged against wherever the *previous* selection was
  // scrolled to, and would refuse to keep this one visible.
  selection.rememberClick(pid);
  refreshCardSelectionVisuals();
  noteFocusedCardVisibility();
  updateBatchBar();
  // The right-clicked card is the opener for any batch dialog launched from
  // this menu (Add Keyword, Add to Collection). It carries tabindex="-1" so
  // VireoDialog's close path can .focus() it when the dialog is dismissed —
  // without this, the dialog restores focus to document.body because the
  // non-focusable menu item is gone by the time the callback runs.
  openContextMenu(e, buildPhotoContextMenu(ids, pid, card));
});

/* ---------- Folder tree context menu ---------- */
function revealFolder(/** @type {number} */ fid) {
  safeFetch('/api/files/reveal', {
    method: 'POST',
    headers: {'Content-Type': 'application/json'},
    body: JSON.stringify({folder_id: fid}),
  }, { toast: false }).then(function(data) {
    showRevealFeedback(data);
  }).catch(function(err) {
    if (typeof showToast === 'function') {
      showToast('Reveal failed: ' + (err.message || 'request failed'), 'error');
    } else {
      console.error('revealFolder failed', err);
    }
  });
}

async function copyFolderPath(/** @type {number} */ fid) {
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

export function hideFolderWorkspaces() {
  folderWorkspacesRequestSeq += 1;
  /** @type {HTMLElement} */ (document.getElementById('folderWorkspacesModal')).classList.remove('open');
}

export async function showFolderWorkspaces(/** @type {number} */ fid) {
  var requestSeq = ++folderWorkspacesRequestSeq;
  var modal = /** @type {HTMLElement} */ (document.getElementById('folderWorkspacesModal'));
  var path = /** @type {HTMLElement} */ (document.getElementById('folderWorkspacesPath'));
  var list = /** @type {HTMLElement} */ (document.getElementById('folderWorkspacesList'));
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
    (data.workspaces || []).forEach(function(/** @type {{name: string, is_root?: boolean, is_active?: boolean}} */ workspace) {
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
    error.textContent = (err instanceof Error ? err.message : '') || 'Could not load associated workspaces.';
    list.appendChild(error);
  }
}

function moveFolder(/** @type {number} */ fid) {
  window.location.href = '/move?folder_id=' + encodeURIComponent(fid);
}

function workLocallyFolder(/** @type {number} */ fid) {
  var local = localFolders();
  if (local) {
    local.stage(fid);
    return;
  }
  window.location.href = '/workspace#work-locally';
}

/** @param {Event} event @param {HTMLElement} button */
export function openPendingArchiveMenu(event, button) {
  event.stopPropagation();
  var fid = Number(button.dataset.folderId);
  var archive = (pendingArchives() || []).find(function(item) {
    return (item.folder_ids || []).some(function(id) { return Number(id) === fid; });
  });
  if (!archive) return;
  var rect = button.getBoundingClientRect();
  openContextMenu({clientX: rect.left, clientY: rect.bottom + 4}, [{
    label: archive.state === 'ready' ? 'Send to NAS…' : 'View NAS transfer…',
    onClick: function() { showPendingArchiveForFolder(fid); },
  }], {onDismiss: function() {
    button.setAttribute('aria-expanded', 'false');
    if (button.isConnected) button.focus();
  }});
  button.setAttribute('aria-expanded', 'true');
  button.setAttribute('aria-controls', 'pendingArchiveFolderMenu');
  var menu = document.querySelector('.vireo-ctx-menu');
  if (!(menu instanceof HTMLElement)) return;
  menu.id = 'pendingArchiveFolderMenu';
  menu.setAttribute('role', 'menu');
  menu.setAttribute('aria-label', 'NAS transfer options');
  const item = menu.querySelector('.vireo-ctx-item');
  if (!(item instanceof HTMLElement)) return;
  item.setAttribute('role', 'menuitem');
  item.tabIndex = 0;
  item.addEventListener('keydown', function(keyEvent) {
    if (keyEvent.key === 'Enter' || keyEvent.key === ' ') {
      keyEvent.preventDefault();
      item.click();
    } else if (keyEvent.key === 'Tab') {
      closeContextMenu();
    } else if (['ArrowDown', 'ArrowUp', 'Home', 'End'].includes(keyEvent.key)) {
      keyEvent.preventDefault();
    }
  });
  item.focus();
}

async function rescanFolder(/** @type {number} */ fid) {
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
    showToast((err instanceof Error ? err.message : '') || 'Could not start folder rescan.', 'error');
  }
}

async function removeWorkspaceRootFromBrowse(/** @type {number} */ fid) {
  var folder = state.browseFolderRows.find(function(row) {
    return Number(row.id) === Number(fid);
  });
  if (!folder || !folder.is_workspace_root) return;

  // Bind the DELETE to the workspace this tree was rendered against, not
  // whatever ``/api/workspaces/active`` returns at click time. Workspace
  // activation is persisted globally, so another tab switching workspaces
  // after render would otherwise redirect the destructive call at the
  // wrong workspace (Codex review r3798912101).
  var wsId = state.browseWorkspaceId;
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
    var childrenByParent = /** @type {Record<string, number[]>} */ ({});
    state.browseFolderRows.forEach(function(row) {
      var pid = row.parent_id;
      if (pid == null) return;
      var key = Number(pid);
      if (!childrenByParent[key]) childrenByParent[key] = [];
      childrenByParent[key]?.push(Number(row.id));
    });
    var ids = /** @type {Record<string, boolean>} */ ({});
    ids[Number(fid)] = true;
    var queue = [Number(fid)];
    while (queue.length) {
      var current = queue.shift(); if (current == null) continue;
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
      var urlFolderId = parseInt(urlParams.get('folder_id') || '', 10);
      if (!isNaN(urlFolderId)) {
        var stillVisible = !removedSubtreeIds[Number(urlFolderId)] &&
          state.browseFolderRows.some(function(row) {
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
    // the tree. Without this, ``localFolderData()`` still
    // carries the removed root when ``renderFolderTree`` runs, so
    // ``folderLocalStatuses`` synthesizes a phantom top-level row for it
    // and leaves the removed folder visible until the next blocker poll
    // (Codex review r3798912105). ``load()`` swallows fetch failures and
    // returns ``null``; rendering against the pre-DELETE
    // ``localFolderData()`` in that case would resurrect the
    // removed root as a phantom, so reload instead of rebuilding from
    // stale local state (Codex review r3799142116).
    var local = localFolders();
    if (local && typeof local.load === 'function') {
      var localRefreshed = await local.load();
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
    if (state.activeFolderId && !loaded[0].some(function(/** @type {BrowseFolder} */ row) {
      return Number(row.id) === Number(state.activeFolderId);
    })) {
      state.activeFolderId = null;
    }
    await resetAndLoad({ preserveCollection: !!state.activeCollectionId });
    loadCollectionCounts();
    loadSummary();
    if (state.timelineMode) loadCalendarData();
    showToast('Folder removed from this workspace.', 'success');
  } catch (err) {
    console.error('removeWorkspaceRootFromBrowse failed', err);
  }
}

document.addEventListener('contextmenu', function(e) {
  var ti = /** @type {HTMLElement | null} */ (e.target instanceof Element ? e.target.closest('.tree-item[data-folder-id]') : null);
  if (!ti) return;
  // Don't fire if the right-click landed on another context menu.
  if (/** @type {HTMLElement | null} */ (e.target instanceof Element ? e.target.closest('.vireo-ctx-menu') : null)) return;
  e.preventDefault();
  var fid = parseInt(ti.dataset.folderId || '', 10);
  if (isNaN(fid)) return;
  var pendingArchive = (pendingArchives() || []).some(function(item) {
    return (item.folder_ids || []).some(function(id) { return Number(id) === fid; });
  });
  var items = [
    { label: 'Filter by this folder', onClick: function() { filterByFolder(fid); } },
    { separator: true },
    { label: revealLabel(), onClick: function() { revealFolder(fid); } },
    { label: 'Copy Path', onClick: function() { copyFolderPath(fid); } },
    { label: 'Show Associated Workspaces…', onClick: function() { showFolderWorkspaces(fid); } },
    { separator: true },
    { label: pendingArchive ? 'Send to NAS\u2026' : 'Work Locally\u2026', onClick: function() {
      if (pendingArchive) showPendingArchiveForFolder(fid);
      else workLocallyFolder(fid);
    } },
    { label: 'Release Local Originals…', disabled: VireoReleaseOriginals.isRunning(),
      onClick: function() {
        VireoReleaseOriginals.release({folder_id: fid}, 'this folder and its subfolders');
      } },
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
async function renameCollection(/** @type {number} */ cid, /** @type {string} */ currentName) {
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

async function duplicateCollection(/** @type {number} */ cid) {
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

async function deleteCollectionById(/** @type {number} */ cid, /** @type {string} */ name) {
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
  if (state.activeCollectionId === cid) {
    state.activeCollectionId = null;
    resetAndLoad();
  } else if (state.openedCollectionId === cid) {
    state.openedCollectionId = null;
    if (VireoFilter.hasFilters()) VireoFilter.clearAll(true);
    resetAndLoad();
  }
  await loadCollections();
  loadCollectionCounts();
}

document.addEventListener('contextmenu', function(e) {
  var ti = /** @type {HTMLElement | null} */ (e.target instanceof Element ? e.target.closest('.tree-item[data-collection-id]') : null);
  if (!ti) return;
  // Don't fire inside another menu, and don't collide with folder-tree delegate.
  if (/** @type {HTMLElement | null} */ (e.target instanceof Element ? e.target.closest('.vireo-ctx-menu') : null)) return;
  if (ti.hasAttribute('data-folder-id')) return;
  e.preventDefault();
  var cid = parseInt(ti.dataset.collectionId || '', 10);
  if (isNaN(cid)) return;
  var nameEl = /** @type {HTMLElement | null} */ (ti.querySelector('.collection-name'));
  var name = nameEl ? nameEl.textContent : '';
  var meta = state.collectionsById[cid];
  var isUnavailable = !!(meta && meta.count_error);
  // Degraded collections can't be filtered (the /photos endpoint would 400)
  // and location review reads from the same collection rules — hide both while
  // keeping the edit/rename/duplicate/delete actions reachable so the user
  // can fix or discard the row.
  var items = [];
  if (!isUnavailable) {
    items.push({ label: 'Filter by this Collection',
      onClick: function() { filterByCollection(cid); } });
    if (!(state.collectionsById[cid] || {}).has_visual) {
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
  // The context menu removes the clicked item before invoking its callback,
  // and the collection row is tabindex=-1 (not in Tab order) but focusable
  // programmatically, so capture the row now and pass it as the return
  // target. Without this the editor restores focus to document.body on
  // Escape or Cancel.
  var editReturnFocus = ti;
  var editActions = [
    { label: 'Edit Rules',
      onClick: function() { editCollection(cid, editReturnFocus); } },
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
