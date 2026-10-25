/* Browse: photo selection (click, range, select all, clear). ES module (see
   browse/README.md). */
import { selection } from './selection-state.js';
import { state } from './state.js';
import { VireoFilter, nativeMenuPhotoIdsOverride, safeFetch, showToast } from './shared.js';
import { updateBatchBar } from './batch-bar.js';
import { abandonDetailFocusForBatch, loadDetail, loadSummary } from './detail.js';
import { renderGrid } from './grid.js';
import { getBrowseRules } from './loading.js';
import { observeBrowseWindow } from './window-requests.js';
import { clearExifSuggestion } from './location.js';
import { noteFocusedCardVisibility } from './scroll.js';
import {
  browseCardSelectionClass, browseSelectionIdsForClick, browseSelectionIncludes,
  browseStackMemberRange, browseStacksEnabled,
} from './stacks.js';

/* ---------- Photo Selection & Detail ---------- */
// A stack member can anchor its cover's grid slot while living outside the
// top-level photo array. The owner folds the focused anchor and clicked target
// into this range so visible highlights and action targets stay identical.
function rangeSelectionIds(/** @type {number} */ id, /** @type {number} */ idx) {
  var members = browseStackMemberRange(selection.focusedPhotoId, id);
  if (members) return members;
  var start = Math.min(selection.anchorIndex, idx);
  var end = Math.max(selection.anchorIndex, idx);
  /** @type {number[]} */ var ids = [];
  for (var i = start; i <= end; i++) {
    var photo = state.photos[i];
    if (photo && (!photo.folder_status || photo.folder_status === 'ok' || photo.folder_status === 'partial')) {
      browseSelectionIdsForClick(photo.id).forEach(function(memberId) { ids.push(memberId); });
    }
  }
  return ids;
}

export function selectPhoto(/** @type {Pick<MouseEvent, "shiftKey" | "metaKey" | "ctrlKey"> & Partial<Pick<MouseEvent, "detail">>} */ e, /** @type {number} */ id, /** @type {number} */ idx, /** @type {{stackAware?: boolean}} */ opts = {}) {
  // Preserve the pre-first-click batch through the second click of a
  // double-click, which otherwise looks identical to reaffirming a batch.
  if (!(e && (e.detail || 0) > 1)) selection.captureClick();
  state.anchorRestoreEpoch++;
  selection.rememberClick(id);
  var clickIds = browseSelectionIdsForClick(id, opts);
  if (e.shiftKey && selection.anchorIndex >= 0) {
    selection.extendRange(rangeSelectionIds(id, idx), clickIds);
  } else if (e.metaKey || e.ctrlKey) {
    if (selection.toggle(clickIds)) {
      var detail = /** @type {HTMLElement} */ (document.getElementById('detailContent'));
      if (detail && detail.classList.contains('visible')) {
        detail.classList.remove('visible');
        var summary = /** @type {HTMLElement} */ (document.getElementById('summaryPanel'));
        if (summary) summary.classList.remove('hidden');
        loadSummary();
      }
      // Retire both the rendered suggestion and its asynchronous owner when
      // focus leaves. An old reverse-geocode must never repaint into a batch.
      clearExifSuggestion();
      state.detailPhotoId = null;
    }
  } else if (clickIds.length > 1) {
    selection.selectBatch(clickIds, idx);
    abandonDetailFocusForBatch();
  } else {
    selection.selectSingle(id, idx);
    loadDetail(id);
  }
  refreshCardSelectionVisuals();
  noteFocusedCardVisibility();
  updateBatchBar();
}

// The batch bar drives Develop/Export/Delete/etc. "Active selection" is
// whichever of {selectedPhotos, selectedPhotoId} has entries — the Set for
// cmd/shift-clicks, the single id for a normal click focus. Single-focused
// photos count as selected for the purposes of batch actions.
export function getActiveSelection() {
  var override = nativeMenuPhotoIdsOverride();
  if (override && override.length) {
    return override.slice();
  }
  return getBrowseSelection();
}

// Read-only panels belong to the visible grid selection. A native-menu action
// may temporarily target a different lightbox photo without changing that view.
export function getBrowseSelection() {
  return selection.ids();
}

// Use the same membership query for Select all and selection reconciliation.
export function buildBrowseIdsRequest() {
  var url;
  var fetchOpts = {};
  if (state.activeCollectionId && !state.dashboardCollectionScope) {
    // Mirror the grid's sort AND its Stacks projection so the selection's
    // first photo — which drives Best Batch seed, burst-review order, and
    // export preview — matches the first visible card even when the
    // collection is sorted by name/rating/sharpness/quality or when a
    // stack's quality-ranked cover isn't the earliest member under the
    // sort (Codex P2 on PR #1561).
    var idsParams = new URLSearchParams();
    idsParams.set('sort', /** @type {HTMLSelectElement} */ (document.getElementById('sortSelect')).value);
    if (browseStacksEnabled()) idsParams.set('stacks', 'true');
    url = '/api/collections/' + state.activeCollectionId + '/photo-ids?' + idsParams.toString();
  } else {
    url = '/api/photos/query';
    /** @type {BrowseQueryBody} */ var idsBody = {
      rules: getBrowseRules() || [],
      sort: /** @type {HTMLSelectElement} */ (document.getElementById('sortSelect')).value,
      ids_only: true,
    };
    // Mirror the grid's Stacks projection on the general query path as
    // well — workspace, folder, dashboard-collection, unsaved-filter,
    // and visual-search Select-all now share the same cover-first
    // projection so Best Batch, burst-review, and export preview start
    // from the visible first card even when a stack's quality-ranked
    // cover isn't the earliest member under the selected sort (Codex
    // P2 on PR #1561).
    if (browseStacksEnabled()) idsBody.stacks = true;
    var idsVisual = VireoFilter && VireoFilter.getVisual ? VireoFilter.getVisual() : null;
    if (idsVisual) idsBody.visual = idsVisual;
    if (state.activeFolderId) idsBody.folder_id = state.activeFolderId;
    if (state.activeCollectionId && state.dashboardCollectionScope) idsBody.collection_id = state.activeCollectionId;
    fetchOpts = {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(idsBody),
    };
  }
  return {url: url, options: fetchOpts};
}

export async function selectAllMatchingPhotos() {
  state.anchorRestoreEpoch++;
  var requestIsCurrent = selection.beginSelectAll();
  var selectionEpoch = state.anchorRestoreEpoch;
  var windowIsCurrent = observeBrowseWindow();
  var request = buildBrowseIdsRequest();

  showToast('Selecting all matching photos...', 'info');
  try {
    /** @type {BrowseIdsResponse} */ var data = await safeFetch(request.url, request.options, { toast: false });
    if (!requestIsCurrent() || selectionEpoch !== state.anchorRestoreEpoch || !windowIsCurrent()) return;
    selection.replaceBatch(data.photo_ids || data.ids || []);
    renderGrid();
    updateBatchBar();
    showToast(
      'Selected ' + selection.batchIds.size.toLocaleString() + ' photo' +
        (selection.batchIds.size === 1 ? '' : 's'),
      'success'
    );
  } catch(e) {
    showToast('Could not select all photos: ' + ((e instanceof Error && e.message) || e), 'error');
  }
}

export function selectionIdsKey(/** @type {readonly number[]} */ ids) {
  return ids.slice().sort(function(a, b) { return a - b; }).join(',');
}

export function updateCompareButton(/** @type {number[]} */ ids) {
  var btn = /** @type {HTMLButtonElement} */ (document.getElementById('compareBtn'));
  if (!btn) return;
  var canCompare = ids.length >= 2;
  btn.style.display = canCompare ? '' : 'none';
  btn.disabled = !canCompare;
}

export function updateBestBatchButton(/** @type {number[]} */ ids) {
  var btn = /** @type {HTMLButtonElement | null} */ (document.getElementById('bestBatchBtn'));
  if (!btn) return;
  var canFind = ids.length >= 1;
  btn.style.display = canFind ? '' : 'none';
  btn.disabled = !canFind;
}

export function updateBurstReviewButton(/** @type {number[]} */ ids) {
  var btn = /** @type {HTMLButtonElement} */ (document.getElementById('burstReviewBtn'));
  if (!btn) return;
  var canReview = ids.length >= 2;
  btn.style.display = canReview ? '' : 'none';
  btn.disabled = !canReview;
}

// Re-apply the .selected highlight to match the current state of
// selectedPhotos / selectedPhotoId. Mirrors the refresh block at the tail
// of selectPhoto(); factored out so the context-menu handler can coerce
// selection without paying the full selectPhoto side-effects.
export function refreshCardSelectionVisuals() {
  var byId = new Map(state.photos.map(function(photo) { return [photo.id, photo]; }));
  /** @type {NodeListOf<HTMLElement>} */ (document.querySelectorAll('.grid-card, .browse-stack-member')).forEach(function(el) {
    var cardId = parseInt(el.dataset.id || '', 10);
    // Tray members are single photos even when their cover is a stack card,
    // so they never take the stack-wide rule: address them by id only.
    var selectionClass = el.classList.contains('browse-stack-member')
      ? (browseSelectionIncludes(cardId) ? ' selected' : '')
      : browseCardSelectionClass(byId.get(cardId));
    el.classList.toggle('selected', selectionClass === ' selected');
    el.classList.toggle('stack-partial', selectionClass === ' stack-partial');
  });
}

export function clearSelection() {
  state.anchorRestoreEpoch++;
  // Clear both tracks that feed getActiveSelection(), otherwise the batch bar
  // reappears immediately with the single-focus photo still armed for actions.
  selection.clear();
  // Batch-bar Clear (and every other caller) has to scrub the EXIF suggestion
  // too. Without this, the suggestion element keeps its data-photo-id and
  // Accept button after clearing, so a later Select All that still contains
  // the previously-open anchor satisfies renderLocationEmpty's owner-in-
  // selection check and resurrects the anchor's Accept line for the whole
  // batch — one click would apply the anchor's GPS-derived place to every
  // selected photo. Codex P2 on PR #1097.
  clearExifSuggestion();
  // Also drop the ambient detail-photo pointer maybeShowExifSuggestion's
  // post-await path uses as its owner check. Scrubbing the DOM alone isn't
  // enough: a reverse-geocode fetch that was in flight when the user clicked
  // Clear still resolves later, and if detailPhotoId still points at the
  // departed anchor a subsequent Select All that contains that photo
  // re-satisfies both async guards and repaints A's Accept line into the
  // batch inspector — clicking it would apply A's EXIF place to every
  // selected photo. Codex P2 on PR #1097 (17:04Z follow-up).
  state.detailPhotoId = null;
  // Repaint from the (now empty) selection rather than stripping one class by
  // hand: a stack card can also be carrying the dashed partial mark, and a
  // hand-rolled scrub that only knew about `selected` left that mark on a
  // cleared grid. One rule, one function. Codex P2 on PR #1672.
  refreshCardSelectionVisuals();
  // If the detail panel is still open after clearing, its actions (setFlag,
  // setColorLabel, addKeyword, etc.) early-return on the null selectedPhotoId
  // and silently do nothing. Hide it so there is no ghost UI to interact with.
  var detail = /** @type {HTMLElement} */ (document.getElementById('detailContent'));
  if (detail && detail.classList.contains('visible')) {
    detail.classList.remove('visible');
    var summary = /** @type {HTMLElement} */ (document.getElementById('summaryPanel'));
    if (summary) summary.classList.remove('hidden');
    loadSummary();
  }
  updateBatchBar();
}
