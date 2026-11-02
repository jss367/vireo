/* Browse: multi-selection panel (edits paste, compare, keyword and wildlife state). ES module (see
   browse/README.md). */
import { selection } from './selection-state.js';
import { state } from './state.js';
import {
  VireoBatchEdits, VireoBrowseCompare, editNav, escapeHtml, safeFetch, showToast,
} from './shared.js';
import {
  _clearRepresentativeStateAfterKeywordRemoval, _refreshBrowseKeywordState,
} from './batch-actions.js';
import { loadDetail, loadSummary, showUndoToast } from './detail.js';
import {
  _batchToggleMixed, refreshPendingSyncBanner, renderBatchInspector,
} from './edit-actions.js';
import { refreshGridCards } from './grid.js';
import { panelLoaders } from './panel-loaders.js';
import { panelRequests } from './panel-requests.js';
import { loadSelectionPredictions } from './prediction-panels.js';
import { selectionPanel } from './selection-panel-state.js';
import { getActiveSelection } from './selection.js';
import {
  MUTATION_KEYWORD, MUTATION_WILDLIFE, refreshActiveCollectionAfterMembershipChange,
  scheduleCollectionCountsRefresh,
} from './sidebar-collections.js';
import { loadKeywords } from './sidebar-keywords.js';
import { browseSelectionStackNote, refreshExpandedBrowseStackMembers } from './stacks.js';

export function updateSelectionPanel(/** @type {number[]} */ ids) {
  var panel = /** @type {HTMLElement} */ (document.getElementById('selectionPanel'));
  if (!panel) return;
  if (ids.length <= 1) {
    var dc = /** @type {HTMLElement} */ (document.getElementById('detailContent'));
    if (dc) dc.classList.remove('batch-mode');
    // Clear batch-only Mixed markers immediately so they can't linger while the
    // single-photo detail fetch is in flight.
    _batchToggleMixed('ratingMixed', false);
    _batchToggleMixed('flagMixed', false);
    _batchToggleMixed('colorMixed', false);
    var wasVisible = !panel.classList.contains('hidden');
    panel.classList.add('hidden');
    panelRequests.keywords.invalidate();
    selectionPanel.keywords.reset();
    var list = /** @type {HTMLElement} */ (document.getElementById('selectionKeywordSuggestions'));
    if (list) list.innerHTML = '';
    // Same teardown for the prediction rows — invalidation drops any
    // in-flight selection response so it can't paint into the panel the user
    // has just collapsed back to a single photo.
    panelRequests.predictions.invalidate();
    selectionPanel.predictions.reset();
    var predList = /** @type {HTMLElement} */ (document.getElementById('selectionPredictions'));
    if (predList) predList.innerHTML = '';
    renderSelectionWildlifeState([]);
    if (wasVisible && selection.focusedPhotoId) {
      loadDetail(selection.focusedPhotoId);
    } else if (wasVisible) {
      var detail = /** @type {HTMLElement} */ (document.getElementById('detailContent'));
      var summary = /** @type {HTMLElement} */ (document.getElementById('summaryPanel'));
      if (detail) detail.classList.remove('visible');
      if (summary) summary.classList.remove('hidden');
      loadSummary();
    }
    return;
  }

  panelRequests.detailPredictions.invalidate();
  detail = /** @type {HTMLElement} */ (document.getElementById('detailContent'));
  summary = /** @type {HTMLElement} */ (document.getElementById('summaryPanel'));
  if (summary) summary.classList.add('hidden');
  // Reuse the detail panel as a batch inspector for the whole selection:
  // Rating/Flag/Color/Location act on all selected photos, single-photo-only
  // sections are hidden via .batch-mode CSS.
  if (detail) {
    detail.classList.add('visible');
    detail.classList.add('batch-mode');
  }
  panel.classList.remove('hidden');
  /** @type {HTMLElement} */ (document.getElementById('selectionCount')).textContent =
    ids.length.toLocaleString() + ' photos selected' + browseSelectionStackNote(ids);
  updatePasteEditSection(ids);
  renderSelectionWildlifeState(ids);
  // Keep any EXIF suggestion fetched for the anchor photo: Accept is
  // batch-aware (_locationApplyPhotoIds), so growing the selection is the
  // designed way to apply one suggestion to many photos.
  renderBatchInspector(ids, { preserveExifSuggestion: true });
  if (ids.length > 1000) {
    panelRequests.keywords.invalidate();
    selectionPanel.keywords.reset();
    list = /** @type {HTMLElement} */ (document.getElementById('selectionKeywordSuggestions'));
    if (list) {
      list.innerHTML = '<div class="selection-empty">Keyword suggestions are available for selections of 1,000 photos or fewer.</div>';
    }
    // Say the cap out loud here too, rather than leaving an empty
    // Predictions box that reads as "nothing predicted".
    panelRequests.predictions.invalidate();
    selectionPanel.predictions.reset();
    predList = /** @type {HTMLElement} */ (document.getElementById('selectionPredictions'));
    if (predList) {
      predList.innerHTML = '<div class="selection-empty">Predictions are available for selections of 1,000 photos or fewer.</div>';
    }
    return;
  }
  loadSelectionKeywordSuggestions(ids);
  loadSelectionPredictions(ids);
}

function updatePasteEditSection(/** @type {number[]} */ ids) {
  var section = /** @type {HTMLElement} */ (document.getElementById('pasteEditSection'));
  var hint = /** @type {HTMLElement} */ (document.getElementById('pasteEditHint'));
  if (!section) return;
  var nav = editNav();
  var copied = nav ? nav.getCopiedRecipe() : null;
  if (!copied || !copied.recipe) {
    section.hidden = true;
    return;
  }
  section.hidden = false;
  if (hint) {
    var n = ids.length.toLocaleString();
    hint.textContent = copied.source
      ? 'Copied from ' + copied.source + ' — applies to ' + n + ' selected photos.'
      : 'Applies the copied development settings to ' + n + ' selected photos.';
  }
}

function _browseHasDevelopmentSettings(/** @type {Record<string, unknown> | null | undefined} */ recipe) {
  return !!(recipe && typeof recipe === 'object' && Object.keys(recipe).some(function(key) {
    return key !== 'version';
  }));
}

var _browseDevelopmentCopySeq = 0;

export async function copyDevelopmentSettingsFromPhoto(/** @type {number} */ photoId) {
  var photo = state.photos.find(function(candidate) { return candidate.id === Number(photoId); });
  var nav = editNav();
  if (!photoId || !nav) return;
  var copySeq = ++_browseDevelopmentCopySeq;
  var copyToken = typeof nav.beginCopiedRecipe === 'function'
    ? await nav.beginCopiedRecipe()
    : null;
  try {
    // Fetch the authoritative recipe so a long-lived Browse tab cannot copy
    // settings that were changed in another surface since its last refresh.
    var data = await /** @type {Promise<{recipe?: Record<string, unknown> | null}>} */ (safeFetch('/api/photos/' + photoId + '/edit-recipe', {}, {toast: false}));
    if (copySeq !== _browseDevelopmentCopySeq) return;
    if (
      copyToken &&
      typeof nav.isCopiedRecipeCurrent === 'function' &&
      !nav.isCopiedRecipeCurrent(copyToken)
    ) return;
    var recipe = data && data.recipe;
    if (!_browseHasDevelopmentSettings(recipe)) {
      if (typeof nav.cancelCopiedRecipe === 'function') {
        if (!await nav.cancelCopiedRecipe(copyToken)) return;
      }
      showToast('This photo has no development settings to copy', 'error');
      return;
    }
    var meta = {
      source: (photo && photo.filename) || null,
      at: Date.now(),
    };
    if (
      copyToken &&
      typeof nav.setCopiedRecipeIfCurrent === 'function'
    ) {
      if (!await nav.setCopiedRecipeIfCurrent(recipe, meta, copyToken)) return;
    } else {
      await nav.setCopiedRecipe(recipe, meta);
    }
    showToast('Development settings copied', 'success');
    updatePasteEditSection(getActiveSelection());
  } catch (e) {
    if (copySeq !== _browseDevelopmentCopySeq) return;
    if (
      copyToken &&
      typeof nav.isCopiedRecipeCurrent === 'function' &&
      !nav.isCopiedRecipeCurrent(copyToken)
    ) return;
    if (typeof nav.cancelCopiedRecipe === 'function') {
      if (!await nav.cancelCopiedRecipe(copyToken)) return;
    }
    showToast((e instanceof Error && e.message) || 'Could not copy development settings', 'error');
  }
}

export async function openBatchDevelopmentEditor() {
  try { await VireoBatchEdits.open(getActiveSelection().slice()); }
  catch (error) { showToast((error instanceof Error && error.message) || 'Could not open batch editor', 'error'); }
}

export var _browseDevelopmentPasteInFlight = false;

export async function pasteEditSettingsToSelection() {
  if (_browseDevelopmentPasteInFlight) {
    showToast('A development settings paste is already running.', 'warning');
    return;
  }
  var ids = getActiveSelection();
  if (ids.length < 1) { showToast('Select photos first.', 'error'); return; }
  var nav = editNav();
  var copied = nav ? nav.getCopiedRecipe() : null;
  if (!copied || !copied.recipe) {
    showToast('Copy development settings from a photo first.', 'error');
    return;
  }
  var btn = /** @type {HTMLButtonElement} */ (document.getElementById('pasteEditBtn'));
  _browseDevelopmentPasteInFlight = true;
  if (btn) btn.disabled = true;
  try {
    var data = await VireoBatchEdits.paste(ids.slice());
    if (data) showToast(VireoBatchEdits.resultMessage(data), data.skipped.length ? 'warning' : 'success');
  } catch (e) {
    showToast((e instanceof Error && e.message) || 'Could not paste development settings', 'error');
  } finally {
    _browseDevelopmentPasteInFlight = false;
    if (btn) btn.disabled = false;
  }
}

export const browseCompare = VireoBrowseCompare.create({
  findPhoto: findBrowsePhoto,
  fetch: safeFetch,
  showToast: showToast
});

export function openBrowseCompare() {
  return browseCompare.open(getActiveSelection());
}

export function findBrowsePhoto(/** @type {number} */ id) {
  var topLevel = state.photos.find(function(p) { return p.id === id; });
  if (topLevel) return topLevel;
  var coverIds = Object.keys(state.browseStackMembers);
  for (var i = 0; i < coverIds.length; i++) {
    var coverKey = coverIds[i]; if (!coverKey) continue;
    var member = (state.browseStackMembers[coverKey] || []).find(function(p) {
      return p.id === id;
    });
    if (member) return member;
  }
  return null;
}

export async function loadSelectionKeywordSuggestions(/** @type {number[]} */ ids) {
  return panelLoaders.keywords.load(ids);
}

export function renderSelectionKeywordSuggestions(/** @type {SelectionKeywordSuggestion[]} */ keywords, /** @type {number} */ selectedCount) {
  var list = /** @type {HTMLElement} */ (document.getElementById('selectionKeywordSuggestions'));
  if (!list) return;
  if (!keywords.length) {
    selectionPanel.keywords.reset();
    list.innerHTML = '<div class="selection-empty">No keywords on selected photos.</div>';
    return;
  }

  var html = '';
  selectionPanel.keywords.replace(keywords);
  var groupDefinitions = [
    { type: 'taxonomy', label: 'Species' },
    { type: 'location', label: 'Locations' },
    { type: 'individual', label: 'Individuals' },
    { type: 'genre', label: 'Genres' },
    { type: 'general', label: 'Keywords' }
  ];
  /** @type {Record<string, SelectionKeywordSuggestion[]>} */ var keywordsByType = {};
  keywords.forEach(function(k) {
    var type = k.type || 'general';
    if (!keywordsByType[type]) keywordsByType[type] = [];
    (keywordsByType[type] || (keywordsByType[type] = [])).push(k);
  });
  function renderKeywordRow(/** @type {SelectionKeywordSuggestion} */ k) {
    var missing = k.missing_count || 0;
    var count = k.count || 0;
    var actions = '';
    if (missing > 0) {
      actions += '<button class="selection-keyword-add" data-selection-action="keyword-add"' +
        ' data-keyword-id="' + Number(k.id) +
        '" title="Add this keyword to the selected photos missing it">Add to ' + missing + '</button>';
    }
    if (count > 0) {
      actions += '<button class="selection-keyword-remove" data-selection-action="keyword-remove"' +
        ' data-keyword-id="' + Number(k.id) +
        '" title="Remove this keyword from selected photos that have it">Remove from ' + count + '</button>';
    }
    return '<div class="selection-keyword-row">' +
      '<div style="min-width:0;">' +
        '<div class="selection-keyword-name">' + escapeHtml(k.name) + '</div>' +
        '<div class="selection-keyword-meta">On ' + count + ' of ' + selectedCount + (missing ? ', missing from ' + missing : '') + '</div>' +
      '</div>' +
      '<div class="selection-keyword-actions">' + actions + '</div>' +
      '</div>';
  }
  groupDefinitions.forEach(function(group) {
    var rows = keywordsByType[group.type] || [];
    if (!rows.length) return;
    html += '<div class="selection-keyword-group" data-keyword-type="' + group.type + '">' +
      '<div class="selection-keyword-group-title">' + group.label + '</div>' +
      '<div class="selection-keyword-group-rows">' + rows.map(renderKeywordRow).join('') + '</div>' +
      '</div>';
  });
  list.innerHTML = html;
}

async function renderSelectionWildlifeState(/** @type {number[]} */ ids) {
  var status = /** @type {HTMLElement} */ (document.getElementById('selectionWildlifeStatus'));
  var actions = /** @type {HTMLElement} */ (document.getElementById('selectionWildlifeActions'));
  if (!status || !actions) return;
  if (!ids || !ids.length) {
    panelRequests.wildlife.invalidate();
    status.textContent = '';
    actions.innerHTML = '';
    return;
  }
  // Derive counts from the full selection, not from the loaded grid:
  // "Select all matching" can include off-page photos that are absent from
  // ``photos``, and filtering client-side would omit them from the counts
  // and hide batch controls that should still be available.
  var request = panelRequests.wildlife.begin(); if (!request) return;
  var data;
  try {
    data = await safeFetch('/api/selection/wildlife-state', {
      method: 'POST',
      headers: {'Content-Type': 'application/json'},
      body: JSON.stringify({photo_ids: ids}),
    }, { toast: false });
  } catch (e) {
    if (request.fail()) {
      status.textContent = '';
      actions.innerHTML = '';
    }
    return;
  }
  if (!request.isCurrent()) return;
  var includedCount = (data && data.included_count) || 0;
  var excludedCount = (data && data.excluded_count) || 0;
  var selectedCount = (data && data.selected_count) || 0;
  var missingCount = (data && data.missing_count) || 0;
  if (!selectedCount && !missingCount) {
    status.textContent = '';
    actions.innerHTML = '';
    return;
  }
  var statusText;
  if (!selectedCount) {
    statusText = '';
  } else if (excludedCount === 0) {
    statusText = 'All accessible photos are included in wildlife processing.';
  } else if (includedCount === 0) {
    statusText = 'All accessible photos are excluded from wildlife processing.';
  } else {
    statusText = includedCount + ' included · ' + excludedCount + ' excluded';
  }
  // Surface missing IDs the same way the state endpoint reports them so
  // the panel never implies its counts cover the full user selection when
  // some photos are unavailable in the active workspace.
  if (missingCount > 0) {
    var missingText = missingCount + ' unavailable in this workspace';
    statusText = statusText ? statusText + ' · ' + missingText : missingText;
  }
  status.textContent = statusText;
  var html = '';
  if (includedCount > 0) {
    html += '<button class="selection-keyword-remove" data-selection-action="wildlife-exclude">Exclude ' + includedCount + '</button>';
  }
  if (excludedCount > 0) {
    html += '<button class="selection-keyword-add" data-selection-action="wildlife-include">Include ' + excludedCount + '</button>';
  }
  actions.innerHTML = html;
}

export async function setSelectionWildlifeExcluded(/** @type {boolean} */ excluded) {
  var ids = getActiveSelection();
  if (!ids.length) return;
  var data;
  try {
    data = await safeFetch('/api/batch/wildlife-excluded', {
      method: 'POST',
      headers: {'Content-Type': 'application/json'},
      body: JSON.stringify({photo_ids: ids, excluded: excluded}),
    });
  } catch (e) { return; }
  var changed = new Set((data && data.photo_ids) || ids);
  changed.forEach(function(photoId) {
    var photo = findBrowsePhoto(photoId);
    if (photo) photo.wildlife_excluded = excluded ? 1 : 0;
  });
  var changedIds = Array.from(changed);
  refreshGridCards(changedIds);
  refreshExpandedBrowseStackMembers(changedIds);
  // The selection may have changed while the batch request was in flight.
  // Refresh the current selection so this completion cannot supersede a
  // newer state request with counts and actions for the old photo ids.
  renderSelectionWildlifeState(getActiveSelection());
  scheduleCollectionCountsRefresh();
  refreshActiveCollectionAfterMembershipChange([MUTATION_WILDLIFE]);
  // The batch endpoint skips missing/out-of-workspace IDs instead of
  // rejecting the whole selection; tell the user how many were skipped
  // so a partial apply is never invisible.
  var skipped = (data && data.skipped_count) || 0;
  if (skipped > 0) {
    showToast('Skipped ' + skipped + ' photo' + (skipped === 1 ? '' : 's') +
              ' not accessible in this workspace.', 'warning');
  }
  showUndoToast();
}

export async function applySelectionKeyword(/** @type {number} */ keywordId) {
  var row = selectionPanel.keywords.get(keywordId);
  var ids = row ? row.missingPhotoIds : [];
  if (!ids.length) return;
  try {
    await safeFetch('/api/batch/keyword', {
      method: 'POST',
      headers: {'Content-Type': 'application/json'},
      body: JSON.stringify({photo_ids: ids, keyword_id: keywordId}),
    });
  } catch(e) { return; }
  await _refreshBrowseKeywordState(ids);
  panelRequests.keywords.invalidate();
  loadSelectionKeywordSuggestions(getActiveSelection());
  loadKeywords();
  scheduleCollectionCountsRefresh();
  refreshActiveCollectionAfterMembershipChange([MUTATION_KEYWORD]);
  refreshPendingSyncBanner();
  showUndoToast();
}

export async function removeSelectionKeyword(/** @type {number} */ keywordId) {
  var row = selectionPanel.keywords.get(keywordId);
  var ids = row ? row.presentPhotoIds : [];
  if (!ids.length) return;
  var removedName = row ? row.name || null : null;
  try {
    await safeFetch('/api/batch/keyword-remove', {
      method: 'POST',
      headers: {'Content-Type': 'application/json'},
      body: JSON.stringify({photo_ids: ids, keyword_id: keywordId}),
    });
  } catch(e) { return; }
  await _refreshBrowseKeywordState(ids);
  _clearRepresentativeStateAfterKeywordRemoval(ids, removedName);
  panelRequests.keywords.invalidate();
  loadSelectionKeywordSuggestions(getActiveSelection());
  loadKeywords();
  scheduleCollectionCountsRefresh();
  // One reload only. ``refreshActiveCollectionAfterMembershipChange`` runs
  // ``resetAndLoad({preserveScroll: true})`` when any active rule reads a
  // keyword-derived field (``dependsOnMutation([MUTATION_KEYWORD])``), and
  // the sidebar's ``activeKeyword`` path installs the same ``keyword``
  // rule into VireoFilter — so a follow-up call to
  // ``refreshActiveKeywordAfterRemoval`` would fire a *second* reset that
  // arrives after the first has already cleared ``photos`` and grid
  // cards, leaving ``captureBrowseViewportAnchor`` nothing to anchor on
  // and dropping the scroll position.
  refreshActiveCollectionAfterMembershipChange([MUTATION_KEYWORD]);
  refreshPendingSyncBanner();
  showUndoToast();
}
