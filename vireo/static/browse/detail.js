/* Browse: single-photo detail panel, metadata, summary panel, undo/redo. ES module (see
   browse/README.md). */
import { selection } from './selection-state.js';
import {renderSpeciesProvenance, speciesDisplayLabel} from './species-provenance.js';
import { searchLaneSeq, state } from './state.js';
import {
  VireoFilter, VireoTime, doRedo, doUndo, escapeHtml, safeFetch, searchLaneHeaders, showToast,
  vireoRememberPhotoEditRecipe, vireoRememberPhotoPair, vireoThumbnailUrl,
  vireoUpdatePairSourceControls,
} from './shared.js';
import { _rememberKeywordNames } from './batch-actions.js';
import { updateBatchBar } from './batch-bar.js';
import {
  _batchToggleMixed, updateDetailColors, updateDetailWildlifeExcluded,
} from './edit-actions.js';
import { getBrowseRules } from './loading.js';
import {
  clearExifSuggestion, maybeShowExifSuggestion, renderCoordinateStatus, renderLocationEmpty,
  renderLocationFilled,
} from './location.js';
import { panelRequests } from './panel-requests.js';
import { loadDetailPredictions } from './prediction-panels.js';
import { refreshCardSelectionVisuals } from './selection.js';
import { appendVisualScopeParams } from './view-controls.js';
import { bindBrowseMarkup } from './markup.js';
import { clearBrowseHistogram, showBrowseHistogram } from './histogram.js';

/* ---------- Undo / Redo ---------- */
export async function undoLast() {
  return doUndo();
}
export async function redoLast() {
  return doRedo();
}

export async function showUndoToast() {
  try {
    var data = await safeFetch('/api/undo/status', {}, { toast: false });
    if (data.available) showToast(data.description + ' — Ctrl+Z to undo', 'success');
  } catch(e) {}
}

/* showToast is now provided globally by _navbar.html */

export async function loadDetail(/** @type {number} */ id) {
  // If a multi-selection is already active, skip this single-photo load — it
  // would strip .batch-mode and rewire the sidebar (rating stars → setRating(id))
  // to the anchor while the user still has N photos selected. Common flow:
  // click A, then Cmd/Ctrl-click B before A's fetch returns. selectedPhotoId
  // stays A, so both the sync class removal below and the post-await
  // renderDetail(A) would overwrite the batch inspector the Cmd-click just set up.
  if (selection.batchIds.size > 1) return;
  clearBrowseHistogram();
  panelRequests.detailPredictions.invalidate();
  /** @type {HTMLElement} */ (document.getElementById('summaryPanel')).classList.add('hidden');
  var detail = /** @type {HTMLElement} */ (document.getElementById('detailContent'));
  // Loading a single photo's detail always exits batch mode.
  if (detail) detail.classList.remove('batch-mode');
  if (detail) detail.classList.toggle('visible', state.detailPhotoId === id);

  // Kill any stale EXIF suggestion tagged for a *different* photo before the
  // fetch returns. Otherwise: open A (suggestion shown, data-photo-id=A) →
  // click B → Select All lands before B's /api/photos response resolves.
  // renderBatchInspector's preserveExifSuggestion check finds A's stale
  // data-photo-id, sees A is in the Select All ids, and resurrects A's
  // Accept line for the whole batch — clicking it applies A's GPS place to
  // every selected photo. Also null detailPhotoId so any in-flight
  // reverse-geocode for A can't repaint through maybeShowExifSuggestion's
  // post-await guard once the batch inspector is on screen.
  if (state.detailPhotoId !== id) {
    clearExifSuggestion();
    state.detailPhotoId = null;
  }

  try {
    var photo = await safeFetch('/api/photos/' + id, {}, { toast: false });
    // Re-check after the fetch: a Cmd/Ctrl-click landing during the await can
    // promote the selection to multi. The batch inspector is already rendered;
    // don't clobber it with this stale single-photo response.
    if (selection.focusedPhotoId === id && selection.batchIds.size <= 1) {
      renderDetail(photo);
      showBrowseHistogram(photo.id);
      if (detail) detail.classList.add('visible');
      // Fetched separately from /api/photos so the panel renders the same
      // enriched rows Review does (nested alternatives, disagreement context)
      // instead of a second, thinner idea of what a prediction is.
      loadDetailPredictions(id);
    }
  } catch(e) {}
}

function renderDetailKeywords(/** @type {BrowsePhoto} */ photo) {
  // Keywords
  var kwTypeIcons = /** @type {Record<string, string>} */ ({general:'●', taxonomy:'🌿', individual:'👤', location:'📍', genre:'🎭'});
  var kwTypeLabels = /** @type {Record<string, string>} */ ({general:'General', taxonomy:'Taxonomy', individual:'Individual', location:'Location', genre:'Genre'});
  _rememberKeywordNames(photo.keywords || []);
  var kwHtml = '';
  (photo.keywords || []).forEach(function(k) {
    var ktype = k.type || 'general';
    var icon = kwTypeIcons[ktype] || '●';
    var dropdownHtml = '<div class="keyword-type-dropdown" data-kw-id="' + k.id + '">';
    Object.keys(kwTypeIcons).forEach(function(t) {
      var activeClass = t === ktype ? ' active' : '';
      dropdownHtml += '<span class="keyword-type-option' + activeClass + '" data-kw-id="' + k.id + '" data-kw-type="' + t + '" data-browse-on="click:setKeywordType">' + kwTypeIcons[t] + ' ' + kwTypeLabels[t] + '</span>';
    });
    dropdownHtml += '</div>';
    kwHtml += '<span class="keyword-tag">' +
      '<span class="keyword-type-indicator" data-kw-id="' + k.id + '" data-browse-on="click:toggleTypeDropdown" title="Type: ' + kwTypeLabels[ktype] + '">' + icon + dropdownHtml + '</span>' +
      escapeHtml(speciesDisplayLabel(k, k.name)) +
      '<span class="remove-kw" data-photo-id="' + photo.id + '" data-kw-id="' + k.id + '" data-browse-on="click:removeKeyword">&times;</span></span>';
  });
  var keywordsEl = /** @type {HTMLElement} */ (document.getElementById('detailKeywords'));
  keywordsEl.innerHTML = kwHtml;
  bindBrowseMarkup(keywordsEl);
  renderSpeciesProvenance(photo, keywordsEl);

}

function renderDetail(/** @type {BrowsePhoto} */ photo) {
  state.detailPhotoId = photo.id;
  // Single-photo view: clear any Mixed markers left over from batch mode.
  _batchToggleMixed('ratingMixed', false);
  _batchToggleMixed('flagMixed', false);
  _batchToggleMixed('colorMixed', false);
  if (
    Object.prototype.hasOwnProperty.call(photo, 'edit_recipe')
  ) {
    vireoRememberPhotoEditRecipe(photo.id, photo.edit_recipe, {
      skipIfLocallyWritten: true,
    });
  }
  vireoRememberPhotoPair(photo);
  vireoUpdatePairSourceControls(photo.id);
  var detailThumb = vireoThumbnailUrl(photo);
  /** @type {HTMLImageElement} */ (document.getElementById('detailImg')).src = detailThumb;
  /** @type {HTMLElement} */ (document.getElementById('detailFilename')).textContent = photo.filename;
  renderCoordinateStatus(photo);

  // Rating stars
  var ratingHtml = '';
  for (var i = 1; i <= 5; i++) {
    var cls = i <= (photo.rating || 0) ? 'detail-star active' : 'detail-star';
    ratingHtml += '<span class="' + cls + '" data-photo-id="' + photo.id + '" data-rating="' + i + '" data-browse-on="click:setDetailRating">&#9733;</span>';
  }
  var ratingEl = /** @type {HTMLElement} */ (document.getElementById('detailRating'));
  ratingEl.innerHTML = ratingHtml;
  bindBrowseMarkup(ratingEl);

  // Flags
  updateDetailFlagButtons(photo.flag, photo.flag_note);

  // Color labels
  updateDetailColors();

  // Wildlife classification exclusion
  updateDetailWildlifeExcluded(photo);

  renderDetailKeywords(photo);

  // Location section
  if (photo.location) {
    renderLocationFilled(photo.location);
  } else {
    renderLocationEmpty();
    maybeShowExifSuggestion(photo);
  }

  // XMP sidecar
  var xmpEl = /** @type {HTMLElement} */ (document.getElementById('detailXmp'));
  if (photo.xmp_exists === false) {
    xmpEl.innerHTML = '<span style="color:var(--text-ghost);">No .xmp sidecar file</span>';
  } else if (photo.xmp_keywords && photo.xmp_keywords.length > 0) {
    var dbKwNames = (photo.keywords || []).map(function(k) { return k.name.toLowerCase(); });
    var xmpHtml = '';
    photo.xmp_keywords.forEach(function(kw) {
      var inDb = dbKwNames.indexOf(kw.toLowerCase()) !== -1;
      var color = inDb ? 'var(--accent)' : 'var(--warning)';
      var title = inDb ? 'In sync with database' : 'In XMP but not in database';
      xmpHtml += '<span style="display:inline-block;background:var(--bg-tertiary);padding:2px 6px;border-radius:3px;margin:2px;font-size:11px;border:1px solid ' + color + ';color:' + color + ';" title="' + title + '">' + escapeHtml(kw) + '</span>';
    });
    // Check for DB keywords not in XMP
    var xmpLower = photo.xmp_keywords.map(function(k) { return k.toLowerCase(); });
    (photo.keywords || []).forEach(function(k) {
      if (xmpLower.indexOf(k.name.toLowerCase()) === -1) {
        xmpHtml += '<span style="display:inline-block;background:var(--bg-tertiary);padding:2px 6px;border-radius:3px;margin:2px;font-size:11px;border:1px solid var(--info);color:var(--info);" title="In database but not in XMP (pending sync)">+ ' + escapeHtml(k.name) + '</span>';
      }
    });
    xmpHtml += '<div style="margin-top:4px;font-size:10px;color:var(--text-ghost);">' +
      '<span style="color:var(--accent);">&#9632;</span> synced ' +
      '<span style="color:var(--warning);">&#9632;</span> XMP only ' +
      '<span style="color:var(--info);">&#9632;</span> DB only (pending sync)</div>';
    xmpEl.innerHTML = xmpHtml;
  } else if (photo.xmp_exists) {
    xmpEl.innerHTML = '<span style="color:var(--text-ghost);">XMP file exists but has no keywords</span>';
  }

  // Quick summary
  var summary = '';
  var meta = photo.metadata;
  if (meta) {
    var exifTags = meta.EXIF || {};
    var composite = meta.Composite || {};

    // Camera (remove make from model if model already starts with it)
    var make = exifTags.Make || '';
    var model = exifTags.Model || '';
    var camera = model.startsWith(make) ? model : (make + ' ' + model).trim();
    if (camera) summary += '<div class="summary-camera">' + escapeHtml(camera) + '</div>';

    // Lens
    var lens = composite.LensID || exifTags.LensModel || composite.Lens || '';
    if (lens) summary += '<div class="summary-lens">' + escapeHtml(String(lens)) + '</div>';

    // Exposure line: focal | aperture | shutter | ISO
    var parts = [];
    if (exifTags.FocalLength) parts.push(Math.round(exifTags.FocalLength) + 'mm');
    if (exifTags.FNumber) parts.push('f/' + exifTags.FNumber);
    if (exifTags.ExposureTime) {
      if (exifTags.ExposureTime < 1) {
        parts.push('1/' + Math.round(1 / exifTags.ExposureTime) + 's');
      } else {
        parts.push(exifTags.ExposureTime + 's');
      }
    }
    if (exifTags.ISO) parts.push('ISO ' + exifTags.ISO);
    if (parts.length) summary += '<div class="summary-exposure">' + parts.join('&nbsp;&nbsp;') + '</div>';
  }

  // Date, dimensions, file size (always from photo fields)
  if (photo.timestamp) summary += 'Date: ' + escapeHtml(VireoTime.captureDateTime(photo.timestamp, true)) + '<br>';
  if (photo.width && photo.height) summary += 'Size: ' + photo.width + ' x ' + photo.height + '<br>';
  if (photo.file_size) {
    var sz = photo.file_size;
    summary += 'File: ' + (sz >= 1048576 ? (sz / 1048576).toFixed(1) + ' MB' : Math.round(sz / 1024) + ' KB') + '<br>';
  }
  /** @type {HTMLElement} */ (document.getElementById('detailSummary')).innerHTML = summary;

  // Path
  /** @type {HTMLElement} */ (document.getElementById('detailPath')).textContent = photo.filename;

  // Clear search and render metadata groups
  var searchInput = /** @type {HTMLInputElement} */ (document.getElementById('metadataSearch'));
  if (searchInput) searchInput.value = '';
  renderMetadataGroups(photo.metadata);
}

// Also renders the note under the buttons saying who set the flag and why,
// when the detail payload carried one. Callers that only know the flag (a
// hand click, the batch inspector) pass no note, and a hand flag has none.
export function updateDetailFlagButtons(
  /** @type {string | null | undefined} */ flag,
  /** @type {BrowseFlagNote | null | undefined} */ note = null,
) {
  /** @type {NodeListOf<HTMLButtonElement>} */ (document.querySelectorAll('.detail-flag-btn')).forEach(function(btn) {
    btn.className = 'detail-flag-btn';
  });
  var flagBtns = /** @type {NodeListOf<HTMLButtonElement>} */ (document.querySelectorAll('.detail-flag-btn'));
  if (flag === 'flagged') flagBtns[1]?.classList.add('active-flag');
  else if (flag === 'rejected') flagBtns[2]?.classList.add('active-reject');
  renderDetailFlagNote(flag, note);
}

// The line under the flag buttons: "Rejected — claude-fable-5-1: eye soft at
// 1:1 · 2026-10-17". Pure: the detail payload's flag and flag_note in, text
// out ('' when the flag was a hand click, which carries no provenance).
function flagNoteText(
  /** @type {string | null | undefined} */ flag,
  /** @type {BrowseFlagNote | null | undefined} */ note,
) {
  if (!note || (!note.source && !note.reason)) return '';
  var normalized = flag || 'none';
  if ((note.flag || 'none') !== normalized) return '';
  var verb = normalized === 'rejected' ? 'Rejected'
    : normalized === 'flagged' ? 'Flagged'
    : 'Flag cleared';
  var who = note.source && note.reason ? note.source + ': ' + note.reason
    : note.source ? 'by ' + note.source
    : String(note.reason);
  var when = note.at ? ' \u00b7 ' + String(note.at).slice(0, 10) : '';
  return verb + ' \u2014 ' + who + when;
}

function renderDetailFlagNote(
  /** @type {string | null | undefined} */ flag,
  /** @type {BrowseFlagNote | null | undefined} */ note,
) {
  var el = document.getElementById('detailFlagNote');
  if (!el) return;
  var text = flagNoteText(flag, note);
  el.textContent = text;
  el.hidden = !text;
}

function renderMetadataGroups(/** @type {BrowseMetadata | undefined} */ metadata) {
  var container = /** @type {HTMLElement} */ (document.getElementById('metadataGroups'));
  var section = /** @type {HTMLElement} */ (document.getElementById('detailMetadataSection'));
  container.innerHTML = '';

  if (!metadata) {
    section.style.display = 'none';
    return;
  }

  // Sort groups: EXIF first, then alphabetical, _meta last
  var groups = Object.keys(metadata).sort(function(a, b) {
    if (a === 'EXIF') return -1;
    if (b === 'EXIF') return 1;
    if (a === '_meta') return 1;
    if (b === '_meta') return -1;
    return a.localeCompare(b);
  });

  // Skip groups with 0 tags
  groups = groups.filter(function(g) {
    return Object.keys(metadata[g] || {}).length > 0;
  });

  if (groups.length === 0) {
    section.style.display = 'none';
    return;
  }

  section.style.display = '';

  groups.forEach(function(group) {
    var tags = metadata[group] || {};
    var tagNames = Object.keys(tags).sort();

    var div = document.createElement('div');
    div.className = 'meta-group';
    div.setAttribute('data-group', group);

    var header = document.createElement('div');
    header.className = 'meta-group-header';
    header.innerHTML = '<span>' + escapeHtml(group) + '</span><span class="meta-group-count">' + tagNames.length + '</span>';
    header.addEventListener('click', function() { div.classList.toggle('open'); });

    var body = document.createElement('div');
    body.className = 'meta-group-body';

    tagNames.forEach(function(tag) {
      var val = tags[tag];
      if (typeof val === 'object' && val !== null) val = JSON.stringify(val);
      var row = document.createElement('div');
      row.className = 'meta-tag-row';
      row.setAttribute('data-tag', tag.toLowerCase());
      row.setAttribute('data-value', String(val).toLowerCase());
      row.innerHTML = '<span class="meta-tag-name">' + escapeHtml(tag) + '</span><span class="meta-tag-value">' + escapeHtml(String(val)) + '</span>';
      body.appendChild(row);
    });

    div.appendChild(header);
    div.appendChild(body);
    container.appendChild(div);
  });
}

export function filterMetadata(/** @type {string} */ query) {
  var q = query.toLowerCase().trim();
  var groups = /** @type {NodeListOf<HTMLElement>} */ (document.querySelectorAll('#metadataGroups .meta-group'));

  groups.forEach(function(group) {
    if (!q) {
      // Reset: show all groups, collapse all
      group.style.display = '';
      group.classList.remove('open');
      /** @type {NodeListOf<HTMLElement>} */ (group.querySelectorAll('.meta-tag-row')).forEach(function(row) {
        row.style.display = '';
      });
      return;
    }

    var rows = /** @type {NodeListOf<HTMLElement>} */ (group.querySelectorAll('.meta-tag-row'));
    var hasMatch = false;

    rows.forEach(function(row) {
      var tag = row.getAttribute('data-tag') || '';
      var val = row.getAttribute('data-value') || '';
      var matches = tag.indexOf(q) !== -1 || val.indexOf(q) !== -1;
      row.style.display = matches ? '' : 'none';
      if (matches) hasMatch = true;
    });

    // Also check if group name matches
    var groupName = (group.getAttribute('data-group') || '').toLowerCase();
    if (groupName.indexOf(q) !== -1) {
      hasMatch = true;
      rows.forEach(function(row) { row.style.display = ''; });
    }

    group.style.display = hasMatch ? '' : 'none';
    if (hasMatch && q) {
      group.classList.add('open');
    }
  });
}

export function hideDetailPanel() {
  /** @type {HTMLElement} */ (document.getElementById('detailContent')).classList.remove('visible');
  /** @type {HTMLElement} */ (document.getElementById('summaryPanel')).classList.remove('hidden');
}

// Trading a single-photo detail focus for a batch. The panel is not the only
// thing that has to go: the EXIF suggestion element keeps its data-photo-id
// and its Accept button, and an in-flight reverse-geocode keeps
// detailPhotoId as its owner check — so a later selection that still
// contains the abandoned photo would resurrect its Accept line for the whole
// batch, and one click would apply that one photo's place to every selected
// photo. closeDetail() and clearSelection() retire both; the paths that
// enter a stack selection need it for the same reason.
export function abandonDetailFocusForBatch() {
  hideDetailPanel();
  clearExifSuggestion();
  state.detailPhotoId = null;
}

export function closeDetail() {
  state.anchorRestoreEpoch++;
  hideDetailPanel();
  selection.focus(null);
  selection.reanchor(-1);
  // Drop any lingering EXIF suggestion attached to the closed detail photo.
  // Without this, the element keeps its data-photo-id and Accept button; a
  // later Ctrl+A (or any batch that still contains that photo) would satisfy
  // renderLocationEmpty's owner-in-selection check and resurrect the anchor's
  // Accept line for the whole batch — clicking it would apply the anchor's
  // GPS-derived place to every selected photo.
  clearExifSuggestion();
  // Null the ambient detail-photo pointer for the same reason. maybeShowExif
  // Suggestion's post-await guard uses detailPhotoId as its owner
  // check: if a reverse-geocode was in flight when we closed the panel, the
  // fetch keeps running and later resolves. With the pointer still set, a
  // Ctrl+A that includes the departed photo would satisfy both async guards
  // and repaint A's Accept line into the batch inspector.
  state.detailPhotoId = null;
  // Re-highlight any cmd/shift-click selections that survive detail close.
  // A blanket .remove('selected') would leave selectedPhotos armed for batch
  // actions with no visible indicator — e.g. click A -> cmd-click B -> cmd-click
  // B drops the set to {A}, and closing detail would otherwise show "1 selected"
  // in the bar against an unhighlighted photo.
  refreshCardSelectionVisuals();
  updateBatchBar();
  loadSummary();
}

function buildSummaryParams() {
  var params = new URLSearchParams();
  if (state.activeFolderId) params.set('folder_id', String(state.activeFolderId));
  var rules = getBrowseRules();
  if (rules) params.set('rules', JSON.stringify(rules));
  appendVisualScopeParams(params);
  if (state.activeCollectionId) params.set('collection_id', String(state.activeCollectionId));
  return params;
}

function reconcileSummaryLoadRenders() {
  var gen = state.summaryLoadGen;
  while (gen > state.summaryRenderDecisionGen) {
    var loadState = state.summaryLoadStates[gen];
    if (!loadState || loadState.status === 'pending') return;
    if (loadState.status === 'success') {
      var currentKey = buildSummaryParams().toString();
      state.summaryRenderDecisionGen = gen;
      if (loadState.key === currentKey) loadState.data && renderSummary(loadState.data);
      Object.keys(state.summaryLoadStates).forEach(function(key) {
        if (Number(key) <= state.summaryLoadGen) delete state.summaryLoadStates[key];
      });
      return;
    }
    gen--;
  }
}

export async function loadSummary() {
  var gen = ++state.summaryLoadGen;
  var params = buildSummaryParams();
  state.summaryLoadStates[gen] = {
    status: 'pending',
    key: params.toString()
  };

  try {
    var data = await safeFetch('/api/browse/summary?' + params.toString(), {
      headers: searchLaneHeaders(
        'summary', searchLaneSeq(state.summaryLane, params.toString())),
    }, { toast: false });
    var loadState = state.summaryLoadStates[gen];
    if (!loadState) return data;
    loadState.status = 'success';
    loadState.data = data;
    reconcileSummaryLoadRenders();
    return data;
  } catch(e) {
    var failedState = state.summaryLoadStates[gen];
    if (failedState) {
      failedState.status = 'failure';
      reconcileSummaryLoadRenders();
    }
    return null;
  }
}

// Recorded completed runs and qualifying results are distinct: no animal
// box does not imply the detector never ran. Missing run coverage on an
// older response is unknown, rather than zero.
// Empty when the scope has no photos or the server sent no coverage.
function analysisCoverageText(/** @type {BrowseSummary} */ summary) {
  var total = Number(summary && summary.filtered_total) || 0;
  var analysis = summary && summary.analysis;
  if (total <= 0 || !analysis) return '';
  var totalText = total.toLocaleString();
  function count(/** @type {number | undefined} */ value) {
    return Math.min(Math.max(Number(value) || 0, 0), total);
  }
  function dataSegment(/** @type {string} */ noun, /** @type {string} */ preposition, /** @type {number | undefined} */ value) {
    var n = count(value);
    if (n === 0) return noun + ' never computed';
    return noun + ' ' + preposition + ' ' + n.toLocaleString() + ' of ' + totalText;
  }
  var misses = count(analysis.with_misses);
  var missesSegment = misses === 0 ? 'misses never computed'
    : misses === total ? 'misses checked on ' + totalText
    : 'misses checked on ' + misses.toLocaleString() + ' of ' + totalText;
  var detected = count(analysis.with_detection);
  var text = [
    analysis.with_detection_run == null ? '' : count(analysis.with_detection_run) === 0
      ? 'no completed detection runs recorded'
      : 'detection checked on ' + count(analysis.with_detection_run).toLocaleString() + ' of ' + totalText,
    detected === 0 ? 'no qualifying animal detections recorded'
      : 'animal detections recorded on ' + detected.toLocaleString() + ' of ' + totalText,
    dataSegment('quality data', 'for', analysis.with_quality),
    dataSegment('eye focus', 'for', analysis.with_eye),
    missesSegment,
  ].filter(Boolean).join(' · ');
  return text.charAt(0).toUpperCase() + text.slice(1);
}

function renderSummary(/** @type {BrowseSummary} */ data) {
  var isFiltered = data.filtered_total !== data.total;

  /** @type {HTMLElement} */ (document.getElementById('summaryPhotoCount')).textContent = data.filtered_total.toLocaleString();
  /** @type {HTMLElement} */ (document.getElementById('summaryFilterNote')).textContent =
    isFiltered ? 'of ' + data.total.toLocaleString() + ' total' : '';

  /** @type {HTMLElement} */ (document.getElementById('summaryClassified')).textContent = data.classified.toLocaleString() + ' classified';
  /** @type {HTMLElement} */ (document.getElementById('summaryUnclassified')).textContent = data.unclassified.toLocaleString() + ' unclassified';
  /** @type {HTMLElement} */ (document.getElementById('summaryAnalysis')).textContent = analysisCoverageText(data);

  renderSummarySpecies(data.top_species || []);
}

function renderSummarySpecies(/** @type {NonNullable<BrowseSummary['top_species']>} */ species) {
  var speciesSection = /** @type {HTMLElement} */ (document.getElementById('summarySpeciesSection'));
  var speciesList = /** @type {HTMLElement} */ (document.getElementById('summarySpeciesList'));
  speciesList.replaceChildren();
  speciesSection.style.display = species.length ? '' : 'none';
  var rows = species.map(function(s, index) {
    var row = document.createElement('button');
    row.type = 'button';
    row.className = 'summary-species-row';
    row.hidden = index >= 5;
    row.title = 'Filter by top predicted species: ' + s.species;
    var name = document.createElement('span');
    name.className = 'summary-species-name';
    name.textContent = s.species;
    var count = document.createElement('span');
    count.className = 'summary-species-count';
    count.textContent = String(s.count);
    row.append(name, count);
    row.addEventListener('click', function() { filterByTopSpecies(s.species); });
    row.addEventListener('keydown', function(event) {
      // Let the native button activate without triggering grid shortcuts.
      if (event.key === 'Enter' || event.key === ' ') event.stopPropagation();
    });
    speciesList.appendChild(row);
    return row;
  });
  if (species.length <= 5) return;

  var expanded = false;
  var toggle = document.createElement('button');
  toggle.type = 'button';
  toggle.className = 'summary-species-toggle';
  toggle.textContent = 'Show more';
  toggle.setAttribute('aria-expanded', 'false');
  toggle.addEventListener('click', function() {
    expanded = !expanded;
    rows.forEach(function(row, index) { row.hidden = !expanded && index >= 5; });
    toggle.textContent = expanded ? 'Show less' : 'Show more';
    toggle.setAttribute('aria-expanded', String(expanded));
  });
  toggle.addEventListener('keydown', function(event) {
    if (event.key === 'Enter' || event.key === ' ') event.stopPropagation();
  });
  speciesList.appendChild(toggle);
}

async function filterByTopSpecies(/** @type {string} */ species) {
  var scopeGen = ++state.browseScopeGen;
  if (!VireoFilter.isReady()) {
    if (!state.browseFilterInitPromise) return;
    try {
      await state.browseFilterInitPromise;
    } catch (e) {
      return;
    }
    if (!VireoFilter.isReady() || scopeGen !== state.browseScopeGen) return;
  }
  // Compose with legacy collection deep links instead of dropping their scope.
  if (state.activeCollectionId) state.dashboardCollectionScope = true;
  var rules = VireoFilter.getUserRules();
  if (rules && rules.mode && rules.mode !== 'all' && rules.rules.length) {
    // Narrow an OR collection as a whole, rather than adding another OR arm.
    VireoFilter.loadExpression({mode: 'all', rules: [
      rules, {field: 'top_predicted_species', op: 'is', value: species}
    ]}, VireoFilter.getVisual(), {reason: 'filterAdded'});
  } else {
    VireoFilter.addRule('top_predicted_species', 'is', species);
  }
}
