// Group Review session state, opening, seeding, closing, and detaching.
// Classic page script; shared globals are initialized before boot.js runs.

/** @param {number} encIdx @param {number | null | undefined} [burstIdx] */
async function detachBurst(encIdx, burstIdx) {
  if (isScopedReviewView() || (pipelineResults && currentReviewResults().source === 'browse-selection')) {
    notifyReadOnlyScopedView();
    return;
  }
  // Folder/Collection filter mode leaves pipelineResults as the full saved
  // review and only hides out-of-scope cards; a whole-burst detach would
  // still rewrite the saved grouping for the hidden photos. Refuse when the
  // burst spans the scope boundary. Per-photo detach stays unaffected
  // because its button is only rendered next to in-scope cards.
  var scopeIds = activeReviewScopeFilter();
  if (scopeIds) {
    var enc = pipelineResults && currentReviewResults().encounters && currentReviewResults().encounters[encIdx];
    var burst = enc && enc.bursts && enc.bursts[burstIdx ?? -1];
    var burstIds = burst ? (burst.photo_ids || burst || []) : [];
    var outside = 0;
    for (var i = 0; i < burstIds.length; i++) {
      if (!scopeIds.has(burstIds[i] ?? -1)) outside++;
    }
    if (outside) {
      var noun = reviewScopeMode === 'folder' ? 'folder' : 'collection';
      showToast(
        'This burst has ' + outside + ' photo' + (outside === 1 ? '' : 's')
          + ' outside the ' + noun + ' and would be rewritten too. '
          + 'Switch Scope to All reviewed photos to detach it.',
        'warning'
      );
      return;
    }
  }

  var resp = await safeFetch('/api/pipeline/detach-burst', {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ encounter_index: encIdx, burst_index: burstIdx }),
  });
  if (!resp || !resp.ok) return;

  currentReviewResults().encounters = resp.encounters;
  if (resp.summary) currentReviewResults().summary = resp.summary;
  renderResults();
  updateSummaryBar(currentReviewResults().summary);
  refreshLatestScopeSnapshotIfCurrent();

  showToast('Burst detached from encounter', 'success');
}

/** @param {number} encIdx @param {number | null | undefined} burstIdx @param {number} photoId */
async function detachPhoto(encIdx, burstIdx, photoId) {
  if (isScopedReviewView() || (pipelineResults && currentReviewResults().source === 'browse-selection')) {
    notifyReadOnlyScopedView();
    return;
  }
  if (!isPhotoActionableInReviewScope(photoId)) return;

  var resp = await safeFetch('/api/pipeline/detach-photo', {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ encounter_index: encIdx, burst_index: burstIdx, photo_id: photoId }),
  });
  if (!resp || !resp.ok) return;

  currentReviewResults().encounters = resp.encounters;
  if (resp.summary) currentReviewResults().summary = resp.summary;
  renderResults();
  updateSummaryBar(currentReviewResults().summary);
  refreshLatestScopeSnapshotIfCurrent();

  showToast('Photo detached from burst', 'success');
}

/* ==================== Group Review Modal ==================== */

/** @type {ReviewGroupState} */ var grmState = {
    dbState: {},
  encIdx: null,       // encounter index in pipelineResults
  burstIdx: null,     // burst index within encounter
  items: [],          // photo objects from currentReviewResults().photos
  picks: new Set(),
  rejects: new Set(),
  removed: new Set(),
  selected: null,     // photo id
  selectedIds: new Set(),
  selectedSubjectByPhoto: {}, // photo id -> detection id (multi-subject photos only)
  selectionAnchor: null,
  touched: new Set(), // photo ids changed by the user before DB seeding lands
  sessionId: 0,       // bumped on each openGroupReview; used to drop stale fetches
  applying: false
};

/**
 * The Group Review selection the shared navbar's native menu acts on while
 * the modal is open (the getGroupModalSelection page hook).
 * @returns {number[]}
 */
function grmNativeMenuSelection() {
  if (grmState.selectedIds && grmState.selectedIds.size) {
    return /** @type {number[]} */ (Array.from(grmState.selectedIds));
  }
  if (grmState.selected != null) return [/** @type {number} */ (grmState.selected)];
  return [];
}

/** @param {number} photoId */
function findBurstForPhoto(photoId) {
  if (!pipelineResults) return null;
  for (var ei = 0; ei < currentReviewResults().encounters.length; ei++) {
    var enc = currentReviewResults().encounters[ei];
    if (!requireReviewValue(enc).bursts) continue;
    for (var bi = 0; bi < requireReviewValue(enc).bursts.length; bi++) {
      var burst = requireReviewValue(enc).bursts[bi];
      var ids = requireReviewValue(burst).photo_ids || burst;
      if (ids.indexOf(photoId) >= 0) {
        return { encIdx: ei, burstIdx: bi, photoIds: ids };
      }
    }
  }
  return null;
}

/** @param {unknown} photoId */
function grmNormalizePhotoId(photoId) {
  var id = parseInt(String(photoId), 10);
  return Number.isInteger(id) ? id : null;
}

/** @param {ReviewPhoto[]} items @param {unknown} photoId @param {Set<number> | null} [removedSet] */
function grmSelectablePhotoId(items, photoId, removedSet) {
  var id = grmNormalizePhotoId(photoId);
  if (id == null) return null;
  if (removedSet && removedSet.has(id)) return null;
  return items.some(function(p) { return p.id === id; }) ? id : null;
}

/** @param {ReviewPhoto[]} items @param {Set<number> | null} [removedSet] */
function grmFirstSelectablePhotoId(items, removedSet) {
  for (var i = 0; i < items.length; i++) {
    if (!removedSet || !removedSet.has(requireReviewValue(items[i]).id)) return requireReviewValue(items[i]).id;
  }
  return null;
}

/** @param {ReviewPhoto[]} items @param {number | null | undefined} preferredId */
function grmFallbackSelectionId(items, preferredId) {
  var removedSet = grmState && grmState.removed ? grmState.removed : null;
  return grmSelectablePhotoId(items, preferredId, removedSet) || grmFirstSelectablePhotoId(items, removedSet);
}

/** @param {ReviewPhoto[]} items */
function grmCurrentSelectionId(items) {
  var removedSet = grmState && grmState.removed ? grmState.removed : null;
  var selected = grmSelectablePhotoId(items, grmState && grmState.selected, removedSet);
  if (selected != null) return selected;
  if (grmState && grmState.selectedIds) {
    var selectedIds = Array.from(grmState.selectedIds);
    for (var i = 0; i < selectedIds.length; i++) {
      var id = grmSelectablePhotoId(items, selectedIds[i], removedSet);
      if (id != null) return id;
    }
  }
  return null;
}

/** @param {ReviewPhoto[]} items @param {number | null | undefined} [fallbackId] */
function grmRenderKeepingCurrentSelection(items, fallbackId) {
  var currentId = grmCurrentSelectionId(items);
  if (currentId != null) {
    if (!grmState.selectedIds) grmState.selectedIds = new Set();
    if (!grmState.selectedIds.has(currentId)) grmState.selectedIds.add(currentId);
    grmState.selected = currentId;
    if (!grmState.selectionAnchor) grmState.selectionAnchor = currentId;
    renderGroupModal();
    grmRefreshSelectedLoupe();
    return;
  }

  var nextId = grmFallbackSelectionId(items, fallbackId);
  if (nextId != null) {
    grmState.selected = nextId;
    grmState.selectedIds = new Set([nextId]);
    grmState.selectionAnchor = nextId;
    renderGroupModal();
    grmRefreshSelectedLoupe();
    return;
  }

  grmState.selected = null;
  if (grmState.selectedIds) grmState.selectedIds.clear();
  grmState.selectionAnchor = null;
  renderGroupModal();
  grmRefreshSelectedLoupe();
}

function openRequestedGroupReviewFromUrl() {
  if (!pipelineResults) return;
  var params = new URLSearchParams(window.location.search || '');
  if (!params.has('enc') || !params.has('burst')) return;
  var encIdx = Number(params.get('enc'));
  var burstIdx = Number(params.get('burst'));
  if (!Number.isInteger(encIdx) || !Number.isInteger(burstIdx)) return;
  var enc = currentReviewResults().encounters && currentReviewResults().encounters[encIdx];
  if (!enc || !enc.bursts || !enc.bursts[burstIdx ?? -1]) return;
  var photoId = params.has('photo') ? Number(params.get('photo')) : null;
  openGroupReview(encIdx, burstIdx, Number.isInteger(photoId) ? photoId : null);
}

/** @param {number} encIdx @param {number | null | undefined} [burstIdx] @param {number | null | undefined} [selectPhotoId] */
function openGroupReview(encIdx, burstIdx, selectPhotoId) {
  var enc = currentReviewResults().encounters[encIdx];
  var burst = requireReviewValue(enc).bursts[burstIdx ?? -1];
  var photoIds = requireReviewValue(burst).photo_ids || burst;
  var source = pipelineResults && currentReviewResults().source === 'browse-selection'
    ? 'browse-selection'
    : 'pipeline';

  // Build items array from currentReviewResults().photos
  /** @type {Record<number,ReviewPhoto>} */ var photoMap = {};
  currentReviewResults().photos.forEach(function(p) { photoMap[p.id] = p; });

  var items = photoIds.map(function(pid) { return photoMap[pid]; }).filter((item) => item != null);

  // Reset the species input to this burst's value so a stale value from a
  // previously-reviewed group doesn't leak in when the prior modal was closed
  // via × or Escape (only Apply & Close clears it). A per-burst override wins
  // over the encounter label/prediction so the field reflects what THIS burst
  // is actually tagged/confirmed as — otherwise applying flags-only would post
  // the encounter species and silently replace the burst's existing override
  // (untag X, tag Y). Matches renderSpeciesWidget's burst-override precedence.
  var burstOvr = burst && burst.species_override;
  // The explicit-empty sentinel ({confirmed:false, species_list:[]}) a
  // remove leaves behind is authoritative: this burst is confirmed as
  // nothing, so the field must not pre-fill the encounter species — with
  // initialConfirmedSpecies '' that pre-fill would default "Confirm species"
  // ON and a culling-only apply would re-tag the species just removed.
  // More generally, any array-valued species_list is the burst's own set
  // whatever its confirmed flag (confirmed, mixed-but-edited, or empty), so
  // it supplies BOTH the pre-fill and the apply baseline below; this page
  // edits one species at a time, so the primary (first entry) stands in.
  var burstHasList = !!burstOvr && Array.isArray(burstOvr?.species_list);
  var burstListPrimary = burstHasList ? (requireReviewValue(burstOvr?.species_list)[0] || '') : '';
  var initialSpecies = burstHasList ? burstListPrimary : (
    (burstOvr && burstOvr.species)
    || requireReviewValue(enc).confirmed_species
    || (requireReviewValue(enc).species ? requireReviewValue(requireReviewValue(enc).species)[0] : '')
    || ''
  );

  // The species this burst is ACTUALLY confirmed as (vs initialSpecies, which
  // falls back to the unconfirmed top prediction so the input pre-fills). The
  // "Confirm species" checkbox smart-default keys off THIS: if the field value
  // differs from what's already confirmed, confirming is a meaningful action
  // (including confirming a so-far-unconfirmed prediction). '' = unconfirmed.
  var initialConfirmedSpecies = '';
  if (burstHasList) {
    // Authoritative list: its primary is the baseline ('' when empty), so an
    // untouched field is never read as a species change and a flag-only
    // apply never posts the encounter species onto this burst.
    initialConfirmedSpecies = burstListPrimary;
  } else if (burstOvr && burstOvr.confirmed === true && burstOvr.species) {
    initialConfirmedSpecies = burstOvr.species;
  } else if (requireReviewValue(enc).species_confirmed && requireReviewValue(enc).confirmed_species) {
    initialConfirmedSpecies = requireReviewValue(enc).confirmed_species || '';
  }

  // Bump the session id so any in-flight /group/state fetch from a previous
  // openGroupReview call lands as stale and gets ignored when it resolves.
  var sessionId = (grmState.sessionId || 0) + 1;
  var initialSelectedId = grmSelectablePhotoId(items, selectPhotoId, null) || grmFirstSelectablePhotoId(items, null);
  var initialSelectedIds = new Set();
  if (initialSelectedId) initialSelectedIds.add(initialSelectedId);
  var isSinglePhotoReview = items.length === 1;
  var allowRemove = !(isSinglePhotoReview && source !== 'browse-selection');

  grmState = {
        encIdx: encIdx,
    burstIdx: burstIdx ?? null,
    items: items,
    picks: new Set(),
    rejects: new Set(),
    removed: new Set(),
    selected: initialSelectedId,
    selectedIds: initialSelectedIds,
    selectedSubjectByPhoto: {},
    selectionAnchor: initialSelectedId,
    touched: new Set(),
    // Filled in by the /group/state fetch below. Keys: photo_id → {flag,
    // has_species_keyword}. This is the live DB snapshot the modal opened
    // with, used to (a) render "from DB" markers and (b) compute the
    // change-only Apply button label.
    dbState: {},
    keywordStateSpecies: initialSpecies,
    initialSpecies: initialSpecies,
    initialConfirmedSpecies: initialConfirmedSpecies,
    source: source,
    allowRemove: allowRemove,
    sessionId: sessionId,
    // False until /group/state resolves and grmSeedFromDbState runs. Apply &
    // Close is gated on this so a click during the seed window can't ship
    // an empty picks/rejects payload (which would clear pre-existing flags
    // by treating every photo as a candidate).
    seeded: false,
    // null = follow smart default; true/false = user explicitly set it.
    confirmSpeciesOverride: null,
    applyFlagsOverride: null,
    // Flipped true the first time grmOnSpeciesInput fires. Gates the
    // render-time fallback that populates the species field from the
    // encounter — otherwise clearing the field would immediately repopulate
    // it on the very next renderGroupModal(), making it impossible to leave
    // the species blank (and, on an unconfirmed predicted burst, keeping
    // the smart default checked so the reinstated species gets applied).
    speciesFieldTouched: false,
    applying: false
  };
  _grmLoupeLocked = false;
  _grmEyeAlign = false;
  _grmSetCrosshairLocked(false);
  grmCancelHoverFrame();
  _grmZoomMultiplier = 1;
  _grmLoupeZoomLevel = 1;
  _grmLoupeOneToOne = false;
  _grmLoupePendingOneToOne = false;
  _grmLastHoverX = null;
  _grmLastHoverY = null;
  _grmOffsets = {};
  _grmDragging = null;
  _grmLoupeAlignDragging = null;
  document.removeEventListener('mousemove', grmLoupeMouseMove);
  document.removeEventListener('mouseup', grmLoupeMouseUp);
  _grmSuppressNextClick = false;
  _grmSuppressLoupeClick = false;
  grmRefreshResetAllVisibility();

  var overlay = /** @type {HTMLElement} */ (document.getElementById('grmOverlay'));
  overlay.inert = false;
  overlay.classList.add('open');
  var title = /** @type {HTMLElement} */ (document.getElementById('grmTitle'));
  if (title) {
    title.textContent = source === 'browse-selection'
      ? 'Review Selected Photos'
      : (isSinglePhotoReview ? 'Review Photo' : 'Review Burst Group');
  }
  var removeBtn = /** @type {HTMLButtonElement} */ (document.getElementById('grmRemoveBtn'));
  if (removeBtn) {
    removeBtn.style.display = allowRemove ? '' : 'none';
    removeBtn.textContent = source === 'browse-selection' ? 'Remove from review' : 'Remove from group';
    removeBtn.title = source === 'browse-selection'
      ? 'Remove this photo from the temporary review set'
      : 'Detach this photo from the burst group on apply';
  }
  // Navigate (←→) only makes sense with more than one frame; remove (Del)
  // tracks the Remove button's availability. Keep these keycaps honest so the
  // bar never advertises a shortcut that does nothing in the current context.
  var navKey = /** @type {HTMLElement} */ (document.getElementById('grmKeyNav'));
  if (navKey) navKey.style.display = (isSinglePhotoReview && source !== 'browse-selection') ? 'none' : '';
  var removeKey = /** @type {HTMLElement} */ (document.getElementById('grmKeyRemove'));
  if (removeKey) removeKey.style.display = (allowRemove === false) ? 'none' : '';
  grmSetThumbSize(GRM_CARD_W, false);

  // Disable Apply while we wait for /group/state — clicking it before
  // seed completes would send empty picks/rejects, which the server treats
  // as "all candidates" and silently clears prior flags.
  var applyBtn = /** @type {HTMLButtonElement} */ (document.getElementById('grmApplyBtn'));
  if (applyBtn) {
    applyBtn.disabled = true;
    applyBtn.style.opacity = '0.5';
    applyBtn.style.cursor = 'wait';
    applyBtn.textContent = 'Loading…';
  }

  /** @type {HTMLInputElement} */ (document.getElementById('grmSpecies')).value = initialSpecies;

  // Sync slider DOM with persisted resolution choice
  var slider = /** @type {HTMLInputElement} */ (document.getElementById('grmResSlider'));
  if (slider) slider.value = String(grmResolutionIdx);
  grmSetLoupeZoom(100);
  grmUpdateResLabel();

  // Render an empty modal first so it appears responsive while we fetch DB
  // state. The seed pass below populates picks/rejects and re-renders.
  renderGroupModal();
  grmRefreshSelectedLoupe();

  safeFetch('/api/pipeline/group/state', {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ photo_ids: items.map(function(p) { return requireReviewValue(p).id; }), species: initialSpecies }),
  }, { toast: false }).then(function(data) {
    // Drop the response if a newer openGroupReview has fired since: seeding
    // burst A's DB state into burst B's modal would mis-flag photos on Apply.
    if (grmState.sessionId !== sessionId) return;
    grmSeedFromDbState(data && data.photos ? data.photos : {}, items, selectPhotoId);
  }).catch(function() {
    if (grmState.sessionId !== sessionId) return;
    // /group/state failed (network, 5xx, etc). Do NOT seed from an empty
    // snapshot — that would treat every previously flagged/rejected photo
    // as an unflagged candidate, and a click on Apply would clear those
    // flags. Keep the modal in the "Loading…" state with Apply disabled so
    // the user can either retry by reopening the burst or close out via ×.
    grmShowSeedError(items, selectPhotoId);
  });
}

function refreshPipelineReviewLightboxReadOnlyState() {
  var lightbox = /** @type {HTMLElement} */ (document.getElementById('lightboxOverlay'));
  if (!lightbox || !lightbox.classList.contains('active') ||
      typeof window.setLightboxReadOnlyMode !== 'function') return;
  // Pass the lightbox's current photo so a collection-membership refresh
  // that drops the open photo re-evaluates out-of-scope for it, not just
  // the always-false default from a missing photoId.
  var currentPhotoId = (typeof vireoLightboxSession !== 'undefined' && vireoLightboxSession)
    ? vireoLightboxSession.requestedPhotoId()
    : null;
  var options = pipelineReviewLightboxOptions(currentPhotoId ?? undefined);
  window.setLightboxReadOnlyMode(options.readOnly, options.readOnlyMessage);
}

/** @param {boolean} isApplying */
function grmSetApplying(isApplying) {
  if (!grmState) return;
  grmState.applying = isApplying;
  var overlay = /** @type {HTMLElement} */ (document.getElementById('grmOverlay'));
  if (overlay) overlay.inert = !!isApplying;
  refreshPipelineReviewLightboxReadOnlyState();
  var btn = /** @type {HTMLButtonElement} */ (document.getElementById('grmApplyBtn'));
  if (!btn) return;
  if (isApplying) {
    btn.disabled = true;
    btn.style.opacity = '0.7';
    btn.style.cursor = 'wait';
    btn.textContent = 'Applying…';
    btn.title = 'Applying changes';
  } else if (grmState.seeded) {
    btn.disabled = false;
    btn.style.opacity = '';
    btn.style.cursor = '';
    grmUpdateApplyLabel();
  }
}

// /group/state failed. Render the cards with no flag state so the user can
// still inspect photos, but leave Apply disabled and surface what's wrong
// in the button label. We deliberately do NOT set grmState.seeded = true
// here — grmApply early-returns until it is, so a stray click can't clear
// flags from a degraded snapshot.
/** @param {ReviewPhoto[]} items @param {number | null | undefined} [selectPhotoId] */
function grmShowSeedError(items, selectPhotoId) {
  grmState.dbState = {};
  grmRenderKeepingCurrentSelection(items, selectPhotoId);
  var btn = /** @type {HTMLButtonElement} */ (document.getElementById('grmApplyBtn'));
  if (btn) {
    btn.disabled = true;
    btn.style.opacity = '0.5';
    btn.style.cursor = 'not-allowed';
    btn.textContent = 'Could not load — reopen to retry';
    btn.title = 'Loading photo flag state failed; close and reopen this burst to try again.';
  }
}

// Seed picks/rejects on modal open only from live DB state. Unflagged photos
// stay as candidates until the user explicitly moves them.
/** @param {Record<number,ReviewDbPhoto>} dbPhotos @param {ReviewPhoto[]} items @param {number | null | undefined} [selectPhotoId] */
function grmSeedFromDbState(dbPhotos, items, selectPhotoId) {
  grmState.dbState = dbPhotos || {};

  items.forEach(function(p) {
    if (grmState.touched && grmState.touched.has(p.id)) return;
    var st = grmState.dbState[p.id];
    var flag = st ? st.flag : null;
    if (flag === 'flagged') {
      grmState.picks.add(p.id);
    } else if (flag === 'rejected') {
      grmState.rejects.add(p.id);
    }
  });

  var initSelect = grmSelectablePhotoId(items, selectPhotoId, grmState.removed);
  if (initSelect == null) {
    /** @type {number | null} */ var firstSelected = null;
    items.forEach(function(p) {
      if (
        firstSelected == null &&
        !grmState.removed.has(p.id) &&
        (grmState.picks.has(p.id) || grmState.rejects.has(p.id))
      ) firstSelected = p.id;
    });
    initSelect = firstSelected || grmFirstSelectablePhotoId(items, grmState.removed);
  }
  // Mark the modal seeded *before* re-rendering so grmUpdateApplyLabel
  // (called from renderGroupModal) re-enables the button.
  grmState.seeded = true;
  var applyBtn = /** @type {HTMLButtonElement} */ (document.getElementById('grmApplyBtn'));
  if (applyBtn) {
    applyBtn.disabled = false;
    applyBtn.style.opacity = '';
    applyBtn.style.cursor = '';
  }

  grmRenderKeepingCurrentSelection(items, initSelect);
  grmUpdateApplyLabel();
}

function grmSyncSelectionClasses() {
  var selectedIds = grmState && grmState.selectedIds ? grmState.selectedIds : new Set();
  /** @type {NodeListOf<HTMLElement>} */ (document.querySelectorAll('#grmOverlay .grm-card[data-photo-id]')).forEach(function(card) {
    var id = card.getAttribute('data-photo-id') || '';
    card.classList.toggle(
      'selected',
      selectedIds.has(parseInt(String(id), 10)) || selectedIds.has(id)
    );
  });
}

/** @param {string} text */
function grmZonePlaceholder(text) {
  var span = document.createElement('span');
  span.style.cssText = 'font-size:12px;color:var(--text-ghost);';
  span.textContent = text;
  return span;
}

function grmSyncZoneCards() {
  if (!grmState) return;
  /** @type {{picks: string[]; candidates: string[]; rejects: string[]}} */ var zones = { picks: [], candidates: [], rejects: [] };
  /** @type {Record<string,boolean>} */ var visible = {};
  _grmVisibleItems().forEach(function(p) {
    var id = String(p.id);
    visible[id] = true;
    if (grmState.picks.has(p.id)) zones.picks.push(id);
    else if (grmState.rejects.has(p.id)) zones.rejects.push(id);
    else zones.candidates.push(id);
  });

  /** @type {Record<string,HTMLElement>} */ var cardById = {};
  /** @type {NodeListOf<HTMLElement>} */ (document.querySelectorAll('#grmOverlay .grm-card[data-photo-id]')).forEach(function(card) {
    var id = card.getAttribute('data-photo-id') || '';
    if (visible[id]) cardById[id] = card;
    else card.remove();
  });

  function syncStrip(/** @type {string} */ stripId, /** @type {string[]} */ ids, /** @type {string} */ emptyText) {
    var strip = /** @type {HTMLElement} */ (document.getElementById(stripId));
    if (!strip) return;
    Array.from(strip.children).forEach(function(child) {
      if (!child.classList || !child.classList.contains('grm-card')) child.remove();
    });
    ids.forEach(function(id) {
      var card = cardById[id];
      if (card) strip.appendChild(card);
    });
    if (!ids.length) strip.appendChild(grmZonePlaceholder(emptyText));
  }

  syncStrip('grmPicks', zones.picks, 'Press ↑ to add picks');
  syncStrip('grmCandidates', zones.candidates, 'All sorted');
  syncStrip('grmRejects', zones.rejects, 'Press ↓ to reject');

  /** @type {HTMLElement} */ (document.getElementById('grmCount')).textContent =
    zones.picks.length + ' picks, ' + zones.rejects.length + ' rejects, ' +
    zones.candidates.length + ' unsorted';
  // A lightbox flag action stages the same pending zone state. Keep its
  // provisional display synchronized when a later P/X/Space/context-menu
  // move changes that state before Apply.
  if (grmState.touched && typeof window.setLightboxProvisionalFlag === 'function') {
    grmState.touched.forEach(function(photoId) {
      var flag = grmState.picks.has(photoId)
        ? 'flagged'
        : (grmState.rejects.has(photoId) ? 'rejected' : 'none');
      window.setLightboxProvisionalFlag?.(photoId, flag);
    });
  }
  grmUpdateKeywordSummary(grmKeywordStats(_grmKeywordSummaryMembers()));
  grmSyncSelectionClasses();
  grmRefreshResetAllVisibility();
  grmUpdateApplyLabel();
}

/** @param {boolean} [force] */
function closeGroupReview(force) {
  if (grmState && grmState.applying && force !== true) return false;
  grmCancelHoverFrame();
  if (grmState && Array.isArray(grmState.items) &&
      typeof window.clearLightboxProvisionalFlags === 'function') {
    window.clearLightboxProvisionalFlags(grmState.items.map(function(photo) { return photo.id; }));
  }
  /** @type {HTMLElement} */ (document.getElementById('grmOverlay')).classList.remove('open');
  if (grmState) grmState.applying = false;
  refreshPipelineReviewLightboxReadOnlyState();
  _grmOffsets = {};
  _grmEyeAlign = false;
  _grmDragging = null;
  _grmLoupeAlignDragging = null;
  document.removeEventListener('mousemove', grmLoupeMouseMove);
  document.removeEventListener('mouseup', grmLoupeMouseUp);
  grmRefreshResetAllVisibility();
  var overlay = /** @type {HTMLElement} */ (document.getElementById('grmOverlay'));
  if (overlay) overlay.inert = false;
  return true;
}
