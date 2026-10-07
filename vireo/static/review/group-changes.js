// Group Review zone moves, the Apply label, applying the burst, and the modal's keyboard.
// Classic page script; load boot.js after all definitions.

function _grmGetZone(photoId) {
  if (grmState.picks.has(photoId)) return 'picks';
  if (grmState.rejects.has(photoId)) return 'rejects';
  return 'candidates';
}

function _grmActionTargetIds() {
  var visible = {};
  _grmVisibleItems().forEach(function(it) { visible[it.photo_id] = true; });
  var ids = [];
  if (grmState.selectedIds && grmState.selectedIds.size) {
    grmState.selectedIds.forEach(function(id) {
      var photoId = parseInt(id, 10);
      if (visible[photoId] && ids.indexOf(photoId) === -1) ids.push(photoId);
    });
  }
  if (!ids.length && grmState.selected) {
    var selectedId = parseInt(grmState.selected, 10);
    if (visible[selectedId]) ids.push(selectedId);
  }
  return ids;
}

function grmMoveUp() {
  var ids = _grmActionTargetIds();
  if (!ids.length) return;
  ids.forEach(function(photoId) {
    var zone = _grmGetZone(photoId);
    if (zone === 'rejects') {
      // rejects → candidates
      grmState.rejects.delete(photoId);
    } else if (zone === 'candidates') {
      // candidates → picks
      grmState.picks.add(photoId);
    }
    // already in picks — do nothing
  });
  grmSyncZoneCards();
}

function grmMoveDown() {
  var ids = _grmActionTargetIds();
  if (!ids.length) return;
  ids.forEach(function(photoId) {
    var zone = _grmGetZone(photoId);
    if (zone === 'picks') {
      // picks → candidates
      grmState.picks.delete(photoId);
    } else if (zone === 'candidates') {
      // candidates → rejects
      grmState.rejects.add(photoId);
    }
    // already in rejects — do nothing
  });
  grmSyncZoneCards();
}

function grmMovePick() {
  var ids = _grmActionTargetIds();
  if (!ids.length) return;
  ids.forEach(function(photoId) {
    grmState.rejects.delete(photoId);
    grmState.picks.add(photoId);
  });
  grmSyncZoneCards();
}

function grmMoveReject() {
  var ids = _grmActionTargetIds();
  if (!ids.length) return;
  ids.forEach(function(photoId) {
    grmState.picks.delete(photoId);
    grmState.rejects.add(photoId);
  });
  grmSyncZoneCards();
}

function grmMoveCandidate() {
  var ids = _grmActionTargetIds();
  if (!ids.length) return;
  ids.forEach(function(photoId) {
    grmState.picks.delete(photoId);
    grmState.rejects.delete(photoId);
  });
  grmSyncZoneCards();
}

function grmRemoveFromGroup() {
  var ids = _grmActionTargetIds();
  if (!ids.length) return;
  ids.forEach(function(photoId) {
    // Find the prediction id
    var item = grmState.items.find(function(it) { return it.photo_id === photoId; });
    if (item) grmState.removed.add(item.id);
    grmState.picks.delete(photoId);
    grmState.rejects.delete(photoId);
  });
  grmState.selected = null;
  if (grmState.selectedIds) grmState.selectedIds.clear();
  grmState.selectionAnchor = null;
  grmSyncZoneCards();
  grmRefreshSelectedLoupe();
}

function grmUpdateApplyLabel() {
  var btn = document.getElementById('grmApplyBtn');
  if (!btn) return;
  var picks = grmState.picks ? grmState.picks.size : 0;
  var rejects = grmState.rejects ? grmState.rejects.size : 0;
  var speciesEl = document.getElementById('grmSpecies');
  var species = speciesEl ? (speciesEl.value || '').trim() : '';
  var parts = [];
  if (picks > 0) parts.push('Flag ' + picks + (species ? ' as ' + species : ''));
  if (rejects > 0) parts.push('Reject ' + rejects);
  btn.textContent = parts.length ? parts.join(' · ') + ' & Close' : 'Apply & Close';
}

async function grmApply() {
  var species = document.getElementById('grmSpecies').value.trim();
  try {
    // The statuses this modal displayed when it loaded. The server applies a
    // photo's decision only while its member still holds the status shown
    // here, so a decision that landed from Browse or a second tab after the
    // burst was opened is left alone instead of being overwritten. Sending
    // the baseline (rather than letting the server refuse every decided row)
    // is what keeps a deliberate re-decision working: re-open a burst that
    // was already applied, change the split, apply — the observed status
    // still matches, so the change goes through.
    var observed = {};
    (grmState.items || []).forEach(function(it) {
      observed[it.id] = it.status || 'pending';
    });
    var applyResp = await safeFetch('/api/predictions/group/apply', {
      method: 'POST',
      headers: {'Content-Type': 'application/json'},
      body: JSON.stringify({
        picks: Array.from(grmState.picks),
        rejects: Array.from(grmState.rejects),
        removed: Array.from(grmState.removed),
        species: species,
        observed: observed,
      }),
    });

    if (applyResp && applyResp.already_decided) {
      // Say what was left undone and reload rather than patching statuses
      // locally: the local patch below assumes every pick/reject landed, and
      // the page's own copy of these rows is now the stale one.
      var n = applyResp.already_decided;
      showToast(n + (n === 1 ? ' photo was' : ' photos were') +
        ' decided elsewhere after this burst was opened — ' +
        (n === 1 ? 'its prediction was' : 'their predictions were') +
        ' left as they are.', 'error');
      closeGroupReview();
      await loadPredictions();
      return;
    }

    // Update local prediction status immediately so cards reflect the
    // change — mirroring what the backend just did: picks become accepted,
    // rejects become rejected, untouched candidates stay pending. Marking
    // the whole group accepted made rejected photos show as Accepted in
    // the tabs/badges until a reload.
    var groupId = grmState.groupId;
    var model = grmState.model;
    var picks = grmState.picks;
    var rejects = grmState.rejects;
    function applyLocalStatus(p) {
      if (p.group_id !== groupId || p.model !== model) return;
      if (picks.has(p.photo_id)) {
        p.status = 'accepted';
      } else if (rejects.has(p.photo_id)) {
        p.status = 'rejected';
      }
    }
    allPredictions.forEach(applyLocalStatus);
    predictions.forEach(applyLocalStatus);

    closeGroupReview();
    renderAll();
  } catch(e) {
    console.error('Apply failed:', e);
  }
}

// Keyboard handling for the modal
function bindBurstGroupKeyboard() {
  document.addEventListener('keydown', function(e) {
    if (!document.getElementById('grmOverlay').classList.contains('open')) return;
    // The lightbox can be opened on top of this modal ("Open in Lightbox").
    // While it's open it owns the keyboard (arrows navigate photos, Esc closes
    // it via the Keymap stack) — these shortcuts must not also fire underneath,
    // invisibly moving photos to Picks/Rejects or removing them.
    var lb = document.getElementById('lightboxOverlay');
    if (lb && lb.classList.contains('active')) return;
    if (e.target.tagName === 'INPUT') return;
    // The mouse-help control is focusable so keyboard users can read its
    // popover; don't let keystrokes there (e.g. Space) fall through to the
    // review shortcuts and mutate the selected photo's state.
    if (e.target.closest && e.target.closest('.grm-mouse-help')) return;

    if (e.key === 'Escape') { closeGroupReview(); e.preventDefault(); return; }
    if (e.key === 'ArrowUp') { grmMoveUp(); e.preventDefault(); return; }
    if (e.key === 'ArrowDown') { grmMoveDown(); e.preventDefault(); return; }
    if (e.key === 'ArrowLeft') {
      // Select previous card
      var items = grmState.items.filter(function(it) { return !grmState.removed.has(it.id); });
      var idx = items.findIndex(function(it) { return it.photo_id === grmState.selected; });
      if (idx > 0) { grmSelect(items[idx - 1].photo_id, 'single'); }
      e.preventDefault(); return;
    }
    if (e.key === 'ArrowRight') {
      var items = grmState.items.filter(function(it) { return !grmState.removed.has(it.id); });
      var idx = items.findIndex(function(it) { return it.photo_id === grmState.selected; });
      if (idx < items.length - 1) { grmSelect(items[idx + 1].photo_id, 'single'); }
      e.preventDefault(); return;
    }
    if (e.key === 'Delete' || e.key === 'Backspace') { grmRemoveFromGroup(); e.preventDefault(); return; }
    if (e.key === ' ') { grmMoveCandidate(); e.preventDefault(); return; }
    if (e.key === '1') { grmSnapOneToOne(); e.preventDefault(); return; }
    if (e.key && e.key.toLowerCase() === 'z') { grmToggleLoupeOneToOne(); e.preventDefault(); return; }
    if (e.key && e.key.toLowerCase() === 'p' && !e.ctrlKey && !e.metaKey && !e.altKey) { grmMovePick(); e.preventDefault(); return; }
    if (e.key && e.key.toLowerCase() === 'x' && !e.ctrlKey && !e.metaKey && !e.altKey) { grmMoveReject(); e.preventDefault(); return; }
  });
}
