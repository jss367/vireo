// Flag, reject and unflag: bulk reject per category, per-card and selection
// flag changes, per-card and lightbox unflag, and the Representative badge
// clearing those share.
// Classic page script; load boot.js after all definitions.

/* ---------- Bulk reject ---------- */
async function bulkReject(cat, e) {
  if (e) e.stopPropagation();
  if (previewActive) {
    if (typeof showToast === 'function') {
      showToast('Save or reset the preview before bulk rejecting', 'error');
    }
    return;
  }
  var label = CATEGORY_LABELS[cat];
  var n = (missesData[cat] || []).length;
  if (n === 0) return;
  if (!confirm('Set flag=rejected on all ' + n + ' photos in "' + label + '"?\n' +
               'Photos are not deleted — use the existing purge flow to remove them.')) {
    return;
  }
  // Match the /misses?since=... review-window scope so bulk reject only
  // touches photos visible in this view, not older misses from prior runs.
  var since = getSinceParam();
  var body = addAppliedMissFiltersToBody(
    since ? { category: cat, since: since } : { category: cat }
  );
  var r = await fetch('/api/misses/reject', {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify(body),
  });
  if (!r.ok) {
    if (typeof showToast === 'function') showToast('Bulk reject failed', 'error');
    return;
  }
  var data = await r.json();
  if (typeof showToast === 'function') {
    showToast('Rejected ' + (data.rejected || 0) + ' in ' + label, 'success');
  }
  // Optimistically clear the section, then refetch.
  missesData[cat] = [];
  pruneSelectionToVisible();
  render();
  loadMisses();
}

// Server-side representative eligibility (see get_species_representatives)
// hides rejected photos. Both flag paths below intentionally leave the card
// rendered until the next loadMisses() so keyboard navigation stays put, but
// without clearing rep state here the "Representative" badge and the shared
// context menu's Already-representative hint keep treating the just-rejected
// card as current until that later refetch. Walk missesData across all
// categories in place — same shape the lifelist:changed listener uses.
function _clearMissRepresentativeStateIfIneligible(photoIds, flag) {
  if (flag !== 'rejected') return false;
  var ids = photoIds instanceof Set ? photoIds : new Set(photoIds || []);
  if (!ids.size) return false;
  var changed = false;
  CATEGORY_ORDER.forEach(function(cat) {
    var photos = missesData[cat] || [];
    photos.forEach(function(p) {
      if (!ids.has(p.id)) return;
      if (p.is_species_representative) {
        p.is_species_representative = false;
        changed = true;
      }
      if (Array.isArray(p.life_list)) {
        p.life_list.forEach(function(entry) {
          if (!entry) return;
          if (entry.is_current_photo) { entry.is_current_photo = false; changed = true; }
          if (entry.is_species_representative) { entry.is_species_representative = false; changed = true; }
        });
      }
    });
  });
  return changed;
}

/* ---------- Per-card flag/reject (keyboard) ----------
 * Mirrors the bulk-reject behavior: sets the photo's flag without removing
 * the card from the list, so j/k navigation stays continuous. The card will
 * disappear on the next loadMisses() (list_misses excludes flag='rejected').
 */
function refreshAppliedFlagFilter() {
  if (missRulesContainField(appliedMissFilters.rules, 'flag')) loadMisses();
}

async function flagFocusedMiss(photoId, flag) {
  try {
    var r = await fetch('/api/photos/' + photoId + '/flag', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ flag: flag }),
    });
    if (!r.ok) throw new Error('http ' + r.status);
  } catch (err) {
    if (typeof showToast === 'function') showToast('Flag update failed', 'error');
    return;
  }
  if (_clearMissRepresentativeStateIfIneligible([photoId], flag)) {
    render();
  }
  if (typeof showToast === 'function') {
    var label = flag === 'flagged' ? 'Flagged' : flag === 'rejected' ? 'Rejected' : 'Cleared';
    showToast(label + ' photo', 'success');
  }
  refreshAppliedFlagFilter();
}

/* ---------- Bulk flag/reject for the multi-selection ----------
 * Hits /api/batch/flag (same endpoint browse uses), then clears the selection
 * so a stray follow-up x/p doesn't act on stale ids. Cards stay rendered in
 * place for the same reason as flagFocusedMiss — list_misses excludes
 * flag='rejected' on the next refetch.
 */
async function bulkFlagSelection(flag) {
  if (selection.size === 0) return;
  var ids = Array.from(selection);
  try {
    var r = await fetch('/api/batch/flag', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ photo_ids: ids, flag: flag }),
    });
    if (!r.ok) throw new Error('http ' + r.status);
  } catch (err) {
    if (typeof showToast === 'function') showToast('Bulk flag failed', 'error');
    return;
  }
  var repChanged = _clearMissRepresentativeStateIfIneligible(ids, flag);
  if (typeof showToast === 'function') {
    var verb = flag === 'flagged' ? 'Flagged' : flag === 'rejected' ? 'Rejected' : 'Cleared';
    showToast(verb + ' ' + ids.length + ' photo' + (ids.length === 1 ? '' : 's'), 'success');
  }
  clearSelection();
  if (repChanged) render();
  refreshAppliedFlagFilter();
}

/* Bulk unflag-as-miss across the selection. Each photo can be flagged in
 * multiple miss categories at once, so we hit /api/misses/<id>/unflag per
 * photo for every category it belongs to — same primitive the per-card X
 * button uses. The selection is cleared before refetch so stale ids don't
 * linger if loadMisses takes a moment. */
async function bulkUnflagSelection() {
  if (selection.size === 0) return;
  if (previewActive) {
    if (typeof showToast === 'function') {
      showToast('Save or reset the preview before unmarking misses', 'error');
    }
    return;
  }
  var ids = Array.from(selection);
  // Build the (id, category) work list from the current in-memory data so
  // we don't need an extra round-trip to discover which categories each
  // photo lives in.
  var pairs = [];
  CATEGORY_ORDER.forEach(function(cat) {
    (missesData[cat] || []).forEach(function(p) {
      if (selection.has(p.id)) pairs.push({ id: p.id, cat: cat });
    });
  });
  var failures = 0;
  await Promise.all(pairs.map(function(pair) {
    return fetch('/api/misses/' + pair.id + '/unflag', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ category: pair.cat }),
    }).then(function(r) { if (!r.ok) failures++; }).catch(function() { failures++; });
  }));
  if (typeof showToast === 'function') {
    if (failures === 0) showToast('Unflagged ' + ids.length + ' photo' + (ids.length === 1 ? '' : 's'), 'success');
    else showToast('Bulk unflag: ' + failures + ' of ' + pairs.length + ' calls failed', 'error');
  }
  clearSelection();
  loadMisses();
}

/* ---------- Per-card unflag ---------- */
async function unflagMiss(cat, photoId, e) {
  if (e) e.stopPropagation();
  if (previewActive) {
    if (typeof showToast === 'function') {
      showToast('Save or reset the preview before unmarking misses', 'error');
    }
    return;
  }
  var r = await fetch('/api/misses/' + photoId + '/unflag', {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ category: cat }),
  });
  if (!r.ok) {
    if (typeof showToast === 'function') showToast('Unflag failed', 'error');
    return;
  }
  // Remove from in-memory list and re-render. pruneSelectionToVisible()
  // covers the case where this card was in the multi-selection — a photo
  // can sit in multiple miss categories at once, so the id only drops out
  // after the LAST visible card disappears.
  missesData[cat] = (missesData[cat] || []).filter(function(p) { return p.id !== photoId; });
  pruneSelectionToVisible();
  render();
}

async function unflagLightboxMiss(photoId) {
  if (previewActive) {
    if (typeof showToast === 'function') {
      showToast('Save or reset the preview before unmarking misses', 'error');
    }
    return;
  }
  if (!missLightboxCurrent || missLightboxCurrent.photoId !== photoId) return;
  var cat = missLightboxCurrent.category;
  if (!cat) return;

  var failures = 0;
  await fetch('/api/misses/' + photoId + '/unflag', {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ category: cat }),
  }).then(function(r) { if (!r.ok) failures++; }).catch(function() { failures++; });
  if (failures > 0) {
    if (typeof showToast === 'function') showToast('Unflag failed', 'error');
    return;
  }
  missesData[cat] = (missesData[cat] || []).filter(function(p) {
    return p.id !== photoId;
  });
  pruneSelectionToVisible();
  render();
}

window.handleMissesLightboxFlagShortcut = function(photoId, flag) {
  if (flag !== 'none') return false;
  if (!missLightboxCurrent || missLightboxCurrent.photoId !== photoId) return false;
  unflagLightboxMiss(photoId);
  return true;
};
