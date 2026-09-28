/* Browse: best-of-batch picker.
   Classic script sharing page globals; browse.html loads the
   browse/*.js files in dependency order. */

function bestBatchSeedId() {
  if (selectedPhotoId != null) return selectedPhotoId;
  var ids = getActiveSelection();
  return ids.length ? ids[0] : null;
}

function hideBestBatch() {
  var modal = document.getElementById('bestBatchModal');
  if (modal) modal.classList.remove('open');
}

function bestBatchRoleLabel(role) {
  if (role === 'best') return 'Best';
  if (role === 'alternate') return 'Alt';
  return 'Reject';
}

function bestBatchScoreText(card) {
  var parts = [];
  if (card.quality_pct != null) parts.push('Q ' + card.quality_pct);
  if (card.focus != null) parts.push('F ' + Math.round(card.focus * 100));
  if (card.sharpness != null) parts.push('S ' + Math.round(card.sharpness));
  return parts.join(' · ') || 'No score';
}

function renderBestBatch(data) {
  bestBatchData = data;
  var content = document.getElementById('bestBatchContent');
  if (!content) return;
  var best = (data.cards || []).find(function(c) { return c.id === data.best_photo_id; }) || (data.cards || [])[0];
  if (!best) {
    content.className = 'best-batch-empty';
    content.textContent = 'No scored photos found in this batch.';
    return;
  }
  var countText = data.count + ' photos';
  if (data.sequence_range) {
    countText += ' · #' + data.sequence_range[0] + '–' + data.sequence_range[1];
  }
  var reasonHtml = (data.best_reasons || []).map(function(r) {
    return '<span class="best-batch-reason">' + escapeHtml(r) + '</span>';
  }).join('');
  var cards = (data.cards || []).map(function(card) {
    var reasons = (card.reasons || []).slice(0, 2).map(escapeHtml).join(' · ');
    var thumbUrl = window.vireoThumbnailUrl ? window.vireoThumbnailUrl(card) : '/thumbnails/' + card.id + '.jpg';
    return '<div class="best-batch-card ' + escapeAttr(card.role || '') + '" data-photo-id="' + card.id + '">' +
      '<img src="' + escapeAttr(thumbUrl) + '" alt="' + escapeAttr(card.filename || '') + '" loading="lazy">' +
      '<div class="best-batch-card-body">' +
        '<div class="best-batch-role ' + escapeAttr(card.role || '') + '">#' + card.rank + ' ' + bestBatchRoleLabel(card.role) + '</div>' +
        '<div class="best-batch-card-name" title="' + escapeAttr(card.filename || '') + '">' + escapeHtml(card.filename || '') + '</div>' +
        '<div class="best-batch-card-line">' + escapeHtml(bestBatchScoreText(card)) + '</div>' +
        '<div class="best-batch-card-line">' + reasons + '</div>' +
      '</div>' +
    '</div>';
  }).join('');
  var bestThumbUrl = window.vireoThumbnailUrl ? window.vireoThumbnailUrl(best) : '/thumbnails/' + best.id + '.jpg';
  content.className = '';
  content.innerHTML =
    '<div class="best-batch-head">' +
      '<div class="best-batch-pick">' +
        '<img src="' + escapeAttr(bestThumbUrl) + '" alt="' + escapeAttr(best.filename || '') + '">' +
        '<div class="best-batch-pick-meta">' +
          '<div class="best-batch-eyebrow">Best Pick</div>' +
          '<div class="best-batch-title">' + escapeHtml(best.filename || '') + '</div>' +
          '<div class="best-batch-score">' + escapeHtml(bestBatchScoreText(best)) + '</div>' +
        '</div>' +
      '</div>' +
      '<div class="best-batch-summary">' +
        '<div class="best-batch-eyebrow">' + escapeHtml(countText) + '</div>' +
        '<div>' + escapeHtml(best.filename || 'This photo') + ' is the top-ranked frame in the detected batch.</div>' +
        '<div class="best-batch-reasons">' + reasonHtml + '</div>' +
      '</div>' +
    '</div>' +
    '<div class="best-batch-list">' + cards + '</div>';
}

function openBestBatchForIds(ids, seedId) {
  ids = Array.isArray(ids) ? ids.filter(function(id) { return id != null; }) : [];
  seedId = seedId || (ids.length ? ids[0] : bestBatchSeedId());
  if (!seedId) {
    showToast('Select a photo first.', 'error');
    return;
  }
  try {
    window.sessionStorage.setItem('vireo.bestBatchIds', JSON.stringify(ids));
    window.sessionStorage.setItem('vireo.bestBatchSeedId', String(seedId));
  } catch (e) {
    showToast('Could not open Best Batch for this selection.', 'error');
    return;
  }
  window.location.href = '/best-batch?photo_id=' + encodeURIComponent(seedId);
}

function openBestBatch() {
  var ids = getActiveSelection();
  openBestBatchForIds(ids, bestBatchSeedId());
}

function openBestBatchInReview() {
  if (!bestBatchData || !bestBatchData.photo_ids || bestBatchData.photo_ids.length < 2) return;
  try {
    window.sessionStorage.setItem('vireo.browseBurstReviewIds', JSON.stringify(bestBatchData.photo_ids));
  } catch (e) {
    showToast('Could not open burst review for this batch.', 'error');
    return;
  }
  window.location.href = '/pipeline/review?browse_burst=1';
}

async function applyBestBatchPickOnly() {
  if (!bestBatchData || !bestBatchData.best_photo_id) return;
  var bestId = bestBatchData.best_photo_id;
  try {
    await safeFetch('/api/batch/flag', {
      method: 'POST',
      headers: {'Content-Type': 'application/json'},
      body: JSON.stringify({photo_ids: [bestId], flag: 'flagged'}),
    });
  } catch(e) { return; }
  var p = findBrowsePhoto(bestId);
  if (p) p.flag = 'flagged';
  await reconcileBrowseStackCovers([bestId]);
  refreshGridCards([bestId]);
  refreshExpandedBrowseStackMembers([bestId]);
  _refreshBatchInspectorIfActive();
  scheduleCollectionCountsRefresh();
  showUndoToast();
  showToast('Flagged best photo.', 'success');
}

async function applyBestBatchPickAndReject() {
  if (!bestBatchData || !bestBatchData.best_photo_id) return;
  var bestId = bestBatchData.best_photo_id;
  var rejectIds = (bestBatchData.suggested_reject_ids || []).filter(function(id) { return id !== bestId; });
  try {
    await safeFetch('/api/batch/best-batch-flags', {
      method: 'POST',
      headers: {'Content-Type': 'application/json'},
      body: JSON.stringify({best_photo_id: bestId, reject_photo_ids: rejectIds}),
    });
  } catch(e) { return; }
  var touchedIds = [bestId].concat(rejectIds);
  touchedIds.forEach(function(id) {
    var p = findBrowsePhoto(id);
    var flag = id === bestId ? 'flagged' : 'rejected';
    if (p) p.flag = flag;
    _clearRepresentativeStateIfIneligible(id, flag);
  });
  await reconcileBrowseStackCovers(touchedIds);
  refreshGridCards(touchedIds);
  refreshExpandedBrowseStackMembers(touchedIds);
  _refreshBatchInspectorIfActive();
  scheduleCollectionCountsRefresh();
  showUndoToast();
  showToast('Applied best-batch flags.', 'success');
  hideBestBatch();
}
