// Duplicates page: category rendering and review/cleanup state. Classic
// script sharing page globals, read after DOMContentLoaded.

var _duplicateCategory = 'all';
var _excludedDuplicateHashes = new Set();

function rememberDuplicateSelections() {
  document.querySelectorAll('.group-check').forEach(function(cb) {
    var proposal = _proposals[Number(cb.getAttribute('data-gi'))];
    if (!proposal) return;
    if (cb.checked) _excludedDuplicateHashes.delete(proposal.file_hash);
    else _excludedDuplicateHashes.add(proposal.file_hash);
  });
}

function groupMatchesPhotoFilter(group) {
  if (!_filterMatchedIds) return true;
  var members = [group.winner].concat(group.other_kept || [], group.losers || []);
  return members.some(function(p) {
    return p && p.id != null && _filterMatchedIds.has(p.id);
  });
}

function groupMatchesFilter(group) {
  return groupMatchesPhotoFilter(group) &&
    (_duplicateCategory === 'all' || DuplicateCategories.classify(group) === _duplicateCategory);
}

function selectDuplicateCategory(id) {
  _duplicateCategory = id;
  if (_lastScanResult && !_scanInProgress) renderResults(_lastScanResult);
  var button = document.querySelector('[data-duplicate-category="' + id + '"]');
  if (button) button.focus();
}

function renderCategorySummary(categories) {
  var total = categories.reduce(function(sum, c) {
    sum.groups += c.groups; sum.extras += c.extras; sum.bytes += c.bytes;
    return sum;
  }, {id: 'all', label: 'All categories', groups: 0, extras: 0, bytes: 0});
  var html = '<div class="category-summary"><h2>Duplicate patterns</h2>' +
    '<p class="category-note">Grouped by filenames and folders within exact-byte matches. ' +
    'Each group appears once. Click a category to review it; sizes are estimates of extra files.</p>' +
    '<div class="category-grid" role="group" aria-label="Filter duplicate patterns">';
  [total].concat(categories).forEach(function(c) {
    html += '<button type="button" class="category-button" data-duplicate-category="' + c.id +
      '" aria-pressed="' + (_duplicateCategory === c.id) +
      '" onclick="selectDuplicateCategory(\'' + c.id + '\')">' +
      '<span class="category-title">' + escapeHtml(c.label) + '</span>' +
      '<span class="category-counts">' + formatCount(c.groups) + ' group' + (c.groups === 1 ? '' : 's') +
      ' &middot; ' + formatCount(c.extras) + ' extra ' + (c.extras === 1 ? 'copy' : 'copies') +
      ' &middot; ' + formatBytes(c.bytes) + '</span></button>';
  });
  return html + '</div></div>';
}

function renderCategoryBadge(group) {
  return '<span class="category-badge">' +
    escapeHtml(DuplicateCategories.label(DuplicateCategories.classify(group))) + '</span>';
}

function renderSummaryBar(result, unresolved, resolved) {
  var extras = unresolved.reduce(function(n, p) { return n + (p.losers || []).length; }, 0);
  var bytes = resolved.reduce(function(n, p) {
    return n + (p.losers || []).reduce(function(size, l) { return size + (l.file_size || 0); }, 0);
  }, 0);
  var emptyGroups = resolved.filter(function(p) { return p.empty_file_group; }).length;
  var html = '<div class="summary-bar">';
  if (!result.catalog_cleanup) html +=
    '<div class="summary-stat"><div class="num">' + unresolved.length + '</div>' +
      '<div class="label">Need review</div></div>' +
    '<div class="summary-stat"><div class="num" style="color:var(--danger);">' + extras + '</div>' +
      '<div class="label">Extras to reject</div></div>';
  if (resolved.length > 0 || result.catalog_cleanup) {
    html +=
      '<div class="summary-stat"><div class="num">' + resolved.length + '</div>' +
        '<div class="label">Cleanup pending</div></div>' +
      '<div class="summary-stat"><div class="num">' + formatBytes(bytes) + '</div>' +
        '<div class="label">Estimated extra file size</div></div>';
    if (emptyGroups > 0) html +=
      '<div class="summary-stat"><div class="num" style="color:var(--warning, #d69e2e);">' +
        emptyGroups + '</div><div class="label">Empty-file groups</div></div>';
  }
  return html + '</div>';
}

function forgetCleanedDuplicateCopies(photoIds) {
  if (!_lastScanResult) return;
  rememberDuplicateSelections();
  photoIds.forEach(function(id) {
    var card = document.querySelector('.dup-card[data-photo-id="' + id + '"]');
    if (!card) return;
    card.classList.add('trashed');
    var status = card.querySelector('[data-trash-status]');
    if (status) status.textContent = 'Cleaned up';
  });
  var emptyHashes = new Set();
  var changedGroups = [];
  (_lastScanResult.proposals || []).forEach(function(p) {
    if (p.status !== 'resolved') return;
    var previousCount = (p.losers || []).length;
    p.losers = (p.losers || []).filter(function(l) { return !photoIds.has(l.id); });
    if (p.losers.length !== previousCount) changedGroups.push({proposal: p, cleaned: previousCount - p.losers.length});
    if (!p.losers.length) emptyHashes.add(p.file_hash);
  });
  pruneResolvedHashesFromScan(emptyHashes);
  var cards = new Map();
  document.querySelectorAll('.dup-group').forEach(function(card) { cards.set(card.getAttribute('data-hash'), card); });
  // A pending cleanup may finish after a category switch. Its surviving
  // copies can enter the current category without having a rendered card.
  // Rebuild that view before offering any bulk action, retaining retry text.
  if (changedGroups.some(function(change) {
    var p = change.proposal;
    return p.losers.length && groupMatchesFilter(p) && !cards.has(p.file_hash);
  })) {
    var statuses = new Map();
    document.querySelectorAll('.dup-card[data-photo-id] [data-trash-status]').forEach(function(status) {
      var card = status.closest('.dup-card');
      var button = card.querySelector('.trash-btn');
      statuses.set(card.getAttribute('data-photo-id'), {text: status.textContent, disabled: button && button.disabled});
    });
    renderResults(_lastScanResult);
    document.querySelectorAll('.dup-card[data-photo-id] [data-trash-status]').forEach(function(status) {
      var card = status.closest('.dup-card');
      var saved = statuses.get(card.getAttribute('data-photo-id'));
      if (!saved) return;
      status.textContent = saved.text;
      var button = card.querySelector('.trash-btn');
      if (button) button.disabled = saved.disabled;
    });
    return;
  }
  changedGroups.forEach(function(change) {
    var p = change.proposal;
    var card = cards.get(p.file_hash);
    if (!card || !p.losers.length) return;
    var category = DuplicateCategories.classify(p);
    if (!groupMatchesFilter(p)) {
      // Retain a completion notice outside the filtered group list. Remaining
      // copies must not keep actionable cards under their former category.
      var notice = document.createElement('div');
      notice.className = 'group-warning cleanup-category-note';
      notice.setAttribute('role', 'status');
      notice.innerHTML = change.cleaned + ' extra ' + (change.cleaned === 1 ? 'copy' : 'copies') +
        ' cleaned up. Remaining copies now appear under ' + escapeHtml(DuplicateCategories.label(category)) + '. ' +
        '<button type="button" class="btn-secondary" onclick="selectDuplicateCategory(\'' + category +
        '\')">View remaining copies</button>';
      card.replaceWith(notice);
    } else {
      var badge = card.querySelector('.category-badge');
      if (badge) badge.textContent = DuplicateCategories.label(category);
      var label = card.querySelector('.dup-group-header label');
      if (label && !p.empty_file_group) label.textContent = p.losers.length + ' extra ' +
        (p.losers.length === 1 ? 'copy' : 'copies') + ' already rejected';
    }
  });
  // Keep completion messages visible until the next category/filter change,
  // but update the pattern counts now and never resurrect cleaned-up cards.
  var categories = DuplicateCategories.summarize(_lastScanResult.proposals.filter(groupMatchesPhotoFilter));
  if (_duplicateCategory !== 'all' && !categories.some(function(c) { return c.id === _duplicateCategory; })) {
    categories.push({id: _duplicateCategory, label: DuplicateCategories.label(_duplicateCategory),
      groups: 0, extras: 0, bytes: 0});
  }
  var summary = document.querySelector('.category-summary');
  if (summary) summary.outerHTML = renderCategorySummary(categories);
  var visible = _lastScanResult.proposals.filter(groupMatchesFilter);
  var bar = document.querySelector('.summary-bar');
  if (bar) bar.outerHTML = renderSummaryBar(_lastScanResult,
    visible.filter(function(p) { return p.status !== 'resolved'; }),
    visible.filter(function(p) { return p.status === 'resolved'; }));
}
