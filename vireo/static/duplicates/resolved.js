// Pruning resolved groups from the last scan in place, and rendering the
// unresolved and resolved groups (with the protected-winner rule the bulk
// Trash action shares).

import { state } from './state.js';
import { renderCard } from './card.js';
import { renderCategoryBadge } from './patterns.js';
import { renderGroupWarning } from './results.js';
import { escapeHtml } from './shared.js';

// Locally prune the given resolved hashes from state.lastScanResult so the
// next render drops matching buckets and unresolved groups. Avoids an
// expensive re-scan round-trip on libraries with thousands of dups.
// Mirrors bucket_unresolved_proposals and duplicate_scan aggregates.
/** @param {Set<string>} resolvedHashes */
export function pruneResolvedHashesFromScan(resolvedHashes) {
  if (!state.lastScanResult) return;
  state.lastScanResult.proposals = (state.lastScanResult.proposals || []).filter(
    function(p) { return !resolvedHashes.has(p.file_hash); }
  );
  /** @type {Record<string, DuplicateProposal>} */
  var proposalsByHash = {};
  (state.lastScanResult.proposals || []).forEach(function(p) {
    proposalsByHash[p.file_hash] = p;
  });
  state.lastScanResult.buckets = (state.lastScanResult.buckets || []).map(function(b) {
    var keptHashes = (b.file_hashes || []).filter(
      function(h) { return !resolvedHashes.has(h); }
    );
    if (keptHashes.length === b.file_hashes.length) return b;
    var keptProposals = keptHashes
      .map(function(h) { return proposalsByHash[h]; })
      .filter(/** @returns {p is DuplicateProposal} */ function(p) { return p != null; });
    var newTotalSize = keptProposals.reduce(function(n, p) {
      return n + ((p.losers || []).length * (p.winner && p.winner.file_size || 0));
    }, 0);
    var newExamples = keptProposals.slice(0, 3).map(function(p) {
      return (p.winner || {}).filename || '';
    });
    return Object.assign({}, b, {
      file_hashes: keptHashes,
      group_count: keptHashes.length,
      total_size: newTotalSize,
      example_filenames: newExamples,
    });
  });
  var prunedProposals = state.lastScanResult.proposals;
  state.lastScanResult.group_count = prunedProposals.length;
  state.lastScanResult.loser_count = prunedProposals.reduce(function(n, p) {
    return p.status === 'unresolved' ? n + (p.losers || []).length : n;
  }, 0);
  state.lastScanResult.resolved_group_count = prunedProposals.reduce(function(n, p) {
    return p.status === 'resolved' ? n + 1 : n;
  }, 0);
  state.lastScanResult.resolved_loser_count = prunedProposals.reduce(function(n, p) {
    return p.status === 'resolved' ? n + (p.losers || []).length : n;
  }, 0);
}

// Swap the bucket's Keep-folder buttons for a single "Retry to Trash"
// button after a partial trash failure. Storing the pending photo IDs
// on the card (rather than piggybacking on the bucket's file_hashes)
// means the retry can call ``/api/duplicates/delete-loser-files``
// directly — the losers were already rejected by bulk-resolve, so
// re-running bulk-resolve on the same hashes is a no-op.
//
// ``resolvedHashes`` is the subset of bucket.file_hashes that
// bulk-resolve actually resolved (i.e. excluding any it returned in
// ``skipped``). The retry uses it to prune state.lastScanResult so a
// partial resolve doesn't drop the skipped hashes' unresolved groups
// from the page.
/** @param {DuplicateProposal} group */
export function renderUnresolvedGroup(group) {
  var winner = group.winner || /** @type {DuplicatePhoto} */ ({});
  var losers = group.losers || [];
  var hash = group.file_hash || '';
  var gi = group._gi;

  var html = '<div class="dup-group" data-hash="' + escapeHtml(hash) + '">';
  html += '<div class="dup-group-header">';
  html += '<label><input type="checkbox" class="group-check" data-gi="' + gi + '"' +
          (state.excludedDuplicateHashes.has(hash) ? '' : ' checked') + '> ' +
          'Include this group (' + losers.length + ' to reject)</label>';
  html += renderCategoryBadge(group);
  html += '<span class="hash">' + escapeHtml(hash.substring(0, 16)) + '...</span>';
  html += '</div>';
  html += renderGroupWarning(group);
  html += '<div class="dup-row">';
  html += renderCard(winner, true, undefined, false);
  losers.forEach(function(loser) {
    html += renderCard(loser, false, loser.reason, false);
  });
  html += '</div></div>';
  return html;
}

/** @param {DuplicateProposal} group */
export function renderResolvedGroup(group) {
  var winner = group.winner || /** @type {DuplicatePhoto} */ ({});
  var losers = group.losers || [];
  var hash = group.file_hash || '';
  // When the kept-file is missing, any surviving loser file is the only
  // remaining copy. Drop trash affordances for that group entirely so the
  // bulk and per-card paths stay consistent — both check ``data-protected``.
  // A winner on an offline volume has unknown state: it may or may not be
  // there, so we can't tell whether the reachable loser is the last copy
  // either. Treat the group as protected until the winner's presence can
  // be confirmed — the ``winner-offline`` label lets ``renderCard`` swap
  // in the reconnect-first messaging instead of the "kept file missing"
  // wording. ``isBulkTrashProtected`` mirrors this so the bulk button
  // never advertises files we've disabled the per-card action for.
  var winnerAbsent = winner.exists === false;
  var winnerOffline = winner.volume_offline === true;
  var winnerMissing = winnerAbsent && !winnerOffline;
  var winnerProtected = winnerAbsent;

  var html = '<div class="dup-group" data-hash="' + escapeHtml(hash) + '"' +
             (winnerProtected
               ? ' data-protected="' + (winnerMissing ? 'winner-missing' : 'winner-offline') + '"'
               : '') + '>';
  html += '<div class="dup-group-header">';
  if (group.empty_file_group) {
    html += '<label style="cursor:default;">' +
            (losers.length + 1 + (group.other_kept || []).length) + ' zero-byte files detected</label>';
  } else {
    html += '<label style="cursor:default;">' +
            losers.length + ' extra ' +
            (losers.length === 1 ? 'copy' : 'copies') +
            ' already rejected</label>';
  }
  html += renderCategoryBadge(group);
  html += '<span class="hash">' + escapeHtml(hash.substring(0, 16)) + '...</span>';
  html += '</div>';
  html += renderGroupWarning(group);
  html += '<div class="dup-row">';
  var kept = [winner].concat(group.other_kept || []);
  var keptIds = new Set(kept.map(function(photo) { return photo.id; }));
  var members = kept.concat(losers);
  // Keep photos in their original slots when the choice changes. Moving the
  // chosen photo left would leave KEEP stationary and another Keep button
  // under the pointer, making a second click undo the choice.
  if (!group._cardOrder) group._cardOrder = members.map(function(photo) { return photo.id; });
  var order = group._cardOrder;
  members.sort(function(a, b) { return order.indexOf(a.id) - order.indexOf(b.id); });
  members.forEach(function(photo) {
    var isKept = keptIds.has(photo.id);
    html += renderCard(photo, isKept, isKept ? undefined : photo.reason, !isKept, winnerProtected, winnerOffline);
  });
  html += '</div></div>';
  return html;
}

export function toggleResolvedSection() {
  var toggle = /** @type {HTMLElement} */ (document.getElementById('resolvedToggle'));
  var list = /** @type {HTMLElement} */ (document.getElementById('resolvedList'));
  if (!toggle || !list) return;
  var expanded = list.classList.toggle('expanded');
  if (expanded) toggle.classList.add('expanded');
  else toggle.classList.remove('expanded');
}

// Protected: kept-file is either missing on disk, or its volume is
// unreachable so we can't tell whether it's still there. In both cases
// a surviving loser file may be the only remaining copy, so this
// predicate matches ``winnerProtected`` in ``renderGroup``. The
// bulk-button label and the bulk action share it so the count never
// advertises files ``trashAllLoserFiles`` would skip.
/** @param {DuplicateProposal | null | undefined} p */
export function isBulkTrashProtected(p) {
  return !!(p && p.winner && p.winner.exists === false);
}
