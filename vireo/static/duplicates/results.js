// Rendering a scan: the summary, the Bulk decide buckets, Needs review and
// the resolved cleanup list, each group's warnings, and Clear results.

import { duplicatesEl, state } from './state.js';
import { DuplicateCategories } from './categories.js';
import {
  bulkResolveByFolder, clearDuplicateTrashProgress, duplicateBucketIsPinned,
  duplicateFileTrashIsPending, refreshDuplicateTrashControls,
} from './cleanup.js';
import { visibleBucket } from './filter.js';
import {
  groupMatchesFilter, groupMatchesPhotoFilter, renderCategorySummary,
  renderSameShotOnlySummaryBar, renderSummaryBar, updateApplyCount,
} from './patterns.js';
import { isBulkTrashProtected, renderResolvedGroup, renderUnresolvedGroup } from './resolved.js';
import { cancelDuplicateLoad } from './restore.js';
import { bucketRevealButton } from './reveal.js';
import { renderSameShotResults } from './same-shot.js';
import { hideProgress, hideRestoredBanner, setEmptyVisible } from './scan.js';
import { escapeHtml, formatBytes } from './shared.js';

/** Keep only the same-shot stat visible when the exact-byte section is hidden.
 *
 * The sticky apply bar still goes away: there are no exact-byte groups
 * to reject here, and leaving a "Reject N extras" button next to the
 * same-shot stat would act on hidden (or absent) groups.
 * @param {DuplicateScan|null} result
 */
function keepSameShotSummaryOnly(result) {
  var summary = duplicatesEl('summary');
  var sameShotBar = result ? renderSameShotOnlySummaryBar(result) : '';
  if (sameShotBar) {
    summary.innerHTML = sameShotBar;
    summary.style.display = '';
  } else {
    summary.style.display = 'none';
  }
  duplicatesEl('applyBar').style.display = 'none';
}

/** Every group is hidden by the active filters: say so, and keep only the same-shot stat visible. @param {number} hiddenByFilter */
export function renderAllGroupsFiltered(hiddenByFilter) {
  setEmptyVisible(false);
  keepSameShotSummaryOnly(state.lastScanResult);
  duplicatesEl('results').innerHTML =
    '<div class="no-results">No duplicate groups match the active filters (' +
    hiddenByFilter + ' hidden). Clear or pause filters to see them.</div>';
  updateApplyCount();
}

/** The scan has no groups left: explain why (cleanup done, out of date, or clean). @param {DuplicateScan} result */
export function renderNoDuplicateGroups(result) {
  setEmptyVisible(false);
  keepSameShotSummaryOnly(result);
  // A restored scan with every group revalidated out is only out of
  // date if the catalog has duplicates a new scan would show: groups
  // added since, or a stale group's hash that still forms one.
  var staleCount = result.stale_group_count || 0;
  var newCount = result.new_group_count || 0;
  var changedCount = result.changed_group_count || 0;
  var results = duplicatesEl('results');
  if (result.catalog_cleanup) {
    results.innerHTML =
      '<div class="no-results">No rejected duplicate copies are pending cleanup. ' +
      'Scan for duplicate files to check for other matches.</div>';
  } else if (newCount > 0) {
    results.innerHTML =
      '<div class="no-results">The last scan is out of date — ' +
      newCount.toLocaleString() +
      (newCount === 1 ? ' new duplicate group has' : ' new duplicate groups have') +
      ' appeared since. Run a new scan to see ' +
      (newCount === 1 ? 'it' : 'them') + '.</div>';
  } else if (staleCount > 0 && changedCount > 0) {
    results.innerHTML =
      '<div class="no-results">The last scan is out of date — ' +
      changedCount.toLocaleString() +
      (changedCount === 1 ? ' group from it has' : ' groups from it have') +
      ' changed since. Run a new scan to check for duplicates.</div>';
  } else {
    results.innerHTML = state.duplicateBucketOperations.size
      ? '<div class="no-results">Duplicate review is complete. Pending cleanup is shown above.</div>'
      : '<div class="no-results">No exact-byte duplicates found in these results.</div>';
  }
  updateApplyCount();
}

/**
 * The Bulk decide section, or '' when no bucket has two visible groups.
 *
 * Iterate over the unfiltered bucket list so `bi` stays a stable index
 * into ``state.lastScanResult.buckets``. Filtering with ``.filter`` first and
 * re-indexing the result would let a partial bulk-resolve (which can drop
 * a bucket to ``group_count === 1`` without removing it from the array)
 * shift later indexes, so subsequent button clicks would resolve the
 * wrong bucket — and with delete enabled, potentially trash the wrong
 * files. Skip 1-group buckets inline instead — the per-group UI handles
 * them more expressively (per-photo cards, ratings, etc.). Under an
 * active filter, each bucket is passed through ``visibleBucket()`` so
 * the card and its Keep-for-all action reflect only matching groups.
 *
 * @param {DuplicateScan} result
 */
export function bulkDecideSectionHtml(result) {
  var allBuckets = result.buckets || [];
  var visibleBuckets = allBuckets.map(function(bucket, bi) {
    return duplicateBucketIsPinned(result, bi) ? null : visibleBucket(bucket);
  });
  var hasVisibleBuckets = visibleBuckets.some(function(b) {
    return b && b.group_count >= 2;
  });
  if (!hasVisibleBuckets) return '';
  var html = '<div class="section-header"><div>' +
    '<h2>Bulk decide</h2>' +
    '<div class="section-sub">' +
    'The same dedup question repeats across multiple groups. Decide ' +
    'once per bucket to clear them all at once.</div></div></div>';
  visibleBuckets.forEach(function(bucket, bi) {
    if (!bucket || bucket.group_count < 2) return;
    html += renderBucket(bucket, bi);
  });
  return html;
}

/** @param {DuplicateProposal[]} unresolved */
export function needsReviewSectionHtml(unresolved) {
  if (!unresolved.length) return '';
  var html = '<div class="section-header"><div>' +
    '<h2>Needs review</h2>' +
    '<div class="section-sub">' + unresolved.length +
    ' duplicate group' + (unresolved.length === 1 ? '' : 's') +
    ' awaiting your decision.</div></div></div>';
  unresolved.forEach(function(group) {
    html += renderUnresolvedGroup(group);
  });
  return html;
}

/** @param {DuplicateScan} result @param {DuplicateProposal[]} resolved */
export function resolvedSectionHtml(result, resolved) {
  var resolvedGroupCount = resolved.length;
  if (!resolvedGroupCount) return '';
  var emptyFileGroupCount = resolved.reduce(function(n, p) {
    return n + (p.empty_file_group ? 1 : 0);
  }, 0);
  var resolvedBytes = 0;
  resolved.forEach(function(p) {
    (p.losers || []).forEach(function(l) {
      if (l.file_size) resolvedBytes += l.file_size;
    });
  });
  // Bulk-trashable count must mirror ``isBulkTrashProtected`` so the button
  // label never advertises files the action would skip. Without this the
  // button claims N losers but ``trashAllLoserFiles`` returns "Nothing to
  // trash" when every remaining group is protected.
  var bulkTrashableLoserCount = resolved.reduce(function(n, p) {
    if (isBulkTrashProtected(p)) return n;
    return n + (p.losers || []).filter(function(l) { return !duplicateFileTrashIsPending(l.id); }).length;
  }, 0);
  // Auto-expand if the user came from the disk-cleanup banner. Detection
  // is intentionally simple — query string is the most direct way to
  // pass intent through a hard navigation without needing storage state.
  var autoExpand = result.catalog_cleanup || state.duplicateCategory !== 'all' ||
    new URLSearchParams(window.location.search).get('show') === 'resolved';
  var listClass = 'resolved-list' + (autoExpand ? ' expanded' : '');
  var toggleClass = 'resolved-toggle' + (autoExpand ? ' expanded' : '');
  var html = '<div class="section-header">' +
    '<div>' +
      '<h2 class="' + toggleClass + '" id="resolvedToggle" data-dup-action="toggle-resolved">' +
        '<span class="chev">&#9656;</span> ' +
        (result.catalog_cleanup ? 'Rejected copies — cleanup pending' : 'Resolved — cleanup pending') +
      '</h2>' +
      '<div class="section-sub">' +
      (emptyFileGroupCount === resolvedGroupCount
        ? 'These groups are zero-byte files, usually from an interrupted copy or storage problem. Check the original source or backup before deleting anything.'
        : result.catalog_cleanup
          ? 'These copies are already rejected in your catalog. Kept copies are shown alongside them; disk availability will be verified before cleanup.'
          : 'Vireo already picked which copy to keep for ' +
          resolvedGroupCount + ' group' + (resolvedGroupCount === 1 ? '' : 's') +
          ', but the extra copies (' + formatBytes(resolvedBytes) +
          ') are still on disk. Empty-file groups need source or backup checks before cleanup.') +
      '</div>' +
    '</div>';
  // Hide the bulk button entirely when every remaining resolved group is
  // protected — otherwise it sticks around as a no-op that always toasts
  // "Nothing to trash" and confuses users about what cleanup is left.
  html += '<div class="section-actions">';
  if (bulkTrashableLoserCount > 0) {
    html += '<button class="btn-secondary" data-dup-action="trash-all" id="trashAllBtn">' +
        'Move ' + bulkTrashableLoserCount + ' extra ' +
        (bulkTrashableLoserCount === 1 ? 'copy' : 'copies') + ' to Trash' +
      '</button>';
  }
  html += '</div></div>';
  html += '<div class="' + listClass + '" id="resolvedList">';
  resolved.forEach(function(group) {
    html += renderResolvedGroup(group);
  });
  return html + '</div>';
}

/** @param {DuplicateScan} result */
export function renderResults(result) {
  hideProgress();
  /** @type {HTMLButtonElement} */ (duplicatesEl('scanBtn')).disabled = false;

  state.lastScanResult = result;
  state.proposals = result.proposals || [];
  renderSameShotResults(result);
  var categories = DuplicateCategories.summarize(state.proposals.filter(groupMatchesPhotoFilter));
  if (!categories.some(function(c) { return c.id === state.duplicateCategory; })) state.duplicateCategory = 'all';

  // Split proposals so the two sections render independently. Unresolved
  // groups still need a user decision (the apply flow); resolved groups
  // are already auto-handled and just need on-disk cleanup. Mixing them
  // would mislead the apply count and confuse the action affordances.
  /** @type {DuplicateProposal[]} */
  var unresolved = [];
  /** @type {DuplicateProposal[]} */
  var resolved = [];
  var hiddenByFilter = 0;
  state.proposals.forEach(function(p, gi) {
    p._gi = gi;  // stable index back into state.proposals for checkbox lookup
    if (!groupMatchesFilter(p)) { hiddenByFilter += 1; return; }
    if (p.status === 'resolved') resolved.push(p);
    else unresolved.push(p);
  });
  if (hiddenByFilter && !unresolved.length && !resolved.length) {
    renderAllGroupsFiltered(hiddenByFilter);
    return;
  }
  if (state.proposals.length === 0) {
    renderNoDuplicateGroups(result);
    return;
  }

  setEmptyVisible(false);
  var summary = duplicatesEl('summary');
  summary.style.display = '';
  summary.innerHTML = renderSummaryBar(result, unresolved, resolved) + renderCategorySummary(categories);
  duplicatesEl('results').innerHTML = bulkDecideSectionHtml(result) +
    needsReviewSectionHtml(unresolved) + resolvedSectionHtml(result, resolved);
  refreshDuplicateTrashControls();

  updateApplyCount();
}

/** @param {DuplicateProposal} group */
export function renderGroupWarning(group) {
  // Severity, high to low:
  //  - all_missing: every file in the group is gone — nothing to keep.
  //  - all_offline: every copy is on an unreachable volume — state unknown,
  //      don't propose orphan cleanup or promote a surviving twin.
  //  - winner missing (status-dependent):
  //      • unresolved: Rule 0 promoted a surviving twin into the KEEP slot.
  //      • resolved: the kept row in the DB is fixed; if a loser file still
  //        exists it's the only surviving copy. Trashing it = data loss.
  //  - winner offline: KEEP card sits on an unreachable volume; the file may
  //      well be there. Never phrase this as "missing".
  //  - any loser missing/offline: informational; trash actions will be no-ops
  //      for it.
  var allMissing = group.all_missing === true;
  var allOffline = group.all_offline === true;
  var winner = group.winner || {};
  var losers = group.losers || [];
  var winnerOffline = winner.volume_offline === true;
  var winnerMissing = winner.exists === false && !winnerOffline;
  var anyLoserMissing = losers.some(function(l) {
    return l.exists === false && l.volume_offline !== true;
  });
  var anyLoserOffline = losers.some(function(l) {
    return l.volume_offline === true;
  });
  if (group.empty_file_group) {
    return '<div class="group-warning severe">' +
      '<b>Zero-byte image files detected.</b> These are not real duplicate photos; ' +
      'they are empty placeholders, usually from an interrupted copy, failed import, ' +
      'or storage issue. Check the camera card, original source, or backup before ' +
      'moving files to Trash. After restoring or deleting them, rescan this folder.' +
      '</div>';
  }
  if (allMissing) {
    return '<div class="group-warning severe">' +
      '<b>All files in this group are missing on disk.</b> ' +
      'Nothing to delete — Vireo just needs to clean up the orphaned DB rows. ' +
      'Re-scanning the affected folders will mark these as missing.' +
      '</div>';
  }
  if (allOffline) {
    return '<div class="group-warning severe">' +
      '<b>Every copy in this group is on a volume that isn’t reachable right now.</b> ' +
      'Their state is unknown — reconnect the drive or NAS and rescan before ' +
      'applying any action here.' +
      '</div>';
  }
  if (winnerMissing) {
    if (group.status === 'resolved') {
      return '<div class="group-warning severe">' +
        '<b>Heads up:</b> the kept file in this resolved group is missing on disk. ' +
        'Any surviving extra copy below may be the only remaining copy — ' +
        'trash actions on those extras are disabled to prevent data loss. ' +
        'Restore the kept file, or rescan the folder so Vireo can re-resolve.' +
        '</div>';
    }
    // status === 'unresolved': Rule 0 promotes a present candidate, so the
    // KEEP card here is genuinely a surviving copy that's been swapped in.
    return '<div class="group-warning severe">' +
      '<b>Heads up:</b> the file Vireo originally picked to keep is missing on disk. ' +
      'A surviving copy was promoted instead — check the KEEP card before applying.' +
      '</div>';
  }
  if (winnerOffline) {
    return '<div class="group-warning">' +
      '<b>Heads up:</b> the KEEP file is on a volume that isn’t reachable right now — ' +
      'its state is unknown. Reconnect the drive or NAS before applying.' +
      '</div>';
  }
  if (anyLoserMissing) {
    return '<div class="group-warning">' +
      'One or more extra copies are already missing on disk. They’ll be ' +
      'rejected in the DB; trash actions will skip them.' +
      '</div>';
  }
  if (anyLoserOffline) {
    return '<div class="group-warning">' +
      'One or more extra copies are on a volume that isn’t reachable right now. ' +
      'Trash actions will skip them until the volume comes back.' +
      '</div>';
  }
  return '';
}

/** @param {DuplicateBucket} bucket @param {number} bi */
export function renderBucket(bucket, bi) {
  var folders = bucket.folders || [];
  var folderListHtml = folders.map(function(f) {
    return '<code>' + escapeHtml(f) + '</code>';
  }).join(' &middot; ');
  var examples = (bucket.example_filenames || []).map(escapeHtml).join(', ');
  var sizeStr = formatBytes(bucket.total_size || 0);
  var html = '<div class="bucket-card" data-bi="' + bi + '" data-reveal-paths="' + escapeHtml(JSON.stringify(folders)) + '">';
  html += '<div class="bucket-headline">' +
    '<span class="count">' + bucket.group_count + '</span> ' +
    'duplicate group' + (bucket.group_count === 1 ? '' : 's') +
    ' shared between: ' + folderListHtml + '</div>';
  var metaParts = [];
  if (examples) metaParts.push('Examples: ' + examples);
  if (bucket.total_size) metaParts.push(sizeStr + ' recoverable');
  if (metaParts.length) {
    html += '<div class="bucket-meta">' + metaParts.join(' &middot; ') + '</div>';
  }
  html += '<div class="bucket-actions">';
  folders.forEach(function(folder, /** @type {number} */ fi) {
    var label = folder.split('/').filter(Boolean).pop() || folder;
    html += '<button class="keep-btn" data-keep-folder="' + fi + '">' +
      'Keep ' + escapeHtml(label) + ' for all ' + bucket.group_count +
      '</button>';
  });
  html += bucketRevealButton(bi, null, folders);
  html += '<label class="delete-toggle">' +
    '<input type="checkbox" data-bi="' + bi + '" data-delete-toggle> ' +
    'Also move extras to Trash</label>';
  html += '</div>';
  html += '<div class="bucket-status" data-bi="' + bi + '" data-bucket-status></div>';
  html += '</div>';
  return html;
}

/** @param {number} bi @param {number} fi */
export function bulkResolveByFolderIdx(bi, fi) {
  // ``visibleBucket`` restricts to filter-matching groups so the button
  // acts only on what the user sees. Without it, a bucket with one
  // matching and one hidden group would still submit both file_hashes,
  // rejecting (and with delete enabled, trashing) the hidden group.
  var bucket = visibleBucket((state.lastScanResult && state.lastScanResult.buckets || [])[bi]);
  if (!bucket) return;
  // After a partial prune a bucket can drop to ``group_count < 2`` and be
  // filtered out of the next render. A button left in the DOM from a stale
  // render must not be allowed to act on it. Same guard covers a bucket
  // shrunk below the threshold by the active filter.
  if (!bucket.file_hashes || bucket.file_hashes.length < 2) return;
  var folder = (bucket.folders || [])[fi];
  if (folder == null) return;
  return bulkResolveByFolder(bi, folder);
}

export function clearResults() {
  cancelDuplicateLoad();
  clearDuplicateTrashProgress();
  state.proposals = [];
  state.duplicateCategory = 'all';
  state.excludedDuplicateHashes.clear();
  state.currentJobId = null;
  // Drop the cached scan snapshot too. Otherwise a later filter-bar
  // change (or pause/resume) would call ``refreshFilterMatches`` and
  // re-render the stale results the user just cleared, resurrecting the
  // duplicate groups and their bulk/apply actions.
  state.lastScanResult = null;
  state.sameShotVisibleLimit = 50;
  state.sameShotDifferingVisibleLimit = 50;
  /** @type {HTMLElement} */ (document.getElementById('sameShotResults')).innerHTML = '';
  /** @type {HTMLElement} */ (document.getElementById('results')).innerHTML = '';
  /** @type {HTMLElement} */ (document.getElementById('summary')).style.display = 'none';
  /** @type {HTMLElement} */ (document.getElementById('applyBar')).style.display = 'none';
  if (!state.duplicateApplyInProgress) state.duplicateApplyMessage = '';
  updateApplyCount();
  hideRestoredBanner();
  setEmptyVisible(true);
}
