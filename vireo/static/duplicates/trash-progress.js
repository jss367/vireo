// Trash progress: the batched cleanup's progress line and which skips the
// user can act on.
// Classic page script.

/** @param {number | null | undefined} n */
function formatCount(n) {
  return Number(n || 0).toLocaleString();
}

/** @param {HTMLElement | null} container @param {DuplicateTrashProgress} progress */
function updateTrashProgress(container, progress) {
  if (!container) return;
  var total = progress.total || 0;
  var completed = Math.min(progress.completed || 0, total);
  var percent = total > 0 ? Math.round(completed / total * 100) : 0;
  var phase = progress.phase || 'moving';
  var headline;
  if (phase === 'complete') {
    headline = 'Cleanup complete';
  } else if (phase === 'complete_with_errors') {
    headline = 'Cleanup finished with problems';
  } else if (phase === 'error') {
    headline = 'Cleanup paused after ' + formatCount(completed) + ' of ' +
      formatCount(total) + ' files';
  } else {
    headline = 'Moving files ' + formatCount(progress.batchStart || 1) +
      '\u2013' + formatCount(progress.batchEnd || total) + ' of ' +
      formatCount(total) + ' to Trash\u2026';
  }

  container.hidden = false;
  container.classList.toggle(
    'error', phase === 'error' || phase === 'complete_with_errors');
  container.innerHTML =
    '<div class="trash-progress-head"><b>' + headline + '</b>' +
      '<span class="trash-progress-percent">' + percent + '%</span></div>' +
    '<div class="trash-progress-track" role="progressbar" ' +
      'aria-label="Moving duplicate files to Trash" aria-valuemin="0" ' +
      'aria-valuemax="' + total + '" aria-valuenow="' + completed + '">' +
      '<div class="trash-progress-fill" style="width:' + percent + '%"></div>' +
    '</div>' +
    '<div class="trash-progress-detail">' + formatCount(completed) + ' processed' +
      ' &middot; ' + formatCount(progress.trashed) + ' moved' +
      ' &middot; ' + formatCount(progress.skipped) + ' skipped' +
      ' &middot; ' + formatCount(progress.failed) + ' failed</div>' +
    '<div class="trash-progress-note">Files are processed in small batches. ' +
      'External and network drives may take longer; moved files remain recoverable ' +
      'from your operating system\u2019s Trash.</div>';
}

/** @param {DuplicateSkip} s */
function isActionableSkip(s) {
  // "file already missing" completes the DB row (see applyTrashBatchToCards)
  // and its card renders "Cleaned up" like a successful trash. Counting it
  // as skipped in the progress detail contradicts the card state — a
  // cleanup of one already-gone file would render "Cleanup complete ·
  // 0 moved · 1 skipped" while the card reads "Cleaned up".
  return s.reason !== 'file already missing';
}
