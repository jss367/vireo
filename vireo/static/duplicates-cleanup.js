// Duplicates page: resolved-copy batch cleanup. The progress container lives
// outside the group list so category/filter renders cannot detach it.

var _duplicateTrashActive = false;

function clearDuplicateTrashProgress() {
  var progress = document.getElementById('trashAllProgress');
  if (progress && !_duplicateTrashActive) progress.hidden = true;
}

function refreshDuplicateTrashControls() {
  var button = document.getElementById('trashAllBtn');
  if (button && _duplicateTrashActive) button.disabled = true;
}

async function trashAllLoserFiles() {
  if (_duplicateTrashActive) return;
  // ``groupMatchesFilter`` restricts to groups the filter actually
  // surfaced. Without it, an active filter shrinks the rendered
  // resolved section and the button label (both driven by the
  // filtered ``resolved`` list in ``renderResults``) while this
  // action still walked every resolved proposal — a click on "Move
  // N extra copies to Trash" would then trash records for groups the
  // filter had hidden from view.
  var resolved = _proposals.filter(function(p) {
    return p.status === 'resolved' && groupMatchesFilter(p);
  });
  var ids = [];
  resolved.forEach(function(p) {
    if (isBulkTrashProtected(p)) return;
    (p.losers || []).forEach(function(l) {
      // Skip ones already trashed in this session.
      var card = document.querySelector('.dup-card[data-photo-id="' + l.id + '"]');
      if (card && card.classList.contains('trashed')) return;
      if (l.id != null) ids.push(l.id);
    });
  });
  if (ids.length === 0) {
    showToast('Nothing to trash', 'success');
    return;
  }
  if (!confirm('Move ' + ids.length + ' file' + (ids.length === 1 ? '' : 's') +
               ' to Trash and remove their entries from Vireo?\n\n' +
               'Files go to your OS Trash (recoverable from there). The ' +
               'corresponding extra-copy records (and their thumbnails/previews) ' +
               'are deleted from Vireo. Kept copies retain all merged keywords ' +
               'and ratings.')) {
    return;
  }

  var btn = document.getElementById('trashAllBtn');
  var progressEl = document.getElementById('trashAllProgress');
  _duplicateTrashActive = true;
  if (btn) { btn.disabled = true; btn.textContent = 'Moving to Trash...'; }

  try {
    var data = await trashLoserFilesInBatches(
      ids,
      function(progress) { updateTrashProgress(progressEl, progress); },
      applyTrashBatchToCards
    );
    var trashed = data.trashed || 0;
    var failed = (data.failed || []).length;

    var actionableSkips = (data.skipped || []).filter(isActionableSkip).length;
    var msg = 'Moved ' + trashed + ' file' + (trashed === 1 ? '' : 's') + ' to Trash';
    if (actionableSkips) msg += ', ' + actionableSkips + ' skipped';
    if (failed) msg += ', ' + failed + ' failed';
    showToast(msg, failed ? 'error' : 'success');

  } catch (e) {
    var completed = e.trashSummary ? e.trashSummary.completed : 0;
    showToast('Trash cleanup paused after ' + completed + ' of ' + ids.length +
              ' files: ' + (e.message || 'request failed') +
              '. Use the button to retry the remaining files.', 'error');
  } finally {
    _duplicateTrashActive = false;
    refreshTrashAllButton(document.getElementById('trashAllBtn'));
  }
}
