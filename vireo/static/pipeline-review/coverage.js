// Collection counts use live catalog data. Cached results never prove inspection.
var _collectionCoverageSequence = 0;
/** @type {ReturnType<typeof setTimeout> | null} */ var _collectionCoverageTimer = null;

function refreshCollectionCoverage() {
  var sequence = ++_collectionCoverageSequence;
  if (_collectionCoverageTimer) clearTimeout(_collectionCoverageTimer);
  var panel = document.getElementById('collectionCoverage');
  if (!panel) return;
  panel.style.display = 'none';
  if (reviewScopeMode !== 'collection' || !reviewScopeCollectionId) return;
  var id = reviewScopeCollectionId;
  _collectionCoverageTimer = setTimeout(function() {
    _collectionCoverageTimer = null;
    /** @type {Promise<ReviewCollectionCoverage>} */ (safeFetch('/api/collections/' + encodeURIComponent(id) + '/coverage', {}, {toast: false}))
      .then(function(/** @type {ReviewCollectionCoverage} */ data) {
        if (sequence !== _collectionCoverageSequence) return;
        if (reviewScopeMode !== 'collection' || reviewScopeCollectionId !== id) return;
        renderCollectionCoverage(data);
      }).catch(function() {
        if (sequence !== _collectionCoverageSequence || !panel) return;
        panel.style.display = 'flex';
        var counts = document.getElementById('collectionCoverageCounts');
        var review = document.getElementById('collectionCoverageReview');
        var definitions = document.getElementById('collectionCoverageDefinitions');
        if (counts) counts.textContent = 'Collection counts could not be refreshed.';
        if (review) review.textContent = '';
        if (definitions) definitions.textContent = 'Use Refresh counts to try again.';
      });
  }, 100);
}

/** @param {ReviewCollectionCoverage} data */
function renderCollectionCoverage(data) {
  var panel = document.getElementById('collectionCoverage');
  var counts = document.getElementById('collectionCoverageCounts');
  var review = document.getElementById('collectionCoverageReview');
  var definitions = document.getElementById('collectionCoverageDefinitions');
  if (!panel || !counts || !review || !definitions) return;
  var original = data.original_import;
  var current = data.current_membership;
  var coverage = data.review;
  counts.textContent = (original.count == null ? 'Original import size unknown' : original.count.toLocaleString() + ' originally imported')
    + ' · ' + current.catalog_count.toLocaleString() + ' current catalog members'
    + ' · ' + current.online_folder_count.toLocaleString() + ' in online folders'
    + ' · ' + current.offline_folder_count.toLocaleString() + ' in offline folders';
  review.textContent = coverage.prediction_decision_count + ' / ' + coverage.denominator
    + ' with recorded prediction decisions · ' + coverage.with_species_count + ' / ' + coverage.denominator
    + ' with species keywords · ' + coverage.without_species_count + ' / ' + coverage.denominator
    + ' without species keywords · '
    + (coverage.unresolved_identity_count == null ? 'Explicit unresolved status: unknown'
      : coverage.unresolved_identity_count + ' / ' + coverage.denominator + ' explicitly unresolved')
    + ' · Inspected: unknown';
  var files = data.original_files;
  var fileText = files.known_missing_count == null
    ? 'Original-file accessibility is unknown here; no fresh Missing Originals scan is available.'
    : files.known_missing_count + ' current members had missing originals in the workspace scan at ' + files.checked_at
      + '. The remaining originals have not all been verified accessible.';
  definitions.textContent = 'Current membership snapshot: ' + data.snapshot_at + '. '
    + (original.recorded_at ? 'Original import total recorded at ' + original.recorded_at + '. ' : 'No recoverable original import total is recorded. ')
    + 'Current membership may differ from the original import; the difference does not establish deletion or data loss. '
    + fileText + ' ' + files.definition + '. '
    + coverage.denominator_definition + '. ' + coverage.decision_definition + '. '
    + coverage.identification_definition + '. ' + coverage.unresolved_definition + '. '
    + coverage.inspection_definition + '.';
  panel.style.display = 'flex';
}

function bindCollectionCoverage() {
  var button = document.getElementById('refreshCollectionCoverage');
  if (button) button.addEventListener('click', refreshCollectionCoverage);
}
