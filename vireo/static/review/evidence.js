// Subject evidence and saved uncertainty are independent of species tags and photo flags.
var reviewEvidenceFilter = 'all';
/** @type {Set<number>} */ var reviewEvidenceSaving = new Set();

/** @param {number|null|undefined} score */
function reviewRelativeScore(score) {
  return typeof score === 'number' && Number.isFinite(score) ? (score * 100).toFixed(2) : 'unknown';
}

/** @param {ReviewPrediction} pred */
function reviewEvidenceDeferred(pred) {
  return ['not_enough_evidence', 'invalid_subject'].includes(pred.evidence?.subject.decision || '');
}

/** @param {number} id */
function reviewEvidenceBlocksAccept(id) {
  var pred = predictions.find(function(p) { return p.id === id; });
  if (!pred) return false;
  var groupId = pred.group_id;
  return reviewEvidenceDeferred(pred) || reviewEvidenceSaving.has(pred.detection_id || 0) || predictions.some(function(sibling) {
    return Boolean(groupId) && sibling.group_id === groupId &&
      (reviewEvidenceDeferred(sibling) || reviewEvidenceSaving.has(sibling.detection_id || 0));
  });
}

/** @param {ReviewPrediction} pred */
function reviewModelEvidenceHtml(pred) {
  var evidence = pred.evidence;
  if (!evidence || evidence.model_candidates.length < 2) return '';
  return '<div class="species-evidence"><strong>' +
    (evidence.disagreement ? 'Models disagree on this region' : 'Models propose the same species on this region') +
    '</strong><ul>' + evidence.model_candidates.map(function(candidate) {
      return '<li>' + escapeHtml(candidate.model) + ': ' + escapeHtml(candidate.species) +
        ' · ' + reviewRelativeScore(candidate.relative_score) + '% relative score</li>';
    }).join('') + '</ul><small>Each score uses its own model and candidate list; compare the names, not the numbers.</small></div>';
}

/** @param {ReviewPrediction} pred */
function reviewEvidenceHtml(pred) {
  var e = pred.evidence;
  if (!e) return '';
  var subject = e.subject;
  var raw = e.score.raw_value == null ? 'Raw match score unavailable.' :
    'Raw match: ' + e.score.raw_value.toFixed(4) + ' (' + escapeHtml(e.score.raw_kind || 'scale unknown') + ').';
  var geography = escapeHtml(e.geography.explanation);
  if (e.geography.latitude != null && e.geography.longitude != null) {
    geography = escapeHtml(e.geography.source || '') + ': ' + e.geography.latitude.toFixed(4) + ', ' +
      e.geography.longitude.toFixed(4) + '. ' + geography;
    if (e.geography.observations_url) geography += ' <a target="_blank" rel="noopener noreferrer" href="' +
      escapeAttr(e.geography.observations_url) + '">Nearby observations (50 km)</a>';
  }
  var decisions = [
    ['not_enough_evidence', 'Not enough evidence'], ['invalid_subject', 'Not an identifiable animal'],
    ['subject_valid', 'Animal visible'], ['unreviewed', 'Clear assessment / revisit'],
  ];
  return '<div class="species-evidence">' +
    (e.reasons.length ? '<ul class="evidence-reasons">' + e.reasons.map(function(reason) {
      return '<li>' + escapeHtml(reason) + '</li>';
    }).join('') + '</ul>' : '') +
    '<details><summary>Score, subject and location evidence</summary>' +
    '<p>' + escapeHtml(e.score.model) + ': ' + escapeHtml(e.score.explanation) +
    (e.score.label_count == null ? ' Candidate count unavailable.' : ' Candidates: ' + e.score.label_count + '.') +
    ' ' + raw + '</p><p>' + escapeHtml(e.match.explanation) + ' A raw match threshold is not a correctness probability.</p>' +
    '<p>Subject: ' + escapeHtml(subject.detector_model || 'detector unknown') +
    ' · detector score ' + (subject.detector_score == null ? 'unavailable' : subject.detector_score.toFixed(4)) +
    ' · category ' + escapeHtml(subject.category || 'unknown') + '. ' + escapeHtml(subject.explanation) + '</p>' +
    '<p>' + geography + '</p></details>' +
    '<p role="status">' + (reviewEvidenceSaving.has(pred.detection_id || 0) ? 'Saving subject assessment…' :
      'Saved subject assessment: <strong>' + escapeHtml(subject.decision.replaceAll('_', ' ')) + '</strong>.') + '</p>' +
    '<div class="evidence-actions">' + decisions.map(function(pair) {
      return '<button type="button" data-evidence-prediction="' + pred.id + '" data-evidence-decision="' + pair[0] + '"' +
        (subject.decision === pair[0] || reviewEvidenceSaving.has(pred.detection_id || 0) ? ' disabled' : '') + '>' + pair[1] + '</button>';
    }).join('') + '</div><small>Saved in this workspace. These assessments do not tag species or change keep/reject flags. Deferred regions cannot be accepted until you clear the assessment to revisit.</small></div>';
}

/** @param {number} predictionId @param {string} decision */
async function saveReviewEvidence(predictionId, decision) {
  if (_predictionsReloading) return;
  var pred = predictions.find(function(p) { return p.id === predictionId; });
  if (!pred?.evidence || !pred.detection_id || reviewEvidenceSaving.has(pred.detection_id)) return;
  var epoch = _loadPredictionsEpoch;
  var detectionId = pred.detection_id;
  reviewEvidenceSaving.add(detectionId);
  renderAll();
  try {
    /** @type {{ok?:boolean;decision:string;revision:number}} */ var saved = await safeFetch('/api/detections/' + detectionId + '/evidence-review', {
      method: 'POST', headers: {'Content-Type': 'application/json'},
      body: JSON.stringify({decision: decision, observed_revision: pred.evidence.subject.revision,
        observed_workspace_id: reviewQueue.workspace}),
    });
    if (!saved || saved.ok !== true) throw new Error('The server did not confirm the subject assessment.');
    if (_predictionEpochStale(epoch)) return;
    await loadPredictions();
  } catch (error) {
    showToast('Subject assessment was not confirmed: ' + reviewErrorMessage(error, 'Reload before retrying.'), 'error');
  } finally {
    reviewEvidenceSaving.delete(detectionId);
    // The reload above advances the epoch. Render the current scope after
    // releasing the save guard so its freshly loaded cards stop saying Saving.
    renderAll();
  }
}

function bindReviewEvidence() {
  document.getElementById('reviewEvidenceFilter')?.addEventListener('change', function(event) {
    reviewEvidenceFilter = /** @type {HTMLSelectElement} */ (event.target).value;
    renderAll();
  });
  document.getElementById('grid')?.addEventListener('click', function(event) {
    if (!(event.target instanceof Element)) return;
    var button = event.target.closest(/** @type {'button'} */ ('button[data-evidence-prediction]'));
    if (button) saveReviewEvidence(Number(button.dataset.evidencePrediction), button.dataset.evidenceDecision || '');
  });
}
