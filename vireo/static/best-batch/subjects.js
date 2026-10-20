// Comparison choices belong to this request, independently of the saved primary.
import { byId, state } from './state.js';
import { fetchJson } from './helpers.js';
import { loadBatch } from './actions.js';

export function startComparison() {
  state.comparisonActive = true;
  loadBatch();
}

export function changeSubject() {
  if (!state.inspectedId || state.analyzing) return;
  var value = /** @type {HTMLSelectElement} */ (byId('subjectChoices')).value;
  if (value) state.subjectOverrides[String(state.inspectedId)] = Number(value);
  else delete state.subjectOverrides[String(state.inspectedId)];
  loadBatch();
}

/** @param {BestBatchCard} card */
export function renderSubject(card) {
  var panel = byId('subjectPanel');
  panel.hidden = !card.subject;
  if (!card.subject) return;
  var select = /** @type {HTMLSelectElement} */ (byId('subjectChoices'));
  select.replaceChildren(new Option('Resolve subject automatically', ''));
  card.subject.candidates.slice().sort(function(a, b) { return a.box.x - b.box.x || a.box.y - b.box.y; }).forEach(function(candidate, index) {
    var name = candidate.predictions.slice(0, 2).map(function(p) { return p.species + ' (' + p.status + ')'; }).join(', ');
    var position = 'detector ' + Math.round(candidate.detector_confidence * 100) + '% · left ' + Math.round(candidate.box.x * 100) + '%, top ' + Math.round(candidate.box.y * 100) + '%';
    select.add(new Option('Subject ' + (index + 1) + ' · ' + position + ' · ' + (name || 'Identity unknown'), String(candidate.detection_id)));
  });
  select.value = String(state.subjectOverrides[String(card.id)] || '');
  var image = /** @type {HTMLImageElement} */ (byId('chosenSubjectImg'));
  image.hidden = !card.crop_url;
  if (card.crop_url) image.src = card.crop_url;
  else image.removeAttribute('src');
  var analysis = card.analysis;
  select.title = card.subject.detection_id ? 'Detection ' + card.subject.detection_id : 'Choose the intended animal';
  /** @type {Record<string, string>} */
  var messages = {
    ready: 'Selected subject has current analysis.',
    subject_analysis_missing: 'Selected subject needs analysis.',
    subject_identity_uncertain: 'Species is not confirmed on a specific subject. Choose the intended animal.',
    subject_identity_conflict: 'This subject has a conflicting species confirmation. Review its identity.',
    multiple_matching_subjects: 'Several subjects match this species. Choose the intended animal.',
  };
  /** @type {Record<string, string>} */
  var eyes = {disabled: 'Eye analysis is off.', model_unavailable: 'Eye model is unavailable.',
    identity_unavailable: 'Eye analysis needs a supported species identification.',
    not_located: 'No reliable eye located.', measured: 'Eye focus measured on the selected subject.'};
  byId('subjectEvidenceStatus').textContent = (messages[card.evidence_status || ''] || '') +
    (card.subject.selection === 'selected' ? ' Subject chosen for this comparison.' : '') +
    (analysis ? ' Mask measurements use a ' + analysis.width + ' × ' + analysis.height + ' analysis image from the original.' : '') +
    ' ' + (eyes[card.eye_status || ''] || '') +
    ' Inspect eye visibility, face obstructions, and pose in the full frame and selected-subject crop.';
}

export async function analyzeSubjects() {
  var data = state.bestBatchData;
  if (!data?.comparison || state.analyzing) return;
  var sequence = ++state.requestSequence;
  state.analyzing = true;
  ['analyzeSubjectsBtn', 'compareSubjectsBtn', 'flagBtn', 'rejectBtn', 'subjectChoices', 'requestedSpecies'].forEach(function(id) {
    /** @type {HTMLButtonElement} */ (byId(id)).disabled = true;
  });
  try {
    var job = await fetchJson('/api/photos/burst-evidence', {method: 'POST', headers: {'Content-Type': 'application/json'},
      body: JSON.stringify({photo_ids: data.photo_ids, compare_subjects: true, ...data.comparison, analyze: true})});
    while (sequence === state.requestSequence) {
      var progress = await fetchJson('/api/jobs/' + job.job_id);
      byId('comparisonStatus').textContent = 'Analyzing selected subjects: ' + (progress.progress?.current || 0) +
        ' of ' + (progress.progress?.total || data.count) + ' frames processed.';
      if (progress.status === 'completed') {
        var result = progress.result || {};
        byId('comparisonStatus').textContent = (result.analyzed || 0) + ' analyzed, ' + (result.cached || 0) +
          ' cached, ' + (result.unresolved?.length || 0) + ' identities unresolved, ' + (result.errors?.length || 0) +
          ' analysis errors.' + (result.errors?.length ? ' ' + result.errors.map(function(/** @type {{photo_id: number, error: string}} */ e) { return 'Photo ' + e.photo_id + ': ' + e.error; }).join('; ') : '');
        break;
      }
      if (progress.status === 'failed' || progress.status === 'cancelled') throw new Error(progress.error || 'Analysis did not complete');
      await new Promise(function(resolve) { setTimeout(resolve, 750); });
    }
  } catch (error) {
    byId('comparisonStatus').textContent = error instanceof Error ? error.message : String(error);
  } finally {
    state.analyzing = false;
    ['analyzeSubjectsBtn', 'compareSubjectsBtn', 'subjectChoices', 'requestedSpecies'].forEach(function(id) {
      /** @type {HTMLButtonElement} */ (byId(id)).disabled = false;
    });
    if (sequence === state.requestSequence) await loadBatch();
  }
}
