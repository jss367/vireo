// Comparison choices belong to this request, independently of the saved primary.
import { byId, state } from './state.js';
import { openSubjectBox } from './shared.js';
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
  renderMaskReview(card);
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
    subject_analysis_unavailable: 'Selected subject analysis is unavailable under the current scoring settings.',
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

let maskControls = new AbortController();

/** @param {BestBatchCard} card */
function renderMaskReview(card) {
  maskControls.abort();
  maskControls = new AbortController();
  const options = {signal: maskControls.signal};
  const review = card.mask_review;
  byId('maskReview').hidden = !review?.mask_digest;
  var history = byId('maskHistory');
  history.replaceChildren();
  for (var previous of review?.history || []) {
    var link = document.createElement('a');
    link.href = previous.url;
    link.target = '_blank';
    link.textContent = 'Earlier ' + previous.verdict + ' mask · subject ' + previous.detection_id;
    history.append(link, document.createElement('br'));
  }
  if (!review?.mask_digest) return;
  var status = byId('maskVerdict');
  status.textContent = (review.verdict === 'invalid' ? 'Reviewer excluded this mask from scoring.' :
    review.verdict === 'valid' ? 'Reviewer confirmed this animal mask.' : 'Mask content has not been reviewed.') +
    (review.hint ? ' ' + review.hint : '');
  byId('inspectMask').setAttribute('href', card.focus_crop_mask_url || '');
  byId('inspectFullMask').setAttribute('href', card.mask_url || '');
  byId('refineMask').hidden = review.verdict !== 'invalid';
  var pending = false;
  for (const verdict of ['valid', 'invalid']) {
    const button = /** @type {HTMLButtonElement} */ (byId(verdict + 'Mask'));
    button.disabled = review.verdict === verdict;
    button.addEventListener('click', async () => {
      if (pending || state.analyzing || !state.bestBatchData?.comparison) return;
      pending = true;
      for (const value of ['valid', 'invalid']) {
        /** @type {HTMLButtonElement} */ (byId(value + 'Mask')).disabled = true;
      }
      try {
        await fetchJson('/api/photos/' + card.id + '/mask-review', {method: 'PUT',
          headers: {'Content-Type': 'application/json'}, body: JSON.stringify({verdict, expected_revision: review.revision,
            detection_id: card.subject?.detection_id, analysis_key: card.subject?.analysis_key, mask_digest: review.mask_digest})});
        await loadBatch();
      } catch (error) {
        if (!options.signal.aborted && state.inspectedId === card.id) {
          status.textContent = 'Review not confirmed. Reload the comparison before choosing again. ' +
            (error instanceof Error ? error.message : String(error));
          // Keep this snapshot read-only until a fresh comparison is loaded.
        }
      }
    }, options);
  }
  byId('refineMask').addEventListener('click', () => {
    if (state.analyzing || !state.bestBatchData?.comparison) return;
    openSubjectBox(card.id, result => {
      if (!result.primary_detection_id) return;
      state.subjectOverrides[String(card.id)] = result.primary_detection_id;
      analyzeSubjects(card.id);
    });
  }, options);
}

/** @param {number=} photoId Analyze only the newly refined region when supplied. */
export async function analyzeSubjects(photoId) {
  var data = state.bestBatchData;
  if (!data?.comparison || state.analyzing) return;
  var sequence = ++state.requestSequence;
  state.analyzing = true;
  ['analyzeSubjectsBtn', 'compareSubjectsBtn', 'flagBtn', 'rejectBtn', 'subjectChoices', 'requestedSpecies'].forEach(function(id) {
    /** @type {HTMLButtonElement} */ (byId(id)).disabled = true;
  });
  try {
    var job = await fetchJson('/api/photos/burst-evidence', {method: 'POST', headers: {'Content-Type': 'application/json'},
      body: JSON.stringify({...data.comparison, photo_ids: photoId ? [photoId] : data.photo_ids,
        subject_overrides: photoId ? {[photoId]: state.subjectOverrides[String(photoId)]} : data.comparison.subject_overrides,
        compare_subjects: true, analyze: true})});
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
