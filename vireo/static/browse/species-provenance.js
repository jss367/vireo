import {loadDetail} from './detail.js';
import {safeFetch, escapeHtml, showToast} from './shared.js';

const methods = {
  manual_unspecified: 'Manual / unspecified',
  direct_visual: 'Direct visual confirmation',
  adjacent_frames: 'Inferred from adjacent frames',
  model_proposed: 'Model proposed',
  imported_unknown: 'Imported / unknown'
};

export function speciesDisplayLabel(/** @type {BrowseSpeciesNames} */ names, /** @type {string} */ fallback) {
  const preferred = names.display_name || fallback;
  return preferred + (names.synonyms?.length ? ' (also known as ' + names.synonyms.join(', ') + ')' : '');
}

export function renderSpeciesProvenance(/** @type {BrowsePhoto} */ photo, /** @type {HTMLElement} */ container) {
  (photo.species_assignments || []).forEach(assignment => {
    const panel = document.createElement('details');
    panel.className = 'species-provenance';
    panel.style.cssText = 'margin-top:8px;white-space:normal;';
    const label = methods[/** @type {keyof typeof methods} */ (assignment.method)] || assignment.method;
    panel.innerHTML = '<summary>' + escapeHtml(speciesDisplayLabel(assignment, assignment.species)) + ': ' + escapeHtml(label) + ' · ' +
      (assignment.review ? (assignment.review.resolution ? 'Reviewed' : 'Conflict needs review') : 'Saved') + '</summary>';
    const evidence = document.createElement('div');
    const notes = [assignment.scientific_name && 'Taxon: ' + assignment.scientific_name +
        ' (catalog ID ' + assignment.taxon_id + ')',
      assignment.reviewer && 'Reviewer: ' + assignment.reviewer,
      assignment.reason && 'Reason: ' + assignment.reason,
      assignment.evidence_source && 'Evidence: ' + assignment.evidence_source,
      assignment.detection_id && 'Subject detection: ' + assignment.detection_id +
        (assignment.subject_photo_id ? ' (source photo ' + assignment.subject_photo_id + ')' : ''),
      assignment.confidence == null ? 'Confidence: not recorded — ' + (assignment.confidence_meaning || 'Not recorded') :
        'Confidence: ' + assignment.confidence + ' — ' + assignment.confidence_meaning];
    notes.filter(Boolean).forEach(note => {
      const line = document.createElement('div');
      line.textContent = String(note);
      evidence.appendChild(line);
    });
    (assignment.supporting_photo_ids || []).forEach(id => {
      const link = document.createElement('a');
      link.href = '/browse?photo_id=' + id;
      link.textContent = 'Supporting frame ' + id;
      link.style.marginRight = '8px';
      evidence.appendChild(link);
    });
    panel.appendChild(evidence);
    if (assignment.review) appendReview(panel, assignment.review);
    panel.appendChild(evidenceForm(photo.id, assignment));
    if (assignment.review_version) panel.appendChild(reviewForm(photo, assignment));
    container.appendChild(panel);
  });
  const attached = new Set(photo.species_assignments?.map(a => a.review?.id));
  for (const review of photo.assignment_reviews || []) {
    if (attached.has(review.id)) continue;
    const details = document.createElement('details');
    const summary = document.createElement('summary');
    summary.textContent = 'Past assignment review · ' + review.species;
    details.appendChild(summary);
    appendReview(details, review);
    container.appendChild(details);
  }
}

function appendReview(/** @type {HTMLElement} */ panel, /** @type {BrowseAssignmentReview} */ review) {
  const original = review.original_provenance;
  const source = document.createElement('p');
  const method = methods[/** @type {keyof typeof methods} */ (original.method)] || original.method;
  source.textContent = ['Original assignment: ' + review.species, method, original.reviewer,
    original.reason, original.evidence_source, original.supporting_photo_ids?.length &&
    'Supporting frames: ' + original.supporting_photo_ids.join(', ')].filter(Boolean).join(' · ');
  panel.appendChild(source);
  for (const [title, note] of /** @type {[string, BrowseReviewNote | null][]} */ ([
    ['Conflict reported for ' + review.species, review.conflict],
    ['Resolution (' + review.resolution?.action + ')', review.resolution],
  ])) {
    if (!note) continue;
    const line = document.createElement('p');
    line.textContent = title + ' by ' + note.reviewer + ' · ' + note.reason + ' · Evidence: ' + note.evidence_source;
    panel.appendChild(line);
  }
}

function reviewForm(/** @type {BrowsePhoto} */ photo, /** @type {BrowseSpeciesAssignment} */ assignment) {
  const pending = assignment.review && !assignment.review.resolution;
  const details = document.createElement('details');
  const summary = document.createElement('summary');
  summary.textContent = pending ? 'Resolve conflict' : 'Report conflicting evidence';
  details.appendChild(summary);
  const form = document.createElement('form');
  const action = document.createElement('select');
  action.name = 'action';
  action.setAttribute('aria-label', 'Resolution');
  for (const [key, label] of pending ? [['retain', 'Retain assignment'], ['amend', 'Change species'],
    ['remove', 'Remove assignment']] : [['report', 'Report conflict']]) action.add(new Option(label, key));
  action.hidden = !pending;
  form.appendChild(action);
  for (const [key, label] of Object.entries({species: 'Replacement species', reviewer: 'Reviewer',
    reason: 'Reason', evidence_source: 'Evidence reference'})) {
    const field = document.createElement('label');
    field.style.display = 'block';
    field.textContent = label;
    const input = document.createElement('input');
    input.name = key;
    input.required = true;
    field.appendChild(input);
    if (key === 'species') {
      const toggle = () => {
        input.disabled = action.value !== 'amend';
        field.style.display = input.disabled ? 'none' : 'block';
      };
      toggle();
      action.addEventListener('change', toggle);
    }
    form.appendChild(field);
  }
  const save = document.createElement('button');
  save.textContent = pending ? 'Save resolution' : 'Save conflict';
  form.appendChild(save);
  form.addEventListener('submit', async event => {
    event.preventDefault();
    save.disabled = true;
    try {
      const body = {...Object.fromEntries(new FormData(form)), observed_workspace_id: photo.review_workspace_id,
        review_version: assignment.review_version};
      const response = await safeFetch('/api/photos/' + photo.id + '/species/' + assignment.keyword_id + '/review',
        {method: 'POST', headers: {'Content-Type': 'application/json'}, body: JSON.stringify(body)});
      if (form.isConnected) await loadDetail(photo.id);
      showToast(response.pipeline_cache_stale ? 'Review saved. Refresh Process results.' : 'Assignment review saved');
    } catch (error) {
      showToast(error instanceof Error ? error.message : 'Could not save review', 'error');
    } finally { save.disabled = false; }
  });
  details.appendChild(form);
  return details;
}

function evidenceForm(/** @type {number} */ photoId, /** @type {BrowseSpeciesAssignment} */ assignment) {
  const form = document.createElement('form');
  const select = document.createElement('select');
  select.setAttribute('aria-label', 'Identification method');
  Object.entries(methods).forEach(([value, label]) => {
    const option = document.createElement('option');
    option.value = value; option.textContent = label;
    select.appendChild(option);
  });
  select.value = assignment.method;
  form.appendChild(select);
  const inputs = /** @type {Record<'reviewer' | 'reason' | 'evidence_source' | 'supporting_photo_ids' | 'confidence' | 'confidence_meaning', HTMLInputElement>} */ (/** @type {unknown} */ ({}));
  const fields = {reviewer: 'Reviewer', reason: 'Reason or explanation', evidence_source: 'Evidence source',
    supporting_photo_ids: 'Supporting frame IDs, separated by commas',
    confidence: 'Confidence (optional, 0–1)', confidence_meaning: 'What confidence measures'};
  Object.entries(fields).forEach(([key, label]) => {
    const field = document.createElement('label');
    field.textContent = label;
    field.style.display = 'block';
    const input = document.createElement('input');
    input.type = key === 'confidence' ? 'number' : 'text'; input.style.width = '100%';
    if (key === 'confidence') { input.min = '0'; input.max = '1'; input.step = 'any'; }
    input.value = key === 'supporting_photo_ids' ? (assignment.supporting_photo_ids || []).join(', ') :
      String(assignment[/** @type {'reviewer' | 'reason' | 'evidence_source' | 'confidence' | 'confidence_meaning'} */ (key)] ?? '');
    if (key === 'confidence_meaning' && input.value === 'Not recorded') input.value = '';
    field.appendChild(input); inputs[/** @type {keyof typeof inputs} */ (key)] = input; form.appendChild(field);
  });
  const save = document.createElement('button');
  save.type = 'submit'; save.textContent = 'Save species evidence';
  form.appendChild(save);
  form.addEventListener('submit', async event => {
    event.preventDefault(); save.disabled = true;
    try {
      const body = {method: select.value, reviewer: inputs.reviewer.value, reason: inputs.reason.value,
        evidence_source: inputs.evidence_source.value, confidence: inputs.confidence.value.trim() ? Number(inputs.confidence.value) : null,
        confidence_meaning: inputs.confidence_meaning.value, detection_id: assignment.detection_id ?? null,
        subject_photo_id: assignment.subject_photo_id ?? null,
        supporting_photo_ids: inputs.supporting_photo_ids.value.trim() ?
          inputs.supporting_photo_ids.value.split(',').map(id => Number(id.trim())) : []};
      await safeFetch('/api/photos/' + photoId + '/species/' + assignment.keyword_id + '/provenance',
        {method: 'POST', headers: {'Content-Type': 'application/json'}, body: JSON.stringify(body)});
      if (form.isConnected) await loadDetail(photoId);
      showToast('Species evidence saved');
    } catch (error) {
      showToast(error instanceof Error ? error.message : 'Could not save species evidence', 'error');
    } finally { save.disabled = false; }
  });
  return form;
}
