import {safeFetch, escapeHtml, showToast} from './shared.js';

const methods = {
  manual_unspecified: 'Manual / unspecified',
  direct_visual: 'Direct visual confirmation',
  adjacent_frames: 'Inferred from adjacent frames',
  model_proposed: 'Model proposed',
  imported_unknown: 'Imported / unknown'
};

export function renderSpeciesProvenance(/** @type {BrowsePhoto} */ photo, /** @type {HTMLElement} */ container) {
  (photo.species_assignments || []).forEach(assignment => {
    const panel = document.createElement('details');
    panel.className = 'species-provenance';
    panel.style.cssText = 'margin-top:8px;white-space:normal;';
    const label = methods[/** @type {keyof typeof methods} */ (assignment.method)] || assignment.method;
    panel.innerHTML = '<summary>' + escapeHtml(assignment.species) + ': ' + escapeHtml(label) + '</summary>';
    const evidence = document.createElement('div');
    const notes = [assignment.reviewer && 'Reviewer: ' + assignment.reviewer,
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
    panel.appendChild(evidenceForm(photo.id, assignment));
    container.appendChild(panel);
  });
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
      const response = await safeFetch('/api/photos/' + photoId + '/species/' + assignment.keyword_id + '/provenance',
        {method: 'POST', headers: {'Content-Type': 'application/json'}, body: JSON.stringify(body)});
      const container = form.closest('.species-provenance')?.parentElement;
      if (container) {
        container.querySelectorAll('.species-provenance').forEach(panel => panel.remove());
        renderSpeciesProvenance({id: photoId, filename: '', rating: null,
          species_assignments: response.species_assignments}, container);
      }
      showToast('Species evidence saved');
    } catch (error) {
      showToast(error instanceof Error ? error.message : 'Could not save species evidence', 'error');
    } finally { save.disabled = false; }
  });
  return form;
}
