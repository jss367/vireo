// Every row comes from the live catalog. Model evidence stays per subject;
// adding a species preserves other subjects, and splitting is always opt-in.
/** @param {ReviewSpeciesEdit} initial @returns {Promise<ReviewSpeciesEdit | null>} */
function reviewEncounterSpecies(initial) {
  return new Promise(function(resolve) {
    var dialog = document.createElement('dialog');
    dialog.id = 'speciesConfirmationPreview';
    dialog.setAttribute('aria-label', 'Preview species confirmation');
    dialog.innerHTML = '<h2>Preview species confirmation</h2>' +
      '<p>Inspect every frame and subject before applying. ' + (initial.group_changes ? 'This confirmation also saves the staged Group Review pick, reject and removal decisions together. Cancel saves no changes. ' : 'Existing flags are preserved. ') + 'Stored model evidence includes historical label sets and low-confidence detections; it is not a confirmation.</p>' +
      '<label>Species action <select class="species-preview-mode"><option value="add">Add species; preserve existing species</option>' +
      '<option value="replace">Replace the named species only</option></select></label> ' +
      '<label>Species to replace <select class="species-preview-previous" disabled></select></label>' +
      '<p><label><input type="checkbox" class="species-preview-split"> Separate selected frames into another encounter (temporal transition)</label></p>' +
      '<p><label>From frame <input class="species-preview-from" type="number" value="1" min="1"></label> ' +
      '<label>Through frame <input class="species-preview-through" type="number" min="1" value="' + initial.photo_ids.length + '"></label> ' +
      '<button class="species-preview-range btn btn-secondary">Select frame range</button> (shown order)</p>' +
      '<p class="species-preview-summary" role="status">Loading exact affected photos…</p>' +
      '<div class="species-preview-matrix"></div>' +
      '<p><button class="species-preview-cancel btn btn-secondary">Cancel</button> <button class="species-preview-apply btn btn-primary" disabled>' + (initial.group_changes ? 'Apply all reviewed changes' : 'Apply confirmation') + '</button></p>';
    document.body.appendChild(dialog);
    var session = createSpeciesPreviewSession(dialog, initial, resolve);
    dialog.querySelector('.species-preview-cancel')?.addEventListener('click', session.cancel);
    dialog.addEventListener('cancel', function(e) { e.preventDefault(); session.cancel(); });
    dialog.showModal();
    session.refresh();
  });
}

/** @param {HTMLDialogElement} dialog @param {ReviewSpeciesEdit} initial @param {(value: ReviewSpeciesEdit | null) => void} resolve */
function createSpeciesPreviewSession(dialog, initial, resolve) {
  var selected = new Set(initial.photo_ids);
  var source = initial.photo_ids.slice();
  var sequence = 0;
  /** @type {ReviewSpeciesEdit | null} */ var approved = null;
  var mode = /** @type {HTMLSelectElement} */ (dialog.querySelector('.species-preview-mode'));
  var previous = /** @type {HTMLSelectElement} */ (dialog.querySelector('.species-preview-previous'));
  var split = /** @type {HTMLInputElement} */ (dialog.querySelector('.species-preview-split'));
  var apply = /** @type {HTMLButtonElement} */ (dialog.querySelector('.species-preview-apply'));
  var summary = /** @type {HTMLElement} */ (dialog.querySelector('.species-preview-summary'));
  var matrix = /** @type {HTMLElement} */ (dialog.querySelector('.species-preview-matrix'));
  function finish() { sequence++; dialog.close(); dialog.remove(); }
  function cancel() { finish(); resolve(null); }
  async function refresh() {
    var seq = ++sequence;
    approved = null;
    apply.disabled = true;
    previous.disabled = mode.value !== 'replace';
    if (!selected.size) { summary.textContent = 'Select at least one photo.'; return; }
    if (mode.value === 'replace' && !previous.value) { summary.textContent = 'Choose the confirmed species to replace.'; return; }
    /** @type {ReviewSpeciesEdit} */ var body = {...initial, photo_ids: source.filter(id => selected.has(id)), source_photo_ids: source, add: mode.value === 'add', split: split.checked};
    if (mode.value === 'replace') body.previous_species = previous.value;
    summary.textContent = 'Revalidating selected photos…';
    try {
      var data = await safeFetch('/api/encounters/species', {method: 'POST', headers: {'Content-Type': 'application/json'}, body: JSON.stringify({...body, preview: true})});
      if (seq !== sequence || !dialog.open) return;
      if (!data.ok || !data.photos || !data.preview_token) throw new Error('Preview could not be loaded');
      if (!previous.options.length) {
        var names = data.previous_species_choices || [];
        previous.innerHTML = names.map(name => '<option>' + escapeHtml(name) + '</option>').join('');
      }
      matrix.innerHTML = renderSpeciesPreviewMatrix(data.photos);
      matrix.querySelectorAll('input[data-photo-id]').forEach(function(node) {
        node.addEventListener('change', function() {
          var input = /** @type {HTMLInputElement} */ (node);
          var id = Number(input.dataset.photoId);
          if (input.checked) selected.add(id); else selected.delete(id);
          refresh();
        });
      });
      summary.textContent = data.photo_count + ' of ' + source.length + ' photos selected · ' + body.species +
        (body.split ? ' · Separate selected frames into another encounter' : ' · Keep encounter membership');
      approved = {...body, preview_token: data.preview_token};
      apply.disabled = false;
    } catch (error) {
      if (seq === sequence && dialog.open) summary.textContent = 'Preview changed or failed. Close and reopen to reload current photos.';
    }
  }
  dialog.querySelector('.species-preview-range')?.addEventListener('click', function() {
    var from = Number(/** @type {HTMLInputElement} */ (dialog.querySelector('.species-preview-from')).value);
    var through = Number(/** @type {HTMLInputElement} */ (dialog.querySelector('.species-preview-through')).value);
    if (!Number.isInteger(from) || !Number.isInteger(through) || from < 1 || through > source.length || from > through) {
      summary.textContent = 'Choose a valid frame range from 1 through ' + source.length + '.'; return;
    }
    selected = new Set(source.slice(from - 1, through));
    refresh();
  });
  mode.addEventListener('change', refresh);
  previous.addEventListener('change', refresh);
  split.addEventListener('change', refresh);
  apply.addEventListener('click', function() { if (approved) { var body = approved; finish(); resolve(body); } });
  return {refresh: refresh, cancel: cancel};
}

/** @param {ReviewSpeciesPreviewPhoto[]} photos */
function renderSpeciesPreviewMatrix(photos) {
  return '<table><thead><tr><th>Apply</th><th>Photo</th><th>Existing confirmed species</th><th>Subjects and model evidence</th><th>Proposed changes</th></tr></thead><tbody>' +
    photos.map(function(photo, index) {
      var subjects = photo.subjects.map(subject => renderSpeciesPreviewSubject(photo.id, subject)).join('');
      return '<tr><td><input type="checkbox" data-photo-id="' + photo.id + '" aria-label="Include ' + escapeHtml(photo.filename) + '"' + (photo.selected ? ' checked' : '') + '></td>' +
        '<td><a href="/thumbnails/' + photo.id + '.jpg" target="_blank" rel="noopener"><img loading="lazy" width="120" src="/thumbnails/' + photo.id + '.jpg" alt="' + escapeHtml(photo.filename) + '"></a><br>' +
        'Frame ' + (index + 1) + ' · ' + escapeHtml(photo.filename) + ' · #' + photo.id + '<br>' + escapeHtml(photo.timestamp || 'No capture time') + '<br>Flag: ' + escapeHtml(photo.flag || 'none') + '</td>' +
        '<td>' + (photo.existing_species.map(escapeHtml).join('<br>') || 'None') + '</td><td>' + (subjects || 'No detected subjects') + '</td><td>' +
        photo.additions.map(name => 'Add ' + escapeHtml(name)).concat(photo.removals.map(name => 'Remove ' + escapeHtml(name))).join('<br>') +
        ((!photo.additions.length && !photo.removals.length) ? (photo.selected ? 'No keyword changes' : 'Excluded; no changes') : '') + '</td></tr>';
    }).join('') + '</tbody></table>';
}

/** @param {number} photoId @param {ReviewSpeciesPreviewPhoto['subjects'][number]} subject */
function renderSpeciesPreviewSubject(photoId, subject) {
  /** @param {ReviewSpeciesPreviewPhoto['subjects'][number]['predictions'][number]} prediction */
  function renderPrediction(prediction) {
    return escapeHtml(prediction.species || '(unnamed)') + ' ' + Math.round(100 * (prediction.confidence || 0)) +
      '% (' + escapeHtml(prediction.classifier_model) + ', labels: ' + escapeHtml(prediction.labels_fingerprint) + ', ' + escapeHtml(prediction.status) + ') · ' +
      (prediction.historical ? 'Historical label set' : 'Latest stored label set');
  }
  var evidence = subject.predictions.filter(p => !p.historical).map(renderPrediction).join('<br>');
  var history = subject.predictions.filter(p => p.historical).map(renderPrediction).join('<br>');
  var box = '<span class="species-preview-subject"><img loading="lazy" width="120" src="/thumbnails/' + photoId + '.jpg" alt="Subject ' + subject.detection_id + '">' +
    '<span style="left:' + Number(subject.box_x || 0) * 100 + '%;top:' + Number(subject.box_y || 0) * 100 + '%;width:' + Number(subject.box_w || 0) * 100 + '%;height:' + Number(subject.box_h || 0) * 100 + '%"></span></span>';
  return '<details open><summary>Subject ' + subject.detection_id + ' · ' + escapeHtml(subject.detector_model) +
    ' · ' + Math.round(100 * (subject.detector_confidence || 0)) + '% detector confidence' +
    (subject.below_threshold ? ' · Below current threshold' : '') + '</summary>' + box + (evidence || 'No latest stored predictions') +
    (history ? '<details><summary>Historical label sets</summary>' + history + '</details>' : '') + '</details>';
}
