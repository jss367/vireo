/* Per-detection suggestions are previews. Only explicit Use actions edit a photo. */
(function () {
  'use strict';
  let data = null;
  let generation = 0;
  let busy = false;
  const panel = document.getElementById('lightboxSubjects');
  if (!panel) return;
  const list = document.getElementById('lightboxSubjectList');
  const details = document.getElementById('lightboxSubjectDetails');
  const status = document.getElementById('lightboxSubjectStatus');
  const corrected = document.getElementById('lightboxSubjectCorrected');
  const automatic = document.getElementById('lightboxSubjectAutomatic');
  const cropButton = document.getElementById('lightboxSubjectUseCrop');
  const analyzeButton = document.getElementById('lightboxSubjectAnalyze');
  const exposureButton = document.getElementById('lightboxSubjectUseExposure');

  function primary() { return data && data.subjects.find(s => s.is_primary); }
  function previewUrl(subject, correction) {
    return '/photos/' + data.photo_id + '/crop?detection_id=' + subject.id
      + (correction ? '&suggested=1' : '') + '&v=' + encodeURIComponent(subject.analysis?.source_key || 'pending');
  }
  function render() {
    list.replaceChildren();
    details.replaceChildren();
    panel.hidden = !data || !data.subjects.length;
    if (panel.hidden) return;
    // Scoped Pipeline Review views open the lightbox with _lbReadOnly to
    // freeze mutating writes. Every subject control here reaches a
    // PUT/POST — subject cards and "Choose automatically" invoke
    // sync_primary which can clear mask/embedding/eye state, "Analyze
    // subjects" enqueues a job that writes detection_subjects and can
    // change the primary, and Use crop / Use exposure PUT the edit
    // recipe — so they all must honor _lbReadOnly (Codex r4056621185).
    const readOnly = typeof _lbReadOnly !== 'undefined' && _lbReadOnly;
    const readOnlyTitle = readOnly && typeof _lbReadOnlyMessage === 'string' ? _lbReadOnlyMessage : null;
    document.getElementById('lightboxSubjectSummary').textContent = 'Subjects (' + data.subjects.length + ')';
    // Stable spatial order keeps thumbnails from jumping when primary changes.
    data.subjects.slice().sort((a, b) => a.box_x - b.box_x || a.box_y - b.box_y || a.id - b.id).forEach((subject, index) => {
      const button = document.createElement('button');
      button.type = 'button';
      button.className = 'lb-subject-card';
      button.setAttribute('aria-pressed', String(subject.is_primary));
      button.disabled = busy || readOnly;
      const img = document.createElement('img');
      img.src = previewUrl(subject, false);
      img.alt = 'Subject ' + (index + 1);
      const caption = document.createElement('span');
      caption.textContent = subject.predictions[0]?.species || 'Subject ' + (index + 1);
      button.append(img, caption);
      button.title = readOnlyTitle
        || (subject.is_primary ? 'Primary: ' : 'Make primary: ') + caption.textContent;
      button.onclick = () => choose(subject.id);
      list.append(button);
    });
    const subject = primary();
    if (!subject) return;
    const img = document.createElement('img');
    img.className = 'lb-subject-preview';
    img.src = previewUrl(subject, corrected.checked);
    img.alt = 'Suggested crop for the primary subject';
    const description = document.createElement('div');
    const species = document.createElement('strong');
    const prediction = subject.predictions[0];
    species.textContent = prediction ? prediction.species + ' · ' + Math.round(prediction.confidence * 100) + '%' : 'Species not predicted';
    const quality = document.createElement('p');
    quality.textContent = subject.analysis ? 'Quality ' + Math.round(subject.analysis.quality_score * 100) + '/100 · Exposure '
      + (subject.analysis.exposure_ev >= 0 ? '+' : '') + subject.analysis.exposure_ev.toFixed(2) + ' EV' : 'Analyze subjects to compute quality and exposure.';
    quality.title = 'Quality is measured within this detection box, before exposure correction.';
    description.append(species, quality);
    details.append(img, description);
    analyzeButton.hidden = data.subjects.every(s => s.analysis);
    analyzeButton.disabled = busy || readOnly;
    automatic.disabled = busy || data.selection === 'automatic' || readOnly;
    corrected.disabled = !subject.analysis;
    cropButton.disabled = busy || !subject.analysis || readOnly;
    exposureButton.disabled = busy || !subject.analysis || readOnly;
    if (readOnlyTitle) {
      analyzeButton.title = readOnlyTitle;
      automatic.title = readOnlyTitle;
      cropButton.title = readOnlyTitle;
      exposureButton.title = readOnlyTitle;
    } else {
      analyzeButton.removeAttribute('title');
      automatic.removeAttribute('title');
      cropButton.removeAttribute('title');
      exposureButton.removeAttribute('title');
    }
    status.textContent = data.choice_unavailable ? 'Your chosen subject is unavailable. Showing the best retained subject; your choice is remembered.'
      : data.selection === 'manual' ? 'Primary chosen by you. Saved edits are unchanged.'
      : data.subjects.every(s => s.analysis) ? 'Primary selected by quality. Suggestions leave saved edits unchanged.'
      : 'Quality analysis is incomplete. Primary selection is provisional.';
  }
  async function load(photoId) {
    const seq = ++generation;
    data = null;
    panel.hidden = true;
    try {
      const result = await safeFetch('/api/photos/' + photoId + '/subjects', {}, {toast: false});
      if (seq !== generation || _lightboxCurrentId !== photoId) return;
      data = result;
      render();
    } catch (error) {
      if (seq !== generation || _lightboxCurrentId !== photoId) return;
      panel.hidden = false;
      list.replaceChildren(); details.replaceChildren();
      status.textContent = 'Could not load subjects: ' + error.message;
    }
  }
  async function choose(detectionId) {
    if (!data || busy || data.photo_id !== _lightboxCurrentId) return;
    // Choosing a primary — including "Choose automatically" — reaches
    // sync_primary, which clears mask, embedding and eye state; block
    // it in a scoped Pipeline Review lightbox (Codex r4056621185).
    if (typeof _lbGuardReadOnly === 'function' && _lbGuardReadOnly()) return;
    const photoId = data.photo_id;
    const seq = generation;
    busy = true;
    render();
    try {
      const result = await safeFetch('/api/photos/' + photoId + '/primary-subject', {
        method: 'PUT', headers: {'Content-Type': 'application/json'}, body: JSON.stringify({detection_id: detectionId})
      }, {toast: false});
      if (seq !== generation || _lightboxCurrentId !== photoId) return;
      if (data.primary_detection_id !== result.primary_detection_id) {
        const photo = _lbPhotoDataByPhoto[String(photoId)];
        if (photo) {
          ['eye_x', 'eye_y', 'eye_conf', 'eye_tenengrad'].forEach(key => { photo[key] = null; });
          _lbRenderEyeCrosshair(photo);
        }
        _lbResetMaskOverlay();
      }
      data = result;
      _lbLoadDetections(photoId);
      _lbLoadMaskVariants(photoId);
      document.dispatchEvent(new CustomEvent('vireo:primary-subject-changed', {detail: {photoId}}));
    } catch (error) {
      // A request that finished after the user opened a different photo
      // belongs to no active lightbox subject panel; surfacing its error
      // would blame the newly-open photo. Match the success-path
      // stale-request checks.
      if (seq !== generation || _lightboxCurrentId !== photoId) return;
      showToast(error.message || 'Could not change primary subject', 'error');
    } finally {
      busy = false;
      if (data?.photo_id === _lightboxCurrentId) render();
    }
  }
  async function useSuggestion(kind) {
    const subject = primary();
    if (!subject?.analysis || busy || _lbEditWritePending || data.photo_id !== _lightboxCurrentId) return;
    // Guard against a rogue click before render() runs after the read-only
    // state flips (Codex r4056563011). _lbGuardReadOnly surfaces the same
    // read-only toast every other edit-recipe writer uses.
    if (typeof _lbGuardReadOnly === 'function' && _lbGuardReadOnly()) return;
    const photoId = data.photo_id;
    const seq = generation;
    busy = true;
    _lbSetEditBusy(true);
    render();
    try {
      _lbFlushPendingAdjustmentSave();
      await _lbWaitForAdjustmentSaveIdle(photoId);
      if (_lightboxCurrentId !== photoId || generation !== seq) return;
      // Fetch the latest recipe so another editor's changes aren't replaced
      // by the lightbox's initially loaded snapshot.
      const current = await safeFetch('/api/photos/' + photoId + '/edit-recipe', {}, {toast: false});
      if (_lightboxCurrentId !== photoId || generation !== seq) return;
      const recipe = current.recipe || {};
      if (kind === 'crop') {
        if (recipe.straighten) throw new Error('Reset straightening before applying a suggested crop.');
        const orientation = {rotation: recipe.rotation, flip: recipe.flip};
        recipe.crop = _lbTransformBoxByRecipe(subject.analysis.crop, orientation);
      } else {
        recipe.adjustments = Object.assign({}, recipe.adjustments, {exposure: subject.analysis.exposure_ev});
      }
      const result = await safeFetch('/api/photos/' + photoId + '/edit-recipe', {
        method: 'PUT', headers: {'Content-Type': 'application/json'},
        body: JSON.stringify({recipe, description: 'Applied subject ' + kind + ' suggestion'})
      }, {toast: false});
      _lbMarkEditRecipeWrite(photoId);
      _lbRememberEditRecipe(photoId, result.recipe);
      _vireoBumpRenderVersion(photoId);
      if (typeof window.vireoRefreshPhotoRenders === 'function') window.vireoRefreshPhotoRenders([photoId]);
      if (_lightboxCurrentId === photoId) _lbReloadCurrentRenderAfterEdit(photoId);
    } catch (error) {
      if (seq !== generation || _lightboxCurrentId !== photoId) return;
      showToast(error.message || 'Could not apply suggestion', 'error');
    } finally {
      busy = false;
      _lbSetEditBusy(false);
      if (data?.photo_id === _lightboxCurrentId) render();
    }
  }
  analyzeButton.onclick = async () => {
    if (!data || busy || data.photo_id !== _lightboxCurrentId) return;
    // Analyze subjects enqueues a job that writes detection_subjects and
    // can flip the primary via sync_primary (clearing mask, embedding
    // and eye state), so it must be blocked in read-only lightboxes
    // (Codex r4056621185).
    if (typeof _lbGuardReadOnly === 'function' && _lbGuardReadOnly()) return;
    const photoId = data.photo_id;
    const seq = generation;
    busy = true; render();
    status.textContent = 'Analyzing retained subjects…';
    try {
      const job = await safeFetch('/api/photos/' + photoId + '/subjects/analyze', {method: 'POST'}, {toast: false});
      while (generation === seq && _lightboxCurrentId === photoId) {
        const progress = await safeFetch('/api/jobs/' + job.job_id, {}, {toast: false});
        if (progress.status === 'completed') { await load(photoId); break; }
        if (progress.status === 'failed' || progress.status === 'cancelled') throw new Error(progress.error || 'Subject analysis did not complete');
        await new Promise(resolve => setTimeout(resolve, 500));
      }
    } catch (error) {
      if (seq !== generation || _lightboxCurrentId !== photoId) return;
      showToast(error.message || 'Could not analyze subjects', 'error');
    } finally {
      busy = false;
      if (data?.photo_id === photoId) render();
    }
  };
  automatic.onclick = () => choose(null);
  corrected.onchange = render;
  cropButton.onclick = () => useSuggestion('crop');
  exposureButton.onclick = () => useSuggestion('exposure');
  document.addEventListener('lightbox:subjectreset', () => { generation++; data = null; panel.hidden = true; });
  document.addEventListener('lightbox:photochanged', event => load(event.detail.photoId));
})();
