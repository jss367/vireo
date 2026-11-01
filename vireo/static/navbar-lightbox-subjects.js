/* Per-detection suggestions are previews. Only explicit Use actions edit a photo. */
import { openSubjectBox } from './lightbox/subject-box.js';
import { lbState } from './lightbox/shared-state.js';
import { _lbGuardReadOnly, _lbPhotoDataByPhoto, vireoLightboxSession } from './lightbox/state.js';
import {
  _lbEditWritePending, _lbFlushPendingAdjustmentSave, _lbMarkEditRecipeWrite,
  _lbReloadCurrentRenderAfterEdit, _lbRememberEditRecipe, _lbSetEditBusy,
  _lbTransformBoxByRecipe, _lbWaitForAdjustmentSaveIdle, _vireoBumpRenderVersion,
  vireoRefreshPhotoRenders,
} from './lightbox/edits.js';
import {
  _lbLoadDetections, _lbLoadMaskVariants, _lbRenderEyeCrosshair, _lbResetMaskOverlay,
} from './lightbox/overlays.js';
import { safeFetch, showToast } from './navbar-shared-helpers.js';

(function () {
  'use strict';
  /** @type {NavSubjectData | null} */ let data = null;
  let generation = 0;
  let busy = false;
  const panel = /** @type {HTMLElement} */ (document.getElementById('lightboxSubjects'));
  if (!panel) return;
  const list = /** @type {HTMLElement} */ (document.getElementById('lightboxSubjectList'));
  const rejectedList = /** @type {HTMLElement} */ (document.getElementById('lightboxSubjectRejected'));
  const details = /** @type {HTMLElement} */ (document.getElementById('lightboxSubjectDetails'));
  const status = /** @type {HTMLElement} */ (document.getElementById('lightboxSubjectStatus'));
  const corrected = /** @type {HTMLInputElement} */ (document.getElementById('lightboxSubjectCorrected'));
  const automatic = /** @type {HTMLButtonElement} */ (document.getElementById('lightboxSubjectAutomatic'));
  const cropButton = /** @type {HTMLButtonElement} */ (document.getElementById('lightboxSubjectUseCrop'));
  const analyzeButton = /** @type {HTMLButtonElement} */ (document.getElementById('lightboxSubjectAnalyze'));
  const drawButton = /** @type {HTMLButtonElement} */ (document.getElementById('lightboxSubjectDraw'));
  const retryButton = /** @type {HTMLButtonElement} */ (document.getElementById('lightboxSubjectRetry'));
  const exposureButton = /** @type {HTMLButtonElement} */ (document.getElementById('lightboxSubjectUseExposure'));

  function primary() { return data && data.subjects.find(s => s.is_primary); }
  /** @param {NavSubject} subject @param {boolean} correction */
  function previewUrl(subject, correction) {
    return '/photos/' + data?.photo_id + '/crop?detection_id=' + subject.id
      + (correction ? '&suggested=1' : '') + '&v=' + encodeURIComponent(subject.analysis?.source_key || 'pending');
  }
  /** @param {NavSubject} subject @param {number} index @param {boolean} readOnly @param {string | null} readOnlyTitle */
  function subjectCard(subject, index, readOnly, readOnlyTitle) {
    const item = document.createElement('div');
    item.className = 'lb-subject-item';
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
    // Rejecting says the box holds no animal: it leaves review, species
    // evidence and grouping everywhere, and the next box becomes primary.
    const reject = document.createElement('button');
    reject.type = 'button';
    reject.className = 'lb-subject-reject';
    reject.textContent = 'Not an animal';
    reject.title = readOnlyTitle || 'Reject this region as background (one Undo step)';
    reject.disabled = busy || readOnly;
    reject.onclick = () => setVerdict(subject.id, 'not_animal');
    item.append(button, reject);
    return item;
  }
  /** @param {NavSubject} region @param {number} index @param {boolean} readOnly @param {string | null} readOnlyTitle */
  function rejectedCard(region, index, readOnly, readOnlyTitle) {
    const item = document.createElement('div');
    item.className = 'lb-subject-item';
    const img = document.createElement('img');
    img.src = previewUrl(region, false);
    img.alt = 'Rejected region ' + (index + 1);
    const caption = document.createElement('span');
    caption.textContent = region.predictions[0]?.species ? 'was ' + region.predictions[0].species : 'Rejected region ' + (index + 1);
    const restore = document.createElement('button');
    restore.type = 'button';
    restore.className = 'lb-subject-restore';
    restore.textContent = 'Restore';
    restore.title = readOnlyTitle || 'Retain this region as a subject again';
    restore.disabled = busy || readOnly;
    restore.onclick = () => setVerdict(region.id, null);
    item.append(img, caption, restore);
    return item;
  }
  /** Disable every primary-subject control while the photo has no retained subject. */
  function renderWithoutPrimary() {
    status.textContent = data?.rejected.length ? 'Every detected region was rejected as not an animal. Restore one to review it again.' : '';
    analyzeButton.hidden = true;
    automatic.disabled = true;
    corrected.disabled = true;
    cropButton.disabled = true;
    exposureButton.disabled = true;
  }
  function render() {
    list.replaceChildren();
    details.replaceChildren();
    rejectedList.replaceChildren();
    panel.hidden = !data;
    if (panel.hidden || !data) return;
    // Scoped Pipeline Review views open the lightbox with _lbReadOnly to
    // freeze mutating writes. Every subject control here reaches a
    // PUT/POST — subject cards and "Choose automatically" invoke
    // sync_primary which can clear mask/embedding/eye state, "Analyze
    // subjects" enqueues a job that writes detection_subjects and can
    // change the primary, and Use crop / Use exposure PUT the edit
    // recipe — so they all must honor _lbReadOnly (Codex r4056621185).
    const readOnly = typeof lbState.readOnly !== 'undefined' && lbState.readOnly;
    const readOnlyTitle = readOnly && typeof lbState.readOnlyMessage === 'string' ? lbState.readOnlyMessage : null;
    /** @type {HTMLElement} */ (document.getElementById('lightboxSubjectSummary')).textContent = 'Subjects (' + data.subjects.length + ')'
      + (data.rejected.length ? ' · ' + data.rejected.length + ' rejected' : '');
    drawButton.disabled = busy || readOnly;
    retryButton.disabled = busy || readOnly || !primary();
    if (data.rejected.length) {
      const heading = document.createElement('p');
      heading.className = 'lb-subject-rejected-heading';
      heading.textContent = 'Rejected as not an animal (not used as evidence):';
      rejectedList.append(heading);
      data.rejected.forEach((region, index) => rejectedList.append(rejectedCard(region, index, readOnly, readOnlyTitle)));
    }
    if (!data.subjects.length) {
      analyzeButton.hidden = true;
      automatic.disabled = corrected.disabled = cropButton.disabled = exposureButton.disabled = true;
      const names = (data.full_image_predictions || []).map(p => p.species).join(', ');
      status.textContent = (data.rejected.length ? 'Every detected region was rejected as not an animal. Restore one to review it again. ' : '')
        + 'No localized subject region. This does not establish animal absence. '
        + (names ? 'Full-image classification only: ' + names + '. ' : '')
        + 'Draw a subject box to measure the visible animal.';
      return;
    }
    // Stable spatial order keeps thumbnails from jumping when primary changes.
    data.subjects.slice().sort((a, b) => a.box_x - b.box_x || a.box_y - b.box_y || a.id - b.id).forEach((subject, index) => {
      list.append(subjectCard(subject, index, readOnly, readOnlyTitle));
    });
    const subject = primary();
    if (!subject) {
      renderWithoutPrimary();
      return;
    }
    const img = document.createElement('img');
    img.className = 'lb-subject-preview';
    img.src = previewUrl(subject, corrected.checked);
    img.alt = 'Suggested crop for the primary subject';
    const description = document.createElement('div');
    const species = document.createElement('strong');
    const prediction = subject.predictions[0];
    species.textContent = (subject.evidence_basis === 'reviewer_box' ? 'Reviewer-selected region · ' : 'Localized detector region · ') + (prediction ? prediction.species + ' · ' + Math.round(prediction.confidence * 100) + '%' : 'Species not predicted');
    const quality = document.createElement('p');
    quality.textContent = subject.analysis ? 'Box quality ' + Math.round(subject.analysis.quality_score * 100) + '/100 · Exposure '
      + (subject.analysis.exposure_ev >= 0 ? '+' : '') + subject.analysis.exposure_ev.toFixed(2) + ' EV' : 'Analyze subjects to compute quality and exposure.';
    quality.title = 'Measured within this box at up to 1024 pixels on the long edge. These are box measurements; mask and eye analysis readiness is separate.';
    const scope = document.createElement('p');
    scope.textContent = 'Box measurements only. Mask and eye analysis readiness is separate.';
    description.append(species, quality, scope);
    details.append(img, description);
    if (data.full_image_predictions?.length) {
      const fallback = document.createElement('p');
      fallback.textContent = 'Full-image classification (not localized to this subject): '
        + data.full_image_predictions.map(p => p.species).join(', ');
      details.append(fallback);
    }
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
  /** @param {number} photoId */
  async function load(photoId) {
    const seq = ++generation;
    data = null;
    panel.hidden = true;
    try {
      /** @type {NavSubjectData} */ const result = await safeFetch('/api/photos/' + photoId + '/subjects', {}, {toast: false});
      if (seq !== generation || vireoLightboxSession.requestedPhotoId() !== photoId) return;
      data = result;
      render();
    } catch (error) {
      if (seq !== generation || vireoLightboxSession.requestedPhotoId() !== photoId) return;
      panel.hidden = false;
      list.replaceChildren(); details.replaceChildren();
      status.textContent = 'Could not load subjects: ' + (error instanceof Error ? error.message : String(error));
    }
  }
  /** @param {number | null} detectionId */
  async function choose(detectionId) {
    if (!data || busy || data.photo_id !== vireoLightboxSession.requestedPhotoId()) return;
    // Choosing a primary — including "Choose automatically" — reaches
    // sync_primary, which clears mask, embedding and eye state; block
    // it in a scoped Pipeline Review lightbox (Codex r4056621185).
    if (typeof _lbGuardReadOnly === 'function' && _lbGuardReadOnly()) return;
    const photoId = data.photo_id;
    const seq = generation;
    busy = true;
    render();
    try {
      /** @type {NavSubjectData} */ const result = await safeFetch('/api/photos/' + photoId + '/primary-subject', {
        method: 'PUT', headers: {'Content-Type': 'application/json'}, body: JSON.stringify({detection_id: detectionId})
      }, {toast: false});
      if (seq !== generation || vireoLightboxSession.requestedPhotoId() !== photoId) return;
      if (data?.primary_detection_id !== result.primary_detection_id) {
        const photo = _lbPhotoDataByPhoto[photoId];
        if (photo) {
          /** @type {const} */ (['eye_x', 'eye_y', 'eye_conf', 'eye_tenengrad']).forEach(key => { photo[key] = null; });
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
      if (seq !== generation || vireoLightboxSession.requestedPhotoId() !== photoId) return;
      showToast((error instanceof Error ? error.message : '') || 'Could not change primary subject', 'error');
    } finally {
      busy = false;
      if (data?.photo_id === vireoLightboxSession.requestedPhotoId()) render();
    }
  }
  /** @param {number} detectionId @param {'not_animal' | null} verdict */
  async function setVerdict(detectionId, verdict) {
    if (!data || busy || data.photo_id !== vireoLightboxSession.requestedPhotoId()) return;
    // A verdict re-projects the primary subject (sync_primary), which clears
    // mask, embedding and eye state, so it is blocked in a read-only lightbox.
    if (typeof _lbGuardReadOnly === 'function' && _lbGuardReadOnly()) return;
    const photoId = data.photo_id;
    const seq = generation;
    busy = true;
    render();
    try {
      /** @type {{ok: boolean; changed: boolean; warning?: string; subjects: NavSubjectData}} */ const result = await safeFetch('/api/detections/' + detectionId + '/subject-verdict', {
        method: 'POST', headers: {'Content-Type': 'application/json'}, body: JSON.stringify({verdict})
      }, {toast: false});
      if (seq !== generation || vireoLightboxSession.requestedPhotoId() !== photoId) return;
      if (data?.primary_detection_id !== result.subjects.primary_detection_id) {
        const photo = _lbPhotoDataByPhoto[photoId];
        if (photo) {
          /** @type {const} */ (['eye_x', 'eye_y', 'eye_conf', 'eye_tenengrad']).forEach(key => { photo[key] = null; });
          _lbRenderEyeCrosshair(photo);
        }
        _lbResetMaskOverlay();
      }
      data = result.subjects;
      _lbLoadDetections(photoId);
      _lbLoadMaskVariants(photoId);
      document.dispatchEvent(new CustomEvent('vireo:primary-subject-changed', {detail: {photoId}}));
      document.dispatchEvent(new CustomEvent('vireo:subject-verdict-changed', {detail: {photoId, detectionId, verdict}}));
      showToast(result.warning || (verdict ? 'Region rejected as not an animal. Undo restores it.' : 'Region restored as a subject.'),
        result.warning ? 'warning' : 'success');
    } catch (error) {
      if (seq !== generation || vireoLightboxSession.requestedPhotoId() !== photoId) return;
      showToast((error instanceof Error ? error.message : '') || 'Could not save the subject verdict', 'error');
    } finally {
      busy = false;
      if (data?.photo_id === vireoLightboxSession.requestedPhotoId()) render();
    }
  }
  /** @param {string} kind */
  async function useSuggestion(kind) {
    const subject = primary();
    if (!subject?.analysis || busy || _lbEditWritePending || !data || data.photo_id !== vireoLightboxSession.requestedPhotoId()) return;
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
      if (vireoLightboxSession.requestedPhotoId() !== photoId || generation !== seq) return;
      // Fetch the latest recipe so another editor's changes aren't replaced
      // by the lightbox's initially loaded snapshot.
      /** @type {{recipe?: NavRecipe | null}} */ const current = await safeFetch('/api/photos/' + photoId + '/edit-recipe', {}, {toast: false});
      if (vireoLightboxSession.requestedPhotoId() !== photoId || generation !== seq) return;
      const recipe = current.recipe || {};
      if (kind === 'crop') {
        if (recipe.straighten) throw new Error('Reset straightening before applying a suggested crop.');
        const orientation = {rotation: recipe.rotation, flip: recipe.flip};
        recipe.crop = _lbTransformBoxByRecipe(subject.analysis.crop, orientation);
      } else {
        recipe.adjustments = Object.assign({}, recipe.adjustments, {exposure: subject.analysis.exposure_ev});
      }
      /** @type {{recipe?: NavRecipe | null}} */ const result = await safeFetch('/api/photos/' + photoId + '/edit-recipe', {
        method: 'PUT', headers: {'Content-Type': 'application/json'},
        body: JSON.stringify({recipe, description: 'Applied subject ' + kind + ' suggestion'})
      }, {toast: false});
      _lbMarkEditRecipeWrite(photoId);
      _lbRememberEditRecipe(photoId, result.recipe);
      _vireoBumpRenderVersion(photoId);
      if (typeof vireoRefreshPhotoRenders === 'function') vireoRefreshPhotoRenders([photoId]);
      if (vireoLightboxSession.requestedPhotoId() === photoId) _lbReloadCurrentRenderAfterEdit(photoId);
    } catch (error) {
      if (seq !== generation || vireoLightboxSession.requestedPhotoId() !== photoId) return;
      showToast((error instanceof Error ? error.message : '') || 'Could not apply suggestion', 'error');
    } finally {
      busy = false;
      _lbSetEditBusy(false);
      if (data?.photo_id === vireoLightboxSession.requestedPhotoId()) render();
    }
  }
  /** @param {number | null} detectionId */
  async function analyze(detectionId = null) {
    if (!data || busy || data.photo_id !== vireoLightboxSession.requestedPhotoId()) return;
    // Analyze subjects enqueues a job that writes detection_subjects and
    // can flip the primary via sync_primary (clearing mask, embedding
    // and eye state), so it must be blocked in read-only lightboxes
    // (Codex r4056621185).
    if (typeof _lbGuardReadOnly === 'function' && _lbGuardReadOnly()) return;
    const photoId = data.photo_id;
    const seq = generation;
    busy = true; render();
    status.textContent = detectionId ? 'Analyzing selected subject box…' : 'Analyzing retained subjects…';
    try {
      /** @type {{job_id: string}} */ const job = await safeFetch('/api/photos/' + photoId + '/subjects/analyze', {method: 'POST', headers: {'Content-Type': 'application/json'}, body: JSON.stringify({detection_id: detectionId})}, {toast: false});
      while (generation === seq && vireoLightboxSession.requestedPhotoId() === photoId) {
        /** @type {{status: string; error?: string}} */ const progress = await safeFetch('/api/jobs/' + job.job_id, {}, {toast: false});
        if (progress.status === 'completed') { await load(photoId); break; }
        if (progress.status === 'failed' || progress.status === 'cancelled') throw new Error(progress.error || 'Subject analysis did not complete');
        await new Promise(resolve => setTimeout(resolve, 500));
      }
    } catch (error) {
      if (seq !== generation || vireoLightboxSession.requestedPhotoId() !== photoId) return;
      showToast((error instanceof Error ? error.message : '') || 'Could not analyze subjects', 'error');
    } finally {
      busy = false;
      if (data?.photo_id === photoId) render();
    }
  };
  analyzeButton.onclick = () => analyze();
  retryButton.onclick = () => { const subject = primary(); if (subject) analyze(subject.id); };
  drawButton.onclick = () => {
    if (!data || busy || _lbGuardReadOnly()) return;
    const photoId = data.photo_id, seq = generation;
    openSubjectBox(photoId, result => {
      if (generation !== seq || vireoLightboxSession.requestedPhotoId() !== photoId) return;
      data = result;
      const photo = _lbPhotoDataByPhoto[photoId];
      if (photo) {
        /** @type {const} */ (['eye_x', 'eye_y', 'eye_conf', 'eye_tenengrad']).forEach(key => { photo[key] = null; });
        _lbRenderEyeCrosshair(photo);
      }
      _lbResetMaskOverlay(); _lbLoadDetections(photoId); _lbLoadMaskVariants(photoId);
      document.dispatchEvent(new CustomEvent('vireo:primary-subject-changed', {detail: {photoId}}));
      render();
    });
  };
  automatic.onclick = () => choose(null);
  corrected.onchange = render;
  cropButton.onclick = () => useSuggestion('crop');
  exposureButton.onclick = () => useSuggestion('exposure');
  document.addEventListener('lightbox:subjectreset', () => { generation++; data = null; panel.hidden = true; });
  document.addEventListener('lightbox:photochanged', event => load(/** @type {CustomEvent<{photoId: number}>} */ (event).detail.photoId));
})();
