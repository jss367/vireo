// The crop preserves decoded raster pixels; no analysis or subject choices are written.
function rqClearCrop() {
  ++reviewQueue.cropSequence;
  var image = /** @type {HTMLImageElement} */ (rqElement('rqCrop'));
  var old = image.dataset.objectUrl;
  image.hidden = true;
  image.removeAttribute('src');
  delete image.dataset.objectUrl;
  if (old) URL.revokeObjectURL(old);
  rqElement('rqCenter').hidden = true;
}

/** @param {[number,number]|null} center */
function rqMarkCenter(center) {
  var marker = rqElement('rqCenter');
  marker.hidden = !center;
  if (center) { marker.style.left = center[0] * 100 + '%'; marker.style.top = center[1] * 100 + '%'; }
}

function rqSubjectCenter() {
  if (!reviewQueue.current) return null;
  var item = rqItem(reviewQueue.current.id);
  if (item.center) return item.center;
  var subject = reviewQueue.subjects.find(function(s) { return s.id === item.subject; });
  if (!subject || ![subject.box_x, subject.box_y, subject.box_w, subject.box_h].every(Number.isFinite) ||
      subject.box_x < 0 || subject.box_y < 0 || subject.box_w <= 0 || subject.box_h <= 0 ||
      subject.box_x + subject.box_w > 1.000001 || subject.box_y + subject.box_h > 1.000001) return null;
  var center = /** @type {[number,number]} */ ([subject.box_x + subject.box_w / 2, subject.box_y + subject.box_h / 2]);
  return center.every(function(v) { return Number.isFinite(v) && v >= 0 && v <= 1; }) ? center : null;
}

async function rqLoadCrop() {
  rqClearCrop();
  if (!reviewQueue.current) return;
  var center = rqSubjectCenter();
  if (!center) {
    rqElement('rqCropStatus').textContent = 'No usable subject location. Choose another subject or place a manual center in the full frame.';
    return;
  }
  var sequence = reviewQueue.cropSequence;
  var photoId = reviewQueue.current.id;
  var url = '/photos/' + photoId + '/focus-crop?size=512&source=' + reviewQueue.source + '&center=' + center.join(',');
  rqElement('rqCropStatus').textContent = 'Loading native focus pixels…';
  rqMarkCenter(center);
  try {
    var response = await fetch(url, {cache: 'no-store'});
    if (!response.ok) {
      var message = 'Focus crop unavailable (HTTP ' + response.status + '). Try a different source or manual center.';
      if ((response.headers.get('Content-Type') || '').includes('application/json')) {
        var data = await response.json().catch(function() { return {}; });
        if (typeof data.error === 'string') message = data.error;
      }
      throw new Error(message);
    }
    var blob = await response.blob();
    if (sequence !== reviewQueue.cropSequence) return;
    var image = /** @type {HTMLImageElement} */ (rqElement('rqCrop'));
    image.dataset.objectUrl = URL.createObjectURL(blob);
    image.src = image.dataset.objectUrl;
    image.hidden = false;
    var h = response.headers;
    var scale = h.get('X-Vireo-Focus-Original-Scale') || 'unknown';
    rqElement('rqCropStatus').textContent = 'Source: ' + (h.get('X-Vireo-Focus-Source') || 'unknown') +
      ' · decoded raster: ' + (h.get('X-Vireo-Focus-Raster-Size') || 'unknown') + ' px · original pixel scale: ' + scale +
      '. One crop pixel per decoded raster pixel; displayed at native size (scroll for more).' +
      (scale !== 'unknown' && Number(scale) < 0.98 ? ' This raster is smaller than the original; it is not a full-resolution RAW inspection.' : '') +
      ' Visual crop only; no new analysis was run.';
  } catch (error) {
    if (sequence !== reviewQueue.cropSequence) return;
    rqElement('rqCropStatus').textContent = reviewErrorMessage(error, 'Focus crop unavailable. Try again.');
  }
}

function rqRenderSubjects() {
  var current = reviewQueue.current;
  if (!current) return;
  var item = rqItem(current.id);
  var select = /** @type {HTMLSelectElement} */ (rqElement('rqSubject'));
  select.replaceChildren(new Option('Choose subject / manual center', ''));
  reviewQueue.subjects.forEach(function(subject, index) {
    select.add(new Option('Subject ' + (index + 1) + ' · left ' + Math.round(subject.box_x * 100) + '% · top ' +
      Math.round(subject.box_y * 100) + '%' + (subject.is_primary ? ' · saved primary' : ''), String(subject.id)));
  });
  select.value = String(item.subject || '');
  var chosen = reviewQueue.subjects.find(function(s) { return s.id === item.subject; });
  rqElement('rqAnalysis').textContent = (chosen?.analysis ? 'Cached box analysis is recorded for this subject; freshness is not checked here.' :
    'No cached box analysis is recorded for this subject.') +
    ' Subject choice and manual centering only change this inspection. They do not change the saved primary, rankings, or analysis.' +
    (item.center ? ' Using the saved manual center.' : ' Focus window centered on the selected subject.');
  /** @type {HTMLSelectElement} */ (rqElement('rqSource')).value = reviewQueue.source;
}

/** @param {MouseEvent} event */
function rqPlaceCenter(event) {
  if (!reviewQueue.manual || !reviewQueue.current) return;
  var image = /** @type {HTMLImageElement} */ (rqElement('rqFull'));
  if (!image.complete || !image.naturalWidth) return;
  var rect = image.getBoundingClientRect();
  rqItem(reviewQueue.current.id).center = [Math.max(0, Math.min(1, (event.clientX - rect.left) / rect.width)),
    Math.max(0, Math.min(1, (event.clientY - rect.top) / rect.height))];
  rqSave(); rqRenderSubjects(); rqLoadCrop();
}
