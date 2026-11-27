// Each inspection owns its requests and object URLs until the pair changes or the dialog closes.
const cullInspection = /** @type {HTMLDialogElement} */ (document.getElementById('cullInspection'));
const cullPair = /** @type {HTMLElement} */ (document.getElementById('cullInspectPair'));
const cullFlip = /** @type {HTMLButtonElement} */ (document.getElementById('cullBlink'));
const cullFlipView = /** @type {HTMLElement} */ (document.getElementById('cullBlinkView'));
/** @type {(() => void)[]} */
let cullCropCleanups = [];
let cullFlipIndex = 0;
/** @type {number | null} */
let cullInspectEsc = null;

function clearCullInspectionPair() {
  cullCropCleanups.splice(0).forEach(dispose => dispose());
  cullPair.replaceChildren();
  resetCullFlip();
}

function resetCullFlip() {
  cullFlipIndex = 0;
  cullFlip.disabled = true;
  cullFlipView.hidden = true;
  cullFlipView.replaceChildren();
}

function flipCullCrops() {
  const pane = cullPair.children[cullFlipIndex];
  const crop = pane?.querySelector('.inspect-pixels');
  if (cullFlip.disabled || !pane || !crop) return;
  cullFlipView.replaceChildren(pane.querySelector('h3')?.textContent || '', crop.cloneNode(true));
  cullFlipView.hidden = false;
  cullFlipIndex = 1 - cullFlipIndex;
}

cullFlip.addEventListener('click', flipCullCrops);
cullInspection.addEventListener('keydown', e => {
  e.stopPropagation();
  if (e.key.toLowerCase() === 'f' && !e.ctrlKey && !e.metaKey && !e.altKey) {
    e.preventDefault();
    flipCullCrops();
  }
});
cullInspection.addEventListener('close', () => {
  clearCullInspectionPair();
  if (cullInspectEsc !== null) window.Keymap?.popEsc(cullInspectEsc);
  cullInspectEsc = null;
});

/** @param {CullSourcePhoto} photo @param {string} label */
function addCullInspectionPane(photo, label) {
  const pane = document.createElement('article');
  pane.innerHTML = '<h3></h3><div class="inspect-frame"><img alt=""><i hidden></i><b class="inspect-eye" hidden title="Measured eye region"></b></div>' +
    '<div class="inspect-pixels"><img alt="Focus crop" hidden></div><p aria-live="polite"></p><small></small>';
  cullPair.append(pane);
  const images = pane.getElementsByTagName('img');
  const full = /** @type {HTMLImageElement} */ (images[0]);
  const crop = /** @type {HTMLImageElement} */ (images[1]);
  const box = /** @type {HTMLElement} */ (pane.querySelector('i'));
  const eyeBox = /** @type {HTMLElement} */ (pane.querySelector('.inspect-eye'));
  const status = /** @type {HTMLElement} */ (pane.querySelector('p'));
  const title = /** @type {HTMLElement} */ (pane.querySelector('h3'));
  title.textContent = label + ' · ' + photo.filename;
  full.addEventListener('error', () => {
    full.alt = 'Full frame unavailable for ' + photo.filename;
    box.hidden = eyeBox.hidden = true;
  });
  full.alt = 'Upright full frame of ' + photo.filename;
  full.src = '/photos/' + photo.id + '/preview?size=1920&view=source';
  const detail = photo.comparable_quality;
  const metric = /** @type {HTMLElement} */ (pane.querySelector('small'));
  const wholeDetail = detail ? 'Whole-animal detail at ' + detail.scale_px + ' px: ' + Math.round(detail.detail).toLocaleString()
    : 'Whole-animal detail unavailable';
  let request = new AbortController();
  let objectUrl = '';
  function release() {
    request.abort();
    if (objectUrl) URL.revokeObjectURL(objectUrl);
    objectUrl = '';
  }
  cullCropCleanups.push(release);

  /** @param {string} query */
  async function load(query) {
    release();
    request = new AbortController();
    const signal = request.signal;
    crop.hidden = box.hidden = eyeBox.hidden = true;
    crop.removeAttribute('src');
    resetCullFlip();
    metric.textContent = wholeDetail;
    status.textContent = 'Loading focus crop…';
    try {
      const result = await fetch('/photos/' + photo.id + '/focus-crop?size=256&' + query, {signal});
      if (!result.ok) throw new Error('Crop unavailable. Click the full frame to choose a location.');
      const blob = await result.blob();
      if (signal.aborted) return;
      objectUrl = URL.createObjectURL(blob);
      crop.src = objectUrl;
      crop.hidden = false;
      positionCullCrop(result.headers, box, crop);
      const region = result.headers.get('X-Vireo-Focus-Eye-Box');
      if (region) {
        const [x, y, w, h] = region.split(',').map(Number);
        if (x != null && y != null && w && h) {
          eyeBox.style.left = x * 100 + '%';
          eyeBox.style.top = y * 100 + '%';
          eyeBox.style.width = w * 100 + '%';
          eyeBox.style.height = h * 100 + '%';
          eyeBox.hidden = false;
        }
      }
      status.textContent = cullCropCaption(result.headers);
      const eye = result.headers.get('X-Vireo-Focus-Eye-Detail');
      if (eye !== null) metric.append(' · eye detail ' + Math.round(Number(eye)).toLocaleString());
      cullFlip.disabled = cullPair.querySelectorAll('.inspect-pixels img:not([hidden])').length !== 2;
    } catch (error) {
      if (!signal.aborted) status.textContent = error instanceof Error ? error.message : 'Crop unavailable.';
    }
  }
  full.addEventListener('click', e => {
    if (!full.naturalWidth) return;
    const rect = full.getBoundingClientRect();
    const x = (e.clientX - rect.left) / rect.width;
    const y = (e.clientY - rect.top) / rect.height;
    load('center=' + x.toFixed(6) + ',' + y.toFixed(6));
  });
  load('basis=head&anchor=eye');
}

/** Draw the actual source window. Offset the native pixels so the chosen point stays put even at frame edges.
 * @param {Headers} headers @param {HTMLElement} box @param {HTMLImageElement} crop */
function positionCullCrop(headers, box, crop) {
  const numbers = (/** @type {string} */ key) => (headers.get('X-Vireo-Focus-' + key) || '').split(',').map(Number);
  const [w, h] = numbers('Raster-Size');
  const [x, y, right, bottom] = numbers('Window');
  const [cx, cy] = numbers('Center');
  if (!w || !h || x == null || y == null || right == null || bottom == null || cx == null || cy == null) return;
  box.style.left = 100 * x / w + '%';
  box.style.top = 100 * y / h + '%';
  box.style.width = 100 * (right - x) / w + '%';
  box.style.height = 100 * (bottom - y) / h + '%';
  box.hidden = false;
  crop.style.transform = 'translate(' + (x - Math.round(cx * w)) + 'px,' + (y - Math.round(cy * h)) + 'px)';
}

/** @param {Headers} h */
function cullCropCaption(h) {
  /** @type {Record<string,string>} */
  const names = {eye:'Detected eye', head:'Estimated head', subject:'Subject center', frame:'Frame center', point:'Manual location'};
  const basis = h.get('X-Vireo-Focus-Basis') || '';
  const parts = [names[basis] || 'Focus crop'];
  const scale = h.get('X-Vireo-Focus-Original-Scale');
  if (!scale || scale === 'unknown') parts.push('original scale unknown');
  else if (Number(scale) < 0.98) parts.push(Math.round(Number(scale) * 100) + '% of original scale');
  const [x, y, w, height] = (h.get('X-Vireo-Focus-Subject-Box') || '').split(',').map(Number);
  if (x != null && y != null && w && height) {
    const edges = [x <= .01 && 'left', y <= .01 && 'top', x + w >= .99 && 'right', y + height >= .99 && 'bottom'].filter(Boolean);
    if (edges.length) parts.push('check ' + edges.join(' and ') + ' frame edge');
  }
  return parts.join(' · ');
}

/** @param {number} id @param {number|null} [reference] */
function openCullInspection(id, reference) {
  const data = pipelineResults || cullSourceResults;
  if (!data) return;
  const photos = new Map(data.photos.map(p => [p.id, p]));
  const current = photos.get(id);
  if (!current) return;
  const encounter = data.encounters.find(e => e.photo_ids.includes(id));
  const burst = encounter?.bursts?.map(b => Array.isArray(b) ? b : b.photo_ids).find(ids => ids.includes(id));
  const frames = (burst || [id]).flatMap(pid => photos.get(pid) || []);
  const film = /** @type {HTMLElement} */ (document.getElementById('cullFilm'));
  film.replaceChildren();
  /** @param {CullSourcePhoto} selected @param {CullSourcePhoto} [ref] */
  function display(selected, ref) {
    clearCullInspectionPair();
    const comparison = document.getElementById('cullDetailComparison');
    if (comparison) comparison.textContent = cullDetailComparison(ref?.comparable_quality, selected.comparable_quality);
    if (ref) addCullInspectionPane(ref, 'A');
    addCullInspectionPane(selected, ref ? 'B' : 'A');
    for (const button of film.querySelectorAll('button')) button.setAttribute('aria-pressed', String(button.value === String(selected.id)));
  }
  /** @param {CullSourcePhoto} frame */
  function neighbor(frame) {
    const i = frames.indexOf(frame);
    return frames[i - 1] || frames[i + 1];
  }
  for (const frame of frames) {
    const button = document.createElement('button');
    const thumb = document.createElement('img');
    thumb.src = '/thumbnails/' + frame.id + '.jpg';
    thumb.loading = 'lazy';
    thumb.alt = '';
    button.value = String(frame.id);
    button.append(thumb, frame.filename);
    button.addEventListener('click', () => display(frame, neighbor(frame)));
    film.append(button);
  }
  if (!cullInspection.open) {
    cullInspection.showModal();
    cullInspectEsc = window.Keymap?.pushEsc(() => cullInspection.close()) ?? null;
  }
  const alternatives = frames.filter(frame => frame.id !== id && frame.comparable_quality);
  alternatives.sort((a, b) => (b.comparable_quality?.detail ?? 0) - (a.comparable_quality?.detail ?? 0));
  display(current, (reference && photos.get(reference)) || alternatives[0] || neighbor(current));
}

/** @param {CullComparableQuality|null|undefined} a @param {CullComparableQuality|null|undefined} b */
function cullDetailComparison(a, b) {
  if (!a || !b || a.scale_px !== b.scale_px) return '';
  const high = Math.max(a.detail, b.detail);
  if (Math.abs(a.detail - b.detail) <= high * 0.1) {
    return 'Whole-animal measurements are within 10% of each other. Compare eye detail, wing position and behavior.';
  }
  return (a.detail > b.detail ? 'A' : 'B') + ' has higher measured whole-animal detail. Check the intended animal and the moment in both frames.';
}
