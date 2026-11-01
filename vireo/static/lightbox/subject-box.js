/* Draw in upright source coordinates, independent of the saved edit recipe. */
import { safeFetch } from '../navbar-shared-helpers.js';

/** @param {number} photoId @param {(data: NavSubjectData) => void} saved */
export function openSubjectBox(photoId, saved) {
  const dialog = document.createElement('dialog');
  dialog.className = 'lb-subject-box-dialog';
  dialog.setAttribute('aria-label', 'Select animal region');
  dialog.innerHTML = '<h2>Select animal region</h2><p>Drag a box around the visible animal on the unedited image. '
    + 'This records your region, without assigning a species.</p><canvas aria-label="Drag to select animal region"></canvas>'
    + '<p role="status">Loading source image…</p><button type="button" data-save disabled>Save subject box</button> '
    + '<button type="button" data-cancel>Cancel</button>';
  const canvas = /** @type {HTMLCanvasElement} */ (dialog.querySelector('canvas'));
  const context = canvas.getContext('2d');
  const status = /** @type {HTMLElement} */ (dialog.querySelector('[role="status"]'));
  const save = /** @type {HTMLButtonElement} */ (dialog.querySelector('[data-save]'));
  const cancel = /** @type {HTMLButtonElement} */ (dialog.querySelector('[data-cancel]'));
  const image = new Image();
  /** @type {NavBox | null} */ let box = null;
  /** @type {{x: number; y: number} | null} */ let start = null;
  let saving = false;
  function close() { dialog.close(); dialog.remove(); }
  function reset() { close(); }
  dialog.addEventListener('close', () => document.removeEventListener('lightbox:subjectreset', reset));
  document.addEventListener('lightbox:subjectreset', reset);
  cancel.addEventListener('click', close);
  dialog.addEventListener('cancel', event => { event.preventDefault(); close(); });
  image.onload = () => {
    if (!dialog.isConnected || !context) return;
    canvas.width = image.naturalWidth; canvas.height = image.naturalHeight;
    context.drawImage(image, 0, 0);
    status.textContent = 'Drag around the animal. Measurements will apply only to this box.';
  };
  image.onerror = () => { status.textContent = 'Source image unavailable. No region has been saved.'; };
  /** @param {PointerEvent} event */
  function point(event) {
    const rect = canvas.getBoundingClientRect();
    return {x: Math.max(0, Math.min(1, (event.clientX - rect.left) / rect.width)),
      y: Math.max(0, Math.min(1, (event.clientY - rect.top) / rect.height))};
  }
  canvas.addEventListener('pointerdown', event => {
    if (saving || !image.complete || !image.naturalWidth) return;
    event.preventDefault(); start = point(event); box = null; save.disabled = true;
    canvas.setPointerCapture(event.pointerId);
  });
  canvas.addEventListener('pointermove', event => {
    if (!start || !context) return;
    const end = point(event);
    box = {x: Math.min(start.x, end.x), y: Math.min(start.y, end.y),
      w: Math.abs(end.x - start.x), h: Math.abs(end.y - start.y)};
    context.drawImage(image, 0, 0);
    context.strokeStyle = '#ffdf40'; context.lineWidth = Math.max(2, canvas.width / 500);
    context.strokeRect(box.x * canvas.width, box.y * canvas.height, box.w * canvas.width, box.h * canvas.height);
  });
  canvas.addEventListener('pointerup', () => {
    start = null;
    save.disabled = !box || box.w * canvas.width < 2 || box.h * canvas.height < 2;
  });
  canvas.addEventListener('pointercancel', () => { start = null; box = null; save.disabled = true; });
  save.addEventListener('click', async () => {
    if (!box || saving) return;
    saving = true; save.disabled = true;
    status.textContent = 'Saving selected subject…';
    try {
      const result = /** @type {NavSubjectData} */ (await safeFetch('/api/photos/' + photoId + '/subjects/reviewer-box', {
        method: 'PUT', headers: {'Content-Type': 'application/json'}, body: JSON.stringify({box})
      }, {toast: false}));
      if (dialog.isConnected) { saved(result); close(); }
    } catch (error) {
      status.textContent = error instanceof Error ? error.message : String(error);
    } finally { saving = false; save.disabled = false; }
  });
  document.body.append(dialog); dialog.showModal();
  image.src = '/photos/' + photoId + '/preview?view=source&size=1920';
}
