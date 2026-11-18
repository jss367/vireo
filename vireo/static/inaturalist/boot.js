import {openExternalLink, openLightbox, safeFetch, submitToInat, thumbnail} from './shared.js';

/** @typedef {{login: string, taxon_ids: number[], refreshed_at: string}} Observer */
/** @typedef {{species: string, scientific_name: string, common_name: string, inat_id: number,
 * taxon_id: number, taxon_rank: string, best: CandidatePhoto, photo_count: number}} QueueEntry */

const form = /** @type {HTMLFormElement} */ (document.querySelector('#account'));
const input = /** @type {HTMLInputElement} */ (document.querySelector('#login'));
const status = /** @type {HTMLElement} */ (document.querySelector('#status'));
const summary = /** @type {HTMLElement} */ (document.querySelector('#summary'));
const queue = /** @type {HTMLElement} */ (document.querySelector('#queue'));
const receipt = /** @type {HTMLElement} */ (document.querySelector('#receipt'));
const storageKey = 'vireo.inaturalist.observer';
/** @type {Observer | undefined} */ let snapshot;
/** @type {QueueEntry[] | undefined} */ let entries;
let generation = 0;

function readSnapshot() {
  try {
    const stored = JSON.parse(localStorage.getItem(storageKey) || 'null');
    if (stored?.login && Array.isArray(stored.taxon_ids)) return stored;
  } catch { /* A refresh restores an unavailable cache. */ }
}

function saveSnapshot() {
  try { localStorage.setItem(storageKey, JSON.stringify(snapshot)); } catch { /* Session still works. */ }
}

/** @param {string} label @param {() => void} action */
function button(label, action) {
  const node = document.createElement('button');
  node.type = 'button';
  node.textContent = label;
  node.addEventListener('click', action);
  return node;
}

/** @param {QueueEntry} entry @param {string} login */
function candidate(entry, login) {
  const row = document.createElement('article');
  row.className = 'candidate';
  const photos = [entry.best];
  let index = 0;
  const image = document.createElement('img');
  image.loading = 'lazy';
  image.alt = '';
  const preview = button('', () => {
    const photo = photos[index];
    if (!photo) return;
    openLightbox(photo.id, photo.filename, photos);
  });
  preview.className = 'preview';
  preview.setAttribute('aria-label', 'Preview ' + entry.species);
  preview.append(image);
  const info = document.createElement('div');
  info.className = 'info';
  const heading = document.createElement('h2');
  heading.textContent = entry.common_name || entry.species;
  const details = document.createElement('div');
  details.className = 'details';
  details.textContent = entry.scientific_name;
  const filename = document.createElement('small');
  info.append(heading, details, filename);
  const controls = document.createElement('div');
  controls.className = 'controls';
  const previous = button('Previous', () => { index--; update(); });
  const next = button('Next', async () => {
    previous.disabled = next.disabled = true;
    try {
      if (index + 1 === photos.length) {
        const params = new URLSearchParams({species: entry.species, taxon_id: String(entry.taxon_id),
          offset: String(photos.length), limit: '24'});
        /** @type {{photos: CandidatePhoto[], photo_count: number}} */ const page = await safeFetch('/api/life-list/species?' + params);
        photos.push(...page.photos);
        entry.photo_count = page.photo_count;
      }
      if (index + 1 < photos.length) index++;
    } catch (error) {
      status.textContent = error instanceof Error ? error.message : String(error);
    } finally { update(); }
  });
  const position = document.createElement('span');
  function update() {
    const photo = photos[index];
    if (!photo) return;
    image.src = thumbnail(photo);
    filename.textContent = photo.filename;
    position.textContent = (index + 1) + ' / ' + entry.photo_count;
    previous.disabled = index === 0;
    next.disabled = index + 1 >= entry.photo_count;
  }
  update();
  if (entry.photo_count > 1) controls.append(previous, position, next);
  const send = button('Send to iNaturalist', () => submitToInat((photos[index] || entry.best).id, {
    login, name: entry.scientific_name, id: entry.inat_id,
  }));
  row.append(preview, info, controls, send);
  return row;
}

function render() {
  if (!snapshot || !entries) return;
  const covered = new Set(snapshot.taxon_ids);
  const comparable = entries.filter(entry => entry.taxon_rank === 'species' && entry.inat_id);
  const missing = comparable.filter(entry => !covered.has(entry.inat_id))
    .sort((a, b) => (a.common_name || a.species).localeCompare(b.common_name || b.species));
  const unchecked = entries.length - comparable.length;
  summary.textContent = snapshot.login + ' · Last refreshed ' +
    new Date(snapshot.refreshed_at).toLocaleString() + ' · ' + missing.length +
    ' of ' + comparable.length + ' linked species missing.' +
    (unchecked ? ' ' + unchecked + ' life list entries lack a species-level match.' : '');
  const login = snapshot.login;
  queue.replaceChildren(...missing.map(entry => candidate(entry, login)));
}

/** @param {boolean} refresh */
async function load(refresh) {
  const current = ++generation;
  const login = input.value.trim();
  status.textContent = refresh ? 'Checking observations…' : 'Loading Life List…';
  try {
    const [life, account] = await Promise.all([
      /** @type {Promise<{species: QueueEntry[]}>} */ (safeFetch('/api/life-list?photos_per_species=1')),
      refresh ? /** @type {Promise<Observer>} */ (safeFetch('/api/inat/account-species?' + new URLSearchParams({login}))) : snapshot,
    ]);
    if (current !== generation || !account) return;
    entries = life.species;
    snapshot = account;
    saveSnapshot();
    input.value = snapshot.login;
    status.textContent = '';
    receipt.replaceChildren();
    render();
  } catch (error) {
    if (current === generation) status.textContent = error instanceof Error ? error.message : String(error);
  }
}

document.addEventListener('inat:species-submitted', event => {
  const {detail} = /** @type {CustomEvent<{login: string, id: number, url: string}>} */ (event);
  if (!snapshot || detail.login !== snapshot.login) return;
  generation++;
  snapshot.taxon_ids.push(detail.id);
  saveSnapshot();
  status.textContent = '';
  const link = document.createElement('a');
  link.href = detail.url;
  link.target = '_blank';
  link.dataset.externalLink = '';
  link.textContent = 'View observation on iNaturalist';
  receipt.replaceChildren(link);
  link.addEventListener('click', event => openExternalLink(event, link.href));
  render();
});

form.addEventListener('submit', event => { event.preventDefault(); load(true); });
snapshot = readSnapshot();
if (snapshot) {
  input.value = snapshot.login;
  load(false);
}
