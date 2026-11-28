import {button, caption, contactSheet, matchControls} from './review.js';
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
let pendingLogin = '';

/** @param {string} id */
function select(id) { return /** @type {HTMLSelectElement} */ (document.getElementById(id)); }
/** @param {string} id */
function date(id) { return /** @type {HTMLInputElement} */ (document.getElementById(id)); }
const folder = select('folder');
const location = select('location');
const view = select('view');
const start = date('date-from');
const end = date('date-to');
/** @type {Set<number>} */ let nearby = new Set();

function tripParams() {
  const params = new URLSearchParams({trip: '1'});
  for (const [key, value] of Object.entries({folder_id: folder.value, location_id: location.value,
    date_from: start.value, date_to: end.value})) if (value) params.set(key, value);
  return params;
}

/** @param {string} message */
function report(message) { status.textContent = message; }

async function tripOptions() {
  /** @type {[{id: number, path: string}[], {id: number, name: string, located: boolean}[]]} */
  const [folders, places] = await Promise.all([safeFetch('/api/folders'), safeFetch('/api/inat/locations')]);
  for (const f of folders) folder.add(new Option(f.path, String(f.id)));
  for (const p of places) {
    const option = new Option(p.name, String(p.id));
    option.dataset.located = String(p.located);
    location.add(option);
  }
}

function readSnapshot() {
  try {
    const stored = JSON.parse(localStorage.getItem(storageKey) || 'null');
    if (stored?.login && Array.isArray(stored.taxon_ids)) return stored;
  } catch { /* A refresh restores an unavailable cache. */ }
}

function saveSnapshot() {
  try { localStorage.setItem(storageKey, JSON.stringify(snapshot)); } catch { /* Session still works. */ }
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
  const scope = tripParams();
  /** @type {Promise<void> | undefined} */ let fetching;
  async function fetchPage() {
    const params = new URLSearchParams(scope);
    for (const [k, v] of Object.entries({species: entry.species, taxon_id: entry.taxon_id ?? 'none',
      offset: photos.length, limit: 24})) params.set(k, String(v));
    /** @type {{photos: CandidatePhoto[], photo_count: number}} */
    const page = await safeFetch('/api/life-list/species?' + params);
    photos.push(...page.photos);
    entry.photo_count = page.photo_count;
  }
  function more() {
    fetching ??= fetchPage().finally(() => { fetching = undefined; });
    return fetching;
  }
  const next = button('Next', async () => {
    previous.disabled = next.disabled = true;
    try {
      if (index + 1 === photos.length) await more();
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
    filename.textContent = caption(photo);
    position.textContent = (index + 1) + ' / ' + entry.photo_count;
    previous.disabled = index === 0;
    next.disabled = index + 1 >= entry.photo_count;
  }
  update();
  if (entry.photo_count > 1) controls.append(previous, position, next);
  row.append(preview, info, controls);
  if (view.value === 'unmatched') {
    details.textContent = entry.taxon_rank ? 'Matched rank: ' + entry.taxon_rank + ' · No linked iNaturalist species'
      : 'No linked taxon';
    row.append(matchControls(entry.species, () => photos[index] || entry.best, () => load(false), report));
  } else {
    /** @param {number[]} ids */
    const send = ids => submitToInat(ids[0] ?? entry.best.id, {
      login, name: entry.scientific_name, id: entry.inat_id, additionalPhotoIds: ids.slice(1),
    });
    row.append(button('Send to iNaturalist', () => send([(photos[index] || entry.best).id])));
    if (entry.photo_count > 1) row.append(contactSheet(photos, more,
      () => photos.length < entry.photo_count, send, report));
  }
  return row;
}

function render() {
  if (!snapshot || !entries) return;
  const covered = new Set(snapshot.taxon_ids);
  const comparable = entries.filter(entry => entry.taxon_rank === 'species' && entry.inat_id);
  const unmatched = entries.filter(entry => !entry.inat_id || entry.taxon_rank !== 'species');
  let visible;
  let description;
  switch (view.value) {
    case 'observed':
      visible = comparable.filter(entry => covered.has(entry.inat_id));
      description = ' of ' + comparable.length + ' linked species already observed.';
      break;
    case 'places':
      visible = comparable.filter(entry => covered.has(entry.inat_id) && !nearby.has(entry.inat_id));
      description = ' species already posted elsewhere with no public record within 10 km of this location.';
      break;
    case 'unmatched':
      visible = unmatched;
      description = ' labels to review.';
      break;
    default:
      visible = comparable.filter(entry => !covered.has(entry.inat_id));
      description = ' of ' + comparable.length + ' linked species missing.';
  }
  visible.sort((a, b) => (a.common_name || a.species).localeCompare(b.common_name || b.species));
  summary.textContent = snapshot.login + ' · Last refreshed ' +
    new Date(snapshot.refreshed_at).toLocaleString() + ' · ' + visible.length + description;
  if (view.value !== 'unmatched' && unmatched.length) summary.append(' ', button('Review ' + unmatched.length + ' unmatched labels', () => {
    view.value = 'unmatched';
    load(false);
  }));
  const login = snapshot.login;
  queue.replaceChildren(...visible.map(entry => candidate(entry, login)));
}

/** @param {boolean} refresh */
async function load(refresh, preserveReceipt = false) {
  const current = ++generation;
  const login = input.value.trim();
  queue.replaceChildren();
  summary.replaceChildren();
  entries = undefined;
  pendingLogin = '';
  const regional = view.value === 'places';
  if (regional && location.selectedOptions[0]?.dataset.located !== 'true') {
    report('Select a location with coordinates to compare public observations within 10 km.');
    return;
  }
  if (start.value && end.value && start.value > end.value) { report('The end date precedes the start date.'); return; }
  refresh ||= snapshot?.login !== login;
  pendingLogin = login.toLowerCase();
  status.textContent = refresh ? 'Checking observations…' : 'Loading Life List…';
  try {
    const [life, account, local] = await Promise.all([
      /** @type {Promise<{species: QueueEntry[]}>} */ (safeFetch('/api/life-list?photos_per_species=1&' + tripParams())),
      refresh ? /** @type {Promise<Observer>} */ (safeFetch('/api/inat/account-species?' + new URLSearchParams({login}))) : snapshot,
      regional ? /** @type {Promise<Observer>} */ (safeFetch('/api/inat/account-species?' + new URLSearchParams({login, location_id: location.value}))) : undefined,
    ]);
    if (current !== generation || !account) return;
    nearby = new Set(local?.taxon_ids);
    entries = life.species;
    snapshot = account;
    saveSnapshot();
    input.value = snapshot.login;
    status.textContent = '';
    if (!preserveReceipt) receipt.replaceChildren();
    render();
  } catch (error) {
    if (current === generation) status.textContent = error instanceof Error ? error.message : String(error);
  } finally {
    if (current === generation) pendingLogin = '';
  }
}

document.addEventListener('inat:species-submitted', event => {
  const {detail} = /** @type {CustomEvent<{login: string, id: number, url: string}>} */ (event);
  if (!snapshot || detail.login !== snapshot.login) return;
  const reload = pendingLogin === detail.login.toLowerCase() || (!pendingLogin && view.value === 'places');
  snapshot.taxon_ids.push(detail.id);
  saveSnapshot();
  const link = document.createElement('a');
  link.href = detail.url;
  link.target = '_blank';
  link.dataset.externalLink = '';
  link.textContent = 'View observation on iNaturalist';
  receipt.replaceChildren(link);
  link.addEventListener('click', event => openExternalLink(event, link.href));
  if (reload) load(false, true);
  else render();
});

tripOptions().catch(e => report(String(e)));
for (const field of [folder, location, start, end, view]) field.addEventListener('change', () => {
  if (input.value.trim()) load(false);
});
form.addEventListener('submit', event => { event.preventDefault(); load(true); });
snapshot = readSnapshot();
if (snapshot) {
  input.value = snapshot.login;
  load(false);
}
