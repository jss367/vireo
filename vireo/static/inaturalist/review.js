import {safeFetch, showPhotoMenu, thumbnail} from './shared.js';

/** @param {string} label @param {() => void} action */
export function button(label, action) {
  const node = document.createElement('button');
  node.type = 'button';
  node.textContent = label;
  node.addEventListener('click', action);
  return node;
}

/** @param {CandidatePhoto} photo */
export function caption(photo) {
  return [photo.filename, photo.timestamp?.slice(0, 16).replace('T', ' '), photo.location_name].filter(Boolean).join(' · ');
}

/** @param {CandidatePhoto[]} photos @param {() => Promise<void>} more @param {() => boolean} hasMore
 * @param {(ids: number[]) => void} send @param {(message: string) => void} error */
export function contactSheet(photos, more, hasMore, send, error) {
  const panel = document.createElement('details');
  const title = document.createElement('summary');
  title.textContent = 'Select views of the same individual';
  const grid = document.createElement('div');
  grid.className = 'sheet';
  /** @type {Set<number>} */ const selected = new Set();
  const submit = button('Review observation', () => send([...selected]));
  submit.disabled = true;
  const next = button('More photos', async () => {
    next.disabled = true;
    try { await more(); draw(); } catch (e) { error(String(e)); }
    finally { next.disabled = false; }
  });
  function draw() {
    grid.replaceChildren(...photos.map(photo => {
      const label = document.createElement('label');
      const check = document.createElement('input');
      check.type = 'checkbox';
      check.checked = selected.has(photo.id);
      check.addEventListener('change', () => {
        if (check.checked && selected.size === 20) {
          check.checked = false;
          error('One observation can include up to 20 views.');
          return;
        }
        if (check.checked) selected.add(photo.id);
        else selected.delete(photo.id);
        submit.disabled = !selected.size;
        submit.textContent = 'Review observation (' + selected.size + ' photos)';
      });
      const image = document.createElement('img');
      image.loading = 'lazy';
      image.alt = '';
      image.src = thumbnail(photo);
      image.addEventListener('contextmenu', event => showPhotoMenu(event, photo));
      label.append(check, image, caption(photo));
      return label;
    }));
    next.hidden = !hasMore();
  }
  panel.addEventListener('toggle', () => {
    if (!panel.open) return;
    draw();
    if (photos.length === 1 && hasMore()) next.click();
  });
  const guidance = document.createElement('p');
  guidance.textContent = 'Choose photos from one sighting. The first one you select supplies the date and location.';
  panel.append(title, guidance, grid, next, submit);
  return panel;
}

/** @param {string} label @param {() => CandidatePhoto} current @param {() => void} refresh
 * @param {(message: string) => void} error */
export function matchControls(label, current, refresh, error) {
  const group = document.createElement('div');
  const name = document.createElement('input');
  name.placeholder = 'Common or scientific name';
  name.setAttribute('aria-label', 'Species to match');
  const result = document.createElement('span');
  const lookup = button('Find taxon', async () => {
    const photo = current();
    result.replaceChildren();
    try {
      /** @type {{name: string, common_name: string, id: number}} */
      const taxon = await safeFetch('/api/inat/species-match?' + new URLSearchParams({name: name.value}));
      const confirm = button('Confirm for ' + photo.filename, async () => {
        confirm.disabled = true;
        try {
          await safeFetch('/api/highlights/relabel', {method: 'POST', headers: {'Content-Type': 'application/json'},
            body: JSON.stringify({photo_ids: [photo.id], species: taxon.name, from_species: label})});
          refresh();
        } catch (e) { error(String(e)); confirm.disabled = false; }
      });
      result.replaceChildren([taxon.common_name, taxon.name].filter(Boolean).join(' · ') + ' ', confirm);
    } catch (e) { error(String(e)); }
  });
  group.append(name, lookup, result);
  return group;
}
