// The merge dialog's per-attribute controls: building them for the record to
// keep, wiring their inputs, the delegated chip and radio handlers, and
// syncing the coordinate inputs to the preview.

import { state } from './state.js';
import { scheduleMergePreview } from './merge-dialog.js';
import { formatCoords, mergeChoice, osmLink, speciesSubtitle, speciesTitle } from './merge-helpers.js';
import { mergeFields } from './merge-state.js';
import { escapeAttr, escapeHtml } from './shared.js';

/* Build the per-attribute controls for one "record to keep". Rebuilt only
   when that dropdown changes -- re-rendering on every preview would fight
   the user's typing in the name and coordinate inputs. */
/** @param {KeywordMergePreview} data */ export function renderMergeFields(data) {
  const options = data.options;
  const resolved = data.resolved;
  const needed = new Set(data.requires_choice);
  const parts = [];

  parts.push('<div class="kw-merge-field">' +
    '<label class="modal-label" for="kwMergeName">Name</label>' +
    '<input type="text" id="kwMergeName" class="modal-input" value="' +
    escapeAttr(resolved.name) + '">' +
    (options.name.length > 1 ? '<div class="kw-merge-chips">' +
      options.name.map(o => '<button type="button" class="kw-merge-chip" data-name="' +
        escapeAttr(o.value) + '">' + escapeHtml(o.value) + '</button>').join('') +
      '</div>' : '') +
    '</div>');

  if (options.parent.length > 1) {
    parts.push('<div class="kw-merge-field">' +
      '<label class="modal-label" for="kwMergeParent">Parent path</label>' +
      '<select id="kwMergeParent" class="modal-input">' +
      options.parent.map(o => '<option value="' + (o.parent_id === null ? '' : o.parent_id) + '"' +
        (o.parent_id === resolved.parent_id ? ' selected' : '') + '>' +
        escapeHtml(o.path.length ? o.path.join(' → ') : 'No parent (top level)') +
        '</option>').join('') +
      '</select></div>');
  }

  if (options.type.length > 1) {
    parts.push('<div class="kw-merge-field">' +
      '<label class="modal-label" for="kwMergeType">Type</label>' +
      '<select id="kwMergeType" class="modal-input">' +
      options.type.map(o => '<option value="' + escapeAttr(o.value) + '"' +
        (o.value === resolved.type ? ' selected' : '') + '>' + escapeHtml(o.value) +
        '</option>').join('') +
      '</select></div>');
  }

  if (options.place.length) {
    parts.push('<div class="kw-merge-field">' +
      '<label class="modal-label">Linked Google place</label>' +
      '<div class="kw-merge-choices">' +
      options.place.map(o => mergeChoice('kwMergePlace', o.place_id,
        !needed.has('place') && o.place_id === resolved.place_id,
        escapeHtml(o.name),
        escapeHtml(o.path.join(' → ')) +
        (o.latitude != null && o.longitude != null
          ? ' · ' + osmLink(o.latitude, o.longitude, formatCoords(o.latitude, o.longitude))
          : ''))).join('') +
      mergeChoice('kwMergePlace', '', !needed.has('place') && resolved.place_id === null,
        'No linked place', '') +
      '</div>' +
      (needed.has('place')
        ? '<p class="kw-merge-needed">These keywords point at different Google places. ' +
          'Pick the one the combined keyword should keep.</p>'
        : '') +
      '</div>');
  }

  if (options.species.length) {
    parts.push('<div class="kw-merge-field">' +
      '<label class="modal-label">Species link</label>' +
      '<div class="kw-merge-choices">' +
      options.species.map((o, index) => mergeChoice('kwMergeSpecies', String(index),
        !needed.has('species') && o.taxon_id === resolved.taxon_id &&
          o.source_taxon_id === resolved.source_taxon_id,
        escapeHtml(speciesTitle(o)),
        escapeHtml([speciesSubtitle(o), 'from “' + o.name + '”'].join(' · ')))).join('') +
      mergeChoice('kwMergeSpecies', '', !needed.has('species') && resolved.taxon_id === null &&
        resolved.source_taxon_id === null, 'No species link', '') +
      '</div>' +
      (needed.has('species')
        ? '<p class="kw-merge-needed">These keywords point at different taxa. ' +
          'Pick the one the combined keyword should keep.</p>'
        : '') +
      '</div>');
  }

  parts.push('<div class="kw-merge-field">' +
    '<label class="modal-label" for="kwMergeLat">Coordinates</label>' +
    '<div class="kw-merge-coords">' +
    '<input type="number" step="any" id="kwMergeLat" class="modal-input" placeholder="Latitude" value="' +
    (resolved.latitude == null ? '' : resolved.latitude) + '">' +
    '<input type="number" step="any" id="kwMergeLng" class="modal-input" placeholder="Longitude" value="' +
    (resolved.longitude == null ? '' : resolved.longitude) + '">' +
    '</div>' +
    '<div class="kw-merge-chips">' +
    options.coordinates.map(o => '<button type="button" class="kw-merge-chip" data-lat="' +
      o.latitude + '" data-lng="' + o.longitude + '">' + escapeHtml(o.name) + ': ' +
      formatCoords(o.latitude, o.longitude) + '</button>').join('') +
    '<button type="button" class="kw-merge-chip" data-clear-coords="1">Clear</button>' +
    '</div></div>');

  mergeFields.innerHTML = parts.join('');
  state.mergeFieldOptions = options;
  wireMergeFields();
}

/* Per-control handlers only. These elements are replaced wholesale by the
   next renderMergeFields, which discards their listeners with them. The two
   delegated handlers live on the persistent #kwMergeFields container and are
   registered once below — re-adding them here would stack a duplicate on
   every target change and dialog reopen, so one chip click would fire a
   growing number of preview requests against stale option data. */
export function wireMergeFields() {
  const nameInput = /** @type {HTMLInputElement} */ (document.getElementById('kwMergeName'));
  nameInput.addEventListener('input', function() {
    state.mergeOverrides.name = nameInput.value;
    scheduleMergePreview(false);
  });
  const latInput = /** @type {HTMLInputElement} */ (document.getElementById('kwMergeLat'));
  const lngInput = /** @type {HTMLInputElement} */ (document.getElementById('kwMergeLng'));
  function readCoords() {
    if (latInput.value.trim() === '' && lngInput.value.trim() === '') {
      state.mergeOverrides.coordinates = null;
    } else {
      state.mergeOverrides.coordinates = {
        latitude: latInput.value.trim() === '' ? null : Number(latInput.value),
        longitude: lngInput.value.trim() === '' ? null : Number(lngInput.value)
      };
    }
    scheduleMergePreview(false);
  }
  latInput.addEventListener('input', readCoords);
  lngInput.addEventListener('input', readCoords);

  const parentSelect = /** @type {HTMLSelectElement} */ (document.getElementById('kwMergeParent'));
  if (parentSelect) {
    parentSelect.addEventListener('change', function() {
      state.mergeOverrides.parent_id = parentSelect.value === '' ? null : Number(parentSelect.value);
      scheduleMergePreview(true);
    });
  }
  const typeSelect = /** @type {HTMLSelectElement} */ (document.getElementById('kwMergeType'));
  if (typeSelect) {
    typeSelect.addEventListener('change', function() {
      state.mergeOverrides.type = typeSelect.value;
      scheduleMergePreview(true);
    });
  }
}

export function bindMergeFieldDelegates() {
  mergeFields.addEventListener('click', function(event) {
    if (!(event.target instanceof Element)) return; const chip = event.target.closest(/** @type {'button'} */ ('.kw-merge-chip'));
    if (!chip) return;
    const nameInput = /** @type {HTMLInputElement} */ (document.getElementById('kwMergeName'));
    const latInput = /** @type {HTMLInputElement} */ (document.getElementById('kwMergeLat'));
    const lngInput = /** @type {HTMLInputElement} */ (document.getElementById('kwMergeLng'));
    if (chip.dataset.name !== undefined) {
      nameInput.value = chip.dataset.name;
      state.mergeOverrides.name = chip.dataset.name;
    } else if (chip.dataset.clearCoords) {
      latInput.value = ''; lngInput.value = '';
      state.mergeOverrides.coordinates = null;
    } else {
      latInput.value = chip.dataset.lat || '';
      lngInput.value = chip.dataset.lng || '';
      state.mergeOverrides.coordinates = {
        latitude: Number(chip.dataset.lat), longitude: Number(chip.dataset.lng)
      };
    }
    scheduleMergePreview(true);
  });

  mergeFields.addEventListener('change', function(event) {
    const input = event.target; if (!(input instanceof HTMLInputElement)) return;
    if (!state.mergeFieldOptions) return;
    if (input.name === 'kwMergePlace') {
      state.mergeOverrides.place_id = input.value === '' ? null : input.value;
      // The boxes are not updated here. Predicting the point a place change
      // lands on means reimplementing the server's resolution rule in the
      // browser, and the two drifted twice: once showing the previous
      // place's point, once showing blank while the merge kept coordinates.
      // syncCoordinateInputs() copies the answer off the next preview
      // instead, so the fields always show what the merge will actually do.
      scheduleMergePreview(true);
    } else if (input.name === 'kwMergeSpecies') {
      const species = state.mergeFieldOptions.species[Number(input.value)]; if (input.value !== '' && !species) return;
      state.mergeOverrides.species = input.value === ''
        ? null
        : {
            taxon_id: species ? species.taxon_id : null,
            source_taxon_id: species ? species.source_taxon_id : null
          };
      scheduleMergePreview(true);
    }
  });
}

/* Show the coordinates the merge will really use. Skipped once the user
   types their own, because from then on the boxes ARE the override and
   overwriting them would fight the typing. */
/** @param {KeywordMergePreview} data */ export function syncCoordinateInputs(data) {
  if (state.mergeOverrides.coordinates !== undefined) return;
  const latInput = /** @type {HTMLInputElement} */ (document.getElementById('kwMergeLat'));
  const lngInput = /** @type {HTMLInputElement} */ (document.getElementById('kwMergeLng'));
  if (!latInput || !lngInput) return;
  latInput.value = data.resolved.latitude == null ? '' : String(data.resolved.latitude);
  lngInput.value = data.resolved.longitude == null ? '' : String(data.resolved.longitude);
}
