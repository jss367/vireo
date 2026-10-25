import { _lbGuardReadOnly } from './state.js';
import { vireoThumbnailUrl } from './edits.js';
import { bindExternalLinks, safeEventSource, safeFetch, showToast } from '../navbar-shared-helpers.js';

/* ---------- iNat Submission ---------- */
/** @type {number | null} */ var _inatEscToken = null;

/** @type {InatQueueItem[]} */ export var inatQueue = [];  // [{photo_id, taxon_name, observed_on, latitude, longitude, description, geoprivacy, filename, already_submitted, existing_url}]
var _inatSubmitOwner = 0;
var _inatSubmitting = false;  // true while inatDoSubmit's loop is running
var _inatCancelled = false;   // set by closeInatModal to stop the loop gracefully
/** @type {InatPreparationFailure[]} */ var _inatQuickFailures = [];
var _inatModalGeneration = 0;
/** @type {EventSource | null} */ export var _inatExportStream = null;

function _closeInatExportStream() {
  if (!_inatExportStream) return;
  try { _inatExportStream.close(); } catch(e) {}
  _inatExportStream = null;
}

/** The name iNaturalist receives for one species option. @param {InatSpeciesOption} option */
function _inatOptionTaxon(option) {
  return option.scientific_name || option.species || '';
}

function _inatQueueItem(/** @type {number} */ photoId, /** @type {InatPreparedPhoto} */ data) {
  var options = (data.species_options || []).filter(function(option) { return !!_inatOptionTaxon(option); });
  // The single-valued fields stay the default; a photo with only a
  // confirmed keyword (no prediction) starts from that keyword instead of
  // an empty taxon.
  var first = options[0];
  var taxon = data.scientific_name || data.species || (first ? _inatOptionTaxon(first) : '');
  return {
    photo_id: photoId,
    taxon_name: taxon,
    species_options: options,
    observed_on: data.timestamp ? data.timestamp.substring(0, 10) : '',
    latitude: data.latitude != null ? data.latitude : '',
    longitude: data.longitude != null ? data.longitude : '',
    description: '',
    geoprivacy: 'open',
    filename: data.filename,
    edit_recipe: data.edit_recipe,
    already_submitted: data.already_submitted,
    existing_url: data.existing_observation_url,
    upload_url: data.upload_url,
  };
}

/** @param {InatSpeciesOption} option */
function _inatOptionLabel(option) {
  var name = option.species || option.scientific_name || '';
  if (option.scientific_name && option.scientific_name !== name) name += ' (' + option.scientific_name + ')';
  var source = option.source === 'confirmed' ? 'confirmed'
    : Math.round(100 * (option.confidence || 0)) + '% prediction';
  return name + ' · ' + source;
}

/**
 * The species picker for a photo carrying more than one species. An
 * observation records one taxon, so the dialog says how many the photo has
 * and lets you choose; the default stays the prefilled taxon.
 *
 * @param {InatQueueItem} item @param {number} idx
 */
function _inatSpeciesChoiceHtml(item, idx) {
  var options = item.species_options || [];
  if (options.length < 2) return '';
  var current = options.findIndex(function(option) { return _inatOptionTaxon(option) === item.taxon_name; });
  if (current < 0) current = options.findIndex(function(option) { return option.primary; });
  var names = options.map(function(option) { return option.species || option.scientific_name; }).join(', ');
  return '<div class="inat-species-choice">' +
    '<p id="inatSpeciesNote' + idx + '">This photo has ' + options.length + ' species: ' + escapeHtml(names) +
      '. An iNaturalist observation records one taxon, so choose which one to send.</p>' +
    '<select id="inatSpecies' + idx + '" data-inat-species="' + idx + '" aria-label="Species to send to iNaturalist">' +
    options.map(function(option, oi) {
      return '<option value="' + oi + '"' + (oi === current ? ' selected' : '') + '>' + escapeHtml(_inatOptionLabel(option)) + '</option>';
    }).join('') +
    '</select></div>';
}

/**
 * Bind each species picker in ``root``: choosing a species updates the
 * queued taxon and calls ``onChange`` with the card index and taxon.
 *
 * @param {HTMLElement} root @param {(idx: number, taxon: string) => void} onChange
 */
function _inatBindSpeciesChoices(root, onChange) {
  root.querySelectorAll('select[data-inat-species]').forEach(function(node) {
    var select = /** @type {HTMLSelectElement} */ (node);
    var idx = Number(select.getAttribute('data-inat-species'));
    select.addEventListener('change', function() {
      var item = inatQueue[idx];
      var option = item && (item.species_options || [])[Number(select.value)];
      if (!item || !option) return;
      item.taxon_name = _inatOptionTaxon(option);
      onChange(idx, item.taxon_name);
    });
  });
}

/** Quick mode: the taxon line follows the picked species. @param {number} idx @param {string} taxon */
function _inatQuickSpeciesChanged(idx, taxon) {
  var text = document.getElementById('inatTaxonText' + idx);
  if (text) text.textContent = taxon;
}

/** Direct mode: the editable taxon field takes the picked species. @param {number} idx @param {string} taxon */
function _inatDirectSpeciesChanged(idx, taxon) {
  var input = /** @type {HTMLInputElement | null} */ (document.getElementById('inatTaxon' + idx));
  if (input) input.value = taxon;
}

/** The quick dialog's "Include with this photo" choices. @param {InatQueueItem} item @param {number} idx */
function _inatQuickOptions(item, idx) {
  var hasTaxon = !!item.taxon_name;
  var hasDate = !!item.observed_on;
  var hasLocation = item.latitude !== '' && item.longitude !== '';
  var taxonText = hasTaxon ? item.taxon_name : 'No detected taxon';
  var dateText = hasDate ? item.observed_on : 'No observation date';
  var locationText = hasLocation
    ? String(item.latitude) + ', ' + String(item.longitude)
    : 'No location';
  return '<fieldset style="border:0;padding:0;margin:12px 0 0;display:flex;flex-direction:column;gap:8px;">' +
    '<legend style="font-size:13px;color:var(--text-primary);margin-bottom:7px;">Include with this photo</legend>' +
    _inatSpeciesChoiceHtml(item, idx) +
    '<label style="font-size:13px;color:var(--text-secondary);display:flex;align-items:center;gap:8px;">' +
      '<input id="inatIncludeTaxon' + idx + '" type="checkbox"' + (hasTaxon ? ' checked' : ' disabled') + '> Taxon: <span id="inatTaxonText' + idx + '">' + escapeHtml(taxonText) + '</span>' +
    '</label>' +
    '<label style="font-size:13px;color:var(--text-secondary);display:flex;align-items:center;gap:8px;">' +
      '<input id="inatIncludeDate' + idx + '" type="checkbox"' + (hasDate ? ' checked' : ' disabled') + '> Date: ' + escapeHtml(dateText) +
    '</label>' +
    '<label style="font-size:13px;color:var(--text-secondary);display:flex;align-items:center;gap:8px;">' +
      '<input id="inatIncludeLocation' + idx + '" type="checkbox"' + (hasLocation ? ' checked' : ' disabled') + '> Location: ' + escapeHtml(locationText) +
    '</label>' +
    '<label style="font-size:13px;color:var(--text-secondary);display:flex;align-items:center;gap:8px;">' +
      '<input id="inatIncludeDescription' + idx + '" type="checkbox"> Description' +
    '</label>' +
    '<textarea id="inatQuickDescription' + idx + '" aria-label="Description" placeholder="Description (optional)" style="width:100%;min-height:48px;box-sizing:border-box;background:var(--bg-secondary);color:var(--text-primary);border:1px solid var(--border-secondary);border-radius:3px;padding:6px 8px;font-size:12px;resize:vertical;"></textarea>' +
  '</fieldset>';
}

async function _inatOpenUrl(/** @type {string} */ url) {
  try {
    if (typeof openExternal === 'function') {
      return await openExternal(url);
    }
  } catch(e) {}
  return false;
}

function _inatQuickUploadUrl(/** @type {number} */ idx) {
  var item = inatQueue[idx]; if (!item) return 'https://www.inaturalist.org/observations/upload';
  var base = (item && item.upload_url) || 'https://www.inaturalist.org/observations/upload';
  base = base.split('?')[0] || base;
  var params = [];
  var taxon = /** @type {HTMLInputElement} */ (document.getElementById('inatIncludeTaxon' + idx));
  var date = /** @type {HTMLInputElement} */ (document.getElementById('inatIncludeDate' + idx));
  var location = /** @type {HTMLInputElement} */ (document.getElementById('inatIncludeLocation' + idx));
  if (taxon && taxon.checked && item.taxon_name) {
    params.push('taxon_name=' + encodeURIComponent(item.taxon_name));
  }
  if (date && date.checked && item.observed_on) {
    params.push('observed_on=' + encodeURIComponent(item.observed_on));
  }
  if (location && location.checked && item.latitude !== '' && item.longitude !== '') {
    params.push('lat=' + encodeURIComponent(String(item.latitude)));
    params.push('lng=' + encodeURIComponent(String(item.longitude)));
  }
  return base + (params.length ? '?' + params.join('&') : '');
}

async function _openInatQuickUploadNative(/** @type {string} */ url) {
  if (await _inatOpenUrl(url)) {
    showToast('Opened iNaturalist in your browser.', 'success');
    return;
  }
  if (typeof showExternalOpenFailure === 'function') {
    showExternalOpenFailure(
      url,
      'Vireo could not open iNaturalist. Retry or copy the upload URL below.'
    );
  }
}

function openInatQuickUpload(/** @type {MouseEvent | undefined} */ event, /** @type {number} */ idx) {
  var url = _inatQuickUploadUrl(idx);
  var link = event && event.currentTarget;
  if (link instanceof HTMLAnchorElement) link.href = url;

  if (typeof isTauri === 'function' && isTauri()) {
    if (event) event.preventDefault();
    _openInatQuickUploadNative(url);
    return false;
  }

  // The loopback page can occasionally load without Tauri's IPC globals.
  // Preserve a real link in that case: a normal browser opens a new tab, and
  // the native shell's on_new_window hook sends the URL to the OS browser.
  // Stop propagation so the delegated external-link handler does not replace
  // this fallback with the unavailable IPC path.
  if (event) event.stopPropagation();
  return true;
}

function copyInatQuickUploadUrl(/** @type {number} */ idx) {
  return copyExternalUrl(_inatQuickUploadUrl(idx));
}

export async function submitToInat(/** @type {number} */ photoId) {
  if (_lbGuardReadOnly()) return false;
  try {
    var data = await /** @type {Promise<InatPreparedPhoto>} */ (safeFetch('/api/inat/prepare/' + photoId, {}, { toast: false }));
    if (data.error) { alert(data.error); return; }

    if (data.mode === 'quick') {
      await openInatQuickModal([_inatQueueItem(photoId, data)], []);
      return;
    }

    // Direct mode: open modal
    inatQueue = [_inatQueueItem(photoId, data)];
    openInatModal([]);
  } catch(e) { const failure = /** @type {Error & {body?: {partial?: boolean, observation_url?: string}}} */ (e instanceof Error ? e : new Error(String(e)));
    alert('Error: ' + failure.message);
  }
}

export async function submitToInatBatch(/** @type {number[]} */ photoIds) {
  if (_lbGuardReadOnly()) return false;
  // Prepare all photos
  inatQueue = [];
  /** @type {InatQueueItem[]} */ var quickQueue = [];
  /** @type {InatPreparationFailure[]} */ var failures = [];
  for (var i = 0; i < photoIds.length; i++) {
    const photoId = photoIds[i]; if (photoId == null) continue;
    try {
      var data = await /** @type {Promise<InatPreparedPhoto>} */ (safeFetch('/api/inat/prepare/' + photoId, {}, { toast: false }));
      if (data.error) {
        failures.push({photo_id: photoId, error: data.error});
        continue;
      }
      var item = _inatQueueItem(photoId, data);
      if (data.mode === 'quick') quickQueue.push(item);
      else inatQueue.push(item);
    } catch(e) { const failure = /** @type {Error & {body?: {partial?: boolean, observation_url?: string}}} */ (e instanceof Error ? e : new Error(String(e)));
      failures.push({photo_id: photoId, error: failure.message || 'Could not prepare iNaturalist upload'});
    }
  }
  if (quickQueue.length) {
    await openInatQuickModal(quickQueue, failures);
    return;
  }
  if (inatQueue.length === 0) {
    var msg = failures.length
      ? 'No photos could be prepared for iNaturalist.'
      : 'No photos to submit.';
    alert(msg);
    return;
  }
  openInatModal(failures);
}

export async function openInatQuickModal(/** @type {InatQueueItem[]} */ items, /** @type {InatPreparationFailure[]} */ failures) {
  _inatModalGeneration++;
  _closeInatExportStream();
  if (_inatEscToken) Keymap.popEsc(_inatEscToken);
  _inatEscToken = Keymap.pushEsc(function() { closeInatModal(); });

  inatQueue = items.slice();
  _inatQuickFailures = (failures || []).slice();
  var title = items.length === 1 ? 'Send to iNaturalist' : 'Send ' + items.length + ' photos to iNaturalist';
  /** @type {HTMLElement} */ (document.getElementById('inatModalTitle')).textContent = title;
  /** @type {HTMLElement} */ (document.getElementById('inatProgress')).style.display = 'none';

  var submitBtn = /** @type {HTMLButtonElement} */ (document.getElementById('inatSubmitBtn'));
  submitBtn.style.display = '';
  submitBtn.disabled = true;
  submitBtn.textContent = items.length === 1 ? 'Send to iNaturalist' : 'Send All to iNaturalist';

  var cancelBtn = document.querySelector('#inatActions .modal-btn-cancel');
  if (cancelBtn) cancelBtn.textContent = 'Close';

  function _inatAlreadySubmittedWarning(/** @type {InatQueueItem} */ item) {
    if (!item.already_submitted) return '';
    var linkOrText = item.existing_url
      ? '<a href="' + escapeAttr(item.existing_url || '') + '" target="_blank" rel="noopener" data-external-link style="color:var(--warning);">' + escapeHtml(item.existing_url || '') + '</a>'
      : 'no observation URL recorded';
    return '<div class="inat-card-status warning" style="margin-top:6px;">&#9888; Already submitted to iNaturalist: ' + linkOrText + '. Opening a new upload will create a duplicate observation.</div>';
  }

  var html = '<div class="inat-card" id="inatTokenSetup">' +
    '<div style="font-size:13px;color:var(--text-primary);font-weight:600;margin-bottom:5px;">Direct submission needs an iNaturalist token</div>' +
    '<div style="font-size:12px;line-height:1.5;color:var(--text-secondary);margin-bottom:9px;">' +
      'Paste a token below and Vireo will validate it before saving. iNaturalist tokens expire after 24 hours. ' +
      '<a href="https://www.inaturalist.org/users/api_token" target="_blank" rel="noopener" data-external-link style="color:var(--accent);">Get a token</a>' +
    '</div>' +
    '<div style="display:flex;gap:8px;align-items:center;">' +
      '<input id="inatQuickToken" type="password" autocomplete="off" placeholder="Paste iNaturalist token" style="flex:1;min-width:0;background:var(--bg-secondary);color:var(--text-primary);border:1px solid var(--border-secondary);border-radius:3px;padding:7px 8px;font-size:12px;font-family:monospace;">' +
      '<button type="button" class="modal-btn modal-btn-primary" id="inatQuickTokenBtn">Validate &amp; Save</button>' +
    '</div>' +
    '<div class="inat-card-status" id="inatQuickTokenStatus">The Send button will be enabled after validation.</div>' +
  '</div>';
  if (items.length === 1 && items[0]) {
    html += '<div class="inat-card">' +
      '<div class="inat-card-header">' +
        '<img class="inat-card-thumb" src="/thumbnails/' + items[0].photo_id + '.jpg" alt="">' +
        '<div style="flex:1;min-width:0;">' +
          '<div style="font-size:13px;color:var(--text-primary);white-space:nowrap;overflow:hidden;text-overflow:ellipsis;">' + escapeHtml(items[0].filename) + '</div>' +
          _inatAlreadySubmittedWarning(items[0]) +
        '</div>' +
      '</div>' +
      '<div style="font-size:13px;line-height:1.5;color:var(--text-secondary);">' +
        'Choose which details Vireo should add to the exported JPEG and upload link.' +
      '</div>' +
      _inatQuickOptions(items[0], 0) +
      '<div style="margin-top:12px;">' +
        '<a class="modal-btn modal-btn-primary" href="' + escapeAttr(items[0].upload_url || 'https://www.inaturalist.org/observations/upload') + '" target="_blank" rel="noopener" data-inat-upload="0" style="display:inline-block;text-decoration:none;">Open Upload Page</a>' +
        '<button type="button" class="modal-btn" data-inat-copy-url="0" style="margin-left:8px;">Copy URL</button>' +
      '</div>' +
    '</div>';
  } else {
    html += '<div class="inat-card">' +
      '<div style="font-size:13px;line-height:1.5;color:var(--text-secondary);margin-bottom:10px;">' +
        'Use the token field above for direct submission, or export the JPEGs and open an upload page for each photo below.' +
      '</div>';
    items.forEach(function(item, idx) {
      html += '<div style="display:flex;flex-direction:column;gap:4px;padding:8px 0;border-top:1px solid var(--border-primary);">' +
        '<div style="display:flex;align-items:center;justify-content:space-between;gap:12px;">' +
          '<span style="font-size:13px;color:var(--text-primary);white-space:nowrap;overflow:hidden;text-overflow:ellipsis;">' + escapeHtml(item.filename) + '</span>' +
          '<span style="display:flex;align-items:center;gap:8px;white-space:nowrap;">' +
            '<a href="' + escapeAttr(item.upload_url || 'https://www.inaturalist.org/observations/upload') + '" target="_blank" rel="noopener" data-inat-upload="' + idx + '" style="color:var(--accent);font-size:12px;">Open Upload</a>' +
            '<button type="button" data-inat-copy-url="' + idx + '" style="border:0;background:none;color:var(--text-secondary);font-size:12px;cursor:pointer;padding:0;">Copy URL</button>' +
          '</span>' +
        '</div>' +
        _inatQuickOptions(item, idx) +
        _inatAlreadySubmittedWarning(item) +
      '</div>';
    });
    html += '</div>';
  }

  if (failures && failures.length) {
    html += '<div class="inat-card-status error" style="margin-top:10px;">' +
      escapeHtml(failures.length + ' photo' + (failures.length === 1 ? '' : 's') + ' could not be prepared.') +
      '</div>';
  }

  var exportLabel = items.length === 1 ? 'Export JPEG\u2026' : 'Export ' + items.length + ' JPEGs\u2026';
  html += '<div class="inat-card">' +
    '<div style="font-size:13px;color:var(--text-primary);font-weight:600;margin-bottom:5px;">Upload through your browser</div>' +
    '<div style="font-size:12px;line-height:1.5;color:var(--text-secondary);margin-bottom:9px;">' +
      'Export edited JPEG' + (items.length === 1 ? '' : 's') + ' with only the checked metadata, then add ' + (items.length === 1 ? 'it' : 'them') + ' on the iNaturalist upload page.' +
    '</div>' +
    '<button type="button" class="modal-btn modal-btn-primary" id="inatQuickExportBtn">' + escapeHtml(exportLabel) + '</button>' +
    '<div class="inat-card-status" id="inatQuickExportStatus"></div>' +
  '</div>';

  _inatRenderQuickCards(html);
  /** @type {HTMLElement} */ (document.getElementById('inatModal')).classList.add('open');
}

/** Show the quick modal's cards and bind their upload, copy, token and export controls. @param {string} html */
function _inatRenderQuickCards(html) {
  var cards = /** @type {HTMLElement} */ (document.getElementById('inatCards'));
  cards.innerHTML = html;
  bindExternalLinks(cards);
  _inatBindSpeciesChoices(cards, _inatQuickSpeciesChanged);
  cards.querySelectorAll('[data-inat-upload]').forEach(function(link) {
    var idx = Number(link.getAttribute('data-inat-upload'));
    link.addEventListener('click', function(event) { openInatQuickUpload(/** @type {MouseEvent} */ (event), idx); });
  });
  cards.querySelectorAll('[data-inat-copy-url]').forEach(function(button) {
    var idx = Number(button.getAttribute('data-inat-copy-url'));
    button.addEventListener('click', function() { copyInatQuickUploadUrl(idx); });
  });
  var tokenButton = document.getElementById('inatQuickTokenBtn');
  if (tokenButton) tokenButton.addEventListener('click', function() { validateAndSaveInatToken(); });
  var exportButton = document.getElementById('inatQuickExportBtn');
  if (exportButton) exportButton.addEventListener('click', function() { exportInatQuickPhotos(); });
}

async function validateAndSaveInatToken() {
  var input = /** @type {HTMLInputElement} */ (document.getElementById('inatQuickToken'));
  var status = /** @type {HTMLElement} */ (document.getElementById('inatQuickTokenStatus'));
  var button = /** @type {HTMLButtonElement} */ (document.getElementById('inatQuickTokenBtn'));
  var generation = _inatModalGeneration;
  var token = input ? input.value.trim() : '';
  if (!token) {
    status.className = 'inat-card-status error';
    status.textContent = 'Paste a token first.';
    return;
  }
  button.disabled = true;
  button.textContent = 'Validating\u2026';
  status.className = 'inat-card-status';
  status.textContent = 'Checking with iNaturalist\u2026';
  try {
    var data = await /** @type {Promise<{login?: string}>} */ (safeFetch('/api/inat/token', {
      method: 'POST',
      headers: {'Content-Type': 'application/json'},
      body: JSON.stringify({token: token}),
    }, { toast: false }));
    if (
      generation !== _inatModalGeneration ||
      !/** @type {HTMLElement} */ (document.getElementById('inatModal')).classList.contains('open') ||
      /** @type {HTMLInputElement} */ (document.getElementById('inatQuickToken')) !== input
    ) return;
    status.className = 'inat-card-status success';
    status.textContent = '\u2713 Valid token' + (data.login ? ' for ' + data.login : '') + '. Opening direct submission\u2026';
    _inatApplyQuickChoicesToQueue();
    openInatModal(_inatQuickFailures);
  } catch(e) { const failure = /** @type {Error & {body?: {partial?: boolean, observation_url?: string}}} */ (e instanceof Error ? e : new Error(String(e)));
    if (generation !== _inatModalGeneration) return;
    status.className = 'inat-card-status error';
    status.textContent = '\u2717 ' + failure.message;
    button.disabled = false;
    button.textContent = 'Validate & Save';
  }
}

function _inatQuickExportSubmissions() {
  return inatQueue.map(function(item, idx) {
    var description = /** @type {HTMLTextAreaElement} */ (document.getElementById('inatQuickDescription' + idx));
    return {
      photo_id: item.photo_id,
      taxon_name: item.taxon_name || '',
      observed_on: item.observed_on || '',
      latitude: item.latitude,
      longitude: item.longitude,
      include_taxon: !!(/** @type {HTMLInputElement | null} */ (document.getElementById('inatIncludeTaxon' + idx)) || {checked: false}).checked,
      include_date: !!(/** @type {HTMLInputElement | null} */ (document.getElementById('inatIncludeDate' + idx)) || {checked: false}).checked,
      include_location: !!(/** @type {HTMLInputElement | null} */ (document.getElementById('inatIncludeLocation' + idx)) || {checked: false}).checked,
      include_description: !!(/** @type {HTMLInputElement | null} */ (document.getElementById('inatIncludeDescription' + idx)) || {checked: false}).checked,
      description: description ? description.value.trim() : '',
    };
  });
}

function _inatApplyQuickChoicesToQueue() {
  var choices = _inatQuickExportSubmissions();
  choices.forEach(function(choice, idx) {
    var item = inatQueue[idx];
    if (!item) return;
    if (!choice.include_taxon) item.taxon_name = '';
    if (!choice.include_date) item.observed_on = '';
    if (!choice.include_location) {
      item.latitude = '';
      item.longitude = '';
    }
    item.description = choice.include_description ? choice.description : '';
  });
}

export async function exportInatQuickPhotos() {
  var status = /** @type {HTMLElement} */ (document.getElementById('inatQuickExportStatus'));
  var button = /** @type {HTMLButtonElement} */ (document.getElementById('inatQuickExportBtn'));
  if (!button || button.disabled) return;
  var generation = _inatModalGeneration;
  var queue = inatQueue;
  var itemCount = queue.length;
  var submissions = _inatQuickExportSubmissions();
  var exportLabel = itemCount === 1 ? 'Export JPEG\u2026' : 'Export ' + itemCount + ' JPEGs\u2026';
  var destination;
  button.disabled = true;
  button.textContent = 'Choosing folder\u2026';
  try {
    if (typeof pickDirectory === 'function' && typeof isTauri === 'function' && isTauri()) {
      destination = await pickDirectory('Export for iNaturalist');
    } else {
      destination = window.prompt('Export folder path:');
    }
  } catch(e) { const failure = /** @type {Error & {body?: {partial?: boolean, observation_url?: string}}} */ (e instanceof Error ? e : new Error(String(e)));
    if (generation !== _inatModalGeneration) return;
    status.className = 'inat-card-status error';
    status.textContent = '\u2717 Could not open the folder picker: ' + failure.message;
    button.disabled = false;
    button.textContent = exportLabel;
    return;
  }
  if (!destination) {
    if (generation === _inatModalGeneration && inatQueue === queue) {
      button.disabled = false;
      button.textContent = exportLabel;
    }
    return;
  }
  if (
    generation !== _inatModalGeneration ||
    inatQueue !== queue ||
    !/** @type {HTMLElement} */ (document.getElementById('inatModal')).classList.contains('open')
  ) return;
  button.textContent = 'Exporting\u2026';
  status.className = 'inat-card-status';
  status.textContent = 'Rendering edited JPEG' + (itemCount === 1 ? '' : 's') + ' and writing metadata\u2026';
  try {
    var started = await /** @type {Promise<{job_id: number}>} */ (safeFetch('/api/inat/export', {
      method: 'POST',
      headers: {'Content-Type': 'application/json'},
      body: JSON.stringify({
        destination: destination,
        submissions: submissions,
        reveal: typeof isTauri === 'function' && isTauri(),
      }),
    }, { toast: false }));
    if (generation !== _inatModalGeneration || inatQueue !== queue) return;
    _closeInatExportStream();
    var exportStream = safeEventSource('/api/jobs/' + started.job_id + '/stream', {
      onProgress: function(/** @type {InatExportProgress} */ progress) {
        if (
          generation !== _inatModalGeneration ||
          _inatExportStream !== exportStream
        ) return;
        var current = progress.current || 0;
        var total = progress.total || itemCount;
        status.textContent = 'Exporting ' + current + ' of ' + total + '\u2026';
      },
      onComplete: function(/** @type {InatExportCompletion} */ done) {
        var isCurrentStream = _inatExportStream === exportStream;
        if (!isCurrentStream || generation !== _inatModalGeneration) return;
        try { exportStream.close(); } catch(e) {}
        _inatExportStream = null;
        var result = done.result || {};
        var count = (result.exported || []).length;
        var failed = (result.errors || []).length;
        if (done.status === 'cancelled') {
          status.className = 'inat-card-status warning';
          status.textContent = 'Export cancelled after ' + count + ' JPEG' + (count === 1 ? '' : 's') + '.';
        } else if (!count) {
          status.className = 'inat-card-status error';
          status.textContent = '\u2717 ' + (
            failed ? (result.errors?.[0]?.error || 'Export failed') : 'No photos were exported.'
          );
        } else {
          status.className = failed ? 'inat-card-status warning' : 'inat-card-status success';
          status.textContent = '\u2713 Exported ' + count + ' JPEG' + (count === 1 ? '' : 's') +
            (failed ? '; ' + failed + ' failed.' : '.') +
            (result.revealed ? ' Revealed in the file manager.' : '');
        }
        button.disabled = false;
        button.textContent = exportLabel;
      },
      onError: function() {
        var isCurrentStream = _inatExportStream === exportStream;
        if (!isCurrentStream || generation !== _inatModalGeneration) return;
        try { exportStream.close(); } catch(e) {}
        _inatExportStream = null;
        status.className = 'inat-card-status error';
        status.textContent = '\u2717 Lost the export progress connection.';
        button.disabled = false;
        button.textContent = exportLabel;
      },
    });
    _inatExportStream = exportStream;
  } catch(e) { const failure = /** @type {Error & {body?: {partial?: boolean, observation_url?: string}}} */ (e instanceof Error ? e : new Error(String(e)));
    if (generation !== _inatModalGeneration) return;
    status.className = 'inat-card-status error';
    status.textContent = '\u2717 ' + failure.message;
    button.disabled = false;
    button.textContent = exportLabel;
  }
}

function openInatModal(/** @type {InatPreparationFailure[]} */ failures) {
  _inatModalGeneration++;
  _closeInatExportStream();
  if (_inatEscToken) Keymap.popEsc(_inatEscToken);
  _inatEscToken = Keymap.pushEsc(function() { closeInatModal(); });

  var title = inatQueue.length === 1 ? 'Submit to iNaturalist' : 'Submit ' + inatQueue.length + ' observations to iNaturalist';
  /** @type {HTMLElement} */ (document.getElementById('inatModalTitle')).textContent = title;
  /** @type {HTMLElement} */ (document.getElementById('inatProgress')).style.display = 'none';
  var submitBtn = /** @type {HTMLButtonElement} */ (document.getElementById('inatSubmitBtn'));
  submitBtn.style.display = '';
  submitBtn.disabled = false;
  submitBtn.textContent = inatQueue.length === 1 ? 'Send to iNaturalist' : 'Send All to iNaturalist';
  var cancelBtn = document.querySelector('#inatActions .modal-btn-cancel');
  if (cancelBtn) cancelBtn.textContent = 'Cancel';

  var html = '';
  inatQueue.forEach(function(item, idx) {
    var warn = '';
    if (item.already_submitted) {
      warn = '<div class="inat-card-status warning">&#9888; Already submitted: <a href="' + escapeAttr(item.existing_url || '') + '" target="_blank" data-external-link style="color:var(--warning);">' + escapeHtml(item.existing_url || '') + '</a></div>';
    }
    var thumbUrl = vireoThumbnailUrl ? vireoThumbnailUrl({id: item.photo_id, edit_recipe: item.edit_recipe}) : '/thumbnails/' + item.photo_id + '.jpg';
    html += '<div class="inat-card" id="inatCard' + idx + '">' +
      '<div class="inat-card-header">' +
        '<img class="inat-card-thumb" src="' + escapeAttr(thumbUrl) + '" alt="">' +
        '<div style="flex:1;min-width:0;">' +
          '<div style="font-size:13px;color:var(--text-primary);white-space:nowrap;overflow:hidden;text-overflow:ellipsis;">' + escapeHtml(item.filename) + '</div>' +
          warn +
        '</div>' +
      '</div>' +
      _inatSpeciesChoiceHtml(item, idx) +
      '<div class="inat-card-fields">' +
        '<div><label>Species / Taxon</label><input id="inatTaxon' + idx + '" value="' + escapeAttr(item.taxon_name) + '"></div>' +
        '<div><label>Date observed</label><input id="inatDate' + idx + '" type="date" value="' + escapeAttr(item.observed_on) + '"></div>' +
        '<div><label>Latitude</label><input id="inatLat' + idx + '" value="' + escapeAttr(String(item.latitude)) + '"></div>' +
        '<div><label>Longitude</label><input id="inatLng' + idx + '" value="' + escapeAttr(String(item.longitude)) + '"></div>' +
        '<div><label>Geoprivacy</label><select id="inatGeo' + idx + '"><option value="open">Open</option><option value="obscured">Obscured</option><option value="private">Private</option></select></div>' +
        '<div></div>' +
        '<textarea id="inatDesc' + idx + '" placeholder="Notes (optional)">' + escapeHtml(item.description || '') + '</textarea>' +
      '</div>' +
      '<div class="inat-card-status" id="inatStatus' + idx + '"></div>' +
    '</div>';
  });
  if (failures && failures.length) {
    html += '<div class="inat-card-status error" style="margin-top:10px;">' +
      escapeHtml(failures.length + ' photo' + (failures.length === 1 ? '' : 's') + ' could not be prepared.') +
      '</div>';
  }
  var cards = /** @type {HTMLElement} */ (document.getElementById('inatCards'));
  cards.innerHTML = html;
  bindExternalLinks(cards);
  _inatBindSpeciesChoices(cards, _inatDirectSpeciesChanged);
  /** @type {HTMLElement} */ (document.getElementById('inatModal')).classList.add('open');
}

export function closeInatModal() {
  _inatModalGeneration++;
  _closeInatExportStream();
  if (_inatEscToken) { Keymap.popEsc(_inatEscToken); _inatEscToken = null; }
  /** @type {HTMLElement} */ (document.getElementById('inatModal')).classList.remove('open');
  if (_inatSubmitting) {
    // Cancel/Esc during an in-flight submit: don't clear the queue out from
    // under the loop — flag it so it stops after the current item and
    // reports a partial result.
    _inatCancelled = true;
  } else {
    inatQueue = [];
    _inatQuickFailures = [];
  }
}

export async function inatDoSubmit() {
  if (_lbGuardReadOnly()) return false;
  var btn = /** @type {HTMLButtonElement} */ (document.getElementById('inatSubmitBtn'));
  btn.disabled = true;
  btn.textContent = 'Submitting...';

  var progress = /** @type {HTMLElement} */ (document.getElementById('inatProgress'));
  var fill = /** @type {HTMLElement} */ (document.getElementById('inatProgressFill'));
  var text = /** @type {HTMLElement} */ (document.getElementById('inatProgressText'));
  progress.style.display = 'block';

  var queue = inatQueue;
  var generation = _inatModalGeneration;
  var owner = ++_inatSubmitOwner;
  var total = queue.length;
  var done = 0;
  var succeeded = 0;
  _inatSubmitting = true;
  _inatCancelled = false;

  for (var i = 0; i < total; i++) {
    if (_inatCancelled || generation !== _inatModalGeneration) break;
    var item = queue[i]; if (!item) continue;
    var statusEl = /** @type {HTMLElement} */ (document.getElementById('inatStatus' + i));
    var latValue = /** @type {HTMLInputElement} */ (document.getElementById('inatLat' + i)).value.trim();
    var lngValue = /** @type {HTMLInputElement} */ (document.getElementById('inatLng' + i)).value.trim();

    // Read possibly-edited fields from the form
    var submission = {
      photo_id: item.photo_id,
      taxon_name: /** @type {HTMLInputElement} */ (document.getElementById('inatTaxon' + i)).value.trim(),
      observed_on: /** @type {HTMLInputElement} */ (document.getElementById('inatDate' + i)).value,
      latitude: latValue === '' ? null : parseFloat(latValue),
      longitude: lngValue === '' ? null : parseFloat(lngValue),
      description: /** @type {HTMLTextAreaElement} */ (document.getElementById('inatDesc' + i)).value.trim(),
      geoprivacy: /** @type {HTMLSelectElement} */ (document.getElementById('inatGeo' + i)).value,
    };

    statusEl.className = 'inat-card-status';
    statusEl.textContent = 'Submitting...';

    try {
      var result = await /** @type {Promise<{error?: string, observation_url?: string}>} */ (safeFetch('/api/inat/submit', {
        method: 'POST',
        headers: {'Content-Type': 'application/json'},
        body: JSON.stringify(submission),
      }, { toast: false }));

      if (generation !== _inatModalGeneration) {
        if (!result.error) succeeded++;
        done++;
        break;
      }
      if (result.error) {
        statusEl.className = 'inat-card-status error';
        statusEl.textContent = '✗ ' + result.error;
      } else {
        statusEl.className = 'inat-card-status success';
        statusEl.innerHTML = '&#10003; Submitted — <a href="' + escapeAttr(result.observation_url || '') + '" target="_blank" data-external-link style="color:var(--accent);">View on iNaturalist</a>';
        bindExternalLinks(statusEl);
        if (total === 1 && result.observation_url && isTauri()) {
          var opened = await openExternal(result.observation_url);
          if (!opened) showToast('Submitted, but could not open iNaturalist: ' + result.observation_url, 'error');
        }
        succeeded++;
      }
    } catch(e) { const failure = /** @type {Error & {body?: {partial?: boolean, observation_url?: string}}} */ (e instanceof Error ? e : new Error(String(e)));
      if (generation !== _inatModalGeneration) break;
      statusEl.className = 'inat-card-status error';
      if (failure.body && failure.body.partial && failure.body.observation_url) {
        statusEl.innerHTML = '&#10007; ' + escapeHtml(failure.message) +
          ' <a href="' + escapeAttr(failure.body.observation_url) + '" target="_blank" rel="noopener" data-external-link style="color:var(--danger);text-decoration:underline;">View created observation</a>';
        bindExternalLinks(statusEl);
      } else {
        statusEl.textContent = '✗ ' + failure.message;
      }
    }

    done++;
    if (generation !== _inatModalGeneration) break;
    fill.style.width = Math.round((done / total) * 100) + '%';
    text.textContent = done + ' / ' + total + ' processed (' + succeeded + ' succeeded)';
  }

  if (owner !== _inatSubmitOwner) return;
  _inatSubmitting = false;
  if (_inatCancelled || generation !== _inatModalGeneration) {
    // Modal is already closed (closeInatModal deferred the queue cleanup
    // to us) — surface the partial result where the user can see it.
    _inatCancelled = false;
    if (inatQueue === queue) inatQueue = [];
    showToast('iNaturalist: submitted ' + succeeded + ' of ' + total + ', cancelled', 'info');
    return;
  }
  btn.textContent = 'Done';
  // Change cancel to Close
  var cancel = document.querySelector('#inatActions .modal-btn-cancel'); if (cancel) cancel.textContent = 'Close';
}
