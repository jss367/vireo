// The Export Life List dialog: formats, CSV columns, and the download.

import { state } from './state.js';

/** @type {Record<'species'|'photos',[string,string][]>} */ export const LIFE_LIST_EXPORT_COLUMNS = {
  species: [
    ['number', 'Life-list number'],
    ['species', 'Displayed species name'],
    ['scientific_name', 'Scientific name'],
    ['common_name', 'Common name'],
    ['photo_count', 'Photo count'],
    ['first_seen', 'First seen date'],
    ['last_seen', 'Last seen date'],
    ['locations', 'Locations'],
    ['best_photo_id', 'Representative photo ID'],
    ['best_filename', 'Representative filename'],
  ],
  photos: [
    ['number', 'Life-list number'],
    ['species', 'Displayed species name'],
    ['scientific_name', 'Scientific name'],
    ['common_name', 'Common name'],
    ['photo_id', 'Photo ID'],
    ['filename', 'Filename'],
    ['timestamp', 'Date and time'],
    ['is_life_list_photo', 'Life-list photo'],
    ['quality_score', 'Quality score'],
    ['locations', 'Locations'],
  ],
};

export function openExportModal() {
  var modal = /** @type {HTMLElement} */ (document.getElementById('exportModal'));
  var status = /** @type {HTMLElement} */ (document.getElementById('exportStatus'));
  if (status) status.textContent = '';
  updateExportControls();
  modal.classList.add('open');
  setTimeout(function() {
    var input = /** @type {HTMLSelectElement} */ (document.getElementById('exportFormat'));
    if (input) input.focus();
  }, 0);
}

export function closeExportModal() {
  /** @type {HTMLElement} */ (document.getElementById('exportModal')).classList.remove('open');
}

/** @param {'species'|'photos'} detail */ export function exportColumnSelection(detail) {
  if (!state.lifeListExportColumnSelections[detail]) {
    /** @type {Record<string,boolean>} */ var selection = {};
    LIFE_LIST_EXPORT_COLUMNS[detail].forEach(function(column) {
      selection[column[0]] = true;
    });
    state.lifeListExportColumnSelections[detail] = selection;
  }
  return state.lifeListExportColumnSelections[detail];
}

/** @param {'species'|'photos'} detail */ export function selectedExportColumns(detail) {
  var selection = exportColumnSelection(detail);
  return LIFE_LIST_EXPORT_COLUMNS[detail]
    .map(function(column) { return column[0]; })
    .filter(function(column) { return selection[column]; });
}

export function updateExportLocationsVisibility() {
  var format = /** @type {HTMLSelectElement} */ (document.getElementById('exportFormat')).value;
  var detail = /** @type {HTMLSelectElement & {value:'species'|'photos'}} */ (document.getElementById('exportDetail')).value;
  var showLocations = format === 'json' || format === 'txt';
  if (format === 'csv') {
    showLocations = selectedExportColumns(detail).indexOf('locations') !== -1;
  }
  /** @type {HTMLElement} */ (document.getElementById('exportLocationsWrap')).style.display = showLocations ? '' : 'none';
}

/** @param {'species'|'photos'} detail */ export function renderExportColumns(detail) {
  var container = /** @type {HTMLElement} */ (document.getElementById('exportColumns'));
  var selection = exportColumnSelection(detail);
  container.textContent = '';
  LIFE_LIST_EXPORT_COLUMNS[detail].forEach(function(column) {
    var label = document.createElement('label');
    label.className = 'export-column-option';
    var checkbox = document.createElement('input');
    checkbox.type = 'checkbox';
    checkbox.value = column[0];
    checkbox.checked = !!selection[column[0]];
    checkbox.addEventListener('change', function() {
      selection[column[0]] = checkbox.checked;
      /** @type {HTMLElement} */ (document.getElementById('exportStatus')).textContent = '';
      updateExportLocationsVisibility();
    });
    var text = document.createElement('span');
    text.textContent = column[1];
    label.appendChild(checkbox);
    label.appendChild(text);
    container.appendChild(label);
  });
}

export function updateExportControls() {
  var format = /** @type {HTMLSelectElement} */ (document.getElementById('exportFormat')).value;
  var detailWrap = /** @type {HTMLElement} */ (document.getElementById('exportDetailWrap'));
  var photosWrap = /** @type {HTMLElement} */ (document.getElementById('exportPhotosWrap'));
  var columnsWrap = /** @type {HTMLElement} */ (document.getElementById('exportColumnsWrap'));
  var detail = /** @type {HTMLSelectElement & {value:'species'|'photos'}} */ (document.getElementById('exportDetail')).value;
  detailWrap.style.display = format === 'csv' ? '' : 'none';
  photosWrap.style.display = (
    format === 'files' || (format === 'csv' && detail === 'photos')
  ) ? '' : 'none';
  columnsWrap.style.display = format === 'csv' ? '' : 'none';
  if (format === 'csv') renderExportColumns(detail);
  updateExportLocationsVisibility();
}

export function downloadLifeListExport() {
  var format = /** @type {HTMLSelectElement} */ (document.getElementById('exportFormat')).value;
  var detail = /** @type {HTMLSelectElement & {value:'species'|'photos'}} */ (document.getElementById('exportDetail')).value;
  var photos = /** @type {HTMLSelectElement} */ (document.getElementById('exportPhotos')).value;
  var includeLocations = /** @type {HTMLInputElement} */ (document.getElementById('exportLocations')).checked ? '1' : '0';
  var columns = format === 'csv' ? selectedExportColumns(detail) : [];
  if (format === 'csv' && !columns.length) {
    /** @type {HTMLElement} */ (document.getElementById('exportStatus')).textContent = 'Select at least one column.';
    return;
  }
  var params = new URLSearchParams({
    format: format,
    detail: detail,
    include_locations: includeLocations,
  });
  if (format === 'csv') params.set('columns', columns.join(','));
  if (format === 'files' || (format === 'csv' && detail === 'photos')) {
    params.set('photos', photos);
  }
  var link = document.createElement('a');
  link.href = '/api/life-list/export?' + params.toString();
  // Explicitly mark this as a download. WKWebView otherwise treats showable
  // formats such as CSV as a normal page navigation even when the response
  // carries Content-Disposition: attachment.
  link.download = '';
  document.body.appendChild(link);
  link.click();
  link.remove();
  closeExportModal();
}
