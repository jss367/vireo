// The Export Life List dialog: formats, CSV columns, and the download.
// Classic page script; load boot.js after all definitions.

var LIFE_LIST_EXPORT_COLUMNS = {
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
var lifeListExportColumnSelections = {species: null, photos: null};

function openExportModal() {
  var modal = document.getElementById('exportModal');
  var status = document.getElementById('exportStatus');
  if (status) status.textContent = '';
  updateExportControls();
  modal.classList.add('open');
  setTimeout(function() {
    var input = document.getElementById('exportFormat');
    if (input) input.focus();
  }, 0);
}

function closeExportModal() {
  document.getElementById('exportModal').classList.remove('open');
}

function exportColumnSelection(detail) {
  if (!lifeListExportColumnSelections[detail]) {
    var selection = {};
    LIFE_LIST_EXPORT_COLUMNS[detail].forEach(function(column) {
      selection[column[0]] = true;
    });
    lifeListExportColumnSelections[detail] = selection;
  }
  return lifeListExportColumnSelections[detail];
}

function selectedExportColumns(detail) {
  var selection = exportColumnSelection(detail);
  return LIFE_LIST_EXPORT_COLUMNS[detail]
    .map(function(column) { return column[0]; })
    .filter(function(column) { return selection[column]; });
}

function updateExportLocationsVisibility() {
  var format = document.getElementById('exportFormat').value;
  var detail = document.getElementById('exportDetail').value;
  var showLocations = format === 'json' || format === 'txt';
  if (format === 'csv') {
    showLocations = selectedExportColumns(detail).indexOf('locations') !== -1;
  }
  document.getElementById('exportLocationsWrap').style.display = showLocations ? '' : 'none';
}

function renderExportColumns(detail) {
  var container = document.getElementById('exportColumns');
  var selection = exportColumnSelection(detail);
  container.textContent = '';
  LIFE_LIST_EXPORT_COLUMNS[detail].forEach(function(column) {
    var label = document.createElement('label');
    label.className = 'export-column-option';
    var checkbox = document.createElement('input');
    checkbox.type = 'checkbox';
    checkbox.value = column[0];
    checkbox.checked = selection[column[0]];
    checkbox.addEventListener('change', function() {
      selection[column[0]] = checkbox.checked;
      document.getElementById('exportStatus').textContent = '';
      updateExportLocationsVisibility();
    });
    var text = document.createElement('span');
    text.textContent = column[1];
    label.appendChild(checkbox);
    label.appendChild(text);
    container.appendChild(label);
  });
}

function updateExportControls() {
  var format = document.getElementById('exportFormat').value;
  var detailWrap = document.getElementById('exportDetailWrap');
  var photosWrap = document.getElementById('exportPhotosWrap');
  var columnsWrap = document.getElementById('exportColumnsWrap');
  var detail = document.getElementById('exportDetail').value;
  detailWrap.style.display = format === 'csv' ? '' : 'none';
  photosWrap.style.display = (
    format === 'files' || (format === 'csv' && detail === 'photos')
  ) ? '' : 'none';
  columnsWrap.style.display = format === 'csv' ? '' : 'none';
  if (format === 'csv') renderExportColumns(detail);
  updateExportLocationsVisibility();
}

function downloadLifeListExport() {
  var format = document.getElementById('exportFormat').value;
  var detail = document.getElementById('exportDetail').value;
  var photos = document.getElementById('exportPhotos').value;
  var includeLocations = document.getElementById('exportLocations').checked ? '1' : '0';
  var columns = format === 'csv' ? selectedExportColumns(detail) : [];
  if (format === 'csv' && !columns.length) {
    document.getElementById('exportStatus').textContent = 'Select at least one column.';
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
