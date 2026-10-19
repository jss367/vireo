// The Manage modal for the thumbnail, preview and embedding caches: listing
// files, selecting, and deleting some or all of them.
// Classic page script.

var _modalType = '';
/** @type {StorageFile[]} */
var _modalFiles = [];
/** Counts and sizes per cache type, from the last overview render (overview.js). @type {Record<string, {count: number, size: number}>} */
var _storageSummary = {};
var STORAGE_MODAL_FILE_LIMIT = 500;
/** @type {Record<string, {title: string, desc: string}>} */
var STORAGE_INFO = {
  thumbnails: {
    title: 'Thumbnails',
    desc: 'Small preview images generated for fast browsing. Deleting these is safe; they regenerate automatically.',
  },
  previews: {
    title: 'Previews',
    desc: 'Larger preview images used for detail views. Deleting these is safe; they regenerate on demand.',
  },
  embeddings: {
    title: 'Label Embeddings',
    desc: 'Cached text embeddings for species labels used during classification.',
  },
};

/** @param {string} type */
async function openStorageModal(type) {
  _modalType = type;
  _modalFiles = [];
  var info = STORAGE_INFO[type] || {title: type, desc: ''};
  /** @type {HTMLElement} */ (document.getElementById('modalTitle')).textContent = 'Manage ' + info.title;
  /** @type {HTMLElement} */ (document.getElementById('modalDesc')).textContent = info.desc;
  /** @type {HTMLElement} */ (document.getElementById('modalLoading')).style.display = 'block';
  /** @type {HTMLElement} */ (document.getElementById('modalLoading')).textContent = 'Loading files...';
  /** @type {HTMLElement} */ (document.getElementById('modalFileList')).style.display = 'none';
  /** @type {HTMLButtonElement} */ (document.getElementById('modalDeleteSelected')).disabled = true;
  /** @type {HTMLElement} */ (document.getElementById('storageModal')).classList.add('open');

  try {
    var url = '/api/storage/files?type=' + encodeURIComponent(type);
    if (type === 'previews' || type === 'thumbnails') {
      url += '&limit=' + STORAGE_MODAL_FILE_LIMIT;
      /** @type {HTMLElement} */ (document.getElementById('modalLoading')).textContent =
        'Loading first ' + STORAGE_MODAL_FILE_LIMIT.toLocaleString() + ' files...';
    }
    var data = /** @type {{files?: StorageFile[], truncated?: boolean}} */ (await safeFetch(url, {}, { toast: false }));
    _modalFiles = data.files || [];
    /** @type {HTMLElement} */ (document.getElementById('modalLoading')).style.display = 'none';
    /** @type {HTMLElement} */ (document.getElementById('modalFileList')).style.display = 'block';

    if (_modalFiles.length === 0) {
      /** @type {HTMLElement} */ (document.getElementById('modalFiles')).innerHTML = '<li style="color:var(--text-ghost);font-size:13px;padding:12px 0;">Cache is empty.</li>';
      /** @type {HTMLButtonElement} */ (document.getElementById('modalDeleteAll')).disabled = true;
      updateSelectedInfo();
      return;
    }
    /** @type {HTMLButtonElement} */ (document.getElementById('modalDeleteAll')).disabled = false;

    var html = '';
    if (data.truncated) {
      var summary = _storageSummary[type] || {count: 0, size: 0};
      var totalCount = summary.count || 0;
      html += '<li style="color:var(--text-ghost);font-size:12px;padding:8px 0;">Showing the first ' +
        _modalFiles.length.toLocaleString() +
        (totalCount ? ' of ' + totalCount.toLocaleString() : '') +
        ' files. Select all shown and Delete Selected affect only this visible list; Delete Entire Cache removes all ' +
        info.title.toLowerCase() + '.</li>';
    }
    _modalFiles.forEach(function(f, /** @type {number} */ i) {
      var displayName = storageEscapeHtml(f.name);
      if (f.meta) {
        displayName = storageEscapeHtml(f.meta.model || 'Unknown model') +
          ' <span style="color:var(--text-faint);">- ' + (f.meta.label_count || '?') + ' labels</span>';
        if (f.meta.created) {
          displayName += ' <span style="color:var(--text-ghost);">- ' + storageEscapeHtml(f.meta.created.split('T')[0]) + '</span>';
        }
      }
      html += '<li class="file-item">' +
        '<input type="checkbox" id="fc' + i + '">' +
        '<label for="fc' + i + '">' + displayName + '</label>' +
        '<span class="file-size">' + storageFormatBytes(f.size) + '</span>' +
      '</li>';
    });
    /** @type {HTMLElement} */ (document.getElementById('modalFiles')).innerHTML = html;
    updateSelectedInfo();
  } catch(e) {
    /** @type {HTMLElement} */ (document.getElementById('modalLoading')).innerHTML = '<span style="color:var(--danger);">Failed to load files.</span>';
  }
}

function closeStorageModal() {
  /** @type {HTMLElement} */ (document.getElementById('storageModal')).classList.remove('open');
}

/** @param {boolean} checked */
function selectAllFiles(checked) {
  var boxes = modalFileBoxes();
  boxes.forEach(function(cb) { cb.checked = checked; });
  updateSelectedInfo();
}

/** The file list's checkboxes, in _modalFiles order. */
function modalFileBoxes() {
  return /** @type {NodeListOf<HTMLInputElement>} */ (document.querySelectorAll('#modalFiles input[type=checkbox]'));
}

function updateSelectedInfo() {
  var boxes = modalFileBoxes();
  var count = 0, size = 0;
  boxes.forEach(function(cb, i) {
    var file = _modalFiles[i];
    if (cb.checked && file) { count++; size += file.size; }
  });
  /** @type {HTMLElement} */ (document.getElementById('modalSelectedInfo')).textContent = count > 0
    ? count + ' selected (' + storageFormatBytes(size) + ')'
    : '';
  /** @type {HTMLButtonElement} */ (document.getElementById('modalDeleteSelected')).disabled = count === 0;
}

async function deleteSelectedFiles() {
  var boxes = modalFileBoxes();
  /** @type {string[]} */
  var names = [];
  boxes.forEach(function(cb, i) {
    var file = _modalFiles[i];
    if (cb.checked && file) names.push(file.name);
  });
  if (names.length === 0) return;
  if (!confirm('Delete ' + names.length + ' file(s)?')) return;
  try {
    await safeFetch('/api/storage/delete-files', {
      method: 'POST',
      headers: {'Content-Type': 'application/json'},
      body: JSON.stringify({type: _modalType, files: names}),
    });
  } catch(e) {}
  loadStoragePanel();
  loadPreviewCacheStatus();
  loadEmbeddingCache();
  openStorageModal(_modalType);
}

async function deleteAllFiles() {
  var info = STORAGE_INFO[_modalType] || {title: _modalType};
  var summary = _storageSummary[_modalType] || {count: 0, size: 0};
  var count = summary.count || _modalFiles.length;
  if (!confirm('Delete all ' + count.toLocaleString() + ' ' + info.title.toLowerCase() + '? They will regenerate as needed.')) return;
  try {
    await safeFetch('/api/storage/clear', {
      method: 'POST',
      headers: {'Content-Type': 'application/json'},
      body: JSON.stringify({type: _modalType}),
    });
  } catch(e) {}
  loadStoragePanel();
  loadPreviewCacheStatus();
  loadEmbeddingCache();
  openStorageModal(_modalType);
}
