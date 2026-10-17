// Folder preview: fetching, rendering, thumbnails, and per-file selection.
// Classic page script; load boot.js after all definitions.

// -- Preview panel state --
/** @type {PipelinePreview | null} */ var _previewData = null;       // response from the preview endpoint
/** @type {Record<string,boolean>} */ var _previewSelected = {};     // { filePath: true/false }
var _previewLoading = false;
/** @type {AbortController | null} */ var _previewAbort = null;      // AbortController for in-flight request
/** @type {(() => void) | null} */ var _thumbSchedulerCancel = null; // cancels the current thumbnail pump on re-render

function fetchFolderPreview() {
  if (_previewAbort) _previewAbort.abort();

  // Folder scope has no per-file preview: the run covers the selected
  // subtrees and the readiness panels report per-stage work honestly.
  if (_sourceMode === 'folders') {
    showPreviewPlaceholder();
    return;
  }

  // Collection mode: fetch from collection-preview endpoint
  if (_sourceMode === 'collection') {
    var collId = /** @type {HTMLSelectElement} */ (document.getElementById('collectionPicker')).value;
    if (!collId) {
      showPreviewPlaceholder();
      return;
    }
    showPreviewLoading();
    _previewAbort = new AbortController();
    fetch('/api/import/collection-preview', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ collection_id: parseInt(collId) }),
      signal: _previewAbort.signal,
    })
      .then(async function(r) {
        /** @type {PipelinePreview} */ var data = await r.json();
        if (!r.ok || data.error) throw new Error(data.error || 'Could not load collection preview');
        return data;
      })
      .then(function(data) {
        _previewData = data;
        _previewSelected = {};
        data.files.forEach(function(f) {
          _previewSelected[f.path] = !f.duplicate;
        });
        renderPreview();
      })
      .catch(function(err) {
        if (err.name === 'AbortError') return;
        showPreviewError('Failed to load preview: ' + err.message);
      });
    return;
  }

  showPreviewPlaceholder();
}

function showPreviewPlaceholder() {
  /** @type {HTMLElement} */ (document.getElementById('previewPlaceholder')).style.display = '';
  /** @type {HTMLElement} */ (document.getElementById('previewContent')).style.display = 'none';
  /** @type {HTMLElement} */ (document.getElementById('previewLoading')).style.display = 'none';
  /** @type {HTMLElement} */ (document.getElementById('previewError')).style.display = 'none';
  _previewLoading = false;
  _previewData = null;
  _previewSelected = {};
  if (_thumbSchedulerCancel) { _thumbSchedulerCancel(); _thumbSchedulerCancel = null; }
  updateSourceSummary();
}

function showPreviewLoading() {
  /** @type {HTMLElement} */ (document.getElementById('previewPlaceholder')).style.display = 'none';
  /** @type {HTMLElement} */ (document.getElementById('previewContent')).style.display = 'none';
  /** @type {HTMLElement} */ (document.getElementById('previewLoading')).style.display = '';
  /** @type {HTMLElement} */ (document.getElementById('previewError')).style.display = 'none';
  _previewLoading = true;
  _previewData = null;
  _previewSelected = {};
  _planFetchSeq++;
  _pipelinePlan = null;
  updateSamVariantWarning(null);
  refreshPipelineUI();
  updateSourceSummary();
  if (_thumbSchedulerCancel) { _thumbSchedulerCancel(); _thumbSchedulerCancel = null; }
}

/** @param {string} msg */
function showPreviewError(msg) {
  /** @type {HTMLElement} */ (document.getElementById('previewPlaceholder')).style.display = 'none';
  /** @type {HTMLElement} */ (document.getElementById('previewContent')).style.display = 'none';
  /** @type {HTMLElement} */ (document.getElementById('previewLoading')).style.display = 'none';
  var errEl = /** @type {HTMLElement} */ (document.getElementById('previewError'));
  errEl.style.display = '';
  errEl.textContent = msg;
  _previewLoading = false;
  _previewData = null;
  _previewSelected = {};
  if (_thumbSchedulerCancel) { _thumbSchedulerCancel(); _thumbSchedulerCancel = null; }
  updateSourceSummary();
}

function renderPreview() {
  /** @type {HTMLElement} */ (document.getElementById('previewPlaceholder')).style.display = 'none';
  /** @type {HTMLElement} */ (document.getElementById('previewLoading')).style.display = 'none';
  /** @type {HTMLElement} */ (document.getElementById('previewError')).style.display = 'none';
  /** @type {HTMLElement} */ (document.getElementById('previewContent')).style.display = '';
  _previewLoading = false;

  const data = _previewData;
  if (!data) return;

  // Summary bar
  var summary = /** @type {HTMLElement} */ (document.getElementById('previewSummary'));
  var sizeStr = formatBytes(data.total_size);
  var typeStrs = Object.keys(data.type_breakdown).map(function(ext) {
    return data.type_breakdown[ext] + ' ' + ext.replace('.', '').toUpperCase();
  });
  summary.innerHTML =
    '<span class="stat"><span class="stat-value">' + data.total_count + '</span> photos</span>' +
    '<span class="stat"><span class="stat-value">' + sizeStr + '</span></span>' +
    '<span class="stat">' + typeStrs.join(' &middot; ') + '</span>';

  // Group files by subfolder
  /** @type {Record<string,PipelinePhoto[]>} */ var groups = {};
  data.files.forEach(function(f) {
    var key = f.subfolder || '';
    if (!groups[key]) groups[key] = [];
    groups[key]?.push(f);
  });

  // Render grid
  var grid = /** @type {HTMLElement} */ (document.getElementById('previewGrid'));
  grid.innerHTML = '';

  Object.keys(groups).sort().forEach(function(subfolder) {
    var files = groups[subfolder] || [];
    var groupEl = document.createElement('div');
    groupEl.className = 'preview-folder-group';

    // Folder header with checkbox
    var header = document.createElement('div');
    header.className = 'preview-folder-header';
    var folderCheck = document.createElement('input');
    folderCheck.type = 'checkbox';
    folderCheck.checked = files.some(function(f) { return _previewSelected[f.path]; });
    folderCheck.style.accentColor = 'var(--accent)';
    folderCheck.onchange = function() {
      files.forEach(function(f) { _previewSelected[f.path] = folderCheck.checked; });
      renderPreview();
    };
    header.appendChild(folderCheck);
    header.appendChild(document.createTextNode(' ' + subfolder + ' (' + files.length + ')'));
    groupEl.appendChild(header);

    // Thumbnail grid
    var thumbsEl = document.createElement('div');
    thumbsEl.className = 'preview-thumbs';
    files.forEach(function(f) {
      var thumb = document.createElement('div');
      thumb.className = 'preview-thumb skeleton' + (f.duplicate ? ' duplicate' : '');
      thumb.dataset.path = f.path;
      if (f.thumb_url) thumb.dataset.thumbUrl = f.thumb_url;

      var check = document.createElement('input');
      check.type = 'checkbox';
      check.className = 'thumb-check';
      check.checked = !!_previewSelected[f.path];
      check.onchange = function(e) {
        e.stopPropagation();
        _previewSelected[f.path] = check.checked;
        updatePreviewCounts();
        // Per-file toggles change the import scope (which paths the next
        // run will touch), so refresh the plan — folder/select-all paths
        // already get a refresh via renderPreview(); this one does not.
        schedulePlanRefresh();
      };
      thumb.appendChild(check);

      var img = document.createElement('img');
      img.alt = f.filename;
      thumb.appendChild(img);

      thumbsEl.appendChild(thumb);
    });
    groupEl.appendChild(thumbsEl);
    grid.appendChild(groupEl);
  });

  // Set up IntersectionObserver for lazy thumbnail loading
  setupThumbnailObserver();
  updatePreviewCounts();
  updateSourceSummary();
  // Preview content drives the run scope (which paths are selected).
  // Refresh debounced — folder-level and select-all checkboxes call us in
  // bursts, and we'd rather not POST to /api/pipeline/plan on every click.
  schedulePlanRefresh();
}

function setupThumbnailObserver() {
  // Cancel any previous scheduler so its done-callbacks don't keep pumping
  // on elements that were removed from the DOM by a re-render.
  if (_thumbSchedulerCancel) { _thumbSchedulerCancel(); _thumbSchedulerCancel = null; }

  var grid = /** @type {HTMLElement} */ (document.getElementById('previewGrid'));
  var pendingQueue = Array.from(/** @type {NodeListOf<HTMLElement>} */ (grid.querySelectorAll('.preview-thumb.skeleton')));
  var visibleSet = new Set();
  var inFlight = 0;
  var CONCURRENCY = 4;
  var cancelled = false;

  // Register a cancel function so the next call to setupThumbnailObserver can
  // stop this scheduler before it finishes draining its queue.
  _thumbSchedulerCancel = function() {
    cancelled = true;
    pendingQueue.length = 0;
    observer.disconnect();
  };

  /** @param {HTMLElement} el */
  function dispatch(el) {
    var path = el.dataset.path || '';
    var img = /** @type {HTMLImageElement} */ (el.querySelector('img'));
    inFlight++;
    var done = function() {
      el.classList.remove('skeleton');
      inFlight--;
      if (!cancelled) pump();
    };
    img.onload = done;
    img.onerror = done;
    img.src = el.dataset.thumbUrl || ('/api/import/folder-preview/thumbnail?path=' + encodeURIComponent(path));
  }

  function pump() {
    while (inFlight < CONCURRENCY && pendingQueue.length > 0) {
      // Prefer a visible thumb; fall back to the top of the queue.
      var idx = 0;
      for (var i = 0; i < pendingQueue.length; i++) {
        if (visibleSet.has(pendingQueue[i])) { idx = i; break; }
      }
      var next = pendingQueue.splice(idx, 1)[0]; if (next) dispatch(next);
    }
  }

  var observer = new IntersectionObserver(function(entries) {
    entries.forEach(function(entry) {
      if (entry.isIntersecting) {
        visibleSet.add(entry.target);
      } else {
        visibleSet.delete(entry.target);
      }
    });
    if (!cancelled) pump();
  }, { root: grid, rootMargin: '200px' });

  pendingQueue.forEach(function(t) { observer.observe(t); });
  // Defer the initial pump so IntersectionObserver has time to fire its initial
  // callbacks and populate visibleSet before we start dispatching. If pump() is
  // called synchronously here, visibleSet is always empty on the first pass and
  // the first CONCURRENCY requests go to top-of-list offscreen thumbs instead of
  // the currently-visible ones, defeating the visible-first scheduling.
  setTimeout(function() { if (!cancelled) pump(); }, 0);
}

function updatePreviewCounts() {
  if (!_previewData) return;
  var selected = 0;
  var total = _previewData.files.length;
  _previewData.files.forEach(function(f) {
    if (_previewSelected[f.path]) selected++;
  });
  var countEl = /** @type {HTMLElement} */ (document.getElementById('previewSelectedCount'));
  if (selected === total) {
    countEl.textContent = '';
  } else {
    countEl.textContent = selected + ' of ' + total + ' selected';
  }
  // Update select-all checkbox
  var chkAll = /** @type {HTMLInputElement} */ (document.getElementById('chkSelectAll'));
  chkAll.checked = selected === total;
  chkAll.indeterminate = selected > 0 && selected < total;
}

function toggleSelectAll() {
  var checked = /** @type {HTMLInputElement} */ (document.getElementById('chkSelectAll')).checked;
  if (_previewData) {
    _previewData.files.forEach(function(f) { _previewSelected[f.path] = checked; });
    renderPreview();
  }
}

