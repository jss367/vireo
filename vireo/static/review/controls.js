// Toolbar and tabs: confidence, model, sort, photo size, stats, status tabs, and the Accept All button.
// Classic page script; load boot.js after all definitions.

/** @param {string} val */ function setMinConfidence(val) {
  minConfidence = Number(val);
  renderAll();
}

/* ---------- Render ---------- */
function renderAll() {
  // Nothing below changes a row or a filter, so the steps share one
  // filtered, sorted and indexed copy of the rows (see reviewVisibleRows).
  reviewVisibleRowsCache = new Map();
  try {
    renderModelFilter();
    renderFingerprintFilterPill();
    renderPhotoFilterPill();
    renderStats();
    renderTabs();
    renderButtons();
    renderGrid();
    checkPendingSync();
    renderReviewQueue();
  } finally {
    reviewVisibleRowsCache = null;
  }
}

function renderModelFilter() {
  var sel = /** @type {HTMLSelectElement} */ (document.getElementById('modelFilter'));
  if (availableModels.length <= 1) {
    sel.style.display = 'none';
    return;
  }
  sel.style.display = '';
  sel.innerHTML = '<option value="all">All models (' + availableModels.length + ')</option>';
  availableModels.forEach(function(m) {
    var count = predictions.filter(function(p) { return p.model === m; }).length;
    var selected = currentModel === m ? ' selected' : '';
    sel.innerHTML += '<option value="' + escapeAttr(m) + '"' + selected + '>' + escapeHtml(m) + ' (' + count + ')</option>';
  });
}

/** @param {string} model */ function switchModel(model) {
  currentModel = model;
  renderAll();
}

/** @param {string} sort */ function changeSort(sort) {
  currentSort = sort;
  renderAll();
}

function renderStats() {
  if (mode !== 'review') return;
  // The tabs carry every count; the heading and the window title name the
  // one that says how much is left to do.
  var title = 'Review Predictions (' + (reviewCardCounts().pending || 0) + ' pending cards)';
  /** @type {HTMLElement} */ (document.getElementById('title')).textContent = title;
  document.title = 'Vireo - ' + title;
}

function renderTabs() {
  var tabsEl = /** @type {HTMLElement} */ (document.getElementById('tabs'));
  if (mode !== 'review') {
    tabsEl.innerHTML = '';
    return;
  }
  var counts = reviewCardCounts();

  var tabs = [
    { id: 'all', label: 'All' },
    { id: 'pending', label: 'Pending' },
    { id: 'accepted', label: 'Accepted' },
    { id: 'rejected', label: 'Rejected' },
  ];
  tabsEl.innerHTML = tabs.map(function(t) {
    var cls = currentTab === t.id ? 'tab active' : 'tab';
    var count = counts[t.id] || 0;
    return '<div class="' + cls + '" data-tab="' + t.id + '" title="' + count + ' cards" onclick="switchTab(\'' + t.id + '\')">' +
      t.label + '<span class="count">' + count + '</span></div>';
  }).join('');
}

/** @param {string} tab */ function switchTab(tab) {
  currentTab = tab;
  renderAll();
}

function renderButtons() {
  var acceptAllBtn = /** @type {HTMLButtonElement} */ (document.getElementById('acceptAllBtn'));
  if (_predictionsReloading) {
    acceptAllBtn.style.display = '';
    acceptAllBtn.disabled = true;
    acceptAllBtn.textContent = 'Reloading…';
    return;
  }
  acceptAllBtn.disabled = false;
  var pending = visiblePendingCards().length;
  if (pending > 0) {
    acceptAllBtn.style.display = '';
    acceptAllBtn.textContent = 'Accept All (' + pending + ')';
  } else {
    acceptAllBtn.style.display = 'none';
  }
}

/* ---------- Settings ---------- */
/** @param {string} val */ function updateThumbSize(val) {
  var parsed = parseInt(val, 10);
  if (isNaN(parsed)) parsed = 400;
  thumbSize = Math.max(200, Math.min(600, parsed));

  var slider = /** @type {HTMLInputElement} */ (document.getElementById('thumbSizeSlider'));
  if (slider) slider.value = String(String(thumbSize));
  var value = /** @type {HTMLElement} */ (document.getElementById('thumbSizeVal'));
  if (value) value.textContent = thumbSize + 'px';
  var grid = /** @type {HTMLElement} */ (document.getElementById('grid'));
  if (grid) grid.style.setProperty('--card-width', thumbSize + 'px');

}
