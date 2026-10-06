// Toolbar and tabs: confidence, model, sort, photo size, stats, status tabs, and the Accept All button.
// Classic page script; load boot.js after all definitions.

function setMinConfidence(val) {
  minConfidence = val;
  renderAll();
}

/* ---------- Render ---------- */
function renderAll() {
  renderModelFilter();
  renderFingerprintFilterPill();
  renderPhotoFilterPill();
  renderStats();
  renderTabs();
  renderButtons();
  renderGrid();
  checkPendingSync();
}

function renderModelFilter() {
  var sel = document.getElementById('modelFilter');
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

function switchModel(model) {
  currentModel = model;
  renderAll();
}

function changeSort(sort) {
  currentSort = sort;
  renderAll();
}

function renderStats() {
  var el = document.getElementById('stats');
  if (mode === 'review') {
    var pending = predictions.filter(function(p) { return p.status === 'pending'; }).length;
    var accepted = predictions.filter(function(p) { return p.status === 'accepted'; }).length;
    el.innerHTML =
      '<span class="stat new-count">' + pending + ' Pending</span>' +
      '<span class="stat accepted-count">' + accepted + ' Accepted</span>' +
      '<span class="stat">' + predictions.length + ' Total</span>';
  } else {
    // Browse mode means zero predictions loaded — the grid's empty state
    // explains the situation, so there's no count worth showing here.
    el.textContent = '';
  }
}

function renderTabs() {
  var tabsEl = document.getElementById('tabs');
  if (mode !== 'review') {
    tabsEl.innerHTML = '';
    return;
  }
  var counts = { all: predictions.length };
  counts.pending = predictions.filter(function(p) { return p.status === 'pending'; }).length;
  counts.accepted = predictions.filter(function(p) { return p.status === 'accepted'; }).length;
  counts.rejected = predictions.filter(function(p) { return p.status === 'rejected'; }).length;

  var tabs = [
    { id: 'all', label: 'All' },
    { id: 'pending', label: 'Pending' },
    { id: 'accepted', label: 'Accepted' },
    { id: 'rejected', label: 'Rejected' },
  ];
  tabsEl.innerHTML = tabs.map(function(t) {
    var cls = currentTab === t.id ? 'tab active' : 'tab';
    return '<div class="' + cls + '" onclick="switchTab(\'' + t.id + '\')">' +
      t.label + '<span class="count">' + (counts[t.id] || 0) + '</span></div>';
  }).join('');
}

function switchTab(tab) {
  currentTab = tab;
  renderAll();
}

function renderButtons() {
  var acceptAllBtn = document.getElementById('acceptAllBtn');
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
function updateThumbSize(val) {
  var parsed = parseInt(val, 10);
  if (isNaN(parsed)) parsed = 400;
  thumbSize = Math.max(200, Math.min(600, parsed));

  var slider = document.getElementById('thumbSizeSlider');
  if (slider) slider.value = String(thumbSize);
  var value = document.getElementById('thumbSizeVal');
  if (value) value.textContent = thumbSize + 'px';
  var grid = document.getElementById('grid');
  if (grid) grid.style.setProperty('--card-width', thumbSize + 'px');

}
