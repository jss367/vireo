/* Storage category cards share the selected ordering across data refreshes. */
var storageOverviewCategories = null;

function renderStorageCards(categories) {
  if (categories) storageOverviewCategories = categories;
  if (!storageOverviewCategories) return;
  document.getElementById('storageSort').disabled = false;
  var order = document.getElementById('storageSort').value;
  var sorted = storageOverviewCategories.slice().sort(function(a, b) {
    var byName = a.name.localeCompare(b.name);
    if (order === 'name') return byName;
    var bySize = (a.size || 0) - (b.size || 0);
    return (order === 'size-asc' ? bySize : -bySize) || byName;
  });
  _storageSummary = {};

  var html = '';
  sorted.forEach(function(c) {
    if (c.size === 0 && !c.type && !(c.release && c.count)) return;
    if (c.type) _storageSummary[c.type] = {count: c.count || 0, size: c.size || 0};
    html += '<div class="stat-card">' +
      '<div class="stat-number">' + storageFormatBytes(c.size) + '</div>' +
      '<div class="stat-label">' + storageEscapeHtml(c.name) +
        (c.count != null ? ' (' + c.count.toLocaleString() + ')' : '') +
      '</div>' +
      '<span class="stat-kind ' + c.kind + '">' + c.kindLabel + '</span>';
    if (c.note) {
      html += '<div class="stat-note">' + storageEscapeHtml(c.note) + '</div>';
    }
    if (c.type) {
      html += '<button class="btn-manage" onclick="openStorageModal(\'' + c.type + '\')">Manage</button>';
    } else if (c.release) {
      html += VireoReleaseOriginals.storageButton();
    } else if (c.hfCache) {
      html += '<button class="btn-manage" onclick="openHfCacheModal()">Manage</button>';
    } else if (c.settings) {
      html += '<button class="btn-manage" onclick="window.location.href=\'/settings\'">Manage in Settings</button>';
    }
    html += '</div>';
  });
  document.getElementById('storageGrid').innerHTML = html || '<span style="color:var(--text-ghost);font-size:13px;">No generated storage yet.</span>';
}

function showStorageLoadError() {
  storageOverviewCategories = null;
  document.getElementById('storageSort').disabled = true;
  document.getElementById('storageGrid').innerHTML = '<span style="color:var(--danger);font-size:12px;">Failed to load storage info</span>';
}
