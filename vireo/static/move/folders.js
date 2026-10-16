// The tab strip and the source-folder selects (/api/folders).
// Classic page script; load boot.js after all definitions.

/* ---------- Tab Switching ---------- */
function switchTab(name) {
  document.querySelectorAll('.move-tab').forEach(function(t) { t.classList.remove('active'); });
  document.querySelectorAll('.tab-panel').forEach(function(p) { p.classList.remove('active'); });
  document.getElementById('tab-' + name).classList.add('active');
  // Highlight the correct tab button
  document.querySelectorAll('.move-tab').forEach(function(t) {
    if (t.textContent.toLowerCase().replace(/\s/g, '').indexOf(name) === 0 ||
        (name === 'quick' && t.textContent.indexOf('Quick') >= 0) ||
        (name === 'photo' && t.textContent.indexOf('Photo') >= 0) ||
        (name === 'rules' && t.textContent.indexOf('Rules') >= 0)) {
      t.classList.add('active');
    }
  });
}

/* ---------- Load Folders ---------- */
async function loadFolders() {
  try {
    var data = await safeFetch('/api/folders');
    allFolders = data || [];
    populateFolderSelects();
  } catch (e) {
    console.error('Failed to load folders:', e);
  }
}

function populateFolderSelects() {
  var selects = ['quickFolderSelect', 'photoFolderSelect'];
  selects.forEach(function(id) {
    var sel = document.getElementById(id);
    // Preserve the first default option
    var defaultOpt = sel.options[0];
    sel.innerHTML = '';
    sel.appendChild(defaultOpt);

    allFolders.forEach(function(f) {
      var opt = document.createElement('option');
      opt.value = f.id;
      var count = f.photo_count || 0;
      opt.textContent = f.path + ' (' + count + ' photos)';
      sel.appendChild(opt);
    });
  });

  var requestedFolderId = new URLSearchParams(window.location.search).get('folder_id');
  if (requestedFolderId) {
    var quickSelect = document.getElementById('quickFolderSelect');
    quickSelect.value = requestedFolderId;
    if (quickSelect.value === requestedFolderId) onQuickFolderChange();
  }
}
