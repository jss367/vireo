// Classifier model picker and label-file selection.
// Classic page script; load boot.js after all definitions.

// -- Card 2: Classify --
async function loadModels() {
  try {
    var data = await safeFetch('/api/models', {}, { toast: false });
    var div = document.getElementById('modelPicker');
    div.innerHTML = '';
    var downloaded = data.models.filter(function(m) { return m.downloaded; });
    if (downloaded.length === 0) {
      div.innerHTML = '<span style="color:var(--text-muted);">No models downloaded — go to Settings</span>';
    } else {
      downloaded.forEach(function(m) {
        var label = document.createElement('label');
        label.style.cssText = 'display:block;cursor:pointer;padding:2px 0;';
        var cb = document.createElement('input');
        cb.type = 'checkbox';
        cb.className = 'model-checkbox';
        cb.value = m.id;
        cb.dataset.name = m.name;
        cb.dataset.supportsLists = m.supports_label_lists ? '1' : '0';
        cb.style.cssText = 'accent-color:var(--accent);margin-right:4px;';
        cb.onchange = function() { updateReadiness(); updateLabelsPickerState(); schedulePlanRefresh(); };
        if (m.id === data.active_id) cb.checked = true;
        label.appendChild(cb);
        label.appendChild(document.createTextNode(m.name + ' (' + m.model_str + ')'));
        if (m.label_list_tag) {
          var pill = document.createElement('span');
          pill.textContent = m.label_list_tag;
          pill.style.cssText = 'margin-left:6px;padding:1px 6px;border-radius:8px;font-size:10px;color:var(--text-faint);background:var(--surface-2,rgba(255,255,255,0.06));';
          label.appendChild(pill);
        }
        div.appendChild(label);
      });
    }
    updateReadiness();
    updateLabelsPickerState();
  } catch(e) {}
}

// Grey out the labels picker when no selected model uses label lists,
// or show a "BioCLIP-only" note when selection is mixed.
function updateLabelsPickerState() {
  var picker = document.getElementById('labelsPicker');
  var note = document.getElementById('labelsPickerNote');
  if (!picker || !note) return;
  var selected = Array.from(document.querySelectorAll('.model-checkbox:checked'));
  if (selected.length === 0) {
    picker.style.opacity = '';
    picker.style.pointerEvents = '';
    note.style.display = 'none';
    return;
  }
  var supporting = selected.filter(function(cb) { return cb.dataset.supportsLists === '1'; });
  var nonSupporting = selected.filter(function(cb) { return cb.dataset.supportsLists !== '1'; });
  if (supporting.length === 0) {
    // Only fixed-class-set models selected — list selection does nothing.
    picker.style.opacity = '0.4';
    picker.style.pointerEvents = 'none';
    var names = nonSupporting.map(function(cb) { return cb.dataset.name; }).join(', ');
    note.textContent = names + ' uses a fixed class set — label lists do not apply.';
    note.style.display = '';
  } else if (nonSupporting.length > 0) {
    // Mixed: list applies only to the supporting models.
    picker.style.opacity = '';
    picker.style.pointerEvents = '';
    var ignoringNames = nonSupporting.map(function(cb) { return cb.dataset.name; }).join(', ');
    note.textContent = 'Note: ' + ignoringNames + ' ignores label lists (fixed class set).';
    note.style.display = '';
  } else {
    picker.style.opacity = '';
    picker.style.pointerEvents = '';
    note.style.display = 'none';
  }
}

function getSelectedLabelFiles() {
  var files = [];
  document.querySelectorAll('.labels-run-cb:checked').forEach(function(cb) {
    files.push(cb.value);
  });
  return files;
}
