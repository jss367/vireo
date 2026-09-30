// ---------- External Editors list ----------
// Source-of-truth for the editor list in settings UI. Populated by
// loadConfig() and read back on save via collectExternalEditors().
var _editorsState = [];

function renderExternalEditors() {
  var container = document.getElementById('cfgExternalEditorsList');
  if (!container) return;
  container.innerHTML = '';
  if (_editorsState.length === 0) {
    var empty = document.createElement('div');
    empty.style.cssText = 'color:var(--text-dim);font-size:12px;font-style:italic;';
    empty.textContent = 'No editors configured. Photos will open in your OS default app.';
    container.appendChild(empty);
    return;
  }
  _editorsState.forEach(function(ed, i) {
    var row = document.createElement('div');
    row.style.cssText = 'display:flex;align-items:center;gap:6px;';
    var inputCss = 'background:var(--bg-input);color:var(--text-primary);border:1px solid var(--border-secondary);border-radius:4px;padding:6px 10px;font-size:12px;';

    var nameInput = document.createElement('input');
    nameInput.type = 'text';
    nameInput.placeholder = 'Display name (e.g. Lightroom)';
    nameInput.value = ed.name || '';
    nameInput.style.cssText = inputCss + 'width:160px;';
    nameInput.addEventListener('input', function() {
      _editorsState[i].name = nameInput.value;
      saveConfig();
    });

    var pathInput = document.createElement('input');
    pathInput.type = 'text';
    pathInput.placeholder = window.VIREO_EDITOR_PATH_PLACEHOLDER || '/usr/bin/darktable';
    pathInput.value = ed.path || '';
    pathInput.style.cssText = inputCss + 'flex:1;min-width:240px;font-family:monospace;';
    pathInput.addEventListener('input', function() {
      _editorsState[i].path = pathInput.value;
      saveConfig();
    });

    var removeBtn = document.createElement('button');
    removeBtn.type = 'button';
    removeBtn.textContent = '×';
    removeBtn.title = 'Remove';
    removeBtn.style.cssText = 'background:var(--bg-tertiary);color:var(--text-dim);border:1px solid var(--border-secondary);border-radius:4px;width:28px;height:28px;font-size:16px;cursor:pointer;line-height:1;';
    removeBtn.addEventListener('click', function() {
      _editorsState.splice(i, 1);
      renderExternalEditors();
      saveConfig();
    });

    row.appendChild(nameInput);
    row.appendChild(pathInput);
    row.appendChild(removeBtn);
    container.appendChild(row);
  });
}

function addExternalEditor() {
  _editorsState.push({ name: '', path: '' });
  renderExternalEditors();
  // Defer save until the user has typed something. Empty entries get filtered
  // out by collectExternalEditors() anyway, but a save here would just be
  // a no-op write that triggers extra disk churn.
}

function collectExternalEditors() {
  // Drop entries with empty paths; default the display name to the basename
  // of the path when the user didn't fill one in. Mirrors what the backend
  // does in cfg.get_editors() so what's shown matches what's used.
  // Defense-in-depth: loadConfig() already coerces _editorsState entries to
  // strings, but if any later code path stored a non-string (or a future edit
  // skips that load step), calling .trim() directly here would TypeError and
  // block every settings autosave until the malformed entry is repaired.
  var asStr = function(v) { return typeof v === 'string' ? v.trim() : ''; };
  return _editorsState
    .map(function(e) { return { name: asStr(e && e.name), path: asStr(e && e.path) }; })
    .filter(function(e) { return e.path.length > 0; })
    .map(function(e) {
      if (!e.name) {
        var parts = e.path.replace(/\/+$/, '').split('/');
        e.name = parts[parts.length - 1] || 'Editor';
      }
      return e;
    });
}
