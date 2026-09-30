// ---- Quick filters (filter-bar button row) ---------------------------------
// Stored entries are {id, label, group, rules}; `rules` is an ordinary filter
// rule node, the same JSON the filter bar's rule builder and saved collections
// use. vireo/filter_shortcuts.py validates and normalizes whatever we save.
var FILTER_SHORTCUT_DEFAULTS = [];   // built-ins, from /api/filters/shortcuts
// Mirrors filter_shortcuts.MAX_SHORTCUTS: the server refuses a longer list,
// so the form stops here rather than showing a row that never saves.
var MAX_FILTER_SHORTCUTS = 24;
// Mirrors filter_shortcuts.MAX_LABEL_LEN. Normalization truncates a longer
// label, so the form has to stop at the same place — otherwise the row keeps
// showing text the filter bar will never render.
var MAX_SHORTCUT_LABEL = 40;
var _filterShortcutsState = [];
var _filterFieldSpecs = null;      // key -> spec from /api/filters/fields
var _shortcutLabelEdited = false;  // stop auto-filling once the user types

// Ops the add-form can build a single value for. `between` needs two inputs
// and `under` a folder picker; both stay in the filter bar's own rule builder.
var SHORTCUT_FORM_OPS = ['is', 'is not', 'contains', 'not_contains', 'starts_with',
                         'ends_with', '>=', '<=', '>', '<', 'recent'];

function shortcutFieldOptions(spec) {
  return (spec.ops || []).filter(function(op) { return SHORTCUT_FORM_OPS.indexOf(op) >= 0; });
}

function loadFilterShortcuts(list) {
  _filterShortcutsState = (Array.isArray(list) ? list : []).filter(function(entry) {
    return entry && typeof entry === 'object' && entry.rules;
  }).map(function(entry) {
    return {
      id: typeof entry.id === 'string' ? entry.id : '',
      label: typeof entry.label === 'string' ? entry.label : '',
      group: typeof entry.group === 'string' ? entry.group : '',
      rules: entry.rules,
    };
  });
  renderFilterShortcuts();
  if (_filterFieldSpecs) return;
  safeFetch('/api/filters/fields', {}, { toast: false }).then(function(data) {
    _filterFieldSpecs = {};
    ((data && data.fields) || []).forEach(function(f) { _filterFieldSpecs[f.key] = f; });
    buildShortcutFieldSelect();
    renderFilterShortcuts();
  }).catch(function() { /* Descriptions fall back to the raw field name. */ });
  // The built-ins come from the same place the filter bar reads them, so
  // "restore" can never drift from what the bar ships with.
  safeFetch('/api/filters/shortcuts', {}, { toast: false }).then(function(data) {
    FILTER_SHORTCUT_DEFAULTS = (data && data.defaults) || [];
  }).catch(function() { /* Restore stays unavailable; the list still edits. */ });
}

// Stable text for a rule node so two expressions can be compared. The
// server refuses two buttons that apply the same rule (neither could own the
// chip), so the form has to notice before the save fails.
function canonicalRule(node) {
  if (Array.isArray(node)) return '[' + node.map(canonicalRule).join(',') + ']';
  if (node && typeof node === 'object') {
    return '{' + Object.keys(node).sort().map(function(k) {
      return JSON.stringify(k) + ':' + canonicalRule(node[k]);
    }).join(',') + '}';
  }
  return JSON.stringify(node);
}

function shortcutWithSameRule(rules) {
  var key = canonicalRule(rules);
  return _filterShortcutsState.find(function(entry) {
    return canonicalRule(entry.rules) === key;
  }) || null;
}

function describeShortcutRules(rules) {
  if (window.VireoFilter && _filterFieldSpecs) {
    try { return VireoFilter.describeRule(rules, _filterFieldSpecs); }
    catch (e) { /* fall through to the raw shape below */ }
  }
  if (rules && rules.field) return rules.field + ' ' + rules.op + ' ' + rules.value;
  return 'Saved filter expression';
}

// Which other buttons this one combines with (OR), so a row says so rather
// than leaving the behavior to be discovered in the grid. Mirrors how the bar
// merges: enum values merge by field wherever they sit, "missing X" buttons
// merge with the others in their group.
function shortcutKind(entry) {
  var rules = entry && entry.rules;
  var spec = rules && rules.field && _filterFieldSpecs ? _filterFieldSpecs[rules.field] : null;
  if (!spec || rules.op !== 'is' || Array.isArray(rules.value)) return 'rules';
  if (spec.type === 'enum') return 'enum';
  if (spec.type === 'boolean' && !rules.value) return 'missing';
  return 'rules';
}

// Some fields exist only on one page (review-only prediction fields, say),
// so a button built on them is hidden elsewhere. Name those pages in the row
// rather than leaving the user to wonder where their button went.
function shortcutPageScope(entry) {
  var rules = entry && entry.rules;
  if (!rules || !_filterFieldSpecs) return '';
  var fields = rules.field ? [rules.field] : [];
  if (!fields.length && Array.isArray(rules.rules)) {
    fields = rules.rules.map(function(r) { return r && r.field; }).filter(Boolean);
  }
  var pages = null;
  fields.forEach(function(key) {
    var spec = _filterFieldSpecs[key];
    if (!spec || !Array.isArray(spec.pages)) return;
    pages = (pages || []).concat(spec.pages);
  });
  if (!pages) return '';
  var unique = pages.filter(function(p, i) { return pages.indexOf(p) === i; });
  return unique.length ? unique.join(', ') : 'no page';
}

function shortcutGroupPeers(entry) {
  var kind = shortcutKind(entry);
  if (kind === 'rules') return [];
  return _filterShortcutsState.filter(function(other) {
    if (other === entry || shortcutKind(other) !== kind) return false;
    return kind === 'enum'
      ? other.rules.field === entry.rules.field
      : Boolean(entry.group) && other.group === entry.group;
  });
}

function renderFilterShortcuts() {
  var container = document.getElementById('cfgFilterShortcutsList');
  if (!container) return;
  var counter = document.getElementById('cfgShortcutCount');
  if (counter) {
    counter.textContent = _filterShortcutsState.length + ' of ' +
      MAX_FILTER_SHORTCUTS + ' quick filters';
  }
  container.innerHTML = '';
  if (!_filterShortcutsState.length) {
    var empty = document.createElement('div');
    empty.style.cssText = 'color:var(--text-dim);font-size:12px;font-style:italic;';
    empty.textContent = 'No quick filters — the filter bar shows the search box and Filters button only.';
    container.appendChild(empty);
    return;
  }
  _filterShortcutsState.forEach(function(entry, i) {
    var row = document.createElement('div');
    row.setAttribute('data-shortcut-row', entry.id || '');
    row.style.cssText = 'display:flex;align-items:center;gap:8px;background:var(--bg-secondary);border:1px solid var(--border-secondary);border-radius:6px;padding:6px 8px;';

    var labelInput = document.createElement('input');
    labelInput.type = 'text';
    labelInput.value = entry.label || '';
    labelInput.placeholder = 'Button text';
    labelInput.maxLength = MAX_SHORTCUT_LABEL;
    labelInput.setAttribute('aria-label', 'Button text');
    labelInput.style.cssText = 'background:var(--bg-input);color:var(--text-primary);border:1px solid var(--border-secondary);border-radius:4px;padding:5px 8px;font-size:12px;width:190px;';
    labelInput.addEventListener('input', function() {
      _filterShortcutsState[i].label = labelInput.value;
      // An empty label is not stored as empty — normalization swaps in the
      // rule's own wording — so hold the write until blur resolves what the
      // button will actually say.
      if (labelInput.value.trim()) saveConfig();
    });
    labelInput.addEventListener('blur', function() {
      if (labelInput.value.trim()) return;
      labelInput.value = describeShortcutRules(_filterShortcutsState[i].rules)
        .slice(0, MAX_SHORTCUT_LABEL);
      _filterShortcutsState[i].label = labelInput.value;
      saveConfig();
    });
    row.appendChild(labelInput);

    var desc = document.createElement('div');
    desc.style.cssText = 'flex:1;min-width:0;font-size:12px;color:var(--text-dim);overflow:hidden;text-overflow:ellipsis;white-space:nowrap;';
    var peers = shortcutGroupPeers(entry);
    desc.textContent = describeShortcutRules(entry.rules);
    var only = shortcutPageScope(entry);
    if (only) {
      var scope = document.createElement('span');
      scope.style.cssText = 'color:var(--text-faint);';
      scope.textContent = ' · only on ' + only;
      desc.appendChild(scope);
    }
    if (peers.length) {
      var tag = document.createElement('span');
      tag.style.cssText = 'color:var(--text-faint);';
      tag.textContent = ' · OR-grouped with ' + peers.map(function(p) {
        return '“' + (p.label || p.id) + '”';
      }).join(', ');
      desc.appendChild(tag);
    }
    // The row truncates; the tooltip carries the whole sentence.
    desc.title = desc.textContent;
    row.appendChild(desc);

    [['↑', -1, 'Move up'], ['↓', 1, 'Move down']].forEach(function(spec) {
      var btn = document.createElement('button');
      btn.type = 'button';
      btn.textContent = spec[0];
      btn.title = spec[2];
      btn.disabled = (spec[1] < 0 && i === 0) || (spec[1] > 0 && i === _filterShortcutsState.length - 1);
      btn.style.cssText = 'background:var(--bg-tertiary);color:var(--text-dim);border:1px solid var(--border-secondary);border-radius:4px;width:26px;height:26px;font-size:12px;cursor:pointer;line-height:1;' +
        (btn.disabled ? 'opacity:.4;cursor:default;' : '');
      btn.addEventListener('click', function() {
        if (btn.disabled) return;
        var target = i + spec[1];
        var moved = _filterShortcutsState.splice(i, 1)[0];
        _filterShortcutsState.splice(target, 0, moved);
        renderFilterShortcuts();
        saveConfig();
      });
      row.appendChild(btn);
    });

    var removeBtn = document.createElement('button');
    removeBtn.type = 'button';
    removeBtn.textContent = '×';
    removeBtn.title = 'Remove this button';
    removeBtn.style.cssText = 'background:var(--bg-tertiary);color:var(--text-dim);border:1px solid var(--border-secondary);border-radius:4px;width:26px;height:26px;font-size:15px;cursor:pointer;line-height:1;';
    removeBtn.addEventListener('click', function() {
      _filterShortcutsState.splice(i, 1);
      renderFilterShortcuts();
      saveConfig();
    });
    row.appendChild(removeBtn);
    container.appendChild(row);
  });
}

function buildShortcutFieldSelect() {
  var select = document.getElementById('cfgShortcutField');
  if (!select || !_filterFieldSpecs) return;
  var byCategory = {};
  Object.keys(_filterFieldSpecs).forEach(function(key) {
    var spec = _filterFieldSpecs[key];
    // Fields the add-form cannot build a complete value for are left to the
    // filter bar's own rule builder rather than offered half-working here.
    if (spec.type === 'folder' || !shortcutFieldOptions(spec).length) return;
    // `pages: []` marks an internal deep-link predicate (its values are
    // encoded tokens, not text a person types). No page offers it in its own
    // field picker, so a button built on it could only ever fail.
    if (Array.isArray(spec.pages) && !spec.pages.length) return;
    (byCategory[spec.category] = byCategory[spec.category] || []).push(key);
  });
  select.innerHTML = '';
  Object.keys(byCategory).forEach(function(category) {
    var group = document.createElement('optgroup');
    group.label = category;
    byCategory[category].forEach(function(key) {
      var option = document.createElement('option');
      option.value = key;
      option.textContent = _filterFieldSpecs[key].label;
      group.appendChild(option);
    });
    select.appendChild(group);
  });
  select.value = 'rating';
  if (!select.value) select.value = select.options.length ? select.options[0].value : '';
  select.onchange = function() { buildShortcutOpSelect(); };
  buildShortcutOpSelect();
}

function buildShortcutOpSelect() {
  var spec = _filterFieldSpecs && _filterFieldSpecs[document.getElementById('cfgShortcutField').value];
  var opSelect = document.getElementById('cfgShortcutOp');
  if (!spec || !opSelect) return;
  var labels = (window.VireoFilter && VireoFilter.opLabels) ? VireoFilter.opLabels() : {};
  opSelect.innerHTML = '';
  shortcutFieldOptions(spec).forEach(function(op) {
    var option = document.createElement('option');
    option.value = op;
    option.textContent = labels[op] || op;
    opSelect.appendChild(option);
  });
  opSelect.onchange = function() { buildShortcutValueInput(); };
  buildShortcutValueInput();
}

function buildShortcutValueInput() {
  var spec = _filterFieldSpecs && _filterFieldSpecs[document.getElementById('cfgShortcutField').value];
  var op = document.getElementById('cfgShortcutOp').value;
  var host = document.getElementById('cfgShortcutValue');
  if (!spec || !host) return;
  var inputCss = 'background:var(--bg-input);color:var(--text-primary);border:1px solid var(--border-secondary);border-radius:4px;padding:6px 8px;font-size:12px;';
  host.innerHTML = '';
  var addSelect = function(values, labels) {
    var select = document.createElement('select');
    select.style.cssText = inputCss;
    select.setAttribute('aria-label', 'Value');
    values.forEach(function(value) {
      var option = document.createElement('option');
      option.value = String(value);
      option.textContent = (labels && labels[value] != null) ? labels[value] : String(value);
      select.appendChild(option);
    });
    host.appendChild(select);
    return select;
  };
  if (op === 'recent') {
    var n = document.createElement('input');
    n.type = 'number';
    n.min = '1';
    n.value = '30';
    n.style.cssText = inputCss + 'width:70px;';
    n.setAttribute('aria-label', 'How many');
    host.appendChild(n);
    addSelect(['days', 'weeks', 'months', 'years']);
  } else if (spec.type === 'boolean') {
    addSelect([1, 0], {1: 'Yes', 0: 'No'});
  } else if (spec.type === 'enum' && spec.values && spec.values.length) {
    addSelect(spec.values, spec.labels);
  } else if (spec.type === 'rating') {
    // Default to a star count that actually narrows: "at least 0" is every
    // photo, which is not a filter anyone means to save.
    addSelect([0, 1, 2, 3, 4, 5]).value = '3';
  } else if (spec.type === 'number') {
    var num = document.createElement('input');
    num.type = 'number';
    num.value = '0';
    num.style.cssText = inputCss + 'width:110px;';
    num.setAttribute('aria-label', 'Value');
    host.appendChild(num);
  } else if (spec.type === 'date') {
    var date = document.createElement('input');
    date.type = 'date';
    date.value = new Date().toISOString().slice(0, 10);
    date.style.cssText = inputCss;
    date.setAttribute('aria-label', 'Date');
    host.appendChild(date);
  } else {
    var text = document.createElement('input');
    text.type = 'text';
    text.placeholder = 'Value';
    text.style.cssText = inputCss + 'width:160px;';
    text.setAttribute('aria-label', 'Value');
    host.appendChild(text);
  }
  host.querySelectorAll('input, select').forEach(function(el) {
    el.addEventListener('input', refreshShortcutPreview);
    el.addEventListener('change', refreshShortcutPreview);
  });
  refreshShortcutPreview();
}

// The rule the add-form currently describes, or null if it is incomplete.
function readShortcutForm() {
  var field = document.getElementById('cfgShortcutField').value;
  var spec = _filterFieldSpecs && _filterFieldSpecs[field];
  var op = document.getElementById('cfgShortcutOp').value;
  if (!spec || !op) return null;
  var controls = document.getElementById('cfgShortcutValue').querySelectorAll('input, select');
  var value;
  if (op === 'recent') {
    var n = parseInt(controls[0].value, 10);
    if (!n || n < 1) return null;
    value = {n: n, unit: controls[1].value};
  } else if (spec.type === 'boolean' || spec.type === 'rating' || spec.type === 'number') {
    value = Number(controls[0].value);
    if (isNaN(value)) return null;
  } else {
    value = controls[0].value;
    if (!String(value).trim()) return null;
  }
  return {field: field, op: op, value: value};
}

function refreshShortcutPreview() {
  var preview = document.getElementById('cfgShortcutPreview');
  var rule = readShortcutForm();
  if (!preview) return;
  preview.textContent = rule ? describeShortcutRules(rule) : 'Pick a field and a value.';
  var labelInput = document.getElementById('cfgShortcutLabel');
  if (rule && labelInput && !_shortcutLabelEdited) {
    // maxlength only constrains typing, so clamp the derived text too.
    labelInput.value = describeShortcutRules(rule).slice(0, MAX_SHORTCUT_LABEL);
  }
}

function addFilterShortcut() {
  if (_filterShortcutsState.length >= MAX_FILTER_SHORTCUTS) {
    showToast('The filter bar holds ' + MAX_FILTER_SHORTCUTS +
              ' quick filters — remove one first.', 'error');
    return;
  }
  var rule = readShortcutForm();
  if (!rule) {
    showToast('Pick a field and fill in a value first.', 'error');
    return;
  }
  var twin = shortcutWithSameRule(rule);
  if (twin) {
    showToast('“' + (twin.label || twin.id) + '” already applies this rule — ' +
              'rename that button instead.', 'error');
    return;
  }
  var labelInput = document.getElementById('cfgShortcutLabel');
  var label = ((labelInput.value || '').trim() ||
               describeShortcutRules(rule)).slice(0, MAX_SHORTCUT_LABEL);
  var id = 'sc_' + Math.random().toString(36).slice(2, 10);
  _filterShortcutsState.push({id: id, label: label, group: '', rules: rule});
  _shortcutLabelEdited = false;
  labelInput.value = '';
  renderFilterShortcuts();
  refreshShortcutPreview();
  saveConfig();
}

function restoreDefaultFilterShortcuts() {
  // Additive on purpose: bring back the built-ins that were removed without
  // discarding anything the user built.
  var have = {};
  _filterShortcutsState.forEach(function(entry) { have[entry.id] = true; });
  if (!FILTER_SHORTCUT_DEFAULTS.length) {
    showToast('Could not read the built-in quick filters — reload the page.', 'error');
    return;
  }
  var restored = 0;
  var skipped = 0;
  FILTER_SHORTCUT_DEFAULTS.forEach(function(entry) {
    if (have[entry.id]) return;
    // A button of your own already applying this rule counts as restored —
    // adding the built-in beside it would be a rejected duplicate.
    if (shortcutWithSameRule(entry.rules)) return;
    if (_filterShortcutsState.length >= MAX_FILTER_SHORTCUTS) { skipped += 1; return; }
    _filterShortcutsState.push(JSON.parse(JSON.stringify(entry)));
    restored += 1;
  });
  if (skipped) {
    showToast('Restored ' + restored + ' — no room for ' + skipped +
              ' more at ' + MAX_FILTER_SHORTCUTS + ' quick filters.', 'info');
  }
  if (!restored) {
    showToast('Every built-in quick filter is already in the list.', 'info');
    return;
  }
  renderFilterShortcuts();
  saveConfig();
}

function collectFilterShortcuts() {
  return _filterShortcutsState
    .filter(function(entry) { return entry && entry.rules; })
    .map(function(entry) {
      return {
        id: entry.id || '',
        label: typeof entry.label === 'string' ? entry.label.trim() : '',
        group: entry.group || '',
        rules: entry.rules,
      };
    });
}
