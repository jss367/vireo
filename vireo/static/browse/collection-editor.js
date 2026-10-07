/* Browse: collection rule editor modal and live match-count preview.
   Classic script sharing page globals; browse.html loads the
   browse/*.js files in dependency order. */

/* ---------- Collection Modal ---------- */
var collectionEditorRoot = {mode: 'all', rules: []};
var collectionEditingId = null;
// Cached dropdown sources for the rule editor. Filled on first modal open
// and reused across rule rows so toggling field=Extension or field=Folder
// doesn't refetch on every render. Re-fetched each time the modal opens
// so newly-imported folders/extensions show up without a page reload.
var collectionExtensions = [];
var collectionFolders = [];

/* Fields whose rule value is a number: they get a number input, default to
   0, and are parsed with parseFloat on save. One list rather than three
   copies — the copies had already drifted, and a field missing from any of
   them silently degrades to a text input or a NaN value on save. */
var NUMERIC_RULE_FIELDS = ['rating', 'quality_score', 'sharpness',
  'subject_sharpness', 'noise_estimate', 'crop_complete',
  'prediction_confidence', 'species_count'];

/* Ops every numeric field accepts. Must be a superset of what the
   registry (vireo/filter_fields.py) advertises for number/rating fields,
   or a rule saved from the filter bar reopens here showing a different
   operator than the one it runs with — and `between`, whose value is a
   [low, high] pair, used to be flattened to a single scalar on save. */
var NUMERIC_OPS = ['is', 'is not', '>=', '<=', '>', '<', 'between'];

/* Valid ops per field — must mirror vireo/db.py::_build_collection_query.
   The UI only shows ops the backend handles; non-matching combos used to
   silently produce zero matches. */
var FIELD_OPS = {
  all:         ['equals'],
  metadata:    ['contains', 'not_contains'],
  keyword:     ['is', 'is not', 'contains', 'not_contains'],
  species_count: NUMERIC_OPS,
  rating:      NUMERIC_OPS,
  flag:        ['is', 'is not'],
  color_label: ['is', 'is not'],
  extension:   ['is', 'is not'],
  folder:      ['under', 'not_under'],
  timestamp:   ['between', 'recent_days'],
  quality_score: NUMERIC_OPS,
  sharpness: NUMERIC_OPS,
  subject_sharpness: NUMERIC_OPS,
  noise_estimate: NUMERIC_OPS,
  crop_complete: NUMERIC_OPS,
  has_mask: ['equals'],
  has_jpeg_companion: ['equals'],
  active_mask_variant: ['is', 'is not', 'contains'],
  has_gps: ['equals'],
  has_location_keyword: ['equals'],
  wildlife_excluded: ['equals'],
  location_keyword_missing: ['equals'],
  inat_submitted: ['equals'],
  is_duplicate: ['equals'],
  prediction_confidence: NUMERIC_OPS,
  classifier_model: ['is', 'is not', 'contains'],
  prediction_status: ['is', 'is not'],
  needs_review: ['equals'],
};
var FIELD_LABELS = {
  all: 'All Photos',
  metadata: 'All metadata',
  keyword: 'Keyword',
  species_count: 'Species Count',
  rating: 'Rating',
  flag: 'Flag',
  color_label: 'Color Label',
  extension: 'Extension',
  folder: 'Folder',
  timestamp: 'Date',
  quality_score: 'Quality Score',
  sharpness: 'Sharpness',
  subject_sharpness: 'Subject Sharpness',
  noise_estimate: 'Noise',
  crop_complete: 'Crop Complete',
  has_mask: 'Has Mask',
  has_jpeg_companion: 'Has JPEG Companion',
  active_mask_variant: 'Mask Variant',
  has_gps: 'Has GPS',
  has_location_keyword: 'Has Location Keyword',
  wildlife_excluded: 'Not Wildlife',
  location_keyword_missing: 'GPS Without Location Keyword',
  inat_submitted: 'iNaturalist Submitted',
  is_duplicate: 'Duplicate',
  prediction_confidence: 'Prediction Confidence',
  classifier_model: 'Classifier Model',
  prediction_status: 'Prediction Status',
  needs_review: 'Needs Review',
};
var OP_LABELS = {
  'equals':       'is',
  'is':           'is',
  'is not':       'is not',
  'contains':     'contains',
  'not_contains': 'does not contain',
  '>=':           '>=',
  '<=':           '<=',
  '>':            '>',
  '<':            '<',
  'under':        'under',
  'not_under':    'not under',
  'between':      'between',
  'recent_days':  'in the last',
};

function defaultValueForRule(field, op) {
  if (field === 'all') return 1;
  if (field === 'color_label') return 'red';
  if (field === 'flag') return 'flagged';
  if (field === 'rating') return op === 'between' ? [0, 0] : 0;
  if (NUMERIC_RULE_FIELDS.indexOf(field) !== -1) {
    return op === 'between' ? [0, 0] : 0;
  }
  if (field === 'extension') return collectionExtensions[0] || '';
  if (field === 'folder') return (collectionFolders[0] && collectionFolders[0].path) || '';
  if (field === 'timestamp') {
    if (op === 'between') return ['', ''];
    if (op === 'recent_days') return 7;
  }
  if (field === 'active_mask_variant') return 'sam2-large';
  if (field === 'prediction_status') return 'pending';
  if (field === 'wildlife_excluded') return 0;
  if (['has_mask', 'has_jpeg_companion', 'has_gps', 'has_location_keyword', 'location_keyword_missing',
       'inat_submitted', 'is_duplicate', 'needs_review'].indexOf(field) !== -1) return 1;
  return '';
}

function normalizeCollectionRules(rawRules) {
  var parsed = rawRules;
  if (typeof parsed === 'string') {
    try { parsed = JSON.parse(parsed); } catch (e) { parsed = []; }
  }
  if (Array.isArray(parsed)) return {mode: 'all', rules: parsed};
  if (parsed && typeof parsed === 'object' && Array.isArray(parsed.rules)) {
    return {
      mode: (['all', 'any', 'none'].indexOf(parsed.mode) !== -1) ? parsed.mode : 'all',
      rules: parsed.rules,
    };
  }
  return {mode: 'all', rules: []};
}

function walkRuleTree(node, fn) {
  if (!node) return;
  if (Array.isArray(node)) {
    node.forEach(function(child) { walkRuleTree(child, fn); });
    return;
  }
  if (node.rules && !node.field) {
    node.rules.forEach(function(child) { walkRuleTree(child, fn); });
  } else {
    fn(node);
  }
}

async function loadCollectionEditorSources() {
  try {
    var [exts, folders] = await Promise.all([
      safeFetch('/api/photos/extensions'),
      safeFetch('/api/folders'),
    ]);
    collectionExtensions = Array.isArray(exts) ? exts : [];
    collectionFolders = Array.isArray(folders) ? folders : [];
  } catch (e) {
    collectionExtensions = [];
    collectionFolders = [];
  }
}

async function showCollectionModal(collection) {
  // Cancel any pending/in-flight preview from a previous session before
  // resetting state — otherwise a stale response can land on the DOM and
  // briefly show the previous rule set's count in the freshly opened modal.
  resetPreviewState();
  collectionEditingId = collection && collection.id ? collection.id : null;
  collectionEditorRoot = normalizeCollectionRules(collection ? collection.rules : []);
  document.getElementById('collectionModalTitle').textContent =
    collectionEditingId ? 'Edit Smart Collection' : 'New Smart Collection';
  document.getElementById('collectionSaveBtn').textContent =
    collectionEditingId ? 'Update' : 'Save';
  document.getElementById('rulePreview').textContent = '';
  document.getElementById('collectionName').value = collection ? (collection.name || '') : '';
  // Clear the rule-row DOM before awaiting the dropdown fetches. Without
  // this, a slow/failed fetch leaves the previous session's <select>s
  // visible and interactive while editor state is already reset, so
  // changing one fires updateRule(idx, ...) against an undefined row and
  // throws.
  renderRules();
  document.getElementById('collectionModal').classList.add('open');
  await loadCollectionEditorSources();
  // Backfill any extension/folder rules added during the await: they got
  // value '' from defaultValueForRule because the source lists were empty.
  // After fetch, the <select> visually picks the first option but state
  // still holds '', so save/preview would run with an empty filter.
  walkRuleTree(collectionEditorRoot, function(r) {
    if (r.field === 'extension' && !r.value) {
      r.value = collectionExtensions[0] || '';
    } else if (r.field === 'folder' && !r.value) {
      r.value = (collectionFolders[0] && collectionFolders[0].path) || '';
    }
  });
  // Only seed the initial row if the user hasn't already added one (or
  // dismissed the modal) during the await. Without this guard, a user who
  // clicks "+ Add Rule" while the fetch is in flight gets a phantom
  // keyword/contains/"" row tacked on when fetches resolve, silently
  // changing the saved collection's semantics.
  if (collectionEditorRoot.rules.length === 0 &&
      document.getElementById('collectionModal').classList.contains('open')) {
    addRuleRow();
  } else {
    // Re-render so any rows added during the await (which were drawn with
    // empty extension/folder dropdowns) pick up the freshly-fetched data.
    renderRules();
  }
}

function hideCollectionModal() {
  resetPreviewState();
  collectionEditingId = null;
  document.getElementById('collectionModal').classList.remove('open');
}

function getRuleContainer(path) {
  if (!path) return collectionEditorRoot;
  var parts = path.split('.').filter(Boolean).map(function(p) { return parseInt(p, 10); });
  var node = collectionEditorRoot;
  parts.forEach(function(idx) { node = node.rules[idx]; });
  return node;
}

function getRuleParent(path) {
  var parts = path.split('.').filter(Boolean);
  var idx = parseInt(parts.pop(), 10);
  var parent = parts.length ? getRuleContainer(parts.join('.')) : collectionEditorRoot;
  return {parent: parent, index: idx};
}

function addRuleRow(groupPath) {
  var group = getRuleContainer(groupPath || '');
  if (!group.rules) group.rules = [];
  group.rules.push({field: 'keyword', op: 'contains', value: ''});
  renderRules();
}

function addRuleGroup(groupPath) {
  var group = getRuleContainer(groupPath || '');
  if (!group.rules) group.rules = [];
  group.rules.push({mode: 'all', rules: [
    {field: 'keyword', op: 'contains', value: ''}
  ]});
  renderRules();
}

function removeRule(path) {
  var ref = getRuleParent(path);
  ref.parent.rules.splice(ref.index, 1);
  renderRules();
}

function updateRule(path, key, val) {
  var r = getRuleContainer(path);
  if (!r) return;
  if (key === 'mode') {
    r.mode = val;
    renderRules();
    return;
  }
  if (key === 'field') {
    r.field = val;
    delete r.rules;
    delete r.mode;
    var ops = FIELD_OPS[val] || [];
    // If current op isn't valid for the new field, default to the field's
    // first listed op. Otherwise keep the user's selection.
    if (ops.indexOf(r.op) === -1) {
      r.op = ops[0] || '';
    }
    r.value = defaultValueForRule(val, r.op);
    renderRules();
    return;
  }
  if (key === 'op') {
    var wasPair = r.op === 'between';
    r.op = val;
    // The value shape depends on the op: timestamp switches between a date
    // pair and a day count, and every numeric field switches between a
    // [low, high] pair and a scalar. Reset when the shape changes, or the
    // stale shape reaches the backend (a scalar under `between` hit
    // value[0] in _numeric_condition and 500'd).
    if (r.field === 'timestamp' || wasPair !== (val === 'between')) {
      r.value = defaultValueForRule(r.field, val);
    }
    renderRules();
    return;
  }
  if (key === 'value_from' && r.op === 'between') {
    // `|| ''` would rewrite a legitimate 0 bound to blank on a numeric
    // pair, so keep the other end verbatim unless it's actually missing.
    if (!Array.isArray(r.value)) r.value = ['', ''];
    r.value = [val, r.value[1] == null ? '' : r.value[1]];
    schedulePreviewUpdate();
    return;
  }
  if (key === 'value_to' && r.op === 'between') {
    if (!Array.isArray(r.value)) r.value = ['', ''];
    r.value = [r.value[0] == null ? '' : r.value[0], val];
    schedulePreviewUpdate();
    return;
  }
  r[key] = val;
  schedulePreviewUpdate();
}

/* ---------- Live match-count preview ---------- */
var _previewTimer = null;
var _previewSeq = 0;

function schedulePreviewUpdate() {
  if (_previewTimer) clearTimeout(_previewTimer);
  _previewTimer = setTimeout(updatePreviewNow, 250);
}

function resetPreviewState() {
  // Bump the sequence so any in-flight fetch's response is discarded as
  // stale, and cancel any pending debounced call. Used when the modal
  // opens or closes so a previous session can't write into a new one.
  if (_previewTimer) {
    clearTimeout(_previewTimer);
    _previewTimer = null;
  }
  _previewSeq++;
}

async function updatePreviewNow() {
  _previewTimer = null;
  var el = document.getElementById('rulePreview');
  if (!el) return;
  var rules = serializeCollectionRules();
  var seq = ++_previewSeq;
  try {
    var data = await safeFetch('/api/collections/preview', {
      method: 'POST',
      headers: {'Content-Type': 'application/json'},
      body: JSON.stringify({rules: rules}),
    }, {toast: false});
    if (seq !== _previewSeq) return;  // stale response
    if (data && typeof data.count === 'number') {
      el.textContent = 'Matches: ' + data.count + ' photo' + (data.count === 1 ? '' : 's');
    } else {
      el.textContent = '';
    }
  } catch (e) {
    if (seq === _previewSeq) el.textContent = '';
  }
}

function commitRuleOnEnter(e) {
  if (e.key !== 'Enter' || e.isComposing || e.keyCode === 229) return;
  var t = e.currentTarget || e.target;
  if (t && t.type === 'date') {
    // Native date pickers use Enter to commit the highlighted date;
    // cancelling it would close the picker without applying the selection.
    // Defer the blur so the picker's own Enter handler runs first.
    setTimeout(function() { t.blur(); }, 0);
    return;
  }
  e.preventDefault();
  t.blur();
}

function renderRuleValueInput(r, i) {
  var p = escapeAttr(i);
  if (r.field === 'all') {
    return '<span style="flex:1;color:var(--text-ghost);font-size:12px;">Matches every photo in this workspace</span>';
  }
  if (['has_mask', 'has_jpeg_companion', 'has_gps', 'has_location_keyword', 'wildlife_excluded',
       'location_keyword_missing', 'inat_submitted', 'is_duplicate',
       'needs_review'].indexOf(r.field) !== -1) {
    var boolVal = (r.value === true || r.value === 1 || r.value === '1' || r.value === 'true') ? '1' : '0';
    return '<select onchange="updateRule(\'' + p + '\',\'value\',this.value)" style="flex:1;">' +
        '<option value="1"' + (boolVal==='1'?' selected':'') + '>Yes</option>' +
        '<option value="0"' + (boolVal==='0'?' selected':'') + '>No</option>' +
      '</select>';
  }
  if (r.field === 'color_label') {
    return '<select onchange="updateRule(\'' + p + '\',\'value\',this.value)" style="flex:1;">' +
        '<option value="red"' + (r.value==='red'?' selected':'') + '>Red</option>' +
        '<option value="yellow"' + (r.value==='yellow'?' selected':'') + '>Yellow</option>' +
        '<option value="green"' + (r.value==='green'?' selected':'') + '>Green</option>' +
        '<option value="blue"' + (r.value==='blue'?' selected':'') + '>Blue</option>' +
        '<option value="purple"' + (r.value==='purple'?' selected':'') + '>Purple</option>' +
      '</select>';
  }
  if (r.field === 'flag') {
    return '<select onchange="updateRule(\'' + p + '\',\'value\',this.value)" style="flex:1;">' +
        '<option value="flagged"' + (r.value==='flagged'?' selected':'') + '>Pick</option>' +
        '<option value="rejected"' + (r.value==='rejected'?' selected':'') + '>Reject</option>' +
        '<option value="none"' + (r.value==='none'?' selected':'') + '>No flag</option>' +
      '</select>';
  }
  if (r.field === 'prediction_status') {
    return '<select onchange="updateRule(\'' + p + '\',\'value\',this.value)" style="flex:1;">' +
        '<option value="pending"' + (r.value==='pending'?' selected':'') + '>Pending</option>' +
        '<option value="accepted"' + (r.value==='accepted'?' selected':'') + '>Accepted</option>' +
        '<option value="rejected"' + (r.value==='rejected'?' selected':'') + '>Rejected</option>' +
      '</select>';
  }
  if (r.field === 'extension') {
    // Extension dropdown is sourced from /api/photos/extensions for the
    // active workspace. If the current rule value isn't in that list
    // (legacy rule, freshly switched field), include it as a sticky
    // option so the user can see+keep it instead of having it silently
    // snap to the first known extension on next render.
    return '<select onchange="updateRule(\'' + p + '\',\'value\',this.value)" style="flex:1;">' +
        (collectionExtensions.length === 0
          ? '<option value="">(no extensions yet — scan a folder)</option>'
          : '') +
        (r.value && collectionExtensions.indexOf(r.value) === -1
          ? '<option value="' + escapeAttr(String(r.value)) + '" selected>' + escapeHtml(String(r.value)) + '</option>'
          : '') +
        collectionExtensions.map(function(ext) {
          return '<option value="' + escapeAttr(ext) + '"' + (r.value===ext?' selected':'') + '>' + escapeHtml(ext) + '</option>';
        }).join('') +
      '</select>';
  }
  if (r.field === 'folder') {
    // Folder dropdown shows the path (full path is the value the
    // backend matches with f.path LIKE ?). Display is the path too —
    // basenames collide across folders and a smart-collection author
    // can't disambiguate "January" between /2023/January and /2024.
    var paths = collectionFolders.map(function(f){ return f.path; });
    var stickyFolder = (r.value && paths.indexOf(r.value) === -1)
      ? '<option value="' + escapeAttr(String(r.value)) + '" selected>' + escapeHtml(String(r.value)) + '</option>'
      : '';
    return '<select onchange="updateRule(\'' + p + '\',\'value\',this.value)" style="flex:1;">' +
        (collectionFolders.length === 0
          ? '<option value="">(no folders in this workspace)</option>'
          : '') +
        stickyFolder +
        collectionFolders.map(function(f) {
          return '<option value="' + escapeAttr(f.path) + '"' + (r.value===f.path?' selected':'') + '>' + escapeHtml(f.path) + '</option>';
        }).join('') +
      '</select>';
  }
  if (r.field === 'timestamp') {
    if (r.op === 'between') {
      var from = Array.isArray(r.value) ? (r.value[0] || '') : '';
      var to   = Array.isArray(r.value) ? (r.value[1] || '') : '';
      return '<div style="flex:1;display:flex;gap:6px;align-items:center;">' +
          '<input type="date" value="' + escapeAttr(from) + '" onchange="updateRule(\'' + p + '\',\'value_from\',this.value)" onkeydown="commitRuleOnEnter(event)" style="flex:1;">' +
          '<span style="color:var(--text-ghost);font-size:11px;">and</span>' +
          '<input type="date" value="' + escapeAttr(to) + '" onchange="updateRule(\'' + p + '\',\'value_to\',this.value)" onkeydown="commitRuleOnEnter(event)" style="flex:1;">' +
        '</div>';
    }
    if (r.op === 'recent_days') {
      var n = (typeof r.value === 'number' || (typeof r.value === 'string' && r.value !== '')) ? r.value : 7;
      return '<div style="flex:1;display:flex;gap:6px;align-items:center;">' +
          '<input type="number" min="1" value="' + escapeAttr(String(n)) + '" onchange="updateRule(\'' + p + '\',\'value\',this.value)" onkeydown="commitRuleOnEnter(event)" style="width:80px;">' +
          '<span style="color:var(--text-ghost);font-size:11px;">days</span>' +
        '</div>';
    }
  }
  var numeric = NUMERIC_RULE_FIELDS.indexOf(r.field) !== -1;
  if (numeric && r.op === 'between') {
    // Two inputs, matching the [low, high] pair the backend expects. A
    // single input here dropped the upper bound on save.
    var lo = Array.isArray(r.value) ? (r.value[0] == null ? '' : r.value[0]) : '';
    var hi = Array.isArray(r.value) ? (r.value[1] == null ? '' : r.value[1]) : '';
    return '<div style="flex:1;display:flex;gap:6px;align-items:center;">' +
        '<input type="number" step="any" value="' + escapeAttr(String(lo)) + '" onchange="updateRule(\'' + p + '\',\'value_from\',this.value)" onkeydown="commitRuleOnEnter(event)" style="flex:1;">' +
        '<span style="color:var(--text-ghost);font-size:11px;">and</span>' +
        '<input type="number" step="any" value="' + escapeAttr(String(hi)) + '" onchange="updateRule(\'' + p + '\',\'value_to\',this.value)" onkeydown="commitRuleOnEnter(event)" style="flex:1;">' +
      '</div>';
  }
  return '<input type="' + (numeric ? 'number' : 'text') + '" value="' +
    escapeAttr(String(r.value == null ? '' : r.value)) +
    '" onchange="updateRule(\'' + p + '\',\'value\',this.value)"' +
    ' onkeydown="commitRuleOnEnter(event)"' +
    ' placeholder="value" style="flex:1;">';
}

function renderFieldOptions(selected) {
  return Object.keys(FIELD_LABELS).map(function(field) {
    return '<option value="' + escapeAttr(field) + '"' + (selected === field ? ' selected' : '') + '>' +
      escapeHtml(FIELD_LABELS[field]) + '</option>';
  }).join('');
}

function renderRuleNode(node, path, depth) {
  var p = escapeAttr(path);
  var indent = Math.min(depth, 4) * 12;
  if (node.rules && !node.field) {
    var groupHtml = '<div style="margin-left:' + indent + 'px;margin-bottom:8px;padding:8px;border:1px solid var(--border-subtle);border-radius:4px;">' +
      '<div class="rule-row" style="margin-bottom:6px;">' +
      '<span style="font-size:12px;color:var(--text-dim);">Match</span>' +
      '<select onchange="updateRule(\'' + p + '\',\'mode\',this.value)">' +
        '<option value="all"' + (node.mode==='all'?' selected':'') + '>all</option>' +
        '<option value="any"' + (node.mode==='any'?' selected':'') + '>any</option>' +
        '<option value="none"' + (node.mode==='none'?' selected':'') + '>none</option>' +
      '</select>' +
      '<span style="font-size:12px;color:var(--text-dim);flex:1;">of these rules</span>';
    if (path) {
      groupHtml += '<span class="remove-rule" onclick="removeRule(\'' + p + '\')">&times;</span>';
    }
    groupHtml += '</div>';
    (node.rules || []).forEach(function(child, idx) {
      groupHtml += renderRuleNode(child, path ? path + '.' + idx : String(idx), depth + 1);
    });
    groupHtml += '<button onclick="addRuleRow(\'' + p + '\')" style="background:var(--bg-tertiary);color:var(--info);border:none;border-radius:4px;padding:3px 8px;font-size:11px;cursor:pointer;margin-right:4px;">+ Rule</button>' +
      '<button onclick="addRuleGroup(\'' + p + '\')" style="background:var(--bg-tertiary);color:var(--info);border:none;border-radius:4px;padding:3px 8px;font-size:11px;cursor:pointer;">+ Group</button>' +
      '</div>';
    return groupHtml;
  }
  var ops = FIELD_OPS[node.field] || [];
  // Preserve newer filter-bar expressions instead of presenting a text
  // input that flattens a value list on edit, or a misleading field/op.
  if (!FIELD_OPS[node.field] || ops.indexOf(node.op) === -1) {
    var description = window.VireoFilter
      ? VireoFilter.describeRule(node) : node.field + ' ' + node.op;
    return '<div class="rule-row" style="margin-left:' + indent + 'px;">' +
      '<span class="collection-preserved-rule" style="flex:1;">' + escapeHtml(description) + '</span>' +
      '<span class="remove-rule" onclick="removeRule(\'' + p + '\')">&times;</span></div>';
  }
  var opOptions = ops.map(function(op) {
    var label = OP_LABELS[op] || op;
    return '<option value="' + escapeAttr(op) + '"' + (node.op===op?' selected':'') + '>' + escapeHtml(label) + '</option>';
  }).join('');
  return '<div class="rule-row" style="margin-left:' + indent + 'px;">' +
    '<select onchange="updateRule(\'' + p + '\',\'field\',this.value)">' +
      renderFieldOptions(node.field) +
    '</select>' +
    '<select onchange="updateRule(\'' + p + '\',\'op\',this.value)">' +
      opOptions +
    '</select>' +
    renderRuleValueInput(node, path) +
    '<span class="remove-rule" onclick="removeRule(\'' + p + '\')">&times;</span>' +
  '</div>';
}

function renderRules() {
  document.getElementById('ruleRows').innerHTML = renderRuleNode(collectionEditorRoot, '', 0);
  schedulePreviewUpdate();
}

function coerceRuleForSave(node) {
  function num(v) {
    var n = parseFloat(v);
    return isNaN(n) ? 0 : n;
  }
  if (node.rules && !node.field) {
    return {
      mode: (['all', 'any', 'none'].indexOf(node.mode) !== -1) ? node.mode : 'all',
      rules: (node.rules || []).map(coerceRuleForSave),
    };
  }
  var val = node.value;
  if (NUMERIC_RULE_FIELDS.indexOf(node.field) !== -1) {
    if (node.op === 'between') {
      var pair = Array.isArray(val) ? val : [val, val];
      val = [num(pair[0]), num(pair[1])];
    } else {
      val = num(val);
    }
  } else if (['has_mask', 'has_jpeg_companion', 'has_gps', 'has_location_keyword',
              'location_keyword_missing', 'inat_submitted', 'is_duplicate',
              'needs_review', 'all'].indexOf(node.field) !== -1) {
    val = (val === true || val === 1 || val === '1' || val === 'true') ? 1 : 0;
  } else if (node.field === 'timestamp' && node.op === 'recent_days') {
    val = parseInt(val, 10) || 0;
  } else if (node.field === 'timestamp' && node.op === 'between') {
      var from = Array.isArray(val) ? (val[0] || '') : '';
      var to   = Array.isArray(val) ? (val[1] || '') : '';
      val = [from, to];
  }
  var saved = {field: node.field, op: node.op, value: val};
  if (typeof node.value_label === 'string') saved.value_label = node.value_label;
  return saved;
}

function serializeCollectionRules() {
  return coerceRuleForSave(collectionEditorRoot);
}

async function editCollection(cid) {
  var collection = collectionsById[cid];
  if (!collection) {
    var list = await safeFetch('/api/collections', {}, {toast: false});
    renderCollectionList(list || []);
    collection = collectionsById[cid];
  }
  if (collection) showCollectionModal(collection);
}

async function saveCollection() {
  var name = document.getElementById('collectionName').value.trim();
  if (!name) return;
  var rules = serializeCollectionRules();

  try {
    var url = collectionEditingId ? '/api/collections/' + collectionEditingId : '/api/collections';
    // This modal only edits rules — the preview and controls never surface
    // the stored visual clause. When editing an existing visual collection,
    // leaving visual_json in place would silently apply a hidden visual
    // prompt on reopen that the user just previewed without (Codex review
    // r3623087112). Clear it so the saved collection matches the preview.
    var body = {name: name, rules: rules};
    if (collectionEditingId) body.visual = null;
    await safeFetch(url, {
      method: collectionEditingId ? 'PUT' : 'POST',
      headers: {'Content-Type': 'application/json'},
      body: JSON.stringify(body),
    });
    hideCollectionModal();
    loadCollections();
    loadCollectionCounts();
  } catch(e) {}
}
