/* Universal photo filter bar — shared across Browse/Map/Review/Duplicates.
 *
 * Owns the filter expression (a smart-collection rule tree, the same JSON
 * collections store), its UI (quick search, chips, popover with quick
 * filters + rule builder), pause state, and per-workspace persistence.
 * All evaluation is server-side: pages call VireoFilter.getRules() and
 * fetch /api/photos/query themselves; the module fires onChange when the
 * effective expression changes.
 *
 * Design + hard UI requirements (chip text = exact semantics, counts =
 * "how many results would I get", pause never loses filters, Advanced
 * toggle never rewrites the tree):
 * docs/plans/2026-07-19-universal-filters-design.md
 */
(function () {
  'use strict';

  const OP_LABELS = {
    'contains': 'contains',
    'not_contains': "doesn't contain",
    'is': 'is',
    'is not': 'is not',
    'starts_with': 'starts with',
    'ends_with': 'ends with',
    '>=': 'is at least',
    '<=': 'is at most',
    '>': 'is more than',
    '<': 'is less than',
    'between': 'is between',
    'in': 'is one of',
    'not_in': 'is not one of',
    'under': 'is under',
    'not_under': 'is not under',
    'recent': 'is in the last',
  };
  const RECENT_UNITS = [
    ['days', 'day', 'days'],
    ['weeks', 'week', 'weeks'],
    ['months', 'month', 'months'],
    ['years', 'year', 'years'],
  ];
  // Quick search fans out over these fields as one replaceable any-group.
  const QUICK_SEARCH_FIELDS = ['filename', 'keyword', 'species', 'camera_make', 'camera_model', 'lens'];

  const state = {
    page: 'browse',
    root: { mode: 'all', rules: [] },
    muted: false,
    scopeLabel: '',
    scopeLocked: true,
    fields: null,          // registry from /api/filters/fields (key -> spec)
    fieldOrder: [],
    onChange: null,
    getContextRules: null, // page-supplied rules ANDed outside the user tree
    workspaceId: null,
    resultTotal: null,
    wouldMatch: null,      // count while paused
    advanced: false,
    ready: false,
  };

  let rootEl = null;
  let snapshots = [];
  let persistTimer = null;
  let suggestTimer = null;
  let toastTimer = null;
  let wouldMatchEpoch = 0;

  const $ = (sel) => rootEl.querySelector(sel);
  const $$ = (sel) => Array.from(rootEl.querySelectorAll(sel));
  const clone = (v) => JSON.parse(JSON.stringify(v));

  function fetchJson(url, options) {
    return fetch(url, options).then((r) => {
      if (!r.ok) return r.json().catch(() => ({})).then((body) => {
        throw new Error(body.error || body.message || `HTTP ${r.status}`);
      });
      return r.json();
    });
  }

  function loadRegistry() {
    if (state.fields) return Promise.resolve(state.fields);
    return fetchJson('/api/filters/fields').then((data) => {
      state.fields = {};
      state.fieldOrder = [];
      data.fields.forEach((f) => {
        state.fields[f.key] = f;
        state.fieldOrder.push(f.key);
      });
      return state.fields;
    });
  }

  // ---- rule model -------------------------------------------------------

  function isGroup(node) {
    return node && typeof node === 'object' && Array.isArray(node.rules) && !('field' in node);
  }

  function allLeaves(node, out) {
    const target = out || [];
    if (!node) return target;
    if (isGroup(node)) node.rules.forEach((child) => allLeaves(child, target));
    else target.push(node);
    return target;
  }

  function defaultOp(spec) {
    if (!spec) return 'is';
    if (spec.type === 'text') return 'contains';
    if (spec.type === 'date') return 'recent';
    if (spec.type === 'enum') return 'in';
    if (spec.type === 'rating') return '>=';
    if (spec.type === 'folder') return 'under';
    return spec.ops[0];
  }

  function defaultValue(spec, op) {
    if (op === 'recent') return { n: 12, unit: 'months' };
    if (op === 'between') {
      if (spec.type === 'date') return ['2025-01-01', new Date().toISOString().slice(0, 10)];
      return [0, 100];
    }
    if (op === 'in' || op === 'not_in') return spec.values ? [spec.values[0]] : [];
    if (spec.type === 'boolean') return 1;
    if (spec.type === 'enum') return (spec.values || [''])[0];
    if (spec.type === 'rating') return 3;
    if (spec.type === 'number') return 0;
    if (spec.type === 'date') return new Date().toISOString().slice(0, 10);
    return '';
  }

  function coerceValue(spec, op, prev) {
    if (op === 'recent') {
      return prev && typeof prev === 'object' && !Array.isArray(prev) && prev.n ? prev : defaultValue(spec, op);
    }
    if (op === 'between') {
      if (Array.isArray(prev) && prev.length === 2) return prev;
      return defaultValue(spec, op);
    }
    if (op === 'in' || op === 'not_in') {
      if (Array.isArray(prev)) return prev.length ? prev : defaultValue(spec, op);
      if (prev != null && prev !== '' && (!spec.values || spec.values.includes(prev))) return [prev];
      return defaultValue(spec, op);
    }
    if (Array.isArray(prev)) return prev.length ? prev[0] : defaultValue(spec, op);
    if (prev && typeof prev === 'object') return defaultValue(spec, op);
    if (spec.type === 'enum' && spec.values && !spec.values.includes(prev)) return spec.values[0];
    if (prev == null || prev === '') return defaultValue(spec, op);
    return prev;
  }

  function makeRule(field, op, value) {
    const spec = state.fields[field];
    const resolvedOp = op || defaultOp(spec);
    return { field, op: resolvedOp, value: value == null ? defaultValue(spec, resolvedOp) : value };
  }

  function getNodeAtPath(path) {
    if (path === 'root' || path === '') return state.root;
    let node = state.root;
    String(path).split('.').forEach((idx) => {
      node = node && node.rules ? node.rules[Number(idx)] : undefined;
    });
    return node;
  }

  function getParentAtPath(path) {
    const parts = String(path).split('.');
    const index = Number(parts.pop());
    let parent = state.root;
    parts.forEach((idx) => { parent = parent.rules[Number(idx)]; });
    return { parent, index };
  }

  function removeByReference(node, target) {
    if (!isGroup(node)) return false;
    const idx = node.rules.indexOf(target);
    if (idx >= 0) { node.rules.splice(idx, 1); return true; }
    return node.rules.some((child) => removeByReference(child, target));
  }

  // Clone the tree with one leaf dropped from its group — never substitute
  // "true", which inverts any/none groups (prototype review finding).
  function rulesWithout(target) {
    function walk(node) {
      if (!isGroup(node)) return node === target ? null : clone(node);
      const kept = node.rules.map(walk).filter((c) => c !== null);
      return { ...clone({ ...node, rules: [] }), rules: kept };
    }
    return walk(state.root);
  }

  // ---- labels -----------------------------------------------------------

  function valueLabel(spec, rule) {
    const labels = spec.labels || {};
    if (rule.op === 'in' || rule.op === 'not_in') {
      const values = Array.isArray(rule.value) ? rule.value : [rule.value];
      return values.map((v) => labels[v] || v).join(', ') || '(none)';
    }
    if (rule.op === 'recent') {
      const spec2 = rule.value || {};
      const unit = RECENT_UNITS.find(([k]) => k === spec2.unit) || RECENT_UNITS[0];
      return `${spec2.n} ${Number(spec2.n) === 1 ? unit[1] : unit[2]}`;
    }
    if (rule.op === 'between') {
      const pair = Array.isArray(rule.value) ? rule.value : ['', ''];
      return `${pair[0]} and ${pair[1]}`;
    }
    if (spec.type === 'boolean') return rule.value ? 'Yes' : 'No';
    if (spec.type === 'rating') {
      return `${rule.value} ${Number(rule.value) === 1 ? 'star' : 'stars'}`;
    }
    return labels[rule.value] != null ? labels[rule.value] : rule.value;
  }

  function ruleLabel(rule) {
    const spec = state.fields[rule.field] || { label: rule.field, type: 'text' };
    const opLabel = OP_LABELS[rule.op] || rule.op;
    return `${spec.label} ${opLabel} ${valueLabel(spec, rule)}`;
  }

  function quickSearchGroup() {
    return state.root.rules.find((n) => isGroup(n) && n._qs);
  }

  function chipEntries() {
    const entries = [];
    state.root.rules.forEach((node) => {
      if (isGroup(node) && node._qs) {
        entries.push({ node, label: `Search: “${node._qs_text}”`, qs: true });
      } else {
        allLeaves(node).forEach((leaf) => entries.push({ node: leaf, label: ruleLabel(leaf) }));
      }
    });
    return entries;
  }

  // ---- public expression ------------------------------------------------

  function userRules() {
    return state.root.rules.length ? clone(state.root) : { mode: 'all', rules: [] };
  }

  function effectiveRules() {
    const context = state.getContextRules ? state.getContextRules() : [];
    const user = state.muted ? [] : state.root.rules;
    if (!context.length && !user.length) return [];
    return { mode: 'all', rules: clone(context).concat(clone(user)) };
  }

  function hasUserFilters() {
    return state.root.rules.length > 0;
  }

  // ---- mutation + change plumbing --------------------------------------

  function snapshot() {
    snapshots.push(clone({ root: state.root, muted: state.muted }));
    if (snapshots.length > 20) snapshots.shift();
  }

  function mutate(fn, opts) {
    const options = opts || {};
    // A pending debounced edit would replay stale input against the
    // re-rendered tree (e.g. overwrite a just-picked suggestion with the
    // half-typed text). Nothing survives across a mutate.
    clearTimeout(editDebounce);
    editDebounce = null;
    if (!options.noSnapshot) snapshot();
    fn();
    // lightRender: state committed from a live input — refresh chips and
    // counters but leave the rule tree DOM (and the focused input + its
    // open suggest dropdown) untouched. Full re-renders mid-typing were
    // the root of a whole class of prototype-review bugs.
    if (options.lightRender) renderLight();
    else render();
    schedulePersist();
    if (state.onChange && !options.silent) state.onChange();
    if (state.muted) refreshWouldMatch();
  }

  function undo() {
    const prev = snapshots.pop();
    if (!prev) return;
    state.root = prev.root;
    state.muted = prev.muted;
    render();
    schedulePersist();
    if (state.onChange) state.onChange();
  }

  function toast(text, withUndo) {
    const el = $('.vf-toast');
    if (!el) return;
    el.querySelector('span').textContent = text;
    el.querySelector('button').hidden = !withUndo;
    el.hidden = false;
    clearTimeout(toastTimer);
    toastTimer = setTimeout(() => { el.hidden = true; }, 4200);
  }

  function refreshWouldMatch() {
    const epoch = ++wouldMatchEpoch;
    const context = state.getContextRules ? state.getContextRules() : [];
    const rules = { mode: 'all', rules: clone(context).concat(clone(state.root.rules)) };
    fetchJson('/api/photos/query', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ rules, per_page: 1 }),
    }).then((data) => {
      if (epoch !== wouldMatchEpoch || !state.muted) return;
      state.wouldMatch = data.total;
      renderMuteState();
    }).catch(() => {});
  }

  // ---- persistence ------------------------------------------------------

  function schedulePersist() {
    if (!state.workspaceId) return;
    clearTimeout(persistTimer);
    persistTimer = setTimeout(persistNow, 800);
  }

  function persistNow() {
    if (!state.workspaceId) return;
    // Read-modify-write ui_state so other keys survive.
    fetchJson('/api/workspaces/active').then((ws) => {
      if (ws.id !== state.workspaceId) return;
      const uiState = parseUiState(ws.ui_state);
      if (!uiState.universal_filters) uiState.universal_filters = {};
      uiState.universal_filters[state.page] = { root: state.root, muted: state.muted };
      // The server JSON-encodes ui_state itself — send the object, or the
      // stored value ends up double-encoded and unreadable on restore.
      return fetch(`/api/workspaces/${state.workspaceId}`, {
        method: 'PUT',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ ui_state: uiState }),
      });
    }).catch(() => {});
  }

  function parseUiState(raw) {
    if (raw == null) return {};
    let value = raw;
    try {
      // Tolerate one level of legacy double-encoding.
      if (typeof value === 'string') value = JSON.parse(value);
      if (typeof value === 'string') value = JSON.parse(value);
    } catch (e) { return {}; }
    return value && typeof value === 'object' ? value : {};
  }

  function restorePersisted() {
    return fetchJson('/api/workspaces/active').then((ws) => {
      state.workspaceId = ws.id;
      const uiState = parseUiState(ws.ui_state);
      const saved = uiState.universal_filters && uiState.universal_filters[state.page];
      if (saved && saved.root && Array.isArray(saved.root.rules)) {
        state.root = saved.root;
        state.muted = Boolean(saved.muted);
        return true;
      }
      return false;
    }).catch(() => false);
  }

  // ---- deep links -------------------------------------------------------

  function applyLegacyParams(params) {
    const rules = [];
    const ratingMin = params.get('rating_min');
    if (ratingMin) rules.push(makeRule('rating', '>=', Number(ratingMin)));
    const flag = params.get('flag');
    if (flag) rules.push(makeRule('flag', 'is', flag));
    const color = params.get('color_label');
    if (color) rules.push(makeRule('color_label', 'in', [color]));
    const dateFrom = params.get('date_from');
    if (dateFrom) rules.push(makeRule('timestamp', '>=', dateFrom));
    const dateTo = params.get('date_to');
    if (dateTo) rules.push(makeRule('timestamp', '<=', dateTo));
    // Preserve legacy Browse deep-link semantics. The old backend distinguished
    // three coordinate sources: `exif` = photo has EXIF GPS; `assigned` = no
    // EXIF GPS but a location keyword supplies coordinates; `none` = neither.
    // `has_gps` was a JS-side alias for `exif`, and `?missing_gps=1` a legacy
    // alias for `?location_status=none`. Mapping only to `has_gps` would break
    // `assigned`/`none` links (photos with an assigned location still have
    // has_gps=false, so they would leak into a `none` grid).
    let locationStatus = params.get('location_status');
    if (!locationStatus && params.get('missing_gps') === '1') locationStatus = 'none';
    if (locationStatus === 'has_gps' || locationStatus === 'exif') {
      rules.push(makeRule('has_gps', 'is', 1));
    } else if (locationStatus === 'assigned') {
      rules.push(makeRule('has_gps', 'is', 0));
      rules.push(makeRule('has_location_keyword', 'is', 1));
    } else if (locationStatus === 'none') {
      rules.push(makeRule('has_gps', 'is', 0));
      rules.push(makeRule('has_location_keyword', 'is', 0));
    }
    const keyword = params.get('keyword');
    if (keyword) rules.push(buildQuickSearchGroup(keyword));
    if (!rules.length) return false;
    state.root = { mode: 'all', rules };
    state.muted = false;
    return true;
  }

  // ---- quick search -----------------------------------------------------

  function buildQuickSearchGroup(text) {
    return {
      mode: 'any',
      _qs: true,
      _qs_text: text,
      rules: QUICK_SEARCH_FIELDS.map((field) => ({ field, op: 'contains', value: text })),
    };
  }

  function applyQuickSearch(text) {
    const value = String(text || '').trim();
    mutate(() => {
      state.root.rules = state.root.rules.filter((n) => !(isGroup(n) && n._qs));
      if (value) state.root.rules.unshift(buildQuickSearchGroup(value));
    });
  }

  function syncQuickSearchInput() {
    const input = $('.vf-search input');
    if (!input || document.activeElement === input) return;
    const group = quickSearchGroup();
    input.value = group ? group._qs_text : '';
  }

  // ---- rendering --------------------------------------------------------

  function render() {
    if (!state.ready) return;
    syncQuickSearchInput();
    renderRules();
    renderLight();
  }

  function renderLight() {
    if (!state.ready) return;
    renderChips();
    renderQuick();
    renderMuteState();
    const count = chipEntries().length;
    const badge = $('.vf-filters-btn .vf-count');
    badge.textContent = count;
    badge.hidden = count === 0;
    $('.vf-clear').hidden = !hasUserFilters();
    $('.vf-mute').hidden = !hasUserFilters() && !state.muted;
    requestAnimationFrame(updateChipOverflow);
  }

  function renderMuteState() {
    const btn = $('.vf-mute');
    btn.textContent = state.muted ? '▶ Resume' : '⏸ Pause';
    btn.classList.toggle('active', state.muted);
    btn.title = 'Temporarily disable filters without losing them (\\)';
    $('.vf-chip-row').classList.toggle('muted', state.muted);
    const note = $('.vf-paused-note');
    if (state.muted) {
      const n = state.wouldMatch;
      note.textContent = n == null
        ? 'Filters paused — showing everything in scope · press \\ to resume'
        : `Filters paused — showing everything in scope · ${n} would match · press \\ to resume`;
      note.hidden = false;
    } else {
      note.hidden = true;
    }
  }

  function esc(value) {
    return String(value == null ? '' : value)
      .replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;')
      .replace(/"/g, '&quot;').replace(/'/g, '&#39;');
  }

  function renderChips() {
    const list = $('.vf-chips');
    $('.vf-scope-chip span').textContent = state.scopeLabel;
    list.innerHTML = chipEntries().map((entry, idx) =>
      `<button class="vf-chip" data-chip="${idx}" type="button" title="Edit filters">
        <span>${esc(entry.label)}</span>
        <span class="vf-chip-x" data-chip-x="${idx}" role="button" aria-label="Remove ${esc(entry.label)}">×</span>
      </button>`).join('');
  }

  function updateChipOverflow() {
    const viewport = $('.vf-chip-viewport');
    const overflow = $('.vf-overflow');
    if (!viewport) return;
    const chips = $$('.vf-chips .vf-chip');
    chips.forEach((c) => { c.style.display = 'inline-flex'; });
    overflow.hidden = true;
    if (!chips.length || viewport.clientWidth <= 0) return;
    let used = 0;
    let hidden = 0;
    chips.forEach((chip) => {
      const w = chip.getBoundingClientRect().width + 6;
      if (used + w > Math.max(20, viewport.clientWidth - 44)) {
        chip.style.display = 'none';
        hidden += 1;
      } else used += w;
    });
    if (hidden) {
      overflow.textContent = `+${hidden}`;
      overflow.hidden = false;
    }
  }

  function findRootRule(field) {
    return state.root.rules.find((n) => !isGroup(n) && n.field === field);
  }

  function quickEnumValues(field) {
    const rule = findRootRule(field);
    if (!rule) return [];
    if (rule.op === 'in' && Array.isArray(rule.value)) return rule.value;
    if (rule.op === 'is') return [rule.value];
    return [];
  }

  function toggleQuickEnum(field, value) {
    mutate(() => {
      const idx = state.root.rules.findIndex((n) => !isGroup(n) && n.field === field);
      if (idx < 0) { state.root.rules.unshift(makeRule(field, 'in', [value])); return; }
      const rule = state.root.rules[idx];
      let values;
      if (rule.op === 'in' && Array.isArray(rule.value)) values = rule.value.slice();
      else if (rule.op === 'is') values = [rule.value];
      else { state.root.rules[idx] = makeRule(field, 'in', [value]); return; }
      if (values.includes(value)) values = values.filter((v) => v !== value);
      else values.push(value);
      if (!values.length) state.root.rules.splice(idx, 1);
      else state.root.rules[idx] = makeRule(field, 'in', values);
    });
  }

  function renderQuick() {
    const rating = findRootRule('rating');
    const opSel = $('.vf-quick-rating select');
    if (rating && ['>=', 'is', '<='].includes(rating.op)) opSel.value = rating.op;
    $$('.vf-quick-rating .vf-star').forEach((btn) => {
      btn.classList.toggle('active', Boolean(rating && Number(rating.value) >= Number(btn.dataset.rating)));
    });
    const flags = quickEnumValues('flag');
    $$('.vf-quick-flags button').forEach((btn) => btn.classList.toggle('active', flags.includes(btn.dataset.flag)));
    const colors = quickEnumValues('color_label');
    $$('.vf-quick-colors button').forEach((btn) => btn.classList.toggle('active', colors.includes(btn.dataset.color)));
  }

  function renderRules() {
    const tree = $('.vf-rule-tree');
    $('.vf-advanced input').checked = state.advanced;
    $('.vf-add-group').hidden = !state.advanced;
    const active = document.activeElement;
    const restore = active && tree.contains(active) && active.dataset && active.dataset.path != null
      ? {
          action: active.dataset.action,
          path: active.dataset.path,
          start: active.selectionStart,
          end: active.selectionEnd,
          suggest: Boolean(active.dataset.suggest),
        }
      : null;
    tree.innerHTML = state.root.rules.length
      ? state.root.rules.map((node, i) => renderNode(node, String(i), 0)).join('')
      : '<div class="vf-empty-rules">No rules yet. Use a quick filter or add any metadata field.</div>';
    if (restore) {
      const el = tree.querySelector(`[data-action="${restore.action}"][data-path="${restore.path}"]`);
      if (el) {
        el.focus({ preventScroll: true });
        if (restore.start != null) {
          try { el.setSelectionRange(restore.start, restore.end); } catch (e) { /* selects */ }
        }
        if (restore.suggest) showValueSuggest(el);
      }
    }
  }

  function renderNode(node, path, depth) {
    if (isGroup(node)) {
      if (node._qs) {
        return `<div class="vf-rule-row vf-qs-row">
          <span class="vf-qs-label">Search all text contains “${esc(node._qs_text)}”</span>
          <button class="vf-remove" data-action="remove" data-path="${path}" type="button" aria-label="Remove search">×</button>
        </div>`;
      }
      return `<div class="vf-group" data-depth="${depth}">
        <div class="vf-group-mode"><span>Match</span>
          <select data-action="mode" data-path="${path}" aria-label="Group logic">
            <option value="all" ${node.mode === 'all' ? 'selected' : ''}>all</option>
            <option value="any" ${node.mode === 'any' ? 'selected' : ''}>any</option>
            <option value="none" ${node.mode === 'none' ? 'selected' : ''}>none</option>
          </select><span>in this group</span>
          <button class="vf-remove" data-action="remove" data-path="${path}" type="button" aria-label="Remove group">×</button>
        </div>
        <div class="vf-group-children">${node.rules.map((c, i) => renderNode(c, `${path}.${i}`, depth + 1)).join('')}</div>
        <div class="vf-group-actions">
          <button class="vf-text-btn" data-action="add-child" data-path="${path}" type="button">＋ Rule</button>
          <button class="vf-text-btn" data-action="add-subgroup" data-path="${path}" type="button">＋ Group</button>
        </div>
      </div>`;
    }
    const spec = state.fields[node.field] || { label: node.field, type: 'text', ops: ['is'] };
    const fieldOptions = state.fieldOrder.map((key) =>
      `<option value="${key}" ${key === node.field ? 'selected' : ''}>${esc(state.fields[key].label)}</option>`).join('');
    const opOptions = spec.ops.map((op) =>
      `<option value="${esc(op)}" ${op === node.op ? 'selected' : ''}>${esc(OP_LABELS[op] || op)}</option>`).join('');
    return `<div class="vf-rule-row">
      <select data-action="field" data-path="${path}" aria-label="Filter field">${fieldOptions}</select>
      <select data-action="op" data-path="${path}" aria-label="Filter operator">${opOptions}</select>
      ${renderValueInput(node, spec, path)}
      <button class="vf-remove" data-action="remove" data-path="${path}" type="button" aria-label="Remove rule">×</button>
      ${spec.case_toggle ? `<div class="vf-rule-opts"><label><input data-action="case" data-path="${path}" type="checkbox" ${node.case ? 'checked' : ''}> Match case</label></div>` : ''}
    </div>`;
  }

  function renderValueInput(node, spec, path) {
    if ((node.op === 'in' || node.op === 'not_in') && spec.values) {
      const selected = Array.isArray(node.value) ? node.value : [node.value];
      const labels = spec.labels || {};
      return `<div class="vf-enum-multi" role="group">${spec.values.map((v) =>
        `<button type="button" class="vf-enum-pill ${selected.includes(v) ? 'active' : ''}" data-action="multi" data-path="${path}" data-value="${esc(v)}" aria-pressed="${selected.includes(v)}">${esc(labels[v] || v)}</button>`).join('')}</div>`;
    }
    if ((node.op === 'in' || node.op === 'not_in') && !spec.values) {
      // Suggest-backed enum (extension): free-entry list via typeahead.
      const selected = Array.isArray(node.value) ? node.value : [];
      return `<span class="vf-value-wrap"><input data-action="multi-text" data-path="${path}" data-suggest="${spec.suggest ? '1' : ''}" type="text" autocomplete="off" spellcheck="false" value="${esc(selected.join(', '))}" placeholder="Comma-separated values" aria-label="Filter values"><div class="vf-suggest" hidden></div></span>`;
    }
    if (node.op === 'recent') {
      const v = node.value || {};
      return `<div class="vf-value-pair"><input data-action="recent-n" data-path="${path}" type="number" min="1" step="1" value="${esc(v.n)}" aria-label="Number"><select data-action="recent-unit" data-path="${path}" aria-label="Unit">${RECENT_UNITS.map(([k, , plural]) => `<option value="${k}" ${v.unit === k ? 'selected' : ''}>${plural}</option>`).join('')}</select></div>`;
    }
    if (node.op === 'between') {
      const pair = Array.isArray(node.value) ? node.value : ['', ''];
      const type = spec.type === 'date' ? 'date' : 'number';
      return `<div class="vf-value-pair"><input data-action="between-lo" data-path="${path}" type="${type}" value="${esc(pair[0])}" aria-label="Lower bound"><span>and</span><input data-action="between-hi" data-path="${path}" type="${type}" value="${esc(pair[1])}" aria-label="Upper bound"></div>`;
    }
    if (spec.type === 'boolean') {
      return `<select data-action="value-bool" data-path="${path}" aria-label="Filter value"><option value="1" ${node.value ? 'selected' : ''}>Yes</option><option value="0" ${!node.value ? 'selected' : ''}>No</option></select>`;
    }
    if (spec.type === 'enum' && spec.values) {
      const labels = spec.labels || {};
      return `<select data-action="value-select" data-path="${path}" aria-label="Filter value">${spec.values.map((v) => `<option value="${esc(v)}" ${v === node.value ? 'selected' : ''}>${esc(labels[v] || v)}</option>`).join('')}</select>`;
    }
    const inputType = spec.type === 'number' || spec.type === 'rating' ? 'number' : (spec.type === 'date' ? 'date' : 'text');
    const extras = spec.type === 'rating' ? 'min="0" max="5" step="1"' : (inputType === 'number' ? 'step="any"' : '');
    if (spec.suggest && inputType === 'text') {
      return `<span class="vf-value-wrap"><input data-action="value-input" data-path="${path}" data-suggest="1" type="text" autocomplete="off" spellcheck="false" value="${esc(node.value)}" placeholder="Type or pick a value…" aria-label="Filter value"><div class="vf-suggest" hidden></div></span>`;
    }
    return `<input data-action="value-input" data-path="${path}" type="${inputType}" ${extras} value="${esc(node.value)}" aria-label="Filter value">`;
  }

  // ---- typeahead --------------------------------------------------------

  function showValueSuggest(input) {
    const wrap = input.closest('.vf-value-wrap');
    if (!wrap) return;
    const drop = wrap.querySelector('.vf-suggest');
    const node = getNodeAtPath(input.dataset.path);
    if (!node || isGroup(node)) { drop.hidden = true; return; }
    const spec = state.fields[node.field];
    if (!spec || !spec.suggest) { drop.hidden = true; return; }
    const raw = input.dataset.action === 'multi-text'
      ? String(input.value).split(',').pop() : input.value;
    const q = String(raw || '').trim();
    // Counts respect everything except the rule being edited, plus the
    // page's context rules — "how many results would I get".
    const context = state.getContextRules ? state.getContextRules() : [];
    const others = rulesWithout(node);
    const rules = { mode: 'all', rules: clone(context).concat(others.rules) };
    const params = new URLSearchParams({ field: node.field, limit: '8' });
    if (q) params.set('q', q);
    params.set('rules', JSON.stringify(rules));
    fetchJson(`/api/filters/values?${params}`).then((data) => {
      if (!document.contains(input)) return;
      if (!data.values.length) { drop.hidden = true; return; }
      drop.innerHTML = `<div class="vf-suggest-hint">In your photos · counts respect other filters</div>` +
        data.values.map((entry) =>
          `<button class="vf-value-option" data-suggest-value="${esc(entry.value)}" data-path="${input.dataset.path}" type="button"><span>${esc(entry.value)}</span><em>${entry.count}</em></button>`).join('');
      drop.hidden = false;
    }).catch(() => { drop.hidden = true; });
  }

  function hideSuggests() {
    $$('.vf-suggest').forEach((d) => { d.hidden = true; });
  }

  // ---- popover ----------------------------------------------------------

  function openPopover(open) {
    const pop = $('.vf-popover');
    const shouldOpen = open == null ? pop.hidden : Boolean(open);
    pop.hidden = !shouldOpen;
    $('.vf-filters-btn').classList.toggle('open', shouldOpen);
    $('.vf-filters-btn').setAttribute('aria-expanded', String(shouldOpen));
    if (shouldOpen) renderRules();
  }

  function renderFieldPicker(query) {
    const needle = String(query || '').trim().toLowerCase();
    const byCategory = new Map();
    state.fieldOrder.forEach((key) => {
      const spec = state.fields[key];
      if (needle && !`${spec.label} ${spec.category}`.toLowerCase().includes(needle)) return;
      if (!byCategory.has(spec.category)) byCategory.set(spec.category, []);
      byCategory.get(spec.category).push([key, spec]);
    });
    $('.vf-field-options').innerHTML = Array.from(byCategory.entries()).map(([category, fields]) =>
      `<div class="vf-field-category">${esc(category)}</div>` + fields.map(([key, spec]) =>
        `<button class="vf-field-option" data-add-field="${key}" type="button"><span>${esc(spec.label)}</span><span>${esc(spec.type)}</span></button>`).join('')).join('')
      || '<div class="vf-empty-rules">No fields found</div>';
  }

  // ---- events -----------------------------------------------------------

  let editDebounce = null;

  function handleRuleEdit(target, fromTyping) {
    const action = target.dataset.action;
    const path = target.dataset.path;
    if (!action || path == null) return;
    mutate(() => {
      const node = getNodeAtPath(path);
      if (!node) return;
      if (isGroup(node)) {
        if (action === 'mode') node.mode = target.value;
        return;
      }
      const spec = state.fields[node.field];
      if (action === 'field') {
        node.field = target.value;
        const next = state.fields[node.field];
        node.op = defaultOp(next);
        node.value = defaultValue(next, node.op);
        delete node.case;
      } else if (action === 'op') {
        node.op = target.value;
        node.value = coerceValue(spec, node.op, node.value);
      } else if (action === 'value-input') {
        node.value = ['number', 'rating'].includes(spec.type) ? Number(target.value) : target.value;
      } else if (action === 'value-select') {
        node.value = target.value;
      } else if (action === 'value-bool') {
        node.value = target.value === '1' ? 1 : 0;
      } else if (action === 'between-lo' || action === 'between-hi') {
        const pair = Array.isArray(node.value) ? node.value.slice() : ['', ''];
        const parsed = spec.type === 'date' ? target.value : Number(target.value);
        pair[action === 'between-lo' ? 0 : 1] = parsed;
        node.value = pair;
      } else if (action === 'recent-n') {
        node.value = { ...(node.value || {}), n: Math.max(1, Number(target.value) || 1) };
      } else if (action === 'recent-unit') {
        node.value = { ...(node.value || {}), unit: target.value };
      } else if (action === 'multi-text') {
        node.value = String(target.value).split(',').map((s) => s.trim()).filter(Boolean);
      } else if (action === 'case') {
        if (target.checked) node.case = true;
        else delete node.case;
      }
    }, {
      noSnapshot: ['value-input', 'between-lo', 'between-hi', 'recent-n', 'multi-text'].includes(action),
      // change-event edits (selects, checkboxes) re-render the row; live
      // typing must not destroy the input under the caret.
      lightRender: ['value-input', 'between-lo', 'between-hi', 'recent-n', 'multi-text'].includes(action) && fromTyping,
    });
  }

  function installEvents() {
    const searchInput = $('.vf-search input');
    searchInput.addEventListener('keydown', (e) => {
      if (e.key === 'Enter') { e.preventDefault(); applyQuickSearch(searchInput.value); }
      else if (e.key === 'Escape') { searchInput.blur(); }
    });
    searchInput.addEventListener('blur', () => {
      // Blur with a cleared box removes the quick-search clause.
      if (!searchInput.value.trim() && quickSearchGroup()) applyQuickSearch('');
      else syncQuickSearchInput();
    });

    $('.vf-filters-btn').addEventListener('click', () => openPopover());
    $('.vf-popover-close').addEventListener('click', () => openPopover(false));
    $('.vf-done').addEventListener('click', () => openPopover(false));
    $('.vf-overflow').addEventListener('click', () => openPopover(true));
    $('.vf-mute').addEventListener('click', toggleMute);
    $('.vf-clear').addEventListener('click', () => {
      mutate(() => { state.root = { mode: 'all', rules: [] }; state.muted = false; });
      toast('Filters cleared', true);
    });
    $('.vf-clear-rules').addEventListener('click', () => {
      mutate(() => { state.root = { mode: 'all', rules: [] }; state.muted = false; });
      toast('Filters cleared', true);
    });
    $('.vf-toast button').addEventListener('click', () => { undo(); $('.vf-toast').hidden = true; });

    $('.vf-chip-row').addEventListener('click', (e) => {
      const x = e.target.closest('[data-chip-x]');
      if (x) {
        e.stopPropagation();
        const entry = chipEntries()[Number(x.dataset.chipX)];
        if (entry) {
          mutate(() => {
            if (isGroup(entry.node)) {
              state.root.rules = state.root.rules.filter((n) => n !== entry.node);
            } else removeByReference(state.root, entry.node);
          });
          toast('Filter removed', true);
        }
        return;
      }
      if (e.target.closest('[data-chip]')) openPopover(true);
    });

    // Quick filters
    $('.vf-quick-rating').addEventListener('click', (e) => {
      const btn = e.target.closest('.vf-star');
      if (!btn) return;
      const op = $('.vf-quick-rating select').value;
      const value = Number(btn.dataset.rating);
      mutate(() => {
        const idx = state.root.rules.findIndex((n) => !isGroup(n) && n.field === 'rating');
        if (idx >= 0 && state.root.rules[idx].op === op && Number(state.root.rules[idx].value) === value) {
          state.root.rules.splice(idx, 1);
        } else if (idx >= 0) state.root.rules[idx] = makeRule('rating', op, value);
        else state.root.rules.unshift(makeRule('rating', op, value));
      });
    });
    $('.vf-quick-rating select').addEventListener('change', (e) => {
      const rating = findRootRule('rating');
      if (rating) mutate(() => { rating.op = e.target.value; });
    });
    $('.vf-quick-flags').addEventListener('click', (e) => {
      const btn = e.target.closest('[data-flag]');
      if (btn) toggleQuickEnum('flag', btn.dataset.flag);
    });
    $('.vf-quick-colors').addEventListener('click', (e) => {
      const btn = e.target.closest('[data-color]');
      if (btn) toggleQuickEnum('color_label', btn.dataset.color);
    });

    $('.vf-advanced input').addEventListener('change', (e) => {
      // Only controls whether group tooling is offered — never rewrites
      // the rule tree (hard requirement from the prototype review).
      state.advanced = e.target.checked;
      renderRules();
    });
    $('.vf-add-group').addEventListener('click', () => {
      mutate(() => state.root.rules.push({ mode: 'any', rules: [makeRule('flag', 'is', 'flagged'), makeRule('rating', 'is', 5)] }));
    });
    $('.vf-add-filter').addEventListener('click', () => {
      const picker = $('.vf-field-picker');
      picker.hidden = !picker.hidden;
      if (!picker.hidden) {
        renderFieldPicker('');
        const search = $('.vf-field-search');
        search.value = '';
        setTimeout(() => search.focus(), 0);
      }
    });
    $('.vf-field-search').addEventListener('input', (e) => renderFieldPicker(e.target.value));
    $('.vf-field-options').addEventListener('click', (e) => {
      const btn = e.target.closest('[data-add-field]');
      if (!btn) return;
      mutate(() => state.root.rules.push(makeRule(btn.dataset.addField)));
      $('.vf-field-picker').hidden = true;
    });

    const tree = $('.vf-rule-tree');
    tree.addEventListener('change', (e) => {
      // Typed inputs commit through the debounced input path; their blur
      // 'change' would re-render mid-click and destroy a suggest option
      // before its pick lands (blur fires before click).
      const action = e.target.dataset.action;
      if (['value-input', 'between-lo', 'between-hi', 'recent-n', 'multi-text'].includes(action)) return;
      handleRuleEdit(e.target);
    });
    tree.addEventListener('input', (e) => {
      const action = e.target.dataset.action;
      if (['value-input', 'between-lo', 'between-hi', 'recent-n', 'multi-text'].includes(action)) {
        clearTimeout(editDebounce);
        const target = e.target;
        editDebounce = setTimeout(() => {
          editDebounce = null;
          if (document.contains(target)) handleRuleEdit(target, true);
        }, 250);
      }
      if (e.target.dataset.suggest) showValueSuggest(e.target);
    });
    tree.addEventListener('focusin', (e) => {
      if (e.target.dataset && e.target.dataset.suggest) showValueSuggest(e.target);
    });
    tree.addEventListener('click', (e) => {
      const suggestion = e.target.closest('[data-suggest-value]');
      if (suggestion) {
        mutate(() => {
          const node = getNodeAtPath(suggestion.dataset.path);
          if (!node || isGroup(node)) return;
          if (node.op === 'in' || node.op === 'not_in') {
            const values = Array.isArray(node.value) ? node.value.slice() : [];
            if (!values.includes(suggestion.dataset.suggestValue)) values.push(suggestion.dataset.suggestValue);
            node.value = values;
          } else node.value = suggestion.dataset.suggestValue;
        });
        hideSuggests();
        return;
      }
      const target = e.target.closest('[data-action]');
      if (!target) return;
      const action = target.dataset.action;
      const path = target.dataset.path;
      if (action === 'multi') {
        mutate(() => {
          const node = getNodeAtPath(path);
          if (!node || isGroup(node)) return;
          let values = Array.isArray(node.value) ? node.value.slice() : [node.value];
          if (values.includes(target.dataset.value)) values = values.filter((v) => v !== target.dataset.value);
          else values.push(target.dataset.value);
          node.value = values;
        });
        return;
      }
      if (action === 'remove') {
        clearTimeout(editDebounce);
        mutate(() => {
          const ref = getParentAtPath(path);
          const container = ref.parent === state.root ? state.root.rules : ref.parent.rules;
          container.splice(ref.index, 1);
        });
        toast('Rule removed', true);
      } else if (action === 'add-child') {
        mutate(() => getNodeAtPath(path).rules.push(makeRule('keyword')));
      } else if (action === 'add-subgroup') {
        mutate(() => getNodeAtPath(path).rules.push({ mode: 'all', rules: [makeRule('rating', '>=', 3)] }));
      }
    });

    document.addEventListener('keydown', (e) => {
      const typing = ['INPUT', 'SELECT', 'TEXTAREA'].includes(e.target.tagName) || e.target.isContentEditable;
      if ((e.metaKey || e.ctrlKey) && e.key.toLowerCase() === 'f' && !e.shiftKey && !e.altKey) {
        e.preventDefault();
        searchInput.focus();
        searchInput.select();
      }
      if (e.key === '\\' && !typing && !e.metaKey && !e.ctrlKey) { e.preventDefault(); toggleMute(); }
      if (e.key === 'Escape') {
        hideSuggests();
        $('.vf-field-picker').hidden = true;
        if (!$('.vf-popover').hidden) openPopover(false);
      }
    });
    document.addEventListener('click', (e) => {
      // A click that triggered a re-render leaves a detached target by the
      // time this bubbles; treating it as an outside click would close the
      // popover after every suggestion pick.
      if (!document.contains(e.target)) return;
      if (!rootEl.contains(e.target)) {
        hideSuggests();
        $('.vf-field-picker').hidden = true;
        if (!$('.vf-popover').hidden && !e.target.closest('.vf-popover')) openPopover(false);
      } else {
        if (!e.target.closest('.vf-value-wrap')) hideSuggests();
        if (!e.target.closest('.vf-add-wrap')) $('.vf-field-picker').hidden = true;
      }
    });
    window.addEventListener('resize', updateChipOverflow);
  }

  function toggleMute() {
    if (!hasUserFilters() && !state.muted) { toast('No filters to pause'); return; }
    mutate(() => { state.muted = !state.muted; });
    toast(state.muted ? 'Filters paused — press \\ to resume' : 'Filters resumed');
  }

  // ---- public API -------------------------------------------------------

  window.VireoFilter = {
    init(options) {
      state.page = options.page || 'browse';
      state.scopeLabel = options.scopeLabel || '';
      state.onChange = options.onChange || null;
      state.getContextRules = options.getContextRules || null;
      rootEl = typeof options.root === 'string' ? document.querySelector(options.root) : options.root;
      if (!rootEl) return Promise.reject(new Error('VireoFilter: missing root element'));
      return loadRegistry().then(() => {
        installEvents();
        const urlParams = new URLSearchParams(window.location.search);
        const fromUrl = applyLegacyParams(urlParams);
        const finish = () => {
          state.ready = true;
          render();
          if (state.muted) refreshWouldMatch();
          return true;
        };
        if (fromUrl) {
          // Deep-link params win; still resolve the workspace for saves.
          return fetchJson('/api/workspaces/active')
            .then((ws) => { state.workspaceId = ws.id; })
            .catch(() => {})
            .then(finish);
        }
        return restorePersisted().then(finish);
      });
    },
    getRules() { return effectiveRules(); },
    getUserRules() { return userRules(); },
    addRule(field, op, value) {
      mutate(() => {
        // Same-field root rule is replaced (sidebar keyword clicks toggle).
        const idx = state.root.rules.findIndex((n) => !isGroup(n) && n.field === field);
        const rule = makeRule(field, op, value);
        if (idx >= 0 && JSON.stringify(state.root.rules[idx]) === JSON.stringify(rule)) {
          state.root.rules.splice(idx, 1);
        } else if (idx >= 0) state.root.rules[idx] = rule;
        else state.root.rules.unshift(rule);
      });
    },
    quickSearch(text) { applyQuickSearch(text); },
    removeField(field) {
      const idx = state.root.rules.findIndex((n) => !isGroup(n) && n.field === field);
      if (idx < 0) return;
      mutate(() => { state.root.rules.splice(idx, 1); });
    },
    hasFilters() { return hasUserFilters(); },
    isMuted() { return state.muted; },
    setScopeLabel(label) {
      state.scopeLabel = label;
      if (state.ready) renderChips();
    },
    setResultTotal(total) {
      state.resultTotal = total;
      if (!state.ready) return;
      const el = $('.vf-total strong');
      if (el) el.textContent = total == null ? '–' : Number(total).toLocaleString();
      const matchEl = $('.vf-match-count');
      if (matchEl) matchEl.textContent = total == null ? '' : `${Number(total).toLocaleString()} matching ${total === 1 ? 'photo' : 'photos'}`;
    },
    refresh() { if (state.ready) render(); },
  };
})();
