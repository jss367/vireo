/* Universal photo filter bar — shared across Browse/Map/Review/Duplicates/Misses.
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
    visual: null,       // {prompt, strength} — the CLIP clause, outside the tree
    visualInfo: null,   // last server-reported visual status/coverage
    scopeLabel: '',
    scopeLocked: true,
    fields: null,          // registry from /api/filters/fields (key -> spec)
    fieldOrder: [],
    onChange: null,
    getContextRules: null, // page-supplied rules ANDed outside the user tree
    getScope: null,        // page-supplied {folder_id, collection_id} for /api/filters/values
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
    if (rule.field === 'photo_ids') {
      const n = Array.isArray(rule.value) ? rule.value.length : 0;
      return `${n} hand-picked photo${n === 1 ? '' : 's'}`;
    }
    const spec = state.fields[rule.field] || { label: rule.field, type: 'text' };
    const opLabel = OP_LABELS[rule.op] || rule.op;
    return `${spec.label} ${opLabel} ${valueLabel(spec, rule)}`;
  }

  function quickSearchGroup() {
    return state.root.rules.find((n) => isGroup(n) && n._qs);
  }

  function chipEntries() {
    const entries = [];
    if (state.visual) {
      const status = state.visualInfo && state.visualInfo.status;
      entries.push({
        visual: true,
        error: Boolean(status && status !== 'ok'),
        label: `✦ Visually similar · “${state.visual.prompt}”`,
      });
    }
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

  // AND the page-context rules with the user's root expression while
  // preserving that expression's group mode. Concatenating
  // ``state.root.rules`` directly into an outer ``{mode: 'all', rules: …}``
  // wrapper would flatten a saved ``{mode: 'any'}`` or ``{mode: 'none'}``
  // root — the Edit Rules modal in Browse can save either — from OR/NOT
  // into AND, silently changing what the reopened collection matches
  // (Codex review r3620791294). Non-'all' roots are wrapped as a nested
  // child so the outer AND with the context is honored without collapsing
  // the inner OR/NOT.
  function composeWithContext(context, root) {
    const ctx = clone(context);
    const rootMode = (root && root.mode) || 'all';
    const rootRules = (root && Array.isArray(root.rules)) ? root.rules : [];
    if (rootMode === 'all') {
      if (!ctx.length && !rootRules.length) return [];
      return { mode: 'all', rules: ctx.concat(clone(rootRules)) };
    }
    if (!ctx.length) return clone(root);
    return { mode: 'all', rules: ctx.concat([clone(root)]) };
  }

  function effectiveRules() {
    const context = state.getContextRules ? state.getContextRules() : [];
    if (state.muted) {
      if (!context.length) return [];
      return { mode: 'all', rules: clone(context) };
    }
    return composeWithContext(context, state.root);
  }

  function hasUserFilters() {
    return state.root.rules.length > 0 || Boolean(state.visual);
  }

  // ---- mutation + change plumbing --------------------------------------

  function snapshot() {
    snapshots.push(clone({ root: state.root, muted: state.muted, visual: state.visual }));
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
    // Persistence writes the expanded rule tree into ui_state. Opening a
    // saved collection would then snapshot its whole photo_ids list into
    // every workspace-state fetch, and a later reload would restore that
    // stale expansion instead of re-resolving the live collection —
    // membership edits/deletes wouldn't show through (Codex r3623087117).
    // Callers opening a saved expression pass ``noPersist: true`` so the
    // load itself isn't persisted; subsequent user edits go through
    // mutate() again without the flag and persist normally.
    if (!options.noPersist) schedulePersist();
    // `reason` (optional string) is forwarded to onChange so pages can
    // pick per-cause reload behavior — e.g. Browse preserving the
    // selected-photo anchor when a quick search is cleared but not for
    // every filter change (arbitrary edits usually exclude the anchor,
    // and loadUntilPhotoRendered would then page through the whole set).
    if (state.onChange && !options.silent) state.onChange({ reason: options.reason || null });
    if (state.muted) refreshWouldMatch();
  }

  function undo() {
    const prev = snapshots.pop();
    if (!prev) return;
    state.root = prev.root;
    state.muted = prev.muted;
    state.visual = prev.visual || null;
    render();
    schedulePersist();
    if (state.onChange) state.onChange({ reason: null });
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
    // Mirror effectiveRules' mode-preserving compose: a paused-view
    // "would match" counter must reflect what unpausing would apply, not
    // an AND-flattened version of an any/none root (Codex r3620791294).
    const rules = composeWithContext(context, state.root);
    fetchJson('/api/photos/query', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ rules, per_page: 1, visual: state.visual || undefined }),
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
      uiState.universal_filters[state.page] = {
        root: state.root, muted: state.muted, visual: state.visual,
      };
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
        state.visual = (
          saved.visual && typeof saved.visual.prompt === 'string' && saved.visual.prompt
        ) ? { prompt: saved.visual.prompt,
              strength: ['broad', 'balanced', 'strict'].includes(saved.visual.strength)
                ? saved.visual.strength : 'balanced' }
          : null;
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
    // alias for `?location_status=none`. `assigned`/`none` require the
    // coordinate-bearing keyword check (`has_coord_location_keyword`) — the
    // plain `has_location_keyword` field also matches free-text locations
    // without lat/lng, so it would misclassify photos the map cannot place.
    let locationStatus = params.get('location_status');
    if (!locationStatus && params.get('missing_gps') === '1') locationStatus = 'none';
    if (locationStatus === 'has_gps' || locationStatus === 'exif') {
      rules.push(makeRule('has_gps', 'is', 1));
    } else if (locationStatus === 'assigned') {
      rules.push(makeRule('has_gps', 'is', 0));
      rules.push(makeRule('has_coord_location_keyword', 'is', 1));
    } else if (locationStatus === 'none') {
      rules.push(makeRule('has_gps', 'is', 0));
      rules.push(makeRule('has_coord_location_keyword', 'is', 0));
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
    // Whitespace tokenizes: "red bill" matches (any field contains "red")
    // AND (any field contains "bill"), so a filename token plus a keyword
    // token still hit — pre-Phase 2 Browse search behavior. Single-token
    // input keeps the original flat any-group shape.
    const tokens = String(text).trim().split(/\s+/).filter(Boolean);
    if (tokens.length <= 1) {
      return {
        mode: 'any',
        _qs: true,
        _qs_text: text,
        rules: QUICK_SEARCH_FIELDS.map((field) => ({ field, op: 'contains', value: text })),
      };
    }
    return {
      mode: 'all',
      _qs: true,
      _qs_text: text,
      rules: tokens.map((tok) => ({
        mode: 'any',
        rules: QUICK_SEARCH_FIELDS.map((field) => ({ field, op: 'contains', value: tok })),
      })),
    };
  }

  function applyQuickSearch(text) {
    const value = String(text || '').trim();
    // A cleared quick search is the one filter edit where the previously
    // selected/open photo is expected to reappear in the wider result set.
    // Flag it so the page can preserve the anchor for this case without
    // reintroducing preservation for every filter change.
    const cleared = !value && !!quickSearchGroup();
    mutate(() => {
      state.root.rules = state.root.rules.filter((n) => !(isGroup(n) && n._qs));
      if (value) state.root.rules.unshift(buildQuickSearchGroup(value));
      // The visual clause and the quick-search clause are alternatives for
      // the top bar: setting one replaces the other. Without this, a
      // text search would compose with a still-active visual clause
      // (visual ∩ text) instead of replacing it.
      if (value) {
        state.visual = null;
        state.visualInfo = null;
      }
    }, cleared ? { reason: 'quickSearchCleared' } : undefined);
  }

  function applyVisualSearch(text) {
    const value = String(text || '').trim();
    mutate(() => {
      // The visual clause and the quick-search clause are alternatives for
      // the top bar: setting one replaces the other (prototype behavior).
      state.root.rules = state.root.rules.filter((n) => !(isGroup(n) && n._qs));
      state.visual = value
        ? { prompt: value, strength: (state.visual && state.visual.strength) || 'balanced' }
        : null;
      if (!value) state.visualInfo = null;
    });
  }

  function clearVisual() {
    if (!state.visual) return;
    mutate(() => { state.visual = null; state.visualInfo = null; });
  }

  function hideSearchSuggest() {
    const drop = $('.vf-search-suggest');
    if (drop) drop.hidden = true;
  }

  function showSearchSuggest() {
    const drop = $('.vf-search-suggest');
    const input = $('.vf-search input');
    const q = input.value.trim();
    if (!drop) return;
    if (!q) { drop.hidden = true; return; }
    drop.innerHTML = `
      <button type="button" data-search-kind="text"><span>⌕</span><span>Search text for “${esc(q)}”</span><em>Enter</em></button>
      <button type="button" data-search-kind="visual" class="vf-suggest-visual"><span>✦</span><span>Visually similar to “${esc(q)}”</span><em></em></button>`;
    drop.hidden = false;
  }

  function syncQuickSearchInput() {
    const input = $('.vf-search input');
    if (!input || document.activeElement === input) return;
    const group = quickSearchGroup();
    input.value = group ? group._qs_text : (state.visual ? state.visual.prompt : '');
  }

  // ---- rendering --------------------------------------------------------

  function render() {
    if (!state.ready) return;
    syncQuickSearchInput();
    renderRules();
    renderLight();
  }

  function renderTotal() {
    const total = state.resultTotal;
    const el = $('.vf-total strong');
    if (el) el.textContent = total == null ? '–' : Number(total).toLocaleString();
    const matchEl = $('.vf-match-count');
    if (matchEl) matchEl.textContent = total == null ? '' : `${Number(total).toLocaleString()} matching ${total === 1 ? 'photo' : 'photos'}`;
  }

  function renderLight() {
    if (!state.ready) return;
    // A page may have reported its total before init resolved (its first
    // fetch races the registry/persistence loads) — paint the stored value.
    renderTotal();
    renderChips();
    renderQuick();
    renderMuteState();
    const count = chipEntries().length;
    const badge = $('.vf-filters-btn .vf-count');
    badge.textContent = count;
    badge.hidden = count === 0;
    $('.vf-clear').hidden = !hasUserFilters();
    $('.vf-mute').hidden = !hasUserFilters() && !state.muted;
    const saveBtn = $('.vf-save-collection');
    if (saveBtn) saveBtn.hidden = !hasUserFilters();
    renderVisualNote();
    renderHandoff();
    requestAnimationFrame(updateChipOverflow);
  }

  const VISUAL_STATUS_MESSAGES = {
    no_model: 'No active model — visual search unavailable',
    model_no_text_search: 'The active model does not support text search',
    no_embeddings: 'No visual index for these photos',
    encoding_failed: 'Visual search failed',
  };

  function renderVisualNote() {
    const note = $('.vf-visual-note');
    if (!note) return;
    const info = state.visualInfo;
    if (!state.visual || state.muted || !info) { note.hidden = true; return; }
    if (info.status !== 'ok') {
      note.textContent = `✦ ${VISUAL_STATUS_MESSAGES[info.status] || info.status} — metadata filters shown only`;
      note.classList.add('error');
      note.hidden = false;
      return;
    }
    note.classList.remove('error');
    if (info.indexed != null && info.candidates != null && info.indexed < info.candidates) {
      note.textContent = `✦ Visual index covers ${info.indexed} of ${info.candidates} photos in scope`;
      note.hidden = false;
    } else {
      note.hidden = true;
    }
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
      `<button class="vf-chip${entry.visual ? ' visual' : ''}${entry.error ? ' error' : ''}" data-chip="${idx}" type="button" title="Edit filters">
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
    const visualRow = state.visual ? `<div class="vf-rule-row vf-visual-row">
      <span class="vf-visual-label">✦ Visually similar to “${esc(state.visual.prompt)}”</span>
      <div class="vf-segmented vf-visual-strength">
        ${['broad', 'balanced', 'strict'].map((s) =>
          `<button type="button" data-action="visual-strength" data-strength="${s}" class="${state.visual.strength === s ? 'active' : ''}">${s}</button>`).join('')}
      </div>
      <button class="vf-remove" data-action="visual-remove" type="button" aria-label="Remove visual search">×</button>
    </div>` : '';
    tree.innerHTML = visualRow + (state.root.rules.length
      ? state.root.rules.map((node, i) => renderNode(node, String(i), 0)).join('')
      : (visualRow ? '' : '<div class="vf-empty-rules">No rules yet. Use a quick filter or add any metadata field.</div>'));
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
    const fieldOptions = state.fieldOrder.filter((key) =>
      fieldAvailable(state.fields[key]) || key === node.field).map((key) =>
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
    // A healthy visual clause narrows the count set server-side, so facet
    // counts keep describing the visually-filtered grid.
    if (state.visual && !state.muted) params.set('visual', JSON.stringify(state.visual));
    // Page scope (folder / dashboard-scoped collection) is passed as
    // separate params to /api/photos/query, so the visible grid is
    // restricted to that scope. Mirror it here or the counts advertised
    // beside each suggestion are computed over the whole workspace and a
    // pick can produce fewer (or zero) grid rows than the badge promised.
    const scope = state.getScope ? state.getScope() : null;
    if (scope) {
      if (scope.folder_id != null) params.set('folder_id', scope.folder_id);
      if (scope.collection_id != null) params.set('collection_id', scope.collection_id);
    }
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

  function fieldAvailable(spec) {
    return !spec.pages || spec.pages.includes(state.page);
  }

  function renderFieldPicker(query) {
    const needle = String(query || '').trim().toLowerCase();
    const byCategory = new Map();
    state.fieldOrder.forEach((key) => {
      const spec = state.fields[key];
      if (!fieldAvailable(spec)) return;
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
      if (e.key === 'Enter') {
        e.preventDefault();
        hideSearchSuggest();
        applyQuickSearch(searchInput.value);
      } else if (e.key === 'Escape') { hideSearchSuggest(); searchInput.blur(); }
    });
    searchInput.addEventListener('input', showSearchSuggest);
    searchInput.addEventListener('focus', showSearchSuggest);
    searchInput.addEventListener('blur', () => {
      setTimeout(hideSearchSuggest, 150);
      // Blur with a cleared box removes the quick-search clause (a visual
      // clause set from this box clears the same way).
      if (!searchInput.value.trim() && quickSearchGroup()) applyQuickSearch('');
      else if (!searchInput.value.trim() && state.visual) applyVisualSearch('');
      else syncQuickSearchInput();
    });
    const searchSuggest = $('.vf-search-suggest');
    if (searchSuggest) {
      // mousedown beats the input's blur, so the pick is never lost.
      searchSuggest.addEventListener('mousedown', (e) => {
        const btn = e.target.closest('[data-search-kind]');
        if (!btn) return;
        e.preventDefault();
        const value = searchInput.value;
        hideSearchSuggest();
        if (btn.dataset.searchKind === 'visual') applyVisualSearch(value);
        else applyQuickSearch(value);
      });
    }

    $('.vf-filters-btn').addEventListener('click', () => openPopover());
    $('.vf-popover-close').addEventListener('click', () => openPopover(false));
    $('.vf-done').addEventListener('click', () => openPopover(false));
    $('.vf-overflow').addEventListener('click', () => openPopover(true));
    $('.vf-mute').addEventListener('click', toggleMute);
    const handoffBtn = $('.vf-handoff');
    if (handoffBtn) {
      handoffBtn.addEventListener('click', openHandoffMenu);
      $('.vf-handoff-menu').addEventListener('click', (e) => {
        const btn = e.target.closest('[data-handoff-path]');
        if (!btn) return;
        // The expression transfers; the destination adds its own locked
        // page scope. URL param wins over the destination's persisted
        // state on load.
        const payload = encodeURIComponent(
          JSON.stringify({ root: state.root, visual: state.visual })
        );
        window.location.href = `${btn.dataset.handoffPath}?filters=${payload}`;
      });
    }
    $('.vf-clear').addEventListener('click', () => {
      mutate(() => {
        state.root = { mode: 'all', rules: [] };
        state.muted = false;
        state.visual = null;
        state.visualInfo = null;
      });
      toast('Filters cleared', true);
    });
    $('.vf-clear-rules').addEventListener('click', () => {
      mutate(() => {
        state.root = { mode: 'all', rules: [] };
        state.muted = false;
        state.visual = null;
        state.visualInfo = null;
      });
      toast('Filters cleared', true);
    });
    $('.vf-toast button').addEventListener('click', () => { undo(); $('.vf-toast').hidden = true; });
    const saveCollectionBtn = $('.vf-save-collection');
    if (saveCollectionBtn) {
      saveCollectionBtn.addEventListener('click', openSaveModal);
      $('.vf-save-cancel').addEventListener('click', closeSaveModal);
      $('.vf-save-backdrop').addEventListener('click', closeSaveModal);
      $('.vf-save-confirm').addEventListener('click', confirmSaveCollection);
      $('.vf-save-name').addEventListener('keydown', (e) => {
        if (e.key === 'Enter') confirmSaveCollection();
        else if (e.key === 'Escape') closeSaveModal();
      });
    }

    $('.vf-chip-row').addEventListener('click', (e) => {
      const x = e.target.closest('[data-chip-x]');
      if (x) {
        e.stopPropagation();
        const entry = chipEntries()[Number(x.dataset.chipX)];
        if (entry) {
          mutate(() => {
            if (entry.visual) {
              state.visual = null;
              state.visualInfo = null;
            } else if (isGroup(entry.node)) {
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
      if (action === 'visual-strength') {
        mutate(() => { if (state.visual) state.visual.strength = target.dataset.strength; });
        return;
      }
      if (action === 'visual-remove') {
        clearVisual();
        toast('Visual search removed', true);
        return;
      }
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
        const handoffMenu = $('.vf-handoff-menu');
        if (handoffMenu) handoffMenu.hidden = true;
        if (!$('.vf-popover').hidden && !e.target.closest('.vf-popover')) openPopover(false);
      } else {
        if (!e.target.closest('.vf-value-wrap')) hideSuggests();
        if (!e.target.closest('.vf-add-wrap')) $('.vf-field-picker').hidden = true;
        // Same rule as the field picker: clicks inside the filter bar but
        // outside the handoff wrap (search input, other dropdowns) should
        // close the menu — otherwise it stays open while the user moves
        // on to something else.
        if (!e.target.closest('.vf-handoff-wrap')) {
          const handoffMenu = $('.vf-handoff-menu');
          if (handoffMenu) handoffMenu.hidden = true;
        }
      }
    });
    window.addEventListener('resize', updateChipOverflow);
  }

  function expressionSummary() {
    const parts = [];
    if (state.visual) parts.push(`✦ Visually similar to “${state.visual.prompt}” (${state.visual.strength})`);
    chipEntries().forEach((entry) => { if (!entry.visual) parts.push(entry.label); });
    return parts.join(' AND ') || 'No filters';
  }

  function openSaveModal() {
    const modal = $('.vf-save-modal');
    const backdrop = $('.vf-save-backdrop');
    // Post-save semantics: the saved Collection reopens unmuted with the
    // visual clause applied — preview THAT count, never the paused view's.
    // Preview the same expression confirmSaveCollection persists
    // (state.root + visual), NOT the current view's page-context scope:
    // the reopened collection matches globally, so including
    // getContextRules() in the preview would show a folder-scoped count
    // that disagrees with what the collection actually contains on
    // reopen (CodeRabbit review r3620473554).
    // Preserve the root's group mode (any/none) so the count reflects
    // what the saved collection actually matches, not an all-mode
    // flattening of it (Codex review r3620791294). ``userRules()`` is
    // the shared helper for that shape.
    const rules = userRules();
    const preview = $('.vf-save-preview');
    preview.textContent = expressionSummary();
    fetchJson('/api/photos/query', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ rules, per_page: 1, visual: state.visual || undefined }),
    }).then((data) => {
      preview.textContent = `${Number(data.total).toLocaleString()} matching photo${data.total === 1 ? '' : 's'} — ${expressionSummary()}`;
    }).catch(() => {});
    $('.vf-save-name').value = '';
    modal.hidden = false;
    backdrop.hidden = false;
    setTimeout(() => $('.vf-save-name').focus(), 0);
  }

  function closeSaveModal() {
    $('.vf-save-modal').hidden = true;
    $('.vf-save-backdrop').hidden = true;
  }

  function confirmSaveCollection() {
    const name = $('.vf-save-name').value.trim();
    if (!name) { $('.vf-save-name').focus(); return; }
    fetchJson('/api/collections', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        name,
        rules: state.root.rules.length ? state.root : [],
        visual: state.visual || null,
      }),
    }).then(() => {
      closeSaveModal();
      toast(`Saved “${name}” as a Collection`);
      if (state.onCollectionSaved) state.onCollectionSaved();
    }).catch((e) => {
      toast(`Could not save: ${e.message}`);
    });
  }

  const HANDOFF_PAGES = [
    ['browse', '/browse', 'Browse', 'Workspace · All available photos'],
    ['map', '/map', 'Map', 'Adds: plottable locations scope'],
    ['review', '/review', 'Review', 'Adds: predictions scope'],
    ['duplicates', '/duplicates', 'Duplicates', 'Adds: duplicate groups scope'],
    ['misses', '/misses', 'Misses', 'Adds: detected misses scope'],
  ];

  function renderHandoff() {
    const btn = $('.vf-handoff');
    if (!btn) return;
    btn.hidden = !hasUserFilters();
  }

  function openHandoffMenu() {
    const menu = $('.vf-handoff-menu');
    if (!menu.hidden) { menu.hidden = true; return; }
    menu.innerHTML = HANDOFF_PAGES.filter(([key]) => key !== state.page).map(([key, path, label, detail]) =>
      `<button type="button" data-handoff-path="${path}"><span>${label}</span><small>${esc(detail)}</small></button>`).join('');
    menu.hidden = false;
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
      state.getScope = options.getScope || null;
      state.onCollectionSaved = options.onCollectionSaved || null;
      rootEl = typeof options.root === 'string' ? document.querySelector(options.root) : options.root;
      if (!rootEl) return Promise.reject(new Error('VireoFilter: missing root element'));
      return loadRegistry().then(() => {
        installEvents();
        const urlParams = new URLSearchParams(window.location.search);
        let fromUrl = false;
        const handoffRaw = urlParams.get('filters');
        if (handoffRaw) {
          try {
            const payload = JSON.parse(handoffRaw);
            if (payload && payload.root && Array.isArray(payload.root.rules)) {
              state.root = payload.root;
              // Reconstruct the visual clause from the allowed fields
              // rather than trusting the parsed URL payload — an invalid
              // ``strength`` (or an unknown extra key) would otherwise
              // ride through to ``/api/photos/query`` and 500 the request.
              // Mirrors ``restorePersisted()``.
              state.visual = (
                payload.visual && typeof payload.visual.prompt === 'string' && payload.visual.prompt
              ) ? { prompt: payload.visual.prompt,
                    strength: ['broad', 'balanced', 'strict'].includes(payload.visual.strength)
                      ? payload.visual.strength : 'balanced' }
                : null;
              fromUrl = true;
            }
          } catch (e) { /* malformed handoff param — fall through */ }
        }
        if (!fromUrl) fromUrl = applyLegacyParams(urlParams);
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
    getVisual() {
      // Pause disables the visual clause with the rest of the user filters.
      return (state.muted || !state.visual) ? null : clone(state.visual);
    },
    setVisualInfo(info) {
      state.visualInfo = info || null;
      if (state.ready) renderLight();
    },
    visualSearch(text) { applyVisualSearch(text); },
    loadExpression(rules, visual, opts) {
      // Open a saved Collection into the bar as editable chips. Accepts the
      // stored rules JSON (legacy flat list or grouped tree) and the
      // visual_json clause; both become live, editable state.
      //
      // The default 'expressionLoaded' reason lets pages preserve the photo
      // anchor — a selected member of the opened collection should stay in
      // place. Membership-refresh callers (a keyword/tag change while the
      // collection is open) pass ``{ reason: 'expressionRefreshed' }`` so
      // the anchor is NOT preserved: the selected photo may have just left
      // the collection, and loadUntilPhotoRendered would then page through
      // the whole refreshed set looking for it (Codex review r3622521603).
      let root = { mode: 'all', rules: [] };
      if (Array.isArray(rules)) root = { mode: 'all', rules: clone(rules) };
      else if (rules && Array.isArray(rules.rules)) root = clone(rules);
      // Legacy default collections use the {"field": "all"} sentinel (no
      // condition) — opening one is simply "show everything", not a chip.
      root.rules = root.rules.filter((r) => !(r && r.field === 'all'));
      const reason = (opts && opts.reason) || 'expressionLoaded';
      mutate(() => {
        state.root = root;
        state.muted = false;
        state.visual = (
          visual && typeof visual.prompt === 'string' && visual.prompt
        ) ? { prompt: visual.prompt,
              strength: ['broad', 'balanced', 'strict'].includes(visual.strength)
                ? visual.strength : 'balanced' }
          : null;
        state.visualInfo = null;
        // See mutate()'s noPersist note (Codex r3623087117): opening a
        // saved collection should not snapshot its expanded rule list
        // (e.g. a huge photo_ids array from a manual collection) into
        // workspaces.ui_state.
      }, { reason, noPersist: true });
    },
    getUserRules() { return userRules(); },
    addRule(field, op, value) {
      mutate(() => {
        const rule = makeRule(field, op, value);
        const existing = state.root.rules.filter((n) => !isGroup(n) && n.field === field);
        // Toggle off when the sole existing rule of this field is
        // identical (sidebar keyword clicks re-toggle to clear).
        if (existing.length === 1 && JSON.stringify(existing[0]) === JSON.stringify(rule)) {
          state.root.rules = state.root.rules.filter((n) => n !== existing[0]);
          return;
        }
        // Otherwise replace ALL same-field root leaves. Legacy deep
        // links can install more than one rule per field (e.g.
        // ?date_from=…&date_to=… → two `timestamp` rules), and
        // replacing only the first would leave the stale bound behind
        // — a calendar day pick would then compose "day X" with the
        // leftover ">= date_from" and silently show wrong results.
        state.root.rules = state.root.rules.filter((n) => isGroup(n) || n.field !== field);
        state.root.rules.unshift(rule);
      });
    },
    quickSearch(text) { applyQuickSearch(text); },
    removeField(field) {
      // Remove ALL matching root leaves — legacy `?date_from=…&date_to=…`
      // and other multi-rule param combinations can install more than
      // one leaf per field. Returns true when a rule was actually
      // removed (and onChange fired) so callers that were relying on the
      // reload — e.g. filterByFolder handing off after dropping a
      // sidebar-installed keyword rule — can fall back to their own
      // reload when this is a no-op.
      const hasMatch = state.root.rules.some((n) => !isGroup(n) && n.field === field);
      if (!hasMatch) return false;
      mutate(() => {
        state.root.rules = state.root.rules.filter((n) => isGroup(n) || n.field !== field);
      });
      return true;
    },
    hasFilters() { return hasUserFilters(); },
    // Wipe restored/current filters without firing onChange. Used when the
    // page detects a deep-link (e.g. plain collection view) that must
    // ignore whatever was persisted — the alternative (applying then
    // dropping them on the first filter interaction) briefly paints the
    // wrong photo set and desyncs chips from the visible grid.
    clearAll(silent) {
      if (silent) {
        state.root = { mode: 'all', rules: [] };
        state.muted = false;
        state.visual = null;
        state.visualInfo = null;
        schedulePersist();
        if (state.ready) render();
        return;
      }
      mutate(() => {
        state.root = { mode: 'all', rules: [] };
        state.muted = false;
        state.visual = null;
        state.visualInfo = null;
      });
    },
    isReady() { return !!state.ready && !!state.fields; },
    isMuted() { return state.muted; },
    setScopeLabel(label) {
      state.scopeLabel = label;
      if (state.ready) renderChips();
    },
    setResultTotal(total) {
      state.resultTotal = total;
      if (!state.ready) return;  // painted by renderTotal() once init finishes
      renderTotal();
    },
    refresh() { if (state.ready) render(); },
  };
})();
