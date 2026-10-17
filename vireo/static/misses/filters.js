// The filter-bar payload: reading, cloning and comparing the rules and visual
// clause, adding them to request bodies, and the filter-change handler.
// Classic page script; load boot.js after all definitions.

/** @param {MissFilters} payload */ function cloneMissFilterPayload(payload) {
  return JSON.parse(JSON.stringify(payload || { rules: [], visual: null }));
}

function missFilterPayload() {
  /** @type {MissFilters['rules']} */ var rules = [];
  var visual = null;
  if (window.VireoFilter && VireoFilter.getRules) {
    rules = VireoFilter.getRules();
    visual = VireoFilter.getVisual ? VireoFilter.getVisual() : null;
  }
  return { rules: rules || [], visual: visual || null };
}

/** @param {MissBody} body @param {MissFilters} payload */ function addFilterPayloadToBody(body, payload) {
  body.rules = payload.rules || [];
  if (payload.visual) body.visual = payload.visual;
  return body;
}

/** @param {MissBody} body */ function addAppliedMissFiltersToBody(body) {
  return addFilterPayloadToBody(body, appliedMissFilters);
}

/** @param {MissFilters} a @param {MissFilters} b */ function sameMissFilterPayload(a, b) {
  return JSON.stringify(a || {}) === JSON.stringify(b || {});
}

/** @param {unknown} node @param {string} field @returns {boolean} */ function missRulesContainField(node, field) {
  if (Array.isArray(node)) {
    return node.some(function(child) { return missRulesContainField(child, field); });
  }
  if (!node || typeof node !== 'object') return false;
  if ('field' in node && node.field === field) return true;
  return 'rules' in node && Array.isArray(node.rules) && node.rules.some(function(child) {
    return missRulesContainField(child, field);
  });
}

function applyMissFilters() {
  if (initializingMissFilters) return;
  // A user-driven filter change replaces the unresolved deep-link scope with
  // whatever they typed, so the fail-closed guard no longer applies.
  clearLegacyScopeError();
  invalidateMissPreview();
  selection.clear();
  focusedCategory = null;
  focusedIndex = -1;
  loadMisses();
}
