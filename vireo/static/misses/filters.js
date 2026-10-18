// The filter-bar payload: reading, cloning and comparing the rules and visual
// clause, adding them to request bodies, and the filter-change handler.
// ES module.

import { state } from './state.js';
import { VireoFilter } from './shared.js';
import { clearLegacyScopeError } from './scope.js';
import { invalidateMissPreview } from './tuning.js';
import { loadMisses } from './loading.js';

/** @param {MissFilters} payload */ export function cloneMissFilterPayload(payload) {
  return JSON.parse(JSON.stringify(payload || { rules: [], visual: null }));
}

export function missFilterPayload() {
  /** @type {MissFilters['rules']} */ var rules = [];
  var visual = null;
  if (VireoFilter && VireoFilter.getRules) {
    rules = VireoFilter.getRules();
    visual = VireoFilter.getVisual ? VireoFilter.getVisual() : null;
  }
  return { rules: rules || [], visual: visual || null };
}

/** @param {MissBody} body @param {MissFilters} payload */ export function addFilterPayloadToBody(body, payload) {
  body.rules = payload.rules || [];
  if (payload.visual) body.visual = payload.visual;
  return body;
}

/** @param {MissBody} body */ export function addAppliedMissFiltersToBody(body) {
  return addFilterPayloadToBody(body, state.appliedMissFilters);
}

/** @param {MissFilters} a @param {MissFilters} b */ export function sameMissFilterPayload(a, b) {
  return JSON.stringify(a || {}) === JSON.stringify(b || {});
}

/** @param {unknown} node @param {string} field @returns {boolean} */ export function missRulesContainField(node, field) {
  if (Array.isArray(node)) {
    return node.some(function(child) { return missRulesContainField(child, field); });
  }
  if (!node || typeof node !== 'object') return false;
  if ('field' in node && node.field === field) return true;
  return 'rules' in node && Array.isArray(node.rules) && node.rules.some(function(child) {
    return missRulesContainField(child, field);
  });
}

export function applyMissFilters() {
  if (state.initializingMissFilters) return;
  // A user-driven filter change replaces the unresolved deep-link scope with
  // whatever they typed, so the fail-closed guard no longer applies.
  clearLegacyScopeError();
  invalidateMissPreview();
  state.selection.clear();
  state.focusedCategory = null;
  state.focusedIndex = -1;
  loadMisses();
}
