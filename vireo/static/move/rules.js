// Move rules: the rule list, and running, editing and deleting a rule.

import { state } from './state.js';
import { showConfirm } from './confirm-modal.js';
import { refreshMoveVisibility } from './move-visibility.js';
import { showRuleForm } from './rule-form.js';
import { escapeHtml, safeFetch, showToast } from './shared.js';

/* ---------- Rules ---------- */

export async function loadRules() {
  try {
    var data = await safeFetch('/api/move-rules');
    state.rulesList = data || [];
    renderRules();
  } catch (e) {
    /** @type {HTMLElement} */ (document.getElementById('rulesList')).innerHTML = '<div class="empty-state">Failed to load rules</div>';
  }
}

export function renderRules() {
  var container = /** @type {HTMLElement} */ (document.getElementById('rulesList'));
  if (state.rulesList.length === 0) {
    container.innerHTML = '<div class="empty-state">No move rules yet. Create one to get started.</div>';
    return;
  }

  var html = '';
  state.rulesList.forEach(function(r) {
    /** @type {MoveCriteria} */ var criteria = {};
    try { criteria = typeof r.criteria === 'string' ? JSON.parse(r.criteria) : (r.criteria || {}); } catch(e) {}
    var critSummary = buildCriteriaSummary(criteria);

    html += '<div class="rule-item" data-id="' + r.id + '">';
    html += '<div class="rule-info">';
    html += '<div class="rule-name">' + escapeHtml(r.name) + '</div>';
    html += '<div class="rule-dest">Destination: ' + escapeHtml(r.destination) + '</div>';
    if (critSummary) {
      html += '<div class="rule-criteria">' + escapeHtml(critSummary) + '</div>';
    }
    html += '</div>';
    html += '<div class="rule-last-run">' + (r.last_run_at ? 'Last run: ' + escapeHtml(r.last_run_at) : 'Never run') + '</div>';
    html += '<div class="rule-actions">';
    html += '<button class="btn btn-primary btn-small" data-rule-action="run">Run</button>';
    html += '<button class="btn btn-secondary btn-small" data-rule-action="edit">Edit</button>';
    html += '<button class="btn btn-danger btn-small" data-rule-action="delete">Delete</button>';
    html += '</div>';
    html += '</div>';
  });
  container.innerHTML = html;
}

/** @param {MoveCriteria} criteria */
export function buildCriteriaSummary(criteria) {
  var parts = [];
  if (criteria.rating_min) parts.push('Rating >= ' + criteria.rating_min);
  if (criteria.flag && criteria.flag !== 'any') parts.push('Flag: ' + criteria.flag);
  if (criteria.species && criteria.species.length) parts.push('Species: ' + criteria.species.join(', '));
  if (criteria.has_predictions) parts.push('Has predictions');
  if (criteria.folder_ids && criteria.folder_ids.length) parts.push('Folders: ' + criteria.folder_ids.length + ' selected');
  if (criteria.imported_before) parts.push('Before: ' + criteria.imported_before);
  return parts.join(' | ');
}

/** @param {number} ruleId */
export function editRule(ruleId) {
  var rule = state.rulesList.find(function(r) { return r.id === ruleId; });
  if (rule) showRuleForm(rule);
}

/** @param {number} ruleId */
export async function deleteRule(ruleId) {
  showConfirm('Delete Rule', 'Are you sure you want to delete this rule? This cannot be undone.', async function() {
    try {
      await safeFetch('/api/move-rules/' + ruleId, {method: 'DELETE'});
      showToast('Rule deleted', 'success');
      loadRules();
    } catch (e) {
      // safeFetch handles error toast
    }
  });
}

/** @param {number} ruleId */
export async function runRule(ruleId) {
  var foundRule = state.rulesList.find(function(r) { return r.id === ruleId; });
  if (!foundRule) return;
  const rule = foundRule;

  /** @type {MoveCriteria} */ var criteria = {};
  try { criteria = typeof rule.criteria === 'string' ? JSON.parse(rule.criteria) : (rule.criteria || {}); } catch(e) {}

  // First, preview to get matching photo IDs
  try {
    var preview = await safeFetch('/api/move-rules/preview', {
      method: 'POST',
      headers: {'Content-Type': 'application/json'},
      body: JSON.stringify({criteria: criteria}),
    });
    var photoIds = preview.photo_ids || [];
    if (photoIds.length === 0) {
      showToast('No photos match this rule', 'error');
      return;
    }

    if (!state.moveVisibilityReady) return;
    var names = await refreshMoveVisibility(photoIds);
    var keepVisible = /** @type {HTMLInputElement} */ (document.getElementById('moveKeepVisible')).checked;
    showConfirm(
      'Run Rule: ' + rule.name,
      'Move ' + photoIds.length + ' matching photo' + (photoIds.length !== 1 ? 's' : '') + ' to "' + rule.destination + '"? Other workspaces: ' + (names.join(', ') || 'none') +
        (keepVisible ? '. Keep moved photos visible there.' : '. Allow removal of their visibility there.'),
      async function() {
        try {
          var resp = await safeFetch('/api/jobs/move-photos', {
            method: 'POST',
            headers: {'Content-Type': 'application/json'},
            body: JSON.stringify({photo_ids: photoIds, destination: rule.destination, rule_id: ruleId, keep_visible: keepVisible}),
          });
          showToast('Rule running (job ' + resp.job_id + ') — track progress in the bottom panel', 'info');
        } catch (e) {
          // safeFetch handles error toast
        }
      }
    );
  } catch (e) {
    // safeFetch handles error toast
  }
}
