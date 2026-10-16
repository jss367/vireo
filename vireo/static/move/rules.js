// Move rules: the rule list, and running, editing and deleting a rule.
// Classic page script; load boot.js after all definitions.

/* ---------- Rules ---------- */
var rulesList = [];

async function loadRules() {
  try {
    var data = await safeFetch('/api/move-rules');
    rulesList = data || [];
    renderRules();
  } catch (e) {
    document.getElementById('rulesList').innerHTML = '<div class="empty-state">Failed to load rules</div>';
  }
}

function renderRules() {
  var container = document.getElementById('rulesList');
  if (rulesList.length === 0) {
    container.innerHTML = '<div class="empty-state">No move rules yet. Create one to get started.</div>';
    return;
  }

  var html = '';
  rulesList.forEach(function(r) {
    var criteria = {};
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
    html += '<button class="btn btn-primary btn-small" onclick="runRule(' + r.id + ')">Run</button>';
    html += '<button class="btn btn-secondary btn-small" onclick="editRule(' + r.id + ')">Edit</button>';
    html += '<button class="btn btn-danger btn-small" onclick="deleteRule(' + r.id + ')">Delete</button>';
    html += '</div>';
    html += '</div>';
  });
  container.innerHTML = html;
}

function buildCriteriaSummary(criteria) {
  var parts = [];
  if (criteria.rating_min) parts.push('Rating >= ' + criteria.rating_min);
  if (criteria.flag && criteria.flag !== 'any') parts.push('Flag: ' + criteria.flag);
  if (criteria.species && criteria.species.length) parts.push('Species: ' + criteria.species.join(', '));
  if (criteria.has_predictions) parts.push('Has predictions');
  if (criteria.folder_ids && criteria.folder_ids.length) parts.push('Folders: ' + criteria.folder_ids.length + ' selected');
  if (criteria.imported_before) parts.push('Before: ' + criteria.imported_before);
  return parts.join(' | ');
}

function editRule(ruleId) {
  var rule = rulesList.find(function(r) { return r.id === ruleId; });
  if (rule) showRuleForm(rule);
}

async function deleteRule(ruleId) {
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

async function runRule(ruleId) {
  var rule = rulesList.find(function(r) { return r.id === ruleId; });
  if (!rule) return;

  var criteria = {};
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

    if (!moveVisibilityReady) return;
    var names = await refreshMoveVisibility(photoIds);
    var keepVisible = document.getElementById('moveKeepVisible').checked;
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
