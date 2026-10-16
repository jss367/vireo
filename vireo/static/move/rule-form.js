// The rule editor form: criteria, preview, and save.
// Classic page script; load boot.js after all definitions.

function showRuleForm(rule) {
  editingRuleId = rule ? rule.id : null;
  var criteria = {};
  if (rule) {
    try { criteria = typeof rule.criteria === 'string' ? JSON.parse(rule.criteria) : (rule.criteria || {}); } catch(e) {}
  }

  var html = '<div class="rule-form">';
  html += '<h4>' + (editingRuleId ? 'Edit Rule' : 'New Rule') + '</h4>';

  html += '<div class="form-row">';
  html += '<label>Name</label>';
  html += '<input type="text" id="ruleNameInput" value="' + escapeAttr(rule ? rule.name : '') + '" placeholder="e.g. Archive hawks">';
  html += '</div>';

  html += '<div class="form-row">';
  html += '<label>Destination</label>';
  html += '<div class="dest-row">';
  html += '<input type="text" id="ruleDestInput" value="' + escapeAttr(rule ? rule.destination : '') + '" placeholder="/path/to/destination">';
  html += '<button class="btn btn-secondary btn-small" onclick="openBrowser(\'ruleDestInput\')">Browse</button>';
  html += '</div>';
  html += '</div>';

  html += '<div class="criteria-grid">';

  html += '<div class="criteria-field">';
  html += '<label>Minimum Rating</label>';
  html += '<select id="ruleCriteriaRating">';
  html += '<option value="">Any</option>';
  for (var i = 1; i <= 5; i++) {
    var sel = (criteria.rating_min === i) ? ' selected' : '';
    html += '<option value="' + i + '"' + sel + '>' + i + '</option>';
  }
  html += '</select>';
  html += '</div>';

  html += '<div class="criteria-field">';
  html += '<label>Flag</label>';
  html += '<select id="ruleCriteriaFlag">';
  var flagOpts = [['', 'Any'], ['flagged', 'Flagged'], ['rejected', 'Rejected'], ['none', 'Unflagged']];
  flagOpts.forEach(function(opt) {
    var sel = (criteria.flag === opt[0] || (!criteria.flag && opt[0] === '')) ? ' selected' : '';
    html += '<option value="' + opt[0] + '"' + sel + '>' + opt[1] + '</option>';
  });
  html += '</select>';
  html += '</div>';

  html += '<div class="criteria-field">';
  html += '<label>Species (comma-separated)</label>';
  html += '<input type="text" id="ruleCriteriaSpecies" value="' + escapeAttr(criteria.species ? criteria.species.join(', ') : '') + '" placeholder="e.g. Red-tailed Hawk, Bald Eagle">';
  html += '</div>';

  html += '<div class="criteria-field">';
  html += '<label>Has Predictions</label>';
  html += '<div class="checkbox-row">';
  html += '<input type="checkbox" id="ruleCriteriaHasPred"' + (criteria.has_predictions ? ' checked' : '') + '>';
  html += '<span style="font-size:12px;color:var(--text-secondary);">Only photos with predictions</span>';
  html += '</div>';
  html += '</div>';

  html += '<div class="criteria-field">';
  html += '<label>Source Folders</label>';
  html += '<select id="ruleCriteriaFolders" multiple style="height:80px;">';
  allFolders.forEach(function(f) {
    var sel = (criteria.folder_ids && criteria.folder_ids.indexOf(f.id) >= 0) ? ' selected' : '';
    html += '<option value="' + f.id + '"' + sel + '>' + escapeHtml(f.path) + '</option>';
  });
  html += '</select>';
  html += '<span style="font-size:11px;color:var(--text-dim);">Hold Ctrl/Cmd to select multiple</span>';
  html += '</div>';

  html += '<div class="criteria-field">';
  html += '<label>Imported Before</label>';
  html += '<input type="date" id="ruleCriteriaImportedBefore" value="' + escapeAttr(criteria.imported_before || '') + '">';
  html += '</div>';

  html += '</div>'; // criteria-grid

  html += '<div id="rulePreviewResult"></div>';

  html += '<div class="form-actions">';
  html += '<button class="btn btn-primary" onclick="saveRule()">Save Rule</button>';
  html += '<button class="btn btn-secondary" onclick="previewRule()">Preview</button>';
  html += '<button class="btn btn-secondary" onclick="hideRuleForm()">Cancel</button>';
  html += '</div>';

  html += '</div>';

  document.getElementById('ruleFormContainer').innerHTML = html;
  document.getElementById('ruleFormContainer').style.display = 'block';
}

function hideRuleForm() {
  document.getElementById('ruleFormContainer').style.display = 'none';
  document.getElementById('ruleFormContainer').innerHTML = '';
  editingRuleId = null;
}

function gatherCriteria() {
  var criteria = {};
  var rating = document.getElementById('ruleCriteriaRating').value;
  if (rating) criteria.rating_min = parseInt(rating);
  var flag = document.getElementById('ruleCriteriaFlag').value;
  if (flag) criteria.flag = flag;
  var species = document.getElementById('ruleCriteriaSpecies').value.trim();
  if (species) {
    criteria.species = species.split(',').map(function(s) { return s.trim(); }).filter(function(s) { return s; });
  }
  if (document.getElementById('ruleCriteriaHasPred').checked) {
    criteria.has_predictions = true;
  }
  var folderSelect = document.getElementById('ruleCriteriaFolders');
  if (folderSelect) {
    var selectedFolders = Array.from(folderSelect.selectedOptions).map(function(o) { return parseInt(o.value); });
    if (selectedFolders.length) criteria.folder_ids = selectedFolders;
  }
  var importedBefore = document.getElementById('ruleCriteriaImportedBefore');
  if (importedBefore && importedBefore.value) {
    criteria.imported_before = importedBefore.value;
  }
  return criteria;
}

async function previewRule() {
  var criteria = gatherCriteria();
  var resultDiv = document.getElementById('rulePreviewResult');
  resultDiv.innerHTML = '<div class="loading">Checking matches...</div>';

  try {
    var resp = await safeFetch('/api/move-rules/preview', {
      method: 'POST',
      headers: {'Content-Type': 'application/json'},
      body: JSON.stringify({criteria: criteria}),
    });
    var count = resp.photo_ids ? resp.photo_ids.length : 0;
    resultDiv.innerHTML = '<div class="preview-result">' + count + ' photo' + (count !== 1 ? 's' : '') + ' match this criteria</div>';
  } catch (e) {
    resultDiv.innerHTML = '<div class="preview-result" style="color:var(--danger);">Preview failed</div>';
  }
}

async function saveRule() {
  var name = document.getElementById('ruleNameInput').value.trim();
  var dest = document.getElementById('ruleDestInput').value.trim();
  if (!name || !dest) {
    showToast('Name and destination are required', 'error');
    return;
  }
  var criteria = gatherCriteria();

  try {
    if (editingRuleId) {
      await safeFetch('/api/move-rules/' + editingRuleId, {
        method: 'PUT',
        headers: {'Content-Type': 'application/json'},
        body: JSON.stringify({name: name, destination: dest, criteria: criteria}),
      });
      showToast('Rule updated', 'success');
    } else {
      await safeFetch('/api/move-rules', {
        method: 'POST',
        headers: {'Content-Type': 'application/json'},
        body: JSON.stringify({name: name, destination: dest, criteria: criteria}),
      });
      showToast('Rule created', 'success');
    }
    hideRuleForm();
    loadRules();
  } catch (e) {
    // safeFetch handles error toast
  }
}
