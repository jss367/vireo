// "What counts as identified?": the active workspace's subject types.
// ES module.

import { byId, fetchActiveWorkspace } from './state.js';
import { safeFetch } from './shared.js';

function subjectTypeBoxes() {
  return /** @type {HTMLInputElement[]} */ (Array.from(document.querySelectorAll('input[data-subject-type]')));
}

export async function loadSubjectTypes() {
  // Read the effective subject_types (global defaults merged with workspace
  // overrides) so checkboxes match the actual current behavior. Reading
  // overrides-only with a hardcoded fallback would lie when the user has
  // customized only the global config.
  try {
    var resp = /** @type {{types?: string[]} | null} */ (await safeFetch('/api/workspaces/active/subject-types', {}, { toast: false }));
    var current = (resp && Array.isArray(resp.types)) ? resp.types : [];
    subjectTypeBoxes().forEach(function(cb) {
      cb.checked = current.indexOf(cb.dataset.subjectType || '') !== -1;
    });
  } catch (e) {
    /* leave checkboxes unchecked on error; user can save to set */
  }
}

export async function saveSubjectTypes() {
  var types = subjectTypeBoxes()
    .filter(function(cb) { return cb.checked; })
    .map(function(cb) { return cb.dataset.subjectType; });
  var status = byId('subjectTypesStatus');
  status.textContent = 'Saving...';
  try {
    var ws = await fetchActiveWorkspace();
    await safeFetch('/api/workspaces/' + ws.id + '/subject-types', {
      method: 'PUT',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ types: types }),
    });
    status.textContent = 'Saved.';
    setTimeout(function() { status.textContent = ''; }, 2000);
  } catch (e) {
    status.textContent = 'Save failed: ' + (e instanceof Error && e.message ? e.message : 'unknown');
  }
}
