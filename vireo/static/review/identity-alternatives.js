// Dedicated unresolved identities: candidate alternatives never count as
// several species actually present. The catalog owns these review groups.
(function() {
  /** @typedef {{group_id:string,candidates:string[],reason:string,missing_evidence:string,photo_ids:number[]}} IdentityGroup */
  /** @type {IdentityGroup[]} */ var groups = [];
  var epoch = 0;
  var busy = false;
  /** @type {string|null} */ var editing = null;

  /** @param {string} id */
  function element(id) { return /** @type {HTMLElement} */ (document.getElementById(id)); }
  /** @param {string} id */
  function field(id) { return /** @type {HTMLTextAreaElement} */ (element(id)); }
  function photos() { return /** @type {HTMLSelectElement} */ (element('identityPhotos')); }
  /** @param {string} message */
  function status(message) { element('identityAlternativesStatus').textContent = message; }
  function selectedCount() {
    element('identityPhotoCount').textContent = photos().selectedOptions.length + ' photos selected';
  }
  function reset() {
    editing = null;
    element('identityCancelEdit').hidden = true;
    /** @type {HTMLFormElement} */ (element('identityAlternativesForm')).reset();
    photos().replaceChildren();
    selectedCount();
  }
  /** @param {number} id @param {string} label @param {boolean} selected */
  function addPhotoOption(id, label, selected) {
    var option = document.createElement('option');
    option.value = String(id);
    option.textContent = label;
    option.selected = selected;
    photos().appendChild(option);
  }
  function choosePhotos() {
    if (busy || _predictionsReloading) return;
    reset();
    var visible = getVisibleItems();
    var ids = new Set(visible.map(function(p) { return p.photo_id; }));
    var bursts = new Set(visible.map(function(p) { return p.group_id; }).filter(Boolean));
    var seen = new Set();
    predictions.forEach(function(p) {
      if ((!ids.has(p.photo_id) && !bursts.has(p.group_id)) || seen.has(p.photo_id)) return;
      seen.add(p.photo_id);
      addPhotoOption(p.photo_id, p.filename || 'Photo ' + p.photo_id, false);
    });
    status(seen.size + ' photos listed. Select the frames that share the uncertain identity.');
  }
  /** @param {IdentityGroup} group */
  function editGroup(group) {
    if (busy) return;
    reset();
    editing = group.group_id;
    element('identityCancelEdit').hidden = false;
    group.photo_ids.forEach(function(id) { addPhotoOption(id, 'Photo ' + id, true); });
    field('identityCandidates').value = group.candidates.join('\n');
    field('identityReason').value = group.reason;
    field('identityEvidence').value = group.missing_evidence;
    selectedCount();
    field('identityCandidates').focus();
  }
  /** @param {string} label @param {()=>unknown} action */
  function button(label, action) {
    var el = document.createElement('button');
    el.type = 'button';
    el.textContent = label;
    el.disabled = busy;
    el.addEventListener('click', action);
    return el;
  }
  function render() {
    var root = element('identityAlternativesGroups');
    root.replaceChildren();
    groups.forEach(function(group) {
      var article = document.createElement('article');
      article.style.cssText = 'padding:12px 0;border-top:1px solid var(--border,#444);';
      var heading = document.createElement('h3');
      heading.textContent = group.candidates.join(' or ') + ' — unresolved';
      article.appendChild(heading);
      var note = document.createElement('p');
      note.textContent = group.photo_ids.length + ' photos. ' + group.reason +
        (group.missing_evidence ? ' Missing evidence: ' + group.missing_evidence : '');
      article.appendChild(note);
      var members = document.createElement('details');
      var summary = document.createElement('summary');
      summary.textContent = 'Inspect photos';
      members.appendChild(summary);
      group.photo_ids.forEach(function(id) {
        var link = document.createElement('a');
        link.href = '/review?photo_id=' + id;
        link.textContent = 'Photo ' + id + ' ';
        members.appendChild(link);
      });
      article.appendChild(members);
      article.appendChild(button('Edit alternatives and evidence', function() { editGroup(group); }));
      group.candidates.forEach(function(name) {
        article.appendChild(button('Resolve as ' + name, function() {
          write('/api/identity-alternatives/resolve', {group_id: group.group_id, photo_ids: group.photo_ids, species: name});
        }));
      });
      root.appendChild(article);
    });
  }
  async function load() {
    var requestEpoch = ++epoch;
    try {
      var result = /** @type {{groups:IdentityGroup[],photo_count:number}} */ (await safeFetch('/api/identity-alternatives', {}, {toast: false}));
      if (requestEpoch !== epoch) return;
      groups = result.groups;
      element('identityAlternativesCount').textContent = '(' + result.photo_count + ' photos in workspace)';
      render();
    } catch (error) {
      if (requestEpoch === epoch) status('Could not load unresolved species groups. Reopen this panel to retry.');
    }
  }
  /** @param {string} url @param {Record<string,unknown>} body */
  async function write(url, body) {
    if (busy) return;
    busy = true;
    ++epoch;
    /** @type {HTMLButtonElement} */ (element('identitySave')).disabled = true;
    render();
    status('Saving identification…');
    try {
      var result = /** @type {{warning?:string}} */ (await safeFetch(url, {
        method: 'POST', headers: {'Content-Type': 'application/json'}, body: JSON.stringify(body),
      }));
      reset();
      status(result.warning || 'Identification saved. Undo is available in edit history.');
      if (result.warning) showToast(result.warning, 'warning');
      document.dispatchEvent(new CustomEvent('vireo:edit-history-changed'));
    } catch (error) {
      status('Could not confirm the save. Reload the group before trying again.');
    } finally {
      busy = false;
      /** @type {HTMLButtonElement} */ (element('identitySave')).disabled = false;
      await load();
    }
  }
  document.addEventListener('DOMContentLoaded', function() {
    element('identityLoadPhotos').addEventListener('click', choosePhotos);
    element('identitySelectPhotos').addEventListener('click', function() {
      if (busy) return;
      Array.from(photos().options).forEach(function(option) { option.selected = true; });
      selectedCount();
    });
    photos().addEventListener('change', selectedCount);
    element('identityCancelEdit').addEventListener('click', reset);
    element('identityAlternativesForm').addEventListener('submit', function(event) {
      event.preventDefault();
      write('/api/identity-alternatives', {
        group_id: editing, photo_ids: Array.from(photos().selectedOptions).map(function(option) { return Number(option.value); }),
        candidates: field('identityCandidates').value.split('\n').filter(function(name) { return name.trim(); }),
        reason: field('identityReason').value, missing_evidence: field('identityEvidence').value,
      });
    });
    element('identityAlternativesPanel').addEventListener('toggle', function() {
      if (!busy) load();
    });
    document.addEventListener('vireo:edit-history-changed', function() { if (!busy) load(); });
    load();
  });
})();
