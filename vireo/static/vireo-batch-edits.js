/* Shared field picker and live selection editor. Requests are serialized by
 * disabling the controls while saving; each committed gesture is undoable. */
window.VireoBatchEdits = (function() {
  'use strict';
  var definitions;
  var opening = false;
  var activeDialog;
  function fields() {
    if (!definitions) definitions = safeFetch('/api/edit-fields', {}, {toast: false})
      .then(function(data) { return data.fields; })
      .catch(function(error) { definitions = null; throw error; });
    return definitions;
  }
  function element(tag, text, parent) {
    var el = document.createElement(tag);
    if (text != null) el.textContent = text;
    if (parent) parent.appendChild(el);
    return el;
  }
  function button(text, parent, action) {
    var el = element('button', text, parent);
    el.type = 'button';
    el.addEventListener('click', action);
    return el;
  }
  function dialog(title) {
    var el = element('dialog', null, document.body);
    el.className = 'development-dialog';
    var heading = element('h2', title, el);
    heading.id = 'developmentTitle' + Math.random().toString(36).slice(2);
    el.setAttribute('aria-labelledby', heading.id);
    // Do not let Browse/editor shortcuts act behind the modal.
    el.addEventListener('keydown', function(event) { event.stopPropagation(); });
    return el;
  }
  async function selectFields(options) {
    var defs = await fields();
    return new Promise(function(resolve) {
      var modal = dialog(options.title || 'Choose development settings');
      element('p', options.hint || 'Only checked settings change. Unchecked settings keep each photo’s existing values. Checked settings that are neutral in the source reset that setting.', modal);
      var chosen = new Set(options.fields || defs.filter(function(f) {
        return f.group !== 'Geometry' && f.path !== 'local';
      }).map(function(f) { return f.path; }));
      var inputs = [];
      var groups = {};
      defs.forEach(function(def) {
        if (options.available && options.available.indexOf(def.path) === -1) return;
        if (!groups[def.group]) {
          var group = element('fieldset', null, modal);
          element('legend', def.group, group);
          groups[def.group] = element('div', null, group);
          groups[def.group].className = 'development-fields';
        }
        var label = element('label', null, groups[def.group]);
        var input = element('input', null, label);
        input.type = 'checkbox'; input.value = def.path; input.checked = chosen.has(def.path);
        element('span', def.label, label);
        inputs.push(input);
      });
      var actions = element('div', null, modal); actions.className = 'development-actions';
      button('Select all', actions, function() { inputs.forEach(function(i) { i.checked = true; }); update(); });
      button('Select none', actions, function() { inputs.forEach(function(i) { i.checked = false; }); update(); });
      button('Cancel', actions, function() { modal.close(); });
      var result = null;
      var apply = button(options.action || 'Apply selected settings', actions, function() {
        result = inputs.filter(function(i) { return i.checked; }).map(function(i) { return i.value; });
        modal.close();
      });
      apply.className = 'primary';
      function update() { apply.disabled = !inputs.some(function(i) { return i.checked; }); }
      inputs.forEach(function(i) { i.addEventListener('change', update); }); update();
      modal.addEventListener('close', function() { modal.remove(); resolve(result); }, {once: true});
      modal.showModal();
    });
  }
  function refresh(data) {
    if (typeof window.vireoRefreshEditRecipeCache === 'function') {
      window.vireoRefreshEditRecipeCache(data.recipes || {});
    }
  }
  function resultMessage(data) {
    var message = 'Updated ' + data.count.toLocaleString() + ' photo' + (data.count === 1 ? '' : 's') + '.';
    if (data.skipped && data.skipped.length) {
      message += ' Skipped ' + data.skipped.length + ' photo(s).';
      var reasons = Object.values(data.local_errors || {});
      if (reasons.length) message += ' ' + Array.from(new Set(reasons)).join('; ');
    }
    return message;
  }
  async function apply(ids, recipe, selectedFields, mode, description) {
    var status = activeDialog && activeDialog.querySelector('.development-status');
    if (status) status.textContent = 'Applying…';
    var data = await safeFetch('/api/photos/edit-recipe/apply', {
      method: 'POST', headers: {'Content-Type': 'application/json'},
      body: JSON.stringify({photo_ids: ids, recipe: recipe, fields: selectedFields, mode: mode || 'merge', description: description}),
    }, {toast: false});
    refresh(data);
    return data;
  }
  async function paste(ids) {
    var copied = window.vireoEditNav && window.vireoEditNav.getCopiedRecipe();
    if (!copied || !copied.recipe) throw new Error('Copy development settings from a photo first.');
    var selectedFields = await selectFields({title: 'Paste development settings to ' + ids.length + ' photos'});
    if (!selectedFields) return null;
    return apply(ids.slice(), copied.recipe, selectedFields, 'merge', 'Pasted selected development settings');
  }
  function put(recipe, path, value) {
    var parts = path.split('.'); var node = recipe;
    parts.slice(0, -1).forEach(function(part) { node = node[part] || (node[part] = {}); });
    node[parts[parts.length - 1]] = value;
  }
  async function open(ids) {
    ids = Array.from(new Set(ids));
    if (!ids.length) return;
    if (opening || activeDialog) return;
    opening = true;
    var defs;
    try { defs = await fields(); } finally { opening = false; }
    var modal = dialog('Edit ' + ids.length.toLocaleString() + ' selected photos');
    activeDialog = modal;
    element('p', 'Changes save to this selection when you release a slider or finish entering a value. Each change can be undone from Edit History. Other settings stay unchanged.', modal);
    var preview = element('div', null, modal); preview.className = 'development-preview';
    ids.slice(0, 6).forEach(function(id) {
      var img = element('img', null, preview); img.dataset.photoId = id; img.alt = 'Selected photo ' + id;
      img.src = '/thumbnails/' + id + '.jpg';
    });
    if (ids.length > 6) element('p', 'Showing 6 of ' + ids.length.toLocaleString() + ' selected photos.', modal);
    var controls = element('fieldset', null, modal);
    element('legend', 'Adjust the selection', controls);
    var row = element('div', null, controls); row.className = 'development-control';
    var setting = element('select', null, row); setting.setAttribute('aria-label', 'Adjustment');
    defs.filter(function(f) { return f.min != null; }).forEach(function(def) {
      var option = element('option', def.label, setting); option.value = def.path;
    });
    var mode = element('select', null, row); mode.setAttribute('aria-label', 'Adjustment mode');
    [['merge', 'Set every photo to'], ['relative', 'Add to each photo']].forEach(function(pair) {
      var option = element('option', pair[1], mode); option.value = pair[0];
    });
    var note = element('p', '', controls);
    var sliderRow = element('div', null, controls); sliderRow.className = 'development-control';
    var slider = element('input', null, sliderRow); slider.type = 'range'; slider.setAttribute('aria-label', 'Adjustment value');
    var number = element('input', null, sliderRow); number.type = 'number'; number.style.width = '90px'; number.setAttribute('aria-label', 'Numeric adjustment value');
    var more = element('fieldset', null, modal); element('legend', 'Reusable settings', more);
    var presetSelect = element('select', null, more); presetSelect.setAttribute('aria-label', 'Batch preset');
    element('option', 'Select a preset…', presetSelect).value = '';
    var presetButton = button('Apply preset…', more, applyPreset); presetButton.disabled = true;
    button('Paste settings…', more, function() { perform(function() { return paste(ids); }); });
    var status = element('div', null, modal); status.className = 'development-status'; status.setAttribute('role', 'status');
    var actions = element('div', null, modal); actions.className = 'development-actions';
    var done = button('Done', actions, function() { modal.close(); });
    var busy = false; var summary = {}; var presets = [];
    function selectedDef() { return defs.find(function(f) { return f.path === setting.value; }); }
    function configure() {
      var def = selectedDef(); var relative = mode.value === 'relative';
      var span = def.max - def.min;
      [slider, number].forEach(function(input) {
        input.min = relative ? -span : def.min; input.max = relative ? span : def.max; input.step = def.step;
      });
      var current = summary[def.path];
      slider.value = relative ? 0 : (typeof current === 'number' ? current : def.default);
      number.value = !relative && current === null ? '' : slider.value;
      number.placeholder = current === null ? 'Mixed' : '';
      note.textContent = relative
        ? 'Adds this amount to each existing value, limited to the supported range. The amount resets after each change.'
        : (current === null ? 'Current values differ. Changing this control sets the same value on every selected photo.' : 'Sets this setting on every selected photo.');
    }
    async function reloadSummary() {
      var data = await safeFetch('/api/photos/edit-recipe/summary', {
        method: 'POST', headers: {'Content-Type': 'application/json'}, body: JSON.stringify({photo_ids: ids}),
      }, {toast: false});
      summary = data.values;
      configure();
    }
    function setBusy(value) { busy = value; controls.disabled = value; more.disabled = value; done.disabled = value; }
    async function perform(action) {
      if (busy) return;
      setBusy(true); status.classList.remove('error'); status.textContent = '';
      try {
        var data = await action();
        if (data) {
          status.textContent = resultMessage(data);
          preview.querySelectorAll('img').forEach(function(img) { img.src = '/thumbnails/' + img.dataset.photoId + '.jpg?v=' + Date.now(); });
          try { await reloadSummary(); } catch (_) { status.textContent += ' Reopen the editor to refresh current values.'; }
        } else status.textContent = '';
      } catch (error) {
        status.textContent = error.message || 'Could not apply settings. Reopen the editor to check current values before retrying.';
        status.classList.add('error');
        // Never automatically retry a relative edit: a lost response can
        // arrive after the server committed the delta.
        try { await reloadSummary(); } catch (_) { /* Keep the original error. */ }
      } finally { setBusy(false); }
    }
    async function commit() {
      if (!number.value || !number.checkValidity()) { number.reportValidity(); return; }
      var value = Number(number.value); var path = setting.value; var operation = mode.value;
      if (operation === 'relative' && value === 0) return;
      var recipe = {}; put(recipe, path, value);
      await perform(function() { return apply(ids, recipe, [path], operation, (operation === 'relative' ? 'Adjusted ' : 'Set ') + selectedDef().label.toLowerCase() + ' on selection'); });
    }
    async function applyPreset() {
      var preset = presets.find(function(p) { return String(p.id) === presetSelect.value; });
      if (!preset) return;
      await perform(async function() {
        var available = preset.fields || defs.filter(function(f) { return f.path.indexOf('adjustments.') === 0; }).map(function(f) { return f.path; });
        var selected = await selectFields({title: 'Apply preset “' + preset.name + '”', fields: available, available: available});
        if (!selected) return null;
        return apply(ids, preset.recipe || {}, selected, 'merge', 'Applied preset ' + preset.name);
      });
    }
    slider.addEventListener('input', function() { number.value = slider.value; });
    slider.addEventListener('change', commit);
    number.addEventListener('change', function() { slider.value = number.value; commit(); });
    number.addEventListener('keydown', function(event) { if (event.key === 'Enter') { event.preventDefault(); number.blur(); } });
    setting.addEventListener('change', configure); mode.addEventListener('change', configure);
    presetSelect.addEventListener('change', function() { presetButton.disabled = !presetSelect.value; });
    modal.addEventListener('cancel', function(event) { if (busy) event.preventDefault(); });
    modal.addEventListener('close', function() { activeDialog = null; modal.remove(); }, {once: true});
    modal.showModal(); setBusy(true);
    try {
      await reloadSummary();
      var data = await safeFetch('/api/edit-presets', {}, {toast: false}); presets = data.presets || [];
      presets.forEach(function(preset) { element('option', preset.name, presetSelect).value = preset.id; });
      setBusy(false);
    } catch (error) {
      status.textContent = error.message || 'Could not load selection settings.'; status.classList.add('error');
      busy = false; done.disabled = false;
    }
  }
  return {fields: fields, selectFields: selectFields, apply: apply, paste: paste, open: open, resultMessage: resultMessage};
})();
