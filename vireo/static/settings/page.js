/** @type {{marks:HTMLElement[];activeIndex:number;observer:MutationObserver|null;refreshTimer:ReturnType<typeof setTimeout>|null}} */ var SETTINGS_FIND_STATE = {
  marks: [],
  activeIndex: -1,
  observer: null,
  refreshTimer: null,
};

function setupSettingsFind() {
  var input = /** @type {HTMLInputElement} */ (document.getElementById('settingsFindInput'));
  if (!input) return;
  input.addEventListener('input', function() {
    refreshSettingsFind(this.value, 0);
  });
  input.addEventListener('keydown', function(e) {
    if (e.key === 'Enter') {
      e.preventDefault();
      moveSettingsFind(e.shiftKey ? -1 : 1);
    } else if (e.key === 'Escape') {
      e.preventDefault();
      closeSettingsFind();
    }
  });
  document.addEventListener('keydown', function(e) {
    var key = (e.key || '').toLowerCase();
    if ((e.metaKey || e.ctrlKey) && !e.altKey && key === 'f') {
      e.preventDefault();
      openSettingsFind();
    } else if (key === 'escape' && isSettingsFindOpen()) {
      e.preventDefault();
      closeSettingsFind();
    }
  }, true);
}

function isSettingsFindOpen() {
  var panel = /** @type {HTMLElement} */ (document.getElementById('settingsFindPanel'));
  return !!panel && panel.classList.contains('open');
}

function openSettingsFind() {
  var panel = /** @type {HTMLElement} */ (document.getElementById('settingsFindPanel'));
  var input = /** @type {HTMLInputElement} */ (document.getElementById('settingsFindInput'));
  if (!panel || !input) return;
  panel.classList.add('open');
  input.focus();
  input.select();
  refreshSettingsFind(input.value, SETTINGS_FIND_STATE.activeIndex >= 0 ? SETTINGS_FIND_STATE.activeIndex : 0);
}

function closeSettingsFind() {
  var panel = /** @type {HTMLElement} */ (document.getElementById('settingsFindPanel'));
  var input = /** @type {HTMLInputElement} */ (document.getElementById('settingsFindInput'));
  if (panel) panel.classList.remove('open');
  if (input) input.value = String('');
  disconnectSettingsFindObserver();
  clearTimeout(SETTINGS_FIND_STATE.refreshTimer ?? undefined);
  SETTINGS_FIND_STATE.refreshTimer = null;
  clearSettingsFindMarks();
  updateSettingsFindStatus();
}

function getSettingsFindRoot() {
  return document.querySelector(/** @type {'div'} */ ('.content'));
}

function disconnectSettingsFindObserver() {
  if (SETTINGS_FIND_STATE.observer) {
    SETTINGS_FIND_STATE.observer.disconnect();
    SETTINGS_FIND_STATE.observer = null;
  }
}

function connectSettingsFindObserver() {
  var root = getSettingsFindRoot();
  var input = /** @type {HTMLInputElement} */ (document.getElementById('settingsFindInput'));
  if (!root || !input || !input.value.trim()) return;
  SETTINGS_FIND_STATE.observer = new MutationObserver(function() {
    clearTimeout(SETTINGS_FIND_STATE.refreshTimer ?? undefined);
    SETTINGS_FIND_STATE.refreshTimer = setTimeout(function() {
      refreshSettingsFind(input.value, SETTINGS_FIND_STATE.activeIndex);
    }, 100);
  });
  SETTINGS_FIND_STATE.observer.observe(root, { childList: true, subtree: true, characterData: true });
}

function clearSettingsFindMarks() {
  disconnectSettingsFindObserver();
  document.querySelectorAll(/** @type {'div'} */ ('mark.settings-find-mark')).forEach(function(mark) {
    var parent = mark.parentNode;
    if (!parent) return;
    parent.replaceChild(document.createTextNode(mark.textContent), mark);
    parent.normalize();
  });
  SETTINGS_FIND_STATE.marks = [];
  SETTINGS_FIND_STATE.activeIndex = -1;
}

/** @param {Node} node */ function shouldSkipSettingsFindTextNode(node) {
  if (!node.nodeValue || !node.nodeValue.trim()) return true;
  var el = node.parentElement;
  if (!el) return true;
  if (el.closest(/** @type {'div'} */ ('.settings-find-panel'))) return true;
  if (el.closest(/** @type {'div'} */ ('script, style, noscript, textarea, input, select, option, mark.settings-find-mark'))) return true;
  return isSettingsFindTextHidden(el);
}

/** @param {Element|null} el */ function isSettingsFindTextHidden(el) {
  var root = getSettingsFindRoot();
  var current = el;
  while (current && current !== root) {
    if (
      !current.classList.contains('collapsible-body') &&
      current.id !== 'advancedContent'
    ) {
      var style = window.getComputedStyle(current);
      if (style.display === 'none' || style.visibility === 'hidden') return true;
    }
    if (current.hasAttribute('hidden') || current.getAttribute('aria-hidden') === 'true') return true;
    current = current.parentElement;
  }
  return false;
}

/** @param {string} query @param {number=} preferredIndex */ function refreshSettingsFind(query, preferredIndex) {
  var root = getSettingsFindRoot();
  if (!root) return;
  disconnectSettingsFindObserver();
  clearSettingsFindMarks();
  var q = (query || '').trim();
  if (!q) {
    updateSettingsFindStatus();
    return;
  }

  var qLower = q.toLowerCase();
  var walker = document.createTreeWalker(root, NodeFilter.SHOW_TEXT, {
    acceptNode: function(node) {
      if (shouldSkipSettingsFindTextNode(node)) return NodeFilter.FILTER_REJECT;
      return (node.nodeValue || '').toLowerCase().indexOf(qLower) !== -1
        ? NodeFilter.FILTER_ACCEPT
        : NodeFilter.FILTER_REJECT;
    }
  });
  var nodes = [];
  var current;
  while ((current = walker.nextNode())) nodes.push(current);

  nodes.forEach(function(node) {
    var text = node.nodeValue || '';
    var lower = text.toLowerCase();
    var frag = document.createDocumentFragment();
    var pos = 0;
    var idx = lower.indexOf(qLower, pos);
    while (idx !== -1) {
      if (idx > pos) frag.appendChild(document.createTextNode(text.slice(pos, idx)));
      var mark = document.createElement('mark');
      mark.className = 'settings-find-mark';
      mark.textContent = String(text.slice(idx, idx + q.length));
      frag.appendChild(mark);
      SETTINGS_FIND_STATE.marks.push(mark);
      pos = idx + q.length;
      idx = lower.indexOf(qLower, pos);
    }
    if (pos < text.length) frag.appendChild(document.createTextNode(text.slice(pos)));
    node.parentNode?.replaceChild(frag, node);
  });

  if (SETTINGS_FIND_STATE.marks.length) {
    setSettingsFindActive(Math.max(0, Math.min(preferredIndex || 0, SETTINGS_FIND_STATE.marks.length - 1)));
  } else {
    SETTINGS_FIND_STATE.activeIndex = -1;
    updateSettingsFindStatus();
  }
  connectSettingsFindObserver();
}

/** @param {number} index */ function setSettingsFindActive(index) {
  var marks = SETTINGS_FIND_STATE.marks;
  marks.forEach(function(mark) { mark.classList.remove('active'); });
  if (!marks.length) {
    SETTINGS_FIND_STATE.activeIndex = -1;
    updateSettingsFindStatus();
    return;
  }
  SETTINGS_FIND_STATE.activeIndex = ((index % marks.length) + marks.length) % marks.length;
  var active = marks[SETTINGS_FIND_STATE.activeIndex]; if (!active) return;
  active.classList.add('active');
  revealSettingsFindMatch(active);
  updateSettingsFindStatus();
}

/** @param {HTMLElement} mark */ function revealSettingsFindMatch(mark) {
  var body = mark.closest(/** @type {'div'} */ ('.collapsible-body'));
  while (body) {
    body.classList.add('open');
    var header = body.previousElementSibling;
    if (header && header.classList.contains('collapsible-header')) header.classList.add('open');
    body = body.parentElement ? body.parentElement.closest(/** @type {'div'} */ ('.collapsible-body')) : null;
  }
  var advanced = mark.closest(/** @type {'div'} */ ('#advancedContent'));
  if (advanced) {
    advanced.style.display = '';
    var advancedTitle = advanced.previousElementSibling;
    var icon = advancedTitle ? advancedTitle.querySelector(/** @type {'div'} */ ('span')) : null;
    if (icon) icon.textContent = String('\u25BC');
  }
  mark.scrollIntoView({ block: 'center', inline: 'nearest' });
}

/** @param {number} delta */ function moveSettingsFind(delta) {
  var input = /** @type {HTMLInputElement} */ (document.getElementById('settingsFindInput'));
  if (!isSettingsFindOpen()) openSettingsFind();
  if (!SETTINGS_FIND_STATE.marks.length && input && input.value.trim()) {
    refreshSettingsFind(input.value, 0);
  }
  if (!SETTINGS_FIND_STATE.marks.length) return;
  setSettingsFindActive(SETTINGS_FIND_STATE.activeIndex + delta);
}

function updateSettingsFindStatus() {
  var status = /** @type {HTMLElement} */ (document.getElementById('settingsFindStatus'));
  if (!status) return;
  var total = SETTINGS_FIND_STATE.marks.length;
  if (!total) {
    status.textContent = String('0 results');
  } else {
    status.textContent = String((SETTINGS_FIND_STATE.activeIndex + 1) + ' of ' + total);
  }
}

/** @param {HTMLElement} header */ function toggleSection(header) {
  header.classList.toggle('open');
  var body = header.nextElementSibling;
  body?.classList.toggle('open');
}

/* ---------- Theme Picker ---------- */
function loadThemePicker() {
  var themes = window.getThemeList ? window.getThemeList() : [];
  var current = document.documentElement.getAttribute('data-theme') || 'vireo-dark';
  var picker = /** @type {HTMLElement} */ (document.getElementById('themePicker'));
  if (!picker || themes.length === 0) return;

  var html = '';
  themes.forEach(function(t) {
    var isActive = t.id === current;
    var borderColor = isActive ? 'var(--accent)' : 'var(--border-secondary)';
    var bg = isActive ? 'var(--bg-tertiary)' : 'var(--bg-input)';
    html += '<button onclick="setTheme(\'' + t.id + '\'); loadThemePicker();" ' +
      'style="background:' + bg + ';color:var(--text-primary);border:2px solid ' + borderColor + ';' +
      'border-radius:6px;padding:8px 14px;font-size:12px;cursor:pointer;min-width:100px;text-align:center;">' +
      '<span style="font-size:18px;display:block;margin-bottom:2px;">' + t.icon + '</span>' +
      t.name +
    '</button>';
  });
  picker.innerHTML = html;
}

/** @param {unknown} error */
function settingsErrorMessage(error) {
  return error && typeof error === 'object' && 'message' in error && typeof error.message === 'string' ? error.message : '';
}
