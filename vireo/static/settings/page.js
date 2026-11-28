/** @param {HTMLElement} header */ function toggleSection(header) {
  header.setAttribute('aria-expanded', String(header.classList.toggle('open')));
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

/** @param {string} id */
function settingsElement(id) {
  return /** @type {HTMLElement} */ (document.getElementById(id));
}

function settingsSearchInput() {
  return /** @type {HTMLInputElement} */ (settingsElement('settingsFindInput'));
}

function openSettingsFind() {
  const input = settingsSearchInput();
  input.focus();
  input.select();
  showSettingsResults();
}

/** @param {HTMLElement} target */
function revealSettingsTarget(target) {
  for (let node = target; node; node = /** @type {HTMLElement} */ (node.parentElement)) {
    if (node instanceof HTMLDetailsElement) node.open = true;
    if (node.classList.contains('collapsible-body')) {
      node.classList.add('open');
      node.previousElementSibling?.classList.add('open');
      node.previousElementSibling?.setAttribute('aria-expanded','true');
    }
    if (node.id === 'advancedContent') node.style.display = '';
  }
  const focus = target.matches('input,button,select,a,summary') ? target :
    target.querySelector('input:not(:disabled),select:not(:disabled),button,a,summary') || target;
  if (focus instanceof HTMLElement) {
    if (focus === target && focus.tabIndex < 0) focus.tabIndex = -1;
    focus.focus({preventScroll:true});
  }
  target.scrollIntoView({block:'center'});
}

function applySettingsAddress() {
  const url = new URL(location.href);
  const id = url.hash.slice(1);
  const target = document.getElementById(id === 'nas-setup' ? 'cfgRemoteTargetsList' : id);
  const category = target?.closest('section')?.dataset.settingsCategory || url.searchParams.get('category') || 'browsing';
  const panels = Array.from(document.querySelectorAll('section[data-settings-category]'));
  const selected = panels.find(p => p.getAttribute('data-settings-category') === category) || panels[0];
  for (const panel of panels) panel.toggleAttribute('hidden', panel !== selected);
  const key = selected?.getAttribute('data-settings-category') || 'browsing';
  for (const link of Array.from(document.querySelectorAll('[data-category-link]'))) {
    if (link.getAttribute('data-category-link') === key) link.setAttribute('aria-current','page');
    else link.removeAttribute('aria-current');
  }
  /** @type {HTMLSelectElement} */ (settingsElement('settingsCategoryPicker')).value = key;
  if (id.startsWith('setting-')) setSettingsScope(url.searchParams.get('scope') || 'global');
  if (target && id !== 'nas-setup') {
    revealSettingsTarget(document.getElementById(id) || target);
  } else window.scrollTo(0,0);
}

/** @param {string} category @param {string} [id] @param {string} [scope] */
function goToSettings(category, id = '', scope = '') {
  const url = new URL(location.href);
  url.searchParams.set('category',category);
  if (scope) url.searchParams.set('scope',scope);
  else url.searchParams.delete('scope');
  url.hash = id;
  if (url.href !== location.href) history.pushState(null,'',url);
  applySettingsAddress();
}

/** @param {HTMLElement} el */
function settingsLabel(el) {
  const row = el.closest('.setting-row');
  const label = row?.querySelector('.setting-label');
  const first = label?.querySelector('label')?.textContent?.trim() || label?.firstChild?.textContent?.trim();
  return el.title || first || label?.textContent?.trim() ||
    el.closest('.section')?.querySelector('.section-title')?.textContent?.trim() || el.id;
}

/** @typedef {{title:string, terms:string, target:HTMLElement, category:string}} SettingsDestination */
/** @param {string} query */
function settingsMatches(query) {
  /** @type {SettingsDestination[]} */ const matches = [];
  const keys = new Set();
  const seen = new Set();
  const schema = ALL_SETTINGS_CACHE.schema || {};
  /** @param {HTMLElement} target @param {string} title @param {string} terms */
  function add(target,title,terms) {
    const panel = target.closest('section[data-settings-category]');
    if (!panel || !title || seen.has(target)) return;
    for (let node = target; node && node !== panel; node = /** @type {HTMLElement} */ (node.parentElement)) {
      if (node.id !== 'advancedContent' && (node.hidden || node.style.display === 'none')) return;
    }
    if (!(title+' '+terms).toLowerCase().includes(query)) return;
    seen.add(target);
    matches.push({title,terms,target,category:panel.getAttribute('data-settings-category') || 'advanced'});
  }
  for (const el of Array.from(document.querySelectorAll(/** @type {'div'} */ ('[data-setting-keys]')))) {
    const owned = (el.dataset.settingKeys || '').split(' ');
    owned.forEach(k => keys.add(k));
    const terms = (el.dataset.searchTerms || '')+' '+owned.map(k => [k,schema[k]?.label,schema[k]?.desc].join(' ')).join(' ');
    const options = el.id === 'cfgCardFieldsContainer' ? el.textContent : '';
    add(el,settingsLabel(el),[terms,options,el.closest('.setting-row')?.querySelector('small')?.textContent].join(' '));
  }
  for (const [key,spec] of Object.entries(schema)) {
    const target = document.getElementById('setting-'+key);
    if (!target) continue;
    if (key.toLowerCase() === query) return [{title:spec.label,terms:key,target,category:'advanced'}];
    if (!keys.has(key)) add(target,spec.label,key+' '+spec.desc);
  }
  for (const el of Array.from(document.querySelectorAll(/** @type {'div'} */ ('section[data-settings-category] > h2,[data-find-name],.section-title,.section button[id],#themePicker,#scanRootsContent > div > span')))) {
    if (el.closest('#allSettingsSection') || el.matches('.tauri-only') && !isTauri()) continue;
    const title = el.dataset.findName || (el.id === 'themePicker' ? 'Theme' : el.textContent?.trim());
    add(el,title || '',el.id==='catalogBackupNowBtn' ? 'backup' : el.dataset.searchTerms || '');
  }
  for (const label of Array.from(document.querySelectorAll(/** @type {'div'} */ ('.setting-row .setting-label')))) {
    const row = label.closest('.setting-row');
    if (!(row instanceof HTMLElement) || row.querySelector('[data-setting-keys],#themePicker')) continue;
    const title = label.firstChild?.textContent?.trim();
    const target = row.querySelector('button,input,select') || row;
    if (target instanceof HTMLElement && title) add(target,title,label.textContent || '');
  }
  const named = '#cfgRemoteTargetsList input[aria-label="Name"],#cfgExternalEditorsList input[placeholder^="Display name"],#cfgFilterShortcutsList input[aria-label="Button text"]';
  for (const el of Array.from(document.querySelectorAll(/** @type {'input'} */ (named)))) {
    add(el,el.value.trim(),'');
  }
  return matches.sort((a,b)=>Number(b.title.toLowerCase().includes(query))-Number(a.title.toLowerCase().includes(query)));
}

function showSettingsResults() {
  const query = settingsSearchInput().value.trim().toLowerCase();
  const box = settingsElement('settingsResults');
  box.replaceChildren();
  box.hidden = !query;
  if (!query) return;
  const matches = settingsMatches(query);
  if (!matches.length) {
    const clear = document.createElement('button');
    clear.textContent = 'No matches for “'+settingsSearchInput().value.trim()+'”. Clear search';
    clear.onclick = () => {settingsSearchInput().value = ''; openSettingsFind();};
    box.append(clear);
  }
  for (const result of matches) {
    const button = document.createElement('button');
    button.textContent = result.title;
    const category = document.createElement('small');
    category.textContent = document.querySelector('[data-category-link="'+result.category+'"]')?.textContent || '';
    button.append(category);
    button.onclick = () => {
      box.hidden = true;
      settingsSearchInput().value = '';
      const key = result.target.dataset.key;
      const scope = key && ALL_SETTINGS_CACHE.schema?.[key]?.scope === 'workspace' ? 'workspace' : '';
      goToSettings(result.category,result.target.id,scope);
      if (!result.target.id) revealSettingsTarget(result.target);
    };
    box.append(button);
  }
}

function setupSettingsFind() {
  const input = settingsSearchInput();
  const box = settingsElement('settingsResults');
  input.addEventListener('input',showSettingsResults);
  input.addEventListener('focus',showSettingsResults);
  document.addEventListener('keydown', event => {
    if ((event.metaKey || event.ctrlKey) && !event.altKey && !event.shiftKey &&
        event.key.toLowerCase() === 'f' && !document.querySelector('dialog:modal')) {
      event.preventDefault();
      openSettingsFind();
    }
  });
  settingsElement('settingsFindPanel').addEventListener('keydown', event => {
    const buttons = Array.from(box.querySelectorAll('button'));
    const index = buttons.indexOf(/** @type {HTMLButtonElement} */ (document.activeElement));
    if (event.key === 'Escape') {input.focus(); box.hidden = true;}
    if (box.hidden || !buttons.length) return;
    if (event.key === 'Enter' && event.target === input) {event.preventDefault(); buttons[0]?.click();}
    if (event.key === 'ArrowDown' || event.key === 'ArrowUp') {
      event.preventDefault();
      buttons[(index + (event.key === 'ArrowDown' ? 1 : -1) + buttons.length) % buttons.length]?.focus();
    }
  });
  document.addEventListener('focusin', event => {
    if (event.target instanceof Element && !event.target.closest('#settingsFindPanel')) box.hidden = true;
  });
  document.querySelectorAll(/** @type {'a'} */ ('[data-category-link]')).forEach(link => {
    link.onclick = event => {
      if (event.metaKey || event.ctrlKey || event.shiftKey || event.altKey) return;
      event.preventDefault(); goToSettings(link.dataset.categoryLink || 'browsing');
    };
  });
  const picker = /** @type {HTMLSelectElement} */ (settingsElement('settingsCategoryPicker'));
  picker.onchange = () => goToSettings(picker.value);
  const updates = new MutationObserver(() => {
    if (document.activeElement === input && input.value && !box.hidden) showSettingsResults();
  });
  updates.observe(/** @type {Element} */ (document.querySelector('.settings-panes')), {subtree:true,childList:true});
  window.addEventListener('popstate',applySettingsAddress);
  applySettingsAddress();
}

function renderOverrideLinks() {
  const cache = ALL_SETTINGS_CACHE;
  if (!cache.schema || !cache.values) return;
  for (const el of Array.from(document.querySelectorAll(/** @type {'div'} */ ('[data-setting-keys]')))) {
    const row = el.closest('.setting-row');
    if (!row) continue;
    row.querySelector('[data-overrides-for="'+el.id+'"]')?.remove();
    const links = document.createElement('div');
    links.className = 'override-links';
    links.dataset.overridesFor = el.id;
    for (const key of (el.dataset.settingKeys || '').split(' ')) {
      const spec = cache.schema[key];
      if (spec?.scope !== 'both' || document.getElementById('wsOverride_'+key)) continue;
      const link = document.createElement('a');
      link.href = '?category=advanced&scope=workspace#setting-'+key;
      if (Object.hasOwn(cache.values.workspace,key)) {
        const qualifier = el.dataset.settingKeys?.includes(' ') ? spec.label+' · ' : '';
        link.textContent = qualifier+settingsWorkspaceName+' uses '+cache.values.workspace[key]+' · Edit override';
      } else {
        link.className = 'inherited';
        link.textContent = 'Override for this workspace';
      }
      link.setAttribute('aria-label',link.textContent+' — '+spec.label);
      link.onclick = event => {event.preventDefault();goToSettings('advanced','setting-'+key,'workspace');};
      links.append(link);
    }
    if (links.childElementCount) row.append(links);
  }
}
