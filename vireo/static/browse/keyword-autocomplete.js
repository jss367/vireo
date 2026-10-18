/* Browse: keyword autocomplete for the keyword inputs. ES module (see
   browse/README.md). */
import { state } from './state.js';
import { escapeHtml, safeFetch } from './shared.js';

export function invalidateKeywordAutocompleteCache() {
  state.keywordAutocompleteCache = null;
  state.keywordAutocompletePromise = null;
}

export async function loadKeywordAutocompleteOptions(/** @type {boolean | undefined} */ force = undefined) {
  if (!force && state.keywordAutocompleteCache !== null) return state.keywordAutocompleteCache;
  if (!force && state.keywordAutocompletePromise) return state.keywordAutocompletePromise;
  state.keywordAutocompletePromise = safeFetch('/api/keywords/all', {}, { toast: false })
    .then(function(/** @type {BrowseKeyword[]} */ rows) {
      state.keywordAutocompleteCache = (rows || [])
        .filter(function(k) { return k && k.name; })
        .map(function(k) {
          return {
            id: k.id,
            name: k.name,
            type: k.type || 'general',
            place_id: k.place_id || null,
            photo_count: k.photo_count || 0,
            search: String(k.name || '').toLowerCase(),
          };
        })
        .sort(function(a, b) {
          return a.search.localeCompare(b.search) || a.id - b.id;
        });
      return state.keywordAutocompleteCache || [];
    })
    .catch(function() {
      return [];
    })
    .finally(function() {
      state.keywordAutocompletePromise = null;
    });
  return state.keywordAutocompletePromise;
}

export function getKeywordAutocompleteState(/** @type {string} */ inputId) {
  if (!state.keywordAutocompleteStates[inputId]) {
    state.keywordAutocompleteStates[inputId] = {
      matches: [],
      activeIndex: -1,
      selectedKeyword: null,
      dropdownId: '',
      submitFn: null,
    };
  }
  return state.keywordAutocompleteStates[inputId];
}

export function keywordMatchScore(/** @type {string} */ search, /** @type {string} */ query) {
  if (search === query) return 0;
  if (search.indexOf(query) === 0) return 1;
  if (search.split(/[\s\-_/]+/).some(function(part) { return part.indexOf(query) === 0; })) return 2;
  var idx = search.indexOf(query);
  return idx === -1 ? 99 : 3 + idx / 1000;
}

export function hideKeywordSuggestions(/** @type {string} */ inputId) {
  var acState = getKeywordAutocompleteState(inputId);
  const dropdown = document.getElementById(acState.dropdownId);
  if (dropdown) {
    dropdown.classList.remove('open');
    dropdown.innerHTML = '';
  }
  acState.matches = [];
  acState.activeIndex = -1;
}

function renderKeywordSuggestions(/** @type {string} */ inputId) {
  const input = /** @type {HTMLInputElement | null} */ (document.getElementById(inputId));
  var acState = getKeywordAutocompleteState(inputId);
  const dropdown = document.getElementById(acState.dropdownId);
  if (!input || !dropdown) return;
  var query = input.value.trim().toLowerCase();
  acState.selectedKeyword = null;
  if (!query || !state.keywordAutocompleteCache || !state.keywordAutocompleteCache.length) {
    hideKeywordSuggestions(inputId);
    return;
  }

  acState.matches = state.keywordAutocompleteCache
    .map(function(k) {
      return { keyword: k, score: keywordMatchScore(k.search, query) };
    })
    .filter(function(item) { return item.score < 99; })
    .sort(function(a, b) {
      return a.score - b.score || a.keyword.search.localeCompare(b.keyword.search) || a.keyword.id - b.keyword.id;
    })
    .slice(0, 8)
    .map(function(item) { return item.keyword; });

  if (!acState.matches.length) {
    hideKeywordSuggestions(inputId);
    return;
  }
  if (acState.activeIndex < 0 || acState.activeIndex >= acState.matches.length) acState.activeIndex = 0;

  dropdown.innerHTML = acState.matches.map(function(k, idx) {
    var active = idx === acState.activeIndex ? ' active' : '';
    var count = k.photo_count === 1 ? '1 photo' : k.photo_count + ' photos';
    return '<div class="keyword-suggestion-option' + active + '" role="option" data-index="' + idx + '">' +
      '<span class="keyword-suggestion-name">' + escapeHtml(k.name) + '</span>' +
      '<span class="keyword-suggestion-meta">' + escapeHtml(count) + '</span>' +
      '</div>';
  }).join('');
  dropdown.classList.add('open');
}

function chooseKeywordSuggestion(/** @type {string} */ inputId, /** @type {number} */ index) {
  const input = /** @type {HTMLInputElement | null} */ (document.getElementById(inputId));
  var acState = getKeywordAutocompleteState(inputId);
  var keyword = acState.matches[index];
  if (!input || !keyword) return;
  input.value = keyword.name;
  acState.selectedKeyword = keyword;
  hideKeywordSuggestions(inputId);
  if (typeof acState.submitFn === 'function') {
    acState.submitFn(keyword);
  }
}

export function bindKeywordAutocomplete(/** @type {string} */ inputId, /** @type {string} */ dropdownId, /** @type {(keyword: BrowseKeywordOption | null) => void} */ submitFn) {
  const input = /** @type {HTMLInputElement | null} */ (document.getElementById(inputId));
  const dropdown = document.getElementById(dropdownId);
  if (!input || !dropdown) return;
  var acState = getKeywordAutocompleteState(inputId);
  acState.dropdownId = dropdownId;
  acState.submitFn = submitFn;

  input.addEventListener('focus', function() {
    loadKeywordAutocompleteOptions().then(function() { renderKeywordSuggestions(inputId); });
  });
  input.addEventListener('input', function() {
    var s = getKeywordAutocompleteState(inputId);
    s.activeIndex = 0;
    s.selectedKeyword = null;
    loadKeywordAutocompleteOptions().then(function() { renderKeywordSuggestions(inputId); });
  });
  input.addEventListener('keydown', function(e) {
    var s = getKeywordAutocompleteState(inputId);
    var isOpen = dropdown.classList.contains('open') && s.matches.length > 0;
    if (isOpen && e.key === 'ArrowDown') {
      e.preventDefault();
      s.activeIndex = (s.activeIndex + 1) % s.matches.length;
      renderKeywordSuggestions(inputId);
      return;
    }
    if (isOpen && e.key === 'ArrowUp') {
      e.preventDefault();
      s.activeIndex = (s.activeIndex - 1 + s.matches.length) % s.matches.length;
      renderKeywordSuggestions(inputId);
      return;
    }
    if (e.key === 'Escape') {
      hideKeywordSuggestions(inputId);
      return;
    }
    if (e.key === 'Enter') {
      e.preventDefault();
      if (isOpen && s.activeIndex >= 0) chooseKeywordSuggestion(inputId, s.activeIndex);
      else if (typeof submitFn === 'function') submitFn(null);
    }
  });
  input.addEventListener('blur', function() {
    setTimeout(function() { hideKeywordSuggestions(inputId); }, 120);
  });
  dropdown.addEventListener('mousedown', function(e) {
    var option = e.target instanceof Element ? /** @type {HTMLElement | null} */ (/** @type {HTMLElement | null} */ (e.target.closest('.keyword-suggestion-option'))) : null;
    if (!option) return;
    e.preventDefault();
    chooseKeywordSuggestion(inputId, parseInt(option.dataset.index || '', 10));
  });
}
