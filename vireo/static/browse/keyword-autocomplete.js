/* Browse: keyword autocomplete for the keyword inputs.
   Classic script sharing page globals; browse.html loads the
   browse/*.js files in dependency order. */

function invalidateKeywordAutocompleteCache() {
  keywordAutocompleteCache = null;
  keywordAutocompletePromise = null;
}

async function loadKeywordAutocompleteOptions(force) {
  if (!force && keywordAutocompleteCache !== null) return keywordAutocompleteCache;
  if (!force && keywordAutocompletePromise) return keywordAutocompletePromise;
  keywordAutocompletePromise = safeFetch('/api/keywords/all', {}, { toast: false })
    .then(function(rows) {
      keywordAutocompleteCache = (rows || [])
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
      return keywordAutocompleteCache;
    })
    .catch(function() {
      return [];
    })
    .finally(function() {
      keywordAutocompletePromise = null;
    });
  return keywordAutocompletePromise;
}

function getKeywordAutocompleteState(inputId) {
  if (!keywordAutocompleteStates[inputId]) {
    keywordAutocompleteStates[inputId] = {
      matches: [],
      activeIndex: -1,
      selectedKeyword: null,
      dropdownId: '',
      submitFn: null,
    };
  }
  return keywordAutocompleteStates[inputId];
}

function keywordMatchScore(search, query) {
  if (search === query) return 0;
  if (search.indexOf(query) === 0) return 1;
  if (search.split(/[\s\-_/]+/).some(function(part) { return part.indexOf(query) === 0; })) return 2;
  var idx = search.indexOf(query);
  return idx === -1 ? 99 : 3 + idx / 1000;
}

function hideKeywordSuggestions(inputId) {
  var state = getKeywordAutocompleteState(inputId);
  var dropdown = document.getElementById(state.dropdownId);
  if (dropdown) {
    dropdown.classList.remove('open');
    dropdown.innerHTML = '';
  }
  state.matches = [];
  state.activeIndex = -1;
}

function renderKeywordSuggestions(inputId) {
  var input = document.getElementById(inputId);
  var state = getKeywordAutocompleteState(inputId);
  var dropdown = document.getElementById(state.dropdownId);
  if (!input || !dropdown) return;
  var query = input.value.trim().toLowerCase();
  state.selectedKeyword = null;
  if (!query || !keywordAutocompleteCache || !keywordAutocompleteCache.length) {
    hideKeywordSuggestions(inputId);
    return;
  }

  state.matches = keywordAutocompleteCache
    .map(function(k) {
      return { keyword: k, score: keywordMatchScore(k.search, query) };
    })
    .filter(function(item) { return item.score < 99; })
    .sort(function(a, b) {
      return a.score - b.score || a.keyword.search.localeCompare(b.keyword.search) || a.keyword.id - b.keyword.id;
    })
    .slice(0, 8)
    .map(function(item) { return item.keyword; });

  if (!state.matches.length) {
    hideKeywordSuggestions(inputId);
    return;
  }
  if (state.activeIndex < 0 || state.activeIndex >= state.matches.length) state.activeIndex = 0;

  dropdown.innerHTML = state.matches.map(function(k, idx) {
    var active = idx === state.activeIndex ? ' active' : '';
    var count = k.photo_count === 1 ? '1 photo' : k.photo_count + ' photos';
    return '<div class="keyword-suggestion-option' + active + '" role="option" data-index="' + idx + '">' +
      '<span class="keyword-suggestion-name">' + escapeHtml(k.name) + '</span>' +
      '<span class="keyword-suggestion-meta">' + escapeHtml(count) + '</span>' +
      '</div>';
  }).join('');
  dropdown.classList.add('open');
}

function chooseKeywordSuggestion(inputId, index) {
  var input = document.getElementById(inputId);
  var state = getKeywordAutocompleteState(inputId);
  var keyword = state.matches[index];
  if (!input || !keyword) return;
  input.value = keyword.name;
  state.selectedKeyword = keyword;
  hideKeywordSuggestions(inputId);
  if (typeof state.submitFn === 'function') {
    state.submitFn(keyword);
  }
}

function bindKeywordAutocomplete(inputId, dropdownId, submitFn) {
  var input = document.getElementById(inputId);
  var dropdown = document.getElementById(dropdownId);
  if (!input || !dropdown) return;
  var state = getKeywordAutocompleteState(inputId);
  state.dropdownId = dropdownId;
  state.submitFn = submitFn;

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
    var option = e.target.closest('.keyword-suggestion-option');
    if (!option) return;
    e.preventDefault();
    chooseKeywordSuggestion(inputId, parseInt(option.dataset.index, 10));
  });
}
