/* Vireo shared utilities — loaded by _navbar.html on every page. */

window.Vireo = window.Vireo || {};
window.Vireo.dom = window.Vireo.dom || {
  setStatus: function(element, state, message) {
    if (!element) return;
    element.classList.remove('ok', 'warning', 'error', 'loading');
    if (state) element.classList.add(state);
    element.textContent = message || '';
    element.setAttribute('role', state === 'error' ? 'alert' : 'status');
    element.setAttribute('aria-live', state === 'error' ? 'assertive' : 'polite');
  },
  clear: function(element) {
    if (element) element.replaceChildren();
  },
};

/*
 * Persistent presentation preferences.
 *
 * Stable view controls opt in with data-view-preference="vireo.page.name".
 * Scope, search, and content-filter controls intentionally do not opt in:
 * restoring those can silently hide content when a user returns to a page.
 */
var VireoViewPreferences = (function() {
  var selector = '[data-view-preference]';

  function keyFor(elementOrKey) {
    if (typeof elementOrKey === 'string') return elementOrKey;
    return elementOrKey && elementOrKey.getAttribute
      ? elementOrKey.getAttribute('data-view-preference')
      : null;
  }

  function read(elementOrKey) {
    var key = keyFor(elementOrKey);
    if (!key) return null;
    try { return window.localStorage.getItem(key); } catch (e) { return null; }
  }

  function write(elementOrKey, value) {
    var key = keyFor(elementOrKey);
    if (!key) return;
    try { window.localStorage.setItem(key, String(value)); } catch (e) {}
  }

  function validValue(element, value) {
    if (value === null || value === undefined) return false;
    if (element.tagName === 'SELECT') {
      return Array.prototype.some.call(element.options, function(option) {
        return option.value === value;
      });
    }
    if (element.type === 'checkbox') return value === '0' || value === '1';
    if (element.type === 'range' || element.type === 'number') {
      var parsed = Number(value);
      if (!Number.isFinite(parsed)) return false;
      if (element.min !== '' && parsed < Number(element.min)) return false;
      if (element.max !== '' && parsed > Number(element.max)) return false;
    }
    return true;
  }

  function restore(element) {
    if (!element) return false;
    var saved = read(element);
    if (!validValue(element, saved)) return false;
    if (element.type === 'checkbox') element.checked = saved === '1';
    else element.value = saved;
    return true;
  }

  function restoreAll(root) {
    var scope = root || document;
    if (scope.matches && scope.matches(selector)) restore(scope);
    if (scope.querySelectorAll) scope.querySelectorAll(selector).forEach(restore);
  }

  function persist(element) {
    if (!element || !element.matches || !element.matches(selector)) return;
    write(element, element.type === 'checkbox' ? (element.checked ? '1' : '0') : element.value);
  }

  function onPreferenceEvent(event) {
    persist(event.target);
  }

  // Delegation covers controls parsed after this shared script and avoids
  // page-specific storage listeners. Range inputs save continuously; select
  // and checkbox controls save on change.
  document.addEventListener('input', onPreferenceEvent, true);
  document.addEventListener('change', onPreferenceEvent, true);
  // Restore controls already parsed (notably shared navbar controls). Pages
  // restore their own controls before bootstrap so page-specific validation
  // can run afterward without a second late restore undoing it.
  restoreAll(document);

  return {
    read: read,
    write: write,
    restore: restore,
    restoreAll: restoreAll,
    persist: persist,
  };
})();

/*
 * The one HTML escaper. Text and attribute contexts share it: a value that
 * encodes all five of & < > " ' is safe between tags and inside a quoted
 * attribute alike, so a page never has to pick the right helper (and a
 * copy that only escaped the text set once let a quote break out of an
 * attribute). test_shared_frontend_helpers.py keeps every page on this
 * definition; no other script may declare escapeHtml, escapeAttr or esc.
 */
function escapeHtml(str) {
  if (str == null) return '';
  return String(str).replace(/[&<>"']/g, function(ch) {
    return {'&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#39;'}[ch];
  });
}

function escapeAttr(str) {
  return escapeHtml(str);
}

/*
 * The one byte formatter (every page's sizes, the navbar's cleanup banner,
 * Work Locally's quotas). null/undefined and non-numbers render as '', so a
 * missing size shows nothing rather than "NaN undefined". Whole bytes stay
 * integers; KB and MB get one decimal, GB and TB two. test_shared_frontend_helpers.py
 * keeps formatBytes to this definition.
 */
function formatBytes(n) {
  if (n == null) return '';
  n = Number(n);
  if (!isFinite(n)) return '';
  if (n < 1024) return n + ' B';
  if (n < 1024 * 1024) return (n / 1024).toFixed(1) + ' KB';
  if (n < 1024 * 1024 * 1024) return (n / 1024 / 1024).toFixed(1) + ' MB';
  if (n < 1024 * 1024 * 1024 * 1024) return (n / 1024 / 1024 / 1024).toFixed(2) + ' GB';
  return (n / 1024 / 1024 / 1024 / 1024).toFixed(2) + ' TB';
}

var VireoTextInputs = (function() {
  var selector = 'input[type="text"], input[type="search"], input:not([type]), textarea';

  function disableCorrection(el) {
    if (!el || !el.setAttribute) return;
    el.setAttribute('autocomplete', 'off');
    el.setAttribute('autocorrect', 'off');
    el.setAttribute('autocapitalize', 'none');
    el.setAttribute('spellcheck', 'false');
  }

  function apply(root) {
    if (!root || !root.querySelectorAll) return;
    if (root.matches && root.matches(selector)) disableCorrection(root);
    root.querySelectorAll(selector).forEach(disableCorrection);
  }

  function start() {
    apply(document);
    if (!window.MutationObserver || !document.body) return;
    var observer = new MutationObserver(function(records) {
      records.forEach(function(record) {
        if (record.type === 'attributes') {
          if (record.target && record.target.nodeType === 1) apply(record.target);
          return;
        }
        record.addedNodes.forEach(function(node) {
          if (node.nodeType === 1) apply(node);
        });
      });
    });
    observer.observe(document.body, {
      childList: true,
      subtree: true,
      attributes: true,
      attributeFilter: ['type'],
    });
  }

  if (document.readyState === 'loading') {
    document.addEventListener('DOMContentLoaded', start);
  } else {
    start();
  }

  return {
    apply: apply,
    disableCorrection: disableCorrection,
  };
})();

var VireoTextSearch = (function() {
  function isWordChar(ch) {
    return !!ch && (/[0-9]/.test(ch) || ch.toLowerCase() !== ch.toUpperCase());
  }

  function containsWholeToken(value, token) {
    var start = 0;
    while (true) {
      var idx = value.indexOf(token, start);
      if (idx < 0) return false;
      var before = idx === 0 || !isWordChar(value.charAt(idx - 1));
      var end = idx + token.length;
      var after = end === value.length || !isWordChar(value.charAt(end));
      if (before && after) return true;
      start = idx + 1;
    }
  }

  function tokenMatches(value, token, options) {
    if (value == null || token == null) return false;
    var text = String(value);
    var needle = String(token);
    if (!needle) return true;
    if (!options || !options.matchCase) {
      text = text.toLowerCase();
      needle = needle.toLowerCase();
    }
    if (options && options.wholeWord) {
      return containsWholeToken(text, needle);
    }
    return text.indexOf(needle) !== -1;
  }

  function tokens(query) {
    return String(query || '').trim().split(/\s+/).filter(Boolean);
  }

  function matchesFields(fields, query, options) {
    var parts = tokens(query);
    if (!parts.length) return true;
    var values = Array.isArray(fields) ? fields : [fields];
    return parts.every(function(token) {
      return values.some(function(value) {
        return tokenMatches(value, token, options || {});
      });
    });
  }

  function readOptions(prefix) {
    var matchCase = document.getElementById(prefix + 'MatchCaseBtn');
    var wholeWord = document.getElementById(prefix + 'WholeWordBtn');
    return {
      matchCase: !!(matchCase && matchCase.classList.contains('active')),
      wholeWord: !!(wholeWord && wholeWord.classList.contains('active')),
    };
  }

  function applyButton(button, active) {
    if (!button) return;
    button.classList.toggle('active', !!active);
    button.setAttribute('aria-pressed', active ? 'true' : 'false');
  }

  function renderOptions(prefix, options) {
    applyButton(document.getElementById(prefix + 'MatchCaseBtn'), options.matchCase);
    applyButton(document.getElementById(prefix + 'WholeWordBtn'), options.wholeWord);
  }

  function toggle(prefix, option, onChange) {
    var options = readOptions(prefix);
    if (option === 'match_case') options.matchCase = !options.matchCase;
    if (option === 'whole_word') options.wholeWord = !options.wholeWord;
    renderOptions(prefix, options);
    if (typeof onChange === 'function') onChange(options);
    return options;
  }

  function appendParams(params, options, matchCaseName, wholeWordName) {
    if (!params || !options) return;
    if (options.matchCase) params.set(matchCaseName || 'match_case', '1');
    if (options.wholeWord) params.set(wholeWordName || 'whole_word', '1');
  }

  return {
    isWordChar: isWordChar,
    tokenMatches: tokenMatches,
    matchesFields: matchesFields,
    readOptions: readOptions,
    renderOptions: renderOptions,
    toggle: toggle,
    appendParams: appendParams,
  };
})();

var VireoPipelineConfig = (function() {
  function defaultPipeline() {
    var defaults = window.VIREO_CONFIG_DEFAULTS;
    if (!defaults || typeof defaults.pipeline !== 'object' || defaults.pipeline === null) {
      throw new Error('Missing rendered pipeline defaults');
    }
    return defaults.pipeline;
  }

  function pipelineValue(pipeline, key) {
    if (pipeline && pipeline[key] != null) return pipeline[key];
    return defaultPipeline()[key];
  }

  function asNumber(value, key) {
    var n = Number(value);
    if (!Number.isFinite(n)) {
      throw new Error('Missing numeric pipeline config: ' + key);
    }
    return n;
  }

  function percent(value, key) {
    return Math.round(asNumber(value, key) * 100);
  }

  function pipelineFromConfig(config) {
    if (!config) return defaultPipeline();
    if (typeof config.pipeline !== 'object' || config.pipeline === null) {
      return defaultPipeline();
    }
    return config.pipeline;
  }

  function embeddingThresholdToDistancePercent(threshold) {
    return Math.round((1 - asNumber(threshold, 'burst_embedding_threshold')) * 100);
  }

  function embeddingDistancePercentToThreshold(distancePercent) {
    return 1 - asNumber(distancePercent, 'burst_embedding_distance') / 100;
  }

  function buildSliderDefaults(config) {
    var p = pipelineFromConfig(config);
    return {
      scoring: {
        reject_crop_complete: percent(
          pipelineValue(p, 'reject_crop_complete'), 'reject_crop_complete'
        ),
        reject_focus: percent(pipelineValue(p, 'reject_focus'), 'reject_focus'),
        reject_clip_high: percent(
          pipelineValue(p, 'reject_clip_high'), 'reject_clip_high'
        ),
        reject_composite: percent(
          pipelineValue(p, 'reject_composite'), 'reject_composite'
        ),
        burst_lambda: percent(pipelineValue(p, 'burst_lambda'), 'burst_lambda'),
        burst_max_keep: asNumber(
          pipelineValue(p, 'burst_max_keep'), 'burst_max_keep'
        ),
        encounter_lambda: percent(
          pipelineValue(p, 'encounter_lambda'), 'encounter_lambda'
        ),
        encounter_max_keep: asNumber(
          pipelineValue(p, 'encounter_max_keep'), 'encounter_max_keep'
        ),
      },
      grouping: {
        w_time: percent(pipelineValue(p, 'w_time'), 'w_time'),
        w_subj: percent(pipelineValue(p, 'w_subj'), 'w_subj'),
        w_global: percent(pipelineValue(p, 'w_global'), 'w_global'),
        w_species: percent(pipelineValue(p, 'w_species'), 'w_species'),
        w_meta: percent(pipelineValue(p, 'w_meta'), 'w_meta'),
        tau_enc: asNumber(pipelineValue(p, 'tau_enc'), 'tau_enc'),
        hard_cut_time: asNumber(
          pipelineValue(p, 'hard_cut_time'), 'hard_cut_time'
        ),
        hard_cut_score: percent(
          pipelineValue(p, 'hard_cut_score'), 'hard_cut_score'
        ),
        soft_cut_score: percent(
          pipelineValue(p, 'soft_cut_score'), 'soft_cut_score'
        ),
        species_hard_cut_confidence: percent(
          pipelineValue(p, 'species_hard_cut_confidence'),
          'species_hard_cut_confidence'
        ),
        species_hard_cut_margin: percent(
          pipelineValue(p, 'species_hard_cut_margin'),
          'species_hard_cut_margin'
        ),
        merge_score: percent(pipelineValue(p, 'merge_score'), 'merge_score'),
        merge_max_gap: asNumber(
          pipelineValue(p, 'merge_max_gap'), 'merge_max_gap'
        ),
        merge_tau: asNumber(pipelineValue(p, 'merge_tau'), 'merge_tau'),
        burst_time_gap: asNumber(
          pipelineValue(p, 'burst_time_gap'), 'burst_time_gap'
        ),
        burst_embedding_distance: embeddingThresholdToDistancePercent(
          pipelineValue(p, 'burst_embedding_threshold')
        ),
      },
    };
  }

  return {
    defaultPipeline: defaultPipeline,
    pipelineValue: pipelineValue,
    asNumber: asNumber,
    percent: percent,
    pipelineFromConfig: pipelineFromConfig,
    embeddingThresholdToDistancePercent: embeddingThresholdToDistancePercent,
    embeddingDistancePercentToThreshold: embeddingDistancePercentToThreshold,
    buildSliderDefaults: buildSliderDefaults,
  };
})();
