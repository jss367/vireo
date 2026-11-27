/* Vireo shared utilities — loaded by _navbar.html on every page. */

window.Vireo = window.Vireo || /** @type {VireoNamespace} */ ({});
window.Vireo.dom = window.Vireo.dom || {
  /** @param {Element | null | undefined} element @param {string} state @param {string} [message] */
  setStatus: function(element, state, message) {
    if (!element) return;
    element.classList.remove('ok', 'warning', 'error', 'loading');
    if (state) element.classList.add(state);
    element.textContent = message || '';
    element.setAttribute('role', state === 'error' ? 'alert' : 'status');
    element.setAttribute('aria-live', state === 'error' ? 'assertive' : 'polite');
  },
  /** @param {Element | null | undefined} element */
  clear: function(element) {
    if (element) element.replaceChildren();
  },
};

/*
 * Questions and notices shown in the page: Vireo.confirm, Vireo.prompt and
 * Vireo.alert.
 *
 * The native dialogs cannot be used. The desktop app's webview replaces
 * window.confirm() with an async function, so `if (!confirm(...)) return;`
 * sees a promise (always truthy) and carries on without showing anything;
 * window.alert() shows nothing and window.prompt() returns null at once.
 *
 *   if (!(await Vireo.confirm({title: ..., message: ..., confirmLabel: ...}))) return;
 *   var name = await Vireo.prompt({title: ..., label: ..., value: ...});
 *   await Vireo.alert({title: ..., message: ...});
 *
 * confirm resolves true only when the confirm button is pressed. prompt
 * resolves the text in its field (or the chosen entry of `choices`), and null
 * when cancelled. Cancel, Esc and a question asked while a dialog is already
 * open answer no: false or null. An alert asked while a dialog is open waits
 * for it to close, so a notice is never dropped.
 *
 * The page keeps running while a dialog is open. After the await, check again
 * whatever the caller checked before asking (a request in flight, the photo
 * still on screen).
 */
(function() {
  var open = false;
  /** Settles when the open dialog closes. @type {Promise<unknown>} */
  var closed = Promise.resolve();

  /** @param {string} tag @param {string} className @param {string} text */
  function element(tag, className, text) {
    var node = document.createElement(tag);
    if (className) node.className = className;
    if (text) node.textContent = text;
    return node;
  }

  /** @param {string} role @param {string} className @param {string} text */
  function button(role, className, text) {
    var node = /** @type {HTMLButtonElement} */ (element('button', 'modal-btn ' + className, text));
    node.type = 'button';
    node.dataset.vireoConfirm = role;
    return node;
  }

  /** @param {VireoDialogContent} options @param {HTMLDialogElement} dialog */
  function fill(options, dialog) {
    var title = element('h3', '', options.title);
    title.id = 'vireoConfirmTitle';
    dialog.setAttribute('aria-labelledby', title.id);
    dialog.appendChild(title);
    String(options.message || '').split('\n\n').forEach(function(paragraph) {
      if (paragraph) dialog.appendChild(element('p', 'vireo-confirm-text', paragraph));
    });
    var details = options.details || [];
    if (details.length) {
      var list = element('ul', 'vireo-confirm-details', '');
      details.forEach(function(line) { list.appendChild(element('li', '', line)); });
      dialog.appendChild(list);
    }
  }

  // The prompt's field: a list when the answer must be one of `choices`,
  // otherwise a text input.
  /** @param {VireoDialogContent} options @param {HTMLDialogElement} dialog */
  function field(options, dialog) {
    var choices = options.choices || [];
    var input = /** @type {HTMLInputElement | HTMLSelectElement} */ (
      element(choices.length ? 'select' : 'input', 'modal-input vireo-confirm-input', ''));
    input.id = 'vireoConfirmInput';
    input.dataset.vireoConfirm = 'input';
    choices.forEach(function(choice) {
      var option = /** @type {HTMLOptionElement} */ (element('option', '', choice));
      option.value = choice;
      input.appendChild(option);
    });
    if (input instanceof HTMLInputElement) {
      input.type = 'text';
      input.autocomplete = 'off';
      input.spellcheck = false;
      if (options.placeholder) input.placeholder = options.placeholder;
    }
    if (options.value != null) input.value = options.value;
    if (options.label) {
      var label = /** @type {HTMLLabelElement} */ (element('label', 'modal-label', options.label));
      label.htmlFor = input.id;
      dialog.appendChild(label);
    } else {
      input.setAttribute('aria-labelledby', 'vireoConfirmTitle');
    }
    dialog.appendChild(input);
    return input;
  }

  /** @param {VireoDialogContent} options @param {VireoDialogKind} kind @returns {VireoDialogParts} */
  function build(options, kind) {
    var dialog = /** @type {HTMLDialogElement} */ (element('dialog', 'modal vireo-confirm', ''));
    dialog.id = 'vireoConfirmDialog';
    dialog.dataset.vireoDialog = kind;
    fill(options, dialog);
    var input = kind === 'prompt' ? field(options, dialog) : null;
    var cancel = kind === 'alert' ? null : button('cancel', 'modal-btn-cancel', options.cancelLabel || 'Cancel');
    var accept = button(
      'accept', options.danger ? 'vireo-confirm-danger' : 'modal-btn-primary', options.confirmLabel || 'OK');
    var actions = element('div', 'modal-actions', '');
    if (cancel) actions.appendChild(cancel);
    actions.appendChild(accept);
    dialog.appendChild(actions);
    return {dialog: dialog, input: input, cancel: cancel, accept: accept};
  }

  // Every key belongs to the dialog while it is open. The page's shortcuts
  // listen on document and would otherwise act on the photo behind it, and
  // Esc would also close the lightbox under it. Typing still reaches the
  // field, and Enter and Space still press the focused button: stopping
  // propagation does not cancel a key's default action.
  /** @param {VireoDialogParts} parts @param {(accepted: boolean) => void} finish */
  function keyHandler(parts, finish) {
    var stops = /** @type {HTMLElement[]} */ ([parts.input, parts.cancel, parts.accept].filter(Boolean));
    /** @param {KeyboardEvent} event */
    return function onKeydown(event) {
      event.stopImmediatePropagation();
      // A key that commits an input-method composition is not an answer.
      if (event.isComposing) return;
      if (event.key === 'Escape') {
        event.preventDefault();
        finish(false);
      } else if (event.key === 'Tab') {
        // WebKit's default keyboard setting skips buttons on Tab.
        event.preventDefault();
        var index = stops.indexOf(/** @type {HTMLElement} */ (document.activeElement));
        var next = stops[(index + (event.shiftKey ? -1 : 1) + stops.length) % stops.length];
        if (next) next.focus();
      } else if (event.key === 'Enter' && parts.input && document.activeElement === parts.input) {
        event.preventDefault();
        finish(true);
      }
    };
  }

  // Resolves null when the dialog is dismissed, otherwise the field's text
  // (an empty string for a dialog with no field).
  /** @param {VireoDialogContent} options @param {VireoDialogKind} kind @returns {Promise<string | null>} */
  function show(options, kind) {
    open = true;
    var parts = build(options, kind);
    var dialog = parts.dialog;
    document.body.appendChild(dialog);
    var previousFocus = document.activeElement;

    var answer = new Promise(function(resolve) {
      /** @param {boolean} accepted */
      function finish(accepted) {
        if (!open) return;
        open = false;
        window.removeEventListener('keydown', onKeydown, true);
        dialog.close();
        dialog.remove();
        if (previousFocus instanceof HTMLElement && previousFocus.isConnected) previousFocus.focus();
        resolve(accepted ? (parts.input ? parts.input.value : '') : null);
      }
      var onKeydown = keyHandler(parts, finish);
      window.addEventListener('keydown', onKeydown, true);
      if (parts.cancel) parts.cancel.addEventListener('click', function() { finish(false); });
      parts.accept.addEventListener('click', function() { finish(true); });
      dialog.addEventListener('cancel', function(event) { event.preventDefault(); finish(false); });
      dialog.showModal();
      // A question keeps the focus off its confirm button, so a stray Enter
      // never confirms.
      (parts.input || parts.cancel || parts.accept).focus();
      if (parts.input instanceof HTMLInputElement) parts.input.select();
    });
    closed = answer;
    return answer;
  }

  /** @type {VireoConfirm} */
  function confirmInPage(options) {
    if (open) return Promise.resolve(false);
    return show(options, 'confirm').then(function(answer) { return answer !== null; });
  }

  /** @type {VireoPrompt} */
  function promptInPage(options) {
    if (open) return Promise.resolve(null);
    return show(options, 'prompt');
  }

  /** @type {VireoAlert} */
  function alertInPage(options) {
    if (open) return closed.then(function() { return alertInPage(options); });
    return show(options, 'alert').then(function() {});
  }

  window.Vireo.confirm = window.Vireo.confirm || confirmInPage;
  window.Vireo.prompt = window.Vireo.prompt || promptInPage;
  window.Vireo.alert = window.Vireo.alert || alertInPage;
})();

/*
 * Persistent presentation preferences.
 *
 * Stable view controls opt in with data-view-preference="vireo.page.name".
 * Scope, search, and content-filter controls intentionally do not opt in:
 * restoring those can silently hide content when a user returns to a page.
 */
var VireoViewPreferences = (function() {
  var selector = '[data-view-preference]';

  /** @param {Element | string | null | undefined} elementOrKey */
  function keyFor(elementOrKey) {
    if (typeof elementOrKey === 'string') return elementOrKey;
    return elementOrKey && elementOrKey.getAttribute
      ? elementOrKey.getAttribute('data-view-preference')
      : null;
  }

  /** @param {Element | string | null | undefined} elementOrKey */
  function read(elementOrKey) {
    var key = keyFor(elementOrKey);
    if (!key) return null;
    try { return window.localStorage.getItem(key); } catch (e) { return null; }
  }

  /** @param {Element | string | null | undefined} elementOrKey @param {unknown} value */
  function write(elementOrKey, value) {
    var key = keyFor(elementOrKey);
    if (!key) return;
    try { window.localStorage.setItem(key, String(value)); } catch (e) {}
  }

  /** @param {HTMLInputElement | HTMLSelectElement} element @param {string | null | undefined} value @returns {value is string} */
  function validValue(element, value) {
    if (value === null || value === undefined) return false;
    if (element instanceof HTMLSelectElement) {
      return Array.from(element.options).some(function(option) {
        return option.value === value;
      });
    }
    if (!(element instanceof HTMLInputElement)) return true;
    if (element.type === 'checkbox') return value === '0' || value === '1';
    if (element.type === 'range' || element.type === 'number') {
      var parsed = Number(value);
      if (!Number.isFinite(parsed)) return false;
      if (element.min !== '' && parsed < Number(element.min)) return false;
      if (element.max !== '' && parsed > Number(element.max)) return false;
    }
    return true;
  }

  /** @param {Element | null | undefined} element */
  function restore(element) {
    if (!(element instanceof HTMLInputElement) && !(element instanceof HTMLSelectElement)) return false;
    var saved = read(element);
    if (!validValue(element, saved)) return false;
    if (element instanceof HTMLInputElement && element.type === 'checkbox') element.checked = saved === '1';
    else element.value = saved;
    return true;
  }

  /** @param {Document | Element} [root] */
  function restoreAll(root) {
    var scope = root || document;
    if (scope instanceof Element && scope.matches(selector)) restore(scope);
    if (scope.querySelectorAll) scope.querySelectorAll(selector).forEach(restore);
  }

  /** @param {EventTarget | null} element */
  function persist(element) {
    if (!(element instanceof HTMLInputElement) && !(element instanceof HTMLSelectElement)) return;
    if (!element.matches(selector)) return;
    write(element, element instanceof HTMLInputElement && element.type === 'checkbox' ? (element.checked ? '1' : '0') : element.value);
  }

  /** @param {Event} event */
  function onPreferenceEvent(event) {
    persist(event.target);
  }

  // Delegation covers controls parsed after this shared script and avoids
  // page-specific storage listeners. Range inputs save continuously; select
  // and checkbox controls save on change.
  document.addEventListener('input', onPreferenceEvent, true);
  document.addEventListener('change', onPreferenceEvent, true);
  // Restore controls already parsed. This script is the navbar's first, so
  // on a navbar page that is nothing yet: navbar-shared-helpers.js, loaded
  // after the navbar markup, restores the navbar's own controls, and pages
  // restore theirs before bootstrap so page-specific validation can run
  // afterward without a second late restore undoing it.
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
/** @param {unknown} str */
function escapeHtml(str) {
  if (str == null) return '';
  return String(str).replace(/[&<>"']/g, function(ch) {
    return {'&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#39;'}[ch] || ch;
  });
}

/** @param {unknown} str */
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
/** @param {unknown} n */
function formatBytes(n) {
  if (n == null) return '';
  var value = Number(n);
  if (!isFinite(value)) return '';
  if (value < 1024) return value + ' B';
  if (value < 1024 * 1024) return (value / 1024).toFixed(1) + ' KB';
  if (value < 1024 * 1024 * 1024) return (value / 1024 / 1024).toFixed(1) + ' MB';
  if (value < 1024 * 1024 * 1024 * 1024) return (value / 1024 / 1024 / 1024).toFixed(2) + ' GB';
  return (value / 1024 / 1024 / 1024 / 1024).toFixed(2) + ' TB';
}

/*
 * A photo's exposure as short labels, in the order Browse's detail panel and
 * the lightbox show them: focal length, aperture, shutter speed, ISO. Takes
 * the photo's `metadata` (ExifTool groups); a value the file does not carry
 * is left out, so a photo without EXIF gives []. Shutter speeds from 0.3s up
 * read in seconds ("0.6s"), faster ones as a fraction ("1/320s").
 */
/** @param {unknown} metadata @returns {string[]} */
function exposureSummaryParts(metadata) {
  var groups = /** @type {Record<string, unknown>} */ (
    metadata && typeof metadata === 'object' ? metadata : {});
  var exif = /** @type {Record<string, unknown>} */ (
    groups.EXIF && typeof groups.EXIF === 'object' ? groups.EXIF : {});
  var parts = [];
  var focal = Number(exif.FocalLength);
  if (focal > 0) parts.push(Math.round(focal) + 'mm');
  var aperture = Number(exif.FNumber);
  if (aperture > 0) parts.push('f/' + aperture);
  var shutter = Number(exif.ExposureTime);
  if (shutter > 0) {
    parts.push(shutter < 0.3
      ? '1/' + Math.round(1 / shutter) + 's'
      : Math.round(shutter * 10) / 10 + 's');
  }
  var iso = Number(exif.ISO);
  if (iso > 0) parts.push('ISO ' + iso);
  return parts;
}

/*
 * The one thumbnail URL for a photo row. The navbar's builder adds the query
 * that follows an edit, so an edited photo's thumbnail is refetched; before
 * the navbar's modules run, or on a page without them, the plain path is
 * used. A row without an id has no thumbnail and renders as ''.
 * test_shared_frontend_helpers.py keeps photoThumbnailUrl to this definition.
 */
/** @param {{id?: number | null} | null | undefined} photo */
function photoThumbnailUrl(photo) {
  if (!photo || photo.id == null) return '';
  return window.vireoThumbnailUrl
    ? window.vireoThumbnailUrl(/** @type {{id: number}} */ (photo))
    : '/thumbnails/' + photo.id + '.jpg';
}

/*
 * How long ago an ISO timestamp was, in one unit: "just now", "5m ago",
 * "3h ago", "2d ago". A missing timestamp renders as ''.
 * test_shared_frontend_helpers.py keeps timeAgo to this definition.
 */
/** @param {string | null | undefined} iso */
function timeAgo(iso) {
  if (!iso) return '';
  var secs = (Date.now() - new Date(iso).getTime()) / 1000;
  if (secs < 60) return 'just now';
  if (secs < 3600) return Math.floor(secs / 60) + 'm ago';
  if (secs < 86400) return Math.floor(secs / 3600) + 'h ago';
  return Math.floor(secs / 86400) + 'd ago';
}

/*
 * The line a page shows while its main request is running.
 *
 * A request for a whole workspace can take from seconds to minutes, so the
 * line counts the seconds once the wait is long (a static "Loading…" reads as
 * a hung page), adds the page's hint about what is being loaded, and turns
 * into the error with a retry button when the request fails.
 *
 * `content` is the element the request fills. While it is empty the line is
 * shown in its place. Once it holds results the line floats at the bottom of
 * the window as a small box and the results are dimmed (`data-vireo-stale`), because they
 * belong to the previous request; a reload that returns within QUIET_MS shows
 * neither, so quick filter changes do not flicker.
 *
 * The status element carries role="status". The seconds are aria-hidden so a
 * screen reader announces the message once, not every second.
 */
var VireoLoadingStatus = (function() {
  var SLOW_AFTER_SECONDS = 4;
  var QUIET_MS = 400;

  /** @param {string} tag @param {string} className @param {string} [text] */
  function part(tag, className, text) {
    var node = document.createElement(tag);
    node.className = className;
    if (text) node.textContent = text;
    return node;
  }

  /** @param {VireoLoadingStatusView} view */
  function stopTimers(view) {
    if (view.ticker !== null) clearInterval(view.ticker);
    if (view.quietTimer !== null) clearTimeout(view.quietTimer);
    view.ticker = null;
    view.quietTimer = null;
  }

  /** @param {VireoLoadingStatusView} view */
  function overResults(view) {
    return !!view.content && view.content.childElementCount > 0;
  }

  // Floating over results, which are dimmed; or in their place when there are
  // none. The floating box sits at the bottom of the window so it never covers
  // the page's controls, above the navbar's bottom panel: that panel is fixed
  // over the status bar, so its top edge is where the page's own area ends.
  /** @param {VireoLoadingStatusView} view @param {boolean} floating */
  function place(view, floating) {
    if (floating) {
      var panel = document.getElementById('bottomPanel');
      var reserved = panel ? window.innerHeight - panel.getBoundingClientRect().top : 0;
      view.el.dataset.overResults = 'true';
      view.el.style.bottom = (reserved + 16) + 'px';
    } else {
      delete view.el.dataset.overResults;
      view.el.style.bottom = '';
    }
    if (!view.content) return;
    if (floating) view.content.dataset.vireoStale = 'true';
    else delete view.content.dataset.vireoStale;
  }

  // The tick only rewrites the seconds and reveals the hint, so a screen
  // reader hears the message once.
  /** @param {VireoLoadingStatusView} view */
  function tick(view) {
    var seconds = Math.floor((Date.now() - view.startedAt) / 1000);
    var slow = seconds >= SLOW_AFTER_SECONDS;
    view.elapsedNode.textContent = slow ? ' ' + seconds + 's' : '';
    view.hintNode.hidden = !(slow && view.hint);
  }

  /** @param {VireoLoadingStatusView} view */
  function describe(view) {
    if (view.messageNode.textContent !== view.message) view.messageNode.textContent = view.message;
    if (view.hintNode.textContent !== view.hint) view.hintNode.textContent = view.hint;
    tick(view);
  }

  /** @param {VireoLoadingStatusView} view */
  function show(view) {
    view.quietTimer = null;
    view.el.dataset.state = 'loading';
    place(view, overResults(view));
    describe(view);
    view.el.replaceChildren(view.messageNode, view.elapsedNode, view.hintNode);
    view.el.hidden = false;
    if (view.ticker === null) view.ticker = setInterval(function() { tick(view); }, 1000);
  }

  /** @param {VireoLoadingStatusView} view @param {string} message @param {string} hint */
  function start(view, message, hint) {
    var waiting = view.el.dataset.state === 'loading' || view.quietTimer !== null;
    // A start during a wait keeps that wait's clock.
    if (!waiting) view.startedAt = Date.now();
    view.message = message;
    view.hint = hint;
    if (view.el.dataset.state === 'loading') describe(view);
    if (waiting) return;
    // An error stays on screen until its retry shows something in its place.
    if (overResults(view) && view.el.dataset.state !== 'error') {
      view.el.hidden = true;
      view.quietTimer = setTimeout(function() { show(view); }, QUIET_MS);
      return;
    }
    show(view);
  }

  /** @param {VireoLoadingStatusView} view */
  function done(view) {
    stopTimers(view);
    delete view.el.dataset.state;
    place(view, false);
    view.el.hidden = true;
    view.el.replaceChildren();
  }

  /** @param {VireoLoadingStatusView} view @param {string} message
   * @param {{retry?: () => void, retryLabel?: string}} options */
  function fail(view, message, options) {
    stopTimers(view);
    view.el.dataset.state = 'error';
    // Results that stay on screen no longer match the controls, so they stay dimmed.
    place(view, overResults(view));
    var nodes = [part('span', 'vireo-loading-status-message', message)];
    var retry = options.retry;
    if (retry) {
      /** @type {() => void} */
      var runRetry = retry;
      var button = part('button', 'vireo-loading-status-retry', options.retryLabel || 'Try again');
      button.setAttribute('type', 'button');
      button.addEventListener('click', function() { runRetry(); });
      nodes.push(button);
    }
    view.el.replaceChildren.apply(view.el, nodes);
    view.el.hidden = false;
  }

  /**
   * @param {HTMLElement} el
   * @param {{content?: HTMLElement | null}} [options]
   * @returns {VireoLoadingStatusHandle}
   */
  function create(el, options) {
    var elapsedNode = part('span', 'vireo-loading-status-elapsed');
    elapsedNode.setAttribute('aria-hidden', 'true');
    /** @type {VireoLoadingStatusView} */
    var view = {
      el: el,
      content: (options && options.content) || null,
      startedAt: 0,
      ticker: null,
      quietTimer: null,
      message: '',
      hint: '',
      messageNode: part('span', 'vireo-loading-status-message'),
      elapsedNode: elapsedNode,
      hintNode: part('p', 'vireo-loading-status-hint'),
    };
    return {
      start: function(message, startOptions) { start(view, message, (startOptions && startOptions.hint) || ''); },
      done: function() { done(view); },
      fail: function(message, failOptions) { fail(view, message, failOptions || {}); },
    };
  }

  return { create: create };
})();

var VireoTextInputs = (function() {
  var selector = 'input[type="text"], input[type="search"], input:not([type]), textarea';

  /** @param {Element | null} el */
  function disableCorrection(el) {
    if (!el || !el.setAttribute) return;
    el.setAttribute('autocomplete', 'off');
    el.setAttribute('autocorrect', 'off');
    el.setAttribute('autocapitalize', 'none');
    el.setAttribute('spellcheck', 'false');
  }

  /** @param {Document | Element | null} root */
  function apply(root) {
    if (!root || !root.querySelectorAll) return;
    if (root instanceof Element && root.matches(selector)) disableCorrection(root);
    root.querySelectorAll(selector).forEach(disableCorrection);
  }

  function start() {
    apply(document);
    if (!window.MutationObserver || !document.body) return;
    var observer = new MutationObserver(function(records) {
      records.forEach(function(record) {
        if (record.type === 'attributes') {
          if (record.target && record.target instanceof Element) apply(record.target);
          return;
        }
        record.addedNodes.forEach(function(node) {
          if (node instanceof Element) apply(node);
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
  /** @param {string} ch */
  function isWordChar(ch) {
    return !!ch && (/[0-9]/.test(ch) || ch.toLowerCase() !== ch.toUpperCase());
  }

  /** @param {string} value @param {string} token */
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

  /** @param {unknown} value @param {unknown} token @param {TextSearchOptions} [options] */
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

  /** @param {unknown} query */
  function tokens(query) {
    return String(query || '').trim().split(/\s+/).filter(Boolean);
  }

  /** @param {unknown | unknown[]} fields @param {unknown} query @param {TextSearchOptions} [options] */
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

  /** @param {string} prefix */
  function readOptions(prefix) {
    var matchCase = document.getElementById(prefix + 'MatchCaseBtn');
    var wholeWord = document.getElementById(prefix + 'WholeWordBtn');
    return {
      matchCase: !!(matchCase && matchCase.classList.contains('active')),
      wholeWord: !!(wholeWord && wholeWord.classList.contains('active')),
    };
  }

  /** @param {Element | null} button @param {boolean | undefined} active */
  function applyButton(button, active) {
    if (!button) return;
    button.classList.toggle('active', !!active);
    button.setAttribute('aria-pressed', active ? 'true' : 'false');
  }

  /** @param {string} prefix @param {TextSearchOptions} options */
  function renderOptions(prefix, options) {
    applyButton(document.getElementById(prefix + 'MatchCaseBtn'), options.matchCase);
    applyButton(document.getElementById(prefix + 'WholeWordBtn'), options.wholeWord);
  }

  /** @param {string} prefix @param {string} option @param {(options: TextSearchOptions) => void} [onChange] */
  function toggle(prefix, option, onChange) {
    var options = readOptions(prefix);
    if (option === 'match_case') options.matchCase = !options.matchCase;
    if (option === 'whole_word') options.wholeWord = !options.wholeWord;
    renderOptions(prefix, options);
    if (typeof onChange === 'function') onChange(options);
    return options;
  }

  /** @param {URLSearchParams} params @param {TextSearchOptions} options @param {string} [matchCaseName] @param {string} [wholeWordName] */
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

  /** @param {PipelineDefaults | null | undefined} pipeline @param {string} key */
  function pipelineValue(pipeline, key) {
    if (pipeline && pipeline[key] != null) return pipeline[key];
    return defaultPipeline()[key];
  }

  /** @param {unknown} value @param {string} key */
  function asNumber(value, key) {
    var n = Number(value);
    if (!Number.isFinite(n)) {
      throw new Error('Missing numeric pipeline config: ' + key);
    }
    return n;
  }

  /** @param {unknown} value @param {string} key */
  function percent(value, key) {
    return Math.round(asNumber(value, key) * 100);
  }

  /** @param {{pipeline?: PipelineDefaults} | null} [config] */
  function pipelineFromConfig(config) {
    if (!config) return defaultPipeline();
    if (typeof config.pipeline !== 'object' || config.pipeline === null) {
      return defaultPipeline();
    }
    return config.pipeline;
  }

  /** @param {unknown} threshold */
  function embeddingThresholdToDistancePercent(threshold) {
    return Math.round((1 - asNumber(threshold, 'burst_embedding_threshold')) * 100);
  }

  /** @param {unknown} distancePercent */
  function embeddingDistancePercentToThreshold(distancePercent) {
    return 1 - asNumber(distancePercent, 'burst_embedding_distance') / 100;
  }

  /** @param {{pipeline?: PipelineDefaults} | null} [config] */
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
