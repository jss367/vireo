/* Vireo browser transport.
 *
 * The backend's SameSite session cookie authenticates reads.  Unsafe
 * same-origin requests also need a non-simple header so a hostile website
 * cannot submit a form to Vireo's localhost server.  Install the wrapper as
 * early as possible so older page controllers gain the protection without a
 * flag-day conversion; new code should call Vireo.api.fetch directly.
 */
(function(global) {
  'use strict';

  var Vireo = global.Vireo = global.Vireo || /** @type {VireoNamespace} */ ({});
  var nativeFetch = global.fetch.bind(global);
  /** @type {Record<string, boolean>} */
  var SAFE_METHODS = {GET: true, HEAD: true, OPTIONS: true};

  /** @param {RequestInfo | URL} input */
  function isSameOrigin(input) {
    try {
      var raw = input instanceof Request ? input.url : input;
      return new URL(String(raw), global.location.href).origin === global.location.origin;
    } catch (e) {
      return false;
    }
  }

  /** @param {RequestInfo | URL} input @param {RequestInit} [init] */
  function withBrowserHeader(input, init) {
    var requestMethod = input instanceof Request ? input.method : 'GET';
    var method = String((init && init.method) || requestMethod || 'GET').toUpperCase();
    if (SAFE_METHODS[method] || !isSameOrigin(input)) return init;

    var next = Object.assign({}, init || {});
    next.headers = new Headers(
      (init && init.headers) || (input instanceof Request ? input.headers : undefined)
    );
    next.headers.set('X-Vireo-Client', 'browser');
    return next;
  }

  var lastRecoveryId = '';
  /** @param {Response} response */
  function showConfigRecovery(response) {
    var notice = response.headers?.get('X-Vireo-Config-Recovery');
    if (!notice || typeof global.showToast !== 'function') return response;
    try {
      var recovery = JSON.parse(notice);
      // Dedupe by recovery identity: a backup-failed read followed by a
      // backup-succeeded one carries the same id but a different message,
      // and two toasts for one recovery would read as a repeat event.
      var recoveryId = recovery && recovery.id;
      if (!recoveryId || recoveryId === lastRecoveryId) return response;
      lastRecoveryId = recoveryId;
      var key = 'vireo.configRecoveryId';
      try { if (global.localStorage.getItem(key) === recoveryId) return response; } catch (_) {}
      global.showToast(recovery.message, 'warning');
      try { global.localStorage.setItem(key, recoveryId); } catch (_) {}
    } catch (_) { /* A notice must never interrupt the request. */ }
    return response;
  }

  /** @param {RequestInfo | URL} input @param {RequestInit} [init] */
  function browserFetch(input, init) {
    var next = withBrowserHeader(input, init);
    var sameOrigin = isSameOrigin(input);
    var send = function() {
      var response = nativeFetch(input, next);
      return sameOrigin ? response.then(showConfigRecovery) : response;
    };
    var historyRequest = Vireo.api && Vireo.api.trackHistoryRequest;
    if (sameOrigin && typeof historyRequest === 'function') {
      return historyRequest(input, next, send);
    }
    return send();
  }

  /** @template [T=unknown] @param {RequestInfo | URL} input @param {RequestInit} [init] @param {{toast?: boolean}} [options] @returns {Promise<T>} */
  async function json(input, init, options) {
    var response;
    try {
      response = await browserFetch(input, init);
    } catch (cause) {
      if (cause && typeof cause === 'object' && 'code' in cause && cause.code === 'history_busy') throw cause;
      var networkMessage = (
        'Couldn’t reach Vireo. Check that the app is still running, then try again.'
      );
      if ((!options || options.toast !== false) && typeof global.showToast === 'function') {
        global.showToast(networkMessage, 'error');
      }
      var networkError = Object.assign(new Error(networkMessage, {cause: cause}), {code: 'network_error'});
      throw networkError;
    }
    if (!response.ok) {
      /** @type {Record<string, unknown>} */
      var body = {};
      try {
        /** @type {unknown} */
        var parsed = await response.json();
        if (parsed && typeof parsed === 'object' && !Array.isArray(parsed)) {
          body = /** @type {Record<string, unknown>} */ (parsed);
        }
      } catch (e) {}
      // APIs may provide a stable machine-readable error code alongside a
      // sentence intended for people. Never put the code in the toast when a
      // friendly message is available.
      var requestId = typeof body.request_id === 'string' ? body.request_id : response.headers.get('X-Request-ID');
      var message = (typeof body.message === 'string' && body.message) ||
        (typeof body.error === 'string' && body.error) || 'Request failed (' + response.status + ')';
      if (body.code === 'internal_error' && requestId) {
        message += ' If it keeps happening, report request ID ' + requestId + '.';
      }
      // A superseded read was cancelled because this page already sent a
      // newer one; the caller ignores it, so the user has nothing to see.
      if ((!options || options.toast !== false) && body.code !== 'search_superseded' &&
          typeof global.showToast === 'function') {
        global.showToast(message, 'error');
      }
      var error = Object.assign(new Error(message), {
        status: response.status, code: body.code, requestId: requestId, body: body,
      });
      throw error;
    }
    var text = await response.text();
    return /** @type {T} */ (text ? JSON.parse(text) : null);
  }

  // Names this page load, so a newer read cancels only this page's own
  // superseded reads of the same loader (services/search_lanes.py).
  var searchLanePage = Math.random().toString(36).slice(2, 10) + Date.now().toString(36);

  // Headers that let the server abandon this read once the page sends one
  // with a higher ``seq`` for ``loader``. Only pass a ``seq`` whose older
  // values the page would discard anyway.
  /** @param {string} loader @param {number} seq */
  function searchLaneHeaders(loader, seq) {
    return {
      'X-Vireo-Search-Lane': searchLanePage + ':' + loader,
      'X-Vireo-Search-Seq': String(seq),
    };
  }

  Vireo.api = Object.assign(Vireo.api || {}, {
    searchLaneHeaders: searchLaneHeaders, fetch: browserFetch, json: json, nativeFetch: nativeFetch,
  });
  global.fetch = browserFetch;
})(window);
