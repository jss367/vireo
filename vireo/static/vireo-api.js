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

  var Vireo = global.Vireo = global.Vireo || {};
  var nativeFetch = global.fetch.bind(global);
  var SAFE_METHODS = {GET: true, HEAD: true, OPTIONS: true};

  function isSameOrigin(input) {
    try {
      var raw = input instanceof Request ? input.url : input;
      return new URL(String(raw), global.location.href).origin === global.location.origin;
    } catch (e) {
      return false;
    }
  }

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

  function browserFetch(input, init) {
    return nativeFetch(input, withBrowserHeader(input, init));
  }

  async function json(input, init, options) {
    var response;
    try {
      response = await browserFetch(input, init);
    } catch (cause) {
      var networkMessage = (
        'Couldn’t reach Vireo. Check that the app is still running, then try again.'
      );
      if ((!options || options.toast !== false) && typeof global.showToast === 'function') {
        global.showToast(networkMessage, 'error');
      }
      var networkError = new Error(networkMessage);
      networkError.code = 'network_error';
      networkError.cause = cause;
      throw networkError;
    }
    if (!response.ok) {
      var body = {};
      try { body = await response.json(); } catch (e) {}
      // APIs may provide a stable machine-readable error code alongside a
      // sentence intended for people. Never put the code in the toast when a
      // friendly message is available.
      var requestId = body.request_id || response.headers.get('X-Request-ID');
      var message = body.message || body.error || 'Request failed (' + response.status + ')';
      if (body.code === 'internal_error' && requestId) {
        message += ' If it keeps happening, report request ID ' + requestId + '.';
      }
      if ((!options || options.toast !== false) && typeof global.showToast === 'function') {
        global.showToast(message, 'error');
      }
      var error = new Error(message);
      error.status = response.status;
      error.code = body.code;
      error.requestId = requestId;
      error.body = body;
      throw error;
    }
    var text = await response.text();
    return text ? JSON.parse(text) : null;
  }

  Vireo.api = Vireo.api || {};
  Vireo.api.fetch = browserFetch;
  Vireo.api.json = json;
  Vireo.api.nativeFetch = nativeFetch;
  global.fetch = browserFetch;
})(window);
