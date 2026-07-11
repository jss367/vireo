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
    var response = await browserFetch(input, init);
    if (!response.ok) {
      var body = {};
      try { body = await response.json(); } catch (e) {}
      var message = body.error || 'Request failed (' + response.status + ')';
      if ((!options || options.toast !== false) && typeof global.showToast === 'function') {
        global.showToast(message, 'error');
      }
      var error = new Error(message);
      error.status = response.status;
      error.code = body.code;
      error.requestId = body.request_id || response.headers.get('X-Request-ID');
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
