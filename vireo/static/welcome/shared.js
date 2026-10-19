// The welcome page's boundary with shared code. The setup page loads no
// navbar (only vireo-api.js, vireo-utils.js and the page-find panel), so the
// navbar's safeFetch and safeEventSource never exist here: these are the
// standalone versions the inline script defined as fallbacks, and the page
// uses nothing else from shared code. Page modules import them from here.
// ES module.

/**
 * Fetch JSON, throwing the server's message on a non-2xx response.
 *
 * Unlike the navbar's, it never toasts: this page shows errors itself.
 *
 * @param {string} url @param {RequestInit=} opts
 * @returns {Promise<any>}
 */
export async function safeFetch(url, opts) {
  var resp = await fetch(url, opts);
  if (!resp.ok) {
    /** @type {{message?: string, error?: string}} */
    var body = {};
    try { body = await resp.json(); } catch(e) {}
    throw new Error(body.message || body.error || 'Request failed (' + resp.status + ')');
  }
  var text = await resp.text();
  if (!text) return null;
  return JSON.parse(text);
}

/**
 * Follow a job's progress stream until it completes or the connection drops.
 *
 * @param {string} url
 * @param {{onProgress?(progress: WelcomeJobProgress): void,
 *   onComplete?(result: WelcomeJobResult): void, onError?(): void}=} callbacks
 */
export function safeEventSource(url, callbacks) {
  callbacks = callbacks || {};
  var handlers = callbacks;
  var source = new EventSource(url);
  source.addEventListener('progress', function(e) {
    if (handlers.onProgress) handlers.onProgress(JSON.parse(/** @type {MessageEvent<string>} */ (e).data));
  });
  source.addEventListener('complete', function(e) {
    source.close();
    if (handlers.onComplete) handlers.onComplete(JSON.parse(/** @type {MessageEvent<string>} */ (e).data));
  });
  source.addEventListener('error', function() {
    source.close();
    if (handlers.onError) handlers.onError();
  });
  return source;
}

/** @param {unknown} error */
export function errorText(error) {
  return (error instanceof Error && error.message) || String(error);
}
