/* Lightbox request tokens and the work tied to one: the source-swap timer,
 * image load listeners, and the initial-load handoff. A token names a photo and
 * the navigation generation it was requested in; work started for an earlier
 * request checks its token before it touches the page. Each function takes the
 * session's state record, which session.js creates and keeps private.
 */
export function captureToken(/** @type {LightboxSessionContext} */ session) {
  return Object.freeze({photoId: session.currentId, generation: session.generation});
}

export function isCurrent(/** @type {LightboxSessionContext} */ session, /** @type {LightboxSessionToken} */ token) {
  return session.currentId != null && token.photoId === session.currentId && token.generation === session.generation;
}

export function cancelSwap(/** @type {LightboxSessionContext} */ session) {
  if (session.swapTimer !== null) session.window.clearTimeout(session.swapTimer);
  session.swapTimer = null;
}

export function scheduleSwap(/** @type {LightboxSessionContext} */ session, /** @type {() => void} */ callback, /** @type {number} */ delay) {
  cancelSwap(session);
  var token = captureToken(session);
  var timer = session.window.setTimeout(function() {
    if (session.swapTimer !== timer) return;
    session.swapTimer = null;
    if (isCurrent(session, token)) callback();
  }, delay);
  session.swapTimer = timer;
}

// Both property handlers and event listeners are detached on navigation or
// close. A queued callback also checks its token before touching the host.
export function watchImage(/** @type {LightboxSessionContext} */ session, /** @type {HTMLImageElement} */ img, /** @type {() => void} */ onload, /** @type {(() => void) | null} */ onerror = null, /** @type {boolean} */ properties = false, /** @type {boolean | undefined} */ persistent = false) {
  var token = captureToken(session);
  function cleanup() {
    if (properties) {
      if (img.onload === loaded) img.onload = null;
      if (img.onerror === failed) img.onerror = null;
    } else {
      img.removeEventListener('load', loaded);
      img.removeEventListener('error', failed);
    }
    session.cleanups.delete(cleanup);
  }
  function loaded() {
    if (!persistent) cleanup();
    if (isCurrent(session, token)) onload();
  }
  function failed() {
    if (!persistent) cleanup();
    if (isCurrent(session, token) && onerror) onerror();
  }
  if (properties) { img.onload = loaded; img.onerror = failed; }
  else { img.addEventListener('load', loaded); img.addEventListener('error', failed); }
  session.cleanups.add(cleanup);
  return cleanup;
}

export function loadSource(/** @type {LightboxSessionContext} */ session, /** @type {string} */ url, /** @type {() => void} */ onload, /** @type {(() => void) | null} */ onerror) {
  var img = new session.window.Image();
  watchImage(session, img, onload, onerror, true, false);
  img.src = url;
}

export function setInitialLoad(/** @type {LightboxSessionContext} */ session, /** @type {() => void} */ commitLoad, /** @type {() => void} */ abandonLoad) {
  session.initialLoad = {token: captureToken(session), commit: commitLoad, abandon: abandonLoad};
}

export function clearInitialLoad(/** @type {LightboxSessionContext} */ session, /** @type {(() => void) | undefined} */ commitLoad) {
  if (!commitLoad || (session.initialLoad && session.initialLoad.commit === commitLoad)) session.initialLoad = null;
}

export function finishInitialLoad(/** @type {LightboxSessionContext} */ session, /** @type {boolean} */ abandon) {
  var pending = session.initialLoad;
  session.initialLoad = null;
  if (!pending || !isCurrent(session, pending.token)) return false;
  (abandon ? pending.abandon : pending.commit)();
  return true;
}
