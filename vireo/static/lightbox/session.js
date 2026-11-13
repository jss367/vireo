/* The lightbox session controller: photo identity, asynchronous image work and
 * preload resources. Creation is inert. The host supplies render policy and its
 * live navigation list; viewport, editing and DOM presentation remain with
 * their existing owners. Its state is one record that never leaves this
 * controller: session-requests.js and session-preloads.js hold the functions
 * that read and write it, and callers reach them only through the frozen
 * methods returned here.
 */
import {
  cancelAdjacent, cancelOriginal, clearAdjacent, decodedPreview, hasDecodedSource, preloadStatus,
  scheduleAdjacent, scheduleOriginal,
} from './session-preloads.js';
import {
  cancelSwap, captureToken, clearInitialLoad, finishInitialLoad, isCurrent, loadSource,
  scheduleSwap, setInitialLoad, watchImage,
} from './session-requests.js';

/** @returns {LightboxSessionContext} */
function initialState(/** @type {LightboxSessionOptions} */ options) {
  var window = options.window || globalThis.window;
  return {
    window: window,
    document: window.document,
    options: options,
    currentId: null,           // the photo requested
    displayedId: null,         // the photo whose bitmap is on screen
    generation: 0,
    preloadSerial: 0,
    swapTimer: null,
    initialLoad: null,
    cleanups: new Set(),
    adjacentPreloads: {},      // bounded navigation window, keyed by photo id + source URL
    adjacentTimer: null,
    adjacentRetry: {},
    lastNavDelta: 1,
    originalTimer: null,       // short dwell before warming the current photo's 100% source
    originalPreload: null,     // retained decoded original for an instant first 100% click
    originalWaiting: null,     // dwell completed; waiting for the shared slot
    speculativeInFlight: null, // oldest outstanding request (including pruned entries)
    speculativeLoads: new Set(),
    preloadConcurrency: options.preloadConcurrency || 3,
    preloadBudgetBytes: options.preloadBudgetBytes || 128 * 1024 * 1024,
  };
}

function retire(/** @type {LightboxSessionContext} */ session) {
  ++session.generation;
  cancelSwap(session);
  session.initialLoad = null;
  session.cleanups.forEach(function(cleanup) { cleanup(); });
  session.cleanups.clear();
  cancelOriginal(session);
  cancelAdjacent(session);
}

function begin(/** @type {LightboxSessionContext} */ session, /** @type {number} */ photoId) {
  if (session.currentId == null) session.lastNavDelta = 1;
  retire(session);
  session.currentId = photoId;
  return captureToken(session);
}

function commit(/** @type {LightboxSessionContext} */ session, /** @type {LightboxSessionToken} */ token) {
  if (!isCurrent(session, token)) return false;
  session.displayedId = token.photoId;
  return true;
}

function close(/** @type {LightboxSessionContext} */ session) {
  var closedId = session.displayedId != null ? session.displayedId : session.currentId;
  retire(session);
  session.currentId = null;
  session.displayedId = null;
  clearAdjacent(session);
  return closedId;
}

/** @returns {LightboxSession} */
function create(/** @type {LightboxSessionOptions} */ options) {
  var session = initialState(options);
  return Object.freeze({
    begin: begin.bind(null, session),
    commit: commit.bind(null, session),
    close: close.bind(null, session),
    capture: captureToken.bind(null, session),
    isCurrent: isCurrent.bind(null, session),
    requestedPhotoId: function() { return session.currentId; },
    displayedPhotoId: function() { return session.displayedId; },
    noteDirection: function(/** @type {number} */ delta) { session.lastNavDelta = delta < 0 ? -1 : 1; },
    cancelSwap: cancelSwap.bind(null, session),
    scheduleSwap: scheduleSwap.bind(null, session),
    hasScheduledSwap: function() { return session.swapTimer !== null; },
    watchImage: watchImage.bind(null, session),
    loadSource: loadSource.bind(null, session),
    watchInitialImage: function(/** @type {HTMLImageElement} */ img, /** @type {() => void} */ onload, /** @type {() => void} */ onerror) {
      return watchImage(session, img, onload, onerror, true, true);
    },
    setInitialLoad: setInitialLoad.bind(null, session),
    clearInitialLoad: clearInitialLoad.bind(null, session),
    hasInitialLoad: function() { return session.initialLoad !== null; },
    finishInitialLoad: function() { return finishInitialLoad(session, false); },
    abandonInitialLoad: function() { return finishInitialLoad(session, true); },
    preloadStatus: preloadStatus.bind(null, session),
    hasDecodedSource: hasDecodedSource.bind(null, session),
    decodedPreview: decodedPreview.bind(null, session),
    cancelAdjacent: cancelAdjacent.bind(null, session),
    clearAdjacent: clearAdjacent.bind(null, session),
    cancelOriginal: cancelOriginal.bind(null, session),
    scheduleOriginal: scheduleOriginal.bind(null, session),
    scheduleAdjacent: scheduleAdjacent.bind(null, session),
  });
}
export const VireoLightboxSession = Object.freeze({create: create});
