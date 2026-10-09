/* ---------- Missing Folders Banner ---------- */
let _missingBannerDismissedCount = 0;
// Snapshot of missing-folder ids from the last /api/folders/missing poll.
// The server's own _folder_health_loop (app.py) flips folders ok\u2194missing on
// its 10-minute cadence with no client involvement, so the modal's
// /api/folders/check-health POST is only one of two paths that mutate this
// set. Without a diff here a long-lived Browse view would keep serving its
// pre-flip photo grid until the user manually reopened the modal or
// reloaded \u2014 the background reconnect that just restored the folder would
// never reach the page (Codex review r3685083009).
let _missingFoldersLastIds = null;
// Race guards for the two paths that observe the missing-folder set —
// the GET poll (checkMissingFolders) and the modal's mutating check-
// health POST (loadMissingFolders). Issuance order alone is not a
// freshness guarantee across them: the POST commits DB updates a
// concurrent GET can't yet see, so a GET started after the POST can
// still return an older snapshot than what the POST will observe once
// its filesystem check finishes. Semantics:
//   * ``_missingFoldersObservationGen`` bumps on every GET only, for
//     GET-vs-GET supersession.
//   * ``_missingFoldersMutationGen`` bumps on every POST only, for
//     POST-vs-POST supersession (a modal reopen while an earlier POST
//     is still in flight; the newest post-mutation observation wins).
//   * ``_missingFoldersMutationInFlight`` counts running POSTs. A GET
//     whose response arrives while > 0 has observed pre-mutation state
//     and must skip its snapshot update / dispatch — the POST is the
//     freshness authority and will fire the event itself
//     (Codex review r3685627312).
//   * ``_missingFoldersMutationEpoch`` bumps when a POST finishes. A
//     GET snapshots it at start and skips if it changed by completion —
//     catches the case where a POST started AFTER the GET and completed
//     BEFORE it (also stale for the GET).
// Together, these preserve r3685515796's guarantee — a stale GET can
// never overwrite a fresher POST — while also giving the POST
// precedence over GETs that happen to start after it
// (Codex review r3685627312).
let _missingFoldersObservationGen = 0;
let _missingFoldersMutationGen = 0;
let _missingFoldersMutationInFlight = 0;
let _missingFoldersMutationEpoch = 0;
// When Browse abandons its short-term retry loop after a folder-health event
// (``/api/folders`` stayed down through every retry), the pre-transition
// grid/tree is still on screen but ``_missingFoldersLastIds`` has already
// advanced to the post-transition IDs. The normal 10-minute poll then sees
// unchanged IDs and never re-emits, so the page stays stale until an
// unrelated health flip or a reload (Codex review r3687331927). This flag
// tells the next successful poll to dispatch a synthetic reconciliation
// event regardless of the ID diff, so Browse can retry the refresh once the
// endpoint recovers.
let _missingFoldersReconciliationPending = false;
window.markMissingFoldersReconciliationPending = function() {
  _missingFoldersReconciliationPending = true;
};
// Incremented whenever a GET, POST, or Browse init snapshot becomes the
// client-side baseline. Browse captures this immediately before requesting
// /api/browse/init, which lets it distinguish a baseline that was already
// applied (init is newer) from one that landed while init was in flight
// (the later observation remains authoritative).
let _missingFoldersSnapshotVersion = 0;
// Monotonic SQLite-backed observation marker returned by every server
// endpoint that can establish the missing-folder baseline. Unlike client
// request generations, this orders responses even when an older server read
// is delivered before a newer init response is applied.
let _missingFoldersServerVersion = null;

function _parseFolderHealthVersion(value) {
  const parsed = Number(value);
  return Number.isSafeInteger(parsed) && parsed >= 0 ? parsed : null;
}

function _missingFolderIdsDiffer(prev, curr) {
  if (prev.length !== curr.length) return true;
  for (let i = 0; i < prev.length; i++) {
    if (prev[i] !== curr[i]) return true;
  }
  return false;
}

// Fire vireo:folder-health-changed based on the transition of the
// workspace-scoped missing-folder ID set — not on the modal POST's
// ``changed`` count, which reflects a global scan and would trigger a
// spurious Browse reset when a folder in another workspace flipped
// (Codex review r3685295409). Returns whether an event was dispatched.
function _dispatchFolderHealthChangeFromIds(prevIds, currentIds, source) {
  if (prevIds === null) return false;
  if (!_missingFolderIdsDiffer(prevIds, currentIds)) return false;
  const prevSet = new Set(prevIds);
  const currSet = new Set(currentIds);
  const restored = prevIds.filter(id => !currSet.has(id));
  const wentMissing = currentIds.filter(id => !prevSet.has(id));
  document.dispatchEvent(new CustomEvent('vireo:folder-health-changed', {
    detail: { restored, wentMissing, source },
  }));
  return true;
}

// Shared banner-update helper used by both the GET poll
// (checkMissingFolders) and the modal POST (loadMissingFolders). The POST
// must NOT delegate to checkMissingFolders here: if the user closes the
// modal while its POST is still pending, closeMissingFoldersModal fires a
// GET that our own _missingFoldersMutationInFlight guard rejects, and a
// tail-call checkMissingFolders() from the POST body is rejected too —
// its GET starts while the in-flight counter is still > 0, then finishes
// after our finally bumps the mutation epoch (stale by both checks). The
// banner would then keep showing the pre-check state until the next
// 10-minute poll (Codex review r3686095481). Updating from the POST's
// authoritative response bypasses the guards entirely.
function _updateMissingFoldersBanner(folders) {
  const banner = document.getElementById('missingFoldersBanner');
  const msg = document.getElementById('missingFoldersMsg');
  if (!banner || !msg) return;
  if (folders.length > 0 && folders.length !== _missingBannerDismissedCount) {
    const s = folders.length === 1 ? '' : 's';
    msg.textContent = `${folders.length} folder${s} can’t be found on disk.`;
    banner.style.display = 'flex';
  } else if (folders.length === 0) {
    banner.style.display = 'none';
    _missingBannerDismissedCount = 0;
  }
}

// Reconcile the missing-folder IDs bundled into /api/browse/init with the
// navbar poll/POST baseline. If the baseline version has not changed since
// the init request began, init is known to be the newer observation: adopt
// it and update the banner without dispatching a backwards transition. If
// the version changed in flight, keep that later observation and dispatch
// init→current so Browse discards its stale init payload.
function _reconcileMissingFoldersInitSnapshot(
  initIds, snapshotVersionAtStart, source, initServerVersion
) {
  const currentIds = initIds.slice().sort((a, b) => a - b);
  const serverVersion = _parseFolderHealthVersion(initServerVersion);
  let initIsNewer;
  if (_missingFoldersServerVersion === null || serverVersion === null) {
    initIsNewer = _missingFoldersLastIds === null ||
      snapshotVersionAtStart === _missingFoldersSnapshotVersion;
  } else if (serverVersion !== _missingFoldersServerVersion) {
    initIsNewer = serverVersion > _missingFoldersServerVersion;
  } else {
    initIsNewer = _missingFoldersLastIds === null ||
      snapshotVersionAtStart === _missingFoldersSnapshotVersion;
  }
  if (initIsNewer) {
    _missingFoldersLastIds = currentIds;
    if (serverVersion !== null) _missingFoldersServerVersion = serverVersion;
    _missingFoldersSnapshotVersion++;
    _updateMissingFoldersBanner(currentIds.map(id => ({ id })));
    return false;
  }
  const dispatched = _dispatchFolderHealthChangeFromIds(
    currentIds, _missingFoldersLastIds, source);
  if (dispatched) return true;
  // A higher server version proves the init payload is stale even when the
  // missing-ID set is unchanged (for example a healthy folder was linked,
  // unlinked, inserted, or deleted). Force the conservative no-ID refresh so
  // bootstrap does not render the obsolete folder tree/photo grid.
  if (serverVersion !== null && _missingFoldersServerVersion !== null &&
      serverVersion < _missingFoldersServerVersion) {
    document.dispatchEvent(new CustomEvent('vireo:folder-health-changed', {
      detail: { restored: [], wentMissing: [], source },
    }));
    return true;
  }
  return false;
}

// Wait for every in-flight POST mutation to settle, then run one
// ``checkMissingFolders()``. Used by loadMissingFolders's catch block
// to guarantee its recovery poll isn't swallowed by its own in-flight
// counter — which would happen when two POSTs overlap and the newer
// one fails while the older one is still pending. A firehose safety
// bound (100 × 50ms = 5s) keeps this from spinning forever if a hung
// fetch somehow leaves the counter stuck (Codex review r3686778064).
//
// If the counter never reaches zero within 5s we drop the recovery
// entirely instead of firing ``checkMissingFolders()`` anyway: its
// own in-flight guard would reject the GET, wasting a round-trip and
// giving the false impression that recovery ran. The 10-minute
// background poll is the only remaining fallback for a genuinely
// hung POST (Codex review r3686842771).
let _missingFoldersRecoveryScheduled = false;
function _scheduleMissingFoldersRecovery() {
  if (_missingFoldersRecoveryScheduled) return;
  _missingFoldersRecoveryScheduled = true;
  let attempts = 0;
  const tick = function() {
    if (_missingFoldersMutationInFlight === 0) {
      _missingFoldersRecoveryScheduled = false;
      checkMissingFolders();
      return;
    }
    if (attempts >= 100) {
      _missingFoldersRecoveryScheduled = false;
      return;
    }
    attempts++;
    setTimeout(tick, 50);
  };
  setTimeout(tick, 0);
}

async function checkMissingFolders() {
  const myGen = ++_missingFoldersObservationGen;
  const mutationEpochAtStart = _missingFoldersMutationEpoch;
  const snapshotVersionAtStart = _missingFoldersSnapshotVersion;
  try {
    const resp = await fetch('/api/folders/missing');
    if (!resp.ok) return;
    const serverVersion = _parseFolderHealthVersion(
      resp.headers.get('X-Vireo-Folder-Health-Version'));
    const folders = await resp.json();
    // A newer GET started while this fetch was pending \u2014 its result
    // supersedes ours (Codex review r3685515796). Skip the whole
    // response: banner too, since a stale banner update would just
    // paint a false state that the newer GET is about to overwrite
    // anyway.
    if (myGen !== _missingFoldersObservationGen) return;
    // A mutating /api/folders/check-health POST is either in flight or
    // completed during our await; its post-mutation observation is the
    // freshness authority and this GET may only reflect pre-mutation
    // truth. Skip snapshot + dispatch + banner so a pre-commit read
    // can't paint the banner ahead of the POST or fire a false
    // ``wentMissing`` for a transition the POST will handle
    // (Codex review r3685627312). The POST paints the banner directly
    // from its own authoritative response via _updateMissingFoldersBanner
    // — see r3686095481 for why it can't safely delegate back here.
    if (_missingFoldersMutationInFlight > 0 ||
        mutationEpochAtStart !== _missingFoldersMutationEpoch) return;
    // Prefer the server's monotonic observation ordering. The client version
    // remains the compatibility/tie-break guard when the response lacks the
    // marker or observed the same SQLite health version.
    if (_missingFoldersServerVersion !== null && serverVersion !== null) {
      if (serverVersion < _missingFoldersServerVersion) return;
      if (serverVersion === _missingFoldersServerVersion &&
          snapshotVersionAtStart !== _missingFoldersSnapshotVersion) return;
    } else if (snapshotVersionAtStart !== _missingFoldersSnapshotVersion) {
      return;
    }
    const currentIds = folders.map(f => f.id).sort((a, b) => a - b);
    const dispatched = _dispatchFolderHealthChangeFromIds(
      _missingFoldersLastIds, currentIds, 'poll');
    // Browse abandoned an earlier refresh because ``/api/folders`` stayed
    // down through every retry; ``_missingFoldersLastIds`` was already
    // advanced then, so this poll's diff can be empty even though the page
    // is still on pre-transition data. Force a synthetic reconciliation so
    // Browse re-attempts the refresh now that the endpoint is answering
    // again. Empty restored/wentMissing routes through the conservative
    // reload path in ``folderHealthTouchesActiveScope`` (Codex review
    // r3687331927).
    if (!dispatched && _missingFoldersReconciliationPending) {
      document.dispatchEvent(new CustomEvent('vireo:folder-health-changed', {
        detail: { restored: [], wentMissing: [], source: 'poll-reconcile' },
      }));
    }
    _missingFoldersReconciliationPending = false;
    _missingFoldersLastIds = currentIds;
    if (serverVersion !== null) _missingFoldersServerVersion = serverVersion;
    _missingFoldersSnapshotVersion++;
    _updateMissingFoldersBanner(folders);
  } catch (e) { /* ignore */ }
}

function dismissMissingBanner() {
  document.getElementById('missingFoldersBanner').style.display = 'none';
  const msg = document.getElementById('missingFoldersMsg').textContent;
  const match = msg.match(/^(\d+)/);
  if (match) _missingBannerDismissedCount = parseInt(match[1]);
}

// Check on page load and every 10 minutes while the window is visible
checkMissingFolders();
Vireo.pollWhileVisible(checkMissingFolders, 600000);

/* ---------- Missing Originals Banner ---------- */
let _missingPhotosCache = [];
// Whatever rows the currently open Missing Originals modal is showing,
// keyed by photo id. Populated by every modal load — including scoped
// ones that intentionally skip _missingPhotosCache to keep the banner's
// dismissed-count math honest — so sidecar cleanup can still look up
// has_xmp_sidecar for a freshly discovered ghost that only exists in a
// folder-scoped view.
let _missingPhotosModalRows = new Map();
let _missingPhotosBannerDismissedCount = 0;
// Both the banner and modal read cached /api/photos/missing status. A slow
// earlier fetch returning after a newer one used to overwrite fresh state
// with stale results — leaving the banner showing ghosts the user just
// deleted.
//
// Cache writes share a single monotonic id across both flows, so only the
// most-recently-started fetch (regardless of which flow issued it) can
// overwrite the shared cache. UI rendering uses per-flow ids so a banner
// poll firing mid-modal-load can't cancel the modal's own render and leave
// it stuck on "Checking photos…", and vice versa.
let _missingPhotosCacheFetchId = 0;
let _missingPhotosBannerFetchId = 0;
let _missingPhotosModalFetchId = 0;
let _missingPhotosBannerInFlight = false;
// Separate timer handles per target: a banner status poll firing while the
// modal is waiting on a long scan used to cancel the modal's own poll (they
// shared a single handle), leaving the modal stuck on "Checking missing
// originals…" until the user closed and reopened it.
let _missingPhotosBannerStatusPoll = null;
let _missingPhotosModalStatusPoll = null;
const MISSING_PHOTOS_INITIAL_DELAY_MS = 180000;
const MISSING_PHOTOS_AUTOMATIC_INTERVAL_MS = 30 * 60 * 1000;

function _missingPhotosStatusUrl(folderId) {
  return folderId != null
    ? '/api/photos/missing?folder_id=' + encodeURIComponent(folderId)
    : '/api/photos/missing';
}

function _missingPhotosPayloadPhotos(payload) {
  if (Array.isArray(payload)) return payload;  // legacy/test fallback
  return (payload && Array.isArray(payload.photos)) ? payload.photos : [];
}

function _scheduleMissingPhotosPoll(folderId, modal) {
  if (modal) {
    if (_missingPhotosModalStatusPoll !== null) clearTimeout(_missingPhotosModalStatusPoll);
    _missingPhotosModalStatusPoll = setTimeout(function() {
      _missingPhotosModalStatusPoll = null;
      loadMissingPhotos();
    }, 3000);
  } else {
    if (_missingPhotosBannerStatusPoll !== null) clearTimeout(_missingPhotosBannerStatusPoll);
    _missingPhotosBannerStatusPoll = setTimeout(function() {
      _missingPhotosBannerStatusPoll = null;
      checkMissingPhotos();
    }, 3000);
  }
}

async function startMissingPhotosCheck(opts) {
  opts = opts || {};
  const body = { automatic: !!opts.automatic };
  if (opts.folderId != null) body.folder_id = opts.folderId;
  const resp = await fetch('/api/photos/missing/check', {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify(body),
  });
  if (!resp.ok) throw new Error('missing originals check failed');
  const payload = await resp.json();
  if (payload && payload.pending) {
    _scheduleMissingPhotosPoll(opts.folderId || null, !!opts.modal);
  } else if (opts.automatic) {
    checkMissingPhotos();
  }
  return payload;
}

async function checkMissingPhotos() {
  if (_missingPhotosBannerInFlight) {
    // Supersede the in-flight banner poll before returning: bump the
    // generation counters so the still-running fetch's response is
    // treated as stale and cannot repaint the banner or overwrite the
    // cache with a pre-invalidation payload once it lands. Without this
    // bump, a newer call fired after e.g. a batch-delete invalidation
    // would return silently and the older response would pass its own
    // ``myId`` checks and paint just-deleted photos back into view.
    _missingPhotosCacheFetchId++;
    _missingPhotosBannerFetchId++;
    // Bumping the generation only invalidates the older response — it
    // does NOT send a new /api/photos/missing GET, so an already-visible
    // banner (with the pre-invalidation count) would stay on-screen
    // until the next 30-minute automatic tick. Schedule a follow-up
    // poll so the banner actually catches up once the in-flight fetch
    // clears; if it's still busy when the timer fires we'll re-enter
    // this branch and re-arm, keeping the banner honest without
    // aborting the in-flight request.
    _scheduleMissingPhotosPoll(null, false);
    return;
  }
  _missingPhotosBannerInFlight = true;
  const cacheId = ++_missingPhotosCacheFetchId;
  const myId = ++_missingPhotosBannerFetchId;
  try {
    const resp = await fetch('/api/photos/missing');
    if (!resp.ok) return;
    if (myId !== _missingPhotosBannerFetchId) return;  // a newer banner poll is authoritative
    const payload = await resp.json();
    if (myId !== _missingPhotosBannerFetchId) return;
    const photos = _missingPhotosPayloadPhotos(payload);
    const ready = Array.isArray(payload) || (payload && payload.status === 'ready');
    if (payload && payload.pending) _scheduleMissingPhotosPoll(null, false);
    if (!ready) {
      const banner = document.getElementById('missingPhotosBanner');
      if (banner) banner.style.display = 'none';
      return;
    }
    if (cacheId === _missingPhotosCacheFetchId) _missingPhotosCache = photos;
    const banner = document.getElementById('missingPhotosBanner');
    const msg = document.getElementById('missingPhotosMsg');
    if (photos.length > 0 && photos.length !== _missingPhotosBannerDismissedCount) {
      const s = photos.length === 1 ? '' : 's';
      msg.textContent = `${photos.length} photo${s} reference${photos.length === 1 ? 's' : ''} a missing original.`;
      banner.style.display = 'flex';
    } else if (photos.length === 0) {
      banner.style.display = 'none';
      _missingPhotosBannerDismissedCount = 0;
    }
  } catch (e) { /* ignore */ }
  finally {
    _missingPhotosBannerInFlight = false;
  }
}

function dismissMissingPhotosBanner() {
  document.getElementById('missingPhotosBanner').style.display = 'none';
  _missingPhotosBannerDismissedCount = _missingPhotosCache.length;
}

// Missing-originals scan touches every photo's source path on disk. On large
// libraries backed by SMB/NAS volumes it can take minutes, so startup uses a
// delayed background job and the banner only renders cached results. After the
// initial run, keep re-checking so a long-lived tab still picks up files
// deleted, or skipped earlier because a heavy job was active, but only while
// the window is visible: a hidden window left open overnight used to re-stat
// the whole library every 30 minutes for a banner nobody was reading.
//
// Read any existing cache immediately: this app spans multiple pages, so each
// navigation resets the delayed POST timer. Without this cheap cache-only GET,
// a ready payload from a prior scan or another tab would stay hidden for the
// full delay window even though showing it costs no filesystem work.
checkMissingPhotos();
Vireo.pollWhileVisible(
  function() { return startMissingPhotosCheck({ automatic: true }); },
  MISSING_PHOTOS_AUTOMATIC_INTERVAL_MS,
  { initialDelayMs: MISSING_PHOTOS_INITIAL_DELAY_MS }
);

/* ---------- New Images Banner ---------- */
// Per-workspace dismissal: one sessionStorage key per workspace id so that
// switching workspaces doesn't clobber other workspaces' dismissal state.
// Storage value is just the dismissed count (string).
//
// Manual trace scenarios (keep in sync with the re-arm semantics below):
//   - Dismiss in A at count=20 -> key `newImagesDismissed_ws_1 = "20"`.
//     Switch to B -> B's dismissal read from its own key (absent/different).
//     Switch back to A with count=20 -> still dismissed. Correct.
//   - Dismiss in A at 20, dismiss in B at 5 -> two independent keys. Correct.
//   - Dismiss in A at 20, count stays at 20 -> banner hidden (dismissal still
//     active). Correct.
//   - Dismiss in A at 20, A's count rises to 25 -> 25 !== 20 -> banner shows
//     (count mismatch). Correct.
//   - Dismiss in A at 20, scan clears it to 0, then 20 new imports arrive ->
//     banner shows. Correct: the 0 observation clears the dismissal (see
//     checkNewImages), so the subsequent 20 is treated as fresh news rather
//     than being suppressed by the old "20 === 20" exact match.
const NEW_IMAGES_DISMISS_PREFIX = 'newImagesDismissed_ws_';

function _newImagesDismissKey(wsId) {
  return NEW_IMAGES_DISMISS_PREFIX + String(wsId);
}

// Dismissal suppresses the banner only while the count remains exactly what
// the user dismissed. A rising/falling count to a *different* non-zero value
// re-arms the banner via the exact-match check below. The zero-count reset
// lives in checkNewImages (not here) because it needs to write to storage,
// not just read it.
function _isNewImagesDismissed(wsId, newCount, offlineRoots) {
  if (wsId === null || wsId === undefined || wsId === '') return false;
  const raw = sessionStorage.getItem(_newImagesDismissKey(wsId));
  if (!raw) return false;
  const stored = Number(raw);
  if (!Number.isFinite(stored)) return false;
  if (stored !== Number(newCount)) return false;
  // The dismissal also covers the exact set of offline roots shown at the
  // time. A volume dropping (or coming back) while the reachable count stays
  // the same is news the user has not seen yet, so it re-arms the banner.
  const storedOffline = sessionStorage.getItem(_newImagesOfflineDismissKey(wsId)) || '';
  return storedOffline === _offlineRootsKey(offlineRoots || []);
}

// Single-slot retry timer + in-flight guard so a long pending state can't
// stack independent 3s polling chains on top of each 60s interval tick.
// Without the in-flight guard, two ticks whose fetches are still in flight
// both schedule timers when their responses land, leaking chains; the
// start-of-call clearTimeout only catches the most recent handle.
let _newImagesPendingTimer = null;
let _newImagesInFlight = false;
// Set when a *forced* check — the manual recheck — is dropped by the
// in-flight guard. That poll was issued before the recheck cleared the
// caches, so its answer predates the request and the client would otherwise
// render the pre-recheck state and wait out the full 60s interval before
// walking again. Run one more check the moment it lands instead.
let _newImagesForcedRerun = false;
// Non-zero while a manual recheck is outstanding. The click takes a token and
// the token is recorded in _newImagesInvalidatedToken once the POST that
// clears the caches returns. A poll that was already in flight at either
// moment is answering a question from before the invalidation: it must not
// release the button or present its payload as the recheck's answer, or the
// button reads "Check again" while the recheck is still running and a second
// click can fire a duplicate invalidation.
let _newImagesRecheckToken = 0;
let _newImagesRecheckSeq = 0;
let _newImagesInvalidatedToken = 0;

// Whether a call that started with ``startInvalidated`` is entitled to
// conclude the outstanding recheck. Evaluated at render time, because the
// token can change while a call is in flight.
function _newImagesAnswersRecheck(startInvalidated) {
  return _newImagesRecheckToken === 0
    || startInvalidated === _newImagesRecheckToken;
}

async function checkNewImages(options) {
  if (_newImagesInFlight) {
    if (options && options.force) _newImagesForcedRerun = true;
    return;
  }
  const startInvalidated = _newImagesInvalidatedToken;
  if (_newImagesPendingTimer !== null) {
    clearTimeout(_newImagesPendingTimer);
    _newImagesPendingTimer = null;
  }
  _newImagesInFlight = true;
  try {
    // While a manual "Check again" is outstanding, ask the server to bypass
    // the foreground-job deferral. Without this, a recheck during a long
    // pipeline job returns pending+deferred_reason and the button stays
    // "Checking..." for the rest of the job — the user's explicit "look
    // now" click gets treated like an automatic poll.
    const url = _newImagesRecheckToken > 0
      ? '/api/workspaces/active/new-images?manual_recheck=1'
      : '/api/workspaces/active/new-images';
    const resp = await fetch(url);
    if (!resp.ok) {
      _failNewImagesRecheck(_newImagesAnswersRecheck(startInvalidated));
      return;
    }
    const data = await resp.json();
    // The backend returns ``pending: true`` when a fresh-cache walk is still
    // running in the background. The result will land in the cache shortly,
    // so re-poll soon instead of waiting for the next 60s tick.
    if (data && data.pending) {
      if (data.deferred_reason) {
        const stamp = document.getElementById('newImagesCheckedAt');
        if (stamp) {
          stamp.textContent = data.deferred_reason === 'storage_move_active'
            ? 'Check deferred until the storage move finishes'
            : 'Check deferred until active processing finishes';
        }
      }
      _newImagesPendingTimer = setTimeout(checkNewImages, data.deferred_reason ? 15000 : 3000);
      return;
    }
    // Persistent backend failure (unreachable volume, DB error). Don't
    // re-poll fast — let the 60s interval try again after the backoff
    // window so we don't hammer a broken resource. Hide the banner.
    if (data && data.error) {
      const banner = document.getElementById('newImagesBanner');
      if (banner) banner.style.display = 'none';
      if (!_newImagesForcedRerun
          && _newImagesAnswersRecheck(startInvalidated)) {
        _newImagesRecheckToken = 0;
        _setNewImagesRecheckBusy(false);
      }
      return;
    }
    const banner = document.getElementById('newImagesBanner');
    const msg = document.getElementById('newImagesMsg');
    if (!banner || !msg) return;
    const wsId = (data && data.workspace_id != null) ? String(data.workspace_id) : '';

    // Count dropping to zero means the user resolved the backlog (e.g. ran a
    // scan / import). Clear any prior dismissal so a future non-zero count is
    // treated as fresh news — without this, the exact-match suppression
    // would keep "20 === 20" active through the round-trip dismiss-20 ->
    // scan-to-0 -> import-20-again, leaving the banner hidden when it
    // should show.
    // Roots the walk could not check because their volume is offline. The
    // count above covers only the reachable roots, so the banner must say
    // so rather than present a partial number as the whole truth.
    const unreachable = Array.isArray(data.unreachable_roots) ? data.unreachable_roots : [];
    // Folders whose originals are staged as a local copy (Work Locally). The
    // walk leaves them out -- the catalog knows those photos by their local
    // path, and an import of them is refused until the copy is synced or
    // discarded -- so the count does not cover them and the banner says so.
    const localCopies = Array.isArray(data.local_copy_excluded) ? data.local_copy_excluded : [];
    const uncheckedRoots = [
      ...unreachable.map(path => 'offline:' + path),
      ...localCopies.map(path => 'local:' + path),
    ];
    const cta = banner.querySelector('.banner-cta');

    // Only a *real* zero — every root checked, nothing new — resets the
    // dismissal. A zero with offline roots is "unknown", not "resolved".
    if (data.new_count === 0 && !uncheckedRoots.length && wsId !== '') {
      sessionStorage.removeItem(_newImagesDismissKey(wsId));
      sessionStorage.removeItem(_newImagesOfflineDismissKey(wsId));
    }

    banner.dataset.ws = wsId;
    if (uncheckedRoots.length) {
      banner.dataset.offline = _offlineRootsKey(uncheckedRoots);
    } else {
      delete banner.dataset.offline;
    }

    if (data.new_count > 0 && wsId !== '' && !_isNewImagesDismissed(wsId, data.new_count, uncheckedRoots)) {
      const s = data.new_count === 1 ? '' : 's';
      let text = `${data.new_count} new image${s} detected in your registered folders.`;
      if (unreachable.length) {
        text += ` ${_offlineRootsPhrase(unreachable)} offline and not checked.`;
      }
      text += _localCopiesSentence(localCopies);
      msg.textContent = text;
      if (cta) cta.style.display = '';
      _applyOfflineBannerDetail(unreachable, data.checked_at);
      _appendBannerTitlePaths(localCopies);
      banner.dataset.count = String(data.new_count);
      banner.style.display = 'flex';
    } else if (data.new_count === 0 && uncheckedRoots.length && wsId !== '' && !_isNewImagesDismissed(wsId, 0, uncheckedRoots)) {
      // Nothing importable was found, but that is not a real zero: at least
      // one registered folder is on a volume that is offline right now.
      // Say that instead of silently hiding, and offer nothing to import.
      // (Gated on a zero count: a dismissed *positive* banner with offline
      // roots must stay dismissed, not fall through to this notice.)
      msg.textContent = (unreachable.length
        ? `Couldn't check for new images: ${_offlineRootsPhrase(unreachable)} offline.`
        : 'No new images found in the checked folders.')
        + _localCopiesSentence(localCopies);
      if (cta) cta.style.display = 'none';
      _applyOfflineBannerDetail(unreachable, data.checked_at);
      _appendBannerTitlePaths(localCopies);
      banner.dataset.count = '0';
      banner.style.display = 'flex';
    } else {
      banner.style.display = 'none';
    }
    // A payload we could render is a finished answer, so the manual
    // "Check again" is done regardless of which branch above ran — unless a
    // forced re-poll is queued behind this one, in which case this payload
    // predates the recheck and the button is still working.
    if (!_newImagesForcedRerun && _newImagesAnswersRecheck(startInvalidated)) {
      _newImagesRecheckToken = 0;
      _setNewImagesRecheckBusy(false);
    }
  } catch (e) {
    // Network error or malformed payload: the recheck produced no answer,
    // so hand the button back rather than leaving it disabled.
    _failNewImagesRecheck(_newImagesAnswersRecheck(startInvalidated));
  }
  finally {
    _newImagesInFlight = false;
    if (_newImagesForcedRerun) {
      _newImagesForcedRerun = false;
      // Supersedes any pending-retry timer this call just scheduled: the
      // queued check is the one that observes post-invalidation state.
      if (_newImagesPendingTimer !== null) clearTimeout(_newImagesPendingTimer);
      _newImagesPendingTimer = setTimeout(
        () => checkNewImages({force: true}), 0,
      );
    }
  }
}

// Beyond this many folders the list is summarized; the full set is always
// on the message's title attribute.
const OFFLINE_ROOTS_MAX_LISTED = 6;

// Subject of "... offline", naming the roots the walk had to skip.
//
// Registered folders are typically siblings under one long path, so spelling
// out five absolute paths inline buries the only part that differs. Factor
// the shared parent out and list just the tails: the differences end up next
// to each other instead of at the end of five near-identical strings.
function _offlineRootsPhrase(roots) {
  if (roots.length === 1) return `${roots[0]} is`;
  const sorted = roots.slice().sort();
  const partsList = sorted.map(p => p.split(/[\\/]/));
  const depth = _commonParentDepth(partsList);
  // Depth 1 is just the filesystem root (or a bare drive) — naming it saves
  // nothing and the "tails" would still be near-full paths.
  const sep = (sorted[0].indexOf('\\') !== -1 && sorted[0].indexOf('/') === -1)
    ? '\\' : '/';
  const prefix = depth >= 2 ? partsList[0].slice(0, depth).join(sep) : '';
  const labels = partsList.map(parts => parts.slice(prefix ? depth : 0).join(sep));
  const shown = labels.slice(0, OFFLINE_ROOTS_MAX_LISTED);
  const hidden = labels.length - shown.length;
  const list = hidden > 0
    ? `${shown.join(', ')} and ${hidden} more`
    : shown.join(', ');
  return `${sorted.length} folders${prefix ? ` in ${prefix}` : ''} (${list}) are`;
}

// Number of leading path components every path shares, never consuming a
// path's own last component so each one keeps a non-empty tail.
function _commonParentDepth(partsList) {
  const shortest = Math.min(...partsList.map(parts => parts.length));
  let depth = 0;
  while (depth < shortest - 1
         && partsList.every(parts => parts[depth] === partsList[0][depth])) {
    depth++;
  }
  return depth;
}

// Detail that rides along with any offline notice: every full path on hover
// (the sentence itself shows only what differs), the "Check again" button,
// and the time the walk behind this answer ran — without that stamp, a
// recheck of a still-offline volume redraws the same sentence and looks
// like nothing happened.
function _applyOfflineBannerDetail(roots, checkedAt) {
  const msg = document.getElementById('newImagesMsg');
  const recheck = document.getElementById('newImagesRecheck');
  const stamp = document.getElementById('newImagesCheckedAt');
  if (msg) msg.title = roots.length ? roots.join('\n') : '';
  if (recheck) recheck.style.display = roots.length ? '' : 'none';
  if (stamp) {
    const when = roots.length ? _formatCheckedAt(checkedAt) : '';
    stamp.textContent = when ? `checked ${when}` : '';
  }
}

// Sentence naming the folders the walk left out because they are working
// locally (empty when there are none), with a leading space so it appends.
function _localCopiesSentence(localCopies) {
  if (!localCopies.length) return '';
  const one = localCopies.length === 1;
  return ` ${_offlineRootsPhrase(localCopies)} working locally and not checked;`
    + ` sync or discard ${one ? 'that local copy' : 'those local copies'}`
    + ` to include ${one ? 'it' : 'them'}.`;
}

// Add full paths to the banner message's hover text after whatever
// _applyOfflineBannerDetail put there.
function _appendBannerTitlePaths(paths) {
  const msg = document.getElementById('newImagesMsg');
  if (!msg || !paths.length) return;
  msg.title = (msg.title ? msg.title + '\n' : '') + paths.join('\n');
}

function _formatCheckedAt(checkedAt) {
  const secs = Number(checkedAt);
  if (!Number.isFinite(secs) || secs <= 0) return '';
  try {
    return new Date(secs * 1000).toLocaleTimeString();
  } catch (e) { return ''; }
}

// A recheck that produced no answer — rejected request, network error,
// unreadable payload — says so and hands the button back, instead of holding
// "Checking..." until some later poll happens to land.
function _failNewImagesRecheck(answersRecheck) {
  if (!answersRecheck || _newImagesRecheckToken === 0) return;
  const stamp = document.getElementById('newImagesCheckedAt');
  if (stamp) stamp.textContent = 'recheck failed \u2014 try again';
  _newImagesRecheckToken = 0;
  _newImagesForcedRerun = false;
  _setNewImagesRecheckBusy(false);
}

// The button stays busy until a final payload renders: a recheck drops the
// cached result, so the poll right after it is usually `pending` and the
// real answer lands a few seconds later.
function _setNewImagesRecheckBusy(busy) {
  const btn = document.getElementById('newImagesRecheck');
  if (!btn) return;
  btn.disabled = !!busy;
  btn.textContent = busy ? 'Checking...' : 'Check again';
}

// Manual recheck. The automatic poll recovers on its own once the offline
// caches expire (30s), but those timers are invisible: someone who just
// remounted the share gets to say "look now" instead of waiting them out.
async function recheckNewImages() {
  const btn = document.getElementById('newImagesRecheck');
  if (btn && btn.disabled) return;
  // Claim the recheck before awaiting anything: a poll that lands while the
  // POST is still open (the endpoint re-reads the mount table, which is
  // bounded but not instant) must not decide the button is idle.
  const token = ++_newImagesRecheckSeq;
  _newImagesRecheckToken = token;
  _setNewImagesRecheckBusy(true);
  let invalidated;
  try {
    const resp = await fetch(
      '/api/workspaces/active/new-images/recheck', {method: 'POST'},
    );
    invalidated = resp.ok;
  } catch (e) { invalidated = false; }
  if (_newImagesRecheckToken !== token) return;
  if (!invalidated) {
    // Nothing was cleared, so polling now would redraw the same cached
    // answer and read as "checked again, still offline". Say the request
    // failed and hand the button back so the click can be retried.
    _failNewImagesRecheck(true);
    return;
  }
  // From here a poll observes post-invalidation state, so it may conclude
  // the recheck.
  _newImagesInvalidatedToken = token;
  await checkNewImages({force: true});
}

function _offlineRootsKey(roots) {
  return roots.slice().sort().join('\n');
}

// Companion to the count key: the exact set of offline roots shown when the
// banner was dismissed (empty string when none). Both must match for the
// dismissal to hold — see _isNewImagesDismissed.
function _newImagesOfflineDismissKey(wsId) {
  return 'newImagesOfflineDismissed_ws_' + String(wsId);
}

function dismissNewImagesBanner() {
  const banner = document.getElementById('newImagesBanner');
  if (!banner) return;
  const wsId = banner.dataset.ws || '';
  const count = Number(banner.dataset.count || 0);
  banner.style.display = 'none';
  if (wsId === '') return;
  // Store the dismissed count (and the offline set it was shown with) under
  // keys scoped to this workspace id so dismissing in workspace B doesn't
  // overwrite A's entry.
  sessionStorage.setItem(_newImagesDismissKey(wsId), String(count));
  sessionStorage.setItem(_newImagesOfflineDismissKey(wsId), banner.dataset.offline || '');
}

async function reviewNewImagesImport(btn) {
  if (btn && btn.disabled) return;
  if (btn) {
    btn.disabled = true;
    btn.textContent = 'Opening...';
  }
  // One POST, then navigate. When the walk that produced the banner count is
  // still cached (the common case), this returns the snapshot instantly and
  // we deep-link to it. If the server is still walking (202) — or anything
  // goes wrong — land on the Import page in its "preparing" state, which
  // owns the polling and shows live walk progress. Never poll here holding
  // the user on a frozen button, and never dump them on a blank wizard with
  // no explanation.
  try {
    const r = await fetch('/api/workspaces/active/new-images/snapshot', {method: 'POST'});
    if (r.ok && r.status !== 202) {
      const data = await r.json();
      if (data && data.snapshot_id != null) {
        window.location.href = `/import?new_images=${data.snapshot_id}`;
        return;
      }
    }
  } catch (e) { /* fall through to the preparing page */ }
  window.location.href = '/import?new_images=preparing';
}

// Run on page load and every 60s while the window is visible. Once the
// server's answer expires (30 minutes), a check re-walks every library
// folder, so a hidden window must not keep asking. Banner dismissal is
// per-workspace via sessionStorage and re-arms automatically on any count
// delta (scan reducing it, or new imports increasing it).
checkNewImages();
Vireo.pollWhileVisible(checkNewImages, 60000);

/* ---------- Duplicate-Cleanup Banner ---------- */
// Surfaces auto-resolved duplicate losers whose files may still be on disk.
// The duplicates page hides them in a collapsed section by default — without
// this banner the user has no way to discover that cleanup is available.
// Library-wide (not workspace-scoped) because photos & file_hash are global.

function _dupCleanupDismissKey() {
  return 'dup_cleanup_dismissed';
}

function _isDupCleanupDismissed(count) {
  var v = sessionStorage.getItem(_dupCleanupDismissKey());
  return v != null && Number(v) === count;
}

let _dupCleanupInFlight = false;
async function checkDupCleanup() {
  if (_dupCleanupInFlight) return;
  _dupCleanupInFlight = true;
  try {
    const resp = await fetch('/api/duplicates/disk-cleanup-summary');
    if (!resp.ok) return;
    const data = await resp.json();
    const banner = document.getElementById('dupCleanupBanner');
    const msg = document.getElementById('dupCleanupMsg');
    if (!banner || !msg) return;

    const count = data.count || 0;
    // Re-arm dismissal when the count drops to zero so a future non-zero
    // count is treated as fresh news (mirrors the new-images banner pattern).
    if (count === 0) {
      sessionStorage.removeItem(_dupCleanupDismissKey());
    }

    if (count > 0 && !_isDupCleanupDismissed(count)) {
      const sizeStr = formatBytesNav(data.total_size || 0);
      msg.textContent = count + ' duplicate file ' + (count === 1 ? 'copy' : 'copies') +
        ' could be cleaned up from disk (estimated ' + sizeStr + ').';
      banner.dataset.count = String(count);
      banner.style.display = 'flex';
    } else {
      banner.style.display = 'none';
    }
  } catch (e) { /* ignore */ }
  finally { _dupCleanupInFlight = false; }
}

function dismissDupCleanupBanner() {
  const banner = document.getElementById('dupCleanupBanner');
  if (!banner) return;
  const count = Number(banner.dataset.count || 0);
  banner.style.display = 'none';
  sessionStorage.setItem(_dupCleanupDismissKey(), String(count));
}

// Local byte formatter — _navbar.html ships before page-specific JS that
// might define one, and a banner can't depend on per-page utilities.
function formatBytesNav(n) {
  if (n == null) return '';
  if (n < 1024) return n + ' B';
  if (n < 1024 * 1024) return (n / 1024).toFixed(1) + ' KB';
  if (n < 1024 * 1024 * 1024) return (n / 1024 / 1024).toFixed(1) + ' MB';
  return (n / 1024 / 1024 / 1024).toFixed(2) + ' GB';
}

checkDupCleanup();
Vireo.pollWhileVisible(checkDupCleanup, 60000);

/* ---------- Missing Folders Modal ---------- */
let _relocatingFolderId = null;
let _relocateSelectedSub = null; // {name, path} of clicked subfolder, or null for current dir
let _relocateBrowserPath = '';
let _pendingCascaded = [];
let _browseRelocateGen = 0;
let _missingFoldersRows = [];
let _missingFoldersRemoving = false;

function openMissingFoldersModal() {
  document.getElementById('missingFoldersModal').classList.add('open');
  loadMissingFolders();
}

function closeMissingFoldersModal() {
  document.getElementById('missingFoldersModal').classList.remove('open');
  checkMissingFolders();
}

async function loadMissingFolders() {
  if (_missingFoldersRemoving) return;
  _missingFoldersRows = [];
  document.getElementById('missingFoldersRemoveAllBtn').style.display = 'none';
  const container = document.getElementById('missingFoldersList');
  container.innerHTML = '<p style="color:var(--text-secondary);">Checking folders…</p>';
  // Own mutation gen (POST-vs-POST supersession) and in-flight counter —
  // the shared observation gen the GET poll uses is deliberately NOT
  // bumped here: a POST that started earlier must not be discarded just
  // because a later GET (e.g. from closeMissingFoldersModal) bumped it
  // (Codex review r3685627312). GET-vs-POST supersession is handled
  // instead via ``_missingFoldersMutationInFlight`` + ``…MutationEpoch``.
  const myMutationGen = ++_missingFoldersMutationGen;
  _missingFoldersMutationInFlight++;
  try {
    const resp = await fetch('/api/folders/check-health', { method: 'POST' });
    if (!resp.ok) throw new Error('check failed');
    const data = await resp.json();
    const folders = data.missing;
    const serverVersion = _parseFolderHealthVersion(
      data.folder_health_version);
    // Folder health is part of the effective photo dataset: ok→missing hides
    // a folder's photos, while missing→ok makes them available again. Notify
    // the current page so a long-lived Browse view does not keep showing the
    // pre-check empty grid after a drive or network share reconnects.
    //
    // Dispatch on the workspace-scoped missing-ID transition rather than
    // ``data.changed``. Two motivations, both from Codex review of 31acc79:
    //   * ``check_folder_health()`` scans every folder in the database and
    //     returns a global change count, so a flip in another workspace's
    //     folder would trigger a needless Browse reset here — clearing the
    //     user's active selection and detail view even though the active
    //     dataset did not change (r3685295409).
    //   * When the modal is used to relocate a missing folder, the
    //     ``/api/folders/<id>/relocate`` endpoint has already flipped the
    //     folder to ``ok`` by the time this POST runs, so ``data.changed``
    //     is zero — but the workspace's missing set did change and Browse
    //     still needs to repopulate (r3685295405).
    // A newer POST superseded this one (rare — modal reopened before this
    // POST finished). Skip *everything* below — snapshot, dispatch,
    // banner, modal rendering. The newer POST is the freshness authority
    // and will paint the correct rows/error when it settles; letting a
    // stale POST run past its guard used to overwrite the newer POST's
    // "Checking folders…" placeholder with pre-mutation rows or a false
    // failure message (Codex review r3686095489).
    //
    // But *client-side* issuance order isn't a freshness guarantee for
    // /api/folders/check-health, which mutates the DB and then reads the
    // post-mutation missing set: if the filesystem changed between the
    // two server-side scans, the discarded response can represent the
    // final committed state while the newer-issued POST's already-
    // dispatched result is now stale. Successful superseded requests
    // scheduled no reconciliation, leaving the snapshot, banner, and
    // Browse grid wrong until the ten-minute poll. Schedule a poll to
    // fire once every in-flight POST has settled — the same wait-for-
    // settle helper the catch path uses — so one GET reconciles with
    // whatever the server actually committed last (Codex review
    // r3686912886).
    if (myMutationGen !== _missingFoldersMutationGen) {
      _scheduleMissingFoldersRecovery();
      return;
    }
    if (_missingFoldersServerVersion !== null && serverVersion !== null &&
        serverVersion < _missingFoldersServerVersion) {
      _scheduleMissingFoldersRecovery();
      return;
    }
    const prevIds = _missingFoldersLastIds;
    const currentIds = folders.map(f => f.id).sort((a, b) => a - b);
    // Keep the poll snapshot in sync with what this POST just observed
    // on the server — closeMissingFoldersModal() and the 10-minute
    // interval both call checkMissingFolders() next, and without this
    // the poll would see (last-poll ≠ post-POST) and fire a second
    // redundant vireo:folder-health-changed for the same transition.
    _missingFoldersLastIds = currentIds;
    if (serverVersion !== null) _missingFoldersServerVersion = serverVersion;
    _missingFoldersSnapshotVersion++;
    const dispatched = _dispatchFolderHealthChangeFromIds(
      prevIds, currentIds, 'check-health');
    // Narrow initial-load race: the user opened this modal before the
    // page-load ``checkMissingFolders()`` finished AND before /api/browse/init
    // seeded the snapshot, so no ID baseline exists yet. Preserve the pre-fix
    // behavior in that window — a status flip in the active workspace still
    // needs to reach Browse — but gate on ``workspace_changed`` (server-side
    // computed pre-vs-post-check diff of THIS workspace's missing set)
    // instead of ``data.changed`` (a global count across all workspaces).
    // Without the workspace-scoped gate this fallback would fire for a
    // cross-workspace flip and blow away the active selection / detail view
    // for a transition that never touched the active dataset
    // (Codex review r3686191131).
    if (!dispatched && prevIds === null && data.workspace_changed) {
      document.dispatchEvent(new CustomEvent('vireo:folder-health-changed', {
        detail: { restored: [], wentMissing: [], source: 'check-health' },
      }));
    }
    // Update the banner directly from this POST's authoritative response
    // rather than tail-calling checkMissingFolders(). See the helper's
    // comment for the race that made the delegation unrecoverable when
    // the modal was closed mid-POST (Codex review r3686095481). Runs for
    // both empty and non-empty results so the banner is right regardless
    // of whether closeMissingFoldersModal's own GET was rejected by our
    // in-flight guard.
    _updateMissingFoldersBanner(folders);
    _missingFoldersRows = folders;
    document.getElementById('missingFoldersRemoveAllBtn').style.display = folders.length ? '' : 'none';
    if (folders.length === 0) {
      container.textContent = '';
      const ok = document.createElement('p');
      ok.style.cssText = 'color:#4caf50;display:flex;align-items:center;gap:8px;font-weight:500;';
      const check = document.createElement('span');
      check.textContent = '✓';
      check.style.cssText = 'font-size:18px;line-height:1;';
      ok.append(check, document.createTextNode('All folders are accounted for.'));
      container.appendChild(ok);
      return;
    }
    // Paths are user-controlled: build rows with DOM methods instead of
    // interpolating into HTML/onclick strings — a quote, backslash, or
    // angle bracket in a folder name broke the inline handlers (and could
    // inject markup). Closures carry the path values safely.
    container.textContent = '';
    folders.forEach(f => {
      const row = document.createElement('div');
      row.className = 'missing-folder-row';
      const pathEl = document.createElement('div');
      pathEl.className = 'missing-folder-path';
      pathEl.title = f.path;
      pathEl.textContent = f.path;
      const countEl = document.createElement('div');
      countEl.className = 'missing-folder-count';
      countEl.textContent = `${f.photo_count} photo${f.photo_count !== 1 ? 's' : ''}`;
      const actions = document.createElement('div');
      actions.className = 'missing-folder-actions';
      const relocateBtn = document.createElement('button');
      relocateBtn.textContent = 'Relocate';
      relocateBtn.addEventListener('click', () => startRelocate(f.id, f.path));
      const removeBtn = document.createElement('button');
      removeBtn.className = 'danger';
      removeBtn.textContent = 'Remove';
      removeBtn.addEventListener('click', () => startRemoveFolder(f.id, f.path, f.photo_count));
      actions.append(relocateBtn, removeBtn);
      row.append(pathEl, countEl, actions);
      container.appendChild(row);
    });
  } catch (e) {
    // Same stale guard as the success path — a superseded POST must not
    // overwrite the newer POST's in-flight "Checking folders…" placeholder
    // with a false failure message (Codex review r3686095489).
    if (myMutationGen === _missingFoldersMutationGen) {
      container.innerHTML = '<p style="color:var(--text-secondary);">Failed to check folders.</p>';
    }
    // When the POST fails (network drop, lost response) the server may have
    // still committed the folder-status flip before the connection died —
    // and page-load / modal-close GETs that started while we were in flight
    // were discarded by our mutation guard. Without a follow-up poll the
    // banner and Browse grid stay stuck on the pre-POST snapshot until the
    // ten-minute interval. Schedule a poll AFTER the finally so it runs
    // once ``_missingFoldersMutationInFlight`` has been decremented (and
    // ``_missingFoldersMutationEpoch`` bumped) — otherwise its own
    // in-flight guard rejects it (Codex review r3686605299).
    //
    // Additionally, wait for *every* overlapping POST to settle before
    // running the poll: if a newer POST failed while an older POST is
    // still pending, decrementing our own counter only takes it from
    // 2→1, checkMissingFolders still sees ``> 0`` and rejects, and the
    // older POST is later discarded by its mutation-gen guard — leaving
    // no request to update the snapshot until the ten-minute interval
    // (Codex review r3686778064).
    _scheduleMissingFoldersRecovery();
  } finally {
    // Decrement + bump epoch in the finally so any GET whose ``await``
    // straddled this POST — start OR end — sees the change and defers
    // (Codex review r3685627312).
    _missingFoldersMutationInFlight--;
    _missingFoldersMutationEpoch++;
  }
}

function startRelocate(folderId, originalPath) {
  _relocatingFolderId = folderId;
  _relocateBrowserPath = '';
  document.getElementById('relocateOriginalPathText').textContent = originalPath || '';
  document.getElementById('relocateBrowserModal').classList.add('open');
  loadRelocateQuickAccess();
  browseRelocate(null);
}

let _quickAccessGen = 0;

async function loadRelocateQuickAccess() {
  const gen = ++_quickAccessGen;
  const container = document.getElementById('relocateQuickAccess');
  container.innerHTML = '';

  function makeQuickBtn(label, path) {
    const btn = document.createElement('button');
    btn.className = 'relocate-quick-btn';
    btn.textContent = label;
    btn.addEventListener('click', function() { browseRelocate(path); });
    return btn;
  }

  container.appendChild(makeQuickBtn('Home', null));
  container.appendChild(makeQuickBtn('/ (Root)', '/'));

  try {
    const data = await safeFetch('/api/browse?path=/Volumes', undefined, {toast: false});
    if (gen !== _quickAccessGen) return;
    if (data && data.dirs && data.dirs.length > 0) {
      data.dirs.forEach(function(d) {
        container.appendChild(makeQuickBtn('\u{1F4BD} ' + d.name, d.path));
      });
    }
  } catch (e) { /* /Volumes may not exist on non-macOS */ }
}

function closeRelocateBrowser() {
  document.getElementById('relocateBrowserModal').classList.remove('open');
  _relocatingFolderId = null;
}

async function browseRelocate(path) {
  _relocateSelectedSub = null;
  updateRelocateBtn();
  const url = path ? `/api/browse?path=${encodeURIComponent(path)}` : '/api/browse';
  const gen = ++_browseRelocateGen;
  const data = await safeFetch(url);
  if (gen !== _browseRelocateGen) return;  // stale response, discard
  _relocateBrowserPath = data.path;

  document.getElementById('relocateBreadcrumb').textContent = data.path;

  const list = document.getElementById('relocateFolderList');
  list.innerHTML = '';
  const normalized = data.path.replace(/\\/g, '/').replace(/\/$/, '');
  const cut = normalized.lastIndexOf('/');
  let parent = cut >= 0 ? normalized.substring(0, cut) : '/';
  if (/^[A-Za-z]:$/.test(parent)) parent += '/';
  if (!parent) parent = '/';
  if (data.path !== '/') {
    const parentEl = document.createElement('div');
    parentEl.className = 'relocate-folder-item';
    parentEl.textContent = '\u{1F4C2} ..';
    parentEl.addEventListener('dblclick', function() { browseRelocate(parent); });
    list.appendChild(parentEl);
  }
  data.dirs.forEach(function(d) {
    const el = document.createElement('div');
    el.className = 'relocate-folder-item';
    el.textContent = '\u{1F4C2} ' + d.name;
    el.addEventListener('click', function() { selectRelocateFolder(el, d.name, d.path); });
    el.addEventListener('dblclick', function() { browseRelocate(d.path); });
    list.appendChild(el);
  });
}

function selectRelocateFolder(el, name, path) {
  document.querySelectorAll('#relocateFolderList .relocate-folder-item.selected').forEach(function(item) {
    item.classList.remove('selected');
  });
  el.classList.add('selected');
  _relocateSelectedSub = {name: name, path: path};
  updateRelocateBtn();
}

function updateRelocateBtn() {
  const btn = document.getElementById('relocateSelectBtn');
  if (_relocateSelectedSub) {
    btn.textContent = 'Select "' + _relocateSelectedSub.name + '"';
  } else {
    btn.textContent = 'Select This Folder';
  }
}

async function confirmRelocate() {
  if (!_relocatingFolderId || !_relocateBrowserPath) return;
  const data = await safeFetch(`/api/folders/${_relocatingFolderId}/relocate`, {
    method: 'POST',
    headers: {'Content-Type': 'application/json'},
    body: JSON.stringify({path: _relocateSelectedSub ? _relocateSelectedSub.path : _relocateBrowserPath}),
  });
  closeRelocateBrowser();

  if (data.cascaded && data.cascaded.length > 0) {
    _pendingCascaded = data.cascaded;
    document.getElementById('cascadeMsg').textContent =
      `${data.cascaded.length} subfolder${data.cascaded.length !== 1 ? 's were' : ' was'} also found at the new location and re-linked.`;
    document.getElementById('cascadeConfirmModal').classList.add('open');
  } else {
    loadMissingFolders();
  }
}

function closeCascadeConfirm() {
  document.getElementById('cascadeConfirmModal').classList.remove('open');
  _pendingCascaded = [];
  loadMissingFolders();
}
function confirmCascade() { closeCascadeConfirm(); }

async function startRemoveFolder(folderId, path, photoCount) {
  if (_missingFoldersRemoving) return;
  if (!confirm(`This will remove ${photoCount} photo${photoCount !== 1 ? 's' : ''} from Vireo.\n\nThe files on disk (if they still exist) won't be touched.\n\nRemove "${path}"?`)) return;
  await safeFetch(`/api/folders/${folderId}`, {method: 'DELETE'});
  loadMissingFolders();
}

async function removeAllMissingFolders() {
  if (_missingFoldersRemoving || !_missingFoldersRows.length) return;
  // Remove children first: deleting a parent also removes its descendants.
  const folders = [..._missingFoldersRows].sort((a, b) => b.path.length - a.path.length);
  const photoCount = folders.reduce((total, folder) => total + folder.photo_count, 0);
  if (!confirm(`Remove all ${folders.length} missing folder${folders.length !== 1 ? 's' : ''} from Vireo?\n\nThese folders contain ${photoCount} photo${photoCount !== 1 ? 's' : ''}. Their subfolders will also be removed from Vireo.\n\nThe files on disk (if they still exist) won't be touched.`)) return;

  _missingFoldersRemoving = true;
  const button = document.getElementById('missingFoldersRemoveAllBtn');
  const controls = document.querySelectorAll('#missingFoldersModal button');
  controls.forEach(control => { control.disabled = true; });
  let removed = 0;
  try {
    for (const folder of folders) {
      button.textContent = `Removing ${removed + 1} of ${folders.length}…`;
      await safeFetch(`/api/folders/${folder.id}`, {method: 'DELETE'});
      removed++;
    }
    showToast(`Removed ${removed} missing folder${removed !== 1 ? 's' : ''} from Vireo.`, 'success');
  } catch (e) {
    // Stop before an ancestor can cascade over a descendant that failed.
    showToast(`Removal stopped after ${removed} of ${folders.length} folders. ${e.message || 'Please try again.'}`, 'error');
  } finally {
    _missingFoldersRemoving = false;
    button.textContent = 'Remove All';
    controls.forEach(control => { control.disabled = false; });
    await loadMissingFolders();
  }
}

/* ---------- Missing Originals Modal ---------- */
let _missingPhotosSelected = new Set();
let _missingPhotosDeleteInFlight = false;
let _missingPhotosRemovalRefreshResult = null;

// Scope for the Missing Originals modal. null folderId = whole workspace.
// The banner poll (checkMissingPhotos) is always workspace-wide; only the
// modal honors scope so a "rescan this folder" review can't offer to delete
// ghosts from folders the user didn't ask about.
let _missingPhotosScope = { folderId: null, label: '' };

function openMissingPhotosModal(folderId, label) {
  _missingPhotosScope = {
    folderId: (folderId == null ? null : folderId),
    label: label || '',
  };
  updateMissingPhotosThumbSize(
    document.getElementById('missingPhotosThumbSizeSlider').value
  );
  const note = document.getElementById('missingPhotosScopeNote');
  if (_missingPhotosScope.folderId != null) {
    note.textContent = 'Showing only: ' + (_missingPhotosScope.label || 'selected folder');
    note.style.display = 'block';
  } else {
    note.style.display = 'none';
    note.textContent = '';
  }
  document.getElementById('missingPhotosDeleteSidecars').checked = true;
  if (!_missingPhotosRemovalRefreshResult) _setMissingPhotosActionStatus('');
  document.getElementById('missingPhotosModal').classList.add('open');
  loadMissingPhotos();
}

function closeMissingPhotosModal() {
  document.getElementById('missingPhotosModal').classList.remove('open');
  checkMissingPhotos();  // refresh banner
}

/* ---------- Rescan Folders ---------- */
// Opens the Rescan modal. Scope defaults to the whole workspace; the folder
// dropdown is populated from the active workspace's root folders.
async function openRescanModal() {
  // Close the workspace dropdown if this was triggered from its menu.
  vireoWorkspaceSwitcher.close();
  const wsRadio = document.querySelector('input[name="rescanScope"][value="workspace"]');
  const folderRadio = document.querySelector('input[name="rescanScope"][value="folder"]');
  if (wsRadio) { wsRadio.checked = true; wsRadio.disabled = false; }
  if (folderRadio) folderRadio.disabled = false;
  const sel = document.getElementById('rescanFolderSelect');
  sel.innerHTML = '';
  sel.disabled = true;
  const emptyNote = document.getElementById('rescanEmptyNote');
  emptyNote.style.display = 'none';
  document.getElementById('rescanRunBtn').disabled = false;
  document.getElementById('rescanModal').classList.add('open');
  try {
    const active = await safeFetch('/api/workspaces/active', {}, { toast: false });
    const folders = await safeFetch('/api/workspaces/' + active.id + '/folders', {}, { toast: false });
    if (!folders || folders.length === 0) {
      emptyNote.textContent = 'This workspace has no folders yet. Open Import to add photos first.';
      emptyNote.style.display = 'block';
      if (folderRadio) folderRadio.disabled = true;
      return;
    }
    folders.forEach(function(f) {
      const opt = document.createElement('option');
      opt.value = f.id;
      opt.textContent = f.name || f.path;
      opt.title = f.path;
      sel.appendChild(opt);
    });
  } catch (e) {
    emptyNote.textContent = 'Could not load this workspace’s folders.';
    emptyNote.style.display = 'block';
    if (folderRadio) folderRadio.disabled = true;
  }
}
window.openRescanModal = openRescanModal;

function onRescanScopeChange() {
  const scopeEl = document.querySelector('input[name="rescanScope"]:checked');
  const isFolder = !!(scopeEl && scopeEl.value === 'folder');
  document.getElementById('rescanFolderSelect').disabled = !isFolder;
}

function closeRescanModal() {
  document.getElementById('rescanModal').classList.remove('open');
}

async function runRescan() {
  const scopeEl = document.querySelector('input[name="rescanScope"]:checked');
  const scope = scopeEl ? scopeEl.value : 'workspace';
  const btn = document.getElementById('rescanRunBtn');
  btn.disabled = true;
  try {
    if (scope === 'folder') {
      const sel = document.getElementById('rescanFolderSelect');
      const fid = parseInt(sel.value, 10);
      if (isNaN(fid)) {
        showToast('Pick a folder to rescan.', 'error');
        btn.disabled = false;
        return;
      }
      const label = sel.options[sel.selectedIndex]
        ? sel.options[sel.selectedIndex].textContent : '';
      const scanRes = await safeFetch('/api/folders/' + fid + '/rescan', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ incremental: true }),
      });
      closeRescanModal();
      showToast('Rescanning “' + label + '” for new and changed photos…', 'info');
      await reviewDeletedAfterRescan(fid, label, scanRes && scanRes.job_id);
    } else {
      const res = await safeFetch('/api/jobs/scan-workspace', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ incremental: true }),
      });
      closeRescanModal();
      let msg = 'Rescanning the workspace for new and changed photos…';
      if (res && res.skipped && res.skipped.length) {
        const n = res.skipped.length;
        msg += ' (' + n + ' offline folder' + (n === 1 ? '' : 's') + ' skipped)';
      }
      showToast(msg, 'info');
      await reviewDeletedAfterRescan(null, '', res && res.job_id);
    }
  } catch (e) {
    // safeFetch already surfaced the error; leave the modal open to retry.
    btn.disabled = false;
    return;
  }
  btn.disabled = false;
}

// Poll a job until it reaches a terminal state (completed / failed /
// cancelled). Used by the rescan flow so the follow-up missing-originals
// check doesn't race the scan itself: the scan job invalidates the
// missing-originals cache in its per-root ``finally``, which bumps the
// in-flight generation — a missing-originals scan started too early
// would have its results discarded and the modal would poll to
// ``not_ready`` instead of showing the just-computed ghosts.
//
// Returns true when the job reached ``completed``, false for any other
// terminal state (or if the status endpoint stops returning the job).
// Callers treat "unknown" the same as "done" so a lost job id doesn't
// hang the review modal indefinitely.
async function _awaitScanJobDone(jobId, opts) {
  opts = opts || {};
  const intervalMs = opts.intervalMs || 1500;
  // Hard cap so a stuck job can never wedge the follow-up modal open.
  // At the default 1.5s cadence this is ~10 minutes, which covers even
  // large NAS/SMB rescans; the scan itself keeps running past the cap.
  const maxAttempts = opts.maxAttempts || 400;
  for (let i = 0; i < maxAttempts; i++) {
    let job;
    try {
      const resp = await fetch('/api/jobs/' + encodeURIComponent(jobId));
      if (resp.status === 404) return false;
      if (!resp.ok) return false;
      job = await resp.json();
    } catch (e) {
      return false;
    }
    const status = job && job.status;
    if (status === 'completed') return true;
    if (status === 'failed' || status === 'cancelled') return false;
    await new Promise(function(r) { setTimeout(r, intervalMs); });
  }
  return false;
}

// The scan job (incremental) only adds/updates — it never removes rows for
// files deleted on disk. So after it finishes, check for missing originals
// in the same scope and open the review modal if any exist. We wait for
// the scan job to reach a terminal state first because the scan's own
// cache invalidation would otherwise bump the missing-originals scan's
// in-flight generation mid-flight and discard the results (the modal
// would then poll to ``not_ready``/"not checked yet" even though a full
// filesystem walk had just completed).
async function reviewDeletedAfterRescan(folderId, label, scanJobId) {
  if (scanJobId) {
    const scanDone = await _awaitScanJobDone(scanJobId);
    if (!scanDone) {
      // ``_awaitScanJobDone`` returns false for terminal failures/
      // cancels, transient ``/api/jobs/<id>`` fetch failures, and the
      // hard polling cap (~10 min) — meaning the rescan job may still
      // be running. Starting the missing-originals scan now would race
      // the scan's per-root ``finally`` cache invalidation, which bumps
      // the missing-scan in-flight generation and discards the freshly
      // computed result — the modal would then poll to ``not_ready``
      // even though a full filesystem walk had just completed. Defer
      // with a toast; the banner's next automatic tick will surface any
      // deletions once the scan actually finishes.
      showToast(
        'Rescan still finishing' + (label ? ' in “' + label + '”' : '')
          + ' — deleted-photo review will run once it completes.',
        'info',
      );
      return;
    }
  }
  try {
    const payload = await startMissingPhotosCheck({
      folderId: folderId,
      automatic: false,
      modal: true,
    });
    const photos = _missingPhotosPayloadPhotos(payload);
    if ((payload && payload.pending) || (photos && photos.length)) {
      openMissingPhotosModal(folderId, label);
    } else {
      showToast('No deleted photos found' + (label ? ' in “' + label + '”' : '') + '.', 'info');
    }
  } catch (e) {
    // Non-fatal: the background scan still runs; the banner will catch
    // deletions on its next poll.
  }
}

function _escapeAttr(s) {
  return String(s == null ? '' : s).replace(/&/g, '&amp;').replace(/"/g, '&quot;').replace(/</g, '&lt;').replace(/>/g, '&gt;');
}
function _escapeHtml(s) { return _escapeAttr(s); }

function updateMissingPhotosThumbSize(value) {
  const list = document.getElementById('missingPhotosList');
  if (list) list.style.setProperty('--missing-thumb-size', value + 'px');
}

async function loadMissingPhotos() {
  const container = document.getElementById('missingPhotosList');
  const toolbar = document.getElementById('missingPhotosToolbar');
  container.innerHTML = '<p style="color:var(--text-secondary);">Checking photos…</p>';
  toolbar.style.display = 'none';
  _missingPhotosSelected = new Set();
  const cacheId = ++_missingPhotosCacheFetchId;
  const myId = ++_missingPhotosModalFetchId;
  try {
    const scoped = _missingPhotosScope.folderId != null;
    const url = _missingPhotosStatusUrl(_missingPhotosScope.folderId);
    const resp = await fetch(url);
    if (!resp.ok) throw new Error('check failed');
    if (myId !== _missingPhotosModalFetchId) return;  // superseded by a newer modal load
    const payload = await resp.json();
    if (myId !== _missingPhotosModalFetchId) return;
    const photos = _missingPhotosPayloadPhotos(payload);
    const ready = Array.isArray(payload) || (payload && payload.status === 'ready');
    if (payload && payload.pending) {
      container.innerHTML = '<p style="color:var(--text-secondary);">Checking missing originals…</p>';
      _scheduleMissingPhotosPoll(_missingPhotosScope.folderId, true);
      return 'pending';
    }
    if (!ready) {
      const err = payload && payload.error ? _escapeHtml(payload.error) : '';
      const msg = payload && payload.status === 'error'
        ? 'Could not check missing originals' + (err ? ': ' + err : '') + '.'
        : 'Missing originals have not been checked yet.';
      container.innerHTML =
        '<p style="color:var(--text-secondary);">' + msg + '</p>' +
        '<button class="modal-btn primary" onclick="refreshMissingPhotosNow()">Check now</button>';
      _finalizeMissingPhotosRemovalRefresh('failed');
      return 'failed';
    }
    const finalizedRemoval = _finalizeMissingPhotosRemovalRefresh('ready');
    if (!finalizedRemoval && !_missingPhotosDeleteInFlight) {
      _setMissingPhotosActionStatus('');
    }
    // A folder-scoped load is a subset — don't overwrite the workspace-wide
    // banner cache with it, or the banner's dismissed-count math goes stale.
    if (!scoped && cacheId === _missingPhotosCacheFetchId) _missingPhotosCache = photos;
    // Sidecar cleanup consults this map (see _deleteMissingPhotoIds) so a
    // freshly discovered ghost in a scoped load still gets its .xmp removed.
    _missingPhotosModalRows = new Map(photos.map(p => [p.id, p]));
    if (photos.length === 0) {
      container.innerHTML = '<p style="color:var(--text-secondary);">No photos with missing originals.</p>';
      checkMissingPhotos();
      return 'ready';
    }
    toolbar.style.display = 'flex';
    document.getElementById('missingPhotosSelectAll').checked = false;
    container.innerHTML = photos.map(p => {
      const ts = p.timestamp ? new Date(p.timestamp).toLocaleDateString() : '';
      const thumbUrl = window.vireoThumbnailUrl
        ? window.vireoThumbnailUrl(p)
        : `/thumbnails/${p.id}.jpg`;
      const thumb = p.has_thumb
        ? `<img src="${_escapeAttr(thumbUrl)}" alt="" loading="lazy">`
        : '<span>no thumb</span>';
      const badges = [
        ['thumb', p.has_thumb],
        ['preview', p.has_preview],
        ['working copy', p.has_working_copy],
        ['XMP', p.has_xmp_sidecar],
      ].map(([label, on]) =>
        `<span class="missing-photo-badge${on ? ' has' : ''}">${on ? '✓ ' : ''}${label}</span>`
      ).join('');
      return `
        <div class="missing-photo-row" data-photo-id="${p.id}">
          <input type="checkbox" onchange="toggleMissingPhoto(${p.id}, this.checked)">
          <div class="missing-photo-thumb">${thumb}</div>
          <div class="missing-photo-info">
            <div class="missing-photo-filename" title="${_escapeAttr(p.filename)}">${_escapeHtml(p.filename)}${ts ? ` <span style="font-weight:400;color:var(--text-secondary);">· ${ts}</span>` : ''}</div>
            <div class="missing-photo-path" title="${_escapeAttr(p.folder_path)}">${_escapeHtml(p.folder_path)}</div>
            <div class="missing-photo-badges">${badges}</div>
          </div>
          <div class="missing-folder-actions">
            <button class="danger" onclick="removeOneMissingPhoto(${p.id})">Remove</button>
          </div>
        </div>
      `;
    }).join('');
    _updateMissingPhotosBulkBtn();
    return 'ready';
  } catch (e) {
    // Same staleness rule as the success path: a failure from a superseded
    // load must not replace the newer load's list with an error.
    if (myId !== _missingPhotosModalFetchId) return;
    container.innerHTML = '<p style="color:var(--text-secondary);">Failed to check photos.</p>';
    _finalizeMissingPhotosRemovalRefresh('failed');
    return 'failed';
  }
}

async function refreshMissingPhotosNow() {
  const container = document.getElementById('missingPhotosList');
  const toolbar = document.getElementById('missingPhotosToolbar');
  if (toolbar) toolbar.style.display = 'none';
  if (container) container.innerHTML = '<p style="color:var(--text-secondary);">Starting missing originals check…</p>';
  try {
    await startMissingPhotosCheck({
      folderId: _missingPhotosScope.folderId,
      automatic: false,
      modal: true,
    });
  } catch (e) {
    if (container) container.innerHTML = '<p style="color:var(--text-secondary);">Failed to start check.</p>';
    return 'failed';
  }
  return await loadMissingPhotos();
}

function toggleMissingPhoto(id, checked) {
  if (checked) _missingPhotosSelected.add(id);
  else _missingPhotosSelected.delete(id);
  _updateMissingPhotosBulkBtn();
}

function toggleAllMissingPhotos(checked) {
  _missingPhotosSelected = new Set();
  document.querySelectorAll('#missingPhotosList .missing-photo-row').forEach(function(row) {
    const cb = row.querySelector('input[type="checkbox"]');
    cb.checked = checked;
    if (checked) _missingPhotosSelected.add(parseInt(row.dataset.photoId, 10));
  });
  _updateMissingPhotosBulkBtn();
}

function _updateMissingPhotosBulkBtn() {
  const n = _missingPhotosSelected.size;
  document.getElementById('missingPhotosSelectedCount').textContent =
    `${n} selected`;
  document.getElementById('missingPhotosRemoveBtn').disabled =
    _missingPhotosDeleteInFlight || n === 0;
}

function _setMissingPhotosActionStatus(message, state) {
  const status = document.getElementById('missingPhotosActionStatus');
  if (!status) return;
  status.textContent = message || '';
  status.hidden = !message;
  status.classList.toggle('error', state === 'error');
  status.setAttribute('role', state === 'error' ? 'alert' : 'status');
  status.setAttribute('aria-live', state === 'error' ? 'assertive' : 'polite');
}

function _finalizeMissingPhotosRemovalRefresh(refreshState) {
  const result = _missingPhotosRemovalRefreshResult;
  if (!result || refreshState === 'pending') return false;
  _missingPhotosRemovalRefreshResult = null;
  if (refreshState === 'ready') {
    _setMissingPhotosActionStatus(
      result.deleted > 0
        ? `Removed ${result.deleted} ${result.noun}.`
        : 'No photos were removed.'
    );
  } else {
    _setMissingPhotosActionStatus(
      result.deleted > 0
        ? `Removed ${result.deleted} ${result.noun}, but could not refresh missing originals.`
        : 'No photos were removed, and missing originals could not be refreshed.',
      'error',
    );
  }
  return true;
}

function _setMissingPhotosDeleteBusy(busy, count) {
  _missingPhotosDeleteInFlight = busy;
  const button = document.getElementById('missingPhotosRemoveBtn');
  const refreshButton = document.getElementById('missingPhotosRefreshBtn');
  if (button) {
    button.textContent = busy
      ? `Removing ${count} photo${count === 1 ? '' : 's'}…`
      : 'Remove selected';
  }
  if (refreshButton) refreshButton.disabled = busy;
  document.querySelectorAll(
    '#missingPhotosToolbar input, #missingPhotosList input, #missingPhotosList button'
  ).forEach(function(control) {
    control.disabled = busy;
  });
  _updateMissingPhotosBulkBtn();
}

async function removeOneMissingPhoto(id) {
  if (_missingPhotosDeleteInFlight) return;
  const deleteSidecars = document.getElementById('missingPhotosDeleteSidecars').checked;
  const sidecarMsg = deleteSidecars
    ? '\n\nLeftover XMP sidecars will also be deleted from disk.'
    : '\n\nThe XMP sidecar on disk will not be touched.';
  if (!confirm(`Remove this photo from Vireo?\n\nCached thumbnail/preview/working copy (if any) will also be deleted.${sidecarMsg}`)) return;
  await _runMissingPhotosRemoval([id], deleteSidecars);
}

async function removeSelectedMissingPhotos() {
  if (_missingPhotosDeleteInFlight) return;
  const ids = Array.from(_missingPhotosSelected);
  if (ids.length === 0) return;
  const deleteSidecars = document.getElementById('missingPhotosDeleteSidecars').checked;
  const sidecarMsg = deleteSidecars ? '\n\nLeftover XMP sidecars will also be deleted from disk.' : '';
  if (!confirm(`Remove ${ids.length} photo${ids.length !== 1 ? 's' : ''} from Vireo?\n\nCached thumbnails/previews/working copies will be deleted.${sidecarMsg}`)) return;
  await _runMissingPhotosRemoval(ids, deleteSidecars);
}

async function _runMissingPhotosRemoval(ids, deleteSidecars) {
  if (_missingPhotosDeleteInFlight || ids.length === 0) return;
  _missingPhotosRemovalRefreshResult = null;
  _setMissingPhotosDeleteBusy(true, ids.length);
  _setMissingPhotosActionStatus(
    `Removing ${ids.length} photo${ids.length === 1 ? '' : 's'}…`
  );
  try {
    let payload;
    try {
      payload = await _deleteMissingPhotoIds(ids, deleteSidecars);
    } catch (err) {
      // safeFetch already surfaces the specific server/network error as a
      // toast. Keep a persistent failure state in the modal too, since the
      // toast fades.
      _setMissingPhotosActionStatus(
        'Could not remove the selected photos. Please try again.',
        'error',
      );
      return;
    }
    const deleted = Number(payload && payload.deleted) || 0;
    const noun = deleted === 1 ? 'photo' : 'photos';
    _missingPhotosRemovalRefreshResult = {deleted: deleted, noun: noun};
    if (deleted > 0) {
      showToast(`Removed ${deleted} ${noun} from Vireo.`, 'success');
      _setMissingPhotosActionStatus(
        `Removed ${deleted} ${noun}. Rechecking missing originals…`
      );
    } else {
      _setMissingPhotosActionStatus(
        'No photos were removed. Rechecking missing originals…'
      );
    }
    let refreshState = 'failed';
    try {
      refreshState = await refreshMissingPhotosNow();
    } catch (err) {
      // Keep refresh failures separate from the deletion result. This also
      // protects callers/tests that replace refreshMissingPhotosNow with a
      // rejecting implementation even though the production helper normally
      // reports failure via its return value.
    }
    if (refreshState !== 'pending') {
      _finalizeMissingPhotosRemovalRefresh(
        refreshState === 'ready' ? 'ready' : 'failed'
      );
    }
  } finally {
    _setMissingPhotosDeleteBusy(false, 0);
  }
}

async function _deleteMissingPhotoIds(ids, deleteSidecars) {
  // Ready /api/photos/missing payloads are cached for up to 30 min, so a
  // photo whose original came back after the last scan could otherwise be
  // deleted from Vireo just by trusting the cache. The dedicated
  // missing/remove endpoint re-checks each source on disk in the active
  // workspace, skips any whose original is back, and only deletes the
  // still-missing rows (plus their sidecars in the same transaction).
  const payload = await safeFetch('/api/photos/missing/remove', {
    method: 'POST',
    headers: {'Content-Type': 'application/json'},
    body: JSON.stringify({
      photo_ids: ids,
      mode: 'vireo',
      delete_sidecars: !!deleteSidecars,
    }),
  });
  const restored = (payload && Array.isArray(payload.restored))
    ? payload.restored : [];
  if (restored.length > 0) {
    const noun = restored.length === 1 ? 'photo' : 'photos';
    showToast(
      `Skipped ${restored.length} ${noun} whose original is back on disk.`,
      'info',
    );
  }
  // The cache can't tell "original truly missing" from "folder unreachable"
  // — if a NAS/SMB mount is currently offline every ready-cache row would
  // look absent, so the endpoint defers those IDs. Surface that so users
  // know why the delete didn't happen and can retry once the volume returns.
  const folderOffline = (payload && Array.isArray(payload.folder_offline))
    ? payload.folder_offline : [];
  if (folderOffline.length > 0) {
    const noun = folderOffline.length === 1 ? 'photo' : 'photos';
    showToast(
      `Skipped ${folderOffline.length} ${noun} whose folder is currently offline.`,
      'warning',
    );
  }
  const skipped = Number(payload && payload.skipped) || 0;
  if (skipped > 0) {
    const noun = skipped === 1 ? 'photo' : 'photos';
    showToast(
      `Skipped ${skipped} ${noun} no longer available in this workspace.`,
      'warning',
    );
  }
  // The server just invalidated the Missing Originals cache, but callers
  // only bump the modal/cache generation counters (via refreshMissingPhotosNow
  // → loadMissingPhotos). A banner /api/photos/missing GET that was already
  // in flight before this delete would still pass its own banner-generation
  // check and repaint the pre-delete count. Route through checkMissingPhotos
  // so the in-flight branch bumps _missingPhotosBannerFetchId and schedules
  // a follow-up poll, and an idle banner refetches fresh state.
  try { checkMissingPhotos(); } catch (err) { /* best-effort banner refresh */ }
  // Best-effort: refreshing the Misses page is an auxiliary UI sync. A
  // failure here must not reject the delete flow or block the caller's
  // own loadMissingPhotos() refresh.
  if (typeof loadMisses === 'function') {
    try {
      await loadMisses();
    } catch (err) {
      console.error('Failed to refresh Misses after delete:', err);
    }
  }
  return payload;
}
