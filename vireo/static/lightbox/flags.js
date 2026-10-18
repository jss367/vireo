import { lbState } from './shared-state.js';
import {
  _lbConfirmedFlags, _lbFlagPendingByPhoto, _lbProvisionalFlagSeq, _lbProvisionalFlags,
  vireoLightboxSession,
} from './state.js';
import { safeFetch } from '../navbar-shared-helpers.js';

/** @returns {LightboxFlag | null} */ export function _lbNormalizeFlag(/** @type {unknown} */ flag) {
  if (flag === 'flagged' || flag === 'rejected' || flag === 'none') return flag;
  if (flag == null || flag === '') return 'none';
  return null;
}

export function _lbSetFlagStatus(/** @type {unknown} */ flag) {
  var normalized = typeof flag === 'undefined' ? null : _lbNormalizeFlag(flag);
  var flagBtn = document.getElementById('lightboxFlagBtn');
  var rejectBtn = document.getElementById('lightboxRejectBtn');
  if (flagBtn) flagBtn.setAttribute('aria-pressed', normalized === 'flagged' ? 'true' : 'false');
  if (rejectBtn) rejectBtn.setAttribute('aria-pressed', normalized === 'rejected' ? 'true' : 'false');
  var el = document.getElementById('lightboxFlagStatus');
  if (!el) return;
  el.className = 'lightbox-flag-status';
  if (!normalized) {
    el.textContent = '';
    return;
  }
  if (normalized === 'flagged') {
    el.textContent = 'Flagged';
    el.classList.add('visible', 'flagged');
  } else if (normalized === 'rejected') {
    el.textContent = 'Rejected';
    el.classList.add('visible', 'rejected');
  } else {
    el.textContent = 'No flag';
    el.classList.add('visible');
  }
}

function _lbPendingFlagLabel(/** @type {LightboxFlag} */ flag) {
  if (flag === 'flagged') return 'Flagging...';
  if (flag === 'rejected') return 'Rejecting...';
  if (flag === 'none') return 'Clearing flag...';
  return 'Saving...';
}

function _lbSetPendingFlagStatus(/** @type {LightboxFlag} */ flag) {
  var el = document.getElementById('lightboxFlagStatus');
  if (!el) return;
  el.className = 'lightbox-flag-status visible pending';
  el.textContent = _lbPendingFlagLabel(flag);
}

function _lbFlagFromPhotoList(/** @type {number | null} */ photoId) {
  var p = lbState.photoList.find(function(x) { return x.id === photoId; });
  if (!p || !Object.prototype.hasOwnProperty.call(p, 'flag')) return undefined;
  return p.flag;
}

export function _lbRememberConfirmedFlag(/** @type {number | null} */ photoId, /** @type {unknown} */ flag) {
  const normalized = _lbNormalizeFlag(flag);
  if (!normalized || photoId == null) return;
  _lbConfirmedFlags[String(photoId)] = normalized;
}

export function _lbForgetConfirmedFlag(/** @type {number | null} */ photoId) {
  if (photoId == null) return;
  delete _lbConfirmedFlags[String(photoId)];
}

export function _lbConfirmedFlagFor(/** @type {number | null} */ photoId) {
  var key = String(photoId);
  if (Object.prototype.hasOwnProperty.call(_lbConfirmedFlags, key)) {
    return _lbConfirmedFlags[key];
  }
  return _lbFlagFromPhotoList(photoId);
}

export function _lbDisplayedFlagFor(/** @type {number | null} */ photoId) {
  var key = String(photoId);
  if (Object.prototype.hasOwnProperty.call(_lbProvisionalFlags, key)) {
    return _lbProvisionalFlags[key];
  }
  return _lbConfirmedFlagFor(photoId);
}

/** @type {LightboxApi['setLightboxProvisionalFlag']} */ export function setLightboxProvisionalFlag(photoId, flag, editSeq) {
  const normalized = _lbNormalizeFlag(flag);
  if (photoId == null || !normalized) return;
  var key = String(photoId);
  var seq = typeof editSeq === 'number' && Number.isInteger(editSeq) ? editSeq : (lbState.flagEditSeq + 1);
  if (seq < (_lbProvisionalFlagSeq[key] || 0)) return;
  lbState.flagEditSeq = Math.max(lbState.flagEditSeq, seq);
  _lbProvisionalFlags[key] = normalized;
  _lbProvisionalFlagSeq[key] = seq;
  if (vireoLightboxSession.requestedPhotoId() === Number(photoId)) {
    _lbSetFlagStatus(normalized);
  }
}

// A page that stages flag edits can clear its lightbox-only provisional
// display when the staging session ends. Forget the confirmed memo too so
// the page's now-authoritative photo list (persisted on Apply, unchanged on
// discard) supplies the next visible value.
/** @type {LightboxApi['clearLightboxProvisionalFlags']} */ export function clearLightboxProvisionalFlags(photoIds) {
  (photoIds || []).forEach(function(photoId) {
    delete _lbProvisionalFlags[String(photoId)];
    delete _lbProvisionalFlagSeq[String(photoId)];
    _lbForgetConfirmedFlag(photoId);
  });
  if (vireoLightboxSession.requestedPhotoId() != null && (photoIds || []).some(function(photoId) {
    return Number(photoId) === vireoLightboxSession.requestedPhotoId();
  })) {
    _lbSetFlagStatus(_lbDisplayedFlagFor(vireoLightboxSession.requestedPhotoId()));
  }
}

export function _lbRecordFlag(/** @type {number | null} */ photoId, /** @type {unknown} */ flag) {
  const normalized = _lbNormalizeFlag(flag);
  if (!normalized) return;
  var p = lbState.photoList.find(function(x) { return x.id === photoId; });
  if (p) p.flag = normalized;
  _lbRememberConfirmedFlag(photoId, normalized);
  if (vireoLightboxSession.requestedPhotoId() === photoId && !lbState.visualTransitionPending) {
    _lbSetFlagStatus(_lbDisplayedFlagFor(photoId));
  }
}

function _lbCacheFlag(/** @type {number} */ photoId, /** @type {unknown} */ flag) {
  const normalized = _lbNormalizeFlag(flag);
  if (!normalized) return;
  var p = lbState.photoList.find(function(x) { return x.id === photoId; });
  if (p) p.flag = normalized;
  _lbRememberConfirmedFlag(photoId, normalized);
}

export function _lbRecordFetchedFlag(/** @type {number} */ photoId, /** @type {unknown} */ flag, /** @type {number} */ flagFetchSeq) {
  if (lbState.flagEditSeq !== flagFetchSeq) return;
  _lbRecordFlag(photoId, flag);
}

export function _lbApplyFlag(/** @type {number | null} */ photoId, /** @type {unknown} */ flag) {
  const normalized = _lbNormalizeFlag(flag);
  if (!normalized || photoId == null) return false;
  var previousFlag = _lbNormalizeFlag(_lbConfirmedFlagFor(photoId)) || 'none';
  var seq = lbState.flagEditSeq + 1;
  lbState.flagEditSeq = seq;
  lbState.flagPendingWrites += 1;
  _lbFlagPendingByPhoto[photoId] = (_lbFlagPendingByPhoto[photoId] || 0) + 1;
  _lbSetPendingFlagStatus(normalized);
  var write;

  var setFlagFor = Vireo.pageHook('setFlagFor') || Vireo.pageHook('setReviewFlag');
  if (setFlagFor) {
    write = /** @type {Promise<boolean | void | {provisional: boolean}>} */ (setFlagFor(photoId, normalized));
  } else if (typeof safeFetch === 'function') {
    write = safeFetch('/api/photos/' + photoId + '/flag', {
      method: 'POST', headers: {'Content-Type': 'application/json'},
      body: JSON.stringify({flag: normalized}),
    }, { toast: false }).then(function() { return true; }).catch(function() { return false; });
  } else {
    _lbSetFlagStatus(_lbConfirmedFlagFor(photoId));
    return false;
  }

  // Resolve both success and rejection through one settle path so the per-photo
  // pending count is always balanced and the quiescence event always fires.
  function settle(/** @type {boolean | void | {provisional: boolean}} */ result) { if (photoId == null || !normalized) return;
    // Page-local flag helpers may deliberately keep a change provisional
    // (for example, Process Review's Group Review stages picks/rejects until
    // Apply). Show that choice in the open lightbox, but do not promote it to
    // the confirmed cache, mutate the shared photo list, or emit the
    // persisted-change event. Plain true/undefined remain successful writes;
    // false remains a failed write for backward compatibility.
    var provisional = !!(
      result && typeof result === 'object' && result.provisional === true
    );
    var landed = result !== false && !provisional;
    lbState.flagPendingWrites = Math.max(0, lbState.flagPendingWrites - 1);
    var remaining = (_lbFlagPendingByPhoto[photoId] || 1) - 1;
    if (remaining > 0) _lbFlagPendingByPhoto[photoId] = remaining;
    else delete _lbFlagPendingByPhoto[photoId];

    if (provisional) {
      setLightboxProvisionalFlag(photoId, normalized, seq);
      if (vireoLightboxSession.requestedPhotoId() === photoId && (lbState.flagEditSeq === seq || lbState.flagPendingWrites === 0)) {
        _lbSetFlagStatus(_lbDisplayedFlagFor(photoId));
      }
    } else if (landed) {
      var provisionalSeq = _lbProvisionalFlagSeq[String(photoId)] || 0;
      if (provisionalSeq <= seq) {
        delete _lbProvisionalFlags[String(photoId)];
        delete _lbProvisionalFlagSeq[String(photoId)];
      }
      // Cache the confirmed flag even if the user has navigated away, so the
      // quiescence emit below reflects what actually landed for this photo.
      _lbCacheFlag(photoId, normalized);
      if (vireoLightboxSession.requestedPhotoId() === photoId && (lbState.flagEditSeq === seq || lbState.flagPendingWrites === 0)) {
        _lbSetFlagStatus(_lbDisplayedFlagFor(photoId));
      }
    } else if (vireoLightboxSession.requestedPhotoId() === photoId && lbState.flagEditSeq === seq) {
      // Write failed: the prior confirmed flag stands; restore the chip to it.
      _lbSetFlagStatus(_lbConfirmedFlagFor(photoId));
    }

    // Once every in-flight write for THIS photo has settled, tell listeners
    // (e.g. Highlights) the photo's final confirmed flag. Emitting on
    // quiescence — rather than per write — means a landed reject whose
    // superseding clear/flag write later FAILED is still reflected, and rapid
    // same-photo toggling collapses to one correct event instead of a guess.
    if (!provisional && !Object.prototype.hasOwnProperty.call(_lbFlagPendingByPhoto, photoId)) {
      try {
        document.dispatchEvent(new CustomEvent('lightbox:flagchanged', {
          detail: {
            photoId: photoId,
            flag: _lbConfirmedFlagFor(photoId),
            previousFlag: previousFlag,
          },
        }));
      } catch (_) {}
    }
  }

  Promise.resolve(write)
    .then(function(result) { settle(result); })
    .catch(function() { settle(false); });

  return true;
}
