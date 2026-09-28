/* Visibility-aware polling.
 *
 * A hidden window (minimized, covered, or a background tab) has nobody
 * reading its banners and badges, yet a plain setInterval keeps asking the
 * server all night, and some of those asks start real work: an expired
 * new-images answer re-walks every library folder and the automatic
 * missing-originals check stats every photo, both over the NAS.
 *
 * Vireo.pollWhileVisible(fn, intervalMs, opts) calls fn every intervalMs
 * while the document is visible. A tick that comes due while it is hidden is
 * skipped and nothing is scheduled; when the document becomes visible again,
 * an overdue tick runs at once, so what the user sees on return is never
 * staler than one interval.
 *
 * opts.initialDelayMs  delay before the first tick (default intervalMs; the
 *                      caller makes its own page-load call if it wants one).
 * opts.runWhileHidden  predicate; while it returns true, ticks keep running
 *                      when hidden (the job poll drives the dock progress
 *                      while a job is active).
 *
 * Returns {stop()}.
 */
(function(global) {
  'use strict';

  var Vireo = global.Vireo = global.Vireo || {};

  Vireo.pollWhileVisible = function(fn, intervalMs, opts) {
    opts = opts || {};
    var doc = global.document;
    var timer = null;
    var stopped = false;
    var dueAt = Date.now() + (opts.initialDelayMs != null ? opts.initialDelayMs : intervalMs);

    function mayRun() {
      return !doc.hidden || !!(opts.runWhileHidden && opts.runWhileHidden());
    }

    function arm(delay) {
      if (timer !== null) clearTimeout(timer);
      timer = setTimeout(tick, Math.max(0, delay));
    }

    function tick() {
      timer = null;
      // Hidden: leave nothing scheduled; onVisibility resumes the poll.
      if (stopped || !mayRun()) return;
      dueAt = Date.now() + intervalMs;
      arm(intervalMs);
      try {
        var result = fn();
        if (result && typeof result.catch === 'function') result.catch(function() {});
      } catch (e) { /* the next tick retries */ }
    }

    function onVisibility() {
      if (stopped || doc.hidden || timer !== null) return;
      arm(dueAt - Date.now());
    }

    doc.addEventListener('visibilitychange', onVisibility);
    arm(dueAt - Date.now());

    return {
      stop: function() {
        stopped = true;
        if (timer !== null) clearTimeout(timer);
        timer = null;
        doc.removeEventListener('visibilitychange', onVisibility);
      },
    };
  };
})(window);
