// Shared display formatting. Photo capture times stay in the camera's clock;
// application events are instants displayed in the browser's local time zone.
(function() {
  'use strict';

  /** @param {unknown} value */
  function setFormat(value) {
    window.VIREO_TIME_FORMAT = value === '12h' || value === '24h' ? value : 'system';
  }

  /** @param {Intl.DateTimeFormatOptions} options */
  function clockOptions(options) {
    var result = Object.assign({}, options);
    if (window.VIREO_TIME_FORMAT === '12h') result.hourCycle = 'h12';
    if (window.VIREO_TIME_FORMAT === '24h') result.hourCycle = 'h23';
    return result;
  }

  /** @param {Date} value @param {Intl.DateTimeFormatOptions} [options] */
  function time(value, options = {}) {
    return value.toLocaleTimeString([], clockOptions(options));
  }

  /** @param {Date} value @param {Intl.DateTimeFormatOptions} [options] */
  function dateTime(value, options = {}) {
    return value.toLocaleString([], clockOptions(options));
  }

  /** @param {string | null | undefined} value */
  function captureParts(value) {
    var match = String(value || '').match(/^(\d{4}-\d{2}-\d{2})[T ](\d{2}:\d{2})(?::(\d{2}))?/);
    if (!match) return null;
    // Use UTC only as a formatting container. Never apply an offset or the
    // browser's time zone to the camera-local components (including at DST).
    var date = new Date(match[1] + 'T' + match[2] + ':' + (match[3] || '00') + 'Z');
    if (isNaN(date.getTime())) return null;
    return {day: match[1], date: date, hasSeconds: !!match[3]};
  }

  /** @param {string | null | undefined} value @param {boolean} [seconds] */
  function captureTime(value, seconds = true) {
    var parts = captureParts(value);
    if (!parts) return value || '';
    return time(parts.date, {
      timeZone: 'UTC', hour: '2-digit', minute: '2-digit',
      ...(seconds && parts.hasSeconds ? {second: '2-digit'} : {}),
    });
  }

  /** @param {string | null | undefined} value @param {boolean} [seconds]
   * @param {Intl.DateTimeFormatOptions} [options] */
  function captureDateTime(value, seconds = false, options) {
    var parts = captureParts(value);
    if (!parts) return value || '';
    if (options) return dateTime(parts.date, {
      hour: '2-digit', minute: '2-digit', ...options, timeZone: 'UTC',
      ...(seconds && parts.hasSeconds ? {second: '2-digit'} : {}),
    });
    return parts.day + ' ' + captureTime(value, seconds);
  }

  setFormat(window.VIREO_TIME_FORMAT);
  window.VireoTime = {setFormat, time, dateTime, captureTime, captureDateTime};
})();
