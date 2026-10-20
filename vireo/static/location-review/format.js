// Number, date, and distance formatting, and the great-circle distance.
// Classic page script; load boot.js after all definitions.
'use strict';

/** @param {number | undefined} value */ function formatNumber(value) { return Number(value || 0).toLocaleString(); }

/** @param {string|null|undefined} value */ function formatDateTime(value) {
  if (!value) return 'Date unavailable';
  return window.VireoTime.captureDateTime(value, false, {
    month: 'short', day: 'numeric', year: 'numeric', hour: 'numeric', minute: '2-digit'
  });
}

/** @param {LocationGroup} group */ function formatCaptureRange(group) {
  if (!group.captured_from) return 'Date unavailable';
  if (!group.captured_to || group.captured_to === group.captured_from) return formatDateTime(group.captured_from);
  return formatDateTime(group.captured_from) + ' – ' + formatDateTime(group.captured_to);
}

/** @param {number} meters */ function formatDistance(meters) {
  meters = Number(meters || 0);
  if (meters < 160.934) return Math.max(1, Math.round(meters * 3.28084)) + ' ft';
  return (meters / 1609.344).toFixed(meters < 16093 ? 1 : 0) + ' mi';
}

/** @param {number} lat1 @param {number} lng1 @param {number} lat2 @param {number} lng2 */ function distanceBetween(lat1, lng1, lat2, lng2) {
  var toRadians = Math.PI / 180;
  var first = lat1 * toRadians;
  var second = lat2 * toRadians;
  var deltaLat = (lat2 - lat1) * toRadians;
  var deltaLng = (lng2 - lng1) * toRadians;
  var value = Math.sin(deltaLat / 2) ** 2 + Math.cos(first) * Math.cos(second) * Math.sin(deltaLng / 2) ** 2;
  return 12742000 * Math.asin(Math.min(1, Math.sqrt(value)));
}
