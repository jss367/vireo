// Storage's own escape and size formatters (storage-overview.js and the
// other Storage scripts share them by global name).
// Classic page script; loads first among the Storage page scripts.

/** @param {unknown} s */
function storageEscapeHtml(s) {
  return String(s == null ? '' : s)
    .replace(/&/g, '&amp;')
    .replace(/</g, '&lt;')
    .replace(/>/g, '&gt;')
    .replace(/"/g, '&quot;')
    .replace(/'/g, '&#39;');
}

/** @param {unknown} s */
function storageEscapeAttr(s) {
  return storageEscapeHtml(s).replace(/`/g, '&#96;');
}

/** @param {number | null | undefined} b */
function storageFormatBytes(b) {
  if (b == null || b === 0) return '0 B';
  if (b < 1024) return b + ' B';
  if (b < 1024 * 1024) return Math.round(b / 1024) + ' KB';
  if (b < 1024 * 1024 * 1024) return (b / (1024 * 1024)).toFixed(1) + ' MB';
  return (b / (1024 * 1024 * 1024)).toFixed(1) + ' GB';
}
