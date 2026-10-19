// Volume suggestions for the source field and the ?source= / ?others=
// links from the import page.
// ES module.

export async function loadVolumeSuggestions() {
  try {
    const resp = await fetch('/api/volumes');
    if (!resp.ok) return;
    const volumes = /** @type {(string | {path?: string})[] | null} */ (await resp.json());
    const dl = /** @type {HTMLElement} */ (document.getElementById('volumeDatalist'));
    (volumes || []).forEach((v) => {
      const opt = document.createElement('option');
      opt.value = typeof v === 'string' ? v : (v.path || String(v));
      dl.appendChild(opt);
    });
  } catch (e) { /* suggestions only */ }
}

// The import page's result card links here with the finished import's card
// path; multi-source imports pass the first one in ?source= and the rest
// in ?others= so this page can name them instead of silently dropping them.
export function cardCleanupApplyQueryParams() {
  const params = new URLSearchParams(window.location.search);
  const source = (params.get('source') || '').trim();
  if (source) /** @type {HTMLInputElement} */ (document.getElementById('card-cleanup-source')).value = source;
  const others = params.getAll('others').filter(s => s && s.trim());
  const note = /** @type {HTMLElement} */ (document.getElementById('card-cleanup-source-note'));
  if (others.length) {
    note.style.display = '';
    note.textContent =
      'That import used ' + (others.length + 1) +
      ' source folders; this filled in the first one. Check the others one ' +
      'at a time: ' + others.join(', ');
  } else {
    note.style.display = 'none';
    note.textContent = '';
  }
}
