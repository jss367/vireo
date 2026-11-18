// The full frame and the exact region sit together for visual adjudication.
import { byId, showCheckError } from './helpers.js';
import { loadSummary } from './summary.js';
import { safeFetch } from './shared.js';

/** @param {AuditSubjectPhoto} row */
function card(row) {
  const figure = document.createElement('figure');
  const link = document.createElement('a');
  link.href = '/browse?photo_id=' + row.id;
  const images = [['Full frame', '/thumbnails/' + row.id + '.jpg']];
  if (row.detection_id) images.push(['Selected region', '/photos/' + row.id + '/crop?detection_id=' + row.detection_id]);
  for (const [alt, src] of images) {
    const img = document.createElement('img');
    img.loading = 'lazy';
    img.alt = alt || '';
    img.src = src || '';
    img.width = 144;
    link.append(img);
  }
  const caption = document.createElement('figcaption');
  caption.textContent = [row.filename, row.species, row.bucket_size && row.bucket_size + ' in bucket',
    row.whole_frame ? 'whole-frame guess' : row.weak && 'weak detector box',
    row.unnamed && 'unclassified subject', row.stale_mask && 'mask prompt mismatch',
    row.stale_score && 'quality score from prior subject'].filter(Boolean).join(' · ');
  figure.append(link, caption);
  return figure;
}

/** @param {string} title @param {AuditSubjectPhoto[]} rows */
function section(title, rows) {
  const group = document.createElement('details');
  const summary = document.createElement('summary');
  summary.textContent = title + ' (' + rows.length + ')';
  const grid = document.createElement('div');
  grid.className = 'subject-cards';
  const more = document.createElement('button');
  more.textContent = 'Show next 24';
  let shown = 0;
  function appendPage() {
    for (const row of rows.slice(shown, shown + 24)) grid.append(card(row));
    shown += 24;
    more.hidden = shown >= rows.length;
  }
  group.addEventListener('toggle', () => { if (group.open && !shown) appendPage(); });
  more.addEventListener('click', appendPage);
  group.append(summary, grid, more);
  return group;
}

export async function runSubjectCheck() {
  const button = /** @type {HTMLButtonElement} */ (byId('btnSubjects'));
  if (button.disabled) return;
  button.disabled = true;
  const status = byId('subjectStatus');
  status.textContent = 'Checking workspace subject evidence…';
  const content = byId('subjectContent');
  content.replaceChildren();
  try {
    const result = /** @type {AuditSubjectReport} */ (await safeFetch('/api/audit/subjects', {}, {toast: false}));
    content.append(section('Subjects missing a classification despite a whole-frame prediction', result.findings.filter(r => r.unnamed)),
      section('Active masks with a different prompt', result.findings.filter(r => r.stale_mask)),
      section('Quality scores from a different subject', result.findings.filter(r => r.stale_score)),
      section('Unconfirmed Highlights entries in small buckets or from weak regions', result.review),
      section('Reference photos', result.references));
    status.textContent = result.findings.length + ' photos with analysis gaps. Compare the crop with the full frame. Add reference photos to a static collection named “Subject audit references”.';
    await loadSummary();
  } catch (error) {
    showCheckError(status, error);
  } finally {
    button.disabled = false;
  }
}
