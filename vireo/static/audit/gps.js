// GPS jumps and reversible catalog corrections.
import { byId, postAuditAction, showCheckError, showCheckResult } from './helpers.js';
import { safeFetch } from './shared.js';
import { loadSummary } from './summary.js';

/** @typedef {{kind:string, folder_id:number, key:string, ids:number[], folder:string, camera:string, first:string, last:string, before:string, after:string, distance:number, seconds:number, back:number, return_seconds:number, original:Point, suggested:Point, neighbor:string, reviewed:boolean, conflict:boolean}} Finding */
/** @typedef {{latitude:number, longitude:number}} Point */

/** @param {Point} p */
function coordinates(p) {
  return p.latitude.toFixed(6) + ', ' + p.longitude.toFixed(6);
}

/** @param {Finding} f */
function findingRow(f) {
  const row = document.createElement('section');
  row.className = 'clock-finding';
  if (f.kind === 'jump') {
    row.textContent = f.folder + ' · ' + f.camera + ' · ' + f.before + ' to ' + f.after +
      ': ' + f.distance + ' m in ' + f.seconds + ' s (' + coordinates(f.original) + ' to ' +
      coordinates(f.suggested) + '). Check these frames. There is no corroborated replacement point.';
    return row;
  }
  const title = document.createElement('strong');
  title.textContent = f.first + ' – ' + f.last + ' (' + f.ids.length + ' photos) · ' + (f.camera || f.folder);
  const evidence = document.createElement('p');
  evidence.textContent = f.folder + ' · From ' + f.before + ', moved ' + f.distance +
    ' m in ' + f.seconds + ' s. Returned ' + f.back + ' m in ' + f.return_seconds + ' s by ' + f.after + '.';
  const preview = document.createElement('p');
  if (f.kind !== 'saved') preview.textContent = 'Recorded ' + coordinates(f.original) + ' · ' +
    (f.reviewed ? 'Using ' : 'Suggested ') + coordinates(f.suggested) + ' from ' + f.neighbor;
  if (f.kind === 'saved') {
    evidence.textContent = 'The original sequence has changed.';
    preview.textContent = 'Saved catalog correction: ' + coordinates(f.suggested);
  }
  const button = document.createElement('button');
  button.className = 'btn';
  button.disabled = f.conflict;
  button.textContent = f.conflict ? 'Conflicting GPS review' : f.reviewed ? 'Restore camera GPS' : 'Use suggested coordinates';
  button.addEventListener('click', async () => {
    button.disabled = true;
    try {
      await postAuditAction('/api/audit/gps-jumps', {key:f.key, folder_id:f.folder_id, action:f.reviewed ? 'restore' : 'apply'});
      await runGpsCheck();
    } finally {
      button.disabled = f.conflict;
    }
  });
  row.append(title, evidence, preview, button);
  return row;
}

export async function runGpsCheck() {
  const status = byId('gpsStatus');
  status.textContent = 'Checking GPS positions in shot order…';
  try {
    const findings = /** @type {Finding[]} */ (await safeFetch('/api/audit/gps-jumps'));
    const count = findings.filter(f => !f.reviewed).length;
    byId('gpsBadge').textContent = String(count);
    byId('gpsContent').replaceChildren(...findings.map(findingRow));
    showCheckResult(status, count, 'sequence(s) to review', 'No unresolved GPS excursions found');
    await loadSummary();
  } catch (e) {
    showCheckError(status, e);
  }
}
