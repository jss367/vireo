// The classification inventory: which model and label set cover which
// photos.
// ES module.

import { escapeHtml, safeFetch } from './shared.js';

export async function loadClassificationInventory() {
  var body = /** @type {HTMLElement} */ (document.getElementById('classInvBody'));
  var summary = /** @type {HTMLElement} */ (document.getElementById('classInvSummary'));
  body.innerHTML = '<span style="color:var(--text-ghost);font-size:13px;">Loading...</span>';
  try {
    var data = /** @type {DashboardInventory | null} */ (await safeFetch('/api/workspace/classification-inventory', {}, { toast: false }));
    renderClassificationInventory(data);
    if (data && data.grand_total) {
      summary.textContent =
        (data.grand_total.total_predictions_rows || 0).toLocaleString() + ' predictions • ' +
        data.grand_total.classified_dets.toLocaleString() + ' / ' +
        (data.grand_total.classified_dets + data.grand_total.pending_dets).toLocaleString() +
        ' detection-pairs (' + data.grand_total.coverage_pct + '%)';
    }
  } catch(e) {
    body.innerHTML = '<span style="color:var(--danger);font-size:13px;">Failed to load inventory.</span>';
  }
}

/** @param {DashboardInventory | null} data */
export function renderClassificationInventory(data) {
  var body = /** @type {HTMLElement} */ (document.getElementById('classInvBody'));
  if (!data || !data.models || data.models.length === 0) {
    body.innerHTML = '<div style="color:var(--text-ghost);font-size:13px;">' +
      'No downloaded models or classification history in this workspace. ' +
      '<a href="/settings" style="color:var(--accent);">Download a model in Settings</a></div>';
    return;
  }
  if (data.total_real_detections === 0) {
    body.innerHTML = '<div style="color:var(--text-ghost);font-size:13px;">' +
      'No subject detections yet in this workspace. Run detection first.</div>';
    return;
  }

  var html = '<table class="content-table"><thead><tr>' +
    '<th style="width:90px;">Status</th>' +
    '<th>Label set</th>' +
    '<th class="num">Classified</th>' +
    '<th class="num">Pending</th>' +
    '<th class="num">Coverage</th>' +
    '<th class="num">Photos</th>' +
    '<th>Last run</th>' +
    '<th class="num">Median top-1</th>' +
    '<th style="width:70px;">Actions</th>' +
    '</tr></thead><tbody>';

  var models = data.models;
  models.forEach(function(m, /** @type {number} */ mi) {
    var headerCls = 'group-header' + (m.legacy || !m.downloaded ? ' muted' : '');
    var headerNote = '';
    if (m.legacy) headerNote = ' <span style="color:var(--text-ghost);font-weight:400;">(legacy — model no longer in registry)</span>';
    else if (!m.downloaded) headerNote = ' <span style="color:var(--text-ghost);font-weight:400;">(not downloaded)</span>';
    html += '<tr class="' + headerCls + '" data-inv-model="' + mi + '">' +
      '<td colspan="9">' +
        '<span class="chev" id="chev-' + mi + '">▾</span>' +
        escapeHtml(m.name) + headerNote +
        ' — <span style="font-weight:400;color:var(--text-secondary);">' +
          m.subtotal.classified_dets.toLocaleString() + ' / ' +
          (m.subtotal.classified_dets + m.subtotal.pending_dets).toLocaleString() +
          ' (' + m.subtotal.coverage_pct + '%)' +
          ' across ' + m.pairs.length + ' label set' + (m.pairs.length === 1 ? '' : 's') +
        '</span>' +
      '</td></tr>';
    m.pairs.forEach(function(p) {
      html += renderInvPairRow(m, p, mi);
    });
  });

  // Grand total
  html += '<tr class="grand-total">' +
    '<td></td>' +
    '<td>Total</td>' +
    '<td class="num">' + data.grand_total.classified_dets.toLocaleString() + '</td>' +
    '<td class="num">' + data.grand_total.pending_dets.toLocaleString() + '</td>' +
    '<td class="num">' + data.grand_total.coverage_pct + '%</td>' +
    '<td class="num"></td>' +
    '<td></td>' +
    '<td class="num"></td>' +
    '<td></td>' +
    '</tr>';
  html += '</tbody></table>';

  // Label sets that classify nothing at all: no row of their own above,
  // so say why rather than leaving the user to notice one is missing.
  if (data.unusable_label_sets && data.unusable_label_sets.length > 0) {
    data.unusable_label_sets.forEach(function(u) {
      html += '<div style="margin-top:10px;font-size:12px;color:var(--warning,#d08700);">' +
        escapeHtml(u.name) + ': no usable species' +
        (u.skipped ? ' — all ' + u.skipped + ' names are shared by several species' : '') +
        '. Download the list again in Settings → Labels to split them by scientific name.</div>';
    });
  }

  html += inventoryFooterHtml(data, models);

  body.innerHTML = html;
}

/**
 * Below the table: outdated label-set fingerprints, or the all-classified
 * note when nothing is outdated or unusable.
 *
 * @param {DashboardInventory} data @param {DashboardInventoryModel[]} models
 */
function inventoryFooterHtml(data, models) {
  var html = '';
  if (data.stale && data.stale.length > 0) {
    html += '<details class="stale-section"><summary>' +
      'Outdated label-set fingerprints (' + data.stale.length + ' row' +
      (data.stale.length === 1 ? '' : 's') + ')</summary>' +
      '<table class="content-table" style="margin-top:8px;"><thead><tr>' +
        '<th>Model</th><th>Fingerprint</th><th class="num">Predictions</th>' +
        '<th>Last run</th><th>Action</th>' +
      '</tr></thead><tbody>';
    data.stale.forEach(function(s) {
      html += '<tr>' +
        '<td>' + escapeHtml(s.model) + '</td>' +
        '<td><code style="font-size:11px;">' + escapeHtml(s.fingerprint.substring(0,12)) + '</code></td>' +
        '<td class="num">' + (s.stale_count || 0).toLocaleString() + '</td>' +
        '<td>' + (s.last_run ? formatRelative(s.last_run) : '—') + '</td>' +
        '<td><a class="act-link" href="/pipeline" title="Open pipeline to reclassify with current labels">Reclassify</a></td>' +
      '</tr>';
    });
    html += '</tbody></table></details>';
  } else {
    // Unusable sets are deliberately absent from data.models, so they
    // cannot make this false on their own — and "all combinations
    // classified" directly above "this set cannot be classified" is a
    // contradiction the user has to resolve.
    var allComplete = !(data.unusable_label_sets || []).length &&
      models.every(function(m) {
        return m.legacy || m.pairs.every(function(p) { return p.status === 'complete'; });
      });
    if (allComplete && (data.total_real_detections || 0) > 0) {
      html += '<div class="all-good" style="margin-top:14px;">✓ All combinations classified.</div>';
    }
  }

  return html;
}

/** @param {DashboardInventoryModel} model @param {DashboardInventoryPair} p @param {number} mi */
export function renderInvPairRow(model, p, mi) {
  var statusLabel = p.status.replace('_', ' ');
  var coverage = p.coverage_pct + '%' +
    '<span class="cov-mini"><span class="cov-mini-fill" style="width:' + p.coverage_pct + '%;"></span></span>';
  var lastRun = p.last_run ? formatRelative(p.last_run) : '—';
  var medianHtml = '—';
  if (p.median_top1_conf !== null && p.median_top1_conf !== undefined) {
    var dim = p.median_sample_size < 100 ? ' median-dim' : '';
    medianHtml = '<span class="' + dim.trim() + '" title="sample size: ' + p.median_sample_size + '">' +
      p.median_top1_conf.toFixed(2) + '</span>';
  }
  var actions = '';
  // Only render the Run deep-link if the model is downloaded — pipeline
  // model checkboxes are populated from downloaded models only, so a deep
  // link to a not-downloaded model is a dead end (nothing to pre-select).
  if (model.id && model.downloaded && (p.status === 'never_run' || p.status === 'partial' || p.status === 'stale')) {
    var labelsParam = '';
    if (p.label_set_path) {
      labelsParam = '&labels=' + encodeURIComponent(p.label_set_path);
    }
    actions += '<a class="act-link" href="/pipeline?models=' + encodeURIComponent(model.id) +
      labelsParam + '" title="Run classify with this combination">▶</a>';
  }
  if (p.classified_dets > 0) {
    actions += ' <a class="act-link" href="/review?model=' + encodeURIComponent(model.name) +
      '&labels_fingerprint=' + encodeURIComponent(p.fingerprint) +
      '" title="Inspect predictions for this combination">\u{1F50D}</a>';
  }
  return '<tr class="inv-row inv-row-m' + mi + '">' +
    '<td><span class="pill ' + p.status + '">' + statusLabel + '</span></td>' +
    '<td>' + escapeHtml(p.label_set) + '</td>' +
    '<td class="num">' + p.classified_dets.toLocaleString() + '</td>' +
    '<td class="num">' + p.pending_dets.toLocaleString() + '</td>' +
    '<td class="num">' + coverage + '</td>' +
    '<td class="num">' + p.photos_covered.toLocaleString() + '</td>' +
    '<td>' + lastRun + '</td>' +
    '<td class="num">' + medianHtml + '</td>' +
    '<td>' + actions + '</td>' +
    '</tr>';
}

/** @param {number} mi */
export function toggleInvModel(mi) {
  var rows = /** @type {NodeListOf<HTMLElement>} */ (document.querySelectorAll('.inv-row-m' + mi));
  var chev = document.getElementById('chev-' + mi);
  var hidden = rows.length > 0 && /** @type {HTMLElement} */ (rows[0]).style.display === 'none';
  rows.forEach(function(r) { r.style.display = hidden ? '' : 'none'; });
  if (chev) chev.textContent = hidden ? '▾' : '▸';
}

/** @param {string | null | undefined} iso */
export function formatRelative(iso) {
  if (!iso) return '—';
  var t = new Date(iso.endsWith('Z') || iso.includes('+') ? iso : iso + 'Z');
  var ms = Date.now() - t.getTime();
  if (isNaN(ms) || ms < 0) return iso;
  var s = Math.floor(ms / 1000);
  if (s < 60) return s + 's ago';
  var m = Math.floor(s / 60);
  if (m < 60) return m + 'm ago';
  var h = Math.floor(m / 60);
  if (h < 24) return h + 'h ago';
  var d = Math.floor(h / 24);
  if (d < 30) return d + 'd ago';
  var mo = Math.floor(d / 30);
  if (mo < 12) return mo + 'mo ago';
  return Math.floor(mo / 12) + 'y ago';
}
