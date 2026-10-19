// The charts: species, months and trend, ratings, flags, classification,
// detection, quality and hours.
// ES module.

import { escapeHtml, safeFetch } from './shared.js';
import { drillAttrs } from './drill.js';

/** @param {DashboardKeyword[]} keywords */
export function renderSpecies(keywords) {
  var species = keywords.filter(function(k) { return k.is_species; });
  var container = /** @type {HTMLElement} */ (document.getElementById('speciesChart'));
  if (species.length === 0) {
    container.innerHTML = '<span style="color:var(--text-ghost);font-size:13px;">No species classified yet. Run classification on the Classify page.</span>';
  } else {
    var max = /** @type {DashboardKeyword} */ (species[0]).photo_count;
    var html = '';
    species.slice(0, 20).forEach(function(k) {
      var pct = Math.round((k.photo_count / max) * 100);
      var attrs = drillAttrs(keywordDrill(k), 'Browse photos tagged ' + k.name)
        .replace('bar-row dashboard-drill', 'bar-row species-bar dashboard-drill');
      html += '<div' + attrs + '>' +
        '<span class="bar-label">' + escapeHtml(k.name) + '</span>' +
        '<div class="bar-track"><div class="bar-fill" style="width:' + pct + '%"></div></div>' +
        '<span class="bar-value">' + k.photo_count.toLocaleString() + '</span>' +
      '</div>';
    });
    container.innerHTML = html;
  }

  // Render top non-species keywords in their own section
  var nonSpecies = keywords.filter(function(k) { return !k.is_species; }).slice(0, 10);
  var otherSection = /** @type {HTMLElement} */ (document.getElementById('otherKeywordsSection'));
  var otherChart = /** @type {HTMLElement} */ (document.getElementById('otherKeywordsChart'));
  if (nonSpecies.length > 0) {
    var nonSpeciesMax = /** @type {DashboardKeyword} */ (nonSpecies[0]).photo_count;
    var otherHtml = '';
    nonSpecies.forEach(function(k) {
      var pct = Math.round((k.photo_count / nonSpeciesMax) * 100);
      otherHtml += '<div' + drillAttrs(keywordDrill(k), 'Browse photos tagged ' + k.name) + '>' +
        '<span class="bar-label">' + escapeHtml(k.name) + '</span>' +
        '<div class="bar-track"><div class="bar-fill" style="width:' + pct + '%;background:var(--text-ghost);"></div></div>' +
        '<span class="bar-value">' + k.photo_count.toLocaleString() + '</span>' +
      '</div>';
    });
    otherChart.innerHTML = otherHtml;
    otherSection.style.display = '';
  } else {
    otherSection.style.display = 'none';
  }
}

/** @param {DashboardKeyword} k @returns {DashboardBrowseParams} */
export function keywordDrill(k) {
  return {keyword_identity: k.identity, keyword_label: k.name};
}

/** @param {DashboardMonth[]} months */
export function renderMonths(months) {
  var container = /** @type {HTMLElement} */ (document.getElementById('monthChart'));
  if (months.length === 0) {
    container.innerHTML = '<span style="color:var(--text-ghost);font-size:13px;">No timestamp data available.</span>';
    return;
  }
  var max = Math.max.apply(null, months.map(function(m) { return m.count; }));
  var html = '';
  months.forEach(function(m) {
    var pct = Math.round((m.count / max) * 100);
    var end = monthEnd(m.month);
    var attrs = drillAttrs({date_from: m.month + '-01', date_to: end},
      'Browse photos captured in ' + m.month)
      .replace('bar-row dashboard-drill', 'bar-row month-bar dashboard-drill');
    html += '<div' + attrs + '>' +
      '<span class="bar-label">' + (m.month || 'Unknown') + '</span>' +
      '<div class="bar-track"><div class="bar-fill" style="width:' + pct + '%"></div></div>' +
      '<span class="bar-value">' + m.count.toLocaleString() + '</span>' +
    '</div>';
  });
  container.innerHTML = html;
}

/** @param {string} month */
export function monthEnd(month) {
  if (!/^\d{4}-\d{2}$/.test(month || '')) return month;
  var parts = month.split('-');
  var end = new Date(Date.UTC(parseInt(parts[0] || '', 10), parseInt(parts[1] || '', 10), 0));
  return end.toISOString().slice(0, 10);
}

/** @param {DashboardMonth[]} months */
export function renderTrendSummary(months) {
  var container = /** @type {HTMLElement} */ (document.getElementById('trendSummary'));
  if (!months.length) { container.innerHTML = ''; return; }
  var latest = /** @type {DashboardMonth} */ (months[months.length - 1]);
  var previous = months.length > 1 ? /** @type {DashboardMonth} */ (months[months.length - 2]) : null;
  var busiest = months.reduce(function(best, row) {
    return row.count > best.count ? row : best;
  }, /** @type {DashboardMonth} */ (months[0]));
  var change = 'First active month';
  if (previous && previous.count > 0) {
    var pct = Math.round(((latest.count - previous.count) / previous.count) * 100);
    change = (pct > 0 ? '+' : '') + pct + '% vs ' + previous.month;
  } else if (previous) {
    change = latest.count > 0 ? 'New activity' : 'No change';
  }
  container.innerHTML =
    '<div class="trend-stat"><strong>' + latest.count.toLocaleString() + '</strong><span>' +
      escapeHtml(latest.month) + '</span></div>' +
    '<div class="trend-stat"><strong>' + escapeHtml(change) + '</strong><span>Month-over-month</span></div>' +
    '<div class="trend-stat"><strong>' + busiest.count.toLocaleString() + '</strong><span>Busiest · ' +
      escapeHtml(busiest.month) + '</span></div>';
}

/** @param {{rating: number, count: number}[]} ratings */
export function renderRatings(ratings) {
  var container = /** @type {HTMLElement} */ (document.getElementById('ratingChart'));
  var total = ratings.reduce(function(sum, r) { return sum + r.count; }, 0);
  if (total === 0) return;
  var max = Math.max.apply(null, ratings.map(function(r) { return r.count; }));
  var html = '';
  for (var i = 0; i <= 5; i++) {
    var r = ratings.find(function(x) { return x.rating === i; });
    var count = r ? r.count : 0;
    var pct = max > 0 ? Math.round((count / max) * 100) : 0;
    var stars = i === 0 ? 'Unrated' : '';
    for (var s = 0; s < i; s++) stars += '\u2605';
    var attrs = i > 0
      ? drillAttrs({rating_min: i}, 'Browse photos rated ' + i + ' stars or higher')
          .replace('bar-row dashboard-drill', 'bar-row rating-bar dashboard-drill')
      : ' class="bar-row rating-bar"';
    html += '<div' + attrs + '>' +
      '<span class="bar-label">' + stars + '</span>' +
      '<div class="bar-track"><div class="bar-fill" style="width:' + pct + '%"></div></div>' +
      '<span class="bar-value">' + count.toLocaleString() + '</span>' +
    '</div>';
  }
  container.innerHTML = html;
}

/** @param {{flag: string, count: number}[]} flags */
export function renderFlags(flags) {
  var container = /** @type {HTMLElement} */ (document.getElementById('flagStats'));
  var html = '';
  flags.forEach(function(f) {
    var color = f.flag === 'flagged' ? 'var(--accent)' : f.flag === 'rejected' ? 'var(--danger)' : 'var(--text-dim)';
    var attrs = drillAttrs({flag: f.flag || 'none'}, 'Browse ' + (f.flag || 'unflagged') + ' photos')
      .replace('bar-row dashboard-drill', 'dashboard-drill');
    html += '<div' + attrs + ' style="display:flex;align-items:center;gap:8px;padding:8px;border-bottom:1px solid var(--border-subtle);">' +
      '<span style="color:' + color + ';font-size:14px;font-weight:600;text-transform:capitalize;">' + f.flag + '</span>' +
      '<span style="margin-left:auto;font-size:14px;color:var(--text-secondary);font-variant-numeric:tabular-nums;">' + f.count.toLocaleString() + '</span>' +
    '</div>';
  });
  container.innerHTML = html;
}

/** @param {{status: string, count: number}[]} predStatus @param {number} classified @param {number} total */
export function renderClassification(predStatus, classified, total) {
  var container = /** @type {HTMLElement} */ (document.getElementById('classificationStats'));
  var unclassified = total - classified;
  // Summary line
  var html = '<div style="margin-bottom:12px;font-size:13px;">' +
    '<span style="color:var(--accent);font-weight:600;">' + classified.toLocaleString() + '</span>' +
    ' <span style="color:var(--text-dim);">of ' + total.toLocaleString() + ' photos classified</span>' +
    (unclassified > 0 ? ' <span style="color:var(--warning);">(' + unclassified.toLocaleString() + ' unclassified)</span>' : '') +
  '</div>';
  if (predStatus.length === 0 && classified === 0) {
    html += '<span style="color:var(--text-ghost);font-size:13px;">No predictions yet. Run classification first.</span>';
    container.innerHTML = html;
    return;
  }
  // Bar chart of prediction statuses
  var max = Math.max.apply(null, predStatus.map(function(p) { return p.count; })) || 1;
  /** @type {Record<string, string>} */
  var colors = {pending: 'var(--warning)', accepted: 'var(--accent)', rejected: 'var(--danger)'};
  predStatus.forEach(function(p) {
    var pct = Math.round((p.count / max) * 100);
    var color = colors[p.status] || 'var(--text-dim)';
    html += '<div class="bar-row pred-bar">' +
      '<span class="bar-label" style="text-transform:capitalize;">' + (p.status || 'unknown') + '</span>' +
      '<div class="bar-track"><div class="bar-fill" style="width:' + pct + '%;background:' + color + ';"></div></div>' +
      '<span class="bar-value">' + p.count.toLocaleString() + '</span>' +
    '</div>';
  });
  container.innerHTML = html;
}

/** @param {number} detected @param {number} total */
export async function renderDetection(detected, total) {
  var container = /** @type {HTMLElement} */ (document.getElementById('detectionStats'));
  if (total === 0) {
    container.innerHTML = '<span style="color:var(--text-ghost);font-size:13px;">No photos.</span>';
    return;
  }
  if (detected === 0) {
    // Check if MegaDetector is even installed
    try {
      var info = /** @type {{megadetector?: string}} */ (await safeFetch('/api/system/info', {}, { toast: false }));
      if (info.megadetector !== 'installed') {
        container.innerHTML = '<div style="text-align:center;padding:16px 0;">' +
          '<div style="font-size:13px;color:var(--text-dim);">Subject detection requires MegaDetector.</div>' +
          '<div style="font-size:12px;color:var(--text-ghost);margin-top:8px;">Download MegaDetector from the <a href="/settings" style="color:var(--accent);">Settings</a> page.</div>' +
          '<div style="font-size:12px;color:var(--text-ghost);margin-top:4px;">Detection runs automatically during classification.</div>' +
        '</div>';
        return;
      }
    } catch(e) {}
  }
  var pct = Math.round((detected / total) * 100);
  var undetected = total - detected;
  var html = '<div style="text-align:center;padding:16px 0;">' +
    '<div style="font-size:36px;font-weight:700;color:var(--accent);">' + pct + '%</div>' +
    '<div style="font-size:12px;color:var(--text-dim);margin-top:4px;">subjects detected</div>' +
  '</div>' +
  '<div style="height:8px;background:var(--bg-tertiary);border-radius:4px;overflow:hidden;margin:8px 0;">' +
    '<div style="height:100%;width:' + pct + '%;background:var(--accent);border-radius:4px;transition:width 0.3s;"></div>' +
  '</div>' +
  '<div style="display:flex;justify-content:space-between;font-size:12px;color:var(--text-dim);">' +
    '<span>' + detected.toLocaleString() + ' detected</span>' +
    '<span>' + undetected.toLocaleString() + ' no detection</span>' +
  '</div>';
  container.innerHTML = html;
}

/** @param {{bucket: number, count: number}[]} dist */
export function renderQuality(dist) {
  var container = /** @type {HTMLElement} */ (document.getElementById('qualityChart'));
  // Separate unscored (bucket -1) from scored
  var unscored = 0;
  /** @type {{bucket: number, count: number}[]} */
  var buckets = [];
  dist.forEach(function(d) {
    if (d.bucket === -1) { unscored = d.count; return; }
    buckets.push(d);
  });
  if (buckets.length === 0 && unscored === 0) {
    container.innerHTML = '<span style="color:var(--text-ghost);font-size:13px;">No quality data. Run sharpness scoring first.</span>';
    return;
  }
  if (buckets.length === 0) {
    container.innerHTML = '<span style="color:var(--text-ghost);font-size:13px;">No scored photos yet (' + unscored.toLocaleString() + ' unscored). Run sharpness scoring first.</span>';
    return;
  }
  var max = Math.max.apply(null, buckets.map(function(b) { return b.count; }));
  var html = '';
  // Fill in empty buckets 0-10
  for (var i = 0; i <= 10; i++) {
    var b = buckets.find(function(x) { return x.bucket === i; });
    var count = b ? b.count : 0;
    var pct = max > 0 ? Math.round((count / max) * 100) : 0;
    var label = (i / 10).toFixed(1) + (i < 10 ? '-' + ((i + 1) / 10).toFixed(1) : '+');
    html += '<div class="bar-row quality-bar">' +
      '<span class="bar-label">' + label + '</span>' +
      '<div class="bar-track"><div class="bar-fill" style="width:' + pct + '%"></div></div>' +
      '<span class="bar-value">' + count.toLocaleString() + '</span>' +
    '</div>';
  }
  if (unscored > 0) {
    html += '<div style="margin-top:8px;font-size:12px;color:var(--text-faint);">' + unscored.toLocaleString() + ' photos not yet scored</div>';
  }
  container.innerHTML = html;
}

/** @param {{hour: number, count: number}[]} hours */
export function renderHours(hours) {
  var container = /** @type {HTMLElement} */ (document.getElementById('hourChart'));
  if (hours.length === 0) {
    container.innerHTML = '<span style="color:var(--text-ghost);font-size:13px;">No timestamp data available.</span>';
    return;
  }
  var max = Math.max.apply(null, hours.map(function(h) { return h.count; }));
  var html = '';
  for (var i = 0; i < 24; i++) {
    var h = hours.find(function(x) { return x.hour === i; });
    var count = h ? h.count : 0;
    var pct = max > 0 ? Math.round((count / max) * 100) : 0;
    var label = (i < 10 ? '0' : '') + i + ':00';
    html += '<div class="bar-row hour-bar">' +
      '<span class="bar-label">' + label + '</span>' +
      '<div class="bar-track"><div class="bar-fill" style="width:' + pct + '%"></div></div>' +
      '<span class="bar-value">' + count.toLocaleString() + '</span>' +
    '</div>';
  }
  container.innerHTML = html;
}
