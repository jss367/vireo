// Review totals and the focused encounter algorithm trace.
// Classic page script; shared globals are initialized before boot.js runs.

// -- Rendering --

/** @type {number | null} */ var _focusedEncounterIdx = null;

/** @param {number} idx */
function setFocusedEncounter(idx) {
  _focusedEncounterIdx = idx;
  /** @type {NodeListOf<HTMLElement>} */ (document.querySelectorAll('.encounter-card.focused')).forEach(function(el) {
    el.classList.remove('focused');
  });
  var card = document.querySelector('.encounter-card[data-encounter-index="' + idx + '"]');
  if (card) card.classList.add('focused');
  renderAlgorithmTrace();
}

function renderAlgorithmTrace() {
  var panel = /** @type {HTMLElement} */ (document.getElementById('algorithmTrace'));
  var label = /** @type {HTMLElement} */ (document.getElementById('traceFocusLabel'));
  if (!panel) return;
  if (_focusedEncounterIdx === null || !pipelineResults || !currentReviewResults().encounters) {
    panel.innerHTML = '';
    if (label) label.textContent = 'Click an encounter to see how it was formed';
    return;
  }
  var enc = currentReviewResults().encounters[_focusedEncounterIdx];
  if (!enc) {
    panel.innerHTML = '';
    if (label) label.textContent = 'Click an encounter to see how it was formed';
    return;
  }
  if (!enc.trace) {
    panel.innerHTML = '<div class="trace-empty">Trace not available &mdash; drag a slider to recompute.</div>';
    if (label) label.textContent = 'Encounter ' + (_focusedEncounterIdx + 1);
    return;
  }
  if (enc.trace.length === 0) {
    var emptyMsg = (enc.photo_count <= 1)
      ? 'Single-photo encounter &mdash; no internal cut points.'
      : 'No adjacent-pair scores (encounter has no internal pairs).';
    panel.innerHTML = '<div class="trace-empty">' + emptyMsg + '</div>';
    if (label) label.textContent = 'Encounter ' + (_focusedEncounterIdx + 1);
    return;
  }
  if (label) {
    label.textContent = 'Encounter ' + (_focusedEncounterIdx + 1) + ' — ' + enc.trace.length + ' adjacent pair' + (enc.trace.length > 1 ? 's' : '');
  }
  /** @type {Record<number,ReviewPhoto>} */ var photoMap = {};
  (currentReviewResults().photos || []).forEach(function(p) { photoMap[p.id] = p; });
  function shortName(/** @type {string} */ fname) {
    if (!fname) return '';
    var dot = fname.lastIndexOf('.');
    return dot > 0 ? fname.slice(0, dot) : fname;
  }
  function pairPhotoHtml(/** @type {number | null | undefined} */ pid, /** @type {string | undefined} */ fallbackName) {
    var p = photoMap[pid ?? -1];
    var name = shortName(p?.filename || fallbackName || '');
    var label = name || ('Photo ' + (pid != null ? pid : '?'));
    if (pid == null) {
      return '<div class="trace-pair-photo"><span class="name">' + escapeHtml(label) + '</span></div>';
    }
    // Under a Folder/Collection filter, render a pair photo outside the
    // scope without a link; openInspect() would refuse it anyway.
    if (!isPhotoActionableInReviewScope(pid)) {
      return '<div class="trace-pair-photo" title="' + escapeHtml(p?.filename || label) + '">'
        + '<span class="name">' + escapeHtml(label) + '</span>'
        + '</div>';
    }
    return '<div class="trace-pair-photo" onclick="openInspect(' + pid + ')" title="' + escapeHtml(p?.filename || label) + '">'
      + '<span class="name">' + escapeHtml(label) + '</span>'
      + '</div>';
  }
  var html = '';
  var thr = enc.trace[0] && enc.trace[0].thresholds;
  if (thr) {
    var thrParts = [];
    if (typeof thr.hard_cut_score === 'number') thrParts.push('hard_cut_score=' + thr.hard_cut_score.toFixed(2));
    if (typeof thr.soft_cut_score === 'number') thrParts.push('soft_cut_score=' + thr.soft_cut_score.toFixed(2));
    if (typeof thr.species_hard_cut_confidence === 'number') {
      thrParts.push('species_confidence=' + thr.species_hard_cut_confidence.toFixed(2));
    }
    if (typeof thr.species_hard_cut_margin === 'number') {
      thrParts.push('species_margin=' + thr.species_hard_cut_margin.toFixed(2));
    }
    if (typeof thr.weak_detection_confidence === 'number') {
      thrParts.push('weak_detection=' + thr.weak_detection_confidence.toFixed(2));
    }
    if (typeof thr.hard_cut_time === 'number') thrParts.push('hard_cut_time=' + thr.hard_cut_time + 's');
    if (thrParts.length) {
      html += '<div class="trace-thresholds">cut if: ' + thrParts.join(' · ') + '</div>';
    }
  }
  enc.trace.forEach(function(t, i) {
    var rowCls = 'trace-row';
    if (t.decision && t.decision.indexOf('cut_') === 0) rowCls += ' cut';
    else if (t.decision === 'merged_back') rowCls += ' merged-back';
    else rowCls += ' kept';
    var dt = (t.dt_seconds === null || t.dt_seconds === undefined) ? '∞' : t.dt_seconds.toFixed(1) + 's';
    var score = (typeof t.score === 'number') ? t.score.toFixed(3) : '-';
    /** @type {Record<string,string>} */ var decisionLabels = {
      kept_weak_detection: 'kept · weak detection rescued',
      kept_species_continuity: 'kept · neighboring frames support the same species'
    };
    var decision = decisionLabels[t.decision || ''] || t.decision || '?';
    html += '<div class="' + rowCls + '">';
    html += '<span>pair ' + (i + 1) + ' · ' + dt + '</span>';
    html += '<span>S=' + score + '</span>';
    html += '<span class="trace-decision">' + decision + '</span>';
    html += '</div>';
    // Caches written before commit 82b3144 lack photo_a_id/photo_b_id on
    // trace entries. Within an encounter the i-th adjacent-pair row is
    // photo_ids[i] → photo_ids[i+1] (cut_microsegments sorts by timestamp
    // and segment_encounters preserves that order), so we can recover the
    // IDs from the encounter without forcing a regroup.
    var encPhotoIds = requireReviewValue(enc).photo_ids || [];
    var aId = (t.photo_a_id != null) ? t.photo_a_id : (encPhotoIds[i] != null ? encPhotoIds[i] : null);
    var bId = (t.photo_b_id != null) ? t.photo_b_id : (encPhotoIds[i + 1] != null ? encPhotoIds[i + 1] : null);
    html += '<div class="trace-pair-photos">';
    html += pairPhotoHtml(aId, t.photo_a_filename);
    html += '<span class="trace-pair-arrow">→</span>';
    html += pairPhotoHtml(bId, t.photo_b_filename);
    html += '</div>';
    [aId, bId].forEach(function(pid) {
      var photo = photoMap[pid ?? -1];
      var weak = photo?.weak_detection_context;
      if (weak && weak.evidence === 'extended_sequence') {
        html += '<div class="trace-thresholds">'
          + escapeHtml(photo?.filename || ('Photo ' + pid))
          + (weak.support === 'anchor_context'
            ? ': weak animal detection kept using matching neighboring frames. This photo contributes no species vote; the neighbors support '
            : ': weak animal detection kept using matching classifier predictions and neighboring frames supporting ')
          + escapeHtml(weak.species) + '.</div>';
      }
      var context = photo?.isolated_species_context;
      if (!context) return;
      html += '<div class="trace-thresholds">'
        + escapeHtml(photoMap[pid ?? -1]?.filename || ('Photo ' + pid))
        + ': set aside the isolated ' + escapeHtml(context.conflicting_species)
        + ' prediction for grouping. Neighboring frames support '
        + escapeHtml(context.anchor_species) + '. The original classifier prediction is unchanged.</div>';
    });
    if (t.components) {
      var nameA = shortName((photoMap[aId ?? -1] || {}).filename || t.photo_a_filename || 'A');
      var nameB = shortName((photoMap[bId ?? -1] || {}).filename || t.photo_b_filename || 'B');
      /** @type {string[]} */ var parts = [];
      ['time', 'subj', 'global', 'species', 'meta'].forEach(function(k) {
        var c = t.components?.[k];
        if (!c) return;
        var v = (typeof c.value === 'number') ? c.value.toFixed(2) : '-';
        var w = (typeof c.weight === 'number') ? c.weight.toFixed(2) : '-';
        if (!c.used) {
          // Three reasons a component can be unused:
          //   (1) zero weight — user dialled it out
          //   (2) both sides subject_absent — neutral, no evidence either
          //       way (compute_s_enc drops the signal but it's NOT a
          //       "missing data, go embed it" prompt)
          //   (3) feature genuinely missing on one or both sides — actionable
          var weightZero = !(c.weight > 0);
          if (weightZero) {
            // Component is intentionally turned off — render in muted form
            // without a "missing" annotation.
            parts.push(k + '=' + v + '×' + w);
          } else if (c.absent_a && c.absent_b) {
            // Both detectors found no subject — signal is neutral, not
            // actionable. Render with the same muted "absent" styling
            // we use for the asymmetric used-but-zero case so users
            // don't go re-embed photos that legitimately have no subject.
            parts.push('<span class="trace-component-absent">' + escapeHtml(k)
              + '=neutral (no subject on both, w=' + w + ')</span>');
          } else if (c.uncertain_a || c.uncertain_b) {
            var uncertainWho;
            if (c.uncertain_a && c.uncertain_b) uncertainWho = 'both';
            else if (c.uncertain_a) uncertainWho = nameA;
            else uncertainWho = nameB;
            parts.push('<span class="trace-component-uncertain">'
              + escapeHtml(k) + '=context-rescued weak detection on '
              + escapeHtml(uncertainWho) + ' (w=' + w + ')</span>');
          } else {
            var who;
            if (c.missing_a && c.missing_b) who = 'both';
            else if (c.missing_a) who = nameA;
            else if (c.missing_b) who = nameB;
            else who = '?';
            parts.push('<span class="trace-component-missing">' + escapeHtml(k)
              + '=missing on ' + escapeHtml(who) + ' (w=' + w + ')</span>');
          }
        } else {
          // c.used is true. If one side is subject_absent (detector ran
          // and found nothing), name that side so the 0 is legible — the
          // value isn't "no info", it's evidence the encounter shouldn't
          // include this frame.
          if (c.absent_a || c.absent_b) {
            var absentWho;
            if (c.absent_a && c.absent_b) absentWho = 'both';
            else if (c.absent_a) absentWho = nameA;
            else absentWho = nameB;
            parts.push(escapeHtml(k) + '=' + v + '×' + w
              + ' <span class="trace-component-absent">(no subject on '
              + escapeHtml(absentWho) + ')</span>');
          } else {
            parts.push(k + '=' + v + '×' + w);
          }
        }
      });
      html += '<div class="trace-components">' + parts.join(' · ') + '</div>';
    }
  });
  panel.innerHTML = html;
}

/** @param {ReviewEncounter} enc */
function missingTimestampBadge(enc) {
  // Warn when an encounter contains photos with no EXIF timestamp (scan I/O
  // errors / unreadable files). Those photos can't be placed on the timeline,
  // so the group was assembled by file order — say so rather than letting it
  // look like a normal time-clustered encounter.
  var n = enc.missing_timestamp_count || 0;
  if (!n) return '';
  var title = n + ' photo' + (n === 1 ? '' : 's') +
    ' in this group ha' + (n === 1 ? 's' : 've') +
    ' no EXIF timestamp (unreadable file or scan error). ' +
    'Grouped by file order, not capture time.';
  return '<span class="enc-warning" title="' + escapeAttr(title) + '">⚠ ' +
    n + ' missing timestamp' + (n === 1 ? '' : 's') + '</span>';
}

/** @param {ReviewResults} results */
function countConfirmationUnits(results) {
  var counts = {confirmed: 0, unconfirmed: 0};
  if (!results || !Array.isArray(results.encounters)) return counts;
  results.encounters.forEach(function(enc) {
    var bursts = enc.bursts || [];
    if (bursts.length > 0) {
      bursts.forEach(function(burst) {
        if (isBurstConfirmed(enc, burst)) counts.confirmed++;
        else counts.unconfirmed++;
      });
    } else if (enc.species_confirmed) {
      counts.confirmed++;
    } else {
      counts.unconfirmed++;
    }
  });
  return counts;
}

function refreshLocalSummaryCounts() {
  if (!pipelineResults) return null;
  var summary = currentReviewResults().summary || {};
  var photos = currentReviewResults().photos || [];
  summary.total_photos = photos.length;
  summary.encounter_count = (currentReviewResults().encounters || []).length;
  summary.burst_count = (currentReviewResults().encounters || []).reduce(function(total, enc) {
    return total + (enc.burst_count || ((enc.bursts || []).length));
  }, 0);
  summary.keep_count = 0;
  summary.review_count = 0;
  summary.reject_count = 0;
  summary.rarity_protected = 0;
  photos.forEach(function(p) {
    if (p.label === 'KEEP') summary.keep_count++;
    else if (p.label === 'REVIEW') summary.review_count++;
    else if (p.label === 'REJECT') summary.reject_count++;
    if (p.rarity_protected) summary.rarity_protected++;
  });
  var confirmCounts = countConfirmationUnits(pipelineResults);
  summary.confirmed_count = confirmCounts.confirmed;
  summary.unconfirmed_count = confirmCounts.unconfirmed;
  currentReviewResults().summary = summary;
  return summary;
}

// Summary counts restricted to a Folder/Collection scope. Encounters and
// bursts count when any of their photos is in scope, matching what renders.
/** @param {ReviewResults} results @param {Set<number>} scopeIds */
function scopedSummaryCounts(results, scopeIds) {
  var summary = Object.assign({}, results.summary || {});
  var photos = (results.photos || []).filter(function(p) { return scopeIds.has(p.id); });
  function anyInScope(/** @type {number[]} */ ids) {
    return (ids || []).some(function(pid) { return scopeIds.has(pid); });
  }
  summary.total_photos = photos.length;
  summary.encounter_count = 0;
  summary.burst_count = 0;
  summary.confirmed_count = 0;
  summary.unconfirmed_count = 0;
  (results.encounters || []).forEach(function(enc) {
    if (!anyInScope(enc.photo_ids)) return;
    summary.encounter_count++;
    var bursts = enc.bursts || [];
    if (bursts.length > 0) {
      bursts.forEach(function(burst) {
        if (!anyInScope(burst.photo_ids || burst)) return;
        summary.burst_count++;
        if (isBurstConfirmed(enc, burst)) summary.confirmed_count++;
        else summary.unconfirmed_count++;
      });
    } else if (enc.species_confirmed) {
      summary.confirmed_count++;
    } else {
      summary.unconfirmed_count++;
    }
  });
  summary.keep_count = 0;
  summary.review_count = 0;
  summary.reject_count = 0;
  summary.rarity_protected = 0;
  photos.forEach(function(p) {
    if (p.label === 'KEEP') summary.keep_count++;
    else if (p.label === 'REVIEW') summary.review_count++;
    else if (p.label === 'REJECT') summary.reject_count++;
    if (p.rarity_protected) summary.rarity_protected++;
  });
  return summary;
}

/** @param {ReviewSummary | null} summary */
function getConfirmationUnitName(summary) {
  var confirmed = Number(requireReviewValue(summary).confirmed_count || 0);
  var unconfirmed = Number(requireReviewValue(summary).unconfirmed_count || 0);
  var totalUnits = confirmed + unconfirmed;
  var bursts = Number(requireReviewValue(summary).burst_count || 0);
  var encounters = Number(requireReviewValue(summary).encounter_count || 0);
  if (totalUnits > 0 && bursts === totalUnits) return totalUnits === 1 ? 'Burst' : 'Bursts';
  if (totalUnits > 0 && encounters === totalUnits) return totalUnits === 1 ? 'Encounter' : 'Encounters';
  return totalUnits === 1 ? 'Review Unit' : 'Review Units';
}

/** @param {string} unitName @param {number} count */
function confirmationUnitForCount(unitName, count) {
  if (count !== 1) return unitName.toLowerCase();
  if (unitName === 'Bursts') return 'burst';
  if (unitName === 'Encounters') return 'encounter';
  if (unitName === 'Review Units') return 'review unit';
  return unitName.toLowerCase();
}

/** @param {number} count @param {string} unitName @param {string} [suffix] */
function formatConfirmationDetail(count, unitName, suffix) {
  return count + ' ' + confirmationUnitForCount(unitName, count) + ' ' + suffix;
}

/** @param {ReviewSummary | null} summary */
function updateSummaryBar(summary) {
  if (pipelineResults && (requireReviewValue(summary).confirmed_count == null || requireReviewValue(summary).unconfirmed_count == null)) {
    summary = refreshLocalSummaryCounts() || summary;
  }
  var scopeIds = activeReviewScopeFilter();
  if (scopeIds && pipelineResults) summary = scopedSummaryCounts(pipelineResults, scopeIds);
  var confirmed = Number(requireReviewValue(summary).confirmed_count || 0);
  var unconfirmed = Number(requireReviewValue(summary).unconfirmed_count || 0);
  var confirmationTotal = confirmed + unconfirmed;
  var confirmationUnit = getConfirmationUnitName(summary);
  var el;
  el = /** @type {HTMLElement} */ (document.getElementById('statTotalPhotos')); if (el) el.textContent = String(requireReviewValue(summary).total_photos);
  el = /** @type {HTMLElement} */ (document.getElementById('statEncounterCount')); if (el) el.textContent = String(requireReviewValue(summary).encounter_count);
  el = /** @type {HTMLElement} */ (document.getElementById('statBurstCount')); if (el) el.textContent = String(requireReviewValue(summary).burst_count);
  el = /** @type {HTMLElement} */ (document.getElementById('statKeepCount')); if (el) el.textContent = String(requireReviewValue(summary).keep_count);
  el = /** @type {HTMLElement} */ (document.getElementById('statReviewCount')); if (el) el.textContent = String(requireReviewValue(summary).review_count);
  el = /** @type {HTMLElement} */ (document.getElementById('statRejectCount')); if (el) el.textContent = String(requireReviewValue(summary).reject_count);
  el = /** @type {HTMLElement} */ (document.getElementById('statConfirmedCount')); if (el) el.textContent = confirmed + ' / ' + confirmationTotal;
  el = /** @type {HTMLElement} */ (document.getElementById('statConfirmedLabel')); if (el) el.textContent = confirmationUnit + ' Confirmed';
  el = /** @type {HTMLElement} */ (document.getElementById('statConfirmedDetail')); if (el) el.textContent = formatConfirmationDetail(unconfirmed, confirmationUnit, 'need confirmation');
  el = /** @type {HTMLElement} */ (document.getElementById('statUnconfirmedCount')); if (el) el.textContent = String(unconfirmed);
  el = /** @type {HTMLElement} */ (document.getElementById('statUnconfirmedLabel')); if (el) el.textContent = 'Need Confirmation';
  el = /** @type {HTMLElement} */ (document.getElementById('statUnconfirmedDetail')); if (el) el.textContent = formatConfirmationDetail(confirmationTotal, confirmationUnit, 'total');
  el = /** @type {HTMLElement} */ (document.getElementById('speciesReviewSummary'));
  if (el) {
    el.title = 'Species confirmation is counted by review unit: bursts when an encounter has bursts, otherwise encounters. Current basis: ' + confirmationUnit.toLowerCase() + '.';
  }
  var protWrap = /** @type {HTMLElement} */ (document.getElementById('statProtectedWrap'));
  if (protWrap) {
    if (requireReviewValue(summary).rarity_protected) {
      protWrap.style.display = '';
      var pe = /** @type {HTMLElement} */ (document.getElementById('statProtected'));
      if (pe) pe.textContent = String(requireReviewValue(summary).rarity_protected);
    } else {
      protWrap.style.display = 'none';
    }
  }
}
