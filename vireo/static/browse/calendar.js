/* Browse: timeline calendar heatmap.
   Classic script sharing page globals; browse.html loads the
   browse/*.js files in dependency order. */

/* ---------- Calendar Heatmap ---------- */
function toggleTimelineMode() {
  timelineMode = !timelineMode;
  var btn = document.getElementById('calToggle');
  btn.style.color = timelineMode ? 'var(--accent)' : 'var(--text-muted)';
  btn.style.borderColor = timelineMode ? 'var(--accent)' : 'var(--border-secondary)';

  var container = document.getElementById('calendarContainer');
  if (timelineMode) {
    container.classList.add('active');
    loadCalendarData();
  } else {
    container.classList.remove('active');
    clearCalendarSelection();
  }
}

function buildCalendarParams() {
  var params = new URLSearchParams();
  params.set('year', calendarYear);
  if (activeFolderId) params.set('folder_id', activeFolderId);
  // Dashboard-scoped Browse composes the collection with active filters —
  // without this the calendar shows counts from the whole workspace/folder
  // and a day-click applies a date chip based on photos not in the collection.
  if (activeCollectionId && dashboardCollectionScope) {
    params.set('collection_id', activeCollectionId);
  }
  // Strip any root-level `timestamp` rule from the calendar's own request.
  // selectCalendarDay adds/replaces exactly such a rule, and the filter
  // bar's onChange calls loadCalendarData() synchronously — feeding the
  // just-selected day back into the heatmap zeros out every other day and
  // makes it impossible to click through to another one. The heatmap is a
  // date picker: it should always show all days matching the non-date
  // filters (the visible grid still applies the date rule).
  var rules = getBrowseRulesWithoutTimestamp();
  if (rules) params.set('rules', JSON.stringify(rules));
  appendVisualScopeParams(params);
  return params;
}

function reconcileCalendarLoadRenders() {
  var gen = calendarDataLoadGen;
  while (gen > calendarRenderDecisionGen) {
    var state = calendarDataLoadStates[gen];
    if (!state || state.status === 'pending') return;
    if (state.status === 'success') {
      var currentKey = buildCalendarParams().toString();
      calendarRenderDecisionGen = gen;
      if (state.key === currentKey) {
        calendarData = state.data;
        renderCalendar();
      }
      Object.keys(calendarDataLoadStates).forEach(function(key) {
        if (Number(key) <= calendarDataLoadGen) delete calendarDataLoadStates[key];
      });
      return;
    }
    gen--;
  }
}

async function loadCalendarData() {
  var gen = ++calendarDataLoadGen;
  var params = buildCalendarParams();
  calendarDataLoadStates[gen] = {
    status: 'pending',
    key: params.toString()
  };

  try {
    var data = await safeFetch('/api/photos/calendar?' + params.toString(), {
      headers: Vireo.api.searchLaneHeaders(
        'calendar', searchLaneSeq(calendarLane, params.toString())),
    });
    var state = calendarDataLoadStates[gen];
    if (!state) return data;
    state.status = 'success';
    state.data = data;
    reconcileCalendarLoadRenders();
    return data;
  } catch(e) {
    var failedState = calendarDataLoadStates[gen];
    if (failedState) {
      failedState.status = 'failure';
      reconcileCalendarLoadRenders();
    }
    return null;
  }
}

function getBrowseRulesWithoutTimestamp() {
  var rules = getBrowseRules();
  if (!rules || !rules.rules) return rules;
  var kept = rules.rules.filter(function(r) {
    return !(r && !Array.isArray(r.rules) && r.field === 'timestamp');
  });
  if (!kept.length) return null;
  return { mode: rules.mode || 'all', rules: kept };
}

function renderCalendar() {
  if (!calendarData) return;

  document.getElementById('calYearLabel').textContent = calendarData.year;
  document.getElementById('calYearPrev').disabled = calendarData.year <= calendarData.min_year;
  document.getElementById('calYearNext').disabled = calendarData.year >= calendarData.max_year;

  // Compute intensity thresholds from data
  var counts = Object.values(calendarData.days);
  var maxCount = counts.length ? Math.max(...counts) : 0;
  var t1 = Math.max(1, Math.ceil(maxCount * 0.15));
  var t2 = Math.max(2, Math.ceil(maxCount * 0.40));
  var t3 = Math.max(3, Math.ceil(maxCount * 0.70));

  function getLevel(count) {
    if (!count) return 0;
    if (count <= t1) return 1;
    if (count <= t2) return 2;
    if (count <= t3) return 3;
    return 4;
  }

  // Build month labels
  var months = ['Jan','Feb','Mar','Apr','May','Jun','Jul','Aug','Sep','Oct','Nov','Dec'];
  var monthLabels = document.getElementById('calMonthLabels');
  monthLabels.innerHTML = months.map(function(m) { return '<span>' + m + '</span>'; }).join('');

  // Build grid: 53 weeks x 7 days
  var grid = document.getElementById('calGrid');
  grid.innerHTML = '';

  var dayNames = ['Sun','Mon','Tue','Wed','Thu','Fri','Sat'];
  for (var d = 0; d < 7; d++) {
    var label = document.createElement('div');
    label.className = 'day-label';
    label.textContent = (d % 2 === 1) ? dayNames[d].charAt(0) : '';
    label.style.gridRow = (d + 1);
    label.style.gridColumn = 1;
    grid.appendChild(label);
  }

  var jan1 = new Date(calendarData.year, 0, 1);
  var startDow = jan1.getDay();
  var isLeap = (calendarData.year % 4 === 0 && (calendarData.year % 100 !== 0 || calendarData.year % 400 === 0));
  var daysInYear = isLeap ? 366 : 365;

  for (var i = 0; i < daysInYear; i++) {
    var dt = new Date(calendarData.year, 0, 1 + i);
    var dow = dt.getDay();
    var weekNum = Math.floor((i + startDow) / 7);
    var dateStr = dt.getFullYear() + '-' +
        String(dt.getMonth() + 1).padStart(2, '0') + '-' +
        String(dt.getDate()).padStart(2, '0');
    var count = calendarData.days[dateStr] || 0;
    var level = getLevel(count);

    var cell = document.createElement('div');
    cell.className = 'day-cell' + (level ? ' level-' + level : ' empty') +
        (selectedDay === dateStr ? ' selected' : '');
    cell.style.gridRow = (dow + 1);
    cell.style.gridColumn = (weekNum + 2);
    cell.dataset.date = dateStr;
    cell.dataset.count = count;

    cell.addEventListener('click', function() {
      var d = this.dataset.date;
      if (selectedDay === d) {
        clearCalendarSelection();
      } else {
        selectCalendarDay(d, parseInt(this.dataset.count));
      }
    });

    cell.addEventListener('mouseenter', function(e) {
      var tip = document.getElementById('dayTooltip');
      var dateObj = new Date(this.dataset.date + 'T12:00:00');
      var formatted = dateObj.toLocaleDateString('en-US', { month: 'long', day: 'numeric', year: 'numeric' });
      var c = parseInt(this.dataset.count);
      tip.textContent = formatted + ' \u2014 ' + c + ' photo' + (c !== 1 ? 's' : '');
      tip.style.display = 'block';
      tip.style.left = (e.clientX + 10) + 'px';
      tip.style.top = (e.clientY - 30) + 'px';
    });

    cell.addEventListener('mousemove', function(e) {
      var tip = document.getElementById('dayTooltip');
      tip.style.left = (e.clientX + 10) + 'px';
      tip.style.top = (e.clientY - 30) + 'px';
    });

    cell.addEventListener('mouseleave', function() {
      document.getElementById('dayTooltip').style.display = 'none';
    });

    grid.appendChild(cell);
  }
}

function selectCalendarDay(dateStr, count) {
  // /api/photos/calendar can resolve before VireoFilter.init() has
  // loaded /api/filters/fields, so a click that lands in that window
  // would reach addRule -> makeRule with state.fields still null and
  // throw \u2014 silently swallowing the day pick and leaving the calendar
  // in a half-selected state (chip up, no filter applied). Bail out
  // until the filter registry is ready; the click will succeed on the
  // next attempt once the field payload lands.
  if (!window.VireoFilter || !VireoFilter.isReady()) return;
  selectedDay = dateStr;
  var dateObj = new Date(dateStr + 'T12:00:00');
  var formatted = dateObj.toLocaleDateString('en-US', { month: 'long', day: 'numeric', year: 'numeric' });
  document.getElementById('calSelectionText').textContent = formatted + ' \u00b7 ' + count + ' photo' + (count !== 1 ? 's' : '');
  document.getElementById('calSelection').classList.add('active');

  // Day selection is a capture-date rule in the filter bar (single-day
  // between; the backend pads the upper bound to end-of-day).
  // Clear the collection scope BEFORE addRule() — its onChange fires the
  // reload synchronously, so mutating scope after would only take effect
  // for later requests (infinite scroll, select-all, summary, calendar).
  // Dashboard-scoped collection Browse composes collection + rules and
  // must keep activeCollectionId.
  if (!dashboardCollectionScope) activeCollectionId = null;
  VireoFilter.addRule('timestamp', 'between', [dateStr, dateStr]);
  renderCalendar(); // update selected state
}

function clearCalendarSelection() {
  // Only drop collection scope when a day was actually selected — that
  // scope is what selectCalendarDay cleared, and its removal here pairs
  // with the removeField('timestamp') reload. Toggling the calendar off
  // (or clearing selection twice) with no day picked hits this path with
  // nothing to remove: removeField would return early without firing
  // onChange, so a silent activeCollectionId=null would leave the grid
  // showing the collection while infinite-scroll/summary/select-all
  // silently widened to the whole workspace.
  //
  // The removeField('timestamp') is also gated on hadSelection: a user
  // with a manual capture-date rule from a deep link or the filter
  // popover (never entered via the calendar) toggling the calendar on
  // and back off would otherwise silently clear their date filter.
  var hadSelection = selectedDay !== null;
  selectedDay = null;
  document.getElementById('calSelection').classList.remove('active');
  if (hadSelection && !dashboardCollectionScope) activeCollectionId = null;
  if (hadSelection && window.VireoFilter) VireoFilter.removeField('timestamp');
  if (calendarData) renderCalendar();
}

function calendarChangeYear(delta) {
  if (delta < 0 && calendarData && calendarYear <= calendarData.min_year) return;
  if (delta > 0 && calendarData && calendarYear >= calendarData.max_year) return;
  calendarYear += delta;
  // Same guards as clearCalendarSelection: only touch collection scope
  // and only removeField('timestamp') when a day was actually picked
  // (that day-pick is what dropped scope and set the timestamp rule).
  // Without them, switching year in a collection view before ever
  // selecting a day silently widens later requests to the workspace,
  // and stepping through years with a manual timestamp rule active
  // would silently clear the user's date filter.
  var hadSelection = selectedDay !== null;
  selectedDay = null;
  document.getElementById('calSelection').classList.remove('active');
  if (hadSelection && !dashboardCollectionScope) activeCollectionId = null;
  if (hadSelection && window.VireoFilter) VireoFilter.removeField('timestamp');
  loadCalendarData();
}
