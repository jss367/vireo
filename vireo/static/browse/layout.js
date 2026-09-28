/* Browse: resizable sidebar and detail panel.
   Classic script sharing page globals; browse.html loads the
   browse/*.js files in dependency order. */

/* ---------- Resizable sidebar ---------- */
var BROWSE_SIDEBAR_DEFAULT_WIDTH = 260;
var BROWSE_SIDEBAR_MIN_WIDTH = 200;
var BROWSE_SIDEBAR_MAX_WIDTH = 600;
var BROWSE_SIDEBAR_STORAGE_KEY = 'vireo.browse.sidebarWidth';
var BROWSE_DETAIL_PANEL_DEFAULT_WIDTH = 340;
var BROWSE_DETAIL_PANEL_MIN_WIDTH = 280;
var BROWSE_DETAIL_PANEL_MAX_WIDTH = 600;
var BROWSE_DETAIL_PANEL_STORAGE_KEY = 'vireo.browse.detailPanelWidth';
// Room the resize cap must leave for the rest of the Browse layout: the
// other adjustable sidebar, both resize handles, and a minimum photo/content
// column so the grid stays usable.
var BROWSE_SIDEBAR_HANDLE_WIDTH = 7;
var BROWSE_MIN_CONTENT_WIDTH = 360;

function currentBrowseSidebarWidth() {
  var sidebar = document.getElementById('browseSidebar');
  return sidebar ? sidebar.getBoundingClientRect().width : BROWSE_SIDEBAR_DEFAULT_WIDTH;
}

function currentBrowseDetailPanelWidth() {
  var detailPanel = document.getElementById('detailPanel');
  return detailPanel ? detailPanel.getBoundingClientRect().width : BROWSE_DETAIL_PANEL_DEFAULT_WIDTH;
}

function browseSidebarMaxWidth() {
  return Math.max(
    BROWSE_SIDEBAR_MIN_WIDTH,
    Math.min(
      BROWSE_SIDEBAR_MAX_WIDTH,
      window.innerWidth
        - currentBrowseDetailPanelWidth()
        - (2 * BROWSE_SIDEBAR_HANDLE_WIDTH)
        - BROWSE_MIN_CONTENT_WIDTH
    )
  );
}

function browseDetailPanelMaxWidth() {
  return Math.max(
    BROWSE_DETAIL_PANEL_MIN_WIDTH,
    Math.min(
      BROWSE_DETAIL_PANEL_MAX_WIDTH,
      window.innerWidth
        - currentBrowseSidebarWidth()
        - (2 * BROWSE_SIDEBAR_HANDLE_WIDTH)
        - BROWSE_MIN_CONTENT_WIDTH
    )
  );
}

function updateBrowsePanelResizeLimits() {
  var sidebarResizer = document.getElementById('browseSidebarResizer');
  var detailResizer = document.getElementById('detailPanelResizer');
  if (sidebarResizer) sidebarResizer.setAttribute('aria-valuemax', String(browseSidebarMaxWidth()));
  if (detailResizer) detailResizer.setAttribute('aria-valuemax', String(browseDetailPanelMaxWidth()));
}

function setBrowseSidebarWidth(width, persist) {
  var sidebar = document.getElementById('browseSidebar');
  var resizer = document.getElementById('browseSidebarResizer');
  if (!sidebar || !resizer) return;
  var nextWidth = Math.round(Math.max(
    BROWSE_SIDEBAR_MIN_WIDTH,
    Math.min(browseSidebarMaxWidth(), Number(width) || BROWSE_SIDEBAR_DEFAULT_WIDTH)
  ));
  sidebar.style.width = nextWidth + 'px';
  sidebar.style.flexBasis = nextWidth + 'px';
  resizer.setAttribute('aria-valuenow', String(nextWidth));
  updateBrowsePanelResizeLimits();
  if (persist) {
    try { localStorage.setItem(BROWSE_SIDEBAR_STORAGE_KEY, String(nextWidth)); } catch (e) {}
  }
  requestAnimationFrame(updateGridTail);
}

function initBrowseSidebarResize() {
  var sidebar = document.getElementById('browseSidebar');
  var resizer = document.getElementById('browseSidebarResizer');
  if (!sidebar || !resizer) return;

  var storedWidth = BROWSE_SIDEBAR_DEFAULT_WIDTH;
  try { storedWidth = Number(localStorage.getItem(BROWSE_SIDEBAR_STORAGE_KEY)) || storedWidth; } catch (e) {}
  setBrowseSidebarWidth(storedWidth, false);

  resizer.addEventListener('pointerdown', function(e) {
    if (e.button !== 0) return;
    e.preventDefault();
    var startX = e.clientX;
    var startWidth = sidebar.getBoundingClientRect().width;
    resizer.setPointerCapture(e.pointerId);
    resizer.classList.add('dragging');
    document.body.classList.add('sidebar-resizing');

    function onPointerMove(moveEvent) {
      setBrowseSidebarWidth(startWidth + moveEvent.clientX - startX, false);
    }
    function stopDragging(upEvent) {
      setBrowseSidebarWidth(sidebar.getBoundingClientRect().width, true);
      resizer.classList.remove('dragging');
      document.body.classList.remove('sidebar-resizing');
      resizer.removeEventListener('pointermove', onPointerMove);
      resizer.removeEventListener('pointerup', stopDragging);
      resizer.removeEventListener('pointercancel', stopDragging);
      if (resizer.hasPointerCapture(upEvent.pointerId)) {
        resizer.releasePointerCapture(upEvent.pointerId);
      }
    }
    resizer.addEventListener('pointermove', onPointerMove);
    resizer.addEventListener('pointerup', stopDragging);
    resizer.addEventListener('pointercancel', stopDragging);
  });

  resizer.addEventListener('keydown', function(e) {
    if (e.key === 'ArrowLeft' || e.key === 'ArrowRight') {
      e.preventDefault();
      e.stopPropagation();
      var direction = e.key === 'ArrowRight' ? 1 : -1;
      var step = e.shiftKey ? 50 : 10;
      setBrowseSidebarWidth(sidebar.getBoundingClientRect().width + direction * step, true);
    } else if (e.key === 'Home') {
      e.preventDefault();
      e.stopPropagation();
      setBrowseSidebarWidth(BROWSE_SIDEBAR_MIN_WIDTH, true);
    } else if (e.key === 'End') {
      e.preventDefault();
      e.stopPropagation();
      setBrowseSidebarWidth(browseSidebarMaxWidth(), true);
    }
  });

  resizer.addEventListener('dblclick', function() {
    setBrowseSidebarWidth(BROWSE_SIDEBAR_DEFAULT_WIDTH, true);
  });
}

function setBrowseDetailPanelWidth(width, persist) {
  var detailPanel = document.getElementById('detailPanel');
  var resizer = document.getElementById('detailPanelResizer');
  if (!detailPanel || !resizer) return;
  var nextWidth = Math.round(Math.max(
    BROWSE_DETAIL_PANEL_MIN_WIDTH,
    Math.min(browseDetailPanelMaxWidth(), Number(width) || BROWSE_DETAIL_PANEL_DEFAULT_WIDTH)
  ));
  detailPanel.style.width = nextWidth + 'px';
  detailPanel.style.flexBasis = nextWidth + 'px';
  resizer.setAttribute('aria-valuenow', String(nextWidth));
  updateBrowsePanelResizeLimits();
  if (persist) {
    try { localStorage.setItem(BROWSE_DETAIL_PANEL_STORAGE_KEY, String(nextWidth)); } catch (e) {}
  }
  requestAnimationFrame(updateGridTail);
}

function initBrowseDetailPanelResize() {
  var detailPanel = document.getElementById('detailPanel');
  var resizer = document.getElementById('detailPanelResizer');
  if (!detailPanel || !resizer) return;

  var storedWidth = BROWSE_DETAIL_PANEL_DEFAULT_WIDTH;
  try { storedWidth = Number(localStorage.getItem(BROWSE_DETAIL_PANEL_STORAGE_KEY)) || storedWidth; } catch (e) {}
  setBrowseDetailPanelWidth(storedWidth, false);

  resizer.addEventListener('pointerdown', function(e) {
    if (e.button !== 0) return;
    e.preventDefault();
    var startX = e.clientX;
    var startWidth = detailPanel.getBoundingClientRect().width;
    resizer.setPointerCapture(e.pointerId);
    resizer.classList.add('dragging');
    document.body.classList.add('sidebar-resizing');

    function onPointerMove(moveEvent) {
      setBrowseDetailPanelWidth(startWidth + startX - moveEvent.clientX, false);
    }
    function stopDragging(upEvent) {
      setBrowseDetailPanelWidth(detailPanel.getBoundingClientRect().width, true);
      resizer.classList.remove('dragging');
      document.body.classList.remove('sidebar-resizing');
      resizer.removeEventListener('pointermove', onPointerMove);
      resizer.removeEventListener('pointerup', stopDragging);
      resizer.removeEventListener('pointercancel', stopDragging);
      if (resizer.hasPointerCapture(upEvent.pointerId)) {
        resizer.releasePointerCapture(upEvent.pointerId);
      }
    }
    resizer.addEventListener('pointermove', onPointerMove);
    resizer.addEventListener('pointerup', stopDragging);
    resizer.addEventListener('pointercancel', stopDragging);
  });

  resizer.addEventListener('keydown', function(e) {
    if (e.key === 'ArrowLeft' || e.key === 'ArrowRight') {
      e.preventDefault();
      e.stopPropagation();
      var direction = e.key === 'ArrowLeft' ? 1 : -1;
      var step = e.shiftKey ? 50 : 10;
      setBrowseDetailPanelWidth(detailPanel.getBoundingClientRect().width + direction * step, true);
    } else if (e.key === 'Home') {
      e.preventDefault();
      e.stopPropagation();
      setBrowseDetailPanelWidth(BROWSE_DETAIL_PANEL_MIN_WIDTH, true);
    } else if (e.key === 'End') {
      e.preventDefault();
      e.stopPropagation();
      setBrowseDetailPanelWidth(browseDetailPanelMaxWidth(), true);
    }
  });

  resizer.addEventListener('dblclick', function() {
    setBrowseDetailPanelWidth(BROWSE_DETAIL_PANEL_DEFAULT_WIDTH, true);
  });
}

initBrowseSidebarResize();
initBrowseDetailPanelResize();
