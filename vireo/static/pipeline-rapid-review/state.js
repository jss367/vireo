// Page state: the loaded results and photo map, the burst sessions and the
// active queue, the staged decisions for the open burst, and the zoom state.
// Classic page script; load boot.js after all definitions.

/** @type {RapidState} */
var rapid = {
  results: null,
  photoMap: {},
  allSessions: [],
  sessions: [],
  sessionIndex: -1,
  items: [],
  picks: new Set(),
  rejects: new Set(),
  reviewed: new Set(),
  touched: new Set(),
  initialFlags: {},
  initialSpeciesKeywords: {},
  keywordStateSpecies: '',
  initialSpecies: '',
  history: [],
  currentIndex: -1,
  seeded: false,
  applying: false,
  loadError: false,
  historyRefreshFailed: false,
  zoomed: false,
  zoomPhotoId: null,
  zoomAnchor: null,
  zoomDrag: null,
  suppressZoomClick: false,
  // Bumped on each openSession; used to drop stale /group/state responses
  // when the user switches bursts before the previous fetch resolves.
  openToken: 0,
  queue: {
    filter: 'needs-species',
    includeRejected: false,
    multiOnly: false,
  },
  queuePromptResolve: null,
};

/** @type {Record<string,{label:string;empty:string}> & {all:{label:string;empty:string}}} */
var QUEUE_FILTERS = {
  'needs-species': { label: 'Needs species', empty: 'No bursts need species review.' },
  'needs-culling': { label: 'Needs culling', empty: 'No bursts need culling review.' },
  'unreviewed': { label: 'Unreviewed', empty: 'No unreviewed bursts.' },
  'review-picks': { label: 'Review picks', empty: 'No reviewed picks yet.' },
  'all': { label: 'All', empty: 'No cached pipeline bursts.' },
};
